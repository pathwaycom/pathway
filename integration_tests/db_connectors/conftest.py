import logging
import pathlib

import pytest
from utils import (
    MYSQL_DB_HOST,
    MYSQL_LOCAL_INFILE_DB_HOST,
    AtlasContext,
    ClickHouseContext,
    DebeziumContext,
    DynamoDBContext,
    ElasticsearchContext,
    MilvusContext,
    MongoDBContext,
    MssqlContext,
    MySQLContext,
    NeonContext,
    PgvectorContext,
    PineconeContext,
    PostgresContext,
    PostgresWithTlsContext,
    QuestDBContext,
    clickhouse_concurrency_slot,
    elasticsearch_concurrency_slot,
    mongodb_concurrency_slot,
    mssql_concurrency_slot,
    mysql_concurrency_slot,
    postgres_concurrency_slot,
)

DEFAULT_TEST_TIMEOUT_SECONDS = 600
_DB_CONNECTORS_DIR = pathlib.Path(__file__).resolve().parent


def _quiet_tiberius_logs() -> None:
    """Silence the tiberius (SQL Server driver) per-connection WARN spam.

    Every MSSQL connection makes the Rust ``tiberius`` crate emit two WARN
    records — ``Trusting the server certificate without validation`` and
    ``Turning TLS off after a login`` — which Pathway forwards to Python's
    ``logging`` through ``pyo3_log`` under logger names like
    ``tiberius.client.tls_stream.rustls_tls_stream``.  Across the whole MSSQL
    suite (every test opens several connections, and the CDC readers reconnect
    on each poll) these two lines dominate the captured output and bury the
    genuinely useful diagnostics — the test failures and the real connector
    errors.  We knowingly trust the cert and downgrade TLS in this test
    harness, so the warnings carry no signal; raise the threshold to ERROR so
    only real tiberius problems get through.

    Set at import time, in the parent process, so the level is in place before
    any connection is made and is inherited by the forked persistence / CDC
    reader subprocesses (``multiprocessing`` uses ``fork`` here, so both the
    Python ``logging`` config and ``pyo3_log``'s level cache carry over).
    """
    logging.getLogger("tiberius").setLevel(logging.ERROR)


_quiet_tiberius_logs()


def pytest_collection_modifyitems(config, items):
    # Default every db_connectors test to a 10-minute wall-clock cap so a single
    # hung pw.run() can't burn the whole Jenkins job. method="thread" dumps every
    # thread's stack on expiry and calls os._exit(1), which works even when the
    # hang is inside a Rust call that's holding the GIL or blocked on I/O —
    # signal mode requires the worker's main Python thread to be schedulable.
    # Scoped by path so this conftest doesn't affect tests outside db_connectors
    # when pytest is invoked higher up the tree.
    for item in items:
        try:
            item_path = pathlib.Path(item.fspath).resolve()
        except (OSError, ValueError):
            continue
        if _DB_CONNECTORS_DIR not in item_path.parents:
            continue
        if any(m.name == "timeout" for m in item.iter_markers()):
            continue
        item.add_marker(
            pytest.mark.timeout(DEFAULT_TEST_TIMEOUT_SECONDS, method="thread")
        )


@pytest.fixture
def postgres():
    # Cap concurrent postgres tests so logical-replication walsender churn stays
    # below the level that makes the server terminate freshly started walsenders
    # under load. Held for the whole test (incl. the forked streaming reader).
    with postgres_concurrency_slot():
        yield PostgresContext()


@pytest.fixture
def postgres_with_tls():
    return PostgresWithTlsContext()


@pytest.fixture
def pgvector():
    return PgvectorContext()


@pytest.fixture
def pinecone():
    # Each test gets a fresh control-plane client and drops every index it
    # created on teardown, since Pinecone Local does not persist across tests
    # but a single emulator instance is shared by the whole suite.
    ctx = PineconeContext()
    yield ctx
    ctx.cleanup()


@pytest.fixture
def neon():
    # ``NeonContext`` is a context manager: on exit it drops every table the
    # test created so nothing survives the run, whatever the verdict — Neon
    # storage quotas are tight and the backend may be a real cloud project,
    # not just an ephemeral branch.
    with NeonContext() as ctx:
        yield ctx


@pytest.fixture
def questdb():
    return QuestDBContext()


@pytest.fixture
def clickhouse():
    # Cap concurrent clickhouse tests to keep the single container off the rare
    # acked-but-not-visible insert window seen under maximum concurrency.
    with clickhouse_concurrency_slot():
        yield ClickHouseContext()


@pytest.fixture(scope="session")
def mongodb():
    ctx = MongoDBContext()
    yield ctx
    ctx.client.close()


@pytest.fixture(scope="session")
def atlas():
    # One shared client against the `mongodb-atlas` compose service for the whole
    # session, mirroring the `mongodb` fixture above. The Atlas Local image is
    # heavy (it runs both mongod and the mongot search process), so the tests are
    # also pinned to a single xdist worker via `xdist_group("mongodb_atlas")`.
    ctx = AtlasContext()
    yield ctx
    ctx.client.close()


@pytest.fixture(autouse=True)
def _mongodb_concurrency_cap(request):
    # The `mongodb` context is session-scoped (one shared client), so the
    # per-test concurrency bound can't live on that fixture. Gate every test
    # that uses `mongodb` here instead, so no more than a fixed number tail the
    # oplog at once — this is what keeps the change-stream readers from timing
    # out under load, the root the module-level `flaky` reruns used to paper
    # over. Tests that don't touch mongodb pass through untouched.
    if "mongodb" in request.fixturenames:
        with mongodb_concurrency_slot():
            yield
    else:
        yield


@pytest.fixture
def elasticsearch():
    # Bound how many ES tests hammer the single, small-heap ES node at once so
    # newly created indices' primary shards activate before writes arrive (see
    # ELASTICSEARCH_MAX_CONCURRENCY).
    with elasticsearch_concurrency_slot():
        ctx = ElasticsearchContext()
        yield ctx
        ctx.cleanup()


@pytest.fixture
def debezium():
    return DebeziumContext()


@pytest.fixture
def dynamodb():
    return DynamoDBContext()


@pytest.fixture
def mysql():
    # Cap concurrent mysql tests so binlog-tailing streaming tests aren't racing
    # a saturated server (the binlog rwlock below only serializes the
    # destructive purge test, not general load).
    with mysql_concurrency_slot():
        ctx = MySQLContext()
        try:
            yield ctx
        finally:
            # Close promptly instead of waiting for garbage collection: under
            # xdist the accumulated open connections otherwise exhaust the
            # server's max_connections and surface as flaky "Too many
            # connections" errors.
            ctx.close()


@pytest.fixture(
    params=[MYSQL_DB_HOST, MYSQL_LOCAL_INFILE_DB_HOST],
    ids=["local_infile_off", "local_infile_on"],
)
def mysql_either_strategy(request):
    """A ``MySQLContext`` against each of the two servers, so a parametrized test
    runs once per write strategy: the default server (``local_infile=OFF``) drives
    the multi-row INSERT fallback, the ``--local-infile=ON`` server drives the
    LOAD DATA LOCAL INFILE fast path. ``ctx.connection_string`` points at the
    matching server."""
    with mysql_concurrency_slot():
        ctx = MySQLContext(host=request.param)
        try:
            yield ctx
        finally:
            ctx.close()


_MSSQL_CAPTURE_JOB_CONFIGURED = False


@pytest.fixture
def mssql():
    # Cap how many MSSQL tests hit the single SQL Server at once. The mark
    # `xdist_group("mssql")` is a no-op under the `worksteal` scheduler, so
    # without this every worker would pile onto the CDC subsystem and
    # occasionally overload it into a cascade of correlated checker timeouts.
    # Acquire the slot before connecting so the whole test (connect + CDC setup
    # + body + cleanup) counts against the cap.
    with mssql_concurrency_slot():
        yield from _mssql_session()


def _mssql_session():
    global _MSSQL_CAPTURE_JOB_CONFIGURED
    ctx = MssqlContext()
    # Guarantee the precondition every CDC test depends on: the database must
    # be CDC-enabled before any `sp_cdc_enable_table` call.  Done on every
    # fixture acquisition (idempotent, cheap no-op once enabled) rather than
    # trusting only the one-shot docker init, which can be skipped or silently
    # fail and would otherwise make the whole MSSQL suite fail in setup.
    ctx.ensure_database_cdc()
    if not _MSSQL_CAPTURE_JOB_CONFIGURED:
        # Make the CDC capture job run back-to-back instead of sleeping
        # the default 5 s between scans.  Under heavy xdist parallelism
        # the capture agent falls many seconds behind, which causes
        # `wait_for_capture_count` to time out in flaky CDC tests.
        # Idempotent — sp_cdc_change_job updates msdb.dbo.cdc_jobs once
        # per server, so it's fine to call from every fixture
        # initialization that wins the race.
        try:
            ctx.execute_sql(
                "EXEC sys.sp_cdc_change_job @job_type=N'capture', @pollinginterval=0",
                drain_status_rows=True,
            )
        except Exception:
            # The proc requires elevated permissions; if it fails, fall
            # back to the default pollinginterval and tolerate the
            # additional latency.
            pass
        _MSSQL_CAPTURE_JOB_CONFIGURED = True
    try:
        yield ctx
    finally:
        # Drop every table + CDC capture instance the test created so the
        # database stays clean between runs.  Orphan capture instances
        # accumulate work in cdc.lsn_time_mapping and the SQL Agent's
        # per-instance scan, slowing every subsequent CDC test.
        ctx.cleanup()


@pytest.fixture
def milvus(tmp_path):
    ctx = MilvusContext(str(tmp_path / "milvus.db"))
    yield ctx
    ctx.close()

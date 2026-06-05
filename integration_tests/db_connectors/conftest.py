import pathlib

import pytest
from utils import (
    ClickHouseContext,
    DebeziumContext,
    DynamoDBContext,
    MilvusContext,
    MongoDBContext,
    MssqlContext,
    MySQLContext,
    PgvectorContext,
    PostgresContext,
    PostgresWithTlsContext,
    QuestDBContext,
)

DEFAULT_TEST_TIMEOUT_SECONDS = 600
_DB_CONNECTORS_DIR = pathlib.Path(__file__).resolve().parent


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
    return PostgresContext()


@pytest.fixture
def postgres_with_tls():
    return PostgresWithTlsContext()


@pytest.fixture
def pgvector():
    return PgvectorContext()


@pytest.fixture
def questdb():
    return QuestDBContext()


@pytest.fixture
def clickhouse():
    return ClickHouseContext()


@pytest.fixture(scope="session")
def mongodb():
    ctx = MongoDBContext()
    yield ctx
    ctx.client.close()


@pytest.fixture
def debezium():
    return DebeziumContext()


@pytest.fixture
def dynamodb():
    return DynamoDBContext()


@pytest.fixture
def mysql():
    ctx = MySQLContext()
    try:
        yield ctx
    finally:
        # Close promptly instead of waiting for garbage collection: under xdist
        # the accumulated open connections otherwise exhaust the server's
        # max_connections and surface as flaky "Too many connections" errors.
        ctx.close()


_MSSQL_CAPTURE_JOB_CONFIGURED = False


@pytest.fixture
def mssql():
    global _MSSQL_CAPTURE_JOB_CONFIGURED
    ctx = MssqlContext()
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

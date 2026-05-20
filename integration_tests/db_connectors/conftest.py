import pathlib

import pytest
from utils import (
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
    return MySQLContext()


@pytest.fixture
def mssql():
    ctx = MssqlContext()
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

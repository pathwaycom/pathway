import pytest
from utils import (
    DebeziumContext,
    DynamoDBContext,
    MongoDBContext,
    MySQLContext,
    PgvectorContext,
    PostgresContext,
    PostgresWithTlsContext,
    QuestDBContext,
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

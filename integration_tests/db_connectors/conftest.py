import pytest
from utils import (
    DebeziumContext,
    DynamoDBContext,
    MongoDBContext,
    MySQLContext,
    PgvectorContext,
    PostgresContext,
    QuestDBContext,
)


@pytest.fixture
def postgres():
    return PostgresContext()


@pytest.fixture
def pgvector():
    return PgvectorContext()


@pytest.fixture
def questdb():
    return QuestDBContext()


@pytest.fixture
def mongodb():
    return MongoDBContext()


@pytest.fixture
def debezium():
    return DebeziumContext()


@pytest.fixture
def dynamodb():
    return DynamoDBContext()


@pytest.fixture
def mysql():
    return MySQLContext()

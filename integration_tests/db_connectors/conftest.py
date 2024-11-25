import pytest
from utils import DebeziumContext, MongoDBContext, PostgresContext


@pytest.fixture
def postgres():
    return PostgresContext()


@pytest.fixture
def mongodb():
    return MongoDBContext()


@pytest.fixture
def debezium():
    return DebeziumContext()

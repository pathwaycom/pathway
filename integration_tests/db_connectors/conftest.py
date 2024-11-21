import pytest
from utils import MongoDBContext, PostgresContext


@pytest.fixture
def postgres():
    return PostgresContext()


@pytest.fixture
def mongodb():
    return MongoDBContext()

# Copyright © 2026 Pathway

import json
import uuid

import pytest

import pathway as pw
from pathway.engine import ExternalIndexFactory
from pathway.tests.utils import assert_table_equality

QDRANT_URL = "http://qdrant:6334"


def make_list(vector_as_str: str) -> list[float]:
    return [float(x) for x in vector_as_str.split(",")]


def unique_table_name_suffix() -> str:
    return str(uuid.uuid4()).replace("-", "")


class InputSchema(pw.Schema):
    pk_source: int = pw.column_definition(primary_key=True)
    data: str


class QuerySchema(pw.Schema):
    pk_source: int = pw.column_definition(primary_key=True)
    data: str
    limit: int


@pytest.mark.flaky(reruns=5)
def test_basic_search():
    index = pw.debug.table_from_markdown(
        """
    pk_source |data
    1         | 1,0,0
    2         | 0,1,0
    3         | 0,0,1
    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data   |limit
    1        |1,0,0  |2
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.qdrant_factory(
        url=QDRANT_URL,
        collection_name=f"test_basic_{unique_table_name_suffix()}",
        vector_size=3,
    )

    result = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).select(match_len=pw.apply_with_type(len, int, pw.this._pw_index_reply))

    class ExpectedSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        match_len: int

    expected = pw.debug.table_from_markdown(
        """
        pk_source|match_len
        1        |2
    """,
        schema=ExpectedSchema,
    ).without(pw.this.pk_source)

    assert_table_equality(result, expected)


@pytest.mark.flaky(reruns=5)
def test_with_deletions():
    index = pw.debug.table_from_markdown(
        """
    pk_source |data       | __time__ | __diff__
    1         | 1,0,0     | 2        | 1
    2         | 0,1,0     | 2        | 1
    3         | 0,0,1     | 2        | 1
    2         | 0,1,0     | 4        | -1
    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data   |limit | __time__
    1        |0,1,0  |5     | 6
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.qdrant_factory(
        url=QDRANT_URL,
        collection_name=f"test_deletions_{unique_table_name_suffix()}",
        vector_size=3,
    )

    result = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).select(match_len=pw.apply_with_type(len, int, pw.this._pw_index_reply))

    class ExpectedSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        match_len: int

    expected = pw.debug.table_from_markdown(
        """
        pk_source|match_len
        1        |2
    """,
        schema=ExpectedSchema,
    ).without(pw.this.pk_source)

    assert_table_equality(result, expected)


@pytest.mark.flaky(reruns=5)
def test_filter():
    class InputSchemaWithFilter(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        filter_data: str

    class QuerySchemaWithFilter(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int
        filter_col: str

    index = pw.debug.table_from_markdown(
        """
    pk_source |data     |filter_data
    1         | 1,0,0   |{"category":"A"}
    2         | 0,1,0   |{"category":"B"}
    3         | 0,0,1   |{"category":"A"}
    """,
        schema=InputSchemaWithFilter,
    ).with_columns(
        data=pw.apply(make_list, pw.this.data),
        filter_data=pw.apply(json.loads, pw.this.filter_data),
    )

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data   |limit|filter_col
    1        |1,0,0  |5    |category=='A'
    """,
        schema=QuerySchemaWithFilter,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    index_factory = ExternalIndexFactory.qdrant_factory(
        url=QDRANT_URL,
        collection_name=f"test_filter_{unique_table_name_suffix()}",
        vector_size=3,
    )

    result = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
        index_filter_data_column=index.filter_data,
        query_filter_column=queries.filter_col,
    ).select(match_len=pw.apply_with_type(len, int, pw.this._pw_index_reply))

    class ExpectedSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        match_len: int

    expected = pw.debug.table_from_markdown(
        """
        pk_source|match_len
        1        |2
    """,
        schema=ExpectedSchema,
    ).without(pw.this.pk_source)

    assert_table_equality(result, expected)

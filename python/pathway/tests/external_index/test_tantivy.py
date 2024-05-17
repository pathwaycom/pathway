# Copyright © 2024 Pathway

import json

import pathway as pw
from pathway.engine import ExternalIndexFactory
from pathway.stdlib.utils.col import unpack_col
from pathway.tests.utils import assert_table_equality


def test_filter():
    class InputSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        filter_data: str

    class QuerySchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int
        filter_col: str

    index = pw.debug.table_from_markdown(
        """
    pk_source |data        |filter_data
    1         |example     |{"path":"foo/bar/"}
    2         |example     |{"path":"foo/foo/"}
    3         |example     |{"path":"bar/bar/"}
    4         |example     |{"path":"Eyjafjallajoekull"}
    """,
        schema=InputSchema,
        # split_on_whitespace=False
    ).with_columns(
        filter_data=pw.apply(json.loads, pw.this.filter_data),
    )

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit|filter_col
    1        |example     |4    |globmatch(`"**/foo/**"`,path)
    2        |example     |4    |globmatch(`"**/bar/**"`,path)
    3        |example     |4    |path=='Eyjafjallajoekull'
    """,
        schema=QuerySchema,
    )

    index_factory = ExternalIndexFactory.tantivy_factory(
        ram_budget=50000000, in_memory_index=True
    )

    answers = index._external_index_as_of_now(
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
        2        |2
        3        |1
    """,
        schema=ExpectedSchema,
    ).without(pw.this.pk_source)

    assert_table_equality(answers, expected)


def test_optional_filter():
    class InputSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        filter_data: str

    class QuerySchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int
        filter_col: str | None

    index = pw.debug.table_from_markdown(
        """
    pk_source |data        |filter_data
    1         |example     |{"path":"foo/bar/"}
    2         |example     |{"path":"foo/foo/"}
    3         |example     |{"path":"bar/bar/"}
    4         |example     |{"path":"Eyjafjallajoekull"}
    """,
        schema=InputSchema,
    ).with_columns(
        filter_data=pw.apply(json.loads, pw.this.filter_data),
    )

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit|filter_col
    1        |example     |4    |globmatch(`"**/foo/**"`,path)
    2        |example     |4    |globmatch(`"**/bar/**"`,path)
    3        |example     |4    |path=='Eyjafjallajoekull'
    4        |example     |4    |
    """,
        schema=QuerySchema,
    )

    index_factory = ExternalIndexFactory.tantivy_factory(
        ram_budget=50000000, in_memory_index=True
    )

    answers = index._external_index_as_of_now(
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
        2        |2
        3        |1
        4        |4
    """,
        schema=ExpectedSchema,
    ).without(pw.this.pk_source)

    assert_table_equality(answers, expected)


def test_score_simple():
    class InputSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str

    class QuerySchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int

    index = pw.debug.table_from_markdown(
        """
    pk_source   |data
    1           |badger
    2           |badger badger
    3           |badger badger badger
    4           |mashroom mashroom
    5           |snake
    """,
        schema=InputSchema,
        split_on_whitespace=False,
    )

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit
    1        |badger      |2
    2        |mashroom    |2
    3        |snake       |2
    """,
        schema=QuerySchema,
    )

    index_factory = ExternalIndexFactory.tantivy_factory(
        ram_budget=50000000, in_memory_index=True
    )

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)

    class InnerSchema(pw.Schema):
        _pw_index_reply_id: pw.Pointer
        _pw_index_reply_score: float

    flattened = answers.flatten(pw.this._pw_index_reply)
    unpacked = flattened + unpack_col(flattened._pw_index_reply, schema=InnerSchema)

    ret = (
        unpacked.asof_now_join(index, pw.left._pw_index_reply_id == pw.right.id)
        .select(
            pw.left.q_pk_source,
            i_pk_source=pw.right.pk_source,
            data=pw.right.data,
            score=pw.this._pw_index_reply_score.num.round(2),
        )
        .with_id_from(pw.this.q_pk_source, pw.this.i_pk_source)
    )

    class ExpectedSchema(pw.Schema):
        q_pk_source: int = pw.column_definition(primary_key=True)
        i_pk_source: int = pw.column_definition(primary_key=True)
        data: str
        score: float

    expected = pw.debug.table_from_markdown(
        """
        q_pk_source | i_pk_source | data                 | score
        1           | 2           | badger badger        | 0.72
        1           | 3           | badger badger badger | 0.74
        2           | 4           | mashroom mashroom    | 1.85
        3           | 5           | snake                | 1.69
        """,
        schema=ExpectedSchema,
        split_on_whitespace=False,
    )
    assert_table_equality(ret, expected)


def test_score_with_deletion():
    class InputSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str

    class QuerySchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int

    index = pw.debug.table_from_markdown(
        """
    pk_source   |data                     | __time__ | __diff__
    1           |badger                   | 1        | 1
    2           |badger badger            | 2        | 1
    3           |badger badger badger     | 3        | 1
    4           |mashroom mashroom        | 4        | 1
    5           |snake                    | 5        | 1
    3           |badger badger badger     | 6        | -1
    2           |badger badger            | 7        | -1
    5           |snake                    | 8        | -1
    4           |mashroom mashroom        | 8        | -1
    1           |badger                   | 9        | -1
    """,
        schema=InputSchema,
        split_on_whitespace=False,
    )

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit| __time__
    1        |badger      |2    | 1
    2        |badger      |2    | 2
    3        |badger      |2    | 3
    4        |badger      |2    | 4
    5        |badger      |2    | 5
    6        |badger      |2    | 6
    7        |badger      |2    | 7
    8        |badger      |2    | 8
    """,
        schema=QuerySchema,
    )

    index_factory = ExternalIndexFactory.tantivy_factory(
        ram_budget=50000000, in_memory_index=True
    )

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)

    class InnerSchema(pw.Schema):
        _pw_index_reply_id: pw.Pointer
        _pw_index_reply_score: float

    flattened = answers.flatten(pw.this._pw_index_reply)
    unpacked = flattened + unpack_col(flattened._pw_index_reply, schema=InnerSchema)

    ret = (
        unpacked.asof_now_join(index, pw.left._pw_index_reply_id == pw.right.id)
        .select(
            pw.left.q_pk_source,
            i_pk_source=pw.right.pk_source,
            data=pw.right.data,
            score=pw.this._pw_index_reply_score.num.round(2),
        )
        .with_id_from(pw.this.q_pk_source, pw.this.i_pk_source)
    )

    class ExpectedSchema(pw.Schema):
        q_pk_source: int = pw.column_definition(primary_key=True)
        i_pk_source: int = pw.column_definition(primary_key=True)
        data: str
        score: float

    expected = pw.debug.table_from_markdown(
        """
    q_pk_source | i_pk_source | data                 | score  |  __time__
    1           | 1           | badger               | 0.29   | 1
    2           | 1           | badger               | 0.21   | 2
    2           | 2           | badger badger        | 0.23   | 2
    3           | 2           | badger badger        | 0.18   | 3
    3           | 3           | badger badger badger | 0.19   | 3
    4           | 2           | badger badger        | 0.49   | 4
    4           | 3           | badger badger badger | 0.51   | 4
    5           | 2           | badger badger        | 0.72   | 5
    5           | 3           | badger badger badger | 0.74   | 5
    6           | 1           | badger               | 0.8    | 6
    6           | 2           | badger badger        | 0.87   | 6
    7           | 1           | badger               | 1.09   | 7
    8           | 1           | badger               | 0.29   | 8
    """,
        schema=ExpectedSchema,
        split_on_whitespace=False,
    )
    assert_table_equality(ret, expected)

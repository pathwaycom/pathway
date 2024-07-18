import json

import pathway as pw
from pathway.engine import ExternalIndexFactory, USearchMetricKind
from pathway.stdlib.utils.col import unpack_col
from pathway.tests.utils import assert_table_equality


class InnerSchema(pw.Schema):
    matched_item_id: pw.Pointer
    matched_item_score: float


class InputSchema(pw.Schema):
    pk_source: int = pw.column_definition(primary_key=True)
    data: str


class QuerySchema(pw.Schema):
    pk_source: int = pw.column_definition(primary_key=True)
    data: str
    limit: int


def make_list(vector_as_str: str) -> list[float]:
    return [float(x) for x in vector_as_str.split(",")]


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
    pk_source |data         |filter_data
    1         | 0.1,0.1,0.1 |{"path":"foo/bar/"}
    2         | 0.2,0.1,0.1 |{"path":"foo/foo/"}
    3         | 0.3,0.1,0.1 |{"path":"bar/bar/"}
    4         | 0.4,0.1,0.1 |{"path":"Eyjafjallajoekull"}
    """,
        schema=InputSchema,
    ).with_columns(
        data=pw.apply(make_list, pw.this.data),
        filter_data=pw.apply(json.loads, pw.this.filter_data),
    )

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit|filter_col
    1        |0.15,0.1,0.1|4    |globmatch(`"**/foo/**"`,path)
    2        |0.15,0.1,0.1|4    |globmatch(`"**/bar/**"`,path)
    3        |0.15,0.1,0.1|4    |path=='Eyjafjallajoekull'
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(
        dimensions=3,
        reserved_space=10,
        metric=USearchMetricKind.L2SQ,
        connectivity=0,
        expansion_add=0,
        expansion_search=0,
    )
    rust_answers = index._external_index_as_of_now(
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

    assert_table_equality(rust_answers, expected)


def test_auto_resize():
    index = pw.debug.table_from_markdown(
        """
    pk_source |data
    1         | 0.1,0.1,0.1
    2         | 0.2,0.1,0.1
    3         | 0.3,0.1,0.1
    4         | 0.4,0.1,0.1
    5         | 0.5,0.1,0.1
    6         | 0.6,0.1,0.1
    7         | 0.7,0.1,0.1
    8         | 0.8,0.1,0.1
    9         | 0.9,0.1,0.1
    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit
    1        |0.15,0.1,0.1|4
    2        |0.25,0.1,0.1|4
    3        |0.35,0.1,0.1|4
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(
        dimensions=3,
        reserved_space=2,
        metric=USearchMetricKind.L2SQ,
        connectivity=0,
        expansion_add=0,
        expansion_search=0,
    )
    answers = index._external_index_as_of_now(
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
        1        |4
        2        |4
        3        |4
        """,
        schema=ExpectedSchema,
    ).without(pw.this.pk_source)

    assert_table_equality(answers, expected)


def test_distance_simple():

    index = pw.debug.table_from_markdown(
        """
    pk_source |data
    1         | 0.1,0.1,0.1
    2         | 0.2,0.1,0.1
    3         | 0.3,0.1,0.1
    4         | 0.4,0.1,0.1
    5         | 0.5,0.1,0.1
    6         | 0.6,0.1,0.1
    7         | 0.7,0.1,0.1
    8         | 0.8,0.1,0.1
    9         | 0.9,0.1,0.1
    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit
    1        |0.05,0.1,0.1|1
    2        |0.05,0.1,0.1|2
    3        |0.05,0.1,0.1|3
    4        |0.05,0.1,0.1|4
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(
        dimensions=3,
        reserved_space=10,
        metric=USearchMetricKind.L2SQ,
        connectivity=0,
        expansion_add=0,
        expansion_search=0,
    )

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)

    flattened = answers.flatten(pw.this._pw_index_reply)
    unpacked = flattened + unpack_col(flattened._pw_index_reply, schema=InnerSchema)

    ret = (
        unpacked.join(index, pw.left.matched_item_id == index.id)
        .select(pw.left.q_pk_source, i_pk_source=pw.right.pk_source, data=pw.right.data)
        .with_id_from(pw.this.q_pk_source, pw.this.i_pk_source)
    )

    class ExpectedSchema(pw.Schema):
        q_pk_source: int = pw.column_definition(primary_key=True)
        i_pk_source: int = pw.column_definition(primary_key=True)
        data: str

    expected = pw.debug.table_from_markdown(
        """
        q_pk_source | i_pk_source | data
        1           | 1           | 0.1,0.1,0.1
        2           | 1           | 0.1,0.1,0.1
        2           | 2           | 0.2,0.1,0.1
        3           | 1           | 0.1,0.1,0.1
        3           | 2           | 0.2,0.1,0.1
        3           | 3           | 0.3,0.1,0.1
        4           | 1           | 0.1,0.1,0.1
        4           | 2           | 0.2,0.1,0.1
        4           | 3           | 0.3,0.1,0.1
        4           | 4           | 0.4,0.1,0.1
    """,
        schema=ExpectedSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))
    assert_table_equality(ret, expected)


def test_with_distance_simple():

    index = pw.debug.table_from_markdown(
        """
    pk_source |data
    1         | 1,0.1,0.1
    2         | 2,0.1,0.1
    3         | 3,0.1,0.1
    4         | 4,0.1,0.1
    5         | 5,0.1,0.1
    6         | 6,0.1,0.1
    7         | 7,0.1,0.1
    8         | 8,0.1,0.1
    9         | 9,0.1,0.1
    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit
    1        |0.5,0.1,0.1|1
    2        |0.5,0.1,0.1|2
    3        |0.5,0.1,0.1|3
    4        |0.5,0.1,0.1|4
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(
        dimensions=3,
        reserved_space=10,
        metric=USearchMetricKind.L2SQ,
        connectivity=0,
        expansion_add=0,
        expansion_search=0,
    )
    raw_ret = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)

    flattened_ret = raw_ret.flatten(pw.this._pw_index_reply)
    unpacked_ret = flattened_ret + unpack_col(
        flattened_ret._pw_index_reply, schema=InnerSchema
    )
    ret = (
        unpacked_ret.join(index, pw.left.matched_item_id == index.id)
        .select(
            pw.left.q_pk_source,
            i_pk_source=pw.right.pk_source,
            distance=-pw.left.matched_item_score,
        )
        .with_id_from(pw.this.q_pk_source, pw.this.i_pk_source)
        .with_columns(distance=pw.this.distance.num.round(2))
    )

    class ExpectedSchema(pw.Schema):
        q_pk_source: int = pw.column_definition(primary_key=True)
        i_pk_source: int = pw.column_definition(primary_key=True)
        distance: float

    expected = pw.debug.table_from_markdown(
        """
        q_pk_source | i_pk_source | distance
        1           | 1           | 0.25
        2           | 1           | 0.25
        2           | 2           | 2.25
        3           | 1           | 0.25
        3           | 2           | 2.25
        3           | 3           | 6.25
        4           | 1           | 0.25
        4           | 2           | 2.25
        4           | 3           | 6.25
        4           | 4           | 12.25
    """,
        schema=ExpectedSchema,
    )
    assert_table_equality(ret, expected)


def test_distance_with_deletion():

    index = pw.debug.table_from_markdown(
        """
    pk_source | data        | __time__ | __diff__
    1         | 0.6,0.1,0.1 | 1        | 1
    2         | 0.5,0.1,0.1 | 2        | 1
    3         | 0.4,0.1,0.1 | 3        | 1
    4         | 0.3,0.1,0.1 | 4        | 1
    5         | 0.2,0.1,0.1 | 5        | 1
    6         | 0.1,0.1,0.1 | 6        | 1
    6         | 0.1,0.1,0.1 | 7        | -1
    5         | 0.2,0.1,0.1 | 8        | -1
    4         | 0.3,0.1,0.1 | 9        | -1
    3         | 0.4,0.1,0.1 | 10       | -1
    2         | 0.5,0.1,0.1 | 11       | -1
    1         | 0.6,0.1,0.1 | 12       | -1

    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))
    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit | __time__ | __diff__
    0        |0.05,0.1,0.1|3     | 0        | 1
    1        |0.05,0.1,0.1|3     | 1        | 1
    2        |0.05,0.1,0.1|3     | 2        | 1
    3        |0.05,0.1,0.1|3     | 3        | 1
    4        |0.05,0.1,0.1|3     | 4        | 1
    5        |0.05,0.1,0.1|3     | 5        | 1
    6        |0.05,0.1,0.1|3     | 6        | 1
    7        |0.05,0.1,0.1|3     | 7        | 1
    8        |0.05,0.1,0.1|3     | 8        | 1
    9        |0.05,0.1,0.1|3     | 9        | 1
    10       |0.05,0.1,0.1|3     | 10       | 1
    11       |0.05,0.1,0.1|3     | 11       | 1
    12       |0.05,0.1,0.1|3     | 12       | 1
    13       |0.05,0.1,0.1|3     | 13       | 1
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(
        dimensions=3,
        reserved_space=10,
        metric=USearchMetricKind.L2SQ,
        connectivity=0,
        expansion_add=0,
        expansion_search=0,
    )

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)

    flattened = answers.flatten(pw.this._pw_index_reply)
    unpacked = flattened + unpack_col(flattened._pw_index_reply, schema=InnerSchema)

    ret = (
        unpacked.asof_now_join(index, pw.left.matched_item_id == index.id)
        .select(pw.left.q_pk_source, i_pk_source=pw.right.pk_source, data=pw.right.data)
        .with_id_from(pw.this.q_pk_source, pw.this.i_pk_source)
    )

    class ExpectedSchema(pw.Schema):
        q_pk_source: int = pw.column_definition(primary_key=True)
        i_pk_source: int = pw.column_definition(primary_key=True)
        data: str

    expected = pw.debug.table_from_markdown(
        """
    q_pk_source | i_pk_source | data
    1           | 1           |0.6,0.1,0.1
    2           | 1           |0.6,0.1,0.1
    2           | 2           |0.5,0.1,0.1
    3           | 1           |0.6,0.1,0.1
    3           | 2           |0.5,0.1,0.1
    3           | 3           |0.4,0.1,0.1
    4           | 2           |0.5,0.1,0.1
    4           | 3           |0.4,0.1,0.1
    4           | 4           |0.3,0.1,0.1
    5           | 3           |0.4,0.1,0.1
    5           | 4           |0.3,0.1,0.1
    5           | 5           |0.2,0.1,0.1
    6           | 4           |0.3,0.1,0.1
    6           | 5           |0.2,0.1,0.1
    6           | 6           |0.1,0.1,0.1
    7           | 3           |0.4,0.1,0.1
    7           | 4           |0.3,0.1,0.1
    7           | 5           |0.2,0.1,0.1
    8           | 2           |0.5,0.1,0.1
    8           | 3           |0.4,0.1,0.1
    8           | 4           |0.3,0.1,0.1
    9           | 1           |0.6,0.1,0.1
    9           | 2           |0.5,0.1,0.1
    9           | 3           |0.4,0.1,0.1
    10          | 1           |0.6,0.1,0.1
    10          | 2           |0.5,0.1,0.1
    11          | 1           |0.6,0.1,0.1
    """,
        schema=ExpectedSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))
    assert_table_equality(ret, expected)


def test_cosine_distance():

    index = pw.debug.table_from_markdown(
        """
    pk_source |data
    1         | 1,0.1,0.1
    2         | 2,0.1,0.1
    3         | 3,0.1,0.1
    4         | 4,0.1,0.1
    5         | 5,0.1,0.1
    6         | 6,0.1,0.1
    7         | 7,0.1,0.1
    8         | 8,0.1,0.1
    9         | 9,0.1,0.1
    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data       |limit
    1        |0.5,0.1,0.1|1
    2        |0.5,0.1,0.1|2
    3        |0.5,0.1,0.1|3
    4        |0.5,0.1,0.1|4
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(
        dimensions=3,
        reserved_space=10,
        metric=USearchMetricKind.COS,
        connectivity=0,
        expansion_add=0,
        expansion_search=0,
    )

    raw_ret = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)

    flattened_ret = raw_ret.flatten(pw.this._pw_index_reply)

    unpacked_ret = flattened_ret + unpack_col(
        flattened_ret._pw_index_reply, schema=InnerSchema
    )
    ret = (
        unpacked_ret.join(index, pw.left.matched_item_id == index.id)
        .select(
            pw.left.q_pk_source,
            i_pk_source=pw.right.pk_source,
            distance=-pw.left.matched_item_score,
        )
        .with_id_from(pw.this.q_pk_source, pw.this.i_pk_source)
        .with_columns(distance=pw.this.distance.num.round(2))
    )

    class ExpectedSchema(pw.Schema):
        q_pk_source: int = pw.column_definition(primary_key=True)
        i_pk_source: int = pw.column_definition(primary_key=True)
        distance: float

    expected = pw.debug.table_from_markdown(
        """
        q_pk_source | i_pk_source | distance
        1           | 1           | 0.01
        2           | 1           | 0.01
        2           | 2           | 0.02
        3           | 1           | 0.01
        3           | 2           | 0.02
        3           | 3           | 0.03
        4           | 1           | 0.01
        4           | 2           | 0.02
        4           | 3           | 0.03
        4           | 4           | 0.03
    """,
        schema=ExpectedSchema,
    )

    assert_table_equality(ret, expected)

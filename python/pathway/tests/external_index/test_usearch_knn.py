import json

import pathway as pw
from pathway.engine import ExternalIndexFactory
from pathway.tests.utils import assert_table_equality


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

    # whitespaces in json string seems to be breaking table_from_markdown
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

    index_factory = ExternalIndexFactory.usearch_knn_factory(3, 10)

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
        index_filter_data_column=index.filter_data,
        query_filter_column=queries.filter_col,
    ).select(match_len=pw.apply_with_type(len, int, pw.this.matched_items))

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


def test_auto_resize():
    class InputSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str

    class QuerySchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int

    # whitespaces in json string seems to be breaking table_from_markdown
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

    index_factory = ExternalIndexFactory.usearch_knn_factory(3, 2)

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).select(match_len=pw.apply_with_type(len, int, pw.this.matched_items))

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
    class InputSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str

    class QuerySchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int

    # whitespaces in json string seems to be breaking table_from_markdown
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
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(3, 2)

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)

    ret = (
        answers.flatten(pw.this.matched_items, pw.this.q_pk_source)
        .asof_now_join(index, pw.left.matched_items == index.id)
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


def test_distance_with_deletion():
    class InputSchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str

    class QuerySchema(pw.Schema):
        pk_source: int = pw.column_definition(primary_key=True)
        data: str
        limit: int

    # whitespaces in json string seems to be breaking table_from_markdown
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

    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))
    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit | __time__ | __diff__
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
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.usearch_knn_factory(3, 2)

    answers = index._external_index_as_of_now(
        queries,
        index_column=index.data,
        query_column=queries.data,
        index_factory=index_factory,
        query_responses_limit_column=queries.limit,
    ).with_columns(q_pk_source=queries.pk_source)
    ret = (
        answers.flatten(pw.this.matched_items, pw.this.q_pk_source)
        .asof_now_join(index, pw.left.matched_items == index.id)
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
    10          | 3           |0.4,0.1,0.1
    """,
        schema=ExpectedSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))
    assert_table_equality(ret, expected)

# Copyright © 2024 Pathway

import pytest

import pathway as pw
from pathway.engine import BruteForceKnnMetricKind, ExternalIndexFactory
from pathway.stdlib.utils.col import unpack_col
from pathway.tests.utils import assert_table_equality


def make_list(vector_as_str: str) -> list[float]:
    return [float(x) for x in vector_as_str.split(",")]


class InputSchema(pw.Schema):
    pk_source: int = pw.column_definition(primary_key=True)
    data: str


class QuerySchema(pw.Schema):
    pk_source: int = pw.column_definition(primary_key=True)
    data: str
    limit: int


class InnerSchema(pw.Schema):
    matched_item_id: pw.Pointer
    matched_item_score: float


class ExpectedSchema(pw.Schema):
    q_pk_source: int = pw.column_definition(primary_key=True)
    i_pk_source: int = pw.column_definition(primary_key=True)
    distance: float


def get_ret(queries, index, index_factory):
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
    return ret


@pytest.mark.parametrize(
    ("res_space", "aux_space"),
    [
        (1, 1000),
        (1000, 1),
    ],
    ids=["test resize", "test slice queries"],
)
def test_space_shenanigans(res_space, aux_space):
    index = pw.debug.table_from_markdown(
        """
    pk_source |data         | __time__
    1         | 1,0.1,0.1   | 2
    2         | 2,0.1,0.1   | 2
    3         | 3,0.1,0.1   | 2
    4         | 4,0.1,0.1   | 1
    5         | 5,0.1,0.1   | 1
    6         | 6,0.1,0.1   | 1
    7         | 7,0.1,0.1   | 1
    8         | 8,0.1,0.1   | 1
    9         | 9,0.1,0.1   | 1
    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit | __time__
    1        |0.5,0.1,0.1 |1     | 3
    2        |0.5,0.1,0.1 |2     | 3
    3        |0.5,0.1,0.1 |3     | 3
    4        |0.5,0.1,0.1 |4     | 3
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.brute_force_knn_factory(
        dimensions=3,
        reserved_space=res_space,
        auxiliary_space=aux_space,
        metric=BruteForceKnnMetricKind.COS,
    )

    ret = get_ret(queries, index, index_factory)

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


def test_resize_after_delete():

    index = pw.debug.table_from_markdown(
        """
    pk_source |data           | __time__ | __diff__
    4         | 4,0.1,0.1     | 1        | 1
    5         | 5,0.1,0.1     | 1        | 1
    6         | 6,0.1,0.1     | 1        | 1
    7         | 7,0.1,0.1     | 1        | 1
    8         | 8,0.1,0.1     | 1        | 1
    9         | 9,0.1,0.1     | 1        | 1
    10        | 0.6,0.1,0.1   | 2        | 1
    11        | 0.6,0.1,0.1   | 2        | 1
    12        | 0.6,0.1,0.1   | 2        | 1
    10        | 0.6,0.1,0.1   | 3        | -1
    11        | 0.6,0.1,0.1   | 3        | -1
    12        | 0.6,0.1,0.1   | 3        | -1
    1         | 1,0.1,0.1     | 4        | 1
    2         | 2,0.1,0.1     | 4        | 1
    3         | 3,0.1,0.1     | 4        | 1


    """,
        schema=InputSchema,
    ).with_columns(data=pw.apply(make_list, pw.this.data))

    queries = pw.debug.table_from_markdown(
        """
    pk_source|data        |limit | __time__
    1        |0.5,0.1,0.1 |1     | 5
    2        |0.5,0.1,0.1 |2     | 5
    3        |0.5,0.1,0.1 |3     | 5
    4        |0.5,0.1,0.1 |4     | 5
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.brute_force_knn_factory(
        dimensions=3,
        reserved_space=1,
        auxiliary_space=1000,
        metric=BruteForceKnnMetricKind.COS,
    )

    ret = get_ret(queries, index, index_factory)

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
    pk_source|data        |limit
    1        |0.5,0.1,0.1|1
    2        |0.5,0.1,0.1|2
    3        |0.5,0.1,0.1|3
    4        |0.5,0.1,0.1|4
    """,
        schema=QuerySchema,
    ).with_columns(data=pw.apply_with_type(make_list, list[float], pw.this.data))

    index_factory = ExternalIndexFactory.brute_force_knn_factory(
        dimensions=3,
        reserved_space=10,
        auxiliary_space=1000,
        metric=BruteForceKnnMetricKind.COS,
    )

    ret = get_ret(queries, index, index_factory)

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


def test_euclidean_sq_distance():

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

    index_factory = ExternalIndexFactory.brute_force_knn_factory(
        dimensions=3,
        reserved_space=10,
        auxiliary_space=1000,
        metric=BruteForceKnnMetricKind.L2SQ,
    )

    ret = get_ret(queries, index, index_factory)

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

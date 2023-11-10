import pytest

import pathway as pw
from pathway.tests.utils import (
    T,
    assert_table_equality_wo_index,
    generate_custom_stream_with_deletions,
)


class ValueInstanceSchema(pw.Schema):
    value: int
    instance: int
    is_query: bool


def stream_data() -> tuple[pw.Table, pw.Table]:
    """Returns (points, queries)."""
    data = [
        (1, 1, True, +1, 1),
        (4, 1, False, +1, 2),
        (2, 1, True, +1, 3),
        (3, 1, True, +1, 4),
        (4, 1, False, -1, 2),
        (5, 1, False, +1, 5),
        (4, 1, True, +1, 6),
        (2, 2, False, +1, 7),
        (5, 2, True, +1, 8),
        (6, 1, True, +1, 9),
        (7, 2, True, +1, 10),
        (2, 2, False, -1, 7),
        (3, 2, False, +1, 11),
        (8, 1, True, +1, 12),
        (9, 2, True, +1, 13),
        (5, 1, False, -1, 5),
        (9, 1, False, +1, 14),
    ]
    value_functions = {
        "value": lambda i: data[i][0],
        "instance": lambda i: data[i][1],
        "is_query": lambda i: data[i][2],
        "diff": lambda i: data[i][3],
        "id": lambda i: data[i][4],
    }

    table = generate_custom_stream_with_deletions(
        value_functions,
        schema=ValueInstanceSchema,
        nb_rows=17,
        autocommit_duration_ms=20,
        input_rate=10,
    )
    return (
        table.filter(~pw.this.is_query).without(pw.this.is_query),
        table.filter(pw.this.is_query).without(pw.this.is_query),
    )


def test_update_old():
    data, queries = stream_data()
    result = queries.join(data, pw.left.instance == pw.right.instance).select(
        query=pw.left.value, ans=pw.right.value
    )
    expected = T(
        """
        query | ans
          1   |  9
          2   |  9
          3   |  9
          4   |  9
          5   |  3
          6   |  9
          7   |  3
          8   |  9
          9   |  3
        """
    )
    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize("set_id", [True, False])
def test_asof_now_inner(set_id: bool):
    if set_id:
        id = pw.left.id
    else:
        id = None
    data, queries = stream_data()
    result = queries.asof_now_join(
        data, pw.left.instance == pw.right.instance, id=id
    ).select(query=pw.left.value, ans=pw.right.value)
    expected = T(
        """
        query | ans
          2   |  4
          3   |  4
          4   |  5
          5   |  2
          6   |  5
          7   |  2
          8   |  5
          9   |  3
        """
    )
    if set_id:
        assert result._universe.is_subset_of(queries._universe)
    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize("set_id", [True, False])
def test_asof_now_left(set_id: bool):
    if set_id:
        id = pw.left.id
    else:
        id = None
    data, queries = stream_data()
    result = queries.asof_now_join_left(
        data, pw.left.instance == pw.right.instance, id=id
    ).select(query=pw.left.value, ans=pw.right.value)
    expected = T(
        """
        query | ans
          1   |
          2   |  4
          3   |  4
          4   |  5
          5   |  2
          6   |  5
          7   |  2
          8   |  5
          9   |  3
        """
    )
    if set_id:
        assert result._universe == queries._universe
    assert_table_equality_wo_index(result, expected)

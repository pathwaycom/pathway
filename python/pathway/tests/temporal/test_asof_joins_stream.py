# Copyright © 2023 Pathway

from __future__ import annotations

import pytest

import pathway as pw
from pathway.stdlib.temporal.temporal_behavior import common_behavior
from pathway.tests.utils import T, assert_stream_equality_wo_index


def get_tables() -> tuple[pw.Table, pw.Table]:
    queries = T(
        """
    a | t | __time__
    1 | 2 |    2
    2 | 3 |    2
    3 | 3 |    4
    4 | 5 |    4
    6 | 2 |    6
    7 | 6 |    8
    """
    )

    data = T(
        """
    b | t | __time__
    1 | 1 |    4
    2 | 4 |    6
    3 | 2 |    8
    """
    )

    return (queries, data)


def test_without_behavior():
    queries, data = get_tables()
    result = queries.asof_join_left(data, pw.left.t, pw.right.t).select(
        a=pw.left.a, tl=pw.left.t, b=pw.right.b, tr=pw.right.t
    )

    expected = T(
        """
      | a | tl | b | tr | __time__ | __diff__
    1 | 1 |  2 |   |    |    2     |    1
    2 | 2 |  3 |   |    |    2     |    1
    1 | 1 |  2 |   |    |    4     |   -1
    2 | 2 |  3 |   |    |    4     |   -1
    1 | 1 |  2 | 1 |  1 |    4     |    1
    2 | 2 |  3 | 1 |  1 |    4     |    1
    3 | 3 |  3 | 1 |  1 |    4     |    1
    4 | 4 |  5 | 1 |  1 |    4     |    1
    5 | 6 |  2 | 1 |  1 |    6     |    1
    4 | 4 |  5 | 1 |  1 |    6     |   -1
    4 | 4 |  5 | 2 |  4 |    6     |    1
    6 | 7 |  6 | 2 |  4 |    8     |    1
    1 | 1 |  2 | 1 |  1 |    8     |   -1
    2 | 2 |  3 | 1 |  1 |    8     |   -1
    3 | 3 |  3 | 1 |  1 |    8     |   -1
    5 | 6 |  2 | 1 |  1 |    8     |   -1
    1 | 1 |  2 | 3 |  2 |    8     |    1
    2 | 2 |  3 | 3 |  2 |    8     |    1
    3 | 3 |  3 | 3 |  2 |    8     |    1
    5 | 6 |  2 | 3 |  2 |    8     |    1
    """
    )

    assert_stream_equality_wo_index(result, expected)


@pytest.mark.parametrize("keep_results", [True, False])
def test_cutoff(keep_results: bool):
    queries, data = get_tables()
    behavior = common_behavior(cutoff=2, keep_results=keep_results)
    result = queries.asof_join_left(
        data, pw.left.t, pw.right.t, behavior=behavior
    ).select(a=pw.left.a, tl=pw.left.t, b=pw.right.b, tr=pw.right.t)

    if keep_results:
        expected = T(
            """
          | a | tl | b | tr | __time__ | __diff__
        1 | 1 |  2 |   |    |    2     |    1
        2 | 2 |  3 |   |    |    2     |    1
        1 | 1 |  2 |   |    |    4     |   -1
        2 | 2 |  3 |   |    |    4     |   -1
        1 | 1 |  2 | 1 |  1 |    4     |    1
        2 | 2 |  3 | 1 |  1 |    4     |    1
        3 | 3 |  3 | 1 |  1 |    4     |    1
        4 | 4 |  5 | 1 |  1 |    4     |    1
        4 | 4 |  5 | 1 |  1 |    6     |   -1
        4 | 4 |  5 | 2 |  4 |    6     |    1
        6 | 7 |  6 | 2 |  4 |    8     |    1
        """
        )
    else:
        expected = T(
            """
          | a | tl | b | tr | __time__ | __diff__
        1 | 1 |  2 |   |    |    2     |    1
        2 | 2 |  3 |   |    |    2     |    1
        1 | 1 |  2 |   |    |    4     |   -1
        2 | 2 |  3 |   |    |    4     |   -1
        1 | 1 |  2 | 1 |  1 |    4     |    1
        2 | 2 |  3 | 1 |  1 |    4     |    1
        3 | 3 |  3 | 1 |  1 |    4     |    1
        4 | 4 |  5 | 1 |  1 |    4     |    1
        4 | 4 |  5 | 1 |  1 |    6     |   -1
        4 | 4 |  5 | 2 |  4 |    6     |    1
        1 | 1 |  2 | 1 |  1 |    8     |   -1
        2 | 2 |  3 | 1 |  1 |    8     |   -1
        3 | 3 |  3 | 1 |  1 |    8     |   -1
        6 | 7 |  6 | 2 |  4 |    8     |    1
        """
        )

    assert_stream_equality_wo_index(result, expected)


def test_delay():
    queries, data = get_tables()
    behavior = common_behavior(delay=2)
    result = queries.asof_join_left(
        data, pw.left.t, pw.right.t, behavior=behavior
    ).select(a=pw.left.a, tl=pw.left.t, b=pw.right.b, tr=pw.right.t)

    expected = T(
        """
      | a | tl | b | tr | __time__ | __diff__
    1 | 1 |  2 |   |    |    4     |    1
    2 | 2 |  3 |   |    |    4     |    1
    3 | 3 |  3 |   |    |    4     |    1
    1 | 1 |  2 |   |    |    6     |   -1
    2 | 2 |  3 |   |    |    6     |   -1
    3 | 3 |  3 |   |    |    6     |   -1
    1 | 1 |  2 | 1 |  1 |    6     |    1
    2 | 2 |  3 | 1 |  1 |    6     |    1
    3 | 3 |  3 | 1 |  1 |    6     |    1
    5 | 6 |  2 | 1 |  1 |    6     |    1
    1 | 1 |  2 | 1 |  1 |    8     |   -1
    2 | 2 |  3 | 1 |  1 |    8     |   -1
    3 | 3 |  3 | 1 |  1 |    8     |   -1
    5 | 6 |  2 | 1 |  1 |    8     |   -1
    1 | 1 |  2 | 3 |  2 |    8     |    1
    2 | 2 |  3 | 3 |  2 |    8     |    1
    3 | 3 |  3 | 3 |  2 |    8     |    1
    5 | 6 |  2 | 3 |  2 |    8     |    1
    6 | 4 | 5  | 2 |  4 | 18446744073709551614 | 1
    7 | 7 | 6  | 2 |  4 | 18446744073709551614 | 1
    """
    )

    assert_stream_equality_wo_index(result, expected)

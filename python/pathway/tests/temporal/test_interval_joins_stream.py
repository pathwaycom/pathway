# Copyright © 2024 Pathway

import pytest

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index


class TimeInputSchema(pw.Schema):
    t: int


@pytest.mark.parametrize("keep_results", [True, False])
@pytest.mark.parametrize(
    "interval", [pw.temporal.interval(0, 0), pw.temporal.interval(-0.1, 0.1)]
)
def test_forgetting(keep_results: bool, interval: pw.temporal.Interval):
    t1 = pw.debug.table_from_markdown(
        """
        t | __time__
        0 |     1
        1 |     2
        2 |     3
        3 |     4
        4 |     5
        0 |     6
        1 |     7
        2 |     8
        3 |     9
        4 |    10
        """
    )

    t2 = pw.debug.table_from_markdown(
        """
        t | __time__
        0 |     1
        1 |     2
        2 |     3
        3 |     4
        4 |     5
        0 |     6
        1 |     7
        2 |     8
        3 |     9
        4 |    10
        """
    )

    result = t1.interval_join(
        t2,
        t1.t,
        t2.t,
        interval,
        behavior=pw.temporal.common_behavior(0, 2, keep_results=keep_results),
    ).select(left_t=pw.left.t, right_t=pw.right.t)
    if keep_results:
        expected = T(
            """
            left_t | right_t
               0   |    0
               1   |    1
               2   |    2
               3   |    3
               3   |    3
               3   |    3
               3   |    3
               4   |    4
               4   |    4
               4   |    4
               4   |    4
            """
        )
    else:
        expected = T(
            """
            left_t | right_t
               3   |    3
               3   |    3
               3   |    3
               3   |    3
               4   |    4
               4   |    4
               4   |    4
               4   |    4
            """
        )
    assert_table_equality_wo_index(result, expected)


class TimeValueInputSchema(pw.Schema):
    t: int
    v: int


@pytest.mark.parametrize("keep_results", [True, False])
@pytest.mark.parametrize(
    "interval", [pw.temporal.interval(0, 0), pw.temporal.interval(-0.1, 0.1)]
)
def test_forgetting_with_instance(keep_results: bool, interval: pw.temporal.Interval):
    t1 = pw.debug.table_from_markdown(
        """
        t | v | __time__
        0 | 0 |     1
        0 | 1 |     1
        1 | 0 |     2
        1 | 1 |     2
        2 | 0 |     3
        2 | 1 |     3
        3 | 0 |     4
        3 | 1 |     4
        4 | 0 |     5
        4 | 1 |     5
        0 | 0 |     6
        0 | 1 |     6
        1 | 0 |     7
        1 | 1 |     7
        2 | 0 |     8
        2 | 1 |     8
        3 | 0 |     9
        3 | 1 |     9
        4 | 0 |    10
        4 | 1 |    10
        """
    )

    t2 = t1.copy()

    result = t1.interval_join(
        t2,
        t1.t,
        t2.t,
        interval,
        t1.v == t2.v,
        behavior=pw.temporal.common_behavior(0, 2, keep_results=keep_results),
    ).select(v=pw.this.v, left_t=pw.left.t, right_t=pw.right.t)
    if keep_results:
        expected = T(
            """
            v | left_t | right_t
            0 |   0    |    0
            0 |   1    |    1
            0 |   2    |    2
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            1 |   0    |    0
            1 |   1    |    1
            1 |   2    |    2
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            """
        )
    else:
        expected = T(
            """
            v | left_t | right_t
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            """
        )
    assert_table_equality_wo_index(result, expected)

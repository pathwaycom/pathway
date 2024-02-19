# Copyright © 2024 Pathway

import datetime
import re
from typing import Optional

import numpy as np
import pandas as pd
import pytest

import pathway as pw
from pathway.internals.dtype import DATE_TIME_NAIVE, DATE_TIME_UTC, NONE
from pathway.tests.utils import T, assert_table_equality, assert_table_equality_wo_index


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize("max_time_difference", [1, 2, 3])
def test_interval_join_time_only(
    join_type: pw.JoinMode, max_time_difference: int
) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -1
    1 | 2 | 0
    2 | 3 | 2
    3 | 4 | 3
    4 | 5 | 7
    5 | 6 | 13
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 2
    1 | 2 | 5
    2 | 3 | 6
    3 | 4 | 10
    4 | 5 | 15
    """
    )

    if max_time_difference == 1:
        expected = T(
            """
          | a | b
        0 | 3 | 1
        1 | 4 | 1
        2 | 5 | 3
          """
        )
        left = T(
            """
          | a | b
        3 | 1 |
        4 | 2 |
        5 | 6 |
            """
        )
        right = T(
            """
          | a | b
        6 |   | 2
        7 |   | 4
        8 |   | 5
            """
        )
    elif max_time_difference == 2:
        expected = T(
            """
          | a | b
        0 | 2 | 1
        1 | 3 | 1
        2 | 4 | 1
        3 | 4 | 2
        4 | 5 | 2
        5 | 5 | 3
        6 | 6 | 5
        """
        )
        left = T(
            """
          | a | b
        7 | 1 |
            """
        )
        right = T(
            """
          | a | b
        8 |   | 4
            """
        )
    else:
        expected = T(
            """
           | a | b
        0  | 1 | 1
        1  | 2 | 1
        2  | 3 | 1
        3  | 3 | 2
        4  | 4 | 1
        5  | 4 | 2
        6  | 4 | 3
        7  | 5 | 2
        8  | 5 | 3
        9  | 5 | 4
        10 | 6 | 4
        11 | 6 | 5
        """
        )
        left = pw.Table.empty(a=int, b=NONE)
        right = pw.Table.empty(a=NONE, b=int)

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    res = {
        pw.JoinMode.INNER: t1.interval_join_inner,
        pw.JoinMode.LEFT: t1.interval_join_left,
        pw.JoinMode.RIGHT: t1.interval_join_right,
        pw.JoinMode.OUTER: t1.interval_join_outer,
    }[join_type](
        t2, t1.t, t2.t, pw.temporal.interval(-max_time_difference, max_time_difference)
    ).select(
        t1.a, t2.b
    )
    assert_table_equality_wo_index(res, expected)
    res2 = t1.interval_join(
        t2,
        t1.t,
        t2.t,
        pw.temporal.interval(-max_time_difference, max_time_difference),
        how=join_type,
    ).select(t1.a, t2.b)
    assert_table_equality(res, res2)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
def test_interval_join_time_only_empty_interval(join_type: pw.JoinMode) -> None:
    t1 = T(
        """
    a | t
    1 | -1
    2 | 0
    3 | 2
    4 | 3
    5 | 4
    6 | 10
    """
    )

    t2 = T(
        """
    b | t
    1 | 0
    2 | 2
    3 | 3
    4 | 5
    5 | 11
    """
    )

    interval = pw.temporal.interval(0, 0)
    expected = T(
        """
    a | b
    2 | 1
    3 | 2
    4 | 3
    """
    )
    left = T(
        """
    a | b
    1 |
    5 |
    6 |
    """
    )
    right = T(
        """
    a | b
      | 4
      | 5
    """
    )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    res = t1.interval_join(
        t2,
        t1.t,
        t2.t,
        interval,
        how=join_type,
    ).select(t1.a, t2.b)

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
def test_interval_join_time_only_empty_interval_shifted(join_type: pw.JoinMode) -> None:
    t1 = T(
        """
    a | t
    1 | -1
    2 | 0
    3 | 2
    4 | 3
    5 | 4
    6 | 10
    """
    )

    t2 = T(
        """
    b | t
    1 | 0
    2 | 2
    3 | 3
    4 | 5
    5 | 11
    """
    )

    interval = pw.temporal.interval(1, 1)
    expected = T(
        """
    a | b
    1 | 1
    3 | 3
    5 | 4
    6 | 5
    """
    )
    left = T(
        """
    a | b
    2 |
    4 |
    """
    )
    right = T(
        """
    a | b
      | 2
    """
    )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    res = t1.interval_join(
        t2,
        t1.t,
        t2.t,
        interval,
        how=join_type,
    ).select(t1.a, t2.b)

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize("bounds", [(1, 0), (15, -10)])
def test_interval_join_negative_time_errors(
    join_type: pw.JoinMode, bounds: tuple[int, int]
) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -1
    1 | 2 | 0
    2 | 3 | 2
    3 | 4 | 3
    4 | 5 | 7
    5 | 6 | 13
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 2
    1 | 2 | 5
    2 | 3 | 6
    3 | 4 | 10
    4 | 5 | 15
    """
    )
    with pytest.raises(ValueError):
        {
            pw.JoinMode.INNER: t1.interval_join_inner,
            pw.JoinMode.LEFT: t1.interval_join_left,
            pw.JoinMode.RIGHT: t1.interval_join_right,
            pw.JoinMode.OUTER: t1.interval_join_outer,
        }[join_type](t2, t1.t, t2.t, pw.temporal.interval(bounds[0], bounds[1]))


@pytest.mark.parametrize(
    "bounds",
    [
        (-1, 0),
        (0, 1),
        (-2, 0),
        (0, 2),
        (-2, 1),
        (-1, 2),
        (-3, 0),
        (0, 3),
        (2, 3),
        (-3, -2),
    ],
)
def test_interval_join_non_symmetric(bounds: tuple[int, int]) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -1
    1 | 2 | 0
    2 | 3 | 2
    3 | 4 | 3
    4 | 5 | 7
    5 | 6 | 13
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 2
    1 | 2 | 5
    2 | 3 | 6
    3 | 4 | 10
    4 | 5 | 15
    """
    )

    expected = T(
        """
       | a | b | left_t | right_t
    0  | 1 | 1 |  -1    |    2
    1  | 2 | 1 |   0    |    2
    2  | 3 | 1 |   2    |    2
    3  | 3 | 2 |   2    |    5
    4  | 4 | 1 |   3    |    2
    5  | 4 | 2 |   3    |    5
    6  | 4 | 3 |   3    |    6
    7  | 5 | 2 |   7    |    5
    8  | 5 | 3 |   7    |    6
    9  | 5 | 4 |   7    |   10
    10 | 6 | 4 |  13    |   10
    11 | 6 | 5 |  13    |   15
    """
    )
    expected = expected.filter(
        (expected.left_t + bounds[0] <= expected.right_t)
        & (expected.right_t <= expected.left_t + bounds[1])
    ).select(pw.this.a, pw.this.b)

    res = t1.interval_join_inner(
        t2, t1.t, t2.t, pw.temporal.interval(bounds[0], bounds[1])
    ).select(t1.a, t2.b)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize("max_time_difference", [1, 2])
def test_interval_join_sharded(
    join_type: pw.JoinMode, max_time_difference: int
) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -2
    1 | 1 | 1
    2 | 1 | 4
    3 | 1 | 7
    4 | 1 | 8
    5 | 2 | -4
    6 | 2 | -3
    7 | 2 | 1
    8 | 2 | 2
    9 | 2 | 4
   10 | 2 | 20
   11 | 3 | 1
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | -5
    1 | 1 | -4
    2 | 1 | -2
    3 | 1 | 0
    4 | 1 | 1
    5 | 1 | 7
    6 | 1 | 9
    7 | 2 | -5
    8 | 2 | -3
    9 | 2 | -1
   10 | 2 | 0
   11 | 2 | 5
   12 | 2 | 6
   13 | 2 | 7
   14 | 4 | 0
    """
    )

    if max_time_difference == 1:
        expected = T(
            """
          | a | b | left_t | right_t
        0 | 1 | 1 | -2     | -2
        1 | 1 | 1 | 1      | 0
        2 | 1 | 1 | 1      | 1
        3 | 1 | 1 | 7      | 7
        4 | 1 | 1 | 8      | 7
        5 | 1 | 1 | 8      | 9
        6 | 2 | 2 | -4     | -5
        7 | 2 | 2 | -4     | -3
        8 | 2 | 2 | -3     | -3
        9 | 2 | 2 | 1      | 0
       10 | 2 | 2 | 4      | 5
          """
        )
        left = T(
            """
          | a | b | left_t | right_t
       11 | 1 |   | 4      |
       12 | 2 |   | 2      |
       13 | 2 |   | 20     |
       14 | 3 |   | 1      |
          """
        )
        right = T(
            """
          | a | b | left_t | right_t
       15 |   | 1 |        | -5
       16 |   | 1 |        | -4
       17 |   | 2 |        | -1
       18 |   | 2 |        | 6
       19 |   | 2 |        | 7
       20 |   | 4 |        | 0
          """
        )
    else:
        expected = T(
            """
          | a | b | left_t | right_t
        0 | 1 | 1 | -2     | -4
        1 | 1 | 1 | -2     | -2
        2 | 1 | 1 | -2     | 0
        3 | 1 | 1 | 1      | 0
        4 | 1 | 1 | 1      | 1
        5 | 1 | 1 | 7      | 7
        6 | 1 | 1 | 7      | 9
        7 | 1 | 1 | 8      | 7
        8 | 1 | 1 | 8      | 9
        9 | 2 | 2 | -4     | -5
       10 | 2 | 2 | -4     | -3
       11 | 2 | 2 | -3     | -5
       12 | 2 | 2 | -3     | -3
       13 | 2 | 2 | -3     | -1
       14 | 2 | 2 | 1      | -1
       15 | 2 | 2 | 1      | 0
       16 | 2 | 2 | 2      | 0
       17 | 2 | 2 | 4      | 5
       18 | 2 | 2 | 4      | 6
        """
        )
        left = T(
            """
          | a | b | left_t | right_t
       19 | 1 |   | 4      |
       20 | 2 |   | 20     |
       21 | 3 |   | 1      |
          """
        )
        right = T(
            """
          | a | b | left_t | right_t
       22 |   | 1 |        | -5
       23 |   | 2 |        | 7
       24 |   | 4 |        | 0
          """
        )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    res = {
        pw.JoinMode.INNER: t1.interval_join_inner,
        pw.JoinMode.LEFT: t1.interval_join_left,
        pw.JoinMode.RIGHT: t1.interval_join_right,
        pw.JoinMode.OUTER: t1.interval_join_outer,
    }[join_type](
        t2,
        t1.t,
        t2.t,
        pw.temporal.interval(-max_time_difference, max_time_difference),
        t1.a == t2.b,
    ).select(
        t1.a, t2.b, left_t=t1.t, right_t=t2.t
    )
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
def test_interval_join_smart_cols(join_type: pw.JoinMode) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -2
    1 | 1 | 1
    2 | 1 | 4
    3 | 1 | 7
    4 | 1 | 8
    5 | 2 | -4
    6 | 2 | -3
    7 | 2 | 2
    8 | 2 | 4
    9 | 3 | 1
    """
    )

    t2 = T(
        """
      | a | t
    0 | 1 | -4
    1 | 1 | -2
    2 | 1 | 1
    3 | 1 | 7
    4 | 1 | 9
    5 | 2 | -3
    6 | 2 | 5
    7 | 2 | 6
    8 | 4 | 0
    """
    )

    expected = T(
        """
      | a | left_t | right_t
    0 | 1 | -2     | -2
    1 | 1 | 1      | 1
    2 | 1 | 7      | 7
    3 | 1 | 8      | 9
    4 | 2 | -4     | -3
    5 | 2 | -3     | -3
    6 | 2 | 4      | 5
        """
    )
    left = T(
        """
       | a | left_t | right_t
    7  | 1 | 4      |
    8  | 2 | 2      |
    9  | 3 | 1      |
        """
    )
    right = T(
        """
       | a | left_t | right_t
    10 | 1 |        | -4
    11 | 2 |        | 6
    12 | 4 |        | 0
        """
    )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    res = {
        pw.JoinMode.INNER: t1.interval_join_inner,
        pw.JoinMode.LEFT: t1.interval_join_left,
        pw.JoinMode.RIGHT: t1.interval_join_right,
        pw.JoinMode.OUTER: t1.interval_join_outer,
    }[join_type](
        t2, pw.left.t, pw.right.t, pw.temporal.interval(0, 1), pw.left.a == pw.right.a
    ).select(
        pw.this.a, left_t=pw.left.t, right_t=pw.right.t
    )
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize("max_time_difference", [0.1, 0.5])
def test_interval_join_float(max_time_difference: float) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -1.0
    1 | 2 | 0
    2 | 3 | 2.7
    3 | 4 | 5.0
    """
    )
    t2 = T(
        """
      | b  | t
    0 | 1  | -1.6
    1 | 2  | -1.4
    2 | 3  | -0.51
    3 | 4  | -0.5
    4 | 5  | -0.49
    5 | 6  | 2.1
    6 | 7  | 2.3
    7 | 8  | 3.4
    8 | 9  | 5.0
    9 | 10 | 5.09
   10 | 11 | 5.11
    """
    )
    if max_time_difference == 0.1:
        expected = T(
            """
         | a | b
       0 | 4 | 9
       1 | 4 | 10
        """
        )
    else:
        expected = T(
            """
         | a | b
       0 | 1 | 2
       1 | 1 | 3
       2 | 1 | 4
       3 | 2 | 4
       4 | 2 | 5
       5 | 3 | 7
       6 | 4 | 9
       7 | 4 | 10
       8 | 4 | 11
        """
        )
    res = t1.interval_join_inner(
        t2, t1.t, t2.t, pw.temporal.interval(-max_time_difference, max_time_difference)
    ).select(t1.a, t2.b)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize("max_time_difference", [1, 1.5])
def test_interval_join_int_float(max_time_difference: float) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | 3
    1 | 2 | 7
    """
    )
    t2 = T(
        """
      | b  | t
    0 | 1  | 1.6
    1 | 2  | 2.1
    2 | 3  | 4.4
    3 | 4  | 6.1
    4 | 5  | 7.8
    5 | 6  | 0.0
    6 | 7  | 9.2
    7 | 8  | 8.3
    """
    )
    if max_time_difference == 1:
        expected = T(
            """
         | a | b
       0 | 1 | 2
       1 | 2 | 4
       2 | 2 | 5
        """
        )
    else:
        expected = T(
            """
         | a | b
       0 | 1 | 1
       1 | 1 | 2
       2 | 1 | 3
       3 | 2 | 4
       4 | 2 | 5
       5 | 2 | 8
        """
        )
    res = t1.interval_join_inner(
        t2, t1.t, t2.t, pw.temporal.interval(-max_time_difference, max_time_difference)
    ).select(t1.a, t2.b)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
def test_non_overlapping_times(join_type: pw.JoinMode) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | 0
    1 | 2 | 1
    2 | 3 | 2
    3 | 4 | 3
    """
    )
    t2 = T(
        """
      | b  | t
    0 | 1  | 9
    1 | 2  | 10
    2 | 3  | 11
    """
    )
    bounds = (-1, 2)
    if join_type == pw.JoinMode.INNER:
        expected = pw.Table.empty(a=int, b=int)
        res = t1.interval_join_inner(
            t2, t1.t, t2.t, pw.temporal.interval(bounds[0], bounds[1]), t1.a == t2.b
        )
    elif join_type == pw.JoinMode.LEFT:
        expected = T(
            """
          | a | b
        0 | 1 |
        1 | 2 |
        2 | 3 |
        3 | 4 |
        """
        ).update_types(
            b=Optional[int]
        )  # FIXME
        res = t1.interval_join_left(
            t2, t1.t, t2.t, pw.temporal.interval(bounds[0], bounds[1]), t1.a == t2.b
        )
    elif join_type == pw.JoinMode.RIGHT:
        expected = T(
            """
          | a | b
        0 |   | 1
        1 |   | 2
        2 |   | 3
        """
        ).update_types(
            a=Optional[int]
        )  # FIXME
        res = t1.interval_join_right(
            t2, t1.t, t2.t, pw.temporal.interval(bounds[0], bounds[1]), t1.a == t2.b
        )
    else:
        expected = T(
            """
          | a | b
        0 | 1 |
        1 | 2 |
        2 | 3 |
        3 | 4 |
        4 |   | 1
        5 |   | 2
        6 |   | 3
        """
        )
        res = t1.interval_join_outer(
            t2, t1.t, t2.t, pw.temporal.interval(bounds[0], bounds[1]), t1.a == t2.b
        )

    res = res.select(t1.a, t2.b)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
def test_interval_join_time_only_automatic(seed: int) -> None:
    n = 10
    time_min = -5
    time_max = 15
    np.random.seed(seed)

    df_a = pd.DataFrame({"a": np.random.randint(time_min, time_max, size=n)})
    t_a = pw.debug.table_from_pandas(df_a)
    df_b = pd.DataFrame({"b": np.random.randint(time_min, time_max, size=n)})
    t_b = pw.debug.table_from_pandas(df_b)

    lower_bound = np.random.randint(-10, 1)
    upper_bound = np.random.randint(1, 10)

    res = t_a.interval_join_inner(
        t_b, t_a.a, t_b.b, pw.temporal.interval(lower_bound, upper_bound)
    ).select(t_a.a, t_b.b)

    expected = (
        t_a.join(t_b)
        .filter((t_a.a + lower_bound <= t_b.b) & (t_b.b <= t_a.a + upper_bound))
        .select(t_a.a, t_b.b)
    )

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
def test_interval_join_sharded_automatic(seed: int) -> None:
    n = 20
    time_min = -5
    time_max = 15
    n_shards = 4

    np.random.seed(seed)

    df_a = pd.DataFrame(
        {
            "a": np.random.randint(time_min, time_max, size=n),
            "k1": np.random.randint(0, n_shards, size=n),
        }
    )
    t_a = pw.debug.table_from_pandas(df_a)
    df_b = pd.DataFrame(
        {
            "b": np.random.randint(time_min, time_max, size=n),
            "k2": np.random.randint(0, n_shards, size=n),
        }
    )
    t_b = pw.debug.table_from_pandas(df_b)

    lower_bound = np.random.randint(1, 5)
    upper_bound = np.random.randint(5, 10)

    res = t_a.interval_join_inner(
        t_b,
        t_a.a,
        t_b.b,
        pw.temporal.interval(lower_bound, upper_bound),
        t_a.k1 == t_b.k2,
    ).select(t_a.a, t_a.k1, t_b.b, t_b.k2)

    expected = (
        t_a.join(t_b, t_a.k1 == t_b.k2)
        .filter((t_a.a + lower_bound <= t_b.b) & (t_b.b <= t_a.a + upper_bound))
        .select(t_a.a, t_a.k1, t_b.b, t_b.k2)
    )

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
def test_interval_join_float_automatic(seed: int) -> None:
    n = 20
    time_min = -0.5
    time_max = 1.5

    np.random.seed(seed)

    df_a = pd.DataFrame({"a": np.random.rand(n) * (time_max - time_min) + time_min})
    t_a = pw.debug.table_from_pandas(df_a)
    df_b = pd.DataFrame({"b": np.random.rand(n) * (time_max - time_min) + time_min})
    t_b = pw.debug.table_from_pandas(df_b)

    lower_bound = np.random.rand() * 0.1 - 0.05
    upper_bound = np.random.rand() * 0.1 + 0.1

    res = t_a.interval_join_inner(
        t_b, pw.left.a, pw.right.b, pw.temporal.interval(lower_bound, upper_bound)
    ).select(a=pw.left.a, b=pw.right.b)

    expected = (
        t_a.join(t_b)
        .filter((t_a.a + lower_bound <= t_b.b) & (t_b.b <= t_a.a + upper_bound))
        .select(t_a.a, t_b.b)
    )

    assert_table_equality_wo_index(res, expected)


def test_interval_inner_join_expressions() -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -1
    1 | 2 | 0
    2 | 3 | 2
    3 | 4 | 3
    4 | 5 | 7
    5 | 6 | 13
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 2
    1 | 2 | 5
    2 | 3 | 6
    3 | 4 | 10
    4 | 5 | 15
    """
    )

    expected = T(
        """
      | a | b | t_diff | t_sum | sth
    0 | 3 | 1 |  0     |  4    | 0b
    1 | 4 | 1 |  1     |  5    | 3a
    2 | 5 | 3 |  1     | 13    | 2a
        """
    )

    res = t1.interval_join_inner(t2, t1.t, t2.t, pw.temporal.interval(-1, 1)).select(
        t1.a,
        t2.b,
        t_diff=t1.t - t2.t,
        t_sum=t1.t + t2.t,
        sth=pw.if_else(
            t1.a + t2.b > 4,
            pw.cast(str, t1.t // t2.b) + "a",
            pw.cast(str, t2.t // t1.a) + "b",
        ),
    )

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type", [pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER]
)
def test_interval_join_expressions(join_type: pw.JoinMode) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -1
    1 | 2 | 0
    2 | 3 | 2
    3 | 4 | 3
    4 | 5 | 7
    5 | 6 | 13
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 2
    1 | 2 | 5
    2 | 3 | 6
    3 | 4 | 10
    4 | 5 | 15
    """
    )

    expected = T(
        """
      | a | b | t_diff | t_sum | sth | only_left | only_right
    0 | 3 | 1 |  0     |  4    | 0y  |    5      |    3
    1 | 4 | 1 |  1     |  5    | 3x  |    7      |    3
    2 | 5 | 3 |  1     | 13    | 2x  |   12      |    9
        """
    )
    left = T(
        """
      | a | b | t_diff | t_sum | sth | only_left | only_right
    0 | 1 |   |        |       |     |    0      |
    1 | 2 |   |        |       |     |    2      |
    2 | 6 |   |        |       |     |   19      |
        """
    )
    right = T(
        """
      | a | b | t_diff | t_sum | sth | only_left | only_right
    0 |   | 2 |        |       |     |           |    7
    1 |   | 4 |        |       |     |           |   14
    2 |   | 5 |        |       |     |           |   20
        """
    )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    res = {
        pw.JoinMode.INNER: t1.interval_join_inner,
        pw.JoinMode.LEFT: t1.interval_join_left,
        pw.JoinMode.RIGHT: t1.interval_join_right,
        pw.JoinMode.OUTER: t1.interval_join_outer,
    }[join_type](t2, t1.t, t2.t, pw.temporal.interval(-1, 1)).select(
        t1.a,
        t2.b,
        t_diff=pw.require(t1.t - t2.t, t1.id, t2.id),
        t_sum=pw.require(t1.t + t2.t, t1.id, t2.id),
        sth=pw.require(
            pw.if_else(
                t1.a + t2.b > 4,
                pw.cast(str, t1.t // t2.b) + "x",
                pw.cast(str, t2.t // t1.a) + "y",
            ),
            t1.id,
            t2.id,
        ),
        only_left=(
            pw.require(t1.t + t1.a, t1.id)
            if join_type in (pw.JoinMode.RIGHT, pw.JoinMode.OUTER)
            else t1.t + t1.a
        ),
        only_right=(
            pw.require(t2.t + t2.b, t2.id)
            if join_type in (pw.JoinMode.LEFT, pw.JoinMode.OUTER)
            else t2.t + t2.b
        ),
    )
    res = res.update_types(
        t_diff=Optional[int],
        t_sum=Optional[int],
    )
    if join_type in (pw.JoinMode.RIGHT, pw.JoinMode.OUTER):
        res = res.update_types(only_left=Optional[int])
    if join_type in (pw.JoinMode.LEFT, pw.JoinMode.OUTER):
        res = res.update_types(only_right=Optional[int])

    assert_table_equality_wo_index(res, expected)


def test_interval_join_coalesce() -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -1
    2 | 3 | 2
    3 | 4 | 3
    4 | 5 | 7
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 2
    1 | 2 | 5
    2 | 3 | 6
    """
    )

    expected = T(
        """
      | a | b | coalesce
    0 | 3 | 1 |    3
    1 | 4 | 1 |    4
    2 | 5 | 3 |    5
    3 | 1 |   |    1
    4 |   | 2 |    2
        """
    )

    res = t1.interval_join_outer(
        t2, pw.left.t, pw.right.t, pw.temporal.interval(-1, 1)
    ).select(
        pw.left.a,
        pw.right.b,
        coalesce=pw.declare_type(int, pw.coalesce(pw.left.a, pw.right.b)),
    )

    assert_table_equality_wo_index(res, expected)


@pytest.mark.xfail(reason="Ix and temporal_joins do not mix.")
def test_interval_join_left_ix() -> None:
    t1 = pw.debug.table_from_markdown(
        """
      | t
    1 | 3
    2 | 4
    3 | 5
    4 | 11
    """
    )
    t2 = pw.debug.table_from_markdown(
        """
      | t
    1 | 0
    2 | 1
    3 | 4
    4 | 5
    """
    )
    expected = T(
        """
        left_t | right_t | other
        3      | 1       | 0
        3      | 4       | 0
        4      | 4       | 1
        4      | 5       | 1
        5      | 4       | 4
        5      | 5       | 4
        11     |         | 5
    """
    )
    join_result = t1.interval_join_left(t2, t1.t, t2.t, pw.temporal.interval(-2, 1))
    res = join_result.select(
        left_t=t1.t, right_t=t2.t, other=t2.ix(t1.id, context=pw.this).t
    )
    assert_table_equality_wo_index(
        res,
        expected,
    )


def test_interval_join_with_time_expressions() -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | 9
    2 | 3 | 12
    3 | 4 | 13
    4 | 5 | 17
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 1
    1 | 2 | 4
    2 | 3 | 5
    """
    )

    expected = T(
        """
      | a | b
    0 | 3 | 1
    1 | 4 | 1
    2 | 5 | 3
    3 | 1 |
    4 |   | 2
        """
    )

    res = t1.interval_join_outer(
        t2,
        (4 * pw.left.t - 40) // 2,
        (6 * pw.right.t + 6) // 3,
        pw.temporal.interval(-2, 2),
    ).select(
        pw.left.a,
        pw.right.b,
    )

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize("with_timezone", [True, False])
def test_with_timestamps(with_timezone: bool) -> None:
    fmt = "%Y-%m-%dT%H:%M:%S"
    if with_timezone:
        fmt += "%z"
        tz = "+02:00"
    else:
        tz = ""

    t1 = T(
        """
      | a | t
    0 | 1 | 2023-05-22T09:59:00
    1 | 2 | 2023-05-22T10:00:00
    2 | 3 | 2023-05-22T10:02:00
    3 | 4 | 2023-05-22T10:03:00
    4 | 5 | 2023-05-22T10:07:00
    5 | 6 | 2023-05-22T10:13:00
    """
    ).with_columns(t=(pw.this.t + tz).dt.strptime(fmt))

    t2 = T(
        """
      | b | t
    0 | 1 | 2023-05-22T10:02:00
    1 | 2 | 2023-05-22T10:05:00
    2 | 3 | 2023-05-22T10:06:00
    3 | 4 | 2023-05-22T10:10:00
    4 | 5 | 2023-05-22T10:15:00
    """
    ).with_columns(t=(pw.this.t + tz).dt.strptime(fmt))

    res = t1.interval_join_outer(
        t2,
        t1.t,
        t2.t,
        pw.temporal.interval(
            datetime.timedelta(minutes=-2), datetime.timedelta(minutes=1)
        ),
    ).select(pw.left.a, pw.right.b)

    expected = T(
        """
       | a | b
     1 |   | 4
     2 |   | 5
     3 | 1 |
     4 | 2 |
     5 | 3 | 1
     6 | 4 | 1
     7 | 5 | 2
     8 | 5 | 3
     9 | 6 |
    """
    )

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_mode",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize(
    "left_type,right_type,lower_bound,upper_bound",
    [
        (int, int, 1, datetime.timedelta(days=1)),
        (int, int, datetime.timedelta(days=-1), datetime.timedelta(days=1)),
        (int, int, datetime.timedelta(days=-1), 1),
        (int, DATE_TIME_NAIVE, 0, 1),
        (DATE_TIME_NAIVE, int, 0, 1),
        (float, DATE_TIME_NAIVE, 1, 0.2),
        (DATE_TIME_NAIVE, DATE_TIME_NAIVE, 1, 2),
        (DATE_TIME_NAIVE, DATE_TIME_NAIVE, datetime.timedelta(days=1), 2),
        (
            DATE_TIME_UTC,
            DATE_TIME_NAIVE,
            datetime.timedelta(days=1),
            datetime.timedelta(days=2),
        ),
        (int, int, datetime.timedelta(seconds=2), 10),
    ],
)
def test_incorrect_args(join_mode, left_type, right_type, lower_bound, upper_bound):
    t1 = pw.Table.empty(a=int, t=left_type)

    t2 = pw.Table.empty(b=int, t=right_type)

    with pytest.raises(
        TypeError,
        match=r"Arguments \(self_time_expression, other_time_expression, lower_bound, "
        + r"upper_bound\) have to be of types .* but are of types .*",
    ):
        {
            pw.JoinMode.INNER: t1.interval_join_inner,
            pw.JoinMode.LEFT: t1.interval_join_left,
            pw.JoinMode.RIGHT: t1.interval_join_right,
            pw.JoinMode.OUTER: t1.interval_join_outer,
        }[join_mode](
            t2, t1.t, t2.t, pw.temporal.interval(lower_bound, upper_bound)
        ).select(
            t1.a, t2.b
        )


def test_incorrect_args_specific():
    t1 = pw.Table.empty(a=int, t=DATE_TIME_NAIVE)

    t2 = pw.Table.empty(b=int, t=int)

    with pytest.raises(
        TypeError,
        match=re.escape(
            "Arguments (self_time_expression, other_time_expression, lower_bound, upper_bound) "
            "have to be of types (INT, INT, INT, INT) or (FLOAT, FLOAT, FLOAT, FLOAT) or "
            "(DATE_TIME_NAIVE, DATE_TIME_NAIVE, DURATION, DURATION) or "
            "(DATE_TIME_UTC, DATE_TIME_UTC, DURATION, DURATION) but are of types "
            "(DATE_TIME_NAIVE, INT, INT, INT)."
        ),
    ):
        t1.interval_join(t2, t1.t, t2.t, pw.temporal.interval(-1, 2))


def test_interval_joins_typing_on():
    left_table = pw.Table.empty(timestamp=int, col=int)
    right_table = pw.Table.empty(timestamp=int, col=str)
    with pytest.raises(expected_exception=TypeError):
        left_table.interval_join(
            right_table,
            left_table.timestamp,
            right_table.timestamp,
            pw.temporal.interval(-1, 2),
            left_table.col == right_table.col,
        )


def test_errors_on_equal_tables():
    t1 = T(
        """
      | a | t
    0 | 1 | -1
    """
    )

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Cannot join table with itself. Use <table>.copy() as one of the arguments of the join."
        ),
    ):
        t1.interval_join(t1, t1.t, t1.t, pw.temporal.interval(-2, 0))


def test_consolidate_for_cutoff():
    t = T(
        """
    a | t  | __time__ | __diff__
    1 | 2  | 2        | 1
    2 | 2  | 2        | 1
    3 | 10 | 2        | 1
    4 | 2  | 2        | 1
    5 | 2  | 4        | 1
    6 | 2  | 4        | 1
    7 | 2  | 4        | 1
    8 | 2  | 4        | 1
    9 | 2  | 4        | 1
    10| 2  | 4        | 1
    11| 2  | 4        | 1
    12| 2  | 4        | 1
    """
    )
    t = t._freeze(threshold_column=pw.this.t + 1, time_column=pw.this.t)

    assert_table_equality_wo_index(
        t,
        T(
            """
            a | t
            1 | 2
            2 | 2
            3 | 10
            4 | 2
            """
        ),
    )


def test_no_columns_added():
    t1 = T(
        """
      | a | t
    0 | 1 | 2
    """
    )
    expected = T(
        """
        a | t | b | s
        1 | 2 | 1 | 2
    """
    )
    t2 = t1.rename({"a": "b", "t": "s"})
    res = t1.interval_join(
        t2, pw.left.t, pw.right.s, interval=pw.temporal.interval(-1, 1)
    ).select(*pw.left, *pw.right)

    assert_table_equality_wo_index(res, expected)

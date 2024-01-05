# Copyright © 2024 Pathway

from __future__ import annotations

import pytest

import pathway as pw
from pathway.internals.dtype import DATE_TIME_NAIVE, DATE_TIME_UTC
from pathway.internals.join_mode import JoinMode
from pathway.tests.utils import (
    T,
    assert_table_equality_wo_index,
    assert_table_equality_wo_index_types,
)


def test_asof_left():
    t1 = T(
        """
            | K | val |  t
        1   | 0 | 1   |  1
        2   | 0 | 2   |  4
        3   | 0 | 3   |  5
        4   | 0 | 4   |  6
        5   | 0 | 5   |  7
        6   | 0 | 6   |  11
        7   | 0 | 7   |  12
        8   | 1 | 8   |  5
        9   | 1 | 9   |  7
    """
    )

    t2 = T(
        """
            | K | val | t
        21   | 1 | 7  | 2
        22   | 1 | 3  | 8
        23   | 0 | 0  | 2
        24   | 0 | 6  | 3
        25   | 0 | 2  | 7
        26   | 0 | 3  | 8
        27   | 0 | 9  | 9
        28   | 0 | 7  | 13
        29   | 0 | 4  | 14
        """
    )
    res = t1.asof_join(
        t2,
        t1.t * 2,
        t2.t * 2,
        t1.K == t2.K,
        how=pw.JoinMode.LEFT,
        defaults={t2.val: -1},
    ).select(
        pw.this.instance,
        pw.this.t,
        val_right=t2.val,
        val_left_times_2_plus_val_right=t1.val * 2 + t2.val,
    )
    assert_table_equality_wo_index(
        res,
        T(
            """
 instance | t  | val_right | val_left_times_2_plus_val_right
 0        |  2 | -1        | 1
 0        |  8 | 6         | 10
 0        | 10 | 6         | 12
 0        | 12 | 6         | 14
 0        | 14 | 2         | 12
 0        | 22 | 9         | 21
 0        | 24 | 9         | 23
 1        | 10 | 7         | 23
 1        | 14 | 7         | 25
          """
        ),
    )


def test_asof_full():
    t1 = T(
        """
            | K | val |  t
        1   | 0 | 1   |  1
        2   | 0 | 2   |  4
        3   | 0 | 3   |  5
        4   | 0 | 4   |  6
        5   | 0 | 5   |  7
        6   | 0 | 6   |  11
        7   | 0 | 7   |  12
        8   | 1 | 8   |  5
        9   | 1 | 9   |  7
    """
    )

    t2 = T(
        """
             | K | val | t
        21   | 1 | 7  | 2
        22   | 1 | 3  | 8
        23   | 0 | 0  | 2
        24   | 0 | 6  | 3
        25   | 0 | 2  | 7
        26   | 0 | 3  | 8
        27   | 0 | 9  | 9
        28   | 0 | 7  | 13
        29   | 0 | 4  | 14
        """
    )
    res = t1.asof_join(
        t2,
        t1.t,
        t2.t,
        t1.K == t2.K,
        how=pw.JoinMode.OUTER,
        defaults={t1.val: 0, t2.val: 0},
    ).select(
        pw.this.instance,
        pw.this.side,
        pw.this.t,
        val_v1=t1.val,
        val_v2=t2.val,
        sum=t1.val + t2.val,
    )

    assert_table_equality_wo_index(
        res,
        T(
            """
instance | side  | t  | val_v1 | val_v2 | sum
0        | False | 1  | 1      | 0      | 1
0        | False | 4  | 2      | 6      | 8
0        | False | 5  | 3      | 6      | 9
0        | False | 6  | 4      | 6      | 10
0        | False | 7  | 5      | 6      | 11
0        | False | 11 | 6      | 9      | 15
0        | False | 12 | 7      | 9      | 16
0        | True  | 2  | 1      | 0      | 1
0        | True  | 3  | 1      | 6      | 7
0        | True  | 7  | 5      | 2      | 7
0        | True  | 8  | 5      | 3      | 8
0        | True  | 9  | 5      | 9      | 14
0        | True  | 13 | 7      | 7      | 14
0        | True  | 14 | 7      | 4      | 11
1        | False | 5  | 8      | 7      | 15
1        | False | 7  | 9      | 7      | 16
1        | True  | 2  | 0      | 7      | 7
1        | True  | 8  | 9      | 3      | 12
"""
        ),
    )


def test_asof_left_forward():
    t1 = T(
        """
            | K | val |  t
        1   | 0 | 1   |  1
        2   | 0 | 2   |  4
        3   | 0 | 3   |  5
        4   | 0 | 4   |  6
        5   | 0 | 5   |  7
        6   | 0 | 6   |  11
        7   | 0 | 7   |  12
        8   | 1 | 8   |  5
        9   | 1 | 9   |  7
        10  | 1 | 10  |  20
    """
    )

    t2 = T(
        """
             | K | val | t
        21   | 1 | 7  | 2
        22   | 1 | 3  | 8
        23   | 0 | 0  | 2
        24   | 0 | 6  | 3
        25   | 0 | 2  | 7
        26   | 0 | 3  | 8
        27   | 0 | 9  | 9
        28   | 0 | 7  | 13
        29   | 0 | 4  | 14
        """
    )
    res = t1.asof_join(
        t2,
        t1.t * 2,
        t2.t * 2,
        t1.K == t2.K,
        how=pw.JoinMode.LEFT,
        direction=pw.temporal._asof_join.Direction.FORWARD,
        defaults={t2.val: 100},
    ).select(
        pw.this.instance,
        pw.this.t,
        val_right=t2.val,
        val_left_times_2_plus_val_right=t1.val * 2 + t2.val,
    )
    assert_table_equality_wo_index(
        res,
        T(
            """
instance | t  | val_right | val_left_times_2_plus_val_right
0        |  2 | 0         | 2
0        |  8 | 2         | 6
0        | 10 | 2         | 8
0        | 12 | 2         | 10
0        | 14 | 2         | 12
0        | 22 | 7         | 19
0        | 24 | 7         | 21
1        | 10 | 3         | 19
1        | 14 | 3         | 21
1        | 40 | 100       | 120
          """
        ),
    )


def test_asof_left_nearest():
    t1 = T(
        """
            |  t
        1   |  1
        2   |  20
        3   |  40
        4   |  60
        5   |  80
    """
    )

    t2 = T(
        """
            | t
        23  | -15
        24  | 10
        26  | 35
        27  | 45
        28  | 50
        """
    )
    res = t1.asof_join(
        t2,
        t1.t * 2,
        t2.t * 2,
        how=pw.JoinMode.LEFT,
        direction=pw.temporal._asof_join.Direction.NEAREST,
    ).select(
        pw.this.instance,
        pw.this.t,
        t_right=t2.t,
    )
    assert_table_equality_wo_index_types(
        res,
        T(
            """
 instance |   t | t_right
          |   2 | 10
          |  40 | 10
          |  80 | 45
          | 120 | 50
          | 160 | 50
          """
        ),
    )


# @pytest.mark.parametrize("join_mode", [JoinMode.LEFT, JoinMode.RIGHT, JoinMode.OUTER])
def test_multiple_keys():
    t1 = T(
        """
         | k1 | k2 |  t
       1 |  1 |  1 |  3
       2 |  1 |  1 |  5
       3 |  1 |  1 |  7
       4 |  1 |  2 |  2
       5 |  1 |  2 |  6
       6 |  2 |  1 | 10
       7 |  2 |  1 | 11
       8 |  2 |  1 | 13
       9 |  2 |  2 | -4
      10 |  2 |  2 | -1
      11 |  2 |  2 |  0
    """
    )

    t2 = T(
        """
         | k1 | k2 |  t
       1 |  1 |  1 |  4
       2 |  1 |  2 |  1
       3 |  1 |  2 |  3
       4 |  2 |  1 | 12
       5 |  2 |  2 | -3
       6 |  2 |  2 | -2
    """
    )

    expected = T(
        """
         | k1 | k2 | lt | rt
       1 |  1 |  1 |  3 |
       2 |  1 |  1 |  5 |  4
       3 |  1 |  1 |  7 |  4
       4 |  1 |  2 |  2 |  1
       5 |  1 |  2 |  6 |  3
       6 |  2 |  1 | 10 |
       7 |  2 |  1 | 11 |
       8 |  2 |  1 | 13 | 12
       9 |  2 |  2 | -4 |
      10 |  2 |  2 | -1 | -2
      11 |  2 |  2 |  0 | -2
    """
    )

    result = t1.asof_join(
        t2,
        pw.left.t,
        pw.right.t,
        pw.left.k1 == pw.right.k1,
        pw.left.k2 == pw.right.k2,
        how=JoinMode.LEFT,
    ).select(k1=pw.left.k1, k2=pw.left.k2, lt=pw.left.t, rt=pw.right.t)

    assert_table_equality_wo_index(result, expected)


def test_with_timestamps():
    fmt = "%Y-%m-%dT%H:%M:%S"
    t1 = T(
        """
         |  t
       1 |  2023-05-10T13:01:00
       2 |  2023-05-10T13:03:00
       3 |  2023-05-10T13:05:00
       4 |  2023-05-10T13:07:00
    """
    ).with_columns(t=pw.this.t.dt.strptime(fmt))

    t2 = T(
        """
         |  t
       1 |  2023-05-10T13:02:00
       2 |  2023-05-10T13:04:00
    """
    ).with_columns(t=pw.this.t.dt.strptime(fmt))

    expected = T(
        """
         |          lt          |        rt
       1 |  2023-05-10T13:01:00 |
       2 |  2023-05-10T13:03:00 | 2023-05-10T13:02:00
       3 |  2023-05-10T13:05:00 | 2023-05-10T13:04:00
       4 |  2023-05-10T13:07:00 | 2023-05-10T13:04:00
    """
    ).with_columns(
        lt=pw.this.lt.dt.strptime(fmt),
        rt=pw.require(
            pw.this.rt.dt.strptime(fmt),
            pw.this.rt,
        ),
    )

    result = t1.asof_join(t2, t1.t, t2.t, how=JoinMode.LEFT).select(lt=t1.t, rt=t2.t)
    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize(
    "left_type,right_type",
    [
        (int, DATE_TIME_UTC),
        (DATE_TIME_NAIVE, int),
        (float, DATE_TIME_NAIVE),
        (DATE_TIME_NAIVE, DATE_TIME_UTC),
    ],
)
def test_incorrect_args(left_type, right_type):
    t1 = pw.Table.empty(t=left_type)

    t2 = pw.Table.empty(t=right_type)
    with pytest.raises(
        TypeError,
        match=r"Arguments \(t_left, t_right\) have to be of types .* but are of types .*",
    ):
        t1.asof_join(
            t2,
            t1.t,
            t2.t,
            how=pw.JoinMode.LEFT,
        )


def test_more_asof_left():
    t1 = T(
        """
       | k1 |  t
     1 |  1 |  3
     2 |  1 |  5
     3 |  1 |  7
     4 |  2 |  2
     5 |  2 |  6
     6 |  3 | 10
     7 |  3 | 11
     8 |  3 | 13
     9 |  4 | -4
    10 |  4 | -1
    11 |  4 |  0
    """
    )

    t2 = T(
        """
      | k1 |  t
    1 |  1 |  4
    2 |  2 |  1
    3 |  2 |  3
    4 |  3 | 12
    5 |  4 | -3
    6 |  4 | -2
    """
    )
    t3 = t1.asof_join(t2, t1.t, t2.t, t1.k1 == t2.k1, how=JoinMode.LEFT).select(
        k1=t1.k1, lt=t1.t, rt=t2.t
    )
    assert_table_equality_wo_index(
        t3,
        T(
            """
    k1 | lt | rt
    1  | 3  |
    1  | 5  | 4
    1  | 7  | 4
    2  | 2  | 1
    2  | 6  | 3
    3  | 10 |
    3  | 11 |
    3  | 13 | 12
    4  | -4 |
    4  | -1 | -2
    4  | 0  | -2
    """
        ),
    )


def test_more_asof_right():
    t1 = T(
        """
       | k1 |  t
     1 |  1 |  3
     2 |  1 |  5
     3 |  1 |  7
     4 |  2 |  2
     5 |  2 |  6
     6 |  3 | 10
     7 |  3 | 11
     8 |  3 | 13
     9 |  4 | -4
    10 |  4 | -1
    11 |  4 |  0
    """
    )

    t2 = T(
        """
      | k1 |  t
    1 |  1 |  4
    2 |  2 |  1
    3 |  2 |  3
    4 |  3 | 12
    5 |  4 | -3
    6 |  4 | -2
    """
    )
    t3 = t1.asof_join(t2, t1.t, t2.t, t1.k1 == t2.k1, how=JoinMode.RIGHT).select(
        k1=t1.k1, lt=t1.t, rt=t2.t
    )
    assert_table_equality_wo_index(
        t3,
        T(
            """
    k1 | lt | rt
       |    | 1
    1  | 3  | 4
    2  | 2  | 3
    3  | 11 | 12
    4  | -4 | -3
    4  | -4 | -2
    """
        ),
    )


def test_more_asof_full():
    t1 = T(
        """
       | k1 |  t
     1 |  1 |  3
     2 |  1 |  5
     3 |  1 |  7
     4 |  2 |  2
     5 |  2 |  6
     6 |  3 | 10
     7 |  3 | 11
     8 |  3 | 13
     9 |  4 | -4
    10 |  4 | -1
    11 |  4 |  0
    """
    )

    t2 = T(
        """
      | k1 |  t
    1 |  1 |  4
    2 |  2 |  1
    3 |  2 |  3
    4 |  3 | 12
    5 |  4 | -3
    6 |  4 | -2
    """
    )
    t3 = t1.asof_join(t2, t1.t, t2.t, t1.k1 == t2.k1, how=JoinMode.OUTER).select(
        k1=t1.k1, lt=t1.t, rt=t2.t
    )
    assert_table_equality_wo_index(
        t3,
        T(
            """
    k1 | lt | rt
       |    | 1
    1  | 3  |
    1  | 3  | 4
    1  | 5  | 4
    1  | 7  | 4
    2  | 2  | 1
    2  | 2  | 3
    2  | 6  | 3
    3  | 10 |
    3  | 11 |
    3  | 11 | 12
    3  | 13 | 12
    4  | -4 |
    4  | -4 | -3
    4  | -4 | -2
    4  | -1 | -2
    4  | 0  | -2
    """
        ),
    )


def test_asof_joins_typing_on():
    left_table = pw.Table.empty(timestamp=int, col=int)
    right_table = pw.Table.empty(timestamp=int, col=str)
    with pytest.raises(expected_exception=TypeError):
        left_table.asof_join_outer(
            right_table,
            left_table.timestamp,
            right_table.timestamp,
            left_table.col == right_table.col,
        )


def test_asof_join_left():
    t1 = T(
        """
        val
          0
         10
         20
         29
         30
    """
    )

    t2 = T(
        """
        val
          0
         10
         20
         30
    """
    )

    expected = T(
        """
          l |  r
          0 |  0
         10 | 10
         20 | 20
         29 | 20
         30 | 30
    """
    ).update_types(r=int | None)

    table = t1.asof_join(
        t2,
        t1.val,
        t2.val,
        how=pw.JoinMode.LEFT,
        direction=pw.temporal.Direction.BACKWARD,
    ).select(l=pw.left.val, r=pw.right.val)

    assert_table_equality_wo_index(table, expected)


@pytest.mark.parametrize("mode", [pw.JoinMode.LEFT, pw.JoinMode.RIGHT])
@pytest.mark.parametrize(
    "dir",
    [
        pw.temporal.Direction.BACKWARD,
        pw.temporal.Direction.FORWARD,
        pw.temporal.Direction.NEAREST,
    ],
)
def test_asof_join_eq(mode, dir):
    t1 = T(
        """
        val
          0
         10
         20
         30
    """
    )

    t2 = T(
        """
        val
          0
         10
         20
         30
    """
    )

    col_name = "r" if mode == pw.JoinMode.LEFT else "l"
    expected = T(
        """
          l |  r
          0 |  0
         10 | 10
         20 | 20
         30 | 30
    """
    ).update_types(**{col_name: int | None})

    table = t1.asof_join(t2, t1.val, t2.val, how=mode, direction=dir).select(
        l=pw.left.val, r=pw.right.val
    )

    assert_table_equality_wo_index(table, expected)


def test_asof_join_instance():
    t1 = T(
        """
        val | i
          0 | 0
         10 | 1
         20 | 1
         25 | 1
         30 | 0
    """
    )

    t2 = T(
        """
        val | i
          0 | 1
         10 | 0
         20 | 1
         30 | 1
    """
    )

    expected = T(
        """
          l |  r
          0 |
         10 |  0
         20 | 20
         25 | 20
         30 | 10
    """
    ).update_types(r=int | None)

    table = t1.asof_join(
        t2,
        t1.val,
        t2.val,
        how=pw.JoinMode.LEFT,
        direction=pw.temporal.Direction.BACKWARD,
        left_instance=t1.i,
        right_instance=t2.i,
    ).select(l=pw.left.val, r=pw.right.val)

    assert_table_equality_wo_index(table, expected)


def test_preserves_column_names():
    table_l = T(
        """
         a | x
         0 | 1
         2 | 1
         4 | 1
         6 | 1
         8 | 1
        10 | 1
        12 | 1
    """
    )

    table_r = T(
        """
         b | y
         1 | 2
         5 | 2
        11 | 2
    """
    )

    expected = T(
        """
         a | x |  b | y
         0 | 1 |    |
         2 | 1 |  1 | 2
         4 | 1 |  1 | 2
         6 | 1 |  5 | 2
         8 | 1 |  5 | 2
        10 | 1 |  5 | 2
        12 | 1 | 11 | 2
    """
    )

    res = table_l.asof_join(table_r, table_l.a, table_r.b, how=pw.JoinMode.LEFT).select(
        **pw.left, **pw.right
    )

    assert_table_equality_wo_index(res, expected)

# Copyright © 2024 Pathway

import datetime

import pytest

import pathway as pw
from pathway.internals.dtype import DATE_TIME_NAIVE, DATE_TIME_UTC
from pathway.tests.utils import T, assert_table_equality, assert_table_equality_wo_index


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize(
    "w",
    [
        pw.temporal.tumbling(1),
        pw.temporal.tumbling(2),
        pw.temporal.sliding(1, 2),
        pw.temporal.sliding(2, 1),
    ],
)
def test_window_join_time_only(join_type: pw.JoinMode, w: pw.temporal.Window) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -2
    1 | 2 | 1
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
    3 | 4 | 7
    4 | 5 | 14
    """
    )

    if w == pw.temporal.tumbling(1):
        expected = T(
            """
          | a | b
        0 | 3 | 1
        1 | 5 | 4
          """
        )
        left = T(
            """
          | a | b
        3 | 1 |
        4 | 2 |
        5 | 4 |
        6 | 6 |
            """
        )
        right = T(
            """
          | a | b
        7 |   | 2
        8 |   | 3
        9 |   | 5
            """
        )
    elif w == pw.temporal.tumbling(2):
        expected = T(
            """
          | a | b
        0 | 3 | 1
        1 | 4 | 1
        2 | 5 | 3
        3 | 5 | 4
        """
        )
        left = T(
            """
          | a | b
        4 | 1 |
        5 | 2 |
        6 | 6 |
            """
        )
        right = T(
            """
          | a | b
        7 |   | 2
        8 |   | 5
            """
        )
    elif w == pw.temporal.sliding(1, 2):
        expected = T(
            """
           | a | b
        0  | 2 | 1
        1  | 3 | 1
        2  | 3 | 1
        3  | 4 | 1
        4  | 5 | 3
        5  | 5 | 4
        6  | 5 | 4
        7  | 6 | 5
        """
        )
        left = T(
            """
          | a | b
        6 | 1 |
        7 | 1 |
        8 | 2 |
        9 | 4 |
       10 | 6 |
            """
        )
        right = T(
            """
          | a | b
        0 |   | 2
        1 |   | 2
        2 |   | 3
        3 |   | 5
            """
        )
    elif w == pw.temporal.sliding(2, 1):
        expected = T(
            """
          | a | b
        0 | 3 | 1
          """
        )
        left = T(
            """
          | a | b
        3 | 1 |
            """
        )
        right = T(
            """
          | a | b
        9 |   | 3
       11 |   | 5
            """
        )
    else:
        raise ValueError("Inappropriate window provided")

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    res = {
        pw.JoinMode.INNER: t1.window_join_inner,
        pw.JoinMode.LEFT: t1.window_join_left,
        pw.JoinMode.RIGHT: t1.window_join_right,
        pw.JoinMode.OUTER: t1.window_join_outer,
    }[join_type](t2, t1.t, t2.t, w).select(t1.a, t2.b)

    assert_table_equality_wo_index(res, expected)

    res2 = t1.window_join(t2, t1.t, t2.t, w, how=join_type).select(t1.a, t2.b)
    assert_table_equality(res, res2)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
def test_window_join_sharded_with_smart_cols(join_type: pw.JoinMode) -> None:
    t1 = T(
        """
      | a | t  | k
    0 | 1 | -2 | 1
    1 | 2 | 1  | 1
    2 | 3 | 2  | 1
    3 | 4 | 3  | 1
    4 | 5 | 7  | 1
    5 | 6 | 13 | 1
    6 | 1 | 2  | 2
    7 | 4 | 4  | 3
    """
    )

    t2 = T(
        """
      | b | t  | k
    0 | 1 | 2  | 1
    1 | 2 | 5  | 1
    2 | 3 | 6  | 1
    3 | 4 | 7  | 1
    4 | 5 | 14 | 1
    5 | 1 | 3  | 2
    6 | 3 | 3  | 4
    """
    )

    expected = T(
        """
      | a | b | k
    0 | 3 | 1 | 1
    1 | 4 | 1 | 1
    2 | 5 | 3 | 1
    3 | 5 | 4 | 1
    4 | 1 | 1 | 2
    """
    )
    left = T(
        """
      | a | b | k
    4 | 1 |   | 1
    5 | 2 |   | 1
    6 | 6 |   | 1
    7 | 4 |   | 3
        """
    )
    right = T(
        """
      | a | b | k
    7 |   | 2 | 1
    8 |   | 5 | 1
    9 |   | 3 | 4
        """
    )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    w = pw.temporal.tumbling(2)

    join_function = {
        pw.JoinMode.INNER: t1.window_join_inner,
        pw.JoinMode.LEFT: t1.window_join_left,
        pw.JoinMode.RIGHT: t1.window_join_right,
        pw.JoinMode.OUTER: t1.window_join_outer,
    }[join_type]
    res = (
        join_function(t2, pw.left.t, pw.right.t, w, t1.k == pw.right.k)
        .select(pw.left.a, pw.right.b, pw.this.k)
        .update_types(k=int)
    )

    assert_table_equality_wo_index(res, expected)


def test_window_join_sharded_by_multiple_cols() -> None:
    t1 = T(
        """
      | a | t  | k1 | k2
    0 | 1 | -2 |  1 |  1
    1 | 2 | 1  |  1 |  1
    2 | 3 | 2  |  1 |  1
    3 | 4 | 3  |  1 |  1
    4 | 5 | 7  |  1 |  1
    5 | 6 | 13 |  1 |  1
    6 | 1 | 2  |  2 |  1
    7 | 4 | 4  |  1 |  2
    """
    )

    t2 = T(
        """
      | b | t  | k1 | k2
    0 | 1 | 2  |  1 |  1
    1 | 2 | 5  |  1 |  1
    2 | 3 | 6  |  1 |  1
    3 | 4 | 7  |  1 |  1
    4 | 5 | 14 |  1 |  1
    5 | 1 | 3  |  2 |  1
    6 | 3 | 3  |  2 |  2
    """
    )

    expected = T(
        """
       | a | b | k
     0 | 3 | 1 | 1
     1 | 4 | 1 | 1
     2 | 5 | 3 | 1
     3 | 5 | 4 | 1
     4 | 1 | 1 | 2
     5 | 1 |   | 1
     6 | 2 |   | 1
     7 | 6 |   | 1
     8 | 4 |   | 1
     9 |   | 2 | 1
    10 |   | 5 | 1
    11 |   | 3 | 2
        """
    )

    w = pw.temporal.tumbling(2)
    res = t1.window_join_outer(
        t2, pw.left.t, pw.right.t, w, t1.k1 == t2.k1, t1.k2 == t2.k2
    )
    res = res.select(pw.left.a, pw.right.b, k=pw.declare_type(int, pw.this.k1))

    assert_table_equality_wo_index(res, expected)


def test_window_join_with_time_expressions() -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -5
    1 | 2 | -2
    2 | 3 | -1
    3 | 4 | 0
    4 | 5 | 4
    5 | 6 | 10
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | 3
    1 | 2 | 6
    2 | 3 | 7
    3 | 4 | 8
    4 | 5 | 15
    """
    )

    expected = T(
        """
      | a | b
    0 | 3 | 1
    1 | 4 | 1
    2 | 5 | 3
    3 | 5 | 4
    """
    )

    res = t1.window_join_inner(
        t2, 4 * (pw.left.t + 3) // 2, 6 * (pw.right.t - 1) // 3, pw.temporal.tumbling(4)
    )
    res = res.select(t1.a, t2.b)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.xfail(reason="Ix and joins do not mix.")
def test_window_left_join_ix() -> None:
    t1 = T(
        """
      | t
    0 | -2
    1 | 1
    2 | 2
    3 | 3
    4 | 7
    5 | 13
    """
    )

    t2 = T(
        """
      | t
    0 | 2
    1 | 5
    2 | 6
    3 | 7
    4 | 14
    5 | 20
    6 | 30
    """
    )
    expected = T(
        """
           | x  | y  | other
        1  | -2 |    | 2
        2  | -2 |    | 2
        3  | 1  |    | 5
        4  | 1  | 2  | 5
        5  | 2  | 2  | 6
        6  | 2  | 2  | 6
        7  | 3  |    | 7
        8  | 3  | 2  | 7
        9  | 7  | 6  | 14
        10 | 7  | 7  | 14
        11 | 7  | 7  | 14
        12 | 13 | 14 | 20
        13 | 13 |    | 20
            """
    )
    join_result = t1.window_join_left(t2, t1.t, t2.t, pw.temporal.sliding(1, 2))
    res = join_result.select(x=t1.t, y=t2.t, other=t2.ix(t1.id).t)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize("max_difference", [1, 2])
@pytest.mark.parametrize("use_predicate", [True, False])
def test_session_window_join_time_only(
    join_type: pw.JoinMode, max_difference: int, use_predicate: bool
):
    t1 = T(
        """
      | a | t
    0 | 1 | 0
    1 | 2 | 5
    2 | 3 | 10
    3 | 4 | 15
    4 | 5 | 17
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | -3
    1 | 2 | 2
    2 | 3 | 3
    3 | 4 | 6
    4 | 5 | 16
    """
    )
    if max_difference == 1:
        expected = T(
            """
          | a | b
        0 | 2 | 4
        1 | 4 | 5
        2 | 5 | 5
          """
        )
        left = T(
            """
          | a | b
        3 | 1 |
        4 | 3 |
            """
        )
        right = T(
            """
          | a | b
        7 |   | 1
        8 |   | 2
        9 |   | 3
            """
        )
    else:
        expected = T(
            """
          | a | b
        0 | 1 | 2
        1 | 1 | 3
        2 | 1 | 4
        3 | 2 | 2
        4 | 2 | 3
        5 | 2 | 4
        6 | 4 | 5
        7 | 5 | 5
          """
        )
        left = T(
            """
          | a | b
        8 | 3 |
            """
        )
        right = T(
            """
          | a | b
        9 |   | 1
            """
        )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    if use_predicate:
        w = pw.temporal.session(predicate=lambda a, b: abs(a - b) <= max_difference)
    else:
        w = pw.temporal.session(max_gap=max_difference + 1)

    res = {
        pw.JoinMode.INNER: t1.window_join_inner,
        pw.JoinMode.LEFT: t1.window_join_left,
        pw.JoinMode.RIGHT: t1.window_join_right,
        pw.JoinMode.OUTER: t1.window_join_outer,
    }[join_type](t2, t1.t, t2.t, w).select(t1.a, t2.b)

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize("max_difference", [1, 2])
def test_session_window_join_sharded_with_smart_cols(
    join_type: pw.JoinMode, max_difference: int
) -> None:
    t1 = T(
        """
      | k | t
    1 | 1 | 3
    2 | 1 | 4
    3 | 1 | 6
    4 | 1 | 11
    5 | 2 | 0
    6 | 2 | 5
    7 | 2 | 10
    8 | 2 | 15
    9 | 2 | 17
   10 | 3 | 4
    """
    )
    t2 = T(
        """
      | k | t
    1 | 1 | 0
    2 | 1 | 1
    3 | 1 | 5
    4 | 2 | -3
    5 | 2 | 2
    6 | 2 | 3
    7 | 2 | 6
    8 | 2 | 16
    9 | 4 | 3
    """
    )
    if max_difference == 1:
        expected = T(
            """
          | k | left_t | right_t
        0 | 1 |    3   |    5
        1 | 1 |    4   |    5
        2 | 1 |    6   |    5
        3 | 2 |    5   |    6
        4 | 2 |   15   |   16
        5 | 2 |   17   |   16
          """
        )
        left = T(
            """
          | k | left_t | right_t
        6 | 1 |   11   |
        7 | 2 |    0   |
        8 | 2 |   10   |
        9 | 3 |    4   |
            """
        )
        right = T(
            """
          | k | left_t | right_t
       10 | 1 |        |    0
       11 | 1 |        |    1
       12 | 2 |        |   -3
       13 | 2 |        |    2
       14 | 2 |        |    3
       15 | 4 |        |    3
            """
        )
    else:
        expected = T(
            """
          | k | left_t | right_t
        0 | 1 |    3   |    0
        1 | 1 |    3   |    1
        2 | 1 |    3   |    5
        3 | 1 |    4   |    0
        4 | 1 |    4   |    1
        5 | 1 |    4   |    5
        6 | 1 |    6   |    0
        7 | 1 |    6   |    1
        8 | 1 |    6   |    5
        9 | 2 |    0   |    2
       10 | 2 |    0   |    3
       11 | 2 |    0   |    6
       12 | 2 |    5   |    2
       13 | 2 |    5   |    3
       14 | 2 |    5   |    6
       15 | 2 |   15   |   16
       16 | 2 |   17   |   16
          """
        )
        left = T(
            """
          | k | left_t | right_t
       17 | 1 |   11   |
       18 | 2 |   10   |
       19 | 3 |    4   |
            """
        )
        right = T(
            """
          | k | left_t | right_t
       20 | 2 |        |   -3
       21 | 4 |        |    3
            """
        )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    w = pw.temporal.session(max_gap=max_difference + 1)

    res = {
        pw.JoinMode.INNER: t1.window_join_inner,
        pw.JoinMode.LEFT: t1.window_join_left,
        pw.JoinMode.RIGHT: t1.window_join_right,
        pw.JoinMode.OUTER: t1.window_join_outer,
    }[join_type](t2, t1.t, t2.t, w, pw.left.k == pw.right.k).select(
        k=pw.declare_type(int, pw.coalesce(t1.k, t2.k)),
        left_t=pw.left.t,
        right_t=pw.right.t,
    )

    assert_table_equality_wo_index(res, expected)


def test_session_window_join_sharded_by_multiple_cols() -> None:
    t1 = T(
        """
      | k1 | k2 | t
    1 | 1  |  1 | 3
    2 | 1  |  1 | 4
    3 | 1  |  1 | 6
    4 | 1  |  1 | 11
    5 | 2  |  1 | 0
    6 | 2  |  1 | 5
    7 | 2  |  1 | 10
    8 | 2  |  1 | 15
    9 | 2  |  1 | 17
   10 | 1  |  2 | 2
    """
    )
    t2 = T(
        """
      | k1 | k2 | t
    1 |  1 |  1 | 0
    2 |  1 |  1 | 1
    3 |  1 |  1 | 5
    4 |  2 |  1 | -3
    5 |  2 |  1 | 2
    6 |  2 |  1 | 3
    7 |  2 |  1 | 6
    8 |  2 |  1 | 16
    9 |  2 |  2 | 4
    """
    )
    expected = T(
        """
       | k | left_t | right_t
     0 | 1 |    3   |    5
     1 | 1 |    4   |    5
     2 | 1 |    6   |    5
     3 | 2 |    5   |    6
     4 | 2 |   15   |   16
     5 | 2 |   17   |   16
     6 | 1 |   11   |
     7 | 2 |    0   |
     8 | 2 |   10   |
     9 | 1 |    2   |
    10 | 1 |        |    0
    11 | 1 |        |    1
    12 | 2 |        |   -3
    13 | 2 |        |    2
    14 | 2 |        |    3
    15 | 2 |        |    4
        """
    )
    w = pw.temporal.session(max_gap=2)
    res = t1.window_join_outer(t2, t1.t, t2.t, w, t1.k1 == t2.k1, t1.k2 == t2.k2)
    res = res.select(
        k=pw.declare_type(int, pw.coalesce(t1.k1, t2.k1)), left_t=t1.t, right_t=t2.t
    )
    assert_table_equality_wo_index(res, expected)


def test_window_session_join_with_time_expressions() -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -3
    1 | 2 | 2
    2 | 3 | 7
    3 | 4 | 12
    4 | 5 | 14
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | -2
    1 | 2 | 3
    2 | 3 | 4
    3 | 4 | 7
    4 | 5 | 17
    """
    )

    expected = T(
        """
      | a | b
    0 | 1 | 2
    1 | 1 | 3
    2 | 1 | 4
    3 | 2 | 2
    4 | 2 | 3
    5 | 2 | 4
    6 | 4 | 5
    7 | 5 | 5
        """
    )

    res = t1.window_join_inner(
        t2,
        4 * (pw.left.t + 3) // 2,
        6 * (pw.right.t - 1) // 3,
        pw.temporal.session(max_gap=5),
    )
    res = res.select(t1.a, t2.b)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.xfail(reason="Duplicates not working in sorting")
@pytest.mark.parametrize(
    "join_type",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
def test_session_window_join_with_duplicates(join_type: pw.JoinMode):
    t1 = T(
        """
      | a | t
    0 | 1 | 3
    1 | 2 | 5
    2 | 3 | 10
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | -3
    1 | 2 | 2
    2 | 3 | 3
    3 | 4 | 6
    """
    )

    expected = T(
        """
      | a | b
    0 | 1 | 2
    1 | 1 | 3
    2 | 1 | 4
    3 | 2 | 2
    4 | 2 | 3
    5 | 2 | 4
        """
    )
    left = T(
        """
      | a | b
    6 | 3 |
        """
    )
    right = T(
        """
      | a | b
    7 |   | 1
        """
    )

    if join_type in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(left)
    if join_type in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
        expected = expected.concat_reindex(right)

    w = pw.temporal.session(max_gap=3)  # max_difference = 3-eps

    res = {
        pw.JoinMode.INNER: t1.window_join_inner,
        pw.JoinMode.LEFT: t1.window_join_left,
        pw.JoinMode.RIGHT: t1.window_join_right,
        pw.JoinMode.OUTER: t1.window_join_outer,
    }[join_type](t2, t1.t, t2.t, w).select(t1.a, t2.b)

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "w",
    [
        pw.temporal.tumbling(0.2),
        pw.temporal.tumbling(0.4),
        pw.temporal.sliding(0.1, 0.3),
        pw.temporal.session(max_gap=0.101),
    ],
)
def test_window_join_float(w: pw.temporal.Window) -> None:
    t1 = T(
        """
      | a | t
    0 | 1 | -0.2
    1 | 2 | -0.05
    2 | 3 | 0.09
    3 | 4 | 0.61
    4 | 5 | 5.29
    5 | 6 | 5.31
    """
    )

    t2 = T(
        """
      | b | t
    0 | 1 | -0.1
    1 | 2 | 0.19
    2 | 3 | 0.2
    3 | 4 | 0.3
    4 | 5 | 0.4
    5 | 6 | 5.0
    """
    )

    if w == pw.temporal.tumbling(0.2):
        expected = T(
            """
          | a | b
        0 | 1 | 1
        1 | 2 | 1
        2 | 3 | 2
          """
        )
    elif w == pw.temporal.tumbling(0.4):
        expected = T(
            """
          | a | b
        0 | 1 | 1
        1 | 2 | 1
        2 | 3 | 2
        3 | 3 | 3
        4 | 3 | 4
        5 | 4 | 5
        """
        )
    elif w == pw.temporal.sliding(0.1, 0.3):
        expected = T(
            """
           | a | b
         0 | 1 | 1
         1 | 1 | 1
         2 | 2 | 1
         3 | 2 | 1
         4 | 2 | 1
         5 | 2 | 2
         6 | 3 | 1
         7 | 3 | 1
         8 | 3 | 2
         9 | 3 | 2
        10 | 3 | 3
        11 | 4 | 5
        12 | 5 | 6
          """
        )
    elif w == pw.temporal.session(max_gap=0.101):
        expected = T(
            """
           | a | b
         0 | 1 | 1
         1 | 2 | 1
         2 | 3 | 2
         3 | 3 | 3
         4 | 3 | 4
         5 | 3 | 5
          """
        )
    else:
        raise ValueError("Inappropriate window provided")

    res = t1.window_join_inner(t2, t1.t, t2.t, w).select(t1.a, t2.b)
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "join_mode",
    [pw.JoinMode.INNER, pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER],
)
@pytest.mark.parametrize(
    "left_type,right_type,window,error_str",
    [
        (
            int,
            int,
            pw.temporal.tumbling(duration=datetime.timedelta(days=1)),
            ", window.hop",
        ),
        (
            int,
            DATE_TIME_NAIVE,
            pw.temporal.tumbling(duration=2, origin=1.1),
            ", window.hop, window.origin",
        ),
        (
            int,
            int,
            pw.temporal.sliding(
                hop=datetime.timedelta(days=1), duration=datetime.timedelta(days=2)
            ),
            ", window.hop, window.duration",
        ),
        (DATE_TIME_NAIVE, float, pw.temporal.tumbling(duration=1.2), ", window.hop"),
        (int, DATE_TIME_UTC, pw.temporal.tumbling(duration=1.2), ", window.hop"),
        (float, DATE_TIME_NAIVE, pw.temporal.session(max_gap=2), ", window.max_gap"),
        (DATE_TIME_UTC, int, pw.temporal.session(predicate=lambda a, b: False), ""),
        (
            DATE_TIME_NAIVE,
            DATE_TIME_NAIVE,
            pw.temporal.sliding(hop=2, duration=3.5),
            ", window.hop, window.duration",
        ),
    ],
)
def test_incorrect_args(join_mode, left_type, right_type, window, error_str):
    t1 = pw.Table.empty(a=int, t=left_type)

    t2 = pw.Table.empty(b=int, t=right_type)

    with pytest.raises(
        TypeError,
        match=rf"Arguments \(left_time_expression, right_time_expression{error_str}"
        + r"\) have to be of types .* but are of types .*",
    ):
        {
            pw.JoinMode.INNER: t1.window_join_inner,
            pw.JoinMode.LEFT: t1.window_join_left,
            pw.JoinMode.RIGHT: t1.window_join_right,
            pw.JoinMode.OUTER: t1.window_join_outer,
        }[join_mode](t2, t1.t, t2.t, window).select(t1.a, t2.b)


def test_complicated_windowjoin():
    clickstream_data = T(
        """
     user_id |   session_id  |   timestamp   |   page_url
    0x7f8b4c |   0x64a0c7    |   1686024012  | /home
    0x7f8b4c |   0x64a0c7    |   1686024098  | /products/0x40c391
    0x5eaf7f |   0x22e5b3    |   1686025112  | /products
    0x5eaf7f |   0xf508e6    |   1686025184  | /products/0x04g7d5
    0x6b9d6e |   0x13f6c4    |   1686025647  | /products/0x7a8c5d
    """
    )

    purchase_data = T(
        """
    purchase_id | user_id | timestamp    | product_url
    0x0a1b2c    | 0x7f8b4c| 1686024015   | /products/0x11b87b
    0x0b1a2d    | 0x32ad44| 1686024205   | /products/0x40c391
    0x0c1b3d    | 0x5eaf7f| 1686025115   | /products/0x31d4a2
    0x0d1e3f    | 0x5eaf7f| 1686025190   | /products/0x04g7d5
    0x0d1e3f    | 0x5eaf7f| 1686025240   | /products/0x04g7d5
    0x0e1f4g    | 0x6b9d6e| 1686025650   | /products/0x7a8c5d
    """
    )
    matched_data = (
        purchase_data.window_join_inner(
            clickstream_data,
            purchase_data.timestamp,
            clickstream_data.timestamp,
            pw.temporal.sliding(
                hop=50, duration=100
            ),  # Change to a sliding window with a hop of 5 and duration of 10
            (pw.left.user_id == pw.right.user_id),
            (pw.left.product_url == pw.right.page_url),
        )
        .select(pw.this._pw_window_start, pw.this._pw_window_end, pw.this._pw_window)
        .groupby(pw.this._pw_window)
        .reduce(
            window_start=pw.reducers.unique(pw.this._pw_window_start),
            window_end=pw.reducers.unique(pw.this._pw_window_end),
            count=pw.reducers.count(),
        )
    )
    expected = T(
        """
window_start | window_end | count
1686025100   | 1686025200 | 1
1686025150   | 1686025250 | 2
1686025600   | 1686025700 | 1
        """
    )
    assert_table_equality_wo_index(matched_data, expected)


def test_window_joins_typing_on():
    left_table = pw.Table.empty(timestamp=int, col=int)
    right_table = pw.Table.empty(timestamp=int, col=str)
    with pytest.raises(expected_exception=TypeError):
        left_table.window_join(
            right_table,
            left_table.timestamp,
            right_table.timestamp,
            pw.temporal.sliding(hop=50, duration=100),
            left_table.col == right_table.col,
        )

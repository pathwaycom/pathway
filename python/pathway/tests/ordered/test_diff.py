# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.tests.utils import T, assert_table_equality_wo_index


def test_diff_single_column():
    t = T(
        """
            | t |  v
        1   | 1 |  1
        2   | 2 |  2
        3   | 3 |  4
        4   | 4 |  7
        5   | 5 |  11
        6   | 6 |  16
        7   | 7 |  22
        8   | 8 |  29
        9   | 9 |  37
    """
    )
    res = t.diff(t.t, t.v)

    expected = T(
        """
            | diff_v
        1   |
        2   | 1
        3   | 2
        4   | 3
        5   | 4
        6   | 5
        7   | 6
        8   | 7
        9   | 8
    """
    )

    assert_table_equality_wo_index(res, expected)


def test_diff_multiple_columns():
    t = T(
        """
            | t |  v1  | v2
        1   | 1 |  1   | 0
        2   | 2 |  2   | 10
        3   | 3 |  4   | 54
        4   | 4 |  7   | 64
        5   | 5 |  11  | 12
        6   | 6 |  16  | 24
        7   | 7 |  22  | 18
        8   | 8 |  29  | -45
        9   | 9 |  37  | 100
    """
    )
    res = t.diff(t.t, t.v1, t.v2)

    expected = T(
        """
            | diff_v1 | diff_v2
        1   |    |
        2   | 1  | 10
        3   | 2  | 44
        4   | 3  | 10
        5   | 4  | -52
        6   | 5  | 12
        7   | 6  | -6
        8   | 7  | -63
        9   | 8  | 145
    """
    )

    assert_table_equality_wo_index(res, expected)


def test_diff_instance():
    t = T(
        """
            | t | i |  v
        1   | 1 | 0 |  1
        2   | 2 | 1 |  2
        3   | 3 | 1 |  4
        4   | 3 | 0 |  7
        5   | 5 | 1 |  11
        6   | 5 | 0 |  16
        7   | 7 | 0 |  22
        8   | 8 | 1 |  29
        9   | 9 | 0 |  37
    """
    )
    res = t.diff(t.t, t.v, instance=t.i)

    expected = T(
        """
            | diff_v
        1   |
        2   |
        3   |  2
        4   |  6
        5   |  7
        6   |  9
        7   |  6
        8   | 18
        9   | 15
    """
    )

    assert_table_equality_wo_index(res, expected)

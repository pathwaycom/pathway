# Copyright © 2024 Pathway

from __future__ import annotations

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index_types


def test_interpolate_already_sorted():
    t = T(
        """
            | t |  v
        1   | 1 |  1
        2   | 2 |  2
        3   | 3 |  3
        4   | 4 |  4
        5   | 5 |  5
        6   | 6 |  6
        7   | 7 |  7
        8   | 8 |  8
        9   | 9 |  9
    """
    )
    res = pw.statistical.interpolate(t, t.t, t.v)

    assert_table_equality_wo_index_types(res, t),


def test_interpolate_multiple_columns():
    t = T(
        """
            | t |  v1 | v2
        1   | 1 |  1  |
        2   | 2 |     | 10
        3   | 3 |  3  | 40
        4   | 4 |     |
        5   | 5 |  5  | 50
        6   | 6 |     |
        7   | 7 |     |
        8   | 8 |     | 80
        9   | 9 |  9  |
    """
    )
    res = pw.statistical.interpolate(t, t.t, t.v1, t.v2)

    expected = T(
        """
            | t |  v1   | v2
        1   | 1 |  1    | 10.0
        2   | 2 |  2.0  | 10
        3   | 3 |  3    | 40
        4   | 4 |  4.0  | 45.0
        5   | 5 |  5    | 50
        6   | 6 |  6.0  | 60.0
        7   | 7 |  7.0  | 70.0
        8   | 8 |  8.0  | 80
        9   | 9 |  9    | 80.0
    """
    )

    assert_table_equality_wo_index_types(res, expected),

# Copyright © 2024 Pathway

import pytest

from pathway import Table
from pathway.tests.utils import T, assert_table_equality, run_all


def test_unsafe_promise_same_universe_as():
    t_latin = T(
        """
            | lower | upper
        1   | a     | A
        2   | b     | B
        26  | z     | Z
        """
    )
    t_num = T(
        """
            | num
        1   | 1
        2   | 2
        26  | 26
        """
    )
    with pytest.deprecated_call():
        t_num = t_num.unsafe_promise_same_universe_as(t_latin)
    with pytest.deprecated_call():
        Table.unsafe_promise_same_universe_as(t_num, t_latin)
    run_all()


def test_unsafe_promise_universe_is_subset_of():
    t1 = T(
        """
        | col
    1  | a
    2  | b
    3  | c
    """
    )
    t2 = T(
        """
       | col
    2  | 1
    3  | 1
    """
    )
    with pytest.deprecated_call():
        t2 = t2.unsafe_promise_universe_is_subset_of(t1)
    with pytest.deprecated_call():
        Table.unsafe_promise_universe_is_subset_of(t2, t1)
    run_all()


def test_unsafe_promise_universes_are_pairwise_disjoint():
    t1 = T(
        """
       | lower | upper
    1  | a     | A
    2  | b     | B
    """
    )
    t2 = T(
        """
       | lower | upper
    3  | c     | C
    """
    )
    with pytest.deprecated_call():
        t2 = t2.unsafe_promise_universes_are_pairwise_disjoint(t1)
    with pytest.deprecated_call():
        Table.unsafe_promise_universes_are_pairwise_disjoint(t2, t1)
    run_all()


def test_left_join():
    t1 = T(
        """
       | lower | upper
    1  | a     | A
    2  | b     | B
    3  | c     | C
    """
    )
    t2 = T(
        """
       | lower | upper
    3  | c     | C
    4  | d     | D
    """
    )
    with pytest.deprecated_call():
        assert_table_equality(
            t1.left_join(t2, t1.lower == t2.lower).select(t1.lower, t2.upper),
            t1.join_left(t2, t1.lower == t2.lower).select(t1.lower, t2.upper),
        )


def test_right_join():
    t1 = T(
        """
       | lower | upper
    1  | a     | A
    2  | b     | B
    3  | c     | C
    """
    )
    t2 = T(
        """
       | lower | upper
    3  | c     | C
    4  | d     | D
    """
    )
    with pytest.deprecated_call():
        assert_table_equality(
            t1.right_join(t2, t1.lower == t2.lower).select(t1.lower, t2.upper),
            t1.join_right(t2, t1.lower == t2.lower).select(t1.lower, t2.upper),
        )


def test_outer_join():
    t1 = T(
        """
       | lower | upper
    1  | a     | A
    2  | b     | B
    3  | c     | C
    """
    )
    t2 = T(
        """
       | lower | upper
    3  | c     | C
    4  | d     | D
    """
    )
    with pytest.deprecated_call():
        assert_table_equality(
            t1.outer_join(t2, t1.lower == t2.lower).select(t1.lower, t2.upper),
            t1.join_outer(t2, t1.lower == t2.lower).select(t1.lower, t2.upper),
        )

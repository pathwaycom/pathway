# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Optional

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    T,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_index_types,
)


# all entries match, should work as inner join
def test_left_join_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_A  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        14  | 14    | 328
        """
    ).update_types(
        S=Optional[int],
        t2_A=Optional[int],
    )

    res = t1.join_left(t2, t1.A == t2.A).select(
        t1.A,
        t2_A=t2.A,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


def test_left_join_universe_asserts():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    xxx = t1.join_left(t2, t1.A == t2.A)
    yyy = t1.join_left(t2, t1.A == t2.A)
    pw.universes.promise_are_equal(xxx, yyy)

    res_x = xxx.select(
        t1.A,
        t2_A=t2.A,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    res_y = yyy.select(
        t1.A,
        t2_A=t2.A,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    assert_table_equality_wo_index(res_x, res_y)


def test_left_join_015():
    t1 = T(
        """
            | A
          1 | 11
          2 | 12
          3 | 13
          4 | 14
        """
    )

    t2 = T(
        """
            | C
          1 | 11
          2 | 12
          3 | 13
          4 | 13
        """
    )

    expected = T(
        """
          | A
        1 | 11
        2 | 12
        3 | 13
        4 | 13
        5 |
        """
    )

    res = t1.join_left(t2, t1.A == t2.C).select(
        A=t2.C  # pw.require(t1.A + t2.C, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


# 14 should not match
def test_left_join_02():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 13 | 214
        """
    )

    expected = T(
        """
        A   | t2_C  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        13  | 13    | 327
        14  |       |
        """
    )

    res = t1.join_left(t2, t1.A == t2.C).select(
        t1.A,
        t2_C=t2.C,
        S=pw.require(t1.B + t2.D, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


# filling in computable columns
# as opposed to test 2, column t1_A2 can be computed out of columns of t1
# as such, it should be filled, even if there was no match
def test_left_join_03():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 13 | 214
        """
    )

    expected = T(
        """
        A   | t1_A2 | S
        11  | 121    | 322
        12  | 144    | 324
        13  | 169    | 326
        13  | 169    | 327
        14  | 196    |
        """
    )

    res = t1.join_left(t2, t1.A == t2.C).select(
        t1.A,
        t1_A2=t1.A * t1.A,
        S=pw.require(t1.B + t2.D, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


def test_right_join_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_A  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        14  | 14    | 328
        """
    ).update_types(
        A=Optional[int],
        S=Optional[int],
    )

    res = t1.join_right(t2, t1.A == t2.A).select(
        t1.A,
        t2_A=t2.A,
        S=pw.require(t1.B + t2.D, t1.id),
    )
    assert_table_equality_wo_index(res, expected)


# 14 should not match
def test_right_join_02():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_C  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        13  | 13    | 327
            | 14    |
        """
    )

    res = t1.join_right(t2, t1.A == t2.C).select(
        t1.A,
        t2_C=t2.C,
        S=pw.require(t1.B + t2.D, t1.id),
    )
    assert_table_equality_wo_index(res, expected)


# filling in computable columns
# as opposed to test 2, column t2_C2 can be computed out of columns of t2
# as such, it should be filled, even if there was no match
def test_right_join_03():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_C2 | S
        11  | 121    | 322
        12  | 144    | 324
        13  | 169    | 326
        13  | 169    | 327
            | 196    |
        """
    )

    res = t1.join_right(t2, t1.A == t2.C).select(
        t1.A,
        t2_C2=t2.C * t2.C,
        S=pw.require(t1.B + t2.D, t1.id),
    )
    assert_table_equality_wo_index(res, expected)


# corner case test: if no column is computable with only outer values,
# each miss creates an empty row
def test_left_join_empty_duplicates_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 13 | 212
          3 | 13 | 213
          4 | 13 | 214
        """
    )

    expected = T(
        """
        t2_C2 | S
        121    | 322
        169    | 325
        169    | 326
        169    | 327
               |
               |
        """
    )

    res = t1.join_left(t2, t1.A == t2.C).select(
        t2_C2=pw.require(t2.C * t2.C, t2.id),
        S=pw.require(t1.B + t2.D, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


# corner case test: all columns are computable with outer values,
# we should see duplicate rows,
# 0 matches for one row creates 1 output row
# k > 0 matches for one row creates k duplicate output rows
def test_left_join_duplicates_02():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 13 | 212
          3 | 13 | 213
          4 | 13 | 214
        """
    )

    expected = T(
        """
        t1_A2  | S
        121    | 122
        169    | 126
        169    | 126
        169    | 126
        144    | 124
        196    | 128
        """
    )

    res = t1.join_left(t2, t1.A == t2.C).select(
        t1_A2=t1.A * t1.A,
        S=t1.A + t1.B,
    )
    assert_table_equality_wo_index(res, expected)


# corner case test: if no column is computable with only-outer values,
# each miss should produce an empty row
def test_right_join_empty_duplicates_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 13 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        t1_A2 | S
        121    | 322
        169    | 325
        169    | 326
        169    | 327
               |
               |
        """
    )

    res = t1.join_right(t2, t1.A == t2.C).select(
        t1_A2=pw.require(t1.A * t1.A, t1.id),
        S=pw.require(t1.B + t2.D, t1.id),
    )
    assert_table_equality_wo_index(res, expected)


# corner case test: all columns are computable with outer values,
# we should see duplicate rows,
# 0 matches for one row creates 1 output row
# k > 0 matches for one row creates k duplicate output rows


def test_right_join_duplicates_02():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 13 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        t2_C2  | S
        121    | 222
        169    | 226
        169    | 226
        169    | 226
        144    | 224
        196    | 228
        """
    )

    res = t1.join_right(t2, t1.A == t2.C).select(t2_C2=t2.C * t2.C, S=t2.C + t2.D)
    assert_table_equality_wo_index(res, expected)


def test_left_join_this():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_A  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        14  | 14    | 328
        """
    ).update_types(
        t2_A=Optional[int],
        S=Optional[int],
    )

    res = t1.join_left(t2, t1.A == t2.A).select(
        pw.left.A,
        t2_A=t2.A,
        S=pw.require(pw.left.B + t2.D, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


# all entries match, should work as inner join
def test_outer_join_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_A  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        14  | 14    | 328
        """
    ).update_types(
        A=Optional[int],
        t2_A=Optional[int],
        S=Optional[int],
    )

    res = t1.join_outer(t2, t1.A == t2.A).select(
        t1.A,
        t2_A=t2.A,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


# 14 should not match, unmatched entry form the right table
def test_outer_join_02():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 13 | 214
        """
    )

    expected = T(
        """
        A   | t2_C  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        13  | 13    | 327
        14  |       |
        """
    ).update_types(A=Optional[int])

    res = t1.join_outer(t2, t1.A == t2.C).select(
        t1.A,
        t2_C=t2.C,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


# 14 should not match, unmatched entry form the right table
def test_outer_join_03():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_C  | S
        11  | 11    | 322
        12  | 12    | 324
        13  | 13    | 326
        13  | 13    | 327
            | 14    |
        """
    ).update_types(t2_C=Optional[int])

    res = t1.join_outer(t2, t1.A == t2.C).select(
        t1.A,
        t2_C=t2.C,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


def test_outer_join_04():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 14 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
        A   | t2_C  | S
        11  | 11    | 322
        12  | 12    | 324
        13  |       |
        13  |       |
            | 14    |
            | 14    |
        """
    )

    res = t1.join_outer(t2, t1.A == t2.C).select(
        t1.A,
        t2_C=t2.C,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


def test_outer_join_smart_cols():
    t1 = T(
        """
            | A
          1 | 11
          2 | 12
          3 | 13
          4 | 14
        """
    )

    t2 = T(
        """
            | A
          2 | 12
          3 | 13
          4 | 14
          5 | 15
        """
    )

    expected = T(
        """
         A | lA  | rA
        11 | 11  |
        12 | 12  | 12
        13 | 13  | 13
        14 | 14  | 14
        15 |     | 15
        """
    ).update_types(A=Optional[int])

    res = t1.join_outer(t2, t1.A == t2.A).select(
        pw.this.A,
        lA=pw.left.A,
        rA=pw.right.A,
    )
    assert_table_equality_wo_index(res, expected)


def test_chained_outer_join_smart_cols():
    t1 = T(
        """
            | A
          1 | 11
          2 | 12
          3 | 13
          4 | 14
        """
    )

    t2 = T(
        """
            | A
          2 | 12
          3 | 13
          4 | 14
          5 | 15
        """
    )

    t3 = T(
        """
            | A
          3 | 13
          4 | 14
          5 | 15
          6 | 16
        """
    )

    expected = T(
        """
         A | lA  | rA | llA | lrA
        11 | 11  |    |  11 |
        12 | 12  |    |  12 | 12
        13 | 13  | 13 |  13 | 13
        14 | 14  | 14 |  14 | 14
        15 | 15  | 15 |     | 15
        16 |     | 16 |     |
        """
    ).update_types(A=Optional[int])

    res = (
        t1.join_outer(t2, t1.A == t2.A)
        .join_outer(t3, pw.left.A == t3.A)
        .select(
            pw.this.A,
            lA=pw.left.A,
            rA=pw.right.A,
            llA=t1.A,
            lrA=t2.A,
        )
    )
    assert_table_equality_wo_index(res, expected)


def test_left_join_set_id_01():
    # ID-s pf t1 and t2 overlap, but are not equal
    # - equal sets of input ID could make test false positive,
    # - overlapping is more difficult to handle than completely disjoint
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          3 | 11 | 211
          4 | 12 | 212
          5 | 13 | 213
          6 | 14 | 214
        """
    )

    res1 = t1.join_left(t2, t1.A == t2.A, id=t1.id)
    assert G.universe_solver.query_are_equal(res1._universe, t1._universe)
    assert_table_equality(res1.select(), t1.select())

    with pytest.raises(KeyError):
        t1.join_left(t2, t1.A == t2.A, id=t2.id)


def test_left_join_set_id_02():
    # ID-s pf t1 and t2 overlap, but are not equal
    # - equal sets of input ID could make test false positive,
    # - overlapping is more difficult to handle than completely disjoint
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          3 | 11 | 211
          4 | 12 | 212
          5 | 13 | 213
          6 | 15 | 214
        """
    )
    # selecting A is relevant for this test;
    # once it behaved differently on select() and select select(t1.A)
    res = t1.join_left(t2, t1.A == t2.A, id=t1.id).select(t1.A)
    assert G.universe_solver.query_are_equal(res._universe, t1._universe)
    assert_table_equality(res.select(), t1.select())


def test_right_join_set_id_01():
    # ID-s pf t1 and t2 overlap, but are not equal
    # - equal sets of input ID could make test false positive,
    # - overlapping is more difficult to handle than completely disjoint
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          3 | 11 | 211
          4 | 12 | 212
          5 | 13 | 213
          6 | 14 | 214
        """
    )

    res2 = t1.join_right(t2, t1.A == t2.A, id=t2.id)
    assert G.universe_solver.query_are_equal(res2._universe, t2._universe)
    assert_table_equality(res2.select(), t2.select())

    with pytest.raises(KeyError):
        t1.join_right(t2, t1.A == t2.A, id=t1.id)


def test_right_join_set_id_02():
    # ID-s pf t1 and t2 overlap, but are not equal
    # - equal sets of input ID could make test false positive,
    # - overlapping is more difficult to handle than completely disjoint
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          3 | 11 | 211
          4 | 12 | 212
          5 | 13 | 213
          6 | 15 | 214
        """
    )

    res = t1.join_right(t2, t1.A == t2.A, id=t2.id)
    assert G.universe_solver.query_are_equal(res._universe, t2._universe)
    assert_table_equality(res.select(), t2.select())


def test_outer_join_set_id_01():
    # ID-s pf t1 and t2 overlap, but are not equal
    # - equal sets of input ID could make test false positive,
    # - overlapping is more difficult to handle than completely disjoint
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          3 | 11 | 211
          4 | 12 | 212
          5 | 13 | 213
          6 | 14 | 214
        """
    )

    with pytest.raises(KeyError):
        t1.join_outer(t2, t1.A == t2.A, id=t2.id)

    with pytest.raises(KeyError):
        t1.join_outer(t2, t1.A == t2.A, id=t1.id)


def test_outer_join_set_id_02():
    # ID-s pf t1 and t2 overlap, but are not equal
    # - equal sets of input ID could make test false positive,
    # - overlapping is more difficult to handle than completely disjoint
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          3 | 11 | 211
          4 | 12 | 212
          5 | 13 | 213
          6 | 14 | 214
        """
    )
    with pytest.raises(AssertionError):
        t1.join_outer(t2, t1.A == t2.A, id=t1.A)


def test_outer_join_desugaring_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 14 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
                  | A   | t2_C  | S
        1139487   | 11  | 11    | 322
        1243425   | 12  | 12    | 324
        2145425   | 13  |       |
        2145234   | 13  |       |
        1234412   |     | 14    |
        1541234   |     | 14    |
        """
    )

    res = t1.join_outer(t2, t1.A == t2.C).select(
        pw.left.A,
        t2_C=pw.right.C,
        S=pw.require(t1.B + t2.D, t1.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


def test_outer_join_desugaring_02():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
                  | A   | t2_A  | S
        1139487   | 11  | 11    | 322
        1243425   | 12  | 12    | 324
        2145425   | 13  | 13    | 326
        1234412   | 14  | 14    | 328
        """
    ).update_types(
        A=Optional[int],
        t2_A=Optional[int],
        S=Optional[int],
    )

    res = t1.join_outer(t2, pw.left.A == pw.right.A).select(
        t1.A,
        t2_A=t2.A,
        S=pw.require(
            pw.left.B + pw.right.D,
            pw.left.id,
            pw.right.id,
        ),
    )
    assert_table_equality_wo_index(res, expected)


def test_outer_join_desugaring_03():
    # ID-s pf t1 and t2 overlap, but are not equal
    # - equal sets of input ID could make test false positive,
    # - overlapping is more difficult to handle than completely disjoint
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | A  | D
          3 | 11 | 211
          4 | 12 | 212
          5 | 13 | 213
          6 | 14 | 214
        """
    )

    with pytest.raises(KeyError):
        t1.join_outer(t2, t1.A == t2.A, id=pw.left.id)
    with pytest.raises(KeyError):
        t1.join_outer(t2, t1.A == t2.A, id=pw.right.id)


def test_right_join_desugaring_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 13 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
                  | A   | t2_C  | S
        1139487   | 11  | 11    | 322
        1243425   | 12  | 12    | 324
        2145425   | 13  | 13    | 326
        2145234   | 13  | 13    | 327
        1234412   |     | 14    |
        """
    )

    res = t1.join_right(t2, t1.A == pw.right.C).select(
        pw.left.A,
        t2_C=t2.C,
        S=pw.require(
            pw.left.B + pw.right.D,
            pw.left.id,
            pw.right.id,
        ),
    )
    assert_table_equality_wo_index(res, expected)


def test_left_join_desugaring_01():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 14 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 13 | 214
        """
    )

    expected = T(
        """
                  | A   | t2_C  | S
        1139487   | 11  | 11    | 322
        1243425   | 12  | 12    | 324
        2145425   | 13  | 13    | 326
        2145234   | 13  | 13    | 327
        1234412   | 14  |       |
        """
    )

    res = t1.join_left(t2, pw.left.A == t2.C).select(
        t1.A,
        t2_C=pw.right.C,
        S=pw.require(pw.left.B + t2.D, pw.left.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


def test_right_join_wid_substitute_and_desugaring():
    t1 = T(
        """
            | A  | B
          1 | 11 | 111
          2 | 12 | 112
          3 | 13 | 113
          4 | 15 | 114
        """
    )

    t2 = T(
        """
            | C  | D
          1 | 11 | 211
          2 | 12 | 212
          3 | 13 | 213
          4 | 14 | 214
        """
    )

    expected = T(
        """
                  | A   | t2_C  | S
        1139487   | 11  | 11    | 322
        1243425   | 12  | 12    | 324
        2145425   | 13  | 13    | 326
        1234412   |     | 14    |
        """
    )

    res = t1.join_right(t2, t1.A == t2.C, id=t2.id).select(
        t1.A,
        t2_C=pw.right.C,
        S=pw.require(pw.left.B + t2.D, pw.left.id, t2.id),
    )
    assert_table_equality_wo_index(res, expected)


def test_outer_join_id():
    t1 = T(
        """
            | A  | B
          1 | a1 | b1
          2 | a2 | b2
        """
    )
    t2 = T(
        """
            | C  | D
          1 | c1 | d1
          3 | c3 | d3
        """
    )
    assert_table_equality(
        t1.join_outer(t2, t1.id == t2.id).select(id_col=pw.this.id),
        t1.join_outer(t2, t1.id == t2.id).select().select(id_col=pw.this.id),
    )


def test_outer_join_chaining_no_cond_leftsided():
    t1 = T(
        """
            | A  | B
          1 | a1 | b1
          2 | a2 | b2
        """
    )
    t2 = T(
        """
            | C  | D
          1 | c1 | d1
          2 | c2 | d2
        """
    )

    t3 = T(
        """
            | E  | F
          1 | e1 | f1
          2 | e2 | f2
        """
    )
    expected = T(
        """
            A  | B  | C  | D  | E  | F
            a1 | b1 | c1 | d1 | e1 | f1
            a1 | b1 | c1 | d1 | e2 | f2
            a1 | b1 | c2 | d2 | e1 | f1
            a1 | b1 | c2 | d2 | e2 | f2
            a2 | b2 | c1 | d1 | e1 | f1
            a2 | b2 | c1 | d1 | e2 | f2
            a2 | b2 | c2 | d2 | e1 | f1
            a2 | b2 | c2 | d2 | e2 | f2
            """
    )
    for tmp in [t1.join(t2), t1.join_outer(t2), t1.join_left(t2), t1.join_right(t2)]:
        for tmp2 in [
            tmp.join(t3),
            tmp.join_outer(t3),
            tmp.join_left(t3),
            tmp.join_right(t3),
        ]:
            from pathway.internals.join import JoinMode

            if tmp._join_mode == JoinMode.INNER and tmp2._join_mode == JoinMode.INNER:
                assert_table_equality_wo_index(tmp2.select(*pw.this), expected)
            else:
                assert_table_equality_wo_index_types(tmp2.select(*pw.this), expected)


def test_outer_join_chaining_some_cond():
    t1 = T(
        """
            | A  | B
          1 | a1 | b1
          2 | a2 | b2
        """
    )
    t2 = T(
        """
            | C  | D
          1 | c1 | d1
          3 | c3 | d3
        """
    )

    t3 = T(
        """
            | E  | F
          2 | e2 | f2
          3 | e3 | f3
        """
    )

    assert_table_equality_wo_index(
        t1.join_outer(t2.join_outer(t3, t2.id == t3.id), t1.id == t2.id).select(
            *pw.this
        ),
        T(
            """
         A  | B  | C  | D  | E  | F
            |    |    |    | e2 | f2
            |    | c3 | d3 | e3 | f3
         a1 | b1 | c1 | d1 |    |
         a2 | b2 |    |    |    |
        """
        ),
    )


def test_outer_join_chaining_no_cond_rightsided():
    t1 = T(
        """
            | A  | B
          1 | a1 | b1
          2 | a2 | b2
        """
    )
    t2 = T(
        """
            | C  | D
          1 | c1 | d1
          2 | c2 | d2
        """
    )

    t3 = T(
        """
            | E  | F
          1 | e1 | f1
          2 | e2 | f2
        """
    )

    expected = T(
        """
        A  | B  | C  | D  | E  | F
        a1 | b1 | c1 | d1 | e1 | f1
        a1 | b1 | c1 | d1 | e2 | f2
        a1 | b1 | c2 | d2 | e1 | f1
        a1 | b1 | c2 | d2 | e2 | f2
        a2 | b2 | c1 | d1 | e1 | f1
        a2 | b2 | c1 | d1 | e2 | f2
        a2 | b2 | c2 | d2 | e1 | f1
        a2 | b2 | c2 | d2 | e2 | f2
        """
    )

    for tmp in [t2.join(t3), t2.join_outer(t3), t2.join_left(t3), t2.join_right(t3)]:
        for tmp2 in [
            t1.join(tmp),
            t1.join_outer(tmp),
            t1.join_left(tmp),
            t1.join_right(tmp),
        ]:
            from pathway.internals.join import JoinMode

            if tmp._join_mode == JoinMode.INNER and tmp2._join_mode == JoinMode.INNER:
                assert_table_equality_wo_index(tmp2.select(*pw.this), expected)
            else:
                assert_table_equality_wo_index_types(tmp2.select(*pw.this), expected)


def test_outer_join_chaining_cond():
    t1 = T(
        """
            | A  | col
          1 | a1 | 1
          2 | a2 | 2
          3 | a3 | 3
          4 | a4 | 4
        """
    )

    t2 = T(
        """
            | B  | col
          1 | b1 | 1
          3 | b3 | 3
          5 | b5 | 5
          7 | b7 | 7
        """
    )

    t3 = T(
        """
            | C  | col
          1 | c1 | 1
          2 | c2 | 2
          5 | c5 | 5
          6 | c6 | 6
        """
    )
    assert_table_equality_wo_index(
        t1.join_outer(t2, t1.col == t2.col)
        .join_outer(t3, t1.col == t3.col)
        .select(t1.A, t2.B, t3.C, col1=t1.col, col2=t2.col, col3=t3.col),
        T(
            """
         A  | B  | C  | col1 | col2 | col3
            |    | c5 |      |      | 5
            |    | c6 |      |      | 6
            | b5 |    |      | 5    |
            | b7 |    |      | 7    |
         a1 | b1 | c1 | 1    | 1    | 1
         a2 |    | c2 | 2    |      | 2
         a3 | b3 |    | 3    | 3    |
         a4 |    |    | 4    |      |
        """
        ),
    )


def test_leftjoin_chain_assign_id():
    left_table = T(
        """
           | a  | b
        1  | a1 | b1
        2  | a2 | b2
        3  | a3 | b3
        4  | a4 | b4
        """
    )

    middle_table = T(
        """
            | bb  | c
        11  | b2 | c2
        12  | b3 | c3
        13  | b4 | c4
        14  | b5 | c5
        """
    )

    right_table = T(
        """
           | cc  | d
        21 | c3 | d3
        22 | c4 | d4
        23 | c5 | d5
        24 | c6 | d6
        """
    )

    assert_table_equality(
        left_table.join_left(middle_table, pw.left.b == pw.right.bb, id=pw.left.id)
        .join_left(right_table, pw.left.c == pw.right.cc, id=pw.left.id)
        .select(*pw.this),
        T(
            """
          | a  | b  | bb | c  | cc | d
        1 | a1 | b1 |    |    |    |
        2 | a2 | b2 | b2 | c2 |    |
        3 | a3 | b3 | b3 | c3 | c3 | d3
        4 | a4 | b4 | b4 | c4 | c4 | d4
        """
        ),
    )


def test_joins_typing_on():
    left_table = pw.Table.empty(col=int)
    right_table = pw.Table.empty(col=str)
    with pytest.raises(expected_exception=TypeError):
        left_table.join(right_table, left_table.col == right_table.col)

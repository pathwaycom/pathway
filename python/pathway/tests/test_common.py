# Copyright © 2024 Pathway

from __future__ import annotations

import functools
import inspect
import multiprocessing
import os
import pathlib
import re
import warnings
from typing import Any, Optional
from unittest import mock

import numpy as np
import pandas as pd
import pytest

import pathway as pw
import pathway.internals.shadows.operator as operator
from pathway.debug import table_from_pandas, table_to_pandas
from pathway.internals import api, dtype as dt
from pathway.internals.parse_graph import warn_if_some_operators_unused
from pathway.internals.table_io import empty_from_schema
from pathway.tests.utils import (
    T,
    assert_stream_equality,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_index_types,
    assert_table_equality_wo_types,
    needs_multiprocessing_fork,
    run_all,
    warns_here,
)


@pytest.mark.parametrize(
    "asserter",
    [
        assert_table_equality,
        assert_table_equality_wo_index,
        assert_table_equality_wo_types,
        assert_table_equality_wo_index_types,
    ],
)
@pytest.mark.parametrize(
    "unexpected",
    [
        """
        foo
        1
        """,
        """
        foo   | bar
        42    | 42
        """,
        """
        bar   | foo
        1     | 1
        """,
    ],
)
def test_assert_table_unexpected_columns(asserter, unexpected):
    input = T(
        """
        foo   | bar
        1     | 1
        """
    )

    with pytest.raises((RuntimeError, AssertionError)):
        asserter(input, T(unexpected))


def test_input_operator():
    input = T(
        """
        foo
        1
        2
        """
    )

    assert_table_equality(
        input,
        T(
            """
            foo
            1
            2
            """
        ),
    )


def test_select_column_ref():
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

    res = t_latin.select(num=t_num.num, upper=t_latin["upper"])

    assert_table_equality(
        res,
        T(
            """
                | num | upper
            1   | 1   | A
            2   | 2   | B
            26  | 26  | Z
            """
        ),
    )


def test_select_arithmetic_with_const():
    table = T(
        """
        a
        42
        """
    )

    res = table.select(
        table.a,
        add=table.a + 1,
        radd=1 + table.a,
        sub=table.a - 1,
        rsub=1 - table.a,
        mul=table.a * 2,
        rmul=2 * table.a,
        truediv=table.a / 4,
        rtruediv=63 / table.a,
        floordiv=table.a // 4,
        rfloordiv=63 // table.a,
        mod=table.a % 4,
        rmod=63 % table.a,
        pow=table.a**2,
        rpow=2**table.a,
    )

    assert_table_equality(
        res,
        T(
            """
            a  | add | radd | sub | rsub | mul | rmul | truediv | rtruediv | floordiv | rfloordiv | mod | rmod | pow  | rpow
            42 | 43  | 43   | 41  | -41  | 84  | 84   | 10.5    | 1.5      | 10       | 1         | 2   | 21   | 1764 | 4398046511104
            """  # noqa: E501
        ),
    )


def test_select_values():
    t1 = T(
        """
    lower | upper
    a     | A
    b     | B
    """
    )

    res = t1.select(foo="alpha", bar="beta")
    assert_table_equality(
        res,
        T(
            """
    foo   | bar
    alpha | beta
    alpha | beta
        """
        ),
    )


def test_select_column_different_universe():
    foo = T(
        """
       | col
    1  | a
    2  | b
    """
    )
    bar = T(
        """
           | col
        3  | a
        4  | b
        5  | c
        """
    )
    with pytest.raises(ValueError):
        foo.select(ret=bar.col)


def test_select_const_expression():
    input = T(
        """
        foo | bar
        1   | 3
        2   | 4
        """
    )

    result = input.select(a=42)

    assert_table_equality(
        result,
        T(
            """
        a
        42
        42
        """
        ),
    )


def test_select_simple_expression():
    input = T(
        """
        foo | bar
        1   | 3
        2   | 4
        """
    )

    result = input.select(a=input.bar + input.foo)

    assert_table_equality(
        result,
        T(
            """
            a
            4
            6
            """
        ),
    )


def test_select_int_unary():
    input = T(
        """
        a
        1
        """
    )

    result = input.select(
        input.a,
        minus=-input.a,
    )

    assert_table_equality(
        result,
        T(
            """
            a | minus
            1 | -1
            """
        ),
    )


def test_select_int_binary():
    input = T(
        """
        a | b
        1 | 2
        """
    )

    result = input.select(
        input.a,
        input.b,
        add=input.a + input.b,
        sub=input.a - input.b,
        truediv=input.a / input.b,
        floordiv=input.a // input.b,
        mul=input.a * input.b,
    )

    assert_table_equality(
        result,
        T(
            """
            a | b | add | sub | truediv | floordiv | mul
            1 | 2 | 3   | -1  | 0.5     | 0        | 2
            """
        ),
    )


def test_select_int_comparison():
    input = T(
        """
        a | b
        1 | 2
        2 | 2
        3 | 2
        """
    )

    result = input.select(
        input.a,
        input.b,
        eq=input.a == input.b,
        ne=input.a != input.b,
        lt=input.a < input.b,
        le=input.a <= input.b,
        gt=input.a > input.b,
        ge=input.a >= input.b,
    )

    assert_table_equality(
        result,
        T(
            """
            a | b | eq    | ne    | lt    | le    | gt    | ge
            1 | 2 | false | true  | true  | true  | false | false
            2 | 2 | true  | false | false | true  | false | true
            3 | 2 | false | true  | false | false | true  | true
            """
        ),
    )


def test_select_float_comparison():
    input = T(
        """
        a   | b
        1.5 | 2.5
        2.5 | 2.5
        3.5 | 2.5
        """
    )

    result = input.select(
        input.a,
        input.b,
        eq=input.a == input.b,
        ne=input.a != input.b,
        lt=input.a < input.b,
        le=input.a <= input.b,
        gt=input.a > input.b,
        ge=input.a >= input.b,
    )

    assert_table_equality(
        result,
        T(
            """
            a   | b   | eq    | ne    | lt    | le    | gt    | ge
            1.5 | 2.5 | false | true  | true  | true  | false | false
            2.5 | 2.5 | true  | false | false | true  | false | true
            3.5 | 2.5 | false | true  | false | false | true  | true
            """
        ),
    )


def test_select_mixed_comparison():
    input = T(
        """
        a   | b
        1.5 | 2
        2.0 | 2
        3.5 | 2
        """
    )
    result = input.select(
        input.a,
        input.b,
        eq=input.a == input.b,
        ne=input.a != input.b,
        lt=input.a < input.b,
        le=input.a <= input.b,
        gt=input.a > input.b,
        ge=input.a >= input.b,
    )

    assert_table_equality(
        result,
        T(
            """
            a   | b | eq    | ne    | lt    | le    | gt    | ge
            1.5 | 2 | false | true  | true  | true  | false | false
            2.0 | 2 | true  | false | false | true  | false | true
            3.5 | 2 | false | true  | false | false | true  | true
            """
        ),
    )


def test_select_float_unary():
    input = T(
        """
        a
        1.25
        """
    )

    result = input.select(
        input.a,
        minus=-input.a,
    )

    assert_table_equality(
        result,
        T(
            """
            a    | minus
            1.25 | -1.25
            """
        ),
    )


def test_select_float_binary():
    input = T(
        """
        a    | b
        1.25 | 2.5
        """
    )

    result = input.select(
        input.a,
        input.b,
        add=input.a + input.b,
        sub=input.a - input.b,
        truediv=input.a / input.b,
        floordiv=input.a // input.b,
        mul=input.a * input.b,
    )

    assert_table_equality(
        result,
        T(
            """
            a    | b   | add  | sub   | truediv | floordiv | mul
            1.25 | 2.5 | 3.75 | -1.25 | 0.5     | 0.0        | 3.125
            """
        ).update_types(floordiv=float),
    )


def test_select_bool_unary():
    input = T(
        """
        a
        true
        false
        """
    )

    result = input.select(
        input.a,
        not_=~input.a,
    )

    assert_table_equality(
        result,
        T(
            """
            a     | not_
            true  | false
            false | true
            """
        ),
    )


def test_select_bool_binary():
    input = T(
        """
        a     | b
        false | false
        false | true
        true  | false
        true  | true
        """
    )

    result = input.select(
        input.a,
        input.b,
        and_=input.a & input.b,
        or_=input.a | input.b,
        xor=input.a ^ input.b,
    )

    assert_table_equality(
        result,
        T(
            """
            a     |  b    | and_  | or_   | xor
            false | false | false | false | false
            false | true  | false | true  | true
            true  | false | false | true  | true
            true  | true  | true  | true  | false
            """
        ),
    )


def test_broadcasting_singlerow():
    table = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
     1   | Bob     | 9
     2   | Alice   | 8
     1   | Bob     | 7
     0   | Eve     | 10
        """
    )

    row = table.reduce(val=1)
    returned = table.select(newval=row.ix_ref().val)

    expected = T(
        """
    newval
     1
     1
     1
     1
     1
        """
    )
    assert_table_equality(returned, expected)


def test_indexing_single_value_groupby():
    indexed_table = T(
        """
    colA   | colB
    10     | A
    20     | A
    30     | B
    40     | B
    """
    )
    grouped_table = indexed_table.groupby(pw.this.colB).reduce(
        pw.this.colB, sum=pw.reducers.sum(pw.this.colA)
    )
    returned = indexed_table + grouped_table.ix_ref(indexed_table.colB)[["sum"]]

    expected = T(
        """
    colA   | colB | sum
    10     | A    | 30
    20     | A    | 30
    30     | B    | 70
    40     | B    | 70
    """
    )
    assert_table_equality_wo_index(returned, expected)


def test_indexing_single_value_groupby_hardcoded_value():
    indexed_table = T(
        """
    colA   | colB
    10     | A
    20     | A
    30     | B
    40     | B
    """
    )
    grouped_table = indexed_table.groupby(pw.this.colB).reduce(
        pw.this.colB, sum=pw.reducers.sum(pw.this.colA)
    )
    returned = indexed_table + grouped_table.ix_ref("A", context=indexed_table)[["sum"]]
    returned2 = indexed_table.select(*pw.this, sum=grouped_table.ix_ref("A").sum)
    expected = T(
        """
    colA   | colB | sum
    10     | A    | 30
    20     | A    | 30
    30     | B    | 30
    40     | B    | 30
    """
    )
    assert_table_equality_wo_index(returned, expected)
    assert_table_equality(returned, returned2)


def test_indexing_two_values_groupby():
    indexed_table = T(
        """
    colA  | colB | colC
    1     | A    | D
    2     | A    | D
    10    | A    | E
    20    | A    | E
    100   | B    | F
    200   | B    | F
    1000  | B    | G
    2000  | B    | G
    """
    )
    grouped_table = indexed_table.groupby(pw.this.colB, pw.this.colC).reduce(
        pw.this.colB, pw.this.colC, sum=pw.reducers.sum(pw.this.colA)
    )
    returned = (
        indexed_table
        + grouped_table.ix_ref(indexed_table.colB, indexed_table.colC)[["sum"]]
    )
    expected = T(
        """
    colA  | colB | colC | sum
    1     | A    | D    | 3
    2     | A    | D    | 3
    10    | A    | E    | 30
    20    | A    | E    | 30
    100   | B    | F    | 300
    200   | B    | F    | 300
    1000  | B    | G    | 3000
    2000  | B    | G    | 3000
    """
    )
    assert_table_equality_wo_index(returned, expected)


def test_ixref_optional():
    indexed_table = T(
        """
    colA  | colB | colC
    1     | A    | D
    2     | A    | D
    10    | A    | E
    20    | A    | E
    100   | B    | F
    200   | B    | F
    1000  | B    | G
    2000  | B    | G
    """
    )
    grouped_table = indexed_table.groupby(pw.this.colB, pw.this.colC).reduce(
        pw.this.colB, pw.this.colC, sum=pw.reducers.sum(pw.this.colA)
    )
    indexer = T(
        """
        refB | refC
        A    | D
        A    | E
        B    | F
        B    | G
             | D
        A    |
             |
        """
    )
    returned = indexer.select(
        *pw.this,
        sum=grouped_table.ix_ref(indexer.refB, indexer.refC, optional=True).sum,
    )
    expected = T(
        """
    refB  | refC | sum
     A    | D    | 3
     A    | E    | 30
     B    | F    | 300
     B    | G    | 3000
          | D    |
     A    |      |
          |      |
    """
    )
    assert_table_equality_wo_index(returned, expected)


def test_indexing_two_values_groupby_hardcoded_values():
    indexed_table = T(
        """
    colA   | colB
    10     | A
    20     | B
    """
    )
    indexed_table = indexed_table.groupby(pw.this.colA, pw.this.colB).reduce(*pw.this)
    tested_table = T(
        """
    colC
    10
    20
    """
    )
    returned = tested_table.select(
        *pw.this,
        new_value=indexed_table.ix_ref(10, "A").colA,
    )
    expected = T(
        """
    colC   | new_value
    10     | 10
    20     | 10
    """
    )
    assert_table_equality(returned, expected)


def test_ix_ref_with_primary_keys():
    indexed_table = T(
        """
    colA   | colB
    10     | A
    20     | B
    """
    )
    indexed_table = indexed_table.with_id_from(pw.this.colB)
    tested_table = T(
        """
    colC
    10
    20
    """
    )
    returned = tested_table.select(*pw.this, new_value=indexed_table.ix_ref("A").colA)
    expected = T(
        """
    colC   | new_value
    10     | 10
    20     | 10
    """
    )
    assert_table_equality(returned, expected)


def test_select_universes():
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
    ).promise_universe_is_subset_of(t1)

    t1_restricted = t1.restrict(t2)

    assert_table_equality(
        t2.select(t1_restricted.col),
        T(
            """
       | col
    2  | b
    3  | c
    """
        ),
    )

    with pytest.raises(ValueError):
        t1.select(t2.col)


@pytest.mark.xfail(
    reason="Columns from a superset of the current universe can't be used"
)
def test_select_op_universes():
    t1 = T(
        """
       | col
    1  | 11
    2  | 12
    3  | 13
    """
    )

    t2 = T(
        """
       | col
    2  | 1
    3  | 1
    """
    ).promise_universe_is_subset_of(t1)

    assert_table_equality(
        t2.select(col=t1.col + t2.col),
        T(
            """
       | col
    2  | 13
    3  | 14
    """
        ),
    )

    with pytest.raises(ValueError):
        t1.select(col=t1.col + t2.col)


def test_select_column_ix_args():
    t1 = T(
        """
      | a | b
    0 | 3 | 1
    1 | 4 | 2
    2 | 7 | 0
    """
    )
    expected = T(
        """
      | a | prev_a
    0 | 4 |   3
    1 | 7 |   4
    2 | 3 |   7
    """
    )
    t2 = t1.select(t1.ix(t1.pointer_from(t1.b)).a, prev_a=t1.a)
    assert_table_equality(t2, expected)


def test_select_in_multiple_independent_tables():
    t = T(
        """
         a  |  c  | b
        1.1 | 1.2 | 1
        2.0 | 2.3 | 2
        3.0 | 3.4 | 0
        4.0 | 4.5 | 3
        """
    )

    u = t.select(a=pw.this.a + pw.this.c, x=10)
    v = u.select(a=pw.this.a, x=20)
    t = t.select(pw.this.c, pw.this.b)
    t += v
    t += t.select(z=pw.this.a + pw.this.x, u=u.x)
    t = t.without(pw.this.b)

    expected = T(
        """
         c  |  a  |  x |   z  |  u
        1.2 | 2.3 | 20 | 22.3 | 10
        2.3 | 4.3 | 20 | 24.3 | 10
        3.4 | 6.4 | 20 | 26.4 | 10
        4.5 | 8.5 | 20 | 28.5 | 10
        """
    )

    assert_table_equality(t, expected)


def test_concat():
    t1 = T(
        """
    lower | upper
    a     | A
    b     | B
    """
    )
    t2 = T(
        """
    lower | upper
    c     | C
    """
    )

    res = pw.Table.concat_reindex(t1, t2)

    expected = T(
        """
    lower | upper
    a     | A
    b     | B
    c     | C
        """,
    )

    assert_table_equality_wo_index(res, expected)


def test_concat_reversed_columns():
    t1 = T(
        """
    lower | upper
    a     | A
    b     | B
    """
    )
    t2 = T(
        """
    upper | lower
    C     | c
    """
    )

    res = pw.Table.concat_reindex(t1, t2)

    expected = T(
        """
    lower | upper
    a     | A
    b     | B
    c     | C
        """,
    )

    assert_table_equality_wo_index(res, expected)


def test_concat_unsafe():
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

    pw.universes.promise_are_pairwise_disjoint(t1, t2)
    res = pw.Table.concat(t1, t2)

    expected = T(
        """
       | lower | upper
    1  | a     | A
    2  | b     | B
    3  | c     | C
        """,
    )
    assert_table_equality(res, expected)


def test_concat_unsafe_collision():
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
    1  | c     | C
    """
    )

    with pytest.raises(ValueError):
        pw.Table.concat(t1, t2)


def test_concat_errors_on_intersecting_universes():
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
    1  | c     | C
    """
    )

    pw.universes.promise_are_pairwise_disjoint(t1, t2)
    result = pw.Table.concat(t1, t2)
    with pytest.raises(
        KeyError,
        match=re.escape("duplicated entries for key ^YYY4HABTRW7T8VX2Q429ZYV70W"),
    ):
        pw.debug.compute_and_print(result)


@pytest.mark.parametrize("dtype", [np.int64, np.float64])
def test_flatten(dtype: Any):
    df = pd.DataFrame(
        {
            "array": [
                np.array([1, 2], dtype=dtype),
                np.array([], dtype=dtype),
                np.array([3, 4], dtype=dtype),
                np.array([10, 11, 12], dtype=dtype),
                np.array([4, 5, 6, 1, 2], dtype=dtype),
            ],
            "other": [-1, -2, -3, -4, -5],
        }
    )
    expected_df = pd.DataFrame(
        {
            "array": np.array([1, 2, 3, 4, 10, 11, 12, 4, 5, 6, 1, 2], dtype=dtype),
            "other": [-1, -1, -3, -3, -4, -4, -4, -5, -5, -5, -5, -5],
        }
    )
    t1 = table_from_pandas(df)
    t1 = t1.flatten(t1.array)
    expected = table_from_pandas(expected_df)
    assert_table_equality_wo_index(t1, expected)


@pytest.mark.parametrize("dtype", [np.int64, np.float64])
def test_flatten_multidimensional(dtype: Any):
    df = pd.DataFrame(
        {
            "array": [
                np.array([[1, 2], [3, 4]], dtype=dtype),
                np.array([[1, 2, 5, 6]], dtype=dtype),
            ]
        }
    )
    expected_rows = [
        np.array([1, 2], dtype=dtype),
        np.array([3, 4], dtype=dtype),
        np.array([1, 2, 5, 6], dtype=dtype),
    ]
    t = table_from_pandas(df)
    t = t.flatten(t.array)
    t_pandas = table_to_pandas(t)
    assert len(expected_rows) == len(t_pandas)
    for expected_row in expected_rows:
        found_equal = False
        for t_row in t_pandas.itertuples():
            if t_row.array.shape != expected_row.shape:
                continue
            if (t_row.array == expected_row).all():
                found_equal = True
                break
        assert found_equal


def test_flatten_string():
    df = pd.DataFrame({"string": ["abc", "defoimkm", "xyz"]})
    t1 = pw.debug.table_from_pandas(df)
    t1 = t1.flatten(t1.string)
    df_expected = pd.DataFrame({"string": list("abcdefoimkmxyz")})
    expected = table_from_pandas(df_expected)
    assert_table_equality_wo_index(t1, expected)


@pytest.mark.parametrize("mul", [1, -2])
@pytest.mark.parametrize("dtype", [np.int64, np.float64])
def test_flatten_explode(mul: int, dtype: Any):
    mul = dtype(mul)
    df = pd.DataFrame(
        {
            "array": [
                np.array([1, 2], dtype=dtype),
                np.array([], dtype=dtype),
                np.array([3, 4], dtype=dtype),
                np.array([10, 11, 12], dtype=dtype),
                np.array([4, 5, 6, 1, 2], dtype=dtype),
            ],
            "other": [-1, -2, -3, -4, -5],
        }
    )
    expected_df = pd.DataFrame(
        {
            "array": [1, 2, 3, 4, 10, 11, 12, 4, 5, 6, 1, 2],
            "other": np.array([-1, -1, -3, -3, -4, -4, -4, -5, -5, -5, -5, -5]) * mul,
        },
        dtype=dtype,
    )
    t1 = table_from_pandas(df)
    t1 = t1.flatten(t1.array).with_columns(
        other=mul * pw.cast({np.int64: int, np.float64: float}[dtype], pw.this.other),
    )
    expected = table_from_pandas(expected_df)
    assert_table_equality_wo_index(t1, expected)


def test_flatten_incorrect_type():
    t = T(
        """
      a | other
      1 | -1
      2 | -2
      3 | -3
    """
    )
    with pytest.raises(
        TypeError,
        match=re.escape("Cannot flatten column of type INT."),
    ):
        t = t.flatten(t.a)


def test_from_columns():
    first = T(
        """
    pet | owner | age
     1  | Alice | 10
     1  | Bob   | 9
     2  | Alice | 8
    """
    )
    second = T(
        """
    foo | aux | baz
    a   | 70  | a
    b   | 80  | c
    c   | 90  | b
    """
    )
    expected = T(
        """
    pet | foo
    1   | a
    1   | b
    2   | c
        """
    )
    assert_table_equality(pw.Table.from_columns(first.pet, second.foo), expected)


def test_from_columns_collision():
    first = T(
        """
    pet | owner | age
     1  | Alice | 10
     1  | Bob   | 9
     2  | Alice | 8
    """
    )
    with pytest.raises(ValueError):
        pw.Table.from_columns(first.pet, first.pet)


def test_from_columns_mismatched_keys():
    first = T(
        """
      | pet | owner | age
    1 |  1  | Alice | 10
    2 |  1  | Bob   | 9
    3 |  2  | Alice | 8
    """
    )
    second = T(
        """
      | foo | aux | baz
    1 | a   | 70  | a
    2 | b   | 80  | c
    4 | c   | 90  | b
    """
    )
    with pytest.raises(ValueError):
        pw.Table.from_columns(first.pet, second.foo)


def test_rename_columns_1():
    old = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
     1   | Bob     | 9
    """
    )

    expected = T(
        """
    owner   | animal | winters
    Alice   |  1     | 10
    Bob     |  1     | 9
    """
    )
    new = old.rename_columns(animal=old.pet, winters=old.age)
    assert_table_equality(new, expected)


def test_rename_columns_2():
    old = T(
        """
    pet | age
     1  | 10
     1  | 9
    """
    )
    expected = T(
        """
    age | pet
     1  | 10
     1  | 9
    """
    )
    new = old.rename_columns(age="pet", pet="age")
    assert_table_equality(new, expected)


def test_rename_by_dict():
    old = T(
        """
    t0  |  t1  | t2
     1   | Alice   | 10
     1   | Bob     | 9
    """
    )

    expected = T(
        """
    col_0  | col_1   | col_2
       1   | Alice   | 10
       1   | Bob     | 9
    """
    )
    new = old.rename_by_dict({f"t{i}": f"col_{i}" for i in range(3)})
    assert_table_equality(new, expected)


def test_rename_with_dict():
    old = T(
        """
    t0   |  t1     | t2
     1   | Alice   | 10
     1   | Bob     | 9
    """
    )
    mapping = {f"t{i}": f"col_{i}" for i in range(3)}
    new = old.rename(mapping)
    expected = old.rename_by_dict(mapping)
    assert_table_equality(new, expected)


def test_rename_with_kwargs():
    old = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
     1   | Bob     | 9
    """
    )

    new = old.rename(animal=old.pet, winters=old.age)
    expected = old.rename_columns(animal=old.pet, winters=old.age)
    assert_table_equality(new, expected)


def test_rename_columns_unknown_column_name():
    old = T(
        """
    pet |  owner  | age
     1  | Alice   | 10
     1  | Bob     | 9
    """
    )
    with pytest.raises(Exception):
        old.rename_columns(pet="animal", habitat="location")


def test_drop_columns():
    old = T(
        """
    pet | owner | age | weight
    1   | Bob   | 11  | 7
    1   | Eve   | 10  | 11
    2   | Eve   | 15  | 13
    """
    )
    new = old.without(old.pet, old.age, pw.this.owner)
    expected = T(
        """
    weight
    7
    11
    13
    """
    )
    assert_table_equality(new, expected)


def test_filter():
    t_latin = T(
        """
            | lower | upper
        1  | a     | A
        2  | b     | B
        26 | z     | Z
        """
    )
    t_tmp = T(
        """
            | bool
        1   | True
        2   | True
        26  | False
        """
    )

    res = t_latin.filter(t_tmp["bool"])

    assert_table_equality(
        res,
        T(
            """
                | lower | upper
            1  | a     | A
            2  | b     | B
            """
        ),
    )


def test_filter_no_columns():
    input = T(
        """
            |
        1   |
        2   |
        3   |
        """
    ).select()

    output = input.filter(input.id == input.id)

    assert_table_equality(
        output,
        T(
            """
                |
            1   |
            2   |
            3   |
            """
        ).select(),
    )


def test_filter_different_universe():
    t_latin = T(
        """
            | lower | upper
        1  | a     | A
        2  | b     | B
        26 | z     | Z
        """
    )
    t_wrong = T(
        """
            | bool
        1   | True
        7   | False
        """
    )

    with pytest.raises(ValueError):
        t_latin.filter(t_wrong.bool)


def test_reindex():
    t1 = T(
        """
            | col
        1   | 11
        2   | 12
        3   | 13
        """
    )
    t2 = T(
        """
       | new_id
    1  | 2
    2  | 3
    3  | 4
    """
    ).select(new_id=t1.pointer_from(pw.this.new_id))
    pw.universes.promise_is_subset_of(t1, t2)
    t2_restricted = t2.restrict(t1)
    assert_table_equality(
        t1.with_id(t2_restricted.new_id),
        T(
            """
                | col
            2   | 11
            3   | 12
            4   | 13
            """
        ),
    )
    with pytest.raises(TypeError):
        # must be column val
        t1.with_id(t1.id + 1),
    with pytest.raises(ValueError):
        # old style is not supported
        t1.select(id=t2_restricted.new_id),


def test_reindex_no_columns():
    t1 = T(
        """
            |
        1   |
        2   |
        3   |
        """
    ).select()
    t2 = T(
        """
            | new_id
        1   | 2
        2   | 3
        3   | 4
        """
    ).select(new_id=t1.pointer_from(pw.this.new_id))
    pw.universes.promise_is_subset_of(t1, t2)
    t2_restricted = t2.restrict(t1)

    assert_table_equality(
        t1.with_id(t2_restricted.new_id),
        T(
            """
                |
            2   |
            3   |
            4   |
            """
        ).select(),
    )


def test_column_fixpoint():
    def collatz_transformer(iterated):
        @pw.udf(deterministic=True)
        def collatz_step(x: float) -> float:
            if x == 1:
                return 1
            elif x % 2 == 0:
                return x / 2
            else:
                return 3 * x + 1

        return iterated.select(val=collatz_step(iterated.val))

    ret = pw.iterate(
        collatz_transformer,
        iterated=table_from_pandas(
            pd.DataFrame(index=range(1, 101), data={"val": np.arange(1.0, 101.0)})
        ),
    )
    expected_ret = table_from_pandas(
        pd.DataFrame(index=range(1, 101), data={"val": 1.0})
    )

    assert_table_equality(ret, expected_ret)


def test_rows_fixpoint():
    def min_id_remove(iterated: pw.Table):
        min_id_table = iterated.reduce(min_id=pw.reducers.min(iterated.id))
        return iterated.filter(iterated.id != min_id_table.ix_ref().min_id)

    ret = pw.iterate(
        min_id_remove,
        iterated=pw.iterate_universe(
            T(
                """
                | foo
            1   | 1
            2   | 2
            3   | 3
            4   | 4
            5   | 5
            """
            )
        ),
    )

    expected_ret = T(
        """
            | foo
        """
    ).update_types(foo=int)

    assert_table_equality_wo_index(ret, expected_ret)


def test_rows_fixpoint_needs_iterate_universe():
    def min_id_remove(iterated: pw.Table):
        min_id_table = iterated.reduce(min_id=pw.reducers.min(iterated.id))
        iterated = iterated.filter(
            iterated.id != min_id_table.ix(min_id_table.pointer_from()).min_id
        )
        return iterated

    with pytest.raises(ValueError):
        pw.iterate(
            min_id_remove,
            iterated=T(
                """
                    | foo
                1   | 1
                2   | 2
                3   | 3
                4   | 4
                5   | 5
                """
            ),
        )


def test_iteration_column_order():
    def iteration_step(iterated):
        iterated = iterated.select(bar=iterated.bar, foo=iterated.foo - iterated.foo)
        return iterated

    ret = pw.iterate(
        iteration_step,
        iterated=T(
            """
                | foo   | bar
            1   | 1     | None
            2   | 2     | None
            3   | 3     | None
            """
        ),
    )

    expected_ret = T(
        """
            | foo   | bar
        1   | 0     | None
        2   | 0     | None
        3   | 0     | None
        """
    )

    assert_table_equality_wo_index(ret, expected_ret)


@pytest.mark.parametrize("limit", [-1, 0])
def test_iterate_with_wrong_limit(limit):
    def iteration_step(iterated):
        iterated = iterated.select(foo=iterated.foo + 1)
        return iterated

    with pytest.raises(ValueError):
        pw.iterate(
            iteration_step,
            iteration_limit=limit,
            iterated=T(
                """
                    | foo
                1   | 0
                """
            ),
        )


@pytest.mark.parametrize("limit", [1, 2, 10])
def test_iterate_with_limit(limit):
    def iteration_step(iterated):
        iterated = iterated.select(foo=iterated.foo + 1)
        return iterated

    ret = pw.iterate(
        iteration_step,
        iteration_limit=limit,
        iterated=T(
            """
                | foo
            1   | 0
            """
        ),
    )

    expected_ret = T(
        f"""
            | foo
        1   | {limit}
        """
    )

    assert_table_equality(ret, expected_ret)


def test_iterate_with_same_universe_outside():
    t = T(
        """
        a | c | b
        1 | 0 | 4
        2 | 3 | 5
        3 | 4 | 7
        4 | 2 | 10
        """
    )

    u = t.select(x=pw.this.c * 3)

    def f(t: pw.Table):
        t = t.select(pw.this.b, a=pw.this.a * 2, c=pw.this.c * 2)
        return t

    t = pw.iterate(f, iteration_limit=2, t=t)

    t = t.select(pw.this.a, u.x)

    assert_table_equality(
        t,
        T(
            """
         a |  x
         4 |  0
         8 |  9
        12 | 12
        16 |  6
        """
        ),
    )


def test_iterate_with_diverging_columns():
    t = T(
        """
        a
        1
        """
    )

    t = t.select(pw.this.a, b=pw.this.a)

    def f(t: pw.Table):
        t = t.select(pw.this.a, b=pw.this.b * 2)
        return t

    t = pw.iterate(f, iteration_limit=2, t=t)

    assert_table_equality(
        t,
        T(
            """
         a | b
         1 | 4
        """
        ),
    )


def test_apply():
    a = T(
        """
        foo
        1
        2
        3
        """
    )

    def inc(x: int) -> int:
        return x + 1

    result = a.select(ret=pw.apply(inc, a.foo))

    assert_table_equality(
        result,
        T(
            """
            ret
            2
            3
            4
            """
        ),
    )


def test_apply_inspect_wrapped_signature():
    a = T(
        """
        foo
        1
        2
        3
        """
    )

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    @decorator
    def inc(x: int) -> int:
        return x + 1

    result = a.select(ret=pw.apply(inc, a.foo))

    assert_table_equality(
        result,
        T(
            """
            ret
            2
            3
            4
            """
        ),
    )


def test_apply_consts():
    a = T(
        """
        foo
        1
        2
        3
        """
    )

    def inc(x: int) -> int:
        return x + 1

    result = a.select(ret=pw.apply(inc, 1))

    assert_table_equality(
        result,
        T(
            """
            ret
            2
            2
            2
            """
        ),
    )


def test_apply_more_args():
    a = T(
        """
        foo
        1
        2
        3
        """
    )
    b = T(
        """
        bar
        2
        -1
        4
        """
    )

    def add(x: int, y: int) -> int:
        return x + y

    result = a.select(ret=pw.apply(add, x=a.foo, y=b.bar))

    assert_table_equality(
        result,
        T(
            """
            ret
            3
            1
            7
            """
        ),
    )


def test_apply_incompatible_keys():
    a = T(
        """
            | foo
        1   | 1
        2   | 2
        3   | 3
        """
    )
    b = T(
        """
            | bar
        1   | 2
        """
    )

    def add(x: float, y: float) -> float:
        return x + y

    with pytest.raises(ValueError):
        a.select(ret=pw.apply(add, x=a.foo, y=b.bar))


def test_apply_wrong_number_of_args():
    a = T(
        """
        foo
        1
        2
        """
    )

    def add(x: float, y: float) -> float:
        return x + y

    with pytest.raises(AssertionError):
        a.select(ret=pw.apply(add))


def test_apply_async():
    import asyncio

    async def inc(a: int) -> int:
        await asyncio.sleep(0.1)
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    result = input.select(ret=pw.apply_async(inc, pw.this.a))

    assert_table_equality(
        result,
        T(
            """
            ret
            2
            3
            4
            """,
        ),
    )


def test_apply_async_more_args():
    import asyncio

    async def add(a: int, b: int, *, c: int) -> int:
        await asyncio.sleep(0.1)
        return a + b + c

    input = pw.debug.table_from_markdown(
        """
        a | b  | c
        1 | 10 | 100
        2 | 20 | 200
        3 | 30 | 300
        """
    )

    result = input.select(ret=pw.apply_async(add, pw.this.a, pw.this.b, c=pw.this.c))

    assert_table_equality(
        result,
        T(
            """
            ret
            111
            222
            333
            """,
        ),
    )


def test_apply_async_wrong_args():
    import asyncio

    async def add(a: int, b: int, *, c: int) -> int:
        await asyncio.sleep(0.1)
        return a + b + c

    input = pw.debug.table_from_markdown(
        """
        a | b  | c
        1 | 10 | 100
        2 | 20 | 200
        3 | 30 | 300
        """
    )

    with pytest.raises(TypeError):
        result = input.select(ret=pw.apply_async(add, pw.this.a, pw.this.b, pw.this.c))

        assert_table_equality(
            result,
            T(
                """
                ret
                111
                222
                333
                """,
            ),
        )


def test_apply_async_coerce_async():
    a = T(
        """
        foo
        1
        2
        3
        """
    )

    def inc(x: int) -> int:
        return x + 1

    result = a.select(ret=pw.apply_async(inc, a.foo))

    assert_table_equality(
        result,
        T(
            """
            ret
            2
            3
            4
            """
        ),
    )


def test_apply_async_disk_cache(tmp_path: pathlib.Path):
    cache_dir = tmp_path / "test_cache"
    counter = mock.Mock()

    @pw.udfs.async_options(cache_strategy=pw.udfs.DiskCache())
    def inc(x: int) -> int:
        counter()
        return x + 1

    input = T(
        """
        foo
        1
        2
        3
        """
    )
    result = input.select(ret=pw.apply_async(inc, pw.this.foo))
    expected = T(
        """
        ret
        2
        3
        4
        """
    )

    # run twice to check if cache is used
    assert_table_equality(
        result,
        expected,
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(cache_dir),
        ),
    )
    assert_table_equality(
        result,
        expected,
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(cache_dir),
        ),
    )
    assert os.path.exists(cache_dir)
    assert counter.call_count == 3


def test_empty_join():
    left = T(
        """
                col | on
            1 | a   | 11
            2 | b   | 12
            3 | c   | 13
        """
    )
    right = T(
        """
                col | on
            1 | d   | 12
            2 | e   | 13
            3 | f   | 14
        """,
    )
    joined = left.join(right, left.on == right.on).select()
    assert_table_equality_wo_index(
        joined,
        T(
            """
                |
            2   |
            3   |
            """
        ).select(),
    )


def test_join_left_assign_id():
    left = T(
        """
                col | on
            1 | a   | 11
            2 | b   | 12
            3 | c   | 13
            4 | d   | 13
        """
    )
    right = T(
        """
                col | on
            1 | d   | 12
            2 | e   | 13
            3 | f   | 14
        """,
    )
    joined = left.join(right, left.on == right.on, id=left.id).select(
        lcol=left.col, rcol=right.col
    )

    assert_table_equality(
        joined,
        T(
            """
        | lcol | rcol
        2 |  b |    d
        3 |  c |    e
        4 |  d |    e
    """
        ),
    )

    with pytest.raises(AssertionError):
        left.join(right, left.on == right.on, id=left.on)

    left.join(right, left.on == right.on, id=right.id).select(
        lcol=left.col, rcol=right.col
    )
    with pytest.raises(KeyError):
        run_all()


def test_join_right_assign_id():
    left = T(
        """
                col | on
            1 | a   | 11
            2 | b   | 12
            3 | c   | 13
        """
    )
    right = T(
        """
                col | on
            0 | c   | 12
            1 | d   | 12
            2 | e   | 13
            3 | f   | 14
        """,
    )
    joined = left.join(right, left.on == right.on, id=right.id).select(
        lcol=left.col, rcol=right.col
    )
    assert_table_equality(
        joined,
        T(
            """
          | lcol | rcol
        0 |    b |    c
        1 |    b |    d
        2 |    c |    e
    """
        ),
    )

    with pytest.raises(AssertionError):
        left.join(right, left.on == right.on, id=right.on)

    left.join(right, left.on == right.on, id=left.id).select(
        lcol=left.col, rcol=right.col
    )
    with pytest.raises(KeyError):
        run_all()


def test_join():
    t1 = T(
        """
            | pet | owner | age
        1   |   1 | Alice |  10
        2   |   1 |   Bob |   9
        3   |   2 | Alice |   8
        """
    )
    t2 = T(
        """
            | pet | owner | age | size
        11  |   3 | Alice |  10 |    M
        12  |   1 |   Bob |   9 |    L
        13  |   1 |   Tom |   8 |   XL
        """
    )
    expected = T(
        """
            owner_name | L | R  | age
            Bob        | 2 | 12 |   9
            """,
    ).with_columns(
        L=t1.pointer_from(pw.this.L),
        R=t2.pointer_from(pw.this.R),
    )
    res = t1.join(t2, t1.pet == t2.pet, t1.owner == t2.owner).select(
        owner_name=t2.owner, L=t1.id, R=t2.id, age=t1.age
    )
    assert_table_equality_wo_index(
        res,
        expected,
    )


def test_join_instance():
    t1 = T(
        """
            | owner | age | instance
        1   | Alice |  10 | 1
        2   |   Bob |   9 | 1
        3   |   Tom |   8 | 1
        4   | Alice |  10 | 2
        5   |   Bob |   9 | 2
        6   |   Tom |   8 | 2
        """
    )
    t2 = T(
        """
            | owner | age | size | instance
        11  | Alice |  10 |    M | 1
        12  |   Bob |   9 |    L | 1
        13  |   Tom |   8 |   XL | 1
        14  | Alice |  10 |    M | 2
        15  |   Bob |   9 |    L | 2
        16  |   Tom |   8 |   XL | 2
        """
    )
    expected = T(
        """
            owner_name | L | R  | age
            Alice      | 1 | 11 |  10
            Bob        | 2 | 12 |   9
            Tom        | 3 | 13 |   8
            Alice      | 4 | 14 |  10
            Bob        | 5 | 15 |   9
            Tom        | 6 | 16 |   8
            """,
    ).with_columns(
        L=t1.pointer_from(pw.this.L),
        R=t2.pointer_from(pw.this.R),
    )
    res = t1.join(
        t2, t1.owner == t2.owner, left_instance=t1.instance, right_instance=t2.instance
    ).select(owner_name=t2.owner, L=t1.id, R=t2.id, age=t1.age)
    assert_table_equality_wo_index(
        res,
        expected,
    )


def test_join_swapped_condition():
    t1 = T(
        """
            | pet | owner | age
        1   |   1 | Alice |  10
        2   |   1 |   Bob |   9
        3   |   2 | Alice |   8
        """
    )
    t2 = T(
        """
            | pet | owner | age | size
        1   |   3 | Alice |  10 |    M
        2   |   1 |   Bob |   9 |    L
        3   |   1 |   Tom |   8 |   XL
        """
    )
    with pytest.raises(ValueError):
        t1.join(t2, t2.pet == t1.pet).select(
            owner_name=t2.owner, L=t1.id, R=t2.id, age=t1.age
        )


@pytest.mark.parametrize(
    "op",
    [
        operator.ne,
        operator.lt,
        operator.gt,
        operator.le,
        operator.ge,
    ],
)
def test_join_illegal_operator_in_condition(op):
    t1 = T(
        """
            | pet | owner | age
        1   |   1 | Alice |  10
        2   |   1 |   Bob |   9
        3   |   2 | Alice |   8
        """
    )
    t2 = T(
        """
            | pet | owner | age | size
        11  |   3 | Alice |  10 |    M
        12  |   1 |   Bob |   9 |    L
        13  |   1 |   Tom |   8 |   XL
        """
    )
    with pytest.raises(ValueError):
        t1.join(t2, op(t1.pet, t2.pet))


def test_join_default():
    t1 = T(
        """
            | pet | owner | age
        1   |   1 | Alice |  10
        2   |   1 |   Bob |   9
        3   |   2 | Alice |   8
        """
    )
    t2 = T(
        """
            | pet | owner | age | size
        11  |   3 | Alice |  10 |    M
        12  |   1 |   Bob |   9 |    L
        13  |   1 |   Tom |   8 |   XL
        """
    )
    res = t1.join(t2, t1.pet == t2.pet).select(
        owner_name=t2.owner, L=t1.id, R=t2.id, age=t1.age
    )
    expected = T(
        """
            owner_name  | L | R  | age
            Bob         | 1 | 12 | 10
            Tom         | 1 | 13 | 10
            Bob         | 2 | 12 |  9
            Tom         | 2 | 13 |  9
        """,
    ).with_columns(
        L=t1.pointer_from(pw.this.L),
        R=t2.pointer_from(pw.this.R),
    )

    assert_table_equality_wo_index(res, expected)


def test_join_self():
    input = T(
        """
        foo   | bar
        1     | 1
        1     | 2
        1     | 3
        """
    )
    with pytest.raises(Exception):
        input.join(input, input.foo == input.bar)


def test_join_select_no_columns():
    left = T(
        """
           | a
        1  | 1
        2  | 2
        """
    )
    right = T(
        """
           | b
        1  | foo
        2  | bar
        """
    )

    ret = left.join(right, left.id == right.id).select().select(col=42)
    assert_table_equality_wo_index(
        ret,
        T(
            """
                | col
            1   | 42
            2   | 42
            """
        ),
    )


def test_cross_join():
    t1 = T(
        """
            | pet | owner | age
        1   |   1 | Alice |  10
        2   |   1 |   Bob |   9
        3   |   2 | Alice |   8
        """
    )
    t2 = T(
        """
            | pet | owner | age | size
        11  |   3 | Alice |  10 |    M
        12  |   1 |   Bob |  9  |    L
        13  |   1 |   Tom |  8  |   XL
        """
    )
    res = t1.join(t2).select(owner_name=t2.owner, L=t1.id, R=t2.id, age=t1.age)
    expected = T(
        """
            owner_name  | L | R | age
            Alice       | 1 | 11 |  10
            Bob         | 1 | 12 |  10
            Tom         | 1 | 13 |  10
            Alice       | 2 | 11 |   9
            Bob         | 2 | 12 |   9
            Tom         | 2 | 13 |   9
            Alice       | 3 | 11 |   8
            Bob         | 3 | 12 |   8
            Tom         | 3 | 13 |   8
        """,
    ).with_columns(
        L=t1.pointer_from(pw.this.L),
        R=t2.pointer_from(pw.this.R),
    )
    assert_table_equality_wo_index(res, expected)


def test_empty_join_2():
    t1 = T(
        """
        v1
        1
        2
        """,
    )
    t2 = T(
        """
        v2
        10
        20
        """,
    )
    t = t1.join(t2).select(t1.v1, t2.v2)
    expected_t = T(
        """
        v1  | v2
        1   | 10
        1   | 20
        2   | 10
        2   | 20
        """,
    )
    assert_table_equality_wo_index(t, expected_t)


def test_ix():
    t_animals = T(
        """
            | genus      | epithet
        1   | upupa      | epops
        2   | acherontia | atropos
        3   | bubo       | scandiacus
        4   | dynastes   | hercules
        """
    )
    t_birds = T(
        """
            | desc   | ptr
        1   | hoopoe | 2
        2   | owl    | 4
        """
    ).with_columns(ptr=t_animals.pointer_from(pw.this.ptr))

    res = t_birds.select(latin=t_animals.ix(t_birds.ptr).genus)
    expected = T(
        """
            | latin
        1   | acherontia
        2   | dynastes
        """
    )
    assert_table_equality(res, expected)


def test_ix_none():
    t_animals = T(
        """
            | genus      | epithet
        1   | upupa      | epops
        2   | acherontia | atropos
        3   | bubo       | scandiacus
        4   | dynastes   | hercules
        """
    )
    t_birds = T(
        """
            | desc   | ptr
        1   | hoopoe | 2
        2   | owl    | 4
        3   | brbrb  |
        """
    ).with_columns(ptr=t_animals.pointer_from(pw.this.ptr, optional=True))

    res = t_birds.select(latin=t_animals.ix(t_birds.ptr, optional=True).genus)
    expected = T(
        """
            | latin
        1   | acherontia
        2   | dynastes
        3   |
        """
    )
    assert_table_equality(res, expected)


def test_ix_this_getitem():
    t_animals = T(
        """
            | genus      | epithet
        1   | upupa      | epops
        2   | acherontia | atropos
        3   | bubo       | scandiacus
        4   | dynastes   | hercules
        """
    )
    t_birds = T(
        """
            | desc   | ptr
        1   | hoopoe | 2
        2   | owl    | 4
        """
    ).with_columns(ptr=t_animals.pointer_from(pw.this.ptr))

    res = t_birds.select(*(t_animals.ix(pw.this.ptr)[["genus", "epithet"]]))

    expected = T(
        """
            | genus         | epithet
        1   | acherontia    | atropos
        2   | dynastes      | hercules
        """
    )
    assert_table_equality(res, expected)


def test_ix_missing_key():
    t_animals = T(
        """
            | genus      | epithet
        1   | upupa      | epops
        2   | acherontia | atropos
        """
    )
    t_birds = T(
        """
            | desc   | ptr
        1   | hoopoe | 1
        2   | owl    | 3
        """
    ).with_columns(ptr=t_animals.pointer_from(pw.this.ptr))
    t_birds.select(latin=t_animals.ix(t_birds.ptr).genus)
    with pytest.raises(KeyError):
        run_all()


def test_ix_none_in_source():
    t_animals = T(
        """
            | genus      | epithet
        1   | upupa      | epops
        2   | acherontia | atropos
        3   | bubo       | scandiacus
        4   |            | hercules
        """
    )
    t_birds = T(
        """
            | desc   | ptr
        1   | hoopoe | 2
        2   | owl    | 4
        """
    ).with_columns(ptr=t_animals.pointer_from(pw.this.ptr))

    res = t_birds.select(latin=t_animals.ix(t_birds.ptr).genus)
    expected = T(
        """
            | latin
        1   | acherontia
        2   |
        """
    )
    assert_table_equality(res, expected)


def test_ix_no_select():
    input = T(
        """
            | foo   | bar
        1   | 1     | 4
        2   | 1     | 5
        3   | 2     | 6
        """
    ).with_columns(foo=pw.this.pointer_from(pw.this.foo))

    result = input.ix(input.foo)[["bar"]]

    assert_table_equality(
        result,
        T(
            """
                | bar
            1   | 4
            2   | 4
            3   | 5
            """
        ),
    )


def test_ix_self_select():
    input = T(
        """
            | foo   | bar
        1   | 1     | 4
        2   | 1     | 5
        3   | 2     | 6
        """
    ).with_columns(foo=pw.this.pointer_from(pw.this.foo))

    result = input.select(result=input.ix(pw.this.foo).bar)

    assert_table_equality(
        result,
        T(
            """
                | result
            1   | 4
            2   | 4
            3   | 5
            """
        ),
    )


def test_ix_sort_1():
    data = T(
        """
        a | t
        0 | 1
        0 | 2
        0 | 3
        1 | 1
        1 | 2
    """
    )
    data_prev_next = data.sort(key=pw.this.t, instance=pw.this.a)
    data_prev = data.ix(data_prev_next.prev, optional=True)
    data_next = data.ix(data_prev_next.next, optional=True)
    result = data.select(pw.this.a, pw.this.t, prev_t=data_prev.t, next_t=data_next.t)
    expected = T(
        """
        a | t | prev_t | next_t
        0 | 1 |        |    2
        0 | 2 |    1   |    3
        0 | 3 |    2   |
        1 | 1 |        |    2
        1 | 2 |    1   |
    """
    )
    assert_table_equality(result, expected)


def test_ix_sort_2():
    data = T(
        """
        a | t
        0 | 1
        0 | 2
        0 | 3
        1 | 1
        1 | 2
    """
    )
    data += data.sort(key=pw.this.t, instance=pw.this.a)
    data_prev = data.ix(data.prev, optional=True)
    data_next = data.ix(data.next, optional=True)
    result = data.select(pw.this.a, pw.this.t, prev_t=data_prev.t, next_t=data_next.t)
    expected = T(
        """
        a | t | prev_t | next_t
        0 | 1 |        |    2
        0 | 2 |    1   |    3
        0 | 3 |    2   |
        1 | 1 |        |    2
        1 | 2 |    1   |
    """
    )
    assert_table_equality(result, expected)


def test_groupby_ix_this():
    left = T(
        """
    pet  |  owner  | age
    dog  | Alice   | 10
    dog  | Bob     | 9
    cat  | Alice   | 8
    cat  | Bob     | 7
    """
    )

    res = left.groupby(left.pet).reduce(
        age=pw.reducers.max(pw.this.age),
        owner=pw.this.ix(pw.reducers.argmax(pw.this.age)).owner,
    )

    assert_table_equality_wo_index(
        res,
        T(
            """
        age | owner
        10  | Alice
        8   | Alice
    """
        ),
    )


def test_groupby_simplest():
    left = T(
        """
    pet  |  owner  | age
    dog  | Alice   | 10
    dog  | Bob     | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    """
    )

    left_res = left.groupby(left.pet).reduce(left.pet)

    assert_table_equality_wo_index(
        left_res,
        T(
            """
        pet
        dog
        cat
    """
        ),
    )


def test_groupby_singlecol():
    left = T(
        """
    pet  |  owner  | age
    dog  | Alice   | 10
    dog  | Bob     | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    """
    )

    left_res = left.groupby(left.pet).reduce(left.pet, ageagg=pw.reducers.sum(left.age))

    assert_table_equality_wo_index(
        left_res,
        T(
            """
        pet  | ageagg
        dog  | 26
        cat  | 8
    """
        ),
    )


def test_groupby_int_sum():
    left = T(
        """
    owner   | val
    Alice   | 1
    Alice   | -1
    Bob     | 0
    Bob     | 0
    Charlie | 1
    Charlie | 0
    Dee     | 5
    Dee     | 5
    """
    )

    left_res = left.groupby(left.owner).reduce(
        left.owner, val=pw.reducers.sum(left.val)
    )

    assert_table_equality_wo_index(
        left_res,
        T(
            """
        owner   | val
        Alice   | 0
        Bob     | 0
        Charlie | 1
        Dee     | 10
    """
        ),
    )


def test_groupby_filter_singlecol():
    left = T(
        """
      pet  |  owner  | age
      dog  | Alice   | 10
      dog  | Bob     | 9
      cat  | Alice   | 8
      dog  | Bob     | 7
      cat  | Alice   | 6
      dog  | Bob     | 5
    """
    )

    left_res = (
        left.filter(left.age > 6)
        .groupby(pw.this.pet)
        .reduce(pw.this.pet, ageagg=pw.reducers.sum(pw.this.age))
    )

    assert_table_equality_wo_index(
        left_res,
        T(
            """
        pet  | ageagg
        dog  | 26
        cat  | 8
    """
        ),
    )


@pytest.mark.xfail(reason="References from universe superset are not allowed.")
def test_groupby_universes():
    left = T(
        """
      | pet  |  owner
    1 | dog  | Alice
    2 | dog  | Bob
    3 | cat  | Alice
    4 | dog  | Bob
    """
    )

    left_prim = T(
        """
      | age
    1 | 10
    2 | 9
    3 | 8
    4 | 7
    5 | 6
    """
    )

    left_bis = T(
        """
      | age
    1 | 10
    2 | 9
    3 | 8
    """
    )
    pw.universes.promise_is_subset_of(left, left_prim)

    left_res = left.groupby(left.pet).reduce(
        left.pet, ageagg=pw.reducers.sum(left_prim.age)
    )

    assert_table_equality_wo_index(
        left_res,
        T(
            """
    pet  | ageagg
    dog  | 26
    cat  | 8
    """
        ),
    )

    with pytest.raises(AssertionError):
        left.groupby(left.pet).reduce(ageagg=pw.reducers.sum(left_bis.age))


def test_groupby_reducer_on_expression():
    left = T(
        """
    pet  |  owner  | age
    dog  | Alice   | 10
    dog  | Bob     | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    """
    )

    left_res = left.groupby(left.pet).reduce(
        left.pet, ageagg=pw.reducers.min(left.age + left.age)
    )

    assert_table_equality_wo_index(
        left_res,
        T(
            """
    pet  | ageagg
    dog  | 14
    cat  | 16
    """
        ),
    )


def test_groupby_expression_on_reducers():
    left = T(
        """
    pet  |  owner  | age
    dog  | Alice   | 10
    dog  | Bob     | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    """
    )

    left_res = left.groupby(left.pet).reduce(
        left.pet,
        ageagg=pw.reducers.min(left.age) + pw.reducers.sum(left.age),
    )
    assert_table_equality_wo_index(
        left_res,
        T(
            """
    pet  | ageagg
    dog  | 33
    cat  | 16
    """
        ),
    )


def test_groupby_reduce_no_columns():
    input = T(
        """
        a
        1
        2
        """
    )

    ret = input.reduce().select(col=42)

    assert_table_equality_wo_index(
        ret,
        T(
            """
            col
            42
            """
        ),
    )


def test_groupby_mutlicol():
    left = T(
        """
    pet  |  owner  | age
    dog  | Alice   | 10
    dog  | Bob     | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    """
    )

    left_res = left.groupby(left.pet, left.owner).reduce(
        left.pet, left.owner, ageagg=pw.reducers.sum(left.age)
    )

    assert_table_equality_wo_index(
        left_res,
        T(
            """
    pet  |  owner  | ageagg
    dog  | Alice   | 10
    dog  | Bob     | 16
    cat  | Alice   | 8
    """
        ),
    )


def test_groupby_mix_key_val():
    left = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
     1   | Bob     | 9
     2   | Alice   | 8
     1   | Bob     | 7
    """
    )

    left_res = left.groupby(left.pet).reduce(
        left.pet, ageagg=pw.reducers.min(left.age + left.pet)
    )

    right = T(
        """
        pet | ageagg
        1   |      8
        2   |     10
        """
    )
    assert_table_equality_wo_index(left_res, right)


def test_groupby_mix_key_val2():
    left = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
     1   | Bob     | 9
     2   | Alice   | 8
     1   | Bob     | 7
    """
    )

    right = T(
        """
          | pet | ageagg
        1 | 1   |      8
        2 | 2   |     10
        """
    )
    res = right.with_id_from(right.pet)

    assert_table_equality(
        res,
        left.groupby(left.pet).reduce(
            left.pet, ageagg=pw.reducers.min(left.age) + left.pet
        ),
    )

    assert_table_equality(
        res,
        left.groupby(left.pet).reduce(
            left.pet, ageagg=pw.reducers.min(left.age + left.pet)
        ),
    )


def test_groupby_key_expressions():
    left = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
     1   | Bob     | 9
     2   | Alice   | 8
     1   | Bob     | 7
    """
    )

    right = T(
        """
        pet  | pet2
        1    | 1
        2    | 2
        """
    )
    res = right.with_id_from(right.pet)

    assert_table_equality(res, left.groupby(left.pet).reduce(left.pet, pet2=left.pet))

    with pytest.raises(Exception):
        left.groupby(left.pet).reduce(age2=left.age)


def test_groupby_setid():
    left = T(
        """
      | pet  |  owner  | age
    1 |  1   | Alice   | 10
    2 |  1   | Bob     | 9
    3 |  2   | Alice   | 8
    4 |  1   | Bob     | 7
    """
    ).with_columns(pet=pw.this.pointer_from(pw.this.pet))

    res = left.groupby(id=left.pet).reduce(
        left.pet,
        agesum=pw.reducers.sum(left.age),
    )

    expected = T(
        """
          | pet | agesum
        1 | 1   | 26
        2 | 2   | 8
        """
    ).with_columns(pet=left.pointer_from(pw.this.pet))

    assert_table_equality(res, expected)

    with pytest.raises(Exception):
        left.groupby(id=left.pet + 1).reduce(age2=left.age)


def test_groupby_similar_tables():
    a = T(
        """
            | pet  |  owner  | age
        1   | dog  | Alice   | 10
        2   | dog  | Bob     | 9
        3   | cat  | Alice   | 8
        4   | dog  | Bob     | 7
        """
    )
    b = a.select(*pw.this)

    r1 = a.groupby(b.pet).reduce(
        a.pet, agemin=pw.reducers.min(a.age), agemax=pw.reducers.max(b.age)
    )
    r2 = b.groupby(a.pet).reduce(
        b.pet, agemin=pw.reducers.min(b.age), agemax=pw.reducers.max(a.age)
    )

    expected = T(
        """
        pet | agemin | agemax
        cat | 8      | 8
        dog | 7      | 10
        """,
        id_from=["pet"],
    )

    assert_table_equality(r1, expected)
    assert_table_equality(r2, expected)


def test_argmin_argmax_tie():
    table = T(
        """
       name   | age
      Charlie |  18
      Alice   |  18
      Bob     |  18
      David   |  19
      Erin    |  19
      Frank   |  20
    """,
        unsafe_trusted_ids=True,
    )

    res = table.groupby(table.age).reduce(
        table.age,
        min=table.ix(pw.reducers.argmin(table.age), context=pw.this).name,
        max=table.ix(pw.reducers.argmax(table.age), context=pw.this).name,
    )

    expected = T(
        """
        age |     min |     max
         18 | Charlie | Charlie
         19 | David   | David
         20 | Frank   | Frank
        """
    )

    assert_table_equality_wo_index(res, expected)


def test_avg_reducer():
    t1 = T(
        """
    owner   | age
    Alice   | 10
    Bob     | 5
    Alice   | 20
    Bob     | 10
    """
    )
    res = t1.groupby(pw.this.owner).reduce(
        pw.this.owner, avg=pw.reducers.avg(pw.this.age)
    )

    expected = T(
        """
     owner  | avg
     Alice  | 15
     Bob    | 7.5
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_npsum_reducer_ints():
    t = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "data": [
                    np.array([1, 2, 3]),
                    np.array([4, 5, 6]),
                    np.array([7, 8, 9]),
                ]
            }
        )
    )
    result = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "sum": [
                    np.array([12, 15, 18]),
                ]
            }
        )
    )
    assert_table_equality_wo_index(t.reduce(sum=pw.reducers.sum(pw.this.data)), result)


def test_npsum_reducer_floats():
    t = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "data": [
                    np.array([1.1, 2.1, 3.1]),
                    np.array([4.1, 5.1, 6.1]),
                    np.array([7.1, 8.1, 9.1]),
                ]
            }
        )
    )
    result = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "sum": [
                    np.array([12.3, 15.3, 18.3]),
                ]
            }
        )
    )
    assert_table_equality_wo_index(t.reduce(sum=pw.reducers.sum(pw.this.data)), result)


def test_ndarray_reducer():
    t = pw.debug.table_from_markdown(
        """
       | colA | colB
    3  | valA | -1
    2  | valA | 1
    5  | valA | 2
    4  | valB | 4
    6  | valB | 4
    1  | valB | 7
    """
    )

    expected = pw.debug.table_from_pandas(
        pd.DataFrame({"tuple": [np.array([1, -1, 2]), np.array([7, 4, 4])]})
    )

    res = t.groupby(t.colA).reduce(tuple=pw.reducers.ndarray(t.colB))
    assert_table_equality_wo_index(res, expected)


def test_ndarray_reducer_on_ndarrays():
    t = pw.debug.table_from_markdown(
        """
        a | b | val
        0 | 0 | 1
        0 | 0 | 2
        0 | 1 | 3
        0 | 1 | 4
        1 | 0 | 5
        1 | 0 | 6
        1 | 0 | 7
        1 | 1 | 8
        1 | 1 | 9
        1 | 1 | 0
    """
    )
    s = t.groupby(pw.this.a, pw.this.b, sort_by=pw.this.val).reduce(
        pw.this.a, val=pw.reducers.ndarray(pw.this.val)
    )
    res = s.groupby(pw.this.a, sort_by=pw.this.val).reduce(
        pw.this.a, val=pw.reducers.ndarray(pw.this.val)
    )
    expected = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "a": [0, 1],
                "val": [np.array([[1, 2], [3, 4]]), np.array([[0, 8, 9], [5, 6, 7]])],
            }
        )
    )
    assert_table_equality_wo_index(res, expected)


def test_earliest_and_latest_reducer():
    t = T(
        """
        a | b | __time__
        1 | 2 |     2
        2 | 3 |     2
        1 | 4 |     4
        2 | 2 |     6
        1 | 1 |     8
    """
    )
    res = t.groupby(pw.this.a).reduce(
        pw.this.a,
        earliest=pw.reducers.earliest(pw.this.b),
        latest=pw.reducers.latest(pw.this.b),
    )
    expected = T(
        """
        a | earliest | latest | __time__ | __diff__
        1 |     2    |    2   |     2    |     1
        2 |     3    |    3   |     2    |     1
        1 |     2    |    2   |     4    |    -1
        1 |     2    |    4   |     4    |     1
        2 |     3    |    3   |     6    |    -1
        2 |     3    |    2   |     6    |     1
        1 |     2    |    4   |     8    |    -1
        1 |     2    |    1   |     8    |     1
    """,
        id_from=["a"],
    )
    assert_stream_equality(res, expected)


def test_earliest_and_latest_reducer_tie():
    t = T(
        """
        a
        1
        2
        3
    """
    )
    res = t.reduce(
        earliest=pw.reducers.earliest(pw.this.a), latest=pw.reducers.latest(pw.this.a)
    )
    expected = T(
        """
        earliest | latest
            2    |    1
    """
    )  # values from the row with the lowest key and greatest key respectively
    assert_table_equality_wo_index(res, expected)


def test_difference():
    t1 = T(
        """
            | col
        1   | 11
        2   | 12
        3   | 13
        """
    )
    t2 = T(
        """
            | col
        2   | 11
        3   | 11
        4   | 11
        """
    )

    assert_table_equality(
        t1.difference(t2),
        T(
            """
                | col
            1   | 11
            """
        ),
    )


def test_intersect():
    t1 = T(
        """
            | col
        1   | 11
        2   | 12
        3   | 13
        """
    )
    t2 = T(
        """
            | col
        2   | 11
        3   | 11
        4   | 11
        """
    )

    assert_table_equality(
        t1.intersect(t2),
        T(
            """
                | col
            2   | 12
            3   | 13
            """
        ),
    )


def test_intersect_empty():
    t1 = T(
        """
            | col
        1   | 11
        2   | 12
        3   | 13
        """
    )
    ret = t1.intersect()

    assert_table_equality(
        ret,
        t1,
    )
    assert ret == t1


def test_intersect_many_tables():
    t1 = T(
        """
            | col
        1   | 11
        2   | 12
        3   | 13
        4   | 14
        """
    )
    t2 = T(
        """
            | col
        2   | 11
        3   | 11
        4   | 11
        5   | 11
        """
    )
    t3 = T(
        """
            | col
        1   | 11
        3   | 11
        4   | 11
        5   | 11
        """
    )

    assert_table_equality(
        t1.intersect(t2, t3),
        T(
            """
                | col
            3   | 13
            4   | 14
            """
        ),
    )


def test_intersect_no_columns():
    t1 = T(
        """
            |
        1   |
        2   |
        3   |
        """
    ).select()
    t2 = T(
        """
            |
        2   |
        3   |
        4   |
        """
    ).select()

    assert_table_equality(
        t1.intersect(t2),
        T(
            """
                |
            2   |
            3   |
            """
        ).select(),
    )


def test_intersect_subset():
    t1 = T(
        """
            | col
        1   | 11
        2   | 12
        3   | 13
        """
    )
    t2 = T(
        """
            | col
        2   | 11
        3   | 11
        """
    )
    pw.universes.promise_is_subset_of(t2, t1)

    res = t1.intersect(t2)

    assert_table_equality(
        res,
        T(
            """
                | col
            2   | 12
            3   | 13
            """
        ),
    )
    assert res._universe != t2._universe


def test_update_cells():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Bob     | 7
        """
    )
    update = T(
        """
            | owner  | age
        1   | Eve    | 10
        4   | Eve    | 3
        """
    )
    expected = T(
        """
            | pet  |  owner  | age
        1   |  1   | Eve     | 10
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Eve     | 3
        """
    )
    pw.universes.promise_is_subset_of(update, old)

    new = old.update_cells(update)
    assert_table_equality(new, expected)
    assert_table_equality(old << update, expected)


def test_update_cells_0_rows():
    old = T(
        """
            | pet  |  owner  | age
        """
    )
    update = T(
        """
            | owner  | age
        """
    )
    expected = T(
        """
            | pet  |  owner  | age
        """
    )

    match = re.escape(
        "Key sets of self and other in update_cells are the same. "
        "Using with_columns instead of update_cells."
    )

    with warns_here(match=match):
        new = old.update_cells(update)
    with warns_here(match=match):
        new2 = old << update
    assert_table_equality(new, expected)
    assert_table_equality(new2, expected)


def test_update_cells_ids_dont_match():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Bob     | 7
        """
    )
    update = T(
        """
            | pet  |  owner  | age
        5   |  0   | Eve     | 10
        """
    )
    with pytest.raises(Exception):
        old.update_cells(update)


def test_update_cells_warns_when_using_with_columns():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        4   |  1   | Bob     | 7
        """
    )
    update = T(
        """
            | owner  | age
        1   | Eve    | 10
        4   | Eve    | 3
        """
    )
    expected = T(
        """
            | pet  |  owner  | age
        1   |  1   | Eve     | 10
        4   |  1   | Eve     | 3
        """
    )

    with warns_here(
        match=re.escape(
            "Key sets of self and other in update_cells are the same. "
            "Using with_columns instead of update_cells."
        ),
    ):
        new = old.update_cells(update)
        new2 = old << update
    assert_table_equality(new, expected)
    assert_table_equality(new2, expected)


def test_update_cells_with_broken_promises():
    tab1 = T(
        """
            | a | b
        1   | 1 | 1
        2   | 2 | 1
        3   | 6 | 1
        4   | 3 | 1
        5   | 9 | 1
        """
    )
    tab2 = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 6
        4   | 3
        6   | 9
        """
    )
    tab1.update_cells(tab2.promise_universe_is_subset_of(tab1))
    with pytest.raises(Exception):
        run_all()


def test_update_cells_with_broken_promises_2():
    tab1 = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 6
        4   | 3
        5   | 9
        """
    )
    tab2 = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 6
        4   | 3
        6   | 9
        """
    )
    tab1.update_cells(tab2.promise_universe_is_subset_of(tab1))
    with pytest.raises(Exception):
        run_all()


def test_update_rows():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Bob     | 7
        """
    )
    update = T(
        """
            | pet |  owner  | age
        1   | 7   | Bob     | 11
        5   | 0   | Eve     | 10
        """
    )
    expected = T(
        """
            | pet  |  owner  | age
        1   |  7   | Bob     | 11
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Bob     | 7
        5   |  0   | Eve     | 10
        """
    )

    new = old.update_rows(update)
    assert_table_equality(new, expected)


def test_update_rows_no_columns():
    old = T(
        """
            |
        1   |
        2   |
        3   |
        4   |
        """
    ).select()
    update = T(
        """
            |
        1   |
        5   |
        """
    ).select()
    expected = T(
        """
            |
        1   |
        2   |
        3   |
        4   |
        5   |
        """
    ).select()
    new = old.update_rows(update)
    assert_table_equality(new, expected)


def test_update_rows_0_rows():
    old = T(
        """
            | pet  |  owner  | age
        """
    )
    update = T(
        """
            | pet |  owner  | age
        """
    )

    expected = T(
        """
            | pet  |  owner  | age
        """
    )
    with warns_here(
        match=re.escape(
            "Universe of self is a subset of universe of other in update_rows. "
            "Returning other."
        ),
    ):
        new = old.update_rows(update)
    assert_table_equality(new, expected)


def test_update_rows_columns_dont_match():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Bob     | 7
        """
    )
    update = T(
        """
            | pet  |  owner  | age | weight
        5   |  0   | Eve     | 10  | 42
        """
    )
    with pytest.raises(Exception):
        old.update_rows(update)


def test_update_rows_subset():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Bob     | 7
        """
    )
    update = T(
        """
            | pet |  owner  | age
        1   | 7   | Bob     | 11
        """
    )
    pw.universes.promise_is_subset_of(update, old)
    expected = T(
        """
            | pet  |  owner  | age
        1   |  7   | Bob     | 11
        2   |  1   | Bob     | 9
        3   |  2   | Alice   | 8
        4   |  1   | Bob     | 7
        """
    )

    new = old.update_rows(update)
    assert_table_equality(new, expected)
    assert new._universe == old._universe


def test_update_rows_warns_when_nooperation():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        2   |  1   | Bob     | 9
        """
    )
    update = T(
        """
            | pet |  owner  | age
        1   |  7  | Bob     | 11
        2   |  1  | Alice   | 15
        5   | 0   | Eve     | 10
        """
    )
    pw.universes.promise_is_subset_of(old, update)
    expected = T(
        """
            | pet |  owner  | age
        1   |  7  | Bob     | 11
        2   |  1  | Alice   | 15
        5   |  0  | Eve     | 10
        """
    )

    with warns_here(
        match=re.escape(
            "Universe of self is a subset of universe of other in update_rows. "
            "Returning other."
        ),
    ):
        new = old.update_rows(update)
    assert_table_equality(new, expected)
    assert new._universe == update._universe


def test_with_columns():
    old = T(
        """
            | pet | owner | age
        1   |  1  | Alice | 10
        2   |  1  | Bob   | 9
        3   |  2  | Alice | 8
        """
    )
    update = T(
        """
            | owner | age | weight
        1   | Bob   | 11  | 7
        2   | Eve   | 10  | 11
        3   | Eve   | 15  | 13
        """
    )
    expected = T(
        """
            | pet | owner | age | weight
        1   | 1   | Bob   | 11  | 7
        2   | 1   | Eve   | 10  | 11
        3   | 2   | Eve   | 15  | 13
        """
    )

    new = old.with_columns(*update)
    assert_table_equality(new, expected)


def test_with_columns_0_rows():
    old = T(
        """
            | pet | owner | age
        """
    )
    update = T(
        """
            | owner | age | weight
        """
    )
    expected = T(
        """
            | pet | owner | age | weight
        """
    )

    assert_table_equality(old.with_columns(**update), expected)


def test_with_columns_ix_args():
    t1 = T(
        """
      | a | b
    0 | 3 | 1
    1 | 4 | 2
    2 | 7 | 0
    """
    )
    t2 = T(
        """
      |  c
    0 | 10
    1 | 20
    2 | 30
    """
    )
    expected = T(
        """
      | a | b |  c
    0 | 3 | 1 | 20
    1 | 4 | 2 | 30
    2 | 7 | 0 | 10
    """
    )
    t3 = t1.with_columns(t2.ix(t2.pointer_from(t1.b)).c)
    assert_table_equality(t3, expected)


def test_with_columns_ids_dont_match():
    old = T(
        """
            | pet  |  owner  | age
        1   |  1   | Alice   | 10
        2   |  1   | Bob     | 9
        """
    )
    update = T(
        """
            | pet  |  owner  | age
        5   |  0   | Eve     | 10
        """
    )
    with pytest.raises(Exception):
        old.with_columns(update)


def test_groupby_ix():
    tab = T(
        """
        grouper | val | output
              0 |   1 |    abc
              0 |   2 |    def
              1 |   1 |    ghi
              1 |   2 |    jkl
              2 |   1 |    mno
              2 |   2 |    pqr
        """,
    ).with_columns(grouper=pw.this.pointer_from(pw.this.grouper))
    res = tab.groupby(id=tab.grouper).reduce(
        col=pw.reducers.argmax(tab.val),
        output=tab.ix(pw.reducers.argmax(tab.val), context=pw.this).output,
    )
    expected = T(
        """
        col | output
          1 | def
          3 | jkl
          5 | pqr
        """,
    ).with_columns(col=tab.pointer_from(pw.this.col))
    assert_table_equality(res, expected)


@pytest.mark.xfail(
    reason="Foreign columns are not supported in reduce because their universe is different."
)
def test_groupby_foreign_column():
    tab = T(
        """
        grouper | col
              0 |   1
              0 |   2
              1 |   3
              1 |   4
              2 |   5
              2 |   6
        """,
    ).with_columns(grouper=pw.this.pointer_from(pw.this.grouper))
    tab2 = tab.select(tab.col)
    grouped = tab.groupby(id=tab.grouper)
    reduced1 = grouped.reduce(
        col=pw.reducers.sum(tab.col),
    )
    reduced2 = grouped.reduce(col=reduced1.col + pw.reducers.sum(tab2.col))
    assert_table_equality_wo_index(
        reduced2,
        T(
            """
            col
            6
            14
            22
            """,
        ),
    )


def test_groupby_instance():
    t = T(
        """
        a | b | col
        0 | 0 |   1
        0 | 0 |   2
        1 | 0 |   3
        1 | 0 |   4
        0 | 1 |   5
        0 | 1 |   6
        """
    )
    expected = T(
        """
        a | b | col
        0 | 0 |   3
        1 | 0 |   7
        0 | 1 |  11
        """
    ).with_id_from(pw.this.b, instance=pw.this.a)

    res = t.groupby(pw.this.b, instance=pw.this.a).reduce(
        pw.this.a, pw.this.b, col=pw.reducers.sum(pw.this.col)
    )
    assert_table_equality(res, expected)


def test_join_ix():
    left = T(
        """
           | a
        1  | 3
        2  | 2
        3  | 1
        """
    ).with_columns(a=pw.this.pointer_from(pw.this.a))
    right = T(
        """
           | b
        0  | baz
        1  | foo
        2  | bar
        """
    )

    ret = left.join(right, left.a == right.id, id=left.id).select(
        col=right.ix(left.a, context=pw.this).b
    )

    ret3 = (
        right.ix(left.a, allow_misses=True)
        .select(col=pw.this.b)
        .filter(pw.this.col.is_not_none())
    )

    # below is the desugared version of above computation
    # it works, and it's magic
    keys_table = left.join(right, left.a == right.id, id=left.id).select(
        join_column=left.a
    )
    desugared_ix = keys_table.join(
        right,
        keys_table.join_column == right.id,
        id=keys_table.id,
    ).select(right.b)
    tmp = left.join(
        right, left.a == right.id, id=left.id
    ).promise_universe_is_subset_of(desugared_ix)
    ret2 = tmp.select(col=desugared_ix.restrict(tmp).b)
    assert_table_equality(
        ret,
        T(
            """
                | col
            3   | foo
            2   | bar
            """
        ),
    )
    assert_table_equality(ret2, ret)
    assert_table_equality(ret3, ret)


def test_join_foreign_col():
    left = T(
        """
           | a
        1  | 1
        2  | 2
        3  | 3
        """
    )
    right = T(
        """
           | b
        0  | baz
        1  | foo
        2  | bar
        """
    )

    joiner = left.join(right, left.id == right.id)
    t1 = joiner.select(col=left.a * 2)
    t2 = joiner.select(col=left.a + t1.col)
    assert_table_equality_wo_index(
        t2,
        T(
            """
                | col
            1   | 3
            2   | 6
            """
        ),
    )


def test_wildcard_basic_usage():
    tab1 = T(
        """
           | a | b
        1  | 1 | 2
        """
    )

    tab2 = T(
        """
           | c | d
        1  | 3 | 4
        """
    )

    left = tab1.select(*tab1, *tab2)

    right = tab1.select(tab1.a, tab1.b, tab2.c, tab2.d)

    assert_table_equality(left, right)


def test_wildcard_shadowing():
    tab = T(
        """
           | a | b | c | d
        1  | 1 | 2 | 3 | 4
        """
    )

    left = tab.select(*tab.without(tab.a, "b"), e=pw.this.a)

    right = tab.select(tab.c, tab.d, e=tab.a)

    assert_table_equality(left, right)


def test_this_magic_1():
    tab = T(
        """
           | a | b | c | d
        1  | 1 | 2 | 3 | 4
        """
    )

    left = tab.select(pw.this.without("a").b)

    right = tab.select(tab.b)

    assert_table_equality(left, right)


def test_this_magic_2():
    tab = T(
        """
           | a | b | c | d
        1  | 1 | 2 | 3 | 4
        """
    )

    with pytest.raises(KeyError):
        tab.select(pw.this.without(pw.this.a).a)


def test_this_magic_3():
    tab = T(
        """
           | a | b | c | d
        1  | 1 | 2 | 3 | 4
        """
    )

    left = tab.select(*pw.this.without(pw.this.a))

    right = tab.select(tab.b, tab.c, tab.d)

    assert_table_equality(left, right)


def test_this_magic_4():
    tab = T(
        """
           | a | b | c | d
        1  | 1 | 2 | 3 | 4
        """
    )

    left = tab.select(*pw.this[["a", "b", pw.this.c]].without(pw.this.a))

    right = tab.select(tab.b, tab.c)

    assert_table_equality(left, right)


def test_join_this():
    t1 = T(
        """
     age  | owner  | pet
      10  | Alice  | 1
       9  | Bob    | 1
       8  | Alice  | 2
     """
    )
    t2 = T(
        """
     age  | owner  | pet | size
      10  | Alice  | 3   | M
      9   | Bob    | 1   | L
      8   | Tom    | 1   | XL
     """
    )
    t3 = t1.join(
        t2, pw.left.pet == pw.right.pet, pw.left.owner == pw.right.owner
    ).select(age=pw.left.age, owner_name=pw.right.owner, size=pw.this.size)

    expected = T(
        """
    age | owner_name | size
    9   | Bob        | L
    """
    )
    assert_table_equality_wo_index(t3, expected)


def test_join_chain_1():
    edges1 = T(
        """
        u | v
        a | b
        b | c
        c | d
        d | e
        e | f
        f | g
        g | a
    """
    )
    edges2 = edges1.copy()
    edges3 = edges1.copy()
    path3 = (
        edges1.join(edges2, edges1.v == edges2.u)
        .join(edges3, edges2.v == edges3.u)
        .select(edges1.u, edges3.v)
    )
    assert_table_equality_wo_index(
        path3,
        T(
            """
        u | v
        a | d
        b | e
        c | f
        d | g
        e | a
        f | b
        g | c
        """
        ),
    )


def test_join_chain_2():
    edges1 = T(
        """
        u | v
        a | b
        b | c
        c | d
        d | e
        e | f
        f | g
        g | a
    """
    )
    edges2 = edges1.copy()
    edges3 = edges1.copy()
    path3 = edges1.join(
        edges2.join(edges3, edges2.v == edges3.u), edges1.v == edges2.u
    ).select(edges1.u, edges3.v)
    assert_table_equality_wo_index(
        path3,
        T(
            """
        u | v
        a | d
        b | e
        c | f
        d | g
        e | a
        f | b
        g | c
        """
        ),
    )


def test_join_leftrightthis():
    left_table = T(
        """
           | a | b | c
        1  | 1 | 2 | 3
        """
    )

    right_table = T(
        """
           | b | c | d
        1  | 2 | 3 | 4
        """
    )

    assert_table_equality_wo_index(
        left_table.join(right_table, pw.left.b == pw.right.b).select(
            pw.left.a, pw.this.b, pw.right.c, pw.right.d
        ),
        T(
            """
        a | b | c | d
        1 | 2 | 3 | 4
        """
        ),
    )

    with pytest.raises(KeyError):
        left_table.join(right_table, pw.left.b == pw.right.b).select(pw.this.c)


def test_chained_join_leftrightthis():
    left_table = T(
        """
           | a | b
        1  | 1 | 2
        """
    )

    middle_table = T(
        """
           | b | c
        1  | 2 | 3
        """
    )

    right_table = T(
        """
           | b | d
        1  | 2 | 4
        """
    )

    assert_table_equality_wo_index(
        left_table.join(middle_table, pw.left.b == pw.right.b)
        .join(right_table, pw.left.b == pw.right.b)
        .select(*pw.this),
        T(
            """
        a | b | c | d
        1 | 2 | 3 | 4
        """
        ),
    )


def test_chained_join_ids():
    left_table = T(
        """
           | a | b
        1  | 1 | 2
        """
    )

    middle_table = T(
        """
           | b | c
        1  | 2 | 3
        """
    )

    right_table = T(
        """
           | b | d
        1  | 2 | 4
        """
    )

    manually = (
        left_table.join(middle_table, pw.left.b == pw.right.b)
        .select(pw.left.b)
        .with_columns(left_id=pw.this.id)
        .join(right_table, pw.left.b == pw.right.b)
        .select(pw.left.left_id, right_id=pw.right.id)
        .with_columns(this_id=pw.this.id)
    )

    assert_table_equality(
        left_table.join(middle_table, pw.left.b == pw.right.b)
        .join(right_table, pw.left.b == pw.right.b)
        .select(left_id=pw.left.id, right_id=pw.right.id, this_id=pw.this.id),
        manually,
    )


def test_multiple_ix():
    indexed_table = T(
        """
           | col
        2  | a
        3  | b
        4  | c
        5  | d
        """
    )

    indexer1 = T(
        """
          | key
        1 | 4
        2 | 3
        3 | 2
        4 | 1
    """
    ).with_columns(key=indexed_table.pointer_from(pw.this.key))

    indexer2 = T(
        """
          | key
        1 | 6
        2 | 5
        3 | 4
        4 | 3
    """
    ).with_columns(key=indexed_table.pointer_from(pw.this.key))

    a = (
        indexed_table.ix(indexer1.key, allow_misses=True)
        .filter(pw.this.col.is_not_none())
        .select(col1=pw.this.col)
    )
    b = (
        indexed_table.ix(indexer2.key, allow_misses=True)
        .filter(pw.this.col.is_not_none())
        .select(col2=pw.this.col)
    )
    result = a.intersect(b)
    result = a.restrict(result) + b.restrict(result)
    assert_table_equality_wo_index(
        result,
        T(
            """
        col1 | col2
           a |    c
           b |    d
        """
        ),
    )


def test_join_desugaring_assign_id():
    left = T(
        """
              | col | on
            1 | a   | 11
            2 | b   | 12
            3 | c   | 13
        """
    )
    right = T(
        """
              | col | on
            1 | d   | 12
            2 | e   | 13
            3 | f   | 14
        """,
    )
    joined_lr = left.join(right, left.on == right.on, id=left.id).select(
        lcol=pw.left.col, rcol=pw.right.col
    )
    assert_table_equality_wo_index(
        joined_lr,
        T(
            """
          | lcol | rcol
        1 |    b |    d
        2 |    c |    e
    """
        ),
    )

    joined_rl = right.join(left, right.on == left.on, id=left.id).select(
        lcol=pw.right.col, rcol=pw.left.col
    )
    assert_table_equality_wo_index(joined_lr, joined_rl)


def test_join_chain_assign_id():
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
            | b  | c
        11  | b2 | c2
        12  | b3 | c3
        13  | b4 | c4
        14  | b5 | c5
        """
    )

    right_table = T(
        """
           | c  | d
        21 | c3 | d3
        22 | c4 | d4
        23 | c5 | d5
        24 | c6 | d6
        """
    )

    assert_table_equality(
        left_table.join(middle_table, pw.left.b == pw.right.b, id=pw.left.id)
        .join(right_table, pw.left.c == pw.right.c, id=pw.left.id)
        .select(*pw.this),
        T(
            """
          | a  | b  | c  | d
        3 | a3 | b3 | c3 | d3
        4 | a4 | b4 | c4 | d4
        """
        ),
    )


def test_desugaring_this_star_groupby_01():
    ret = (
        T(
            """
           | A | B
        31 | x | y
        33 | x | y
    """
        )
        .groupby(*pw.this)
        .reduce(*pw.this)
    )

    expected = T(
        """
           | A | B
        31 | x | y

    """
    )
    # expected is not build inline,
    # so that we do have to use any desugaring
    expected = expected.with_id_from(expected.A, expected.B)
    assert_table_equality(ret, expected)


@pytest.mark.parametrize(
    "from_,to_",
    [
        (
            [10, 0, -1, -2, 2**32 + 1, 2**45 + 1],
            [10.0, 0, -1.0, -2, float(2**32 + 1), float(2**45 + 1)],
        ),
        (
            [10, 0, -1, -2, 2**32 + 1, 2**45 + 1],
            [True, False, True, True, True, True],
        ),
        (
            [10, 0, -1, -2, 2**32 + 1, 2**45 + 1],
            ["10", "0", "-1", "-2", "4294967297", "35184372088833"],
        ),
        (
            [
                10.345,
                10.999,
                -1.012,
                -1.99,
                -2.01,
                float(2**32 + 1),
                float(2**45 + 1),
                float(2**60 + 1),
            ],
            [10, 10, -1, -1, -2, 2**32 + 1, 2**45 + 1, 2**60],
        ),
        ([10.345, 10.999, -1.012, -1.99, 0.0], [True, True, True, True, False]),
        (
            [
                10.345,
                10.999,
                -1.012,
                -1.99,
                -2.01,
                2**32 + 0.2,
                2**45 + 0.1,
            ],
            [
                "10.345",
                "10.999",
                "-1.012",
                "-1.99",
                "-2.01",
                "4294967296.2",
                "35184372088832.1",
            ],
        ),
        ([False, True], [0, 1]),
        ([False, True], [0.0, 1.0]),
        ([False, True], ["False", "True"]),
        (
            ["10", "0", "-1", "-2", "4294967297", "35184372088833"],
            [10, 0, -1, -2, 2**32 + 1, 2**45 + 1],
        ),
        (
            [
                "10.345",
                "10.999",
                "-1.012",
                "-1.99",
                "-2.01",
                "4294967297",
                "35184372088833",
            ],
            [
                10.345,
                10.999,
                -1.012,
                -1.99,
                -2.01,
                float(2**32 + 1),
                float(2**45 + 1),
            ],
        ),
        (["", "False", "True", "12", "abc"], [False, True, True, True, True]),
    ],
)
def test_cast(from_: list, to_: list):
    from_dtype = type(from_[0])
    to_dtype = type(to_[0])

    def move_to_pathway_with_the_right_type(list: list, dtype: Any):
        df = pd.DataFrame({"a": list}, dtype=dtype)
        table = table_from_pandas(df)
        return table

    table = move_to_pathway_with_the_right_type(from_, from_dtype)
    expected = move_to_pathway_with_the_right_type(to_, to_dtype)
    table = table.select(a=pw.cast(to_dtype, pw.this.a))
    assert_table_equality(table, expected)


def test_cast_optional():
    tab = T(
        """
          | a
        1 | 1
        2 |
        3 | 1
        """
    )
    ret = tab.select(a=pw.cast(Optional[float], pw.this.a))
    expected = T(
        """
          | a
        1 | 1.0
        2 |
        3 | 1.0
        """
    ).update_types(a=Optional[float])
    assert_table_equality(ret, expected)


def test_lazy_coalesce():
    tab = T(
        """
    col
    1
    2
    3
    """
    )
    ret = tab.select(col=pw.coalesce(tab.col, tab.col // 0))
    assert_table_equality(ret, tab)


def test_coalesce_optional_int_float():
    table = T(
        """
          | a |  b
        1 | 1 | 1.2
        2 |   | 2.3
        3 | 3 | 3.4
        4 |   | 4.5
    """
    )
    expected = T(
        """
        res
        1 | 1.5
        2 | 2.8
        3 | 3.5
        4 | 5.0
    """
    )
    ret = table.select(res=pw.coalesce(pw.this.a, pw.this.b) + 0.5)
    assert_table_equality(ret, expected)


def test_coalesce_errors_on_incompatible_types():
    table = T(
        """
          | a |  b
        1 | 1 | abc
        2 | 2 | x
        3 | 3 | xxx
        4 | 4 | d
    """
    )
    with pytest.raises(TypeError):
        table.select(res=pw.coalesce(pw.this.a, pw.this.b))


def test_require_01():
    tab = T(
        """
    col1 | col2
    2   | 2
    1   |
    3   | 3
    """
    )

    expected = T(
        """
    sum | dummy
    4   | 1
        | 1
    6   | 1
    """
    ).select(pw.this.sum)

    def f(a, b):
        return a + b

    app_expr = pw.apply(f, tab.col1, tab.col2)
    req_expr = pw.require(app_expr, tab.col2)

    res = tab.select(sum=req_expr)

    assert_table_equality_wo_index_types(res, expected)

    assert req_expr._dependencies() == app_expr._dependencies()


def test_if_else():
    tab = T(
        """
    a | b
    1 | 0
    2 | 2
    3 | 3
    4 | 2
        """
    )

    ret = tab.select(res=pw.if_else(tab.b != 0, tab.a // tab.b, 0))

    assert_table_equality(
        ret,
        T(
            """
        res
        0
        1
        1
        2
        """
        ),
    )


def test_if_else_int_float():
    table = T(
        """
        a |  b
        1 | 1.2
        2 | 2.3
        3 | 3.4
        4 | 4.5
        """
    )
    expected = T(
        """
        res
        1.3
        2.4
        3.1
        4.1
    """
    )
    ret = table.select(res=pw.if_else(pw.this.a > 2, pw.this.a, pw.this.b) + 0.1)

    assert_table_equality(ret, expected)


def test_if_else_optional_int_float():
    table = T(
        """
          | a |  b  | c
        1 | 1 | 1.2 | False
        2 | 2 | 2.3 | False
        3 | 3 | 3.4 | True
        4 |   | 4.5 | True
    """
    )
    expected = T(
        """
        res
        1 | 1.2
        2 | 2.3
        3 | 3.0
        4 |
    """
    )
    ret = table.select(res=pw.if_else(pw.this.c, pw.this.a, pw.this.b))
    assert_table_equality(ret, expected)


def test_if_else_errors_on_incompatible_types():
    table = T(
        """
          | a |  b
        1 | 1 | abc
        2 | 2 | x
        3 | 3 | xxx
        4 | 4 | d
    """
    )
    with pytest.raises(TypeError):
        table.select(res=pw.if_else(pw.this.a > 2, pw.this.a, pw.this.b))


def test_join_filter_1():
    left = T(
        """
            val
            10
            11
            12
        """
    )
    right = T(
        """
            val
            10
            11
            12
        """,
    )
    joined = (
        left.join(right)
        .filter(pw.left.val < pw.right.val)
        .select(left_val=pw.left.val, right_val=pw.right.val)
    )
    assert_table_equality_wo_index(
        joined,
        T(
            """
            left_val | right_val
                  10 |        11
                  10 |        12
                  11 |        12
            """
        ),
    )


def test_join_filter_2():
    tA = T(
        """
             a
            10
            11
            12
        """
    )
    tB = T(
        """
             b
            10
            11
            12
        """
    )
    tC = T(
        """
             c
            10
            11
            12
        """
    )
    tD = T(
        """
             d
            10
            11
            12
        """
    )

    result = (
        tA.join(tB)
        .filter(pw.this.a <= pw.this.b)
        .join(tC)
        .join(tD)
        .filter(pw.this.c <= pw.this.d)
        .filter(pw.this.a + pw.this.b == pw.this.c + pw.this.d)
        .select(*pw.this)
    )
    expected = T(
        """
 a  | b  | c  | d
 10 | 10 | 10 | 10
 10 | 11 | 10 | 11
 10 | 12 | 10 | 12
 10 | 12 | 11 | 11
 11 | 11 | 10 | 12
 11 | 11 | 11 | 11
 11 | 12 | 11 | 12
 12 | 12 | 12 | 12
        """
    )

    assert_table_equality_wo_index(result, expected)


def test_outerjoin_filter_1():
    left = T(
        """
            val
            10
            11
            12
        """
    )
    right = T(
        """
            val
            11
            12
            13
        """,
    )
    joined = (
        left.join_outer(right, left.val == right.val)
        .filter(pw.left.val.is_not_none())
        .filter(pw.right.val.is_not_none())
        .select(left_val=pw.left.val, right_val=pw.right.val)
    )
    assert_table_equality_wo_index(
        joined,
        T(
            """
            left_val | right_val
                  11 |        11
                  12 |        12
            """
        ),
    )


def test_outerjoin_filter_2():
    left = T(
        """
            val
            10
            11
            12
        """
    )
    right = T(
        """
            val
            11
            12
            13
        """,
    )
    joined = (
        left.join_outer(right, left.val == right.val)
        .filter(pw.left.val.is_not_none())
        .filter(pw.right.val.is_not_none())
        .select(val=pw.unwrap(pw.left.val) + pw.unwrap(pw.right.val))
    )
    assert_table_equality_wo_index(
        joined,
        T(
            """
            val
             22
             24
            """
        ),
    )


def test_join_reduce_1():
    left = T(
        """
            a
            10
            11
            12
        """
    )
    right = T(
        """
            b
            11
            12
            13
        """,
    )
    result = left.join(right).reduce(col=pw.reducers.count())
    expected = T(
        """
        col
        9
    """
    )
    assert_table_equality_wo_index(result, expected)


def test_join_reduce_2():
    left = T(
        """
            a
            10
            11
            12
        """
    )
    right = T(
        """
            b
            11
            12
            13
        """,
    )
    result = left.join(right).reduce(col=pw.reducers.sum(pw.left.a * pw.right.b))
    result2 = left.join(right).reduce(col=pw.reducers.sum(pw.this.a * pw.this.b))
    expected = T(
        f"""
        col
        {(10+11+12)*(11+12+13)}
    """
    )
    assert_table_equality_wo_index(result, expected)
    assert_table_equality_wo_index(result2, expected)


def test_join_groupby_1():
    left = T(
        """
            a  | lcol
            10 |    1
            11 |    1
            12 |    2
            13 |    2
        """
    )
    right = T(
        """
            b  | rcol
            11 |    1
            12 |    1
            13 |    2
            14 |    2
        """,
    )
    result = (
        left.join(right)
        .groupby(pw.this.lcol, pw.this.rcol)
        .reduce(pw.this.lcol, pw.this.rcol, res=pw.reducers.sum(pw.this.a * pw.this.b))
    )
    expected = T(
        f"""
    lcol | rcol | res
       1 |    1 | {(10+11)*(11+12)}
       1 |    2 | {(10+11)*(13+14)}
       2 |    1 | {(12+13)*(11+12)}
       2 |    2 | {(12+13)*(13+14)}
    """
    )
    assert_table_equality_wo_index(result, expected)


def test_join_groupby_2():
    left = T(
        """
            a  |  col
            10 |    1
            11 |    1
            12 |    2
            13 |    2
        """
    )
    right = T(
        """
            b  |  col
            11 |    1
            12 |    1
            13 |    2
            14 |    2
        """,
    )
    result = (
        left.join(right, left.col == right.col)
        .groupby(pw.this.col)
        .reduce(pw.this.col, res=pw.reducers.sum(pw.this.a * pw.this.b))
    )
    expected = T(
        f"""
    col | res
      1 | {(10+11)*(11+12)}
      2 | {(12+13)*(13+14)}
    """
    )
    assert_table_equality_wo_index(result, expected)


def test_join_filter_reduce():
    left = T(
        """
            a
            10
            11
            12
        """
    )
    right = T(
        """
            b
            11
            12
            13
        """,
    )
    result = (
        left.join(right).filter(pw.this.a >= pw.this.b).reduce(col=pw.reducers.count())
    )
    expected = T(
        """
        col
        3
    """
    )
    assert_table_equality_wo_index(result, expected)


def test_make_tuple():
    t = T(
        """
        a | b  | c
        1 | 10 | a
        2 | 20 |
        3 | 30 | c
        """
    )
    result = t.select(zip_column=pw.make_tuple(t.a * 2, pw.this.b, pw.this.c))

    def three_args_tuple(x, y, z) -> tuple:
        return (x, y, z)

    expected = t.select(
        zip_column=pw.apply_with_type(
            three_args_tuple,
            tuple[int, int, Optional[str]],  # type: ignore[arg-type]
            pw.this.a * 2,
            pw.this.b,
            pw.this.c,
        )
    )
    assert_table_equality_wo_index(result, expected)


def test_sequence_get_unchecked_fixed_length():
    t1 = T(
        """
    i | s
    4 | xyz
    3 | abc
    7 | d
    """
    )

    t2 = t1.select(tup=pw.make_tuple(pw.this.i, pw.this.s))
    t3 = t2.select(i=pw.this.tup[0], s=pw.this.tup[1])

    assert_table_equality(t3, t1)


def test_sequence_get_unchecked_fixed_length_dynamic_index_1():
    t1 = T(
        """
    i | s   | a
    4 | xyz | 0
    3 | abc | 1
    7 | d   | 0
    """
    )

    t2 = t1.select(tup=pw.make_tuple(pw.this.i, pw.this.s), a=pw.this.a)
    t3 = t2.select(r=pw.this.tup[pw.this.a])
    assert t3.schema._dtypes() == {"r": dt.ANY}


def test_sequence_get_unchecked_fixed_length_dynamic_index_2():
    t1 = T(
        """
    a | b | c
    4 | 1 | 0
    3 | 2 | 1
    7 | 3 | 1
    """
    )
    expected = T(
        """
    r
    4
    2
    3
    """
    )

    t2 = t1.select(tup=pw.make_tuple(pw.this.a, pw.this.b), c=pw.this.c)
    t3 = t2.select(r=pw.this.tup[pw.this.c])

    assert_table_equality(t3, expected)


def test_sequence_get_checked_fixed_length_dynamic_index():
    t1 = T(
        """
    a | b | c
    4 | 1 | 0
    3 | 2 | 1
    7 | 3 | 1
    """
    )
    expected = T(
        """
    r
    4
    2
    3
    """
    )

    t2 = t1.select(tup=pw.make_tuple(pw.this.a, pw.this.b), c=pw.this.c)
    t3 = t2.select(r=pw.this.tup.get(pw.this.c))

    assert t3.schema._dtypes() == {"r": dt.Optional(dt.INT)}
    assert_table_equality_wo_types(t3, expected)


def _create_tuple(n: int) -> tuple[int, ...]:
    return tuple(range(n, 0, -1))


def test_sequence_get_unchecked_variable_length():
    t1 = T(
        """
    a
    3
    4
    5
    """
    )
    expected = T(
        """
    x | y
    1 | 3
    2 | 3
    3 | 3
    """
    )

    t2 = t1.select(tup=pw.apply(_create_tuple, pw.this.a))
    t3 = t2.select(x=pw.this.tup[2], y=pw.this.tup[-3])

    assert_table_equality(t3, expected)


def test_sequence_get_unchecked_variable_length_untyped():
    t1 = T(
        """
    a
    3
    4
    5
    """
    )
    expected = T(
        """
    x | y
    1 | 3
    2 | 3
    3 | 3
    """
    )

    t2 = t1.select(tup=pw.apply(_create_tuple, pw.this.a))
    t3 = t2.select(x=pw.this.tup[2], y=pw.this.tup[-3])

    assert_table_equality(t3, expected)


def test_sequence_get_checked_variable_length():
    t1 = T(
        """
    a
    1
    2
    3
    """
    )
    expected = T(
        """
    x | y
      | 1
    1 | 1
    2 | 1
    """
    ).update_types(y=int | None)

    t2 = t1.select(tup=pw.apply(_create_tuple, pw.this.a))
    t3 = t2.select(x=pw.this.tup.get(1), y=pw.this.tup.get(-1))

    assert_table_equality(t3, expected)


def test_sequence_get_unchecked_variable_length_errors():
    t1 = T(
        """
    a
    1
    2
    5
    """
    )

    t2 = t1.select(tup=pw.apply(_create_tuple, pw.this.a))
    t2.select(x=pw.this.tup[1])
    with pytest.raises(IndexError):
        run_all()


def test_sequence_get_unchecked_fixed_length_errors():
    t1 = T(
        """
    a | b
    4 | 10
    3 | 9
    7 | 8
    """
    )

    t2 = t1.select(tup=pw.make_tuple(pw.this.a, pw.this.b))
    with pytest.raises(
        IndexError,
        match=(
            re.escape(f"Index 2 out of range for a tuple of type {tuple[int,int]}.")
        ),
    ):
        t2.select(i=pw.this.tup[2])


def test_sequence_get_checked_fixed_length_errors():
    t1 = T(
        """
    a | b  |  c
    4 | 10 | abc
    3 | 9  | def
    7 | 8  | xx
    """
    )
    expected = T(
        """
     c
    abc
    def
    xx
    """
    )

    t2 = t1.with_columns(tup=pw.make_tuple(pw.this.a, pw.this.b))
    with pytest.warns(
        match=(
            "(?s)"  # make dot match newlines
            + re.escape(f"Index 2 out of range for a tuple of type {tuple[int,int]}. ")
            + ".*"
            + re.escape("Consider using just the default value without .get().")
        ),
    ):
        t3 = t2.select(c=pw.this.tup.get(2, default=pw.this.c))
        assert_table_equality(t3, expected)


@pytest.mark.parametrize("dtype", [int, float])
@pytest.mark.parametrize("index", [pw.this.index_pos, pw.this.index_neg])
@pytest.mark.parametrize("checked", [True, False])
def test_sequence_get_from_1d_ndarray(dtype, index, checked):
    t = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "a": [
                    np.array([1, 2, 3], dtype=dtype),
                    np.array([4, 5], dtype=dtype),
                    np.array([0, 0], dtype=dtype),
                ],
                "index_pos": [1, 1, 1],
                "index_neg": [-2, -1, -1],
            }
        )
    )
    expected = T(
        """
        a
        2
        5
        0
    """
    ).update_types(a=dtype)
    if checked:
        result = t.select(a=pw.this.a.get(index))
    else:
        result = t.select(a=pw.this.a[index])
    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize("dtype", [int, float])
@pytest.mark.parametrize("index", [1, -1])
@pytest.mark.parametrize("checked", [True, False])
def test_sequence_get_from_2d_ndarray(dtype, index, checked):
    t = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "a": [
                    np.array([[1, 2, 3], [4, 5, 6]], dtype=dtype),
                    np.array([[4, 5], [6, 7]], dtype=dtype),
                    np.array([[0, 0], [1, 1]], dtype=dtype),
                ]
            }
        )
    )
    expected = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "a": [
                    np.array([4, 5, 6], dtype=dtype),
                    np.array([6, 7], dtype=dtype),
                    np.array([1, 1], dtype=dtype),
                ]
            }
        )
    )

    if checked:
        result = t.select(a=pw.this.a.get(index))
    else:
        result = t.select(a=pw.this.a[index])

    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize("dtype", [int, float])
@pytest.mark.parametrize(
    "index,expected", [([2, 2, 2], [3, -1, -1]), ([-3, -2, -3], [1, 4, -1])]
)
def test_sequence_get_from_1d_ndarray_default(dtype, index, expected):
    t = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "a": [
                    np.array([1, 2, 3], dtype=dtype),
                    np.array([4, 5], dtype=dtype),
                    np.array([0, 0], dtype=dtype),
                ],
                "index": index,
            }
        )
    )
    expected = pw.debug.table_from_pandas(
        pd.DataFrame({"a": expected}).astype(
            dtype={"a": {int: "int", float: "float"}[dtype]}
        )
    )
    result = t.select(a=pw.this.a.get(pw.this.index, default=-1))
    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize("dtype", [int, float])
@pytest.mark.parametrize("index", [[2, 2, 2], [-3, -2, -3]])
def test_sequence_get_from_1d_ndarray_out_of_bounds(dtype, index):
    t = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "a": [
                    np.array([1, 2, 3], dtype=dtype),
                    np.array([4, 5], dtype=dtype),
                    np.array([0, 0], dtype=dtype),
                ],
                "index": index,
            }
        )
    )
    t.select(a=pw.this.a[pw.this.index])
    with pytest.raises(IndexError):
        run_all()


def test_unique():
    left = T(
        """
    pet  |  owner  | age
    dog  | Bob     | 10
    cat  | Alice   | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    foo  | Charlie | 6
    """
    )

    left_res = left.groupby(left.pet).reduce(left.pet, pw.reducers.unique(left.owner))

    assert_table_equality_wo_index(
        left_res,
        T(
            """
        pet | owner
        dog | Bob
        cat | Alice
        foo | Charlie
    """
        ),
    )
    left.groupby(left.pet).reduce(pw.reducers.unique(left.age))
    with pytest.raises(Exception):
        run_all()


def test_any():
    left = T(
        """
    pet  |  owner  | age
    dog  | Bob     | 10
    cat  | Alice   | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    foo  | Charlie | 6
    """
    )

    left_res = left.reduce(
        pw.reducers.any(left.pet),
        pw.reducers.any(left.owner),
        pw.reducers.any(left.age),
    )

    joined = left.join(
        left_res,
        left.pet == left_res.pet,
        left.owner == left_res.owner,
        left.age == left_res.age,
    ).reduce(cnt=pw.reducers.count())

    assert_table_equality_wo_index(
        joined,
        T(
            """
    cnt
    1
    """
        ),
    )


def test_slices_1():
    left = T(
        """
            col | on
            a   | 11
            b   | 12
            c   | 13
        """
    )
    right = T(
        """
            col | on
            d   | 12
            e   | 13
            f   | 14
        """,
    )
    res = left.join(right, left.on == right.on).select(
        **left.slice.with_suffix("_l").with_prefix("t"),
        **right.slice.with_suffix("_r").with_prefix("t"),
    )
    expected = T(
        """
tcol_l | ton_l | tcol_r | ton_r
b      | 12    | d      | 12
c      | 13    | e      | 13
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_slices_2():
    left = T(
        """
            col | on
            a   | 11
            b   | 12
            c   | 13
        """
    )
    right = T(
        """
            col | on
            d   | 12
            e   | 13
            f   | 14
        """,
    )
    res = left.join(right, left.on == right.on).select(
        **pw.left.with_suffix("_l").with_prefix("t"),
        **pw.right.with_suffix("_r").with_prefix("t"),
    )
    expected = T(
        """
tcol_l | ton_l | tcol_r | ton_r
b      | 12    | d      | 12
c      | 13    | e      | 13
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_slices_3():
    left = T(
        """
            col | on
            a   | 11
            b   | 12
            c   | 13
        """
    )
    right = T(
        """
            col | on
            d   | 12
            e   | 13
            f   | 14
        """,
    )
    res = left.join(right, left.on == right.on).select(
        **pw.left.without("col"),
        **pw.right.rename({"col": "col2"}),
    )
    expected = T(
        """
on | col2
12 | d
13 | e
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_slices_4():
    left = T(
        """
            col | on
            a   | 11
            b   | 12
            c   | 13
        """
    )
    right = T(
        """
            col | on
            d   | 12
            e   | 13
            f   | 14
        """,
    )
    res = left.join(right, left.on == right.on).select(
        **pw.left.without(pw.this.col),
        **pw.right.rename({pw.this.col: pw.this.col2}),
    )
    expected = T(
        """
on | col2
12 | d
13 | e
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_slices_5():
    left = T(
        """
            col | on
            a   | 11
            b   | 12
            c   | 13
        """
    )
    right = T(
        """
            col | on
            d   | 12
            e   | 13
            f   | 14
        """,
    )
    res = left.join(right, left.on == right.on).select(
        **pw.left.without(left.col),
        **pw.right.rename({right.col: pw.this.col2})[["col2"]],
    )
    expected = T(
        """
on | col2
12 | d
13 | e
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_slices_6():
    left = T(
        """
            col | on
            a   | 11
            b   | 12
            c   | 13
        """
    )
    right = T(
        """
            col | on
            d   | 12
            e   | 13
            f   | 14
        """,
    )
    res = left.join(right, left.on == right.on).select(
        left.slice.on,
    )
    expected = T(
        """
on
12
13
    """
    )
    assert_table_equality_wo_index(res, expected)
    assert_table_equality_wo_index(res, expected)


def test_unwrap():
    a = T(
        """
        foo
        1
        2
        3
        None
        """
    )
    result = a.filter(a.foo.is_not_none()).select(ret=pw.unwrap(pw.this.foo))

    assert_table_equality(
        result,
        T(
            """
            ret
            1
            2
            3
            """
        ),
    )


def test_unwrap_with_nones():
    a = T(
        """
        foo
        1
        2
        3
        None
        """
    )
    a.select(ret=pw.unwrap(pw.this.foo))

    with pytest.raises(ValueError):
        run_all()


@pytest.mark.parametrize(
    "reducer, skip_nones, expected",
    [
        (
            pw.reducers.tuple,
            False,
            [(1, -1, None), (7, 4, 4)],
        ),
        (
            pw.reducers.tuple,
            True,
            [(1, -1), (7, 4, 4)],
        ),
        (
            pw.reducers.sorted_tuple,
            False,
            [(None, -1, 1), (4, 4, 7)],
        ),
        (
            pw.reducers.sorted_tuple,
            True,
            [(-1, 1), (4, 4, 7)],
        ),
    ],
)
def test_tuple_reducer(reducer, skip_nones, expected):
    t = pw.debug.table_from_markdown(
        """
           | colA | colB
        3  | valA | -1
        2  | valA | 1
        5  | valA |
        4  | valB | 4
        6  | valB | 4
        1  | valB | 7
        """,
    )

    df = pd.DataFrame({"tuple": expected})
    expected = pw.debug.table_from_pandas(
        df,
        schema=pw.schema_from_types(
            tuple=list[int] if skip_nones else list[Optional[int]]
        ),
    )

    res = t.groupby(t.colA).reduce(tuple=reducer(t.colB, skip_nones=skip_nones))
    assert_table_equality_wo_index(res, expected)


def test_tuple_reducer_consistency():
    left = T(
        """
    pet  |  owner  | age
    dog  | Bob     | 10
    cat  | Alice   | 9
    cat  | Alice   | 8
    dog  | Bob     | 7
    foo  | Charlie | 6
    """
    )

    left_res = left.reduce(
        pet=pw.reducers.tuple(left.pet),
        owner=pw.reducers.tuple(left.owner),
        age=pw.reducers.tuple(left.age),
    )

    t2 = left_res.select(
        pet=pw.this.pet.get(3), owner=pw.this.owner.get(3), age=pw.this.age.get(3)
    )
    print(t2.schema)

    joined = left.join(
        t2,
        left.pet == t2.pet,
        left.owner == t2.owner,
        left.age == t2.age,
    ).reduce(cnt=pw.reducers.count())

    assert_table_equality_wo_index(
        joined,
        T(
            """
            cnt
            1
            """
        ),
    )


@pytest.mark.parametrize(
    "table_schema, schema, allow_superset, ignore_primary_keys",
    [
        (
            {"col_a": pw.column_definition(dtype=int)},
            {"col_a": pw.column_definition(dtype=int)},
            False,
            False,
        ),
        (
            {
                "col_a": pw.column_definition(dtype=int),
                "col_b": pw.column_definition(dtype=float),
            },
            {"col_a": pw.column_definition(dtype=int)},
            True,
            False,
        ),
        (
            {
                "col_a": pw.column_definition(dtype=int, primary_key=True),
                "col_b": pw.column_definition(dtype=float),
            },
            {"col_a": pw.column_definition(dtype=int, primary_key=True)},
            True,
            False,
        ),
        (
            {
                "col_a": pw.column_definition(dtype=int, primary_key=True),
                "col_b": pw.column_definition(dtype=float),
            },
            {"col_a": pw.column_definition(dtype=int)},
            True,
            True,
        ),
        (
            {
                "col_a": pw.column_definition(dtype=int),
                "col_b": pw.column_definition(dtype=dt.NONE),
                "col_c": pw.column_definition(dtype=Optional[int]),
                "col_d": pw.column_definition(dtype=int | None),
            },
            {
                "col_a": pw.column_definition(dtype=int | None),
                "col_b": pw.column_definition(dtype=Optional[int]),
                "col_c": pw.column_definition(dtype=int | None),
                "col_d": pw.column_definition(dtype=Optional[int]),
            },
            True,
            True,
        ),
    ],
)
def test_assert_table_has_schema_passes(
    table_schema, schema, allow_superset, ignore_primary_keys
):
    table_schema = pw.schema_builder(table_schema)
    table = empty_from_schema(table_schema)
    schema = pw.schema_builder(schema)

    pw.assert_table_has_schema(
        table,
        schema,
        allow_superset=allow_superset,
        ignore_primary_keys=ignore_primary_keys,
    )


@pytest.mark.parametrize(
    "table_schema, schema, allow_superset, ignore_primary_keys, allow_subtype",
    [
        (
            {"col_a": pw.column_definition(dtype=int)},
            {"col_a": pw.column_definition(dtype=str)},
            False,
            False,
            True,
        ),
        (
            {"col_a": pw.column_definition(dtype=int)},
            {"col_a": pw.column_definition(dtype=float)},
            False,
            False,
            False,
        ),
        (
            {
                "col_a": pw.column_definition(dtype=int),
                "col_b": pw.column_definition(dtype=float),
            },
            {"col_a": pw.column_definition(dtype=int)},
            False,
            False,
            True,
        ),
        (
            {"col_a": pw.column_definition(dtype=int, primary_key=True)},
            {
                "col_a": pw.column_definition(dtype=int, primary_key=True),
                "col_b": pw.column_definition(dtype=float),
            },
            True,
            False,
            True,
        ),
        (
            {
                "col_a": pw.column_definition(dtype=int, primary_key=True),
                "col_b": pw.column_definition(dtype=float),
            },
            {"col_a": pw.column_definition(dtype=int)},
            True,
            False,
            True,
        ),
        (
            {"col_a": pw.column_definition(dtype=int | None)},
            {"col_a": pw.column_definition(dtype=str | None)},
            True,
            True,
            True,
        ),
        (
            {"col_a": pw.column_definition(dtype=int | None)},
            {"col_a": pw.column_definition(dtype=int)},
            True,
            True,
            True,
        ),
    ],
)
def test_assert_table_has_schema_fails(
    table_schema, schema, allow_superset, ignore_primary_keys, allow_subtype
):
    table_schema = pw.schema_builder(table_schema)
    table = empty_from_schema(table_schema)
    schema = pw.schema_builder(schema)

    with pytest.raises(AssertionError):
        pw.assert_table_has_schema(
            table,
            schema,
            allow_superset=allow_superset,
            ignore_primary_keys=ignore_primary_keys,
            allow_subtype=allow_subtype,
        )


def test_assert_table_has_schema_default_arguments():
    table_schema = pw.schema_builder(
        {
            "col_a": pw.column_definition(dtype=int, primary_key=True),
            "col_b": pw.column_definition(dtype=float),
        }
    )
    table = empty_from_schema(table_schema)

    # checking ignore_primary_keys argument
    schema = pw.schema_from_types(col_a=int, col_b=float)
    pw.assert_table_has_schema(
        table,
        schema,
    )

    # checking allow_superset argument
    schema = pw.schema_from_types(col_a=int)
    pw.assert_table_has_schema(
        table,
        schema,
    )


def test_table_transformer_decorator():
    class MySchema1(pw.Schema):
        foo: int

    class MySchema2(pw.Schema):
        foo: int
        bar: float

    class MySchema3(pw.Schema):
        foo: int
        bar: float
        baz: Any

    t1 = empty_from_schema(MySchema1)
    t2 = empty_from_schema(MySchema2)
    t3 = empty_from_schema(MySchema3)

    @pw.table_transformer(allow_superset=True, locals=locals())
    def fun(
        a,
        b: int,
        c: pw.Table[MySchema2],
        *,
        d: pw.Table[MySchema2],
    ) -> pw.Table[MySchema2]:
        return a

    fun(t2, 5, t3, d=t2)
    # incorrect kwarg
    with pytest.raises(AssertionError):
        fun(t2, 5, t3, d=t1)
    # incorrect arg
    with pytest.raises(AssertionError):
        fun(t2, 5, t1, d=t2)
    # incorrect return value
    with pytest.raises(AssertionError):
        fun(t1, 5, t3, d=t2)


def test_table_transformer_decorator_args_as_mapping():
    class MySchema1(pw.Schema):
        foo: int = pw.column_definition(primary_key=False)
        bar: float

    class MySchema2(pw.Schema):
        foo: int = pw.column_definition(primary_key=True)
        bar: float

    class MySchema3(pw.Schema):
        foo: int = pw.column_definition(primary_key=False)
        bar: float
        baz: Any

    t1 = empty_from_schema(MySchema1)
    t2 = empty_from_schema(MySchema2)
    t3 = empty_from_schema(MySchema3)

    @pw.table_transformer(
        allow_superset={"c": True, "return": False},
        ignore_primary_keys={"c": True, "return": False},
        locals=locals(),
    )
    def fun(
        a,
        b: int,
        c: pw.Table[MySchema2],
        *,
        d: pw.Table[MySchema2],
    ) -> pw.Table[MySchema1]:
        return a

    # c can be a superset and ignores primary keys
    fun(t1, 5, t3, d=t3)
    # return value cannot be a superset
    with pytest.raises(AssertionError):
        fun(t3, 5, t3, d=t3)
    # return value cannot have different primary keys
    with pytest.raises(AssertionError):
        fun(t2, 5, t3, d=t3)


# MySchema2 needs to be defined globally for test_table_transformer_decorator_no_args
class MySchema2(pw.Schema):
    foo: int
    bar: float


def test_table_transformer_decorator_no_args():
    class MySchema1(pw.Schema):
        foo: int

    class MySchema3(pw.Schema):
        foo: int
        bar: float
        baz: Any

    t1 = empty_from_schema(MySchema1)
    t2 = empty_from_schema(MySchema2)
    t3 = empty_from_schema(MySchema3)

    @pw.table_transformer
    def fun(
        a,
        b: int,
        c: pw.Table[MySchema2],
        *,
        d: pw.Table[MySchema2],
    ) -> pw.Table[MySchema2]:
        return a

    fun(t2, 5, t3, d=t2)
    # incorrect kwarg
    with pytest.raises(AssertionError):
        fun(t2, 5, t3, d=t1)
    # incorrect arg
    with pytest.raises(AssertionError):
        fun(t2, 5, t1, d=t2)
    # incorrect return value
    with pytest.raises(AssertionError):
        fun(t1, 5, t3, d=t2)


@pytest.mark.parametrize(
    "reducer, expected, expected_type",
    [
        (
            pw.reducers.tuple,
            [(1, 3), (3, 2), (3, 2, 9)],
            list[int],
        ),
        (
            pw.reducers.min,
            [1, 2, 2],
            int,
        ),
        (
            pw.reducers.any,
            [3, 2, 9],
            int,
        ),
    ],
)
def test_reducers_ix(reducer, expected, expected_type):
    values = T(
        """
        | v
    1   | 1
    2   | 2
    3   | 6
    4   | 3
    5   | 9
    """
    )
    t = T(
        """
        | t |  ptr
    1   | 1 |  4
    2   | 2 |  1
    3   | 3 |  4
    4   | 3 |  2
    5   | 2 |  4
    6   | 3 |  5
    7   | 1 |  2
    """
    ).select(pw.this.t, ptr=values.pointer_from(pw.this.ptr))
    result = t.groupby(t.t).reduce(v=reducer(values.ix(t.ptr).v))

    df = pd.DataFrame({"v": expected})
    expected = pw.debug.table_from_pandas(
        df,
        schema=pw.schema_from_types(v=expected_type),
    )

    assert_table_equality_wo_index(result, expected)


def test_pw_run_signature():
    assert inspect.signature(pw.run) == inspect.signature(pw.run_all)


def test_groupby_caching_doesnt_explode():
    # minimal failing example for a particular bug in caching
    tab = pw.Table.empty(a=int)
    tab.groupby(pw.this.a)
    tab.with_columns(b=0).groupby(pw.this.a).reduce(b=pw.reducers.any(pw.this.b))


def test_error_when_changing_incompatible_types():
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Cannot change type from INT to STR.\n"
            + "Table.update_types() should be used only for type narrowing or type extending.\n"
            + "Occurred here:\n"
            + "    Line: pw.Table.empty(foo=int).update_types(foo=str)\n"
        ),
    ):
        pw.Table.empty(foo=int).update_types(foo=str)


def test_error_when_wrong_indexing():
    tab = pw.Table.empty(a=int)
    index = tab.groupby(pw.this.a).reduce(*pw.this)
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Indexing a table with a Pointer type with probably mismatched primary keys."
            + " Type used was Pointer(INT, INT)."
            + " Indexed id type was Pointer(INT).\n"
            + "Occurred here:\n"
            + "    Line: index.ix_ref(tab.a, tab.a)\n"
        ),
    ):
        index.ix_ref(tab.a, tab.a)


def test_groupby_pointer_type():
    tab = pw.Table.empty(a=int)
    index = tab.groupby(pw.this.a).reduce()
    assert index.schema.id.dtype == dt.Pointer(dt.INT)


def test_concat_id_type_exception():
    a = pw.Table.empty().update_id_type(pw.Pointer[int])
    b = pw.Table.empty().update_id_type(pw.Pointer[str])
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Incompatible types for a concat operation.\n"
            + "The types are: (Pointer(INT), Pointer(STR)). You might try casting the id type to pw.Pointer"
            + " to circumvent this, but this is most probably an error.\n"
            + "Occurred here:\n"
            + "    Line: pw.Table.concat(a, b)\n"
        ),
    ):
        pw.Table.concat(a, b)


def test_concat_id_type_no_exception():
    a = pw.Table.empty().update_id_type(pw.Pointer[int])
    b = pw.Table.empty().update_id_type(pw.Pointer[Any])
    assert pw.Table.concat(a, b)._id_column.dtype.typehint == pw.Pointer[Any]


def test_concat_reindex_id_type_no_exception():
    a = pw.Table.empty().update_id_type(pw.Pointer[int])
    b = pw.Table.empty().update_id_type(pw.Pointer[str])
    assert pw.Table.concat_reindex(a, b)._id_column.dtype.typehint == pw.Pointer


def test_join_pointer_types_join_exception():
    a = pw.Table.empty(col=pw.Pointer[int])
    b = pw.Table.empty(col=pw.Pointer[str])
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Incompatible types in a join condition.\n"
            + "The types are: Pointer(INT) and Pointer(STR)."
            + " You might try casting the respective columns to Any type to circumvent this,"
            + " but this is most probably an error.\n"
            + "Occurred here:\n    "
            + "Line: a.join(b, a.col == b.col).select()\n"
        ),
    ):
        a.join(b, a.col == b.col).select()


def test_join_pointer_types_select_exception():
    a = pw.Table.empty(col=pw.Pointer[int])
    b = pw.Table.empty(col=pw.Pointer[str]).with_universe_of(a)
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Incompatible types in for a binary operator.\n"
            + "The types are: Pointer(INT) and Pointer(STR)."
            + " You might try casting the expressions to Any type to circumvent this,"
            + " but this is most probably an error.\n"
            + "Occurred here:\n    "
            + "Line: a.filter(a.col == b.col)\n"
        ),
    ):
        a.filter(a.col == b.col)


def test_join_pointer_int_float_types_select_exception():
    a = pw.Table.empty(col=pw.Pointer[int])
    b = pw.Table.empty(col=pw.Pointer[float]).with_universe_of(a)
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Incompatible types in for a binary operator.\n"
            + "The types are: Pointer(INT) and Pointer(FLOAT)."
            + " You might try casting the expressions to Any type to circumvent this,"
            + " but this is most probably an error.\n"
            + "Occurred here:\n    "
            + "Line: a.filter(a.col == b.col)\n"
        ),
    ):
        a.filter(a.col == b.col)


def test_join_pointer_types_no_exception():
    a = pw.Table.empty(col=pw.Pointer[int])
    b = pw.Table.empty(col=pw.Pointer[Any])
    assert a.join(b, a.col == b.col).select()._id_column.dtype.typehint == pw.Pointer


def test_warns_empty_univ():
    a = T(
        """col
            1"""
    )
    with pytest.warns(
        match=re.escape(
            "Found universe that is always empty, but wasn't declared as such"
            + " -- this is potentially a bug."
        )
    ):
        a.promise_universes_are_disjoint(a)


def test_no_warn():
    with warnings.catch_warnings():
        pw.Table.empty(foo=int)


def test_warns_no_second_warning():
    a = T(
        """col
            1"""
    )

    a.promise_universes_are_disjoint(a)
    with warnings.catch_warnings():
        a.promise_universes_are_disjoint(a)


def test_remove_retractions():
    t = T(
        """
        a | __time__ | __diff__
        1 |     2    |     1
        2 |     4    |     1
        3 |     6    |     1
        2 |     8    |    -1
        4 |    10    |     1
        3 |    12    |    -1
    """,
        id_from=["a"],
    )

    expected_with_retractions = T(
        """
        a
        1
        4
    """,
        id_from=["a"],
    )
    expected_without_retractions = T(
        """
        a
        1
        2
        3
        4
    """,
        id_from=["a"],
    )

    res = t._remove_retractions()

    assert_table_equality(
        (t, res),
        (expected_with_retractions, expected_without_retractions),
    )

    expected_stream = T(
        """
        a | __time__ | __diff__
        1 |     2    |     1
        2 |     4    |     1
        3 |     6    |     1
        4 |    10    |     1
    """,
        id_from=["a"],
    )

    assert_stream_equality(res, expected_stream)


@needs_multiprocessing_fork
def test_termination_after_fork():
    p = multiprocessing.Process(target=pw.run)
    p.start()
    try:
        p.join(timeout=1)
        assert p.exitcode == 0
    finally:
        p.terminate()
        p.join()


def test_dtype_pandas():
    assert (
        pw.debug.table_to_pandas(
            pw.debug.table_from_markdown(
                """
                a
                None
                1
                """
            )
        )["a"].dtype
        == pd.Int64Dtype()
    )


def test_warns_if_unused_operators():
    T(
        """
        a | b
        1 | 2
    """
    )
    with pytest.warns(
        UserWarning,
        match=re.escape(
            "There are operators in the computation graph that haven't been used."
            + " Use pathway.run() (or similar) to run the computation involving these nodes."
        ),
    ):
        warn_if_some_operators_unused()


def test_doesnt_warn_if_all_operators_used():
    t = T(
        """
        a | b
        1 | 2
    """
    )
    pw.debug.compute_and_print(t)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        warn_if_some_operators_unused()


def test_python_tuple_select():
    t = T(
        """
        a | b
        1 | 2
        3 | 4
        5 | 6
    """
    )
    t1 = t.select(c=(pw.this.a, pw.this.b), d=(pw.this.a, 2))
    t2 = t.select(c=(t.a, pw.this.b), d=(t.a, 2))
    t3 = t.select(c=(pw.this.a, t.b), d=(pw.this.a, 2))
    expected = t.select(
        c=pw.make_tuple(pw.this.a, pw.this.b), d=pw.make_tuple(pw.this.a, 2)
    )
    assert_table_equality(t1, expected)
    assert_table_equality(t2, expected)
    assert_table_equality(t3, expected)


def test_python_tuple_comparison():
    t = T(
        """
        a | b
        1 | 2
        4 | 3
        5 | 5
    """
    )
    t1 = t.select(
        x=pw.make_tuple(pw.this.a, pw.this.b) < (pw.this.b, pw.this.a),
        y=(pw.this.a, pw.this.b) < pw.make_tuple(pw.this.b, pw.this.a),
        z=pw.make_tuple(pw.this.a, pw.this.b) > (pw.this.b, pw.this.a),
        t=(pw.this.a, pw.this.b) > pw.make_tuple(pw.this.b, pw.this.a),
        e=pw.make_tuple(pw.this.a, pw.this.b) == (pw.this.b, pw.this.a),
        n=(pw.this.a, pw.this.b) != pw.make_tuple(pw.this.b, pw.this.a),
    )
    expected = T(
        """
            x |     y |     z |     t |     e |     n
         True |  True | False | False | False |  True
        False | False |  True |  True | False |  True
        False | False | False | False |  True | False
    """
    )

    assert_table_equality(t1, expected)


def test_python_tuple_inside_udf():
    t = T(
        """
        a | b | c
        1 | 2 | 3
        4 | 3 | 3
        5 | 5 | 3
    """
    )

    @pw.udf
    def foo(x: tuple) -> int:
        return sum(x)

    res = t.select(s=foo((pw.this.a, pw.this.c, pw.this.b)))

    expected = T(
        """
         s
         6
        10
        13
    """
    )
    assert_table_equality(res, expected)


def test_python_tuple_if_else():
    t = T(
        """
        a | b | c
        0 | 2 | 3
        1 | 3 | 0
        1 | 4 | 5
        0 | 5 | 2
    """
    )
    res = t.select(
        z=pw.if_else(
            pw.this.a == 1, (pw.this.b, pw.this.c), (pw.this.c, pw.this.b)
        ).get(0)
    )
    expected = T(
        """
        z
        3
        3
        4
        2
    """
    )
    assert_table_equality(res, expected)


def test_python_tuple_stateful_reducer():
    @pw.reducers.stateful_single  # type: ignore[arg-type]
    def sum2d(state: int | None, values: tuple[int, int]) -> int:
        if state is None:
            state = 0
        state += values[0] + values[1]
        return state

    t = T(
        """
        a | b
        1 | 2
        3 | 4
    """
    )
    res = t.reduce(s=sum2d((pw.this.a, pw.this.b)))
    expected = T(
        """
        s
        10
    """
    )
    assert_table_equality_wo_index_types(res, expected)


def test_python_tuple_sorting():
    t = T(
        """
        a | b | c
        1 | 3 | 2
        2 | 4 | 1
        3 | 3 | 6
        4 | 2 | 8
        5 | 5 | 6
        6 | 1 | 4
        7 | 2 | 2
        8 | 3 | 3
    """
    )
    sorted = t.sort(key=(pw.this.b, pw.this.c))
    result = t.select(pw.this.a, prev_a=t.ix(sorted.prev, optional=True).a)
    expected = T(
        """
        a | prev_a
        1 |      4
        2 |      3
        3 |      8
        4 |      7
        5 |      2
        6 |
        7 |      6
        8 |      1
    """
    )
    assert_table_equality(result, expected)


def test_table_to_pandas_with_id():
    t = T(
        """
      | a
    1 | 3
    2 | 6
    3 | 7
    """
    )

    df = table_to_pandas(t)
    expected = pd.DataFrame(
        {"a": [3, 6, 7]},
        index=[api.ref_scalar(1), api.ref_scalar(2), api.ref_scalar(3)],
    )
    assert all(df == expected)


def test_table_to_pandas_without_id():
    t = T(
        """
      | a
    1 | 3
    2 | 6
    3 | 7
    """
    )

    df = table_to_pandas(t, include_id=False)
    expected = pd.DataFrame({"a": [3, 6, 7]})
    assert all(df == expected)


def test_table_to_pandas_without_id_optional():
    t = T(
        """
      | a
    1 | 3
    2 |
    3 | 7
    """
    )

    df = table_to_pandas(t, include_id=False)
    expected = pd.DataFrame({"a": [3, None, 7]})
    assert all(df == expected)

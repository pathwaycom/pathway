# Copyright © 2024 Pathway

import copy
import datetime
import itertools
import operator
import re
from collections.abc import Callable, Mapping
from typing import Any

import numpy as np
import pandas as pd
import pytest
from dateutil import tz

import pathway as pw
from pathway.debug import table_from_pandas, table_to_pandas
from pathway.tests.utils import T, assert_table_equality, run_all


@pytest.mark.parametrize(
    "op_fun,data",
    [
        (operator.not_, [False, True]),
        (
            operator.neg,
            [
                -2,
                -1,
                0,
                1,
                2,
                3,
                4,
                5,
                90623803388717388,
                88814567067209860,
                -2502820103020854,
            ],
        ),
        (
            operator.neg,
            [
                -2,
                -1,
                0,
                1,
                2,
                3,
                4,
                5,
                90623803388717388,
                88814567067209860,
                -2502820103020854,
                0.69213224,
                -0.04078913,
                0.37567623,
                -0.53781894,
                0.71950524,
            ],
        ),
        (
            operator.neg,
            [
                pd.Timedelta(0),
                pd.Timedelta(-1),
                pd.Timedelta(-2),
                pd.Timedelta(1),
                pd.Timedelta(2),
                pd.Timedelta(milliseconds=3),
                pd.Timedelta(seconds=-2),
                pd.Timedelta(days=24),
                pd.Timedelta(weeks=-13),
                pd.Timedelta(-560647988758320624),
                pd.Timedelta(21569578082613316),
            ],
        ),
    ],
)
def test_unary(op_fun: Callable, data: list[Any]) -> None:
    df = pd.DataFrame({"a": data})
    table = table_from_pandas(df)
    if op_fun == operator.not_:
        table_pw = table.select(c=~table.a)
        df_new = pd.DataFrame({"c": ~df["a"]})
    else:
        table_pw = table.select(c=op_fun(table.a))
        df_new = pd.DataFrame({"c": op_fun(df["a"])})
    table_pd = table_from_pandas(df_new)
    assert_table_equality(table_pw, table_pd)


def _check_pandas_pathway_return_the_same(
    df: pd.DataFrame,
    op_fun: Any,
    dtypes: Mapping[str, type] = {},
    res_dtype: type | None = None,
):
    table = table_from_pandas(copy.deepcopy(df))
    table = table.update_types(**dtypes)
    table_pw = table.select(pw.this.a, pw.this.b, c=op_fun(pw.this.a, pw.this.b))
    df["c"] = op_fun(df["a"], df["b"])
    table_pd = table_from_pandas(df)
    if res_dtype:
        table_pd = table_pd.update_types(c=res_dtype)
    assert_table_equality(table_pw, table_pd)


@pytest.mark.parametrize("op_fun", [operator.and_, operator.or_, operator.xor])
def test_bool(op_fun: Any):
    df = pd.DataFrame(
        {"a": [False, False, True, True], "b": [False, True, False, True]}
    )
    _check_pandas_pathway_return_the_same(df, op_fun)


@pytest.mark.parametrize(
    "op_fun",
    [
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        operator.add,
        operator.sub,
        operator.mul,
        operator.floordiv,
        operator.truediv,
        operator.mod,
        operator.and_,
        operator.or_,
        operator.xor,
    ],
)
def test_int(op_fun: Any):
    pairs = np.array(
        [
            [-2, 0],
            [-1, 3],
            [0, 1],
            [1, 10],
            [2, -9],
            [3, 8],
            [4, -7],
            [5, 6],
            [-331399, -227463],
            [253173, -207184],
            [-741012, -856821],
            [-935893, 341112],
            [-284786, -559808],
            [825347, 802488],
            [-778696, 740473],
            [-763723, 431098],
            [-980333, 562122],
            [12035, 846654],
            [490378, -106109],
            [-93465, -348397],
            [262849, -473516],
            [908064, 450927],
            [217134, 217134],
            [10, 10],
            [-10, -3],
            [-10, 3],
            [10, -3],
            [10, 3],
        ],
        dtype=np.int64,
    )
    df = pd.DataFrame({"a": pairs[:, 0], "b": pairs[:, 1]})
    if op_fun in (operator.floordiv, operator.truediv, operator.mod):
        df.loc[df["b"] == 0, "b"] = 1
    _check_pandas_pathway_return_the_same(df, op_fun)


@pytest.mark.parametrize(
    "op_fun",
    [
        operator.floordiv,
        operator.truediv,
        operator.mod,
    ],
)
def test_int_div_zero(op_fun: Any):
    pairs = np.array([[1, 0], [10000, 0], [-1, 0], [0, 0], [-9829480, 0]]).reshape(
        -1, 2, 1
    )
    for pair in pairs:
        df = pd.DataFrame({"a": pair[0], "b": pair[1]})
        table = table_from_pandas(df)
        table.select(c=op_fun(pw.this["a"], pw.this["b"]))

        with pytest.raises(ZeroDivisionError):
            run_all()


@pytest.mark.parametrize(
    "op_fun",
    [operator.pow, operator.lshift, operator.rshift],
)
def test_int_pow_shift(op_fun: Any):
    pairs = np.array(
        [
            [0, 1],
            [0, 2],
            [0, 63],
            [1, 0],
            [1, 1],
            [1, 2],
            [1, 3],
            [1, 62],
            [2, 0],
            [2, 1],
            [2, 2],
            [2, 61],
            [3, 0],
            [3, 1],
            [3, 2],
            [3, 39],
            [4, 0],
            [4, 1],
            [4, 31],
            [9, 18],
            [10, 18],
            [14, 16],
            [23, 13],
            [-1, 0],
            [-1, 1],
            [-1, 2],
            [-1, 3],
            [-1, 62],
            [-1, 63],
            [-2, 0],
            [-2, 1],
            [-2, 2],
        ],
        dtype=np.int64,
    )
    df = pd.DataFrame({"a": pairs[:, 0], "b": pairs[:, 1]})
    table = table_from_pandas(df)
    table_pw = table.select(c=op_fun(pw.this["a"], pw.this["b"]))
    result = op_fun(pairs[:, 0], pairs[:, 1])
    df_new = pd.DataFrame({"c": result})
    table_pd = table_from_pandas(df_new)
    assert_table_equality(table_pw, table_pd)


@pytest.mark.parametrize(
    "op_fun",
    [
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        operator.add,
        operator.sub,
        operator.mul,
        operator.floordiv,
        operator.truediv,
        operator.mod,
    ],
)
def test_float(op_fun: Any):
    pairs = np.array(
        [
            [-2, 0],
            [-1, 3],
            [0, 1],
            [1, 10],
            [2, -9],
            [3, 8],
            [4, -7],
            [5, 6],
            [-331399, -227463],
            [253173, -207184],
            [-741012, -856821],
            [-935893, 341112],
            [-284786, -559808],
            [825347, 802488],
            [-778696, 740473],
            [-763723, 431098],
            [-980333, 562122],
            [12035, 846654],
            [490378, -106109],
            [-93465, -348397],
            [262849, -473516],
            [908064, 450927],
            [217134, 217134],
            [10, 10],
            [-10, -3],
            [-10, 3],
            [10, -3],
            [10, 3],
            [0, 1],
            [0, 2],
            [0, 63],
            [1, 0],
            [1, 1],
            [1, 2],
            [1, 3],
            [1, 62],
            [2, 0],
            [2, 1],
            [2, 2],
            [2, 61],
            [3, 0],
            [3, 1],
            [3, 2],
            [3, 39],
            [4, 0],
            [4, 1],
            [4, 31],
            [9, 18],
            [10, 18],
            [14, 16],
            [23, 13],
            [-1, 0],
            [-1, 1],
            [-1, 2],
            [-1, 3],
            [-1, 62],
            [-1, 63],
            [-2, 0],
            [-2, 1],
            [-2, 2],
            [-0.24548424, -0.56365047],
            [-0.10302759, 0.58539945],
            [0.03638815, -0.1907964],
            [0.69213224, -0.04078913],
            [0.37567623, -0.53781894],
            [0.71950524, -0.53945945],
            [-0.90912489, -0.16098464],
            [-0.52092329, -0.21976854],
            [-0.33810194, -0.52811729],
            [-0.57169316, -0.02546098],
            [1.5, 0],
            [1.5, -1],
            [1.5, -2],
            [1.5, 2],
            [0.69213224, -10.3],
        ],
        dtype=np.float64,
    )
    df = pd.DataFrame({"a": pairs[:, 0], "b": pairs[:, 1]})
    if op_fun in (operator.floordiv, operator.truediv, operator.mod):
        mask = df["b"] == 0.0
        df.loc[mask, "b"] = 1.0
    _check_pandas_pathway_return_the_same(df, op_fun)


def assert_relative_error_small(table: pd.Series, expected: pd.Series):
    assert np.isclose(table.to_numpy(), expected.to_numpy(), rtol=1e-15, atol=0.0).all()


def test_float_pow():
    pairs = np.array(
        [
            [-2, 0],
            [-1, 3],
            [0, 1],
            [1, 10],
            [2, -9],
            [3, 8],
            [4, -7],
            [5, 6],
            [-331399, 55],
            [253173, 56],
            [-741012, 51],
            [-935893, -51],
            [-284786, 55],
            [825347, 51],
            [-778696, 51],
            [-763723, 51],
            [-980333, 50],
            [12035, 74],
            [490378, 53],
            [-93465, 61],
            [262849, -56],
            [908064, 51],
            [217134, 57],
            [10, 10],
            [-10, -3],
            [-10, 3],
            [10, -3],
            [10, 3],
            [0, 1],
            [0, 2],
            [0, 63],
            [1, 0],
            [1, 1],
            [1, 2],
            [1, 3],
            [1, 62],
            [2, 0],
            [2, 1],
            [2, 2],
            [2, 61],
            [3, 0],
            [3, 1],
            [3, 2],
            [3, 39],
            [4, 0],
            [4, 1],
            [4, 31],
            [9, 18],
            [10, 18],
            [14, 16],
            [23, 13],
            [-1, 0],
            [-1, 1],
            [-1, 2],
            [-1, 3],
            [-1, 62],
            [-1, 63],
            [-2, 0],
            [-2, 1],
            [-2, 2],
            [0.2454, -0.5636],
            [0.1030, 0.5853],
            [0.0363, 0.1907],
            [0.6921, -0.0407],
            [0.3756, -0.5378],
            [0.7195, 0.5394],
            [0.9091, -0.1609],
            [0.5209, -0.2197],
            [0.3381, 0.5281],
            [0.5716, -0.0254],
            [1.5, 0],
            [1.5, -1],
            [1.5, -2],
            [1.5, 2],
            [0.6921, -10.3],
        ],
        dtype=np.float64,
    )
    df = pd.DataFrame({"a": pairs[:, 0], "b": pairs[:, 1]})
    table = table_from_pandas(df)
    table = table.select(pw.this.a, pw.this.b, c=pw.this.a**pw.this.b)
    df["c"] = df["a"] ** df["b"]
    df = df.sort_values(by=["a", "b"])
    df_pw = table_to_pandas(table)
    df_pw = df_pw.sort_values(by=["a", "b"])
    assert_relative_error_small(df_pw["c"], df["c"])


@pytest.mark.parametrize(
    "op_fun",
    [
        operator.floordiv,
        operator.truediv,
        operator.mod,
    ],
)
def test_float_div_zero(op_fun: Any):
    pairs = np.array(
        [
            [1, 0],
            [10000, 0],
            [-1, 0],
            [0, 0],
            [-9829480, 0],
            [12.452, 0.0],
            [0.001, 0.0],
        ],
        dtype=np.float64,
    ).reshape(-1, 2, 1)
    for pair in pairs:
        df = pd.DataFrame({"a": pair[0], "b": pair[1]})
        table = table_from_pandas(df)
        table.select(c=op_fun(pw.this["a"], pw.this["b"]))

        with pytest.raises(ZeroDivisionError):
            run_all()


@pytest.mark.parametrize("reverse", [True, False])
@pytest.mark.parametrize(
    "op_fun",
    [
        operator.add,
        operator.sub,
        operator.mul,
        operator.floordiv,
        operator.truediv,
        operator.mod,
    ],
)
def test_mixed_int_float(op_fun: Any, reverse: bool):
    ints = [-741012, -331399, -10, -9, -2, -1, 0, 1, 2, 3, 4, 5, 10, 11, 253173, 980333]
    floats = [
        -10.0,
        -3.0,
        -2.0,
        -1.0,
        0.0,
        1.0,
        2.0,
        3.0,
        10.0,
        -227463.0,
        450927.0,
        -1.9758,
        -0.2261,
        0.0177,
        0.123406802,
        0.7423,
    ]
    pairs = list(itertools.product(ints, floats))
    pairs_T = list(zip(*pairs))
    if reverse:
        df = pd.DataFrame({"a": pairs_T[1], "b": pairs_T[0]})
    else:
        df = pd.DataFrame({"a": pairs_T[0], "b": pairs_T[1]})
    if op_fun in (operator.floordiv, operator.truediv, operator.mod):
        if reverse:
            df.loc[df["b"] == 0, "b"] = 1
        else:
            df.loc[df["b"] == 0.0, "b"] = 1.0
    _check_pandas_pathway_return_the_same(df, op_fun)


@pytest.mark.parametrize("reverse", [True, False])
def test_mixed_int_float_pow(reverse: bool):
    ints = [-3, -2, -1, 1, 2, 3, 12, 13, 123456]
    floats = [-2.0, -1.0, -0.5636, 0.6853, 1.0, 2.0]
    pairs = list(itertools.product(ints, floats))

    pairs_T = list(zip(*pairs))
    if reverse:
        df = pd.DataFrame({"a": pairs_T[1], "b": pairs_T[0]})
    else:
        df = pd.DataFrame({"a": pairs_T[0], "b": pairs_T[1]})
    if not reverse:
        df["a"] = abs(df["a"])
    table = table_from_pandas(df)
    table = table.select(pw.this.a, pw.this.b, c=pw.this.a**pw.this.b)
    df["c"] = df["a"] ** df["b"]
    df = df.sort_values(by=["a", "b"])
    df_pw = table_to_pandas(table)
    df_pw = df_pw.sort_values(by=["a", "b"])
    assert_relative_error_small(df_pw["c"], df["c"])


@pytest.mark.parametrize(
    "op_fun",
    [
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        operator.add,
    ],
)
def test_string(op_fun: Any):
    pairs = np.array(
        [
            ["", ""],
            ["", "a"],
            ["a", ""],
            ["a", "a"],
            ["aaaaa", "a"],
            ["aaaaa", "aaaaa"],
            ["aaaaa", "aaaab"],
            ["", "jnbewoifnq"],
            ["123", "14"],
            ["!%@%@", "$^&^"],
            ["/**/", "%%%"],
            ['E iRR5}KPtz$R$"t&mMW', "{A?vUmy"],
            ["*e[mX%rhI(p<.X", ""],
            ["(`-=,~j?uA_E-'{4", "EvDj@o1Ok@=!A>%&Oa"],
            ["{rC3#?y7AB{)pL>%[A(", "VoBAiF"],
            ["5I^We<N.KfQ3fH#@c)~", "`[Q"],
            ["B|vQL!!MSaZ(n%K;q%:", "y&dxBaCn<rBI !"],
            ['z0U\\?jmoz_.+1W"Y[OIv', "68lWwz|/O@_s"],
            ["g{:", '7KdUc4JvQo"hrpYa'],
            ["HV", "fc9%Wtvf"],
            ["D$<edj8m@L-", 'Vn`E^V)"J23;-(]qg'],
            ['z0U\\?jmoz_.+1W"Y[OIv', 'z0U\\?jmoz_.+1W"Y[OIv'],
        ]
    )
    df = pd.DataFrame({"a": pairs[:, 0], "b": pairs[:, 1]}, dtype=str)
    dtypes = {"a": str, "b": str}
    _check_pandas_pathway_return_the_same(df, op_fun, dtypes)


@pytest.mark.parametrize("reverse_columns", [False, True])
def test_string_mul(reverse_columns: bool):
    pairs = [
        ["", 0],
        ["", 1],
        ["", 2],
        ["", -1],
        ["a", 0],
        ["a", 1],
        ["a", 2],
        ["a", 10],
        ["a", -10],
        ["aaaaa", 0],
        ["aaaaa", 20],
        ["aaaaa", -20],
        ["jnbewoifnq", 13],
        ["123", 2],
        ["321", 10],
        ["oemcoejo", 1000000],
        ["oemcoejo", 999999],
        ["/**/", 1000001],
        ['E iRR5}KPtz$R$"t&mMW', 4],
        ["*e[mX%rhI(p<.X", 3],
        ["(`-=,~j?uA_E-'{4", 4213],
        ["{rC3#?y7AB{)pL>%[A(", 532],
        ["5I^We<N.KfQ3fH#@c)~", 214],
        ["B|vQL!!MSaZ(n%K;q%:", -10],
        ['z0U\\?jmoz_.+1W"Y[OIv', 5],
        ["g{:", 12345],
        ["HV", 54321],
        ["D$<edj8m@L-", 0],
        ['z0U\\?jmoz_.+1W"Y[OIv', 2000000],
    ]
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame({"a": pairs_T[0], "b": pairs_T[1]})
    dtypes = {"a": str}
    if reverse_columns:
        df["a"], df["b"] = df["b"], df["a"]
        dtypes = {"b": str}
    _check_pandas_pathway_return_the_same(df, operator.mul, dtypes, str)


def test_pointer_eq():
    t = T(
        """
       | true_id | false_id
    1  | 1       |  2
    2  | 2       |  3
    3  | 3       |  1
    """
    )
    t = t.select(
        *pw.this,
        true_pointer=pw.this.pointer_from(t.true_id),
        false_pointer=pw.this.pointer_from(t.false_id),
    )
    res = t.select(
        a=(t.id == t.true_pointer),
        b=(t.id == t.false_pointer),
        c=(t.id != t.true_pointer),
        d=(t.id != t.false_pointer),
    )
    expected = T(
        """
       |   a  |   b   |   c   |   d
    1  | True | False | False | True
    2  | True | False | False | True
    3  | True | False | False | True
    """
    )
    assert_table_equality(res, expected)


def test_pointer_order():
    t = T(
        """
    ptrA | ptrB | ptrC
       1 |   11 |   21
       2 |   12 |   22
       3 |   13 |   23
       4 |   14 |   24
       5 |   15 |   25
    """
    ).with_columns(
        ptrA=pw.this.pointer_from(pw.this.ptrA),
        ptrB=pw.this.pointer_from(pw.this.ptrB),
        ptrC=pw.this.pointer_from(pw.this.ptrC),
    )
    res = t.select(
        a1=(t.ptrA < t.ptrB) == (t.ptrA <= t.ptrB),
        a2=(t.ptrA > t.ptrB) == (t.ptrA >= t.ptrB),
        a3=(t.ptrB < t.ptrC) == (t.ptrB <= t.ptrC),
        a4=(t.ptrB > t.ptrC) == (t.ptrB >= t.ptrC),
        a5=(t.ptrA < t.ptrC) == (t.ptrA <= t.ptrC),
        a6=(t.ptrA > t.ptrC) == (t.ptrA >= t.ptrC),
        b1=(t.ptrA < t.ptrB) != (t.ptrA > t.ptrB),
        b2=(t.ptrA < t.ptrC) != (t.ptrA > t.ptrC),
        b3=(t.ptrB < t.ptrC) != (t.ptrB > t.ptrC),
        # <= below on bools is -> implies
        c1=((t.ptrA < t.ptrB) & (t.ptrB < t.ptrC)) <= (t.ptrA < t.ptrC),
        c2=((t.ptrA < t.ptrC) & (t.ptrC < t.ptrB)) <= (t.ptrA < t.ptrB),
        c3=((t.ptrB < t.ptrA) & (t.ptrA < t.ptrC)) <= (t.ptrB < t.ptrC),
        c4=((t.ptrB < t.ptrC) & (t.ptrC < t.ptrA)) <= (t.ptrB < t.ptrA),
        c5=((t.ptrC < t.ptrA) & (t.ptrA < t.ptrB)) <= (t.ptrC < t.ptrB),
        c6=((t.ptrC < t.ptrB) & (t.ptrB < t.ptrA)) <= (t.ptrC < t.ptrA),
    )

    expected = t.select(
        a1=True,
        a2=True,
        a3=True,
        a4=True,
        a5=True,
        a6=True,
        b1=True,
        b2=True,
        b3=True,
        c1=True,
        c2=True,
        c3=True,
        c4=True,
        c5=True,
        c6=True,
    )
    assert_table_equality(res, expected)


@pytest.mark.parametrize(
    "op_fun",
    [
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        operator.add,
        operator.sub,
        operator.floordiv,
        operator.truediv,
        operator.mod,
    ],
)
def test_duration(op_fun: Any) -> None:
    pairs = [
        [pd.Timedelta(0), pd.Timedelta(0)],
        [pd.Timedelta(1), pd.Timedelta(0)],
        [pd.Timedelta(0), pd.Timedelta(1)],
        [pd.Timedelta(2), pd.Timedelta(1)],
        [pd.Timedelta(2), pd.Timedelta(0)],
        [pd.Timedelta(2), pd.Timedelta(-1)],
        [pd.Timedelta(-2), pd.Timedelta(-2)],
        [pd.Timedelta(-331399), pd.Timedelta(-227463)],
        [pd.Timedelta(253173), pd.Timedelta(-207184)],
        [pd.Timedelta(-741012), pd.Timedelta(-856821)],
        [pd.Timedelta(-935893), pd.Timedelta(341112)],
        [pd.Timedelta(-284786), pd.Timedelta(-559808)],
        [pd.Timedelta(825347), pd.Timedelta(802488)],
        [pd.Timedelta(-778696), pd.Timedelta(740473)],
        [pd.Timedelta(-763723), pd.Timedelta(431098)],
        [pd.Timedelta(-980333), pd.Timedelta(562122)],
        [pd.Timedelta(milliseconds=1), pd.Timedelta(milliseconds=2)],
        [pd.Timedelta(milliseconds=-2), pd.Timedelta(milliseconds=3)],
        [pd.Timedelta(seconds=1), pd.Timedelta(seconds=2)],
        [pd.Timedelta(seconds=-2), pd.Timedelta(seconds=3)],
        [pd.Timedelta(minutes=1), pd.Timedelta(minutes=2)],
        [pd.Timedelta(minutes=-2), pd.Timedelta(minutes=3)],
        [pd.Timedelta(hours=1), pd.Timedelta(hours=2)],
        [pd.Timedelta(hours=-2), pd.Timedelta(hours=3)],
        [pd.Timedelta(days=1), pd.Timedelta(days=2)],
        [pd.Timedelta(days=-2), pd.Timedelta(days=3)],
        [pd.Timedelta(weeks=1), pd.Timedelta(weeks=2)],
        [pd.Timedelta(weeks=-2), pd.Timedelta(weeks=3)],
        [pd.Timedelta(weeks=1), pd.Timedelta(seconds=2)],
        [pd.Timedelta(weeks=-2), pd.Timedelta(seconds=3)],
    ]

    pairs_T = list(zip(*pairs))
    df = pd.DataFrame({"a": pairs_T[0], "b": pairs_T[1]})
    if op_fun in (operator.floordiv, operator.truediv, operator.mod):
        df.loc[df["b"] == pd.Timedelta(0), "b"] = pd.Timedelta(1)
    _check_pandas_pathway_return_the_same(df, op_fun)


@pytest.mark.parametrize(
    "op_fun",
    [
        operator.floordiv,
        operator.truediv,
        operator.mod,
    ],
)
def test_duration_div_zero(op_fun: Any) -> None:
    pairs = [
        [pd.Timedelta(-763723)],
        [pd.Timedelta(-980333)],
        [pd.Timedelta(milliseconds=1)],
    ]
    for pair in pairs:
        df = pd.DataFrame({"a": [pair[0]], "b": [pd.Timedelta(0)]})
        table = table_from_pandas(df)
        table.select(c=op_fun(pw.this["a"], pw.this["b"]))

        with pytest.raises(ZeroDivisionError):
            run_all()


@pytest.mark.parametrize("is_naive", [True, False])
@pytest.mark.parametrize(
    "op_fun",
    [
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        operator.sub,
    ],
)
def test_date_time(op_fun: Any, is_naive: bool) -> None:
    pairs = [
        ["1960-02-03 08:00:00.000000000", "2023-03-25 16:43:21.123456789"],
        ["2008-02-29 08:00:00.000000000", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 12:00:00.000000000", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 12:00:00.000000001", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 12:00:00.123456789", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 16:43:21.123456788", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 16:43:21.123456789", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 17:00:01.987000000", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 18:43:21.123456789", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 22:59:59.999999999", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 23:00:00.000000001", "2023-03-25 16:43:21.123456789"],
        ["2023-03-25 23:59:59.999999999", "2023-03-25 16:43:21.123456789"],
        ["2023-03-26 00:00:00.000000001", "2023-03-25 16:43:21.123456789"],
        ["2023-03-26 12:00:00.000000001", "2023-03-25 16:43:21.123456789"],
        ["2123-03-26 12:00:00.000000001", "2023-03-25 16:43:21.123456789"],
        ["2123-03-31 23:00:00.000000001", "2023-03-25 16:43:21.123456789"],
    ]
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    if not is_naive:
        fmt += "%z"
        pairs = [[a + "+01:30", b + "-00:30"] for a, b in pairs]
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame(
        {
            "a": pd.to_datetime(pairs_T[0], format=fmt),
            "b": pd.to_datetime(pairs_T[1], format=fmt),
        }
    )
    _check_pandas_pathway_return_the_same(df, op_fun)


@pytest.mark.parametrize("is_naive", [True, False])
@pytest.mark.parametrize(
    "op_fun",
    [
        operator.add,
        operator.sub,
    ],
)
def test_date_time_and_duration(op_fun: Any, is_naive: bool) -> None:
    pairs = [
        ["1960-02-03 08:00:00.000000000", pd.Timedelta(-1)],
        ["2008-02-29 08:00:00.000000000", pd.Timedelta(1)],
        ["2023-03-25 12:00:00.000000000", pd.Timedelta(825347)],
        ["2023-03-25 12:00:00.000000001", pd.Timedelta(249333862623082067)],
        ["2023-03-25 12:00:00.123456789", pd.Timedelta(-462593511970998050)],
        ["2023-03-25 16:43:21.123456788", pd.Timedelta(days=3)],
        ["2023-03-25 16:43:21.123456789", pd.Timedelta(hours=20)],
        ["2023-03-25 17:00:01.987000000", pd.Timedelta(weeks=12)],
        ["2023-03-25 18:43:21.123456789", pd.Timedelta(days=-10)],
        ["2023-03-25 22:59:59.999999999", pd.Timedelta(hours=-34)],
        ["2023-03-25 23:00:00.000000001", pd.Timedelta(minutes=-3)],
        ["2023-03-25 23:59:59.999999999", pd.Timedelta(1)],
        ["2023-03-26 00:00:00.000000001", pd.Timedelta(-1345)],
        ["2023-03-26 01:59:59.999999999", pd.Timedelta(hours=-1)],
        ["2023-03-26 01:59:59.999999999", pd.Timedelta(-2)],
        ["2023-03-26 01:59:59.999999999", pd.Timedelta(-1)],
        ["2023-03-26 01:59:59.999999999", pd.Timedelta(1)],
        ["2023-03-26 01:59:59.999999999", pd.Timedelta(2)],
        ["2023-03-26 01:59:59.999999999", pd.Timedelta(hours=1)],
        ["2023-03-26 03:00:00.000000001", pd.Timedelta(hours=-1)],
        ["2023-03-26 03:00:00.000000001", pd.Timedelta(-2)],
        ["2023-03-26 03:00:00.000000001", pd.Timedelta(-1)],
        ["2023-03-26 03:00:00.000000001", pd.Timedelta(1)],
        ["2023-03-26 03:00:00.000000001", pd.Timedelta(1)],
        ["2023-03-26 03:00:00.000000001", pd.Timedelta(hours=1)],
        ["2023-03-26 12:00:00.000000001", pd.Timedelta(seconds=1)],
        ["2123-03-26 12:00:00.000000001", pd.Timedelta(seconds=-971716231)],
        ["2123-03-31 23:00:00.000000001", pd.Timedelta(0)],
    ]
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame(
        {
            "a": pd.to_datetime(pairs_T[0], format=fmt),
            "b": pairs_T[1],
        }
    )
    if not is_naive:
        df["a"] = df["a"].dt.tz_localize(tz.UTC)
    _check_pandas_pathway_return_the_same(df, op_fun)
    if op_fun == operator.add:
        df["a"], df["b"] = df["b"], df["a"]
        _check_pandas_pathway_return_the_same(df, op_fun)


@pytest.mark.parametrize(
    "op_fun,dtype",
    [
        (operator.mul, int),
        (operator.floordiv, int),
        (operator.truediv, int),
        (operator.mul, float),
        (operator.truediv, float),
    ],
)
def test_duration_and_int(op_fun: Any, dtype: Any) -> None:
    pairs = [
        [pd.Timedelta(0), 0],
        [pd.Timedelta(1), 0],
        [pd.Timedelta(0), 1],
        [pd.Timedelta(2), 1],
        [pd.Timedelta(2), 0],
        [pd.Timedelta(2), -1],
        [pd.Timedelta(-2), -2],
        [pd.Timedelta(10), 3],
        [pd.Timedelta(10), -3],
        [pd.Timedelta(-10), 3],
        [pd.Timedelta(-10), -3],
        [pd.Timedelta(11), 3],
        [pd.Timedelta(11), -3],
        [pd.Timedelta(-11), 3],
        [pd.Timedelta(-11), -3],
        [pd.Timedelta(-331399), -227463],
        [pd.Timedelta(253173), -207184],
        [pd.Timedelta(-741012), -856821],
        [pd.Timedelta(-935893), 341112],
        [pd.Timedelta(-284786), -559808],
        [pd.Timedelta(825347), 802488],
        [pd.Timedelta(-778696), 740473],
        [pd.Timedelta(-763723), 431098],
        [pd.Timedelta(-980333), 562122],
        [pd.Timedelta(milliseconds=1), -96],
        [pd.Timedelta(milliseconds=-2), 88],
        [pd.Timedelta(seconds=1), -3],
        [pd.Timedelta(seconds=-2), -60],
        [pd.Timedelta(minutes=1), 54],
        [pd.Timedelta(minutes=-2), 44],
        [pd.Timedelta(hours=1), -31],
        [pd.Timedelta(hours=-2), 60],
        [pd.Timedelta(days=1), -91],
        [pd.Timedelta(days=-2), 28],
        [pd.Timedelta(weeks=1), -90],
        [pd.Timedelta(weeks=-2), -65],
        [pd.Timedelta(weeks=1), 10],
        [pd.Timedelta(weeks=-2), -45],
    ]
    if op_fun in {operator.floordiv, operator.truediv}:
        pairs = [[a, b if b != 0 else 1] for a, b in pairs]
    expected = table_from_pandas(
        pd.DataFrame({"c": [op_fun(a, dtype(b)) for a, b in pairs]})
    )
    # computing manually because in pandas when operating on columns
    # (pd.Timedelta(days=-2) // 28).value == -6171428571428, but should be
    # -6171428571429. Suprisingly, working on single values but not on columns
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame({"a": pairs_T[0], "b": pairs_T[1]})
    df["b"] = df["b"].astype(dtype)
    table = table_from_pandas(df)
    result = table.select(c=op_fun(table.a, table.b))
    assert_table_equality(result, expected)
    if op_fun == operator.mul:
        result_2 = table.select(c=op_fun(table.b, table.a))
        assert_table_equality(result_2, expected)


def test_duration_and_div_zero() -> None:
    pairs = [
        [pd.Timedelta(-763723)],
        [pd.Timedelta(-980333)],
        [pd.Timedelta(milliseconds=1)],
    ]
    for pair in pairs:
        df = pd.DataFrame({"a": [pair[0]], "b": [0]})
        table = table_from_pandas(df)
        table.select(c=pw.this["a"] // pw.this["b"])

        with pytest.raises(ZeroDivisionError):
            run_all()


@pytest.mark.parametrize(
    "const",
    [
        datetime.datetime(2023, 5, 15, 10, 51),
        pd.Timestamp(2023, 5, 15, 10, 51),
        datetime.timedelta(days=1),
        pd.Timedelta(days=1),
    ],
)
def test_datetime_naive_sub_const(const: Any) -> None:
    datetimes = [
        "2023-05-15 01:59:59.999999999",
        "2023-05-15 11:59:59.999999999",
    ]
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    df = pd.DataFrame({"a": datetimes})
    table = table_from_pandas(df)
    table_with_dt = table.select(a=table.a.dt.strptime(fmt))
    table_pw = table_with_dt.select(a=table_with_dt.a - const)

    df_new = pd.DataFrame({"a": pd.to_datetime(datetimes, format=fmt) - const})
    table_pd = table_from_pandas(df_new)

    assert_table_equality(table_pw, table_pd)


@pytest.mark.parametrize(
    "const",
    [
        datetime.datetime(2023, 5, 15, 10, 51, tzinfo=tz.UTC),
        datetime.datetime(2023, 5, 15, 10, 51, tzinfo=tz.gettz("America/New_York")),
        datetime.datetime(2023, 5, 15, 10, 51, tzinfo=tz.gettz("Europe/Warsaw")),
        pd.Timestamp(2023, 5, 15, 10, 51).tz_localize(tz.UTC),
        pd.Timestamp(2023, 5, 15, 10, 51).tz_localize("America/New_York"),
        pd.Timestamp(2023, 5, 15, 10, 51).tz_localize("Europe/Warsaw"),
        datetime.timedelta(days=1),
        datetime.timedelta(microseconds=1),
        datetime.timedelta(seconds=1),
        datetime.timedelta(minutes=1),
        datetime.timedelta(hours=1),
        datetime.timedelta(weeks=1),
        pd.Timedelta(days=1),
        pd.Timedelta(milliseconds=1),
    ],
)
def test_datetime_utc_sub_const(const: Any) -> None:
    datetimes = [
        "2023-05-15 01:59:59.999999999-02:00",
        "2023-05-15 11:59:59.999999999-02:00",
        "2023-05-15 12:51:00.000000000-02:00",
    ]
    fmt = "%Y-%m-%d %H:%M:%S.%f%z"
    df = pd.DataFrame({"a": datetimes})
    table = table_from_pandas(df)
    table_with_dt = table.select(a=table.a.dt.strptime(fmt))
    table_pw = table_with_dt.select(a=table_with_dt.a - const)

    df_new = pd.DataFrame({"a": pd.to_datetime(datetimes, format=fmt) - const})
    table_pd = table_from_pandas(df_new)

    assert_table_equality(table_pw, table_pd)


def run_matrix_multiplcation(
    pairs: list[tuple[np.ndarray, np.ndarray]], dtype: type
) -> None:
    pairs_T = list(zip(*pairs))
    a = [a_i.astype(dtype) for a_i in pairs_T[0]]
    b = [b_i.astype(dtype) for b_i in pairs_T[1]]
    t = table_from_pandas(pd.DataFrame({"a": a, "b": b, "i": list(range(len(a)))}))
    res = t.select(pw.this.i, c=t.a @ t.b)
    res_pd = table_to_pandas(res).sort_values(by="i")["c"]
    expected = [a_i @ b_i for a_i, b_i in zip(a, b)]
    for res_i, exp_i in zip(res_pd, expected):
        if dtype == float:
            assert np.isclose(res_i, exp_i, rtol=1e-15, atol=0.0).all()
        else:
            assert (res_i == exp_i).all()
        assert res_i.shape == exp_i.shape


@pytest.mark.parametrize("dtype", [int, float])
def test_matrix_multiplication_2d_by_2d(dtype: type) -> None:
    np.random.seed(42)
    r = np.random.randn
    pairs: list[tuple[np.ndarray, np.ndarray]] = [
        (r(3, 3), r(3, 3)),
        (r(4, 2), r(2, 3)),
        (r(4, 1), r(1, 4)),
        (r(1, 3), r(3, 1)),
        (r(0, 4), r(4, 5)),
        (r(0, 0), r(0, 1)),
        (r(0, 0), r(0, 0)),
        (r(0, 2), r(2, 0)),
        (np.array([[1, 2], [3, 4], [5, 6]]), np.array([[1, 2], [3, 4]])),
    ]
    run_matrix_multiplcation(pairs, dtype)


@pytest.mark.parametrize("dtype", [int, float])
def test_matrix_multiplication_2d_by_1d(dtype: type) -> None:
    np.random.seed(42)
    r = np.random.randn
    pairs: list[tuple[np.ndarray, np.ndarray]] = [
        (r(3, 3), r(3)),
        (r(4, 2), r(2)),
        (r(4, 4), r(4)),
        (r(1, 3), r(3)),
        (r(4, 0), r(0)),
        (r(0, 2), r(2)),
        (np.array([[1, 2], [3, 4], [5, 6]]), np.array([1, 2])),
    ]
    run_matrix_multiplcation(pairs, dtype)


@pytest.mark.parametrize("dtype", [int, float])
def test_matrix_multiplication_1d_by_2d(dtype: type) -> None:
    np.random.seed(42)
    r = np.random.randn
    pairs: list[tuple[np.ndarray, np.ndarray]] = [
        (r(3), r(3, 3)),
        (r(2), r(2, 3)),
        (r(2), r(2, 4)),
        (r(3), r(3, 1)),
        (r(0), r(0, 3)),
        (r(3), r(3, 0)),
        (np.array([1, 2]), np.array([[1, 2], [3, 4]])),
    ]
    run_matrix_multiplcation(pairs, dtype)


@pytest.mark.parametrize("dtype", [int, float])
def test_matrix_multiplication_1d_by_1d(dtype: type) -> None:
    pairs: list[tuple[np.ndarray, np.ndarray]] = [
        (np.ones(2), np.ones(2)),
        (np.ones(3), np.ones(3)),
        (np.ones(4), np.ones(4)),
        (np.ones(0), np.ones(0)),
        (np.array([1, 2]), np.array([1, 2])),
    ]
    run_matrix_multiplcation(pairs, dtype)


@pytest.mark.xfail(reason="Multidimensional matrix multiplication not supported")
@pytest.mark.parametrize("dtype", [int, float])
def test_matrix_multiplication_multidimensional(dtype: type) -> None:
    np.random.seed(42)
    r = np.random.randn
    pairs: list[tuple[np.ndarray, np.ndarray]] = [
        (r(3, 4, 5), r(3, 5, 6)),
        (r(4, 5), r(3, 5, 6)),
        (r(3, 4, 5), r(5, 6)),
        (r(1, 4, 5), r(3, 5, 6)),
        (r(3, 4, 5), r(1, 5, 6)),
        (r(3), r(1, 2, 3, 4)),
        (r(1, 2, 3, 4), r(4)),
        (r(5, 1, 5, 2, 3), r(1, 5, 5, 3, 2)),
        (r(1, 0), r(0, 1)),
        (r(2, 0), r(0, 5)),
        (r(4, 5), r(0, 3, 5, 6)),
        (r(0, 4), r(4, 0)),
    ]
    run_matrix_multiplcation(pairs, dtype)


@pytest.mark.parametrize(
    "a,b",
    [
        (np.zeros((4, 5)), np.zeros((4, 5))),
        (np.zeros((2, 3)), np.zeros((4, 2))),
        (np.zeros((4, 5)), np.zeros(())),
        (np.zeros(3), np.zeros(())),
        (np.zeros((2, 3)), np.zeros(2)),
        (np.zeros(3), np.zeros((2, 3))),
        (np.zeros(()), np.zeros(1)),
        (np.zeros(()), np.zeros((1, 2))),
    ],
)
def test_matrix_multiplication_errors_on_shapes_mismatch(a, b) -> None:
    t = table_from_pandas(pd.DataFrame({"a": [a], "b": [b]}))
    t.select(c=t.a @ t.b)
    with pytest.raises(ValueError):
        run_all()


def test_optional_int_vs_float():
    table = T(
        """
    a | b
    1 | 1.0
      | 2.0
    3 | 3.5
    """
    )
    result = table.select(resA=table.a == table.b, resB=table.a != table.b)
    expected = T(
        """
    resA  | resB
    True  | False
    False | True
    False | True
    """
    )
    assert_table_equality(result, expected)


def test_int_vs_optional_float():
    table = T(
        """
    a | b
    1 | 1.0
    2 |
    3 | 3.5
    """
    )
    result = table.select(resA=table.a == table.b, resB=table.a != table.b)
    expected = T(
        """
    resA  | resB
    True  | False
    False | True
    False | True
    """
    )
    assert_table_equality(result, expected)


def test_optional_int_addition():
    table = T(
        """
    a | b
    1 | 1
      | 2
    3 |
    """
    )
    result = (
        table.filter(pw.this.a.is_not_none())
        .filter(pw.this.b.is_not_none())
        .select(resA=pw.this.a + pw.this.b)
    )
    expected = T(
        """
    resA
    2
    """
    )
    assert_table_equality(result, expected)


def test_tuples():
    table = T(
        """
    a | b
    1 | 1
    2 | 3
    4 | 3
    """
    ).with_columns(
        x=pw.make_tuple(pw.this.a, pw.this.b), y=pw.make_tuple(pw.this.b, pw.this.a)
    )

    result = table.select(
        pw.this.a,
        pw.this.b,
        eq=pw.this.x == pw.this.y,
        ne=pw.this.x != pw.this.y,
        lt=pw.this.x < pw.this.y,
        le=pw.this.x <= pw.this.y,
        gt=pw.this.x > pw.this.y,
        ge=pw.this.x >= pw.this.y,
    )
    expected = T(
        """
    a | b |  eq   |   ne  |   lt  |   le  |   gt  |   ge
    1 | 1 |  True | False | False |  True | False |  True
    2 | 3 | False |  True |  True |  True | False | False
    4 | 3 | False |  True | False | False |  True |  True
    """
    )
    assert_table_equality(result, expected)


def test_nested_tuples():
    table = T(
        """
    a | b
    1 | 1
    2 | 3
    4 | 3
    """
    ).with_columns(
        x=pw.make_tuple("a", pw.make_tuple(pw.this.a, pw.this.b)),
        y=pw.make_tuple("a", pw.make_tuple(pw.this.b, pw.this.a)),
    )

    result = table.select(
        pw.this.a,
        pw.this.b,
        eq=pw.this.x == pw.this.y,
        ne=pw.this.x != pw.this.y,
        lt=pw.this.x < pw.this.y,
        le=pw.this.x <= pw.this.y,
        gt=pw.this.x > pw.this.y,
        ge=pw.this.x >= pw.this.y,
    )
    expected = T(
        """
    a | b |  eq   |   ne  |   lt  |   le  |   gt  |   ge
    1 | 1 |  True | False | False |  True | False |  True
    2 | 3 | False |  True |  True |  True | False | False
    4 | 3 | False |  True | False | False |  True |  True
    """
    )
    assert_table_equality(result, expected)


@pytest.mark.parametrize(
    "op", [operator.eq, operator.ne, operator.lt, operator.le, operator.gt, operator.ge]
)
def test_tuples_error_on_incorrect_types(op):
    table = T(
        """
    a | b
    1 | a
    2 | b
    4 | c
    """
    ).with_columns(
        x=pw.make_tuple(pw.this.a, pw.this.a),
        y=pw.make_tuple(pw.this.a, pw.this.b),
    )
    with pytest.raises(
        TypeError,
        match=re.escape(
            f"Pathway does not support using binary operator {op.__name__} on columns "
            "of types tuple[int, int], tuple[int, str]."
        ),
    ):
        table.select(z=op(pw.this.x, pw.this.y))


def test_lists_lexicographical():
    def make_list(n) -> list[int]:
        return list(range(n))

    table = T(
        """
    a | b
    5 | 5
    2 | 3
    4 | 3
    """
    ).with_columns(
        x=pw.apply(make_list, pw.this.a),
        y=pw.apply(make_list, pw.this.b),
    )

    result = table.select(
        pw.this.a,
        pw.this.b,
        eq=pw.this.x == pw.this.y,
        ne=pw.this.x != pw.this.y,
        lt=pw.this.x < pw.this.y,
        le=pw.this.x <= pw.this.y,
        gt=pw.this.x > pw.this.y,
        ge=pw.this.x >= pw.this.y,
    )
    expected = T(
        """
    a | b |  eq   |   ne  |   lt  |   le  |   gt  |   ge
    5 | 5 |  True | False | False |  True | False |  True
    2 | 3 | False |  True |  True |  True | False | False
    4 | 3 | False |  True | False | False |  True |  True
    """
    )
    assert_table_equality(result, expected)


@pytest.mark.parametrize("cast", ["a", "b"])
def test_tuples_int_float(cast: str):
    table = (
        T(
            """
    a | b
    1 | 1
    2 | 3
    4 | 3
    """
        )
        .with_columns(**{cast: pw.cast(float, pw.this[cast])})
        .with_columns(
            x=pw.make_tuple(pw.this.a, pw.this.b), y=pw.make_tuple(pw.this.b, pw.this.a)
        )
    )

    result = table.select(
        pw.this.a,
        pw.this.b,
        eq=pw.this.x == pw.this.y,
        ne=pw.this.x != pw.this.y,
        lt=pw.this.x < pw.this.y,
        le=pw.this.x <= pw.this.y,
        gt=pw.this.x > pw.this.y,
        ge=pw.this.x >= pw.this.y,
    )
    expected = T(
        """
    a | b |  eq   |   ne  |   lt  |   le  |   gt  |   ge
    1 | 1 |  True | False | False |  True | False |  True
    2 | 3 | False |  True |  True |  True | False | False
    4 | 3 | False |  True | False | False |  True |  True
    """
    ).with_columns(**{cast: pw.cast(float, pw.this[cast])})
    assert_table_equality(result, expected)


def test_tuples_none():
    table = T(
        """
    a | b
    1 |
      |
    1 | 1
    """
    ).with_columns(
        x=pw.make_tuple(pw.this.a, pw.this.b), y=pw.make_tuple(pw.this.b, pw.this.a)
    )

    result = table.select(
        pw.this.a,
        pw.this.b,
        eq=pw.this.x == pw.this.y,
        ne=pw.this.x != pw.this.y,
    )
    expected = T(
        """
    a | b |  eq   |   ne
    1 |   | False |  True
      |   |  True | False
    1 | 1 |  True | False
    """
    )
    assert_table_equality(result, expected)


@pytest.mark.parametrize("op", [operator.lt, operator.le, operator.gt, operator.ge])
def test_tuples_none_cmp(op):
    table = T(
        """
    a | b
    1 |
      |
    1 | 1
    """
    ).with_columns(
        x=pw.make_tuple(pw.this.a, pw.this.b), y=pw.make_tuple(pw.this.b, pw.this.a)
    )

    with pytest.raises(
        TypeError,
        match=re.escape(
            f"Pathway does not support using binary operator {op.__name__} on columns "
            "of types tuple[int | None, int | None], tuple[int | None, int | None].",
        ),
    ):
        table.select(z=op(pw.this.x, pw.this.y))


def test_and_or_are_lazy():
    table = T(
        """
        a | b
        1 | 0
        3 | 2
        6 | 3
    """
    )

    result = table.select(
        x=(pw.this.b != 0) & (pw.this.a // pw.this.b > 1),
        y=(pw.this.b == 0) | (pw.this.a // pw.this.b > 1),
    )
    expected = T(
        """
            x |     y
        False |  True
        False | False
         True |  True
    """
    )

    assert_table_equality(result, expected)

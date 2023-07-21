# Copyright © 2023 Pathway

import copy
import datetime
import itertools
import operator
from typing import Any, Callable, List, Mapping, Optional

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
def test_unary(op_fun: Callable, data: List[Any]) -> None:
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


def _change_dtypes(table: pw.Table, dtypes: Mapping[str, type]):
    for key, dtype in dtypes.items():
        table = table.select(
            *pw.this.without(key), **{key: pw.declare_type(dtype, pw.this[key])}
        )
    return table


def _check_pandas_pathway_return_the_same(
    df: pd.DataFrame,
    op_fun: Any,
    dtypes: Mapping[str, type] = {},
    res_dtype: Optional[type] = None,
):
    table = table_from_pandas(copy.deepcopy(df))
    table = _change_dtypes(table, dtypes)
    table_pw = table.select(pw.this.a, pw.this.b, c=op_fun(pw.this.a, pw.this.b))
    df["c"] = op_fun(df["a"], df["b"])
    table_pd = table_from_pandas(df)
    if res_dtype:
        table_pd = _change_dtypes(table_pd, {"c": res_dtype})
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


def test_pointer():
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
        false_pointer=pw.this.pointer_from(t.false_id)
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
    "op_fun",
    [
        operator.mul,
        operator.floordiv,
    ],
)
def test_duration_and_int(op_fun: Any) -> None:
    pairs = [
        [pd.Timedelta(0), 0],
        [pd.Timedelta(1), 0],
        [pd.Timedelta(0), 1],
        [pd.Timedelta(2), 1],
        [pd.Timedelta(2), 0],
        [pd.Timedelta(2), -1],
        [pd.Timedelta(-2), -2],
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
    if op_fun == operator.floordiv:
        pairs = [[a, b if b != 0 else 1] for a, b in pairs]
    expected = table_from_pandas(pd.DataFrame({"c": [op_fun(a, b) for a, b in pairs]}))
    # computing manually because in pandas when operating on columns
    # (pd.Timedelta(days=-2) // 28).value == -6171428571428, but should be
    # -6171428571429. Suprisingly, working on single values but not on columns
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame({"a": pairs_T[0], "b": pairs_T[1]})
    table = table_from_pandas(df)
    result = table.select(c=op_fun(table.a, table.b))
    assert_table_equality(result, expected)
    if op_fun == operator.mul:
        result_2 = table.select(c=op_fun(table.a, table.b))
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

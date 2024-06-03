# Copyright © 2024 Pathway

from __future__ import annotations

import pandas as pd
import pytest

import pathway as pw
from pathway.stdlib.utils.col import (
    apply_all_rows,
    groupby_reduce_majority,
    multiapply_all_rows,
    unpack_col,
)
from pathway.stdlib.utils.filtering import argmax_rows, argmin_rows
from pathway.tests.utils import (
    T,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_types,
    run_all,
)


def test_ix_optional():
    t_animals = T(
        """
            | genus      | epithet
        1   | upupa      | epops
        2   | acherontia | atropos
        3   | bubo       | scandiacus
        """
    )
    t_indexer = T(
        """
           | indexer
         0 | 3
         1 | 1
         2 | 0
         3 | 4
         4 | 2
        """,
    ).with_columns(indexer=pw.this.pointer_from(pw.this.indexer))
    ret = t_animals.ix(t_indexer.indexer, allow_misses=True).filter(
        pw.this.genus.is_not_none()
    )[["genus"]]
    expected = T(
        """
           | genus
        0  | bubo
        1  | upupa
        4  | acherontia
        """
    )
    assert_table_equality(ret, expected)


def test_unpack_col():
    data = pd.DataFrame(
        {
            "data": [
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9],
            ]
        }
    )
    data = T(data, format="pandas")
    result = unpack_col(data.data, "coord1", "coord2", "coord3")
    assert_table_equality(
        result,
        T(
            """
       | coord1 | coord2 | coord3
    0  | 1      | 2      | 3
    1  | 4      | 5      | 6
    2  | 7      | 8      | 9
    """
        ),
    )


def test_unpack_col_schema():
    class TestSchema(pw.Schema):
        coord1: int
        coord2: float
        coord3: str

    data = T(
        """
            | a   | b   | c
        1   | 11  | 1.1 | abc
        2   | 12  | 1.2 | def
        3   | 13  | 1.3 | ghi
        """
    )
    data = data.select(combined=pw.make_tuple(pw.this.a, pw.this.b, pw.this.c))
    result = unpack_col(data.combined, schema=TestSchema)
    assert_table_equality(
        result,
        T(
            """
            | coord1  | coord2 | coord3
        1   | 11      | 1.1    | abc
        2   | 12      | 1.2    | def
        3   | 13      | 1.3    | ghi
        """
        ),
    )


def test_apply_all_rows():
    t1 = T(
        """
    | col1 | col2
 0  | 1    | 2
 1  | 3    | 4
 2  | 5    | 6
"""
    )

    def fun(col1, col2):
        sum_all = sum(col1) + sum(col2)
        return [x + sum_all for x in col1]

    result = apply_all_rows(t1.col1, t1.col2, fun=fun, result_col_name="res")

    assert_table_equality_wo_types(
        result,
        T(
            """
       | res
    0  | 22
    1  | 24
    2  | 26
    """
        ),
    )


def test_multi_apply_all_rows():
    t1 = T(
        """
    | col1 | col2
 0  | 1    | 2
 1  | 3    | 4
 2  | 5    | 6
"""
    )

    def fun(col1, col2):
        sum_all = sum(col1) + sum(col2)
        return [x + sum_all for x in col1], [x + sum_all for x in col2]

    result = multiapply_all_rows(
        t1.col1, t1.col2, fun=fun, result_col_names=["res1", "res2"]
    )

    assert_table_equality_wo_types(
        result,
        T(
            """
       | res1 | res2
    0  | 22   | 23
    1  | 24   | 25
    2  | 26   | 27
    """
        ),
    )


def test_groupby_reduce_majority():
    t1 = T(
        """
    | group | value
 0  | 1     | 1
 1  | 1     | 1
 2  | 1     | 2
 3  | 2     | 3
 4  | 2     | 4
 5  | 2     | 4
 6  | 2     | 4
"""
    )

    result = groupby_reduce_majority(t1.group, t1.value)
    assert_table_equality_wo_index(
        result,
        T(
            """
       group | majority
       1     | 1
       2     | 4
    """,
        ),
    )


def test_pandas_transformer():
    input = T(
        """
            | foo  | bar
        0   | 10   | 100
        1   | 20   | 200
        2   | 30   | 300
        """
    )

    class Output(pw.Schema):
        sum: int

    @pw.pandas_transformer(output_schema=Output)
    def sum_cols(t: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(t.sum(axis=1))

    result = sum_cols(input)

    assert_table_equality(
        result,
        T(
            """
            | sum
        0   | 110
        1   | 220
        2   | 330
        """
        ),
    )


def test_pandas_transformer_column_names():
    input = T(
        """
            | foo  | bar
        0   | 10   | 100
        1   | 20   | 200
        2   | 30   | 300
        """
    )

    class Output(pw.Schema):
        sum: int

    @pw.pandas_transformer(output_schema=Output)
    def sum_cols(t: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(t["foo"] + t["bar"])

    result = sum_cols(input)

    assert_table_equality(
        result,
        T(
            """
                | sum
            0   | 110
            1   | 220
            2   | 330
            """
        ),
    )


def test_pandas_transformer_multiple_inputs():
    input1 = T(
        """
            | foo
        0   | 10
        1   | 20
        2   | 30
        """
    )
    input2 = T(
        """
            | bar   | baz
        0   | 100   | 1
        1   | 200   | 2
        2   | 300   | 3
        """
    )

    class Output(pw.Schema):
        sum: int

    @pw.pandas_transformer(output_schema=Output)
    def sum_cols(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(a["foo"] + b["bar"] + b["baz"])

    result = sum_cols(input1, input2)

    assert_table_equality(
        result,
        T(
            """
                | sum
            0   | 111
            1   | 222
            2   | 333
            """
        ),
    )


def test_pandas_transformer_series_output():
    input = T(
        """
            | foo
        0   | 1
        1   | 2
        2   | 3
        """
    )

    class Output(pw.Schema):
        bar: int

    @pw.pandas_transformer(output_schema=Output)
    def series_output(df: pd.DataFrame) -> pd.Series:
        return df["foo"]

    assert_table_equality(
        series_output(input),
        T(
            """
                | bar
            0   | 1
            1   | 2
            2   | 3
            """
        ),
    )


@pytest.mark.parametrize(
    "output_universe_arg",
    [1, "bar"],
)
def test_pandas_transformer_choose_output_universe(output_universe_arg):
    input1 = T(
        """
            | foo
        4   | 10
        5   | 20
        6   | 30
        """
    )
    input2 = T(
        """
            | bar
        4   | 1
        6   | 2
        """
    )

    class Output(pw.Schema):
        foo: int

    @pw.pandas_transformer(output_schema=Output, output_universe=output_universe_arg)
    def filter_by(foo: pd.DataFrame, bar: pd.DataFrame) -> pd.DataFrame:
        return foo[foo.index.isin(bar.index)]

    result = filter_by(input1, input2) + input2
    assert_table_equality(
        result,
        T(
            """
                | foo   | bar
            4   | 10    | 1
            6   | 30    | 2
            """
        ),
    )


@pytest.mark.parametrize(
    "output_universe_arg",
    [-1, 2, "bar"],
)
def test_pandas_transformer_choose_output_universe_wrong_arg(output_universe_arg):
    class Output(pw.Schema):
        foo: int

    input = pw.Table.empty()

    @pw.pandas_transformer(output_schema=Output, output_universe=output_universe_arg)
    def transformer(foo: pd.DataFrame) -> pd.DataFrame:
        return foo

    with pytest.raises(ValueError):
        transformer(input)


def test_pandas_transformer_choose_output_universe_mismatch():
    class Output(pw.Schema):
        foo: int

    input = T(
        """
            | foo
        1   | 1
        2   | 2
        3   | 3
        """
    )

    @pw.pandas_transformer(output_schema=Output, output_universe="foo")
    def transformer(foo: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"foo": [1, 2]}, index=[1, 2])

    transformer(input)
    with pytest.raises(ValueError):
        run_all()


def test_pandas_transformer_choose_output_universe_not_unique():
    class Output(pw.Schema):
        foo: int

    @pw.pandas_transformer(output_schema=Output)
    def transformer() -> pd.DataFrame:
        return pd.DataFrame({"foo": [1, 2, 3]}, index=[1, 2, 2])

    with pytest.raises(ValueError):
        transformer()


def test_pandas_transformer_generators():
    class Output(pw.Schema):
        foo: int

    @pw.pandas_transformer(output_schema=Output)
    def generator() -> pd.DataFrame:
        return pd.DataFrame({"foo": [1, 2, 3]})

    assert_table_equality(
        generator(),
        T(
            """
                | foo
            0   | 1
            1   | 2
            2   | 3
            """
        ),
    )


def test_pandas_transformer_generator_series_output():
    class Output(pw.Schema):
        foo: int

    @pw.pandas_transformer(output_schema=Output)
    def generator() -> pd.Series:
        return pd.Series([1, 2, 3])

    assert_table_equality(
        generator(),
        T(
            """
                | foo
            0   | 1
            1   | 2
            2   | 3
            """
        ),
    )


def test_argmin_rows_01():
    input = T(
        """
        | foo   | bar
    0   |  1    | 5
    1   |  2    | 6
    2   |  3    | 8
    3   |  2    | 8
    4   |  3    | 8
    5   |  3    | 5
    6   |  5    | 2
    7   |  6    | 1
    """
    )
    expected = T(
        """
        | foo   | bar
    0   |  1    | 5
    1   |  2    | 6
    5   |  3    | 5
    6   |  5    | 2
    7   |  6    | 1
    """
    )
    assert_table_equality(argmin_rows(input, *[input.foo], what=input.bar), expected)


def test_argmin_rows_02():
    input = T(
        """
        | foo   | bar
    0   |  1    | 5
    1   |  2    | 6
    2   |  3    | 8
    3   |  2    | 8
    4   |  3    | 5
    5   |  3    | 5
    6   |  5    | 2
    7   |  6    | 1
    """
    )
    expected = T(
        """
        | foo   | bar
    0   |  1    | 5
    1   |  2    | 6
    5   |  3    | 5
    6   |  5    | 2
    7   |  6    | 1
    """
    )

    assert_table_equality_wo_index(
        argmin_rows(input, *[input.foo], what=input.bar), expected
    )


def test_argmax_rows_01():
    input = T(
        """
        | foo   | bar
    0   |  1    | 5
    1   |  2    | 6
    2   |  3    | 8
    3   |  2    | 8
    4   |  3    | 7
    5   |  3    | 5
    6   |  5    | 2
    7   |  6    | 1
    """
    )
    expected = T(
        """
        | foo   | bar
    0   |  1    | 5
    2   |  3    | 8
    3   |  2    | 8
    6   |  5    | 2
    7   |  6    | 1
    """
    )
    assert_table_equality(argmax_rows(input, *[input.foo], what=input.bar), expected)


def test_argmax_rows_02():
    input = T(
        """
        | foo   | bar
    0   |  1    | 5
    1   |  2    | 6
    2   |  3    | 8
    3   |  2    | 8
    4   |  3    | 8
    5   |  3    | 5
    6   |  5    | 2
    7   |  6    | 1
    """
    )
    expected = T(
        """
        | foo   | bar
    0   |  1    | 5
    2   |  3    | 8
    3   |  2    | 8
    6   |  5    | 2
    7   |  6    | 1
    """
    )

    assert_table_equality_wo_index(
        argmax_rows(input, *[input.foo], what=input.bar), expected
    )


def test_table_from_rows_stream():
    class TestSchema(pw.Schema):
        foo: int = pw.column_definition(primary_key=True)
        bar: int

    rows = [
        (1, 2, 2, 1),
        (1, 2, 4, -1),
        (1, 3, 4, 1),
        (4, 2, 4, 1),
    ]
    expected = T(
        """
        foo   | bar
         1    | 3
         4    | 2
    """
    ).with_id_from(pw.this.foo)

    table = pw.debug.table_from_rows(schema=TestSchema, rows=rows, is_stream=True)
    assert_table_equality(table, expected)


def test_table_from_rows():
    class TestSchema(pw.Schema):
        foo: int = pw.column_definition(primary_key=True)
        bar: int

    rows = [
        (1, 2),
        (2, 2),
        (3, 3),
        (4, 2),
    ]
    expected = T(
        """
        foo   | bar
         1    | 2
         2    | 2
         3    | 3
         4    | 2
    """
    ).with_id_from(pw.this.foo)

    table = pw.debug.table_from_rows(schema=TestSchema, rows=rows, is_stream=False)
    assert_table_equality(table, expected)

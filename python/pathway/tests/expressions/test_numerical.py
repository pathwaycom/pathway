# Copyright © 2024 Pathway

import pytest

import pathway as pw
from pathway.debug import table_from_markdown
from pathway.tests.utils import assert_table_equality


@pytest.mark.parametrize("use_namespace", [True, False])
def test_abs_int(use_namespace: bool) -> None:
    table = table_from_markdown(
        """
        v
        -110
        -3
        7
        -1
        12
        """
    )
    if use_namespace:
        results = table.select(v_abs=table.v.num.abs())
    else:
        results = table.select(v_abs=abs(table.v))
    expected = table_from_markdown(
        """
        v_abs
        110
        3
        7
        1
        12
        """
    )
    assert_table_equality(results, expected)


@pytest.mark.parametrize("use_namespace", [True, False])
def test_abs_float(use_namespace: bool) -> None:
    table = table_from_markdown(
        """
        v
        -110.5
        -3.8
        7.2
        -1.6
        12.9
        """
    )
    if use_namespace:
        results = table.select(v_abs=table.v.num.abs())
    else:
        results = table.select(v_abs=abs(table.v))
    expected = table_from_markdown(
        """
        v_abs
        110.5
        3.8
        7.2
        1.6
        12.9
        """
    )
    assert_table_equality(results, expected)


def test_round():
    table = table_from_markdown(
        """
        v
        1
        1.2
        1.23
        1.234
        1.2345
        """
    )
    results = table.select(v_round=table.v.num.round(2))
    expected = table_from_markdown(
        """
        v_round
        1.0
        1.20
        1.23
        1.23
        1.23
        """
    )
    assert_table_equality(results, expected)


def test_round_column():
    table = table_from_markdown(
        """
        value   | precision
        3       | 0
        3.1     | 1
        3.14    | 1
        3.141   | 2
        3.1415  | 2
        """
    )
    results = table.select(v_round=table.value.num.round(pw.this.precision))
    expected = table_from_markdown(
        """
        v_round
        3.0
        3.1
        3.1
        3.14
        3.14
        """
    )
    assert_table_equality(results, expected)


def test_fill_na_optional_float():
    table = table_from_markdown(
        """
        index | v
        1     | 1.0
        2     | None
        3     | 3.5
        4     | nan
        5     | 5.0
        """
    ).with_columns(v=pw.require(pw.apply(float, pw.this.v), pw.this.v))

    results = table.select(v_filled=table.v.num.fill_na(0))
    expected = table_from_markdown(
        """
        v_filled
        1.0
        0.0
        3.5
        0.0
        5.0
        """
    )
    assert_table_equality(results, expected)


def test_fill_na_optional_int():
    table = table_from_markdown(
        """
        index | v
        1     | 1
        2     | None
        3     | 3
        4     | 4
        5     | 5
        """
    )
    results = table.select(v_filled=table.v.num.fill_na(0))
    expected = table_from_markdown(
        """
        v_filled
        1
        0
        3
        4
        5
        """
    )
    assert_table_equality(results, expected)


def test_fill_na_float():
    table = table_from_markdown(
        """
        index | v
        1     | 1.1
        2     | 2.2
        3     | 3.3
        4     | 4.4
        5     | 5.5
        """
    )
    results = table.select(v_filled=table.v.num.fill_na(0))
    expected = table_from_markdown(
        """
        v_filled
        1.1
        2.2
        3.3
        4.4
        5.5
        """
    )
    assert_table_equality(results, expected)


def test_fill_na_int():
    table = table_from_markdown(
        """
        index | v
        1     | 1
        2     | 2
        3     | 3
        4     | 4
        5     | 5
        """
    )
    results = table.select(v_filled=table.v.num.fill_na(0))
    expected = table_from_markdown(
        """
        v_filled
        1
        2
        3
        4
        5
        """
    )
    assert_table_equality(results, expected)

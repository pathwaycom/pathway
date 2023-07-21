# Copyright © 2023 Pathway

from typing import Optional

import pathway as pw
from pathway.debug import table_from_markdown
from pathway.tests.utils import assert_table_equality


def test_abs_float():
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
    results = table.select(v_abs=table.v.num.abs())
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
    )
    table = table.select(
        v=pw.apply_with_type(
            lambda x: x if x is None else float(x), Optional[float], pw.this.v
        )
    )
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

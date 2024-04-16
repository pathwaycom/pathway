# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Any

import pandas as pd

import pathway as pw
from pathway import Table, this
from pathway.tests.utils import T, assert_table_equality_wo_index


def test_flatten_simple():
    tab = T(pd.DataFrame.from_dict({"col": [[1, 2, 3, 4]]}), format="pandas")

    assert_table_equality_wo_index(
        tab.flatten(this.col, origin_id="origin_id"),
        T(
            """
    col | origin_id
      1 | 0
      2 | 0
      3 | 0
      4 | 0
    """,
        ).with_columns(origin_id=tab.pointer_from(this.origin_id)),
    )


def test_flatten_no_origin():
    tab = T(pd.DataFrame.from_dict({"col": [[1, 2, 3, 4]]}), format="pandas")

    assert_table_equality_wo_index(
        tab.flatten(this.col),
        T(
            """
    col
      1
      2
      3
      4
    """,
        ),
    )


def test_flatten_inner_repeats():
    tab = T(pd.DataFrame.from_dict({"col": [[1, 1, 1, 3]]}), format="pandas")

    assert_table_equality_wo_index(
        tab.flatten(this.col, origin_id="origin_id"),
        T(
            """
    col | origin_id
      1 | 0
      1 | 0
      1 | 0
      3 | 0
    """,
        ).with_columns(origin_id=tab.pointer_from(this.origin_id)),
    )


def test_flatten_more_repeats():
    tab = T(pd.DataFrame.from_dict({"col": [[1, 1, 1, 3], [1]]}), format="pandas")

    assert_table_equality_wo_index(
        tab.flatten(this.col, origin_id="origin_id"),
        T(
            """
    col | origin_id
      1 | 0
      1 | 0
      1 | 0
      3 | 0
      1 | 1
    """,
        ).with_columns(origin_id=tab.pointer_from(this.origin_id)),
    )


def test_flatten_empty_lists():
    tab = T(pd.DataFrame.from_dict({"col": [[], []]}), format="pandas")

    assert_table_equality_wo_index(
        tab.flatten(this.col, origin_id="origin_id"),
        Table.empty(col=Any, origin_id=pw.Pointer),
    )

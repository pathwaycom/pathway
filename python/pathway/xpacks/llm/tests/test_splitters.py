# Copyright © 2024 Pathway

from __future__ import annotations

import pandas as pd

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.splitters import (
    NullSplitter,
    RecursiveSplitter,
    TokenCountSplitter,
)


def test_null():
    splitter = NullSplitter()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    result = input_table.select(ret=splitter(pw.this.ret)[0][0])

    assert_table_equality(result, input_table)


def test_tokencount():
    splitter = TokenCountSplitter()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    result = input_table.select(ret=splitter(pw.this.ret)[0][0])

    assert_table_equality(result, input_table)


def test_recursive_from_encoding():
    splitter = RecursiveSplitter(
        encoding_name="cl100k_base", chunk_size=30, chunk_overlap=0
    )
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."  # 26 tokens in cl100k_base
    big_txt = "\n\n".join([txt] * 5)
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=big_txt)]))

    result = input_table.select(ret=splitter(pw.this.ret)).flatten(pw.this.ret)
    result = pw.debug.table_to_pandas(result)

    assert len(result) == 5
    assert result.iloc[0].ret[0] == txt
    assert result.iloc[0].ret[1] == pw.Json({})


def test_recursive_from_model_name():
    splitter = RecursiveSplitter(model_name="gpt-4", chunk_size=30, chunk_overlap=0)
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."  # 26 tokens in cl100k_base
    big_txt = "\n\n".join([txt] * 5)
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=big_txt)]))

    result = input_table.select(ret=splitter(pw.this.ret)).flatten(pw.this.ret)
    result = pw.debug.table_to_pandas(result)

    assert len(result) == 5
    assert result.iloc[0].ret[0] == txt
    assert result.iloc[0].ret[1] == pw.Json({})

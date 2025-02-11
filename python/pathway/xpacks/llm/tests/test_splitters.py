# Copyright © 2024 Pathway

from __future__ import annotations

import pandas as pd

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.splitters import NullSplitter, TokenCountSplitter


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

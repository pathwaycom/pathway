# Copyright © 2024 Pathway

from __future__ import annotations

import pandas as pd

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.parsers import ParseUtf8

pw.Type


def test_parseutf8():
    parser = ParseUtf8()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_df = pd.DataFrame([dict(raw=txt.encode("utf8"))])

    class schema(pw.Schema):
        raw: bytes

    input_table = pw.debug.table_from_pandas(input_df, schema=schema)
    result = input_table.select(ret=parser(pw.this.raw)[0][0])

    assert_table_equality(
        result, pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    )

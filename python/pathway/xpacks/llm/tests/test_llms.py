# Copyright © 2024 Pathway

from __future__ import annotations

import pandas as pd

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm import llms


def test_prompt_chat_single_qa():
    func = llms.prompt_chat_single_qa
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    result = input_table.select(ret=pw.unwrap(func(pw.this.ret)[0]["content"].as_str()))

    assert_table_equality(result, input_table)

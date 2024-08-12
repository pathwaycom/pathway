# Copyright © 2024 Pathway

from __future__ import annotations

import nltk
import pandas as pd
import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.parsers import ParseUnstructured, ParseUtf8

pw.Type


for _ in range(10):
    try:
        nltk.download("stopwords", force=True)
        nltk.download("wordnet", force=True)
        nltk.download("punkt", force=True)
        nltk.download("punkt_tab", force=True)
        nltk.download("averaged_perceptron_tagger", force=True)
        nltk.download("averaged_perceptron_tagger_eng", force=True)
    except Exception:
        pass
    else:
        break


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


@pytest.mark.environment_changes
def test_parse_unstructured(monkeypatch):
    parser = ParseUnstructured()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_df = pd.DataFrame([dict(raw=txt.encode("utf8"))])

    class schema(pw.Schema):
        raw: bytes

    input_table = pw.debug.table_from_pandas(input_df, schema=schema)
    result = input_table.select(ret=parser(pw.this.raw)[0][0])

    assert_table_equality(
        result, pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    )

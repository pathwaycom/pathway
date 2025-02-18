import os

import pandas as pd
import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.parsers import UnstructuredParser


@pytest.mark.environment_changes
def test_parse_unstructured(monkeypatch):
    parser = UnstructuredParser()
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
@pytest.mark.asyncio
def test_parse_unstructured_unk_exception(monkeypatch):
    parser = UnstructuredParser()

    binary_data = b"NONEXISTING_FMT" + os.urandom(2048)

    input_df = pd.DataFrame([dict(raw=binary_data)])

    class schema(pw.Schema):
        raw: bytes

    input_table = pw.debug.table_from_pandas(input_df, schema=schema)

    with pytest.raises(Exception) as excinfo:
        result = input_table.select(ret=parser(pw.this.raw)[0][0])
        pw.debug.compute_and_print(result)

    exception_msg = str(excinfo.value)

    assert (
        "This error may indicate libmagic (magic) dependency is missing."
        in exception_msg
    )
    assert "FileType.UNK" in exception_msg

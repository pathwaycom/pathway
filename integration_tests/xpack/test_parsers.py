import json
import os

import pandas as pd
import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality, run_all
from pathway.xpacks.llm.parsers import (
    DoclingParser,
    PaddleOCRParser,
    PypdfParser,
    UnstructuredParser,
)

FOLDER_WITH_ONE_FILE_ID = "1XisWrSjKMCx2jfUW8OSgt6L8veq8c4Mh"


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


single_page_parsers = {
    "docling": lambda: DoclingParser(table_parsing_strategy="docling", chunk=False),
    "paddle_ocr_structure": lambda: PaddleOCRParser(concatenate_pages=True),
}


@pytest.mark.parametrize("parser_name", single_page_parsers.keys())
def test_parse_pdf_single_page(parser_name, tmp_path, credentials_dir):
    files_table = pw.io.gdrive.read(
        FOLDER_WITH_ONE_FILE_ID,
        mode="static",
        service_user_credentials_file=str(credentials_dir / "credentials.json"),
        object_size_limit=None,
        with_metadata=True,
    )

    parser = single_page_parsers[parser_name]()

    parse_table = files_table.select(parsed_text=parser(pw.this.data)[0][0])

    pw.io.jsonlines.write(parse_table, tmp_path / "output.jsonl")

    run_all()

    rows_count = 0
    with open(tmp_path / "output.jsonl", "r") as f:
        for raw_row in f:
            row = json.loads(raw_row)
            text = row["parsed_text"]
            rows_count += 1

        assert rows_count == 1
        assert "first decomposed with a parse tree and converted" in text


parsers = {
    "pypdf": lambda: PypdfParser(apply_text_cleanup=True),
    "paddle_ocr_structure": lambda: PaddleOCRParser(),
}


@pytest.mark.parametrize("parser_name", parsers.keys())
def test_parse_pdf_multi_page_output(parser_name, tmp_path, credentials_dir):
    files_table = pw.io.gdrive.read(
        FOLDER_WITH_ONE_FILE_ID,
        mode="static",
        service_user_credentials_file=str(credentials_dir / "credentials.json"),
        object_size_limit=None,
        with_metadata=True,
    )

    parser = parsers[parser_name]()

    parse_table = (
        files_table.select(result=parser(pw.this.data))
        .flatten(pw.this.result)
        .select(
            text=pw.this.result[0],
            page_number=pw.this.result[1]["page_number"].as_int(),
        )
    )

    pw.io.jsonlines.write(parse_table, tmp_path / "output.jsonl")

    run_all()

    pages = set()
    with open(tmp_path / "output.jsonl", "r") as f:
        for raw_row in f:
            row = json.loads(raw_row)
            pages.add(row["page_number"])
            assert len(row["text"]) > 100

    assert pages == set(range(0, 10))

# Copyright © 2024 Pathway

from __future__ import annotations

import os
from pathlib import Path

import nltk
import pandas as pd
import pytest
from fpdf import FPDF

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.parsers import (
    OpenParse,
    ParseUnstructured,
    ParseUtf8,
    PypdfParser,
)

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


def _create_temp_pdf_with_text(text: str, path: Path) -> Path:
    class PDF(FPDF):
        def header(self):
            self.set_font("Arial", size=12)
            self.cell(0, 10, "", ln=1)

        def footer(self):
            pass

    pdf_path: Path = path / "generated_test_file.pdf"
    pdf = PDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.multi_cell(0, 10, text)
    pdf.output(pdf_path)

    return pdf_path


@pytest.mark.environment_changes
def test_parse_openparse(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    for tesseract_dir in [
        "/usr/share/tesseract/tessdata/",
        "/usr/share/tesseract-ocr/5/tessdata",
    ]:
        if os.path.exists(tesseract_dir):
            monkeypatch.setenv(
                "TESSDATA_PREFIX", tesseract_dir
            )  # fix for tesseract ocr
            break

    if not os.getenv("TESSDATA_PREFIX"):
        monkeypatch.setenv("TESSDATA_PREFIX", "/usr/share/tesseract/tessdata/")

    txt = (
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod"
        "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,"
        "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
    )

    pdf_path = _create_temp_pdf_with_text(txt, tmp_path)

    parser = OpenParse(table_args={"parsing_algorithm": "pymupdf"})

    with open(pdf_path, "rb") as pdf_file:
        raw_pdf_data = pdf_file.read()

    input_df = pd.DataFrame([dict(raw=raw_pdf_data)])

    class Schema(pw.Schema):
        raw: bytes

    @pw.udf
    def remove_line_breaks(t: str) -> str:
        return t.replace("<br>", " ").replace("  ", " ")

    input_table = pw.debug.table_from_pandas(input_df, schema=Schema)
    result = input_table.select(ret=parser(pw.this.raw)[0][0])
    # pdf creator adds line breaks
    result = result.with_columns(ret=remove_line_breaks(pw.this.ret))

    expected_df = pd.DataFrame([dict(ret=txt)])
    expected_table = pw.debug.table_from_pandas(expected_df)

    assert_table_equality(result, expected_table)


def test_parse_pypdf(tmp_path: Path):
    parser = PypdfParser()

    txt = (
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod"
        "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,"
        "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
    )

    pdf_path = _create_temp_pdf_with_text(txt, tmp_path)

    with open(pdf_path, "rb") as pdf_file:
        raw_pdf_data = pdf_file.read()

    input_df = pd.DataFrame([dict(raw=raw_pdf_data)])

    class Schema(pw.Schema):
        raw: bytes

    input_table = pw.debug.table_from_pandas(input_df, schema=Schema)
    result = input_table.select(ret=parser(pw.this.raw)[0][0])

    assert_table_equality(
        result, pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    )

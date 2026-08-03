# Copyright © 2026 Pathway

from __future__ import annotations

import sys
from pathlib import Path

import nltk
import pandas as pd
import pytest
from fpdf import FPDF

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.parsers import PaddleOCRParser, PypdfParser, Utf8Parser

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


def test_utf8parser():
    parser = Utf8Parser()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_df = pd.DataFrame([dict(raw=txt.encode("utf8"))])

    class schema(pw.Schema):
        raw: bytes

    input_table = pw.debug.table_from_pandas(input_df, schema=schema)
    result = input_table.select(ret=parser(pw.this.raw)[0][0])

    assert_table_equality(
        result, pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    )


def test_utf8parser_on_strings():
    parser = Utf8Parser()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_df = pd.DataFrame([dict(raw=txt)])

    class schema(pw.Schema):
        raw: str

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


@pytest.mark.skipif(
    sys.version_info < (3, 14),
    reason="the guard fires only where paddlepaddle is unavailable",
)
def test_paddle_parser_reports_unavailability_on_python_3_14():
    with pytest.raises(RuntimeError, match="paddlepaddle does not publish packages"):
        PaddleOCRParser()

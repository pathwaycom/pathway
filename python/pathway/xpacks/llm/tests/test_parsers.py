# Copyright © 2026 Pathway

from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

import nltk
import pandas as pd
import pytest
from fpdf import FPDF
from PIL import Image

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.parsers import (
    PaddleOCRParser,
    PypdfParser,
    Utf8Parser,
    _detect_ocr_input_type,
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


def test_parsers_explain_non_bytes_contents():
    import asyncio

    from pathway.xpacks.llm.parsers import UnstructuredParser

    parser = UnstructuredParser()
    with pytest.raises(
        TypeError, match=r"expects the raw file contents as bytes.*Json"
    ):
        asyncio.run(parser.__wrapped__(pw.Json({"text": "hello"})))
    with pytest.raises(TypeError, match=r"expects the raw file contents as bytes.*str"):
        asyncio.run(parser.__wrapped__("hello"))


def _zip_with(*names: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in names:
            archive.writestr(name, "<xml/>")
    return buffer.getvalue()


def _png() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(buffer, format="PNG")
    return buffer.getvalue()


_OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


@pytest.mark.parametrize(
    "contents,expected",
    [
        (b"%PDF-1.4\n%%EOF\n", "pdf"),
        (_zip_with("[Content_Types].xml", "ppt/presentation.xml"), "pptx"),
        (_OLE_MAGIC + b"\0" * 64 + "PowerPoint Document".encode("utf-16-le"), "ppt"),
        # legacy Word and Excel files are OLE files too, but not slides
        (_OLE_MAGIC + b"\0" * 64 + "WordDocument".encode("utf-16-le"), "image"),
        (_OLE_MAGIC + b"\0" * 64 + "Workbook".encode("utf-16-le"), "image"),
        (_png(), "image"),
        (_zip_with("word/document.xml"), "image"),
        (_zip_with("ppt/notes.txt"), "image"),
        (b"PK\x03\x04 not really a zip", "image"),
    ],
    ids=[
        "pdf",
        "pptx",
        "ppt",
        "doc",
        "xls",
        "png",
        "docx",
        "zip-without-presentation",
        "broken-zip",
    ],
)
def test_paddle_parser_detects_input_type(contents: bytes, expected: str):
    assert _detect_ocr_input_type(contents) == expected


def _paddle_parser_without_pipeline() -> PaddleOCRParser:
    # ``_normalize_input`` does not touch the Paddle pipeline, so the parser can
    # be tested without paddle installed.
    parser = object.__new__(PaddleOCRParser)
    parser.intermediate_image_format = "jpg"
    parser.max_image_size = 15 * 1024 * 1024
    parser.downsize_horizontal_width = 1920
    return parser


def test_paddle_parser_normalizes_image():
    images, filetype = _paddle_parser_without_pipeline()._normalize_input(_png())

    assert filetype == "image"
    assert len(images) == 1
    assert images[0].mode == "RGB"
    assert images[0].size == (8, 8)


def test_paddle_parser_skips_non_image_zip():
    parser = _paddle_parser_without_pipeline()

    assert parser._normalize_input(_zip_with("word/document.xml")) == ([], None)


class _MarkdownResult(dict):
    """Stands in for paddlex's ``MarkdownResult``, a ``dict`` subclass."""


@pytest.mark.parametrize(
    "concatenated",
    [
        "## Title\n\nSome text",
        _MarkdownResult(markdown_texts="## Title\n\nSome text"),
    ],
    ids=["str", "markdown-result"],
)
def test_paddle_structure_parser_returns_plain_text(concatenated):
    from types import SimpleNamespace

    from pathway.xpacks.llm.parsers import _PaddlePPStructureV3Parser

    class FakePipeline:
        def concatenate_markdown_pages(self, pages):
            assert pages == [{"markdown_texts": "## Title"}, {"markdown_texts": "x"}]
            return concatenated

    parser = _PaddlePPStructureV3Parser(FakePipeline())  # type: ignore[arg-type]
    ocr_result = [
        SimpleNamespace(markdown={"markdown_texts": "## Title"}),
        SimpleNamespace(markdown={"markdown_texts": "x"}),
    ]

    assert parser.extract_text(ocr_result) == "## Title\n\nSome text"

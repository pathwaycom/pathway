# Copyright © 2026 Pathway

"""
The ``xpack-llm`` extra does not include the document-parsing packages
(``xpack-llm-docs``) or the local-inference packages (``xpack-llm-local``).
Importing ``pathway.xpacks.llm`` must not require any of them: parsers that
need a missing package must fail at construction with the ``optional_imports``
message naming the extra to install, and parsers without extra requirements
must keep working.

The test simulates the minimal environment by blocking the optional modules
in a subprocess, so it catches packaging regressions even when run from a
full environment.
"""

import subprocess
import sys

# Top-level modules that only the xpack-llm-docs / xpack-llm-local extras
# provide. None of them may be imported at module scope by pathway.xpacks.llm.
BLOCKED_MODULES = [
    # xpack-llm-docs
    "docling",
    "docling_core",
    "docx",
    "paddleocr",
    "paddle",
    "pdf2image",
    "pdfminer",
    "pypdf",
    "unstructured",
    # xpack-llm-local
    "sentence_transformers",
    "transformers",
]

_BLOCK_IMPORTS_PREAMBLE = """
import sys

BLOCKED = set(sys.argv[1:])


class _BlockOptionalDeps:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise ModuleNotFoundError(
                f"No module named {fullname!r} (blocked by test)", name=fullname
            )
        return None


sys.meta_path.insert(0, _BlockOptionalDeps())
"""

_MINIMAL_ENV_SCRIPT = (
    _BLOCK_IMPORTS_PREAMBLE
    + """
import asyncio

import pathway.xpacks.llm
from pathway.xpacks.llm import parsers

# Parsers without extra requirements construct and work.
parser = parsers.Utf8Parser()
assert asyncio.run(parser.__wrapped__(b"foo")) == [("foo", {})]

# Parsers that need a missing package fail lazily at construction, naming
# the extra to install.
try:
    parsers.UnstructuredParser()
except ImportError as e:
    assert "xpack-llm-docs" in str(e), str(e)
else:
    raise AssertionError("UnstructuredParser() should require xpack-llm-docs")

try:
    parsers.PaddleOCRParser()
except ImportError as e:
    # Below 3.14 the parser asks for the extra that provides paddleocr.
    assert sys.version_info < (3, 14), str(e)
    assert "xpack-llm-docs" in str(e), str(e)
except RuntimeError as e:
    # On 3.14+ the parser reports upfront that paddlepaddle has no
    # packages for this Python version, before any import is attempted.
    assert sys.version_info >= (3, 14), str(e)
    assert "paddlepaddle does not publish packages" in str(e), str(e)
else:
    raise AssertionError("PaddleOCRParser() should be unavailable here")
"""
)


def test_xpack_imports_without_docs_and_local_extras():
    result = subprocess.run(
        [sys.executable, "-c", _MINIMAL_ENV_SCRIPT, *BLOCKED_MODULES],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stderr


# The image and vision helpers in ``_parser_utils`` (used by PaddleOCRParser,
# ImageParser and SlideParser) need only Pillow, so they must stay importable
# without docling; only the docling chunkers require ``xpack-llm-docs``.
_PARSER_UTILS_WITHOUT_DOCLING_SCRIPT = (
    _BLOCK_IMPORTS_PREAMBLE
    + """
import PIL.Image

from pathway.xpacks.llm import _parser_utils

img = PIL.Image.new("RGB", (40, 20))
assert _parser_utils.maybe_downscale(img, 10**6, 10).size == (40, 20)
assert _parser_utils.maybe_downscale(img, 100, 10).size == (10, 5)

try:
    from pathway.xpacks.llm import _docling_chunking  # noqa: F401
except ImportError as e:
    assert "xpack-llm-docs" in str(e), str(e)
else:
    raise AssertionError("_docling_chunking should require xpack-llm-docs")
"""
)


def test_parser_utils_without_docling():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _PARSER_UTILS_WITHOUT_DOCLING_SCRIPT,
            "docling",
            "docling_core",
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stderr

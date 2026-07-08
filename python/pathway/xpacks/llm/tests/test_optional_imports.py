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

_MINIMAL_ENV_SCRIPT = """
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
    assert "xpack-llm-docs" in str(e), str(e)
else:
    raise AssertionError("PaddleOCRParser() should require xpack-llm-docs")
"""


def test_xpack_imports_without_docs_and_local_extras():
    result = subprocess.run(
        [sys.executable, "-c", _MINIMAL_ENV_SCRIPT, *BLOCKED_MODULES],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stderr

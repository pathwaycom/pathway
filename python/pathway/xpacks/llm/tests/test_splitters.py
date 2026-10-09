# Copyright © 2026 Pathway

from __future__ import annotations

import pandas as pd
import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.splitters import (
    NullSplitter,
    RecursiveSplitter,
    TokenCountSplitter,
)


def test_null():
    splitter = NullSplitter()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    result = input_table.select(ret=splitter(pw.this.ret)[0][0])

    assert_table_equality(result, input_table)


def test_tokencount():
    splitter = TokenCountSplitter()
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    result = input_table.select(ret=splitter(pw.this.ret)[0][0])

    assert_table_equality(result, input_table)


def test_tokencount_does_not_drop_characters():
    # Regression: when a chunk was cut at a punctuation mark, the cursor advanced
    # by the re-encoded kept text, which could skip the tokens straddling the cut
    # and silently drop characters. Concatenating the chunks must reconstruct the
    # (unicode-normalized) input exactly.
    import unicodedata

    splitter = TokenCountSplitter(min_tokens=1, max_tokens=3)
    txt = "a.b.c.d.e.f.g.h.i.j.k.l."
    chunks = [chunk for chunk, _ in splitter.chunk(txt)]

    assert "".join(chunks) == unicodedata.normalize("NFKC", txt)


@pytest.mark.parametrize(
    "txt",
    [
        "Привет, мир. Это проверка разбиения текста! Работает ли оно? Да. " * 5,
        "你好,世界。这是一个测试!它有效吗?是的. " * 10,
    ],
    ids=["russian", "chinese"],
)
def test_tokencount_does_not_duplicate_non_ascii(txt):
    import unicodedata

    splitter = TokenCountSplitter()
    chunks = [chunk for chunk, _ in splitter.chunk(txt)]

    assert "".join(chunks) == unicodedata.normalize("NFKC", txt)


def test_tokencount_preserves_token_straddling_first_punctuation_cut():
    # cl100k_base encodes "...)" as a single token. If the
    # punctuation prefix has no whole token, emit the window intact.
    splitter = TokenCountSplitter(min_tokens=0, max_tokens=1)
    txt = "...) tail"
    metadata = {"source": "example"}
    chunks = splitter.chunk(txt, metadata)

    assert "".join(chunk for chunk, _ in chunks) == txt
    assert all(chunk and meta == metadata for chunk, meta in chunks)


def test_recursive_from_encoding():
    splitter = RecursiveSplitter(
        encoding_name="cl100k_base", chunk_size=30, chunk_overlap=0
    )
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."  # 26 tokens in cl100k_base
    big_txt = "\n\n".join([txt] * 5)
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=big_txt)]))

    result = input_table.select(ret=splitter(pw.this.ret)).flatten(pw.this.ret)
    result = pw.debug.table_to_pandas(result)

    assert len(result) == 5
    assert result.iloc[0].ret[0] == txt
    assert result.iloc[0].ret[1] == pw.Json({})


def test_recursive_from_model_name():
    splitter = RecursiveSplitter(model_name="gpt-4", chunk_size=30, chunk_overlap=0)
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."  # 26 tokens in cl100k_base
    big_txt = "\n\n".join([txt] * 5)
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=big_txt)]))

    result = input_table.select(ret=splitter(pw.this.ret)).flatten(pw.this.ret)
    result = pw.debug.table_to_pandas(result)

    assert len(result) == 5
    assert result.iloc[0].ret[0] == txt
    assert result.iloc[0].ret[1] == pw.Json({})

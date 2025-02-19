# Copyright © 2024 Pathway

from __future__ import annotations

import json
import os

import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm import embedders


@pytest.mark.skip(reason="fails on CI for lack of api keys")
def test_oai_vs_llm():
    if "OPENAI_API_KEY" not in os.environ:
        from common.shadows import fs

        api_key = json.loads(
            fs.open("vault://kv.v2:deployments@/legal_rag_demo").read()
        )["OPENAI_KEY"]
    else:
        api_key = os.environ["OPENAI_API_KEY"]
    embedder_llm = embedders.LiteLLMEmbedder(model="text-embedding-ada-002")
    t = pw.debug.table_from_markdown(
        """
    txt  | model
    Text | text-embedding-ada-002
    """
    )
    r1 = t.select(ret=embedder_llm(pw.this.txt, api_key=api_key))

    embedder_oai = embedders.OpenAIEmbedder(model=None, api_key=api_key)
    r2 = t.select(ret=embedder_oai(pw.this.txt, model=pw.this.model))

    assert_table_equality(r1, r2)


@pytest.mark.parametrize("model", ["text-embedding-ada-002", "text-embedding-3-large"])
@pytest.mark.parametrize("strategy", ["start", "end"])
def test_openai_context_truncation(model: str, strategy: str):
    # A -> 0.125 tokens, B -> 0.25 tokens
    text = "A" * 65600 + "B" * 65600

    expected_substring = "A" if strategy == "start" else "B"
    trunced_text = embedders.OpenAIEmbedder.truncate_context(
        model=model, text=text, strategy=strategy
    )

    assert expected_substring * 3 in trunced_text


@pytest.mark.parametrize("model", ["text-embedding-ada-002", "text-embedding-3-large"])
@pytest.mark.parametrize("strategy", ["start", "end"])
def test_openai_context_no_truncation(model: str, strategy: str):
    text = "A" * 200 + "B" * 200

    processed_text = embedders.OpenAIEmbedder.truncate_context(
        model=model, text=text, strategy=strategy
    )
    assert len(processed_text) == 400
    assert "A" in processed_text
    assert "B" in processed_text

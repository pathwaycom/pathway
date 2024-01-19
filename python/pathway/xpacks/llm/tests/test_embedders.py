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

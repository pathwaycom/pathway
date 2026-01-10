# Copyright © 2026 Pathway

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


# ===== BedrockEmbedder Tests =====

from pathway.internals.udfs import DiskCache, ExponentialBackoffRetryStrategy


@pytest.mark.parametrize(
    "model_id",
    ["amazon.titan-embed-text-v2:0", "amazon.titan-embed-text-v1", None],
)
@pytest.mark.parametrize(
    "retry_strategy",
    [ExponentialBackoffRetryStrategy(max_retries=6, backoff_factor=2.5), None],
)
@pytest.mark.parametrize(
    "cache_strategy",
    [DiskCache(), None],
)
def test_bedrock_embedder_init(model_id, retry_strategy, cache_strategy):
    embedder = embedders.BedrockEmbedder(
        model_id=model_id,
        region_name="us-east-1",
        retry_strategy=retry_strategy,
        cache_strategy=cache_strategy,
    )

    assert embedder is not None
    assert embedder.kwargs is not None
    assert embedder.executor is not None

    if cache_strategy is None:
        assert embedder.cache_strategy is None
    else:
        assert embedder.cache_strategy is not None


@pytest.mark.parametrize(
    "model_id",
    [
        "amazon.titan-embed-text-v2:0",
        "cohere.embed-english-v3",
        "cohere.embed-multilingual-v3",
    ],
)
def test_bedrock_embedder_model_id(model_id):
    embedder = embedders.BedrockEmbedder(model_id=model_id, region_name="us-east-1")

    assert embedder.kwargs.get("model_id") == model_id


def test_bedrock_embedder_default_model():
    embedder = embedders.BedrockEmbedder(region_name="us-east-1")

    assert embedder.kwargs.get("model_id") == "amazon.titan-embed-text-v2:0"


@pytest.mark.parametrize(
    "region_name",
    ["us-east-1", "eu-west-1", "ap-northeast-1", None],
)
def test_bedrock_embedder_region_config(region_name):
    embedder = embedders.BedrockEmbedder(
        model_id="amazon.titan-embed-text-v2:0",
        region_name=region_name,
    )

    assert embedder.region_name == region_name


def test_bedrock_embedder_aws_credentials():
    embedder = embedders.BedrockEmbedder(
        model_id="amazon.titan-embed-text-v2:0",
        region_name="us-east-1",
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        aws_session_token="test_token",
    )

    assert embedder.aws_access_key_id == "test_key"
    assert embedder.aws_secret_access_key == "test_secret"
    assert embedder.aws_session_token == "test_token"


def test_bedrock_embedder_extra_kwargs():
    embedder = embedders.BedrockEmbedder(
        model_id="amazon.titan-embed-text-v2:0",
        region_name="us-east-1",
        dimensions=512,
        normalize=True,
    )

    assert embedder.kwargs.get("dimensions") == 512
    assert embedder.kwargs.get("normalize") is True

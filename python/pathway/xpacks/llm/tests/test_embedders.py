# Copyright © 2026 Pathway

from __future__ import annotations

import json
import os

import pytest

import pathway as pw
from pathway.internals.udfs import (
    DiskCache,
    ExponentialBackoffRetryStrategy,
    FixedDelayRetryStrategy,
)
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


def test_bedrock_embedder_extra_kwargs():
    embedder = embedders.BedrockEmbedder(
        model_id="amazon.titan-embed-text-v2:0",
        region_name="us-east-1",
        dimensions=512,
        normalize=True,
    )

    assert embedder.kwargs.get("dimensions") == 512
    assert embedder.kwargs.get("normalize") is True


# ===== Retries on direct `__wrapped__` calls =====


@pytest.mark.asyncio
async def test_openai_embedder_wrapped_retries_transient_errors():
    from unittest.mock import AsyncMock, MagicMock

    embedder = embedders.OpenAIEmbedder(
        model="text-embedding-3-small",
        api_key="mock-key",
        truncation_keep_strategy=None,
        retry_strategy=FixedDelayRetryStrategy(max_retries=4, delay_ms=1),
    )

    ok_response = MagicMock()
    ok_response.data = [MagicMock(embedding=[0.1, 0.2])]
    create = AsyncMock(
        side_effect=[RuntimeError("connection reset by peer"), ok_response]
    )
    embedder.client = MagicMock()
    embedder.client.embeddings.create = create

    result = await embedder.__wrapped__(["."])

    assert len(result) == 1
    assert result[0].shape == (2,)
    assert create.await_count == 2


@pytest.mark.asyncio
async def test_litellm_embedder_wrapped_retries_transient_errors():
    from unittest.mock import AsyncMock, MagicMock, patch

    embedder = embedders.LiteLLMEmbedder(
        model="text-embedding-3-small",
        retry_strategy=FixedDelayRetryStrategy(max_retries=4, delay_ms=1),
    )

    ok_response = MagicMock()
    ok_response.data = [{"embedding": [0.1, 0.2]}]
    aembedding = AsyncMock(
        side_effect=[RuntimeError("connection reset by peer"), ok_response]
    )

    with patch("litellm.aembedding", aembedding):
        result = await embedder.__wrapped__(".")

    assert result.shape == (2,)
    assert aembedding.await_count == 2


@pytest.mark.asyncio
async def test_bedrock_embedder_wrapped_retries_transient_errors():
    from unittest.mock import AsyncMock, MagicMock, patch

    embedder = embedders.BedrockEmbedder(
        model_id="amazon.titan-embed-text-v2:0",
        region_name="us-east-1",
        retry_strategy=FixedDelayRetryStrategy(max_retries=4, delay_ms=1),
    )

    body = MagicMock()
    body.read = AsyncMock(return_value=json.dumps({"embedding": [0.1, 0.2]}))
    mock_client = AsyncMock()
    mock_client.invoke_model = AsyncMock(
        side_effect=[RuntimeError("connection reset by peer"), {"body": body}]
    )
    mock_client_cm = AsyncMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_client_cm.__aexit__.return_value = None
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client_cm

    with patch.object(embedder, "_session", mock_session):
        result = await embedder.__wrapped__("hello")

    assert result.shape == (2,)
    assert mock_client.invoke_model.await_count == 2


@pytest.mark.asyncio
async def test_marengo_embedder_wrapped_retries_transient_errors():
    from unittest.mock import AsyncMock, MagicMock

    embedder = embedders.MarengoEmbedder(
        retry_strategy=FixedDelayRetryStrategy(max_retries=4, delay_ms=1),
    )

    segment = MagicMock()
    segment.float_ = [0.1, 0.2]
    ok_response = MagicMock()
    ok_response.text_embedding.segments = [segment]
    create = AsyncMock(
        side_effect=[RuntimeError("connection reset by peer"), ok_response]
    )
    embedder._aclient = MagicMock()
    embedder._aclient.embed.create = create

    result = await embedder.__wrapped__(["hi"])

    assert len(result) == 1
    assert result[0].shape == (2,)
    assert create.await_count == 2


# ===== `get_embedding_dimension` =====


@pytest.mark.parametrize(
    "model,expected",
    [("text-embedding-3-small", 1536), ("text-embedding-3-large", 3072)],
)
def test_openai_embedder_dimension_of_known_model_sends_no_request(
    model: str, expected: int
):
    from unittest.mock import AsyncMock, MagicMock

    embedder = embedders.OpenAIEmbedder(model=model, api_key="mock-key")
    create = AsyncMock(side_effect=AssertionError("should not be called"))
    embedder.client = MagicMock()
    embedder.client.embeddings.create = create

    assert embedder.get_embedding_dimension() == expected
    assert create.await_count == 0


def test_openai_embedder_dimension_of_unknown_model_is_computed():
    from unittest.mock import AsyncMock, MagicMock

    embedder = embedders.OpenAIEmbedder(
        model="custom-embedder", api_key="mock-key", truncation_keep_strategy=None
    )
    response = MagicMock()
    response.data = [MagicMock(embedding=[0.1, 0.2, 0.3])]
    create = AsyncMock(return_value=response)
    embedder.client = MagicMock()
    embedder.client.embeddings.create = create

    assert embedder.get_embedding_dimension() == 3
    assert create.await_count == 1


def test_openai_embedder_dimension_with_shortened_vectors_is_computed():
    from unittest.mock import AsyncMock, MagicMock

    embedder = embedders.OpenAIEmbedder(
        model="text-embedding-3-large", api_key="mock-key", dimensions=2
    )
    response = MagicMock()
    response.data = [MagicMock(embedding=[0.1, 0.2])]
    create = AsyncMock(return_value=response)
    embedder.client = MagicMock()
    embedder.client.embeddings.create = create

    assert embedder.get_embedding_dimension() == 2
    assert create.await_count == 1

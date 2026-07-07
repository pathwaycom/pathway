# Copyright © 2026 Pathway

from __future__ import annotations

import pandas as pd
import pytest

import pathway as pw
from pathway.internals.udfs import (
    DiskCache,
    ExponentialBackoffRetryStrategy,
    FixedDelayRetryStrategy,
)
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm import llms


def test_prompt_chat_single_qa():
    func = llms.prompt_chat_single_qa
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=txt)]))
    result = input_table.select(ret=pw.unwrap(func(pw.this.ret)[0]["content"].as_str()))

    assert_table_equality(result, input_table)


@pytest.mark.parametrize(
    "model",
    ["gpt-4", None],
)
@pytest.mark.parametrize(
    "retry_strategy",
    [ExponentialBackoffRetryStrategy(max_retries=6, backoff_factor=2.5), None],
)
@pytest.mark.parametrize(
    "cache_strategy",
    [DiskCache(), None],
)
def test_openai_chat_init(model, retry_strategy, cache_strategy):
    llm = llms.OpenAIChat(
        model=model, retry_strategy=retry_strategy, cache_strategy=cache_strategy
    )

    assert llm is not None
    assert llm.kwargs is not None
    assert llm.executor is not None

    if cache_strategy is None:
        assert llm.cache_strategy is None
    else:
        assert llm.cache_strategy is not None


@pytest.mark.parametrize("model", ["gpt-4", "gpt-4o", None])
def test_llm_model_field(model):
    llm = llms.OpenAIChat(model=model)

    if model is None:
        assert llm.model is None
    else:
        assert model == llm.model


def test_empty_init_kwargs():
    llm = llms.OpenAIChat(model=None)

    assert llm.kwargs == {}

    assert llm.model is None


@pytest.mark.parametrize(
    "kwargs",
    [{"base_url": "openai_api"}, {}],
)
def test_init_kwargs(kwargs):
    llm = llms.OpenAIChat(**kwargs)

    assert llm.kwargs.get("temperature", "not_set") == kwargs.get(
        "temperature", "not_set"
    )


@pytest.mark.asyncio
async def test_openai_chat_wrapped_retries_transient_errors():
    from unittest.mock import AsyncMock, MagicMock

    llm = llms.OpenAIChat(
        model="gpt-3.5-turbo",
        api_key="mock-key",
        retry_strategy=FixedDelayRetryStrategy(max_retries=4, delay_ms=1),
    )

    ok_response = MagicMock()
    ok_response.choices[0].message.content = "mocked"
    create = AsyncMock(
        side_effect=[
            RuntimeError("upstream connect error or disconnect/reset before headers"),
            ok_response,
        ]
    )
    llm.client = MagicMock()
    llm.client.chat.completions.create = create

    response = await llm.__wrapped__(
        [{"role": "system", "content": "hey, whats your name?"}], max_tokens=2
    )

    assert response == "mocked"
    assert create.await_count == 2


VALID_ARGS = ["top_p", "temperature", "max_tokens"]
INVALID_ARGS = ["made_up_arg"]


@pytest.mark.parametrize("model", ["gpt-4", "gpt-4o", None])
@pytest.mark.parametrize("call_arg", [*VALID_ARGS, *INVALID_ARGS])
def test_openai_call_args(model, call_arg):
    llm = llms.OpenAIChat(model=model)

    if model is None:
        assert llm._accepts_call_arg(call_arg) is False
    else:
        assert llm._accepts_call_arg(call_arg) is (call_arg in VALID_ARGS)


# Fails in public repo CI: https://github.com/pathwaycom/pathway/actions/runs/22984451334/job/66733559648#step:5:7905
# I didn't manage to reproduce it locally so far
@pytest.mark.xfail
@pytest.mark.parametrize(
    "model",
    [
        "claude-3-5-sonnet-20240620",
        "claude-3-opus-20240229",
        "anthropic/claude-3-5-sonnet-20240620",
        None,
    ],
)
@pytest.mark.parametrize("call_arg", [*VALID_ARGS, *INVALID_ARGS])
def test_anthropic_call_args(model, call_arg):
    llm = llms.LiteLLMChat(model=model)

    if model is None:
        assert llm._accepts_call_arg(call_arg) is False
    else:
        assert llm._accepts_call_arg(call_arg) is (call_arg in VALID_ARGS)


@pytest.mark.parametrize("model", ["cohere/command-r", "antrophic/claude-3-5-sonnet"])
@pytest.mark.parametrize("call_arg", ["stream_options", "response_format"])
def test_mixed_call_args(model, call_arg):
    # arguments that antrophic supports but the cohere does not

    llm = llms.LiteLLMChat(model=model)

    if model is None:
        assert llm._accepts_call_arg(call_arg) is False
    else:
        if llm.model == "command-r":
            assert llm._accepts_call_arg(call_arg) is False
        elif llm.model == "claude-3-5-sonnet":
            assert llm._accepts_call_arg(call_arg)


# ===== BedrockChat Tests =====


@pytest.mark.parametrize(
    "model_id",
    ["anthropic.claude-3-sonnet-20240229-v1:0", "meta.llama3-70b-instruct-v1:0", None],
)
@pytest.mark.parametrize(
    "retry_strategy",
    [ExponentialBackoffRetryStrategy(max_retries=6, backoff_factor=2.5), None],
)
@pytest.mark.parametrize(
    "cache_strategy",
    [DiskCache(), None],
)
def test_bedrock_chat_init(model_id, retry_strategy, cache_strategy):
    llm = llms.BedrockChat(
        model_id=model_id,
        region_name="us-east-1",
        retry_strategy=retry_strategy,
        cache_strategy=cache_strategy,
    )

    assert llm is not None
    assert llm.kwargs is not None
    assert llm.executor is not None

    if cache_strategy is None:
        assert llm.cache_strategy is None
    else:
        assert llm.cache_strategy is not None


@pytest.mark.parametrize(
    "model_id",
    ["anthropic.claude-3-sonnet-20240229-v1:0", "amazon.titan-text-premier-v1:0", None],
)
def test_bedrock_model_field(model_id):
    llm = llms.BedrockChat(model_id=model_id, region_name="us-east-1")

    if model_id is None:
        assert llm.model is None
    else:
        assert model_id == llm.model


def test_bedrock_empty_init_kwargs():
    llm = llms.BedrockChat(model_id=None, region_name="us-east-1")

    assert "model_id" not in llm.kwargs
    assert llm.model is None


BEDROCK_VALID_ARGS = ["max_tokens", "temperature", "top_p", "stop_sequences", "top_k"]
BEDROCK_INVALID_ARGS = ["made_up_arg", "logit_bias"]


@pytest.mark.parametrize(
    "model_id",
    ["anthropic.claude-3-sonnet-20240229-v1:0", None],
)
@pytest.mark.parametrize("call_arg", [*BEDROCK_VALID_ARGS, *BEDROCK_INVALID_ARGS])
def test_bedrock_call_args(model_id, call_arg):
    llm = llms.BedrockChat(model_id=model_id, region_name="us-east-1")

    # BedrockChat always returns based on supported_args, model_id doesn't affect it
    assert llm._accepts_call_arg(call_arg) is (call_arg in BEDROCK_VALID_ARGS)


@pytest.mark.asyncio
async def test_bedrock_chat_wrapped_retries_transient_errors():
    from unittest.mock import AsyncMock, MagicMock, patch

    llm = llms.BedrockChat(
        model_id="anthropic.claude-3",
        region_name="us-east-1",
        retry_strategy=FixedDelayRetryStrategy(max_retries=4, delay_ms=1),
    )

    mock_client = AsyncMock()
    mock_client.converse = AsyncMock(
        side_effect=[
            RuntimeError("connection reset by peer"),
            {"output": {"message": {"content": [{"text": "mocked"}]}}},
        ]
    )
    mock_client_cm = AsyncMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_client_cm.__aexit__.return_value = None
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client_cm

    with patch.object(llm, "_session", mock_session):
        response = await llm.__wrapped__([{"role": "user", "content": "hi"}])

    assert response == "mocked"
    assert mock_client.converse.await_count == 2


@pytest.mark.asyncio
async def test_bedrock_dynamic_args_routing():
    from unittest.mock import AsyncMock, MagicMock, patch

    llm = llms.BedrockChat(model_id="anthropic.claude-3", region_name="us-east-1")

    mock_client = AsyncMock()
    mock_client.converse = AsyncMock(
        return_value={"output": {"message": {"content": [{"text": "mocked"}]}}}
    )

    # Explicit async context manager returned by session.client(...)
    mock_client_cm = AsyncMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_client_cm.__aexit__.return_value = None

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client_cm

    with patch.object(llm, "_session", mock_session):
        await llm.__wrapped__(
            [{"role": "user", "content": "hi"}], top_k=250, temperature=0.7
        )

    mock_client.converse.assert_called_once()
    call_kwargs = mock_client.converse.call_args.kwargs

    assert call_kwargs["inferenceConfig"]["temperature"] == 0.7
    assert "additionalModelRequestFields" in call_kwargs
    assert call_kwargs["additionalModelRequestFields"]["top_k"] == 250

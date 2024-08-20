# Copyright © 2024 Pathway

from __future__ import annotations

import pandas as pd
import pytest

import pathway as pw
from pathway.internals.udfs import DiskCache, ExponentialBackoffRetryStrategy
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

    assert llm.kwargs.get("base_url", "not_set") == kwargs.get("base_url", "not_set")


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


@pytest.mark.parametrize(
    "model",
    [
        "claude-3-5-sonnet-20240620",
        "claude-3-opus-20240229",
        "antrophic/claude-3-5-sonnet-20240620",
        None,
    ],
)
@pytest.mark.parametrize("call_arg", [*VALID_ARGS, *INVALID_ARGS])
def test_antrophic_call_args(model, call_arg):
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

import pytest

import pathway as pw
from pathway.udfs import ExponentialBackoffRetryStrategy
from pathway.xpacks.llm import llms
from pathway.xpacks.llm._utils import _run_async


@pytest.mark.parametrize(
    "model",
    ["gpt-3.5-turbo"],
)
def test_llm_func_openai(model):
    llm = llms.OpenAIChat(
        model=model, retry_strategy=ExponentialBackoffRetryStrategy(max_retries=4)
    )
    response = _run_async(
        llm.func(
            messages=[dict(role="system", content="hey, whats your name?")],
            max_tokens=2,
        )
    )

    assert isinstance(response, str)


@pytest.mark.parametrize(
    "model",
    ["gpt-3.5-turbo"],
)
def test_llm_wrapped_openai(model):
    llm = llms.OpenAIChat(
        model=model, retry_strategy=ExponentialBackoffRetryStrategy(max_retries=4)
    )
    response = _run_async(
        llm.__wrapped__(
            messages=[dict(role="system", content="hey, whats your name?")],
            max_tokens=2,
        )
    )

    assert isinstance(response, str)


def test_llm_apply_openai():
    chat = llms.OpenAIChat(
        model=None, retry_strategy=ExponentialBackoffRetryStrategy(max_retries=4)
    )
    t = pw.debug.table_from_markdown(
        """
        txt     | model
        Wazzup? | gpt-3.5-turbo
        """
    )
    resp_table = t.select(
        ret=chat(llms.prompt_chat_single_qa(t.txt), model=t.model, max_tokens=2)
    )

    _, table_values = pw.debug.table_to_dicts(resp_table)

    values_ls = list(table_values["ret"].values())

    assert isinstance(values_ls[0], str)

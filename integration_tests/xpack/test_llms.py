import pandas as pd
import pytest

import pathway as pw
from pathway.udfs import ExponentialBackoffRetryStrategy
from pathway.xpacks.llm import llms, rerankers
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


def parse_response_func(response) -> float:
    if response == "RELEVANT":
        return 1.0
    elif response == "NOT_RELEVANT":
        return 0.0
    elif response == "NOT_SURE":
        return 0.5
    else:
        raise ValueError(f"Invalid response: {response}")


def test_llm_rerank():
    docs = [
        {"text": "Pasta is an italian dish"},
        {"text": "Köfte is a Turkish dish"},
        {"text": "Sushi is a Japanese dish"},
        {"text": "Foobar"},
    ]
    query = "Where does pasta come from?"

    df = pd.DataFrame({"docs": docs, "query": query})

    chat = llms.OpenAIChat(model="gpt-4o-mini")
    reranker = rerankers.LLMReranker(llm=chat)
    docs_table = pw.debug.table_from_pandas(df)

    res = docs_table.select(score=reranker(pw.this.docs["text"], pw.this.query))
    res_df = pw.debug.table_to_pandas(res)
    assert len(res_df) == len(docs)

    custom_prompt = (
        "Rank the supplied document's relevance to the given query."
        "Answer with the following categories:\n"
        "RELEVANT, NOT_RELEVANT, NOT_SURE\n"
        "Do not output anything other than the category.\n"
        "-- Document --\n"
        "{context}\n"
        "-- End Document --\n"
        "-- Query --\n"
        "{query}\n"
        "-- End Query --\n"
    )

    reranker = rerankers.LLMReranker(
        llm=chat, prompt_template=custom_prompt, response_parser=parse_response_func
    )

    docs_table = pw.debug.table_from_pandas(df)
    res = docs_table.select(score=reranker(pw.this.docs["text"], pw.this.query))
    res_df = pw.debug.table_to_pandas(res)
    assert len(res_df) == len(docs)


def test_llm_openai_list_of_jsons():
    chat = llms.OpenAIChat(
        model=None, retry_strategy=ExponentialBackoffRetryStrategy(max_retries=4)
    )
    t = pw.debug.table_from_rows(
        pw.schema_from_types(txt=list[pw.Json]),
        rows=[
            (
                [
                    {"role": "user", "content": "Wazzup?"},
                ],
            )
        ],
    )
    resp_table = t.select(ret=chat(t.txt, model="gpt-3.5-turbo", max_tokens=2))

    _, table_values = pw.debug.table_to_dicts(resp_table)

    values_ls = list(table_values["ret"].values())

    assert isinstance(values_ls[0], str)


def test_hf_pipeline_gpt2():
    chat = llms.HFPipelineChat(model="gpt2")
    t = pw.debug.table_from_markdown(
        """
        txt
        Wazzup?
        """
    )
    resp_table = t.select(ret=chat(t.txt, max_new_tokens=2))

    _, table_values = pw.debug.table_to_dicts(resp_table)

    values_ls = list(table_values["ret"].values())

    assert isinstance(values_ls[0], str)


def test_hf_pipeline_tinyllama():
    chat = llms.HFPipelineChat(model="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    t = pw.debug.table_from_markdown(
        """
        txt
        Wazzup?
        """
    )
    resp_table = t.select(ret=chat(llms.prompt_chat_single_qa(t.txt), max_new_tokens=2))

    _, table_values = pw.debug.table_to_dicts(resp_table)

    values_ls = list(table_values["ret"].values())

    assert isinstance(values_ls[0], str)

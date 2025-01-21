# Copyright © 2024 Pathway

from __future__ import annotations

import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm import llms
from pathway.xpacks.llm._utils import _unwrap_udf
from pathway.xpacks.llm.question_answering import BaseRAGQuestionAnswerer
from pathway.xpacks.llm.vector_store import VectorStoreServer

from .mocks import IdentityMockChat
from .utils import build_vector_store, create_rag_app


@pw.udf
def fake_embeddings_model(x: str) -> list[float]:
    return [
        1.0 if x == "foo" else 0.0,
        1.0 if x in ("foo", "bar") else 0.0,
        1.0,
    ]


@pw.udf
def identity_chat_model(x: list[dict[str, pw.Json]], model: str) -> str:
    return model + "," + x[0]["content"].as_str()


@pw.udf
def _prompt_template(query: str, context: str) -> str:
    return context


@pw.udf
def _summarize_template(docs: list[str]) -> str:
    return f"summarize,{','.join(docs)}"


def test_base_rag():
    schema = pw.schema_from_types(data=bytes, _metadata=dict)
    input = pw.debug.table_from_rows(
        schema=schema, rows=[("foo", {}), ("bar", {}), ("baz", {})]
    )

    vector_server = VectorStoreServer(
        input,
        embedder=fake_embeddings_model,
    )

    rag = BaseRAGQuestionAnswerer(
        IdentityMockChat(),
        vector_server,
        prompt_template=_prompt_template,
        summarize_template=_summarize_template,
        search_topk=1,
    )

    answer_queries = pw.debug.table_from_rows(
        schema=rag.AnswerQuerySchema,
        rows=[
            ("foo", None, "gpt3.5", False),
        ],
    )

    answer_output = rag.answer_query(answer_queries)

    casted_table = answer_output.select(
        result=pw.apply_with_type(lambda x: x.value, str, pw.this.result["response"])
    )

    assert_table_equality(
        casted_table,
        pw.debug.table_from_markdown(
            """
            result
            gpt3.5,foo
            """
        ),
    )

    summarize_query = pw.debug.table_from_rows(
        schema=rag.SummarizeQuerySchema,
        rows=[(["foo", "bar"], "gpt2")],
    )

    summarize_outputs = rag.summarize_query(summarize_query)

    assert_table_equality(
        summarize_outputs.select(result=pw.this.result),
        pw.debug.table_from_markdown(
            """
            result
            gpt2,summarize,foo,bar
            """
        ),
    )


def test_rag_app_set_prompt():
    prompt_template = "Answer the question. Context: {context}\nQuestion: {query}"

    rag_app = create_rag_app(prompt_template=prompt_template)

    assert isinstance(rag_app.prompt_udf, pw.UDF)

    assert _unwrap_udf(rag_app.prompt_udf)(query=" ", context=" ")


def test_rag_app_set_callable_prompt():
    def prompt_template(query: str, context: str) -> str:
        return f"Q: {query}, C: {context}"

    rag_app = create_rag_app(prompt_template=prompt_template)

    assert isinstance(rag_app.prompt_udf, pw.UDF)

    assert _unwrap_udf(rag_app.prompt_udf)(query=" ", context=" ")


def test_rag_app_set_udf_prompt():
    @pw.udf
    def prompt_template(query: str, context: str) -> str:
        return f"Q: {query}, C: {context}"

    rag_app = create_rag_app(prompt_template=prompt_template)

    assert isinstance(rag_app.prompt_udf, pw.UDF)

    assert _unwrap_udf(rag_app.prompt_udf)(query=" ", context=" ")


@pytest.mark.parametrize(
    "prompt",
    [
        "Context: {context}, query: {query}, abc: {abc}",
        "Context: {something}, query: {else}",
        "Context: {context}",
        "No placeholder template.",
    ],
)
def test_invalid_prompt_template_raises_error(prompt: str):
    @pw.udf
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    class FakeChatModel(llms.BaseChat):
        async def __wrapped__(self, *args, **kwargs) -> str:
            return "Text"

        def _accepts_call_arg(self, arg_name: str) -> bool:
            return True

    chat = FakeChatModel()

    vector_server = build_vector_store(fake_embeddings_model)

    with pytest.raises(ValueError) as exc_info:
        BaseRAGQuestionAnswerer(
            llm=chat,
            indexer=vector_server,
            prompt_template=prompt,
        )

    err_msg = str(exc_info.value)

    assert "context" in err_msg
    assert "query" in err_msg

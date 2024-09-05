# Copyright © 2024 Pathway

from __future__ import annotations

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.question_answering import BaseRAGQuestionAnswerer
from pathway.xpacks.llm.vector_store import VectorStoreServer

from .mocks import IdentityMockChat


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
def _short_template(query: str, docs: list[pw.Json]) -> str:
    return f"short,{query},{','.join(doc.as_dict()['text'] for doc in docs)}"


@pw.udf
def _long_template(query: str, docs: list[pw.Json]) -> str:
    return f"long,{query},{','.join(doc.as_dict()['text'] for doc in docs)}"


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
        short_prompt_template=_short_template,
        long_prompt_template=_long_template,
        summarize_template=_summarize_template,
        search_topk=2,
    )

    answer_queries = pw.debug.table_from_rows(
        schema=rag.AnswerQuerySchema,
        rows=[
            ("foo", None, "gpt3.5", "short"),
            ("baz", None, "gpt4", "long"),
        ],
    )

    answer_output = rag.answer_query(answer_queries)
    assert_table_equality(
        answer_output.select(result=pw.this.result),
        pw.debug.table_from_markdown(
            """
            result
            gpt3.5,short,foo,foo,bar
            gpt4,long,baz,baz,bar
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

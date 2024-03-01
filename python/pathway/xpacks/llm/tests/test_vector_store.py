# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.vector_store import VectorStoreServer


class DebugStatsInputSchema(VectorStoreServer.StatisticsQuerySchema):
    debug: str | None = pw.column_definition(default_value=None)


def _test_vs(fake_embeddings_model):
    docs = pw.io.fs.read(
        __file__,
        format="binary",
        mode="static",
        with_metadata=True,
    )

    vector_server = VectorStoreServer(
        docs,
        embedder=fake_embeddings_model,
    )

    info_queries = pw.debug.table_from_rows(
        schema=DebugStatsInputSchema,
        rows=[
            (None,),
        ],
    ).select()

    info_outputs = vector_server.statistics_query(info_queries)
    assert_table_equality(
        info_outputs.select(result=pw.unwrap(pw.this.result["file_count"].as_int())),
        pw.debug.table_from_markdown(
            """
            result
            1
            """
        ),
    )

    input_queries = pw.debug.table_from_rows(
        schema=VectorStoreServer.InputsQuerySchema,
        rows=[
            (None, "**/*.py"),
        ],
    )

    input_outputs = vector_server.inputs_query(input_queries)

    @pw.udf
    def get_file_name(result_js) -> str:
        if len(result_js):
            return result_js[0]["path"].value.split("/")[-1].replace('"', "")
        else:
            return str(result_js)

    assert_table_equality(
        input_outputs.select(result=pw.unwrap(get_file_name(pw.this.result))),
        pw.debug.table_from_markdown(
            """
            result
            test_vector_store.py
            """
        ),
    )

    _, rows = pw.debug.table_to_dicts(input_outputs)
    (val,) = rows["result"].values()
    val = val[0]  # type: ignore

    assert isinstance(val, pw.Json)
    input_result = val.value
    assert isinstance(input_result, dict)

    assert "owner" in input_result.keys()
    assert "modified_at" in input_result.keys()

    # parse_graph.G.clear()
    retrieve_queries = pw.debug.table_from_markdown(
        """
        query | k metadata_filter | filepath_globpattern
        "Foo" | 1                 |
        """,
        schema=VectorStoreServer.RetrieveQuerySchema,
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.value  # type: ignore # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["dist"] < 1.0e-6  # type: ignore # the dist is not 0 due to float normalization
    assert query_result["text"]  # just check if some text was returned


def test_sync_embedder():
    @pw.udf
    def fake_embeddings_model(x: str):
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model)


def test_async_embedder():
    @pw.udf_async
    async def fake_embeddings_model(x: str):
        asyncio.sleep
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model)

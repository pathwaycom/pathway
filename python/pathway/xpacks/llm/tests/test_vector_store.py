# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import os
import pathlib
import time
from multiprocessing import Process

import pytest
import requests

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.vector_store import VectorStoreClient, VectorStoreServer

PATHWAY_HOST = "127.0.0.1"
PATHWAY_PORT = int(os.environ.get("PATHWAY_MONITORING_HTTP_PORT", "20000")) + 20000


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
    def get_file_name(path) -> str:
        return path.split("/")[-1].replace('"', "")

    assert_table_equality(
        input_outputs.select(
            result=pw.unwrap(
                get_file_name(pw.this.result["input_files"][0].to_string())
            )
        ),
        pw.debug.table_from_markdown(
            """
            result
            test_vector_store.py
            """
        ),
    )

    # parse_graph.G.clear()
    retrieve_queries = pw.debug.table_from_markdown(
        """
        metadata_filter | filepath_globpattern | query | k
                        |                      | "Foo" | 1
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


def pathway_server(tmp_path):
    docs = pw.io.fs.read(
        tmp_path,
        format="binary",
        mode="streaming",
        with_metadata=True,
    )

    @pw.udf_async
    async def fake_embeddings_model(x: str):
        return [1.0, 1.0, 0.0]

    vector_server = VectorStoreServer(
        docs,
        embedder=fake_embeddings_model,
    )

    thread = vector_server.run_server(
        host=PATHWAY_HOST,
        port=PATHWAY_PORT,
        threaded=True,
        with_cache=False,
    )
    thread.join()


@pytest.mark.skip(reason="works locally, fails on CI; to be investigated")
def test_similarity_search_without_metadata(tmp_path: pathlib.Path):
    with open(tmp_path / "file_one.txt", "w+") as f:
        f.write("foo")

    p = Process(target=pathway_server, args=[tmp_path])
    p.start()
    time.sleep(5)
    client = VectorStoreClient(host=PATHWAY_HOST, port=PATHWAY_PORT)
    MAX_ATTEMPTS = 5
    attempts = 0
    output = []
    while attempts < MAX_ATTEMPTS:
        try:
            output = client("foo")
        except requests.exceptions.RequestException:
            pass
        else:
            break
        time.sleep(1)
        attempts += 1
    p.terminate()
    time.sleep(2)
    assert len(output) == 1
    assert output[0]["dist"] < 0.0001
    assert output[0]["text"] == "foo"
    assert "metadata" in output[0]

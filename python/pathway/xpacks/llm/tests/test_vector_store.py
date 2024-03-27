# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import multiprocessing
import pathlib
import time

import pytest
from langchain.text_splitter import CharacterTextSplitter
from langchain_core.embeddings import Embeddings

import pathway as pw
from pathway.tests.utils import assert_table_equality, xfail_on_multiple_threads
from pathway.xpacks.llm.vector_store import VectorStoreClient, VectorStoreServer


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


class FakeEmbeddings(Embeddings):
    def embed_query(self, text: str) -> list[float]:
        return [1.0, 1.0, 1.0 if text == "foo" else -1.0]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self.embed_query(text) for text in texts]


def pathway_server(tmp_path, host, port):
    data_sources = []
    data_sources.append(
        pw.io.fs.read(
            tmp_path,
            format="binary",
            mode="streaming",
            with_metadata=True,
        )
    )

    embeddings_model = FakeEmbeddings()
    splitter = CharacterTextSplitter("\n\n", chunk_size=4, chunk_overlap=0)

    vector_server = VectorStoreServer.from_langchain_components(
        *data_sources, embedder=embeddings_model, splitter=splitter
    )
    thread = vector_server.run_server(
        host=host,
        port=port,
        threaded=True,
        with_cache=False,
    )
    thread.join()


@pytest.mark.flaky(reruns=4, reruns_delay=20)
@xfail_on_multiple_threads
def test_vector_store_with_langchain(tmp_path: pathlib.Path) -> None:
    host = "0.0.0.0"
    port = 8773
    with open(tmp_path / "file_one.txt", "w+") as f:
        f.write("foo\n\nbar")

    time.sleep(5)
    p = multiprocessing.Process(target=pathway_server, args=[tmp_path, host, port])
    p.start()
    time.sleep(5)
    client = VectorStoreClient(host=host, port=port)
    MAX_ATTEMPTS = 8
    attempts = 0
    output = []
    while attempts < MAX_ATTEMPTS:
        try:
            output = client.query("foo", 1)
        except Exception:
            pass
        time.sleep(3)
        attempts += 1
    time.sleep(3)
    p.terminate()
    assert len(output) == 1
    assert output[0]["text"] == "foo"

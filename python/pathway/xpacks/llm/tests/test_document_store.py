# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio

import pytest

import pathway as pw
from pathway.engine import BruteForceKnnMetricKind
from pathway.stdlib.indexing import (
    BruteForceKnnFactory,
    HybridIndexFactory,
    TantivyBM25Factory,
    UsearchKnnFactory,
)
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.document_store import DocumentStore
from pathway.xpacks.llm.servers import DocumentStoreServer


class DebugStatsInputSchema(DocumentStore.StatisticsQuerySchema):
    debug: str | None = pw.column_definition(default_value=None)


def _test_vs(fake_embeddings_model):
    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {"path": "pathway/xpacks/llm/tests/test_vector_store.py"},
            )
        ],
    )
    index_factory = BruteForceKnnFactory(
        dimensions=3,
        reserved_space=10,
        embedder=fake_embeddings_model,
        metric=BruteForceKnnMetricKind.COS,
    )

    vector_server = DocumentStore(docs, retriever_factory=index_factory)

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
        schema=DocumentStore.InputsQuerySchema,
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

    assert "path" in input_result.keys()

    # parse_graph.G.clear()
    retrieve_queries = pw.debug.table_from_markdown(
        """
        query | k | metadata_filter | filepath_globpattern
        "Foo" | 1 |                 |
        """,
        schema=DocumentStore.RetrieveQuerySchema,
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
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model)


def test_async_embedder():
    @pw.udf_async
    async def fake_embeddings_model(x: str) -> list[float]:
        asyncio.sleep
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model)


@pytest.mark.parametrize(
    "glob_filter",
    [
        "",
        "**/*.py",
        "pathway/xpacks/llm/tests/test_vector_store.py",
    ],
)
def test_vs_filtering(glob_filter):
    @pw.udf
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {"path": "pathway/xpacks/llm/tests/test_vector_store.py"},
            )
        ],
    )

    index_factory = BruteForceKnnFactory(
        dimensions=3,
        reserved_space=10,
        embedder=fake_embeddings_model,
        metric=BruteForceKnnMetricKind.COS,
    )

    vector_server = DocumentStore(docs, retriever_factory=index_factory)

    # parse_graph.G.clear()
    retrieve_queries = pw.debug.table_from_markdown(
        f"""
        query | k | metadata_filter | filepath_globpattern
        "Foo" | 1 |                 | {glob_filter}
        """,
        schema=DocumentStore.RetrieveQuerySchema,
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.as_list()  # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["dist"] < 1.0e-6  # type: ignore # the dist is not 0 due to float normalization
    assert query_result["text"]  # just check if some text was returned


@pytest.mark.parametrize(
    "glob_filter",
    [
        "somefile.pdf",
        "**/*.txt",
        "pathway/test_vector_store.py",
        "src.py",
        "`pathway/xpacks/llm/tests/test_vector_store.py`",
    ],
)
def test_vs_filtering_negatives(glob_filter):
    @pw.udf
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {"path": "pathway/xpacks/llm/tests/test_vector_store.py"},
            )
        ],
    )

    index_factory = BruteForceKnnFactory(
        dimensions=3,
        reserved_space=10,
        embedder=fake_embeddings_model,
        metric=BruteForceKnnMetricKind.COS,
    )

    vector_server = DocumentStore(docs, retriever_factory=index_factory)

    # parse_graph.G.clear()
    retrieve_queries = pw.debug.table_from_markdown(
        f"""
        query | k | metadata_filter | filepath_globpattern
        "Foo" | 1 |                 | {glob_filter}
        """,
        schema=DocumentStore.RetrieveQuerySchema,
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)

    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    assert len(val.as_list()) == 0


@pytest.mark.parametrize(
    "metadata_filter",
    [
        "",
        "contains(path, `test_vector_store`)",
        'contains(path, `"test_vector_store"`)',
        "contains(path, `pathway/xpacks/llm/tests/test_vector_store.py`)",
        "path == `pathway/xpacks/llm/tests/test_vector_store.py`",
        "globmatch(`pathway/xpacks/llm/tests/test_vector_store.py`, path)",
    ],
)
def test_vs_filtering_metadata(metadata_filter):
    @pw.udf
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {"path": "pathway/xpacks/llm/tests/test_vector_store.py"},
            )
        ],
    )

    index_factory = BruteForceKnnFactory(
        dimensions=3,
        reserved_space=10,
        embedder=fake_embeddings_model,
        metric=BruteForceKnnMetricKind.COS,
    )

    vector_server = DocumentStore(docs, retriever_factory=index_factory)

    retrieve_queries = pw.debug.table_from_rows(
        schema=DocumentStore.RetrieveQuerySchema,
        rows=[("Foo", 1, metadata_filter, None)],
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.as_list()  # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["dist"] < 1.0e-6  # type: ignore # the dist is not 0 due to float normalization
    assert query_result["text"]  # just check if some text was returned


@pytest.mark.parametrize(
    "metadata_filter",
    [
        "",
        "contains(path, `Document Enregistrement Universel 2023 publié à l'XYZ le 28 février 2024.pdf`)",
        "path == `Document Enregistrement Universel 2023 publié à l'XYZ le 28 février 2024.pdf`",
        'path == "`Document Enregistrement Universel 2023 publié à l\'XYZ le 28 février 2024.pdf"`',
        "contains(path, `Document Enregistrement`)",
    ],
)
@pytest.mark.parametrize("globbing_filter", [None, "*.pdf"])
def test_vs_filtering_edge_cases(metadata_filter, globbing_filter):
    @pw.udf
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {
                    "path": "Document Enregistrement Universel 2023 publié à l'XYZ le 28 février 2024.pdf"
                },
            )
        ],
    )

    index_factory = BruteForceKnnFactory(
        dimensions=3,
        reserved_space=10,
        embedder=fake_embeddings_model,
        metric=BruteForceKnnMetricKind.COS,
    )

    vector_server = DocumentStore(docs, retriever_factory=index_factory)

    retrieve_queries = pw.debug.table_from_rows(
        schema=DocumentStore.RetrieveQuerySchema,
        rows=[("Foo", 1, metadata_filter, globbing_filter)],
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.as_list()  # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["text"]  # just check if some text was returned


@pytest.mark.parametrize(
    "host",
    ["0.0.0.0"],
)
@pytest.mark.parametrize(
    "port",
    [8000],
)
def test_docstore_server_hybridindex_builds(host, port):
    @pw.udf
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {"path": "pathway/xpacks/llm/tests/test_vector_store.py"},
            )
        ],
    )
    vector_index = UsearchKnnFactory(
        embedder=fake_embeddings_model, reserved_space=40, dimensions=3
    )
    bm25 = TantivyBM25Factory()

    hybrid_index = HybridIndexFactory([vector_index, bm25])

    document_store = DocumentStore(docs, retriever_factory=hybrid_index)

    document_server = DocumentStoreServer(
        host=host, port=port, document_store=document_store
    )

    assert document_server is not None  # :)

    retrieve_queries = pw.debug.table_from_rows(
        schema=DocumentStore.RetrieveQuerySchema,
        rows=[("Foo", 1, None, None)],
    )

    retrieve_outputs = document_store.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.as_list()  # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["text"]  # just check if some text was returned
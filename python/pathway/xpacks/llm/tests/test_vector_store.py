# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import pathlib

import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm import parsers
from pathway.xpacks.llm.vector_store import VectorStoreServer


class DebugStatsInputSchema(VectorStoreServer.StatisticsQuerySchema):
    debug: str | None = pw.column_definition(default_value=None)


def _test_vs(fake_embeddings_model, **run_kwargs):
    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {"path": "pathway/xpacks/llm/tests/test_vector_store.py"},
            )
        ],
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
        **run_kwargs,
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
        **run_kwargs,
    )

    _, rows = pw.debug.table_to_dicts(input_outputs, **run_kwargs)
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
        schema=VectorStoreServer.RetrieveQuerySchema,
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs, **run_kwargs)
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
    @pw.udf
    async def fake_embeddings_model(x: str) -> list[float]:
        await asyncio.sleep(0.001)
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model)


def test_embedder_preserves_params():
    call_count = 0

    @pw.udf(cache_strategy=pw.udfs.InMemoryCache())
    def fake_embeddings_model(x: str) -> list[float]:
        nonlocal call_count
        call_count += 1
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model)
    _test_vs(fake_embeddings_model)
    assert call_count == 4  # dimension x 2 (no cache used), doc, query


@pytest.mark.parametrize(
    "cache_strategy_cls",
    [
        None,
        pw.udfs.InMemoryCache,
        pw.udfs.DiskCache,
    ],
)
def test_embedder_cache_strategy(cache_strategy_cls, tmp_path: pathlib.Path):
    if cache_strategy_cls is not None:
        cache_strategy = cache_strategy_cls()
    else:
        cache_strategy = None

    persistent_storage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config.simple_config(
        pw.persistence.Backend.filesystem(persistent_storage_path),
    )

    @pw.udf(cache_strategy=cache_strategy)
    async def fake_embeddings_model(x: str) -> list[float]:
        await asyncio.sleep(0.001)
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model, persistence_config=persistence_config)


def test_async_embedder_preserves_params():
    call_count = 0

    @pw.udf(cache_strategy=pw.udfs.InMemoryCache())
    async def fake_embeddings_model(x: str) -> list[float]:
        await asyncio.sleep(0.001)
        nonlocal call_count
        call_count += 1
        return [1.0, 1.0, 0.0]

    _test_vs(fake_embeddings_model)
    _test_vs(fake_embeddings_model)
    assert call_count == 4  # dimension x 2 (no cache used), doc, query


@pytest.mark.parametrize("parser_cls", [parsers.Utf8Parser])
def test_vs_parsing(parser_cls):
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

    vector_server = VectorStoreServer(
        docs,
        parser=parser_cls(),
        embedder=fake_embeddings_model,
    )

    retrieve_queries = pw.debug.table_from_markdown(
        """
        query | k | metadata_filter | filepath_globpattern
        "Foo" | 1 |                 |
        """,
        schema=VectorStoreServer.RetrieveQuerySchema,
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.as_list()  # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["dist"] < 1.0e-6  # type: ignore # the dist is not 0 due to float normalization
    assert query_result["text"] == "test"


@pytest.mark.parametrize(
    "glob_filter",
    [
        "",
        "**/*.py",
        "pathway/xpacks/llm/tests/test_vector_store.py",
    ],
)
def test_vs_filtering(glob_filter):
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

    vector_server = VectorStoreServer(
        docs,
        embedder=fake_embeddings_model,
    )

    # parse_graph.G.clear()
    retrieve_queries = pw.debug.table_from_markdown(
        f"""
        query | k | metadata_filter | filepath_globpattern
        "Foo" | 1 |                 | {glob_filter}
        """,
        schema=VectorStoreServer.RetrieveQuerySchema,
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

    vector_server = VectorStoreServer(
        docs,
        embedder=fake_embeddings_model,
    )

    # parse_graph.G.clear()
    retrieve_queries = pw.debug.table_from_markdown(
        f"""
        query | k | metadata_filter | filepath_globpattern
        "Foo" | 1 |                 | {glob_filter}
        """,
        schema=VectorStoreServer.RetrieveQuerySchema,
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
        "(path == `pathway/xpacks/llm/tests/test_vector_store.py`) && (published_date >= to_number(`1724351400`))",
    ],
)
def test_vs_filtering_metadata(metadata_filter):
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {
                    "path": "pathway/xpacks/llm/tests/test_vector_store.py",
                    "published_date": 1724351401,
                },
            )
        ],
    )

    vector_server = VectorStoreServer(
        docs,
        embedder=fake_embeddings_model,
    )

    retrieve_queries = pw.debug.table_from_rows(
        schema=VectorStoreServer.RetrieveQuerySchema,
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

    vector_server = VectorStoreServer(
        docs,
        embedder=fake_embeddings_model,
    )

    retrieve_queries = pw.debug.table_from_rows(
        schema=VectorStoreServer.RetrieveQuerySchema,
        rows=[("Foo", 1, metadata_filter, globbing_filter)],
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.as_list()  # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["text"]  # just check if some text was returned


def test_docstore_on_table_without_metadata():
    @pw.udf
    def fake_embeddings_model(x: str) -> list[float]:
        return [1.0, 1.0, 0.0]

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes),
        rows=[("test".encode("utf-8"),)],
    )

    vector_server = VectorStoreServer(
        docs,
        embedder=fake_embeddings_model,
    )

    retrieve_queries = pw.debug.table_from_rows(
        schema=vector_server.RetrieveQuerySchema,
        rows=[("Foo", 1, None, None)],
    )

    retrieve_outputs = vector_server.retrieve_query(retrieve_queries)
    _, rows = pw.debug.table_to_dicts(retrieve_outputs)
    (val,) = rows["result"].values()
    assert isinstance(val, pw.Json)
    (query_result,) = val.as_list()  # extract the single match
    assert isinstance(query_result, dict)
    assert query_result["text"] == "test"  # just check if some text was returned

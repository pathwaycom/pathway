# Copyright © 2024 Pathway

"""
Pathway vector search server and client.

The server reads source documents and build a vector index over them, then starts serving
HTTP requests.

The client queries the server and returns matching documents.
"""

import asyncio
import functools
import json
import threading
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import requests

import pathway as pw
import pathway.xpacks.llm.parsers
import pathway.xpacks.llm.splitters
from pathway.stdlib.ml import index


class QueryInputSchema(pw.Schema):
    query: str
    k: int
    metadata_filter: str | None = pw.column_definition(default_value=None)


class StatsInputSchema(pw.Schema):
    pass


@dataclass
class GraphResultTables:
    retrieval_results: pw.Table
    info_results: pw.Table
    input_results: pw.Table


def _unwrap_udf(func):
    if isinstance(func, pw.UDF):
        return func.__wrapped__
    return func


# https://stackoverflow.com/a/75094151
class _RunThread(threading.Thread):
    def __init__(self, coroutine):
        self.coroutine = coroutine
        self.result = None
        super().__init__()

    def run(self):
        self.result = asyncio.run(self.coroutine)


def _run_async(coroutine):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        thread = _RunThread(coroutine)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(coroutine)


def _coerce_sync(func: Callable) -> Callable:
    if asyncio.iscoroutinefunction(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return _run_async(func(*args, **kwargs))

        return wrapper
    else:
        return func


class VectorStoreServer:
    """
    Builds a document indexing pipeline and starts an HTTP REST server for nearest neighbors queries.

    Args:
        - docs: pathway tables typically coming out of connectors which contain source documents.
        - embedder: callable that embeds a single document
        - parser: callable that parses file contents into a list of documents
        - splitter: callable that splits long documents
    """

    def __init__(
        self,
        *docs: pw.Table,
        embedder: Callable[[str], list[float]],
        parser: Callable[[bytes], list[tuple[str, dict]]] | None = None,
        splitter: Callable[[str], list[tuple[str, dict]]] | None = None,
    ):
        self.docs = docs

        self.parser: Callable[[bytes], list[tuple[str, dict]]] = _unwrap_udf(
            parser if parser is not None else pathway.xpacks.llm.parsers.ParseUtf8()
        )
        self.splitter = _unwrap_udf(
            splitter
            if splitter is not None
            else pathway.xpacks.llm.splitters.null_splitter
        )
        self.embedder = _unwrap_udf(embedder)

        # detect the dimensionality of the embeddings
        self.embedding_dimension = len(_coerce_sync(self.embedder)("."))

    def _build_graph(
        self, retrieval_queries, info_queries, input_queries
    ) -> GraphResultTables:
        """
        Builds the pathway computation graph for indexing documents and serving queries.
        """
        docs_s = self.docs
        if not docs_s:
            raise ValueError(
                """Please provide at least one data source, e.g. read files from disk:

pw.io.fs.read('./sample_docs', format='binary', mode='static', with_metadata=True)
"""
            )
        if len(docs_s) == 1:
            (docs,) = docs_s
        else:
            docs: pw.Table = docs_s[0].concat_reindex(*docs_s[1:])  # type: ignore

        @pw.udf
        def parse_doc(data: bytes, metadata) -> list[pw.Json]:
            rets = self.parser(data)
            metadata = metadata.value
            return [dict(text=ret[0], metadata={**metadata, **ret[1]}) for ret in rets]  # type: ignore

        parsed_docs = docs.select(data=parse_doc(docs.data, docs._metadata)).flatten(
            pw.this.data
        )

        @pw.udf
        def split_doc(data_json: pw.Json) -> list[pw.Json]:
            data: dict = data_json.value  # type:ignore
            text = data["text"]
            metadata = data["metadata"]
            rets = self.splitter(text)
            return [
                dict(text=ret[0], metadata={**metadata, **ret[1]})  # type:ignore
                for ret in rets
            ]

        chunked_docs = parsed_docs.select(data=split_doc(pw.this.data)).flatten(
            pw.this.data
        )

        if asyncio.iscoroutinefunction(self.embedder):

            @pw.udf_async
            async def embedder(txt):
                result = await self.embedder(txt)
                return np.asarray(result)

        else:

            @pw.udf
            def embedder(txt):
                result = self.embedder(txt)
                return np.asarray(result)

        chunked_docs += chunked_docs.select(
            embedding=embedder(pw.this.data["text"].as_str())
        )

        knn_index = index.KNNIndex(
            chunked_docs.embedding,
            chunked_docs,
            distance_type="cosine",
            n_dimensions=self.embedding_dimension,
            metadata=chunked_docs.data["metadata"],
        )

        # VectorStore statistics computation
        @pw.udf
        def format_stats(counts, last_modified) -> pw.Json:
            if counts is not None:
                response = {"file_count": counts, "last_modified": last_modified.value}
            else:
                response = {"file_count": 0, "last_modified": None}
            return pw.Json(response)

        @pw.udf
        def format_inputs(paths: list[pw.Json]) -> pw.Json:
            input_set = []
            if paths:
                input_set = list(set([i.value for i in paths]))

            response = {"input_files": input_set}
            return pw.Json(response)  # type: ignore

        parsed_docs += parsed_docs.select(
            modified=pw.this.data["metadata"]["modified_at"],
            path=pw.this.data["metadata"]["path"],
        )
        stats = parsed_docs.reduce(
            count=pw.reducers.count(), last_modified=pw.reducers.max(pw.this.modified)
        )
        inputs = parsed_docs.reduce(paths=pw.reducers.tuple(pw.this.path))

        info_results = info_queries.join_left(stats, id=info_queries.id).select(
            result=format_stats(stats.count, stats.last_modified)
        )

        input_results = input_queries.join_left(inputs, id=input_queries.id).select(
            result=format_inputs(pw.this.paths)  # pw.this.paths#
        )

        # Relevant document search
        retrieval_queries += retrieval_queries.select(
            embedding=embedder(pw.this.query),
        )

        retrieval_results = retrieval_queries + knn_index.get_nearest_items(
            retrieval_queries.embedding,
            k=pw.this.k,
            collapse_rows=True,
            metadata_filter=retrieval_queries.metadata_filter,
            with_distances=True,
        ).select(
            result=pw.this.data,
            dist=pw.this.dist,
        )

        retrieval_results = retrieval_results.select(
            result=pw.apply_with_type(
                lambda x, y: pw.Json(
                    [{**res.value, "dist": dist} for res, dist in zip(x, y)]
                ),
                pw.Json,
                pw.this.result,
                pw.this.dist,
            )
        )

        return GraphResultTables(retrieval_results, info_results, input_results)

    def run_server(
        self,
        host,
        port,
        threaded: bool = False,
        with_cache: bool = True,
        cache_backend: pw.persistence.Backend
        | None = pw.persistence.Backend.filesystem("./Cache"),
    ):
        """
        Builds the document processing pipeline and runs it.

        Args:
            - host: host to bind the HTTP listener
            - port: to bind the HTTP listener
            - threaded: if True, run in a thread. Else block computation
            - with_cache: if True, embedding requests for the same contents are cached
            - cache_backend: the backend to use for caching if it is enabled. The
              default is the disk cache, hosted locally in the folder ``./Cache``. You
              can use ``Backend`` class of the
              [`persistence API`](/developers/api-docs/persistence-api/#pathway.persistence.Backend)
              to override it.

        Returns:
            If threaded, return the Thread object. Else, does not return.
        """

        webserver = pw.io.http.PathwayWebserver(host=host, port=port)
        retrieval_queries, retrieval_response_writer = pw.io.http.rest_connector(
            webserver=webserver,
            route="/query",
            schema=QueryInputSchema,
            autocommit_duration_ms=50,
            delete_completed_queries=True,
        )
        info_queries, info_response_writer = pw.io.http.rest_connector(
            webserver=webserver,
            route="/stats",
            schema=StatsInputSchema,
            autocommit_duration_ms=50,
            delete_completed_queries=True,
        )
        input_queries, inputs_response_writer = pw.io.http.rest_connector(
            webserver=webserver,
            route="/get_inputs",
            schema=StatsInputSchema,
            autocommit_duration_ms=50,
            delete_completed_queries=True,
        )

        graph_tables = self._build_graph(retrieval_queries, info_queries, input_queries)

        retrieval_response_writer(graph_tables.retrieval_results)
        info_response_writer(graph_tables.info_results)
        inputs_response_writer(graph_tables.input_results)

        def run():
            if with_cache:
                if cache_backend is None:
                    raise ValueError(
                        "Cache usage was requested but the backend is unspecified"
                    )
                persistence_config = pw.persistence.Config.simple_config(
                    cache_backend,
                    persistence_mode=pw.PersistenceMode.UDF_CACHING,
                )
            else:
                persistence_config = None

            pw.run(
                monitoring_level=pw.MonitoringLevel.NONE,
                persistence_config=persistence_config,
            )

        if threaded:
            t = threading.Thread(target=run)
            t.start()
            return t
        else:
            run()


class VectorStoreClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def query(self, query, k=3, metadata_filter=None) -> list[dict]:
        """Perform a query to the vector store and fetch results."""

        data = {"query": query, "k": k}
        if metadata_filter is not None:
            data["metadata_filter"] = metadata_filter
        url = f"http://{self.host}:{self.port}/query"
        response = requests.post(
            url,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            timeout=3,
        )
        responses = response.json()
        return sorted(responses, key=lambda x: x["dist"])

    # Make an alias
    __call__ = query

    def get_vectorstore_statistics(self):
        """Fetch basic statistics about the vector store."""
        url = f"http://{self.host}:{self.port}/stats"
        response = requests.post(
            url,
            json={},
            headers={"Content-Type": "application/json"},
        )
        responses = response.json()
        return responses

    def get_input_files(self):
        """Fetch basic statistics about the vector store."""
        url = f"http://{self.host}:{self.port}/get_inputs"
        response = requests.post(
            url,
            json={},
            headers={"Content-Type": "application/json"},
        )
        responses = response.json()
        return responses

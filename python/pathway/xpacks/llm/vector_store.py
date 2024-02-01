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

import jmespath
import numpy as np
import requests

import pathway as pw
import pathway.xpacks.llm.parsers
import pathway.xpacks.llm.splitters
from pathway.stdlib.ml import index
from pathway.stdlib.ml.classifiers import _knn_lsh


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

        self._graph = self._build_graph()

    def _build_graph(self) -> dict:
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

        parsed_docs += parsed_docs.select(
            modified=pw.this.data["metadata"]["modified_at"],
            path=pw.this.data["metadata"]["path"],
        )

        stats = parsed_docs.reduce(
            count=pw.reducers.count(),
            last_modified=pw.reducers.max(pw.this.modified),
            paths=pw.reducers.tuple(pw.this.path),
        )
        return locals()

    class StatisticsQuerySchema(pw.Schema):
        pass

    class QueryResultSchema(pw.Schema):
        result: pw.Json

    @pw.table_transformer
    def statistics_query(
        self, info_queries: pw.Table[StatisticsQuerySchema]
    ) -> pw.Table[QueryResultSchema]:
        stats = self._graph["stats"]

        # VectorStore statistics computation
        @pw.udf
        def format_stats(counts, last_modified) -> pw.Json:
            if counts is not None:
                response = {"file_count": counts, "last_modified": last_modified.value}
            else:
                response = {"file_count": 0, "last_modified": None}
            return pw.Json(response)

        info_results = info_queries.join_left(stats, id=info_queries.id).select(
            result=format_stats(stats.count, stats.last_modified)
        )
        return info_results

    class FilterSchema(pw.Schema):
        metadata_filter: str | None = pw.column_definition(default_value=None)
        filepath_globpattern: str | None = pw.column_definition(default_value=None)

    InputsQuerySchema = FilterSchema

    @staticmethod
    def merge_filters(queries: pw.Table):
        @pw.udf
        def _get_jmespath_filter(
            metadata_filter: str, filepath_globpattern: str
        ) -> str | None:
            ret_parts = []
            if metadata_filter:
                ret_parts.append(f"({metadata_filter})")
            if filepath_globpattern:
                ret_parts.append(f'globmatch(`"{filepath_globpattern}"`, path)')
            if ret_parts:
                return " && ".join(ret_parts)
            return None

        queries = queries.without(
            *VectorStoreServer.FilterSchema.__columns__.keys()
        ) + queries.select(
            metadata_filter=_get_jmespath_filter(
                pw.this.metadata_filter, pw.this.filepath_globpattern
            )
        )
        return queries

    @pw.table_transformer
    def inputs_query(
        self, input_queries: pw.Table[InputsQuerySchema]  # type: ignore
    ) -> pw.Table[QueryResultSchema]:
        docs = self._graph["docs"]
        # TODO: compare this approach to first joining queries to dicuments, then filtering,
        # then grouping to get each response.
        # The "dumb" tuple approach has more work precomputed for an all inputs query
        all_metas = docs.reduce(metadatas=pw.reducers.tuple(pw.this._metadata))

        input_queries = self.merge_filters(input_queries)

        @pw.udf
        def format_inputs(
            metadatas: list[pw.Json] | None, metadata_filter: str | None
        ) -> pw.Json:
            metadatas: list = metadatas if metadatas is not None else []  # type:ignore
            assert metadatas is not None
            if metadata_filter:
                metadatas = [
                    m
                    for m in metadatas
                    if jmespath.search(
                        metadata_filter, m.value, options=_knn_lsh._glob_options
                    )
                ]
            return pw.Json({"input_files": [m.value["path"] for m in metadatas]})  # type: ignore

        input_results = input_queries.join_left(all_metas, id=input_queries.id).select(
            all_metas.metadatas, input_queries.metadata_filter
        )
        input_results = input_results.select(
            result=format_inputs(pw.this.metadatas, pw.this.metadata_filter)
        )
        return input_results

    # TODO: fix
    # class RetrieveQuerySchema(FilterSchema):
    #     query: str
    #     k: int
    # Keep a weird column ordering to match one that stems from the class inheritance above.
    class RetrieveQuerySchema(pw.Schema):
        metadata_filter: str | None = pw.column_definition(default_value=None)
        filepath_globpattern: str | None = pw.column_definition(default_value=None)
        query: str
        k: int

    @pw.table_transformer
    def retrieve_query(
        self, retrieval_queries: pw.Table[RetrieveQuerySchema]
    ) -> pw.Table[QueryResultSchema]:
        embedder = self._graph["embedder"]
        knn_index = self._graph["knn_index"]

        # Relevant document search
        retrieval_queries = self.merge_filters(retrieval_queries)
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

        return retrieval_results

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

        webserver = pw.io.http.PathwayWebserver(host=host, port=port, with_cors=True)

        # TODO(move into webserver??)
        def serve(route, schema, handler):
            queries, writer = pw.io.http.rest_connector(
                webserver=webserver,
                route=route,
                methods=("GET", "POST"),
                schema=schema,
                autocommit_duration_ms=50,
                delete_completed_queries=True,
            )
            writer(handler(queries))

        serve("/v1/retrieve", self.RetrieveQuerySchema, self.retrieve_query)
        serve("/v1/statistics", self.StatisticsQuerySchema, self.statistics_query)
        serve("/v1/inputs", self.InputsQuerySchema, self.inputs_query)

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
        url = f"http://{self.host}:{self.port}/v1/retrieve"
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
        url = f"http://{self.host}:{self.port}/v1/statistics"
        response = requests.post(
            url,
            json={},
            headers={"Content-Type": "application/json"},
        )
        responses = response.json()
        return responses

    def get_input_files(self):
        """Fetch basic statistics about the vector store."""
        url = f"http://{self.host}:{self.port}/v1/inputs"
        response = requests.post(
            url,
            json={},
            headers={"Content-Type": "application/json"},
        )
        responses = response.json()
        return responses

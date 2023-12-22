"""
Pathway vector search server and client.

The server reads source documents and build a vector index over them, then starts serving
HTTP requests.

The client queries the server and returns matching documents.
"""


import json
import threading
from typing import Callable, Optional

import numpy as np
import requests

import pathway as pw
import pathway.xpacks.llm.parser
import pathway.xpacks.llm.splitter
from pathway.stdlib.ml import index


class QueryInputSchema(pw.Schema):
    query: str
    k: int
    metadata_filter: str | None = pw.column_definition(default_value=None)
    stats: bool


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
        parser: Optional[Callable[[bytes], list[tuple[str, dict]]]] = None,
        splitter: Optional[Callable[[str], list[tuple[str, dict]]]] = None,
    ):
        self.docs = docs
        self.parser: Callable[[bytes], list[tuple[str, dict]]] = (
            parser
            if parser is not None
            else pathway.xpacks.llm.parser.ParseUtf8()  # type:ignore
        )
        self.splitter = (
            splitter
            if splitter is not None
            else pathway.xpacks.llm.splitter.null_splitter
        )
        self.embedder = embedder

        # detect the dimensionality of the embeddings
        self.embedding_dimension = len(embedder("."))

    def _build_graph(
        self,
        host,
        port,
    ):
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

        @pw.udf_async
        def embedder(txt):
            return np.asarray(self.embedder(txt))

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

        post_query, response_writer = pw.io.http.rest_connector(
            host=host,
            port=port,
            route="/",
            schema=QueryInputSchema,
            autocommit_duration_ms=50,
            delete_completed_queries=True,
        )

        stats_query, query = post_query.split(pw.this.stats)

        # VectorStore statistics computation
        @pw.udf
        def format_stats(counts, last_modified):
            if counts is not None:
                response = {"file_count": counts, "last_modified": last_modified.value}
            else:
                response = {"file_count": 0, "last_modified": None}
            return json.dumps(response)

        parsed_docs += parsed_docs.select(
            modified=pw.this.data["metadata"]["modified_at"]
        )
        stats = parsed_docs.reduce(
            count=pw.reducers.count(), last_modified=pw.reducers.max(pw.this.modified)
        )

        stats_results = stats_query.join_left(stats, id=stats_query.id).select(
            result=format_stats(stats.count, stats.last_modified)
        )

        # Relevant document search
        query += query.select(
            embedding=embedder(pw.this.query),
        )

        query_results = query + knn_index.get_nearest_items(
            query.embedding,
            k=pw.this.k,
            collapse_rows=True,
            metadata_filter=query.metadata_filter,
            with_distances=True,
        ).select(
            result=pw.this.data,
            dist=pw.this.dist,
        )

        query_results = query_results.select(
            result=pw.apply(
                lambda x, y: [{**res.value, "dist": dist} for res, dist in zip(x, y)],
                pw.this.result,
                pw.this.dist,
            )
        )

        results = query_results.concat(stats_results)
        response_writer(results)

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
        self._build_graph(host=host, port=port)

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

    def __call__(self, query, k=3, metadata_filter=None) -> list[dict]:
        """Perform a query to the vector store and fetch results."""

        data = {"query": query, "k": k, "stats": False}
        if metadata_filter is not None:
            data["metadata_filter"] = metadata_filter
        url = f"http://{self.host}:{self.port}"
        response = requests.post(
            url,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            timeout=3,
        )
        responses = response.json()
        return responses

    def get_vectorstore_statistics(self):
        """Fetch basic statistics about the vector store."""
        data = {"query": "", "k": 0, "stats": True}
        url = f"http://{self.host}:{self.port}"
        response = requests.post(
            url, data=json.dumps(data), headers={"Content-Type": "application/json"}
        )
        responses = response.json()
        return responses

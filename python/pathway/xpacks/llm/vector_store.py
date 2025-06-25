# Copyright © 2024 Pathway

"""
Pathway vector search server and client.

The server reads source documents and build a vector index over them, then starts serving
HTTP requests.

The client queries the server and returns matching documents.
"""

import threading
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, cast
from warnings import warn

import jmespath
import numpy as np

import pathway as pw
from pathway.stdlib.indexing import DefaultKnnFactory
from pathway.stdlib.ml.classifiers import _knn_lsh
from pathway.xpacks.llm.document_store import DocumentStore, DocumentStoreClient

if TYPE_CHECKING:
    import langchain_core.documents
    import langchain_core.embeddings
    import llama_index.core.schema


class VectorStoreServer(DocumentStore):
    """
    Builds a document indexing pipeline and starts an HTTP REST server for nearest neighbors queries.

    Args:
        docs: pathway tables typically coming out of connectors which contain source documents.
        embedder: callable that embeds a single document
        parser: callable that parses file contents into a list of documents
        splitter: callable that splits long documents
        doc_post_processors: optional list of callables that modify parsed files and metadata.
            any callable takes two arguments (text: str, metadata: dict) and returns them as a tuple.
    """

    def __init__(
        self,
        *docs: pw.Table,
        embedder: pw.UDF,
        parser: Callable[[bytes], list[tuple[str, dict]]] | pw.UDF | None = None,
        splitter: Callable[[str], list[tuple[str, dict]]] | pw.UDF | None = None,
        doc_post_processors: (
            list[Callable[[str, dict], tuple[str, dict]]] | None
        ) = None,
    ):
        super().__init__(
            docs,
            retriever_factory=DefaultKnnFactory(embedder=embedder),
            parser=parser,
            splitter=splitter,
            doc_post_processors=doc_post_processors,
        )

        self.embedder = embedder

    def run_server(
        self,
        host,
        port,
        threaded: bool = False,
        with_cache: bool = True,
        cache_backend: (
            pw.persistence.Backend | None
        ) = pw.persistence.Backend.filesystem("./Cache"),
        **kwargs,
    ):
        """
        Builds the document processing pipeline and runs it.

        Args:
            host: host to bind the HTTP listener
            port: to bind the HTTP listener
            threaded: if True, run in a thread. Else block computation
            with_cache: if True, embedding requests for the same contents are cached
            cache_backend: the backend to use for caching if it is enabled. The
              default is the disk cache, hosted locally in the folder ``./Cache``. You
              can use ``Backend`` class of the
              [`persistence API`](/developers/api-docs/persistence-api/#pathway.persistence.Backend)
              to override it.
            kwargs: optional parameters to be passed to :py:func:`~pathway.run`.

        Returns:
            If threaded, return the Thread object. Else, does not return.
        """

        webserver = pw.io.http.PathwayWebserver(host=host, port=port, with_cors=True)

        # TODO(move into webserver??)
        def serve(route, schema, handler, documentation):
            queries, writer = pw.io.http.rest_connector(
                webserver=webserver,
                route=route,
                methods=("GET", "POST"),
                schema=schema,
                autocommit_duration_ms=50,
                delete_completed_queries=False,
                documentation=documentation,
            )
            writer(handler(queries))

        serve(
            "/v1/retrieve",
            self.RetrieveQuerySchema,
            self.retrieve_query,
            pw.io.http.EndpointDocumentation(
                summary="Do a similarity search for your query",
                description="Request the given number of documents from the "
                "realtime-maintained index.",
                method_types=("GET",),
            ),
        )
        serve(
            "/v1/statistics",
            self.StatisticsQuerySchema,
            self.statistics_query,
            pw.io.http.EndpointDocumentation(
                summary="Get current indexer stats",
                description="Request for the basic stats of the indexer process. "
                "It returns the number of documents that are currently present in the "
                "indexer and the time the last of them was added.",
                method_types=("GET",),
            ),
        )
        serve(
            "/v1/inputs",
            self.InputsQuerySchema,
            self.inputs_query,
            pw.io.http.EndpointDocumentation(
                summary="Get indexed documents list",
                description="Request for the list of documents present in the indexer. "
                "It returns the list of metadata objects.",
                method_types=("GET",),
            ),
        )

        def run():
            if with_cache:
                if cache_backend is None:
                    raise ValueError(
                        "Cache usage was requested but the backend is unspecified"
                    )
                persistence_config = pw.persistence.Config(
                    cache_backend,
                    persistence_mode=pw.PersistenceMode.UDF_CACHING,
                )
            else:
                persistence_config = None

            pw.run(
                monitoring_level=pw.MonitoringLevel.NONE,
                persistence_config=persistence_config,
                **kwargs,
            )

        if threaded:
            t = threading.Thread(target=run, name="VectorStoreServer")
            t.start()
            return t
        else:
            run()

    # def __repr__(self):
    #     # TODO: Decide how to handle
    #     return f"VectorStoreServer({str(self._graph)})"

    @classmethod
    def from_langchain_components(  # type: ignore
        cls,
        *docs,
        embedder: "langchain_core.embeddings.Embeddings",
        parser: Callable[[bytes], list[tuple[str, dict]]] | None = None,
        splitter: "langchain_core.documents.BaseDocumentTransformer | None" = None,
        **kwargs,
    ):
        """
        Initializes VectorStoreServer by using LangChain components.
        Args:
            docs: pathway tables typically coming out of connectors which contain source documents
            embedder: Langchain component for embedding documents
            parser: callable that parses file contents into a list of documents
            splitter: Langchaing component for splitting documents into parts
        """
        try:
            from langchain_core.documents import Document
        except ImportError:
            raise ImportError(
                "Please install langchain_core: `pip install langchain_core`"
            )

        generic_splitter = None
        if splitter:
            generic_splitter = lambda x: [  # noqa
                (doc.page_content, doc.metadata)
                for doc in splitter.transform_documents([Document(page_content=x)])
            ]

        @pw.udf
        async def generic_embedded(x: str) -> np.ndarray:
            res = await embedder.aembed_documents([x])
            return np.array(res[0])

        return cls(
            *docs,
            embedder=generic_embedded,
            parser=parser,
            splitter=generic_splitter,
            **kwargs,
        )

    @classmethod
    def from_llamaindex_components(  # type: ignore
        cls,
        *docs,
        transformations: list["llama_index.core.schema.TransformComponent"],
        parser: Callable[[bytes], list[tuple[str, dict]]] | None = None,
        **kwargs,
    ):
        """
        Initializes VectorStoreServer by using LlamaIndex TransformComponents.
        Args:
            docs: pathway tables typically coming out of connectors which contain source documents
            transformations: list of LlamaIndex components. The last component in this list
                is required to inherit from LlamaIndex `BaseEmbedding`
            parser: callable that parses file contents into a list of documents
        """
        try:
            from llama_index.core.base.embeddings.base import BaseEmbedding
            from llama_index.core.ingestion.pipeline import run_transformations
            from llama_index.core.schema import BaseNode, MetadataMode, TextNode
        except ImportError:
            raise ImportError(
                "Please install llama-index-core: `pip install llama-index-core`"
            )
        try:
            from llama_index.legacy.embeddings.base import (
                BaseEmbedding as LegacyBaseEmbedding,
            )

            legacy_llama_index_not_imported = True
        except ImportError:
            legacy_llama_index_not_imported = False

        def node_transformer(x: str) -> list[BaseNode]:
            return [TextNode(text=x)]

        def node_to_pathway(x: Sequence[BaseNode]) -> list[tuple[str, dict]]:
            return [
                (node.get_content(metadata_mode=MetadataMode.NONE), node.extra_info)
                for node in x
            ]

        if transformations is None or not transformations:
            raise ValueError("Transformations list cannot be None or empty.")

        if not isinstance(transformations[-1], BaseEmbedding) and (
            legacy_llama_index_not_imported
            or not isinstance(transformations[-1], LegacyBaseEmbedding)
        ):
            raise ValueError(
                f"Last step of transformations should be an instance of {BaseEmbedding.__name__}, "
                f"found {type(transformations[-1])}."
            )

        embedder = cast(BaseEmbedding, transformations.pop())

        @pw.udf
        async def embedding_callable(x: str) -> np.ndarray:
            embedding = await embedder.aget_text_embedding(x)
            return np.array(embedding)

        def generic_transformer(x: str) -> list[tuple[str, dict]]:
            starting_node = node_transformer(x)
            final_node = run_transformations(starting_node, transformations)
            return node_to_pathway(final_node)

        return VectorStoreServer(
            *docs,
            embedder=embedding_callable,
            parser=parser,
            splitter=generic_transformer,
            **kwargs,
        )


class SlidesVectorStoreServer(VectorStoreServer):
    """
    Accompanying vector index server for the ``slide-search`` demo.
    Builds a document indexing pipeline and starts an HTTP REST server.

    Modifies the ``VectorStoreServer``'s ``pw_list_document`` endpoint to return set of
    metadata after the parsing and document post processing stages.
    """

    excluded_response_metadata = ["b64_image"]

    @pw.table_transformer
    def inputs_query(
        self,
        input_queries: pw.Table[VectorStoreServer.InputsQuerySchema],
    ) -> pw.Table:
        docs = self.parsed_docs

        all_metas = docs.reduce(metadatas=pw.reducers.tuple(pw.this.data["metadata"]))

        input_queries = self.merge_filters(input_queries)

        @pw.udf
        def format_inputs(
            metadatas: list[pw.Json] | None,
            metadata_filter: str | None,
        ) -> list[pw.Json]:
            metadatas = metadatas if metadatas is not None else []
            assert metadatas is not None
            if metadata_filter:
                metadatas = [
                    m
                    for m in metadatas
                    if jmespath.search(
                        metadata_filter, m.value, options=_knn_lsh._glob_options
                    )
                ]

            metadata_list: list[dict] = [m.as_dict() for m in metadatas]

            for metadata in metadata_list:
                for metadata_key in self.excluded_response_metadata:
                    metadata.pop(metadata_key, None)

            return [pw.Json(m) for m in metadata_list]

        input_results = input_queries.join_left(all_metas, id=input_queries.id).select(
            all_metas.metadatas,
            input_queries.metadata_filter,
        )
        input_results = input_results.select(
            result=format_inputs(pw.this.metadatas, pw.this.metadata_filter)
        )
        return input_results

    @pw.table_transformer
    def parsed_documents_query(
        self,
        parse_docs_queries: pw.Table[VectorStoreServer.InputsQuerySchema],
    ) -> pw.Table:
        return self.inputs_query(parse_docs_queries)


class VectorStoreClient(DocumentStoreClient):
    """
    A client you can use to query VectorStoreServer.

    Please provide either the ``"url"``, or ``"host"`` and ``"port"``.

    Args:
        host: host on which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer>`_ listens
        port: port on which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer>`_ listens
        url: url at which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer>`_ listens
        timeout: timeout for the post requests in seconds
    """  # noqa

    def __init__(self, *args, **kwargs):
        warn(
            "`VectorStoreClient` is being deprecated, please use `DocumentStoreClient` instead. "
            "You can import with the following: `from pathway.xpacks.llm.document_store import DocumentStoreClient`",
            DeprecationWarning,
        )
        super().__init__(*args, **kwargs)

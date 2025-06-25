# Copyright © 2024 Pathway

"""
Pathway Document Store for processing and indexing documents.

The document store reads source documents and build a vector index over them, and exposes
multiple methods for querying.
"""
import json
import warnings
from collections.abc import Callable, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Iterable

import jmespath
import requests

import pathway as pw
import pathway.xpacks.llm.parsers
import pathway.xpacks.llm.splitters
from pathway.stdlib.indexing.data_index import _SCORE, DataIndex
from pathway.stdlib.indexing.retrievers import AbstractRetrieverFactory
from pathway.stdlib.ml.classifiers import _knn_lsh
from pathway.xpacks.llm._utils import _wrap_doc_post_processor, _wrap_udf
from pathway.xpacks.llm.utils import combine_metadata

if TYPE_CHECKING:
    import langchain_core.documents
    import langchain_core.embeddings
    import llama_index.core.schema


@pw.udf
def _get_jmespath_filter(metadata_filter: str, filepath_globpattern: str) -> str | None:
    ret_parts = []
    if metadata_filter:
        metadata_filter = (
            metadata_filter.replace("'", r"\'").replace("`", "'").replace('"', "")
        )
        ret_parts.append(f"({metadata_filter})")
    if filepath_globpattern:
        ret_parts.append(f"globmatch('{filepath_globpattern}', path)")
    if ret_parts:
        return " && ".join(ret_parts)
    return None


class IndexingStatus(str, Enum):
    INDEXED = "INDEXED"
    INGESTED = "INGESTED"


class DocumentStore:
    """
    Builds a document indexing pipeline for processing documents and querying closest documents
    to a query according to a specified index.

    Args:
        docs: pathway tables typically coming out of connectors which contain source documents.
            The table needs to contain a ``data`` column of type bytes - usually by setting
            format of the connector to be ``"raw""``. Optionally, it can contain
            a ``_metadata`` column containing a dictionary with metadata which is then
            used for filters. Some connectors offer ``with_metadata`` argument for returning
            ``_metadata`` column.
        retriever_factory: factory for building an index, which will be provided
            texts by the ``DocumentStore``.
        parser: callable that parses file contents into a list of documents.
        splitter: callable that splits long documents.
        doc_post_processors: optional list of callables that modify parsed files and metadata.
            Each doc_post_processor is a Callable that takes two arguments
            (text: str, metadata: dict) and returns them as a tuple.
    """

    def __init__(
        self,
        docs: pw.Table | Iterable[pw.Table],
        retriever_factory: AbstractRetrieverFactory,
        parser: Callable[[bytes], list[tuple[str, dict]]] | pw.UDF | None = None,
        splitter: Callable[[str], list[tuple[str, dict]]] | pw.UDF | None = None,
        doc_post_processors: (
            list[Callable[[str, dict], tuple[str, dict]]] | None
        ) = None,
    ):
        self.docs = docs
        self.retriever_factory: AbstractRetrieverFactory = retriever_factory
        self.parser: pw.UDF = (
            _wrap_udf(parser)
            if parser is not None
            else pathway.xpacks.llm.parsers.Utf8Parser()
        )
        self.doc_post_processors: list[pw.UDF] = (
            [_wrap_doc_post_processor(p) for p in doc_post_processors]
            if doc_post_processors
            else []
        )
        self.splitter: pw.UDF = (
            _wrap_udf(splitter)
            if splitter is not None
            else pathway.xpacks.llm.splitters.NullSplitter()
        )

        self.build_pipeline()

    @classmethod
    def from_langchain_components(
        cls,
        docs: pw.Table | Iterable[pw.Table],
        retriever_factory: AbstractRetrieverFactory,
        parser: Callable[[bytes], list[tuple[str, dict]]] | None = None,
        splitter: "langchain_core.documents.BaseDocumentTransformer | None" = None,
        **kwargs,
    ):
        """
        Initializes DocumentStore by using LangChain components.

        Args:
            docs: pathway tables typically coming out of connectors which contain source documents
            retriever_factory: factory for building an index, which will be provided
                texts by the ``DocumentStore``.
            parser: callable that parses file contents into a list of documents
            splitter: Langchain component for splitting documents into parts
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

        return cls(
            docs,
            retriever_factory=retriever_factory,
            parser=parser,
            splitter=generic_splitter,
            **kwargs,
        )

    @classmethod
    def from_llamaindex_components(
        cls,
        docs: pw.Table | Iterable[pw.Table],
        retriever_factory: AbstractRetrieverFactory,
        transformations: list["llama_index.core.schema.TransformComponent"],
        parser: Callable[[bytes], list[tuple[str, dict]]] | None = None,
        **kwargs,
    ):
        """
        Initializes DocumentStore by using LlamaIndex TransformComponents.

        Args:
            docs: pathway tables typically coming out of connectors which contain source documents
            retriever_factory: factory for building an index, which will be provided
                texts by the ``DocumentStore``.
            transformations: list of LlamaIndex components.
            parser: callable that parses file contents into a list of documents
        """
        try:
            from llama_index.core.ingestion.pipeline import run_transformations
            from llama_index.core.schema import BaseNode, MetadataMode, TextNode
        except ImportError:
            raise ImportError(
                "Please install llama-index-core: `pip install llama-index-core`"
            )

        def node_transformer(x: str) -> list[BaseNode]:
            return [TextNode(text=x)]

        def node_to_pathway(x: Sequence[BaseNode]) -> list[tuple[str, dict]]:
            return [
                (node.get_content(metadata_mode=MetadataMode.NONE), node.extra_info)
                for node in x
            ]

        def generic_transformer(x: str) -> list[tuple[str, dict]]:
            starting_node = node_transformer(x)
            final_node = run_transformations(starting_node, transformations)
            return node_to_pathway(final_node)

        return cls(
            docs,
            retriever_factory=retriever_factory,
            parser=parser,
            splitter=generic_transformer,
            **kwargs,
        )

    class _RawDocumentSchema(pw.Schema):
        # schema for the raw, unparsed documents
        text: bytes
        metadata: pw.Json

    class _DocumentWithMetaSchema(pw.Schema):
        # schema for document processed by UDFs that outputs both text and metadata into a single column
        text: tuple[str, dict]
        metadata: pw.Json

    class _DocumentSchema(pw.Schema):
        # cleaned schema for documents where text and metadata are in separate columns
        text: str
        metadata: pw.Json

    class StatisticsQuerySchema(pw.Schema):
        pass

    class FilterSchema(pw.Schema):
        metadata_filter: str | None = pw.column_definition(
            default_value=None, description="Metadata filter in JMESPath format"
        )
        filepath_globpattern: str | None = pw.column_definition(
            default_value=None, description="An optional Glob pattern for the file path"
        )

    class InputsQuerySchema(pw.Schema):
        metadata_filter: str | None = pw.column_definition(
            default_value=None, description="Metadata filter in JMESPath format"
        )
        filepath_globpattern: str | None = pw.column_definition(
            default_value=None, description="An optional Glob pattern for the file path"
        )
        return_status: bool = pw.column_definition(
            default_value=False,
            description="Flag whether _indexing_status should be returned for each file",
        )

    class InputsResultSchema(pw.Schema):
        result: list[pw.Json]

    class RetrieveQuerySchema(pw.Schema):
        query: str = pw.column_definition(
            description="Your query for the similarity search",
            example="Pathway data processing framework",
        )
        k: int = pw.column_definition(
            description="The number of documents to provide", example=2
        )
        metadata_filter: str | None = pw.column_definition(
            default_value=None, description="Metadata filter in JMESPath format"
        )
        filepath_globpattern: str | None = pw.column_definition(
            default_value=None, description="An optional Glob pattern for the file path"
        )

    class QueryResultSchema(pw.Schema):
        result: pw.Json

    def _clean_tables(self, docs: pw.Table | Iterable[pw.Table]) -> list[pw.Table]:
        if isinstance(docs, pw.Table):
            docs = [docs]

        def _clean_table(doc: pw.Table) -> pw.Table:
            if "_metadata" not in doc.column_names():
                warnings.warn(
                    f"`_metadata` column is not present in Table {doc}. Filtering will not work for this Table"
                )
                doc = doc.with_columns(_metadata=dict())

            return doc.select(pw.this.data, pw.this._metadata)

        return [_clean_table(doc) for doc in docs]

    @pw.table_transformer
    def apply_processor(
        self, table: pw.Table, processor: pw.UDF
    ) -> pw.Table[_DocumentSchema]:

        processed_docs: pw.Table[DocumentStore._DocumentWithMetaSchema] = (
            table.select(
                text=processor(pw.this.text),
                metadata=pw.this.metadata,
                # some processors might split document into multiple parts so we flatten the results
                # metadata will be propagated to all new rows
            )
            .await_futures()
            .flatten(pw.this.text)
        )
        # combine_metadata will transform our columns as follows:
        # `text` column: tuple[str, new_meta_dict] -> str
        # `metadata` column: old_meta_dict -> old_meta_dict | new_meta_dict
        return combine_metadata(processed_docs)

    @pw.table_transformer
    def apply_doc_post_processor(
        self, table: pw.Table, processor: pw.UDF
    ) -> pw.Table[_DocumentSchema]:

        processed_docs: pw.Table[DocumentStore._DocumentSchema] = table.select(
            text=processor(pw.this.text, pw.this.metadata)
            # some processors might split document into multiple parts so we flatten the results
            # metadata will be propagated to all new rows
        ).select(text=pw.this.text[0], metadata=pw.this.text[1])
        # combine_metadata will transform our columns as follows:
        # `text` column: tuple[str, new_meta_dict] -> str
        # `metadata` column: new_meta_dict
        return processed_docs

    def build_pipeline(self):

        cleaned_tables = self._clean_tables(self.docs)
        if len(cleaned_tables) == 0:
            raise ValueError(
                """Please provide at least one data source, e.g. read files from disk:
pw.io.fs.read('./sample_docs', format='binary', mode='static', with_metadata=True)
"""
            )

        docs = pw.Table.concat_reindex(*cleaned_tables)

        @pw.udf
        def add_file_id(metadata: pw.Json, id) -> dict:
            metadata_dict = metadata.as_dict()
            id = str(id)
            metadata_dict["_file_id"] = id
            return metadata_dict

        # rename columns to be consistent with the rest of the pipeline
        self.input_docs: pw.Table[DocumentStore._RawDocumentSchema] = docs.select(
            text=pw.this.data,
            metadata=add_file_id(pw.this._metadata, pw.this.id),
            # path=pw.this.metadata["path"].as_str(),
        )

        # PARSING
        self.parsed_docs: pw.Table[DocumentStore._DocumentSchema] = (
            self.apply_processor(self.input_docs, self.parser)
        )

        # POST PROCESSING
        self.post_processed_docs = self.parsed_docs
        for post_processor in self.doc_post_processors:
            self.post_processed_docs = self.apply_doc_post_processor(
                self.post_processed_docs, post_processor
            )

        # CHUNKING
        self.chunked_docs: pw.Table[DocumentStore._DocumentSchema] = (
            self.apply_processor(self.post_processed_docs, self.splitter)
        )

        # INDEXING
        self._retriever = self.retriever_factory.build_index(
            self.chunked_docs.text,
            self.chunked_docs,
            metadata_column=self.chunked_docs.metadata,
        )

        progress_table = self.input_docs.select(
            file_id=pw.this.metadata["_file_id"].as_str(),
            metadata=pw.this.metadata,
        )
        chunked_stats = (
            self.chunked_docs.with_columns(
                file_id=pw.this.metadata["_file_id"].as_str()
            )
            .groupby(pw.this.file_id)
            .reduce(
                file_id=pw.this.file_id,
                chunks=pw.reducers.count(),
            )
        )
        self.progress_table = (
            progress_table.join_left(
                chunked_stats,
                progress_table.file_id == chunked_stats.file_id,
            )
            .select(
                *pw.left,
                chunks=pw.right.chunks,
            )
            .with_columns(is_parsed=pw.this.chunks.is_not_none())
        )

        parsed_docs_with_metadata = self.parsed_docs.with_columns(
            modified=pw.this.metadata["modified_at"].as_int(),
            indexed=pw.this.metadata["seen_at"].as_int(),
            path=pw.this.metadata["path"].as_str(),
        )

        self.stats = parsed_docs_with_metadata.reduce(
            count=pw.reducers.count(),
            last_modified=pw.reducers.max(pw.this.modified),
            last_indexed=pw.reducers.max(pw.this.indexed),
            paths=pw.reducers.tuple(pw.this.path),
        )

    @pw.table_transformer
    def statistics_query(
        self, info_queries: pw.Table[StatisticsQuerySchema]
    ) -> pw.Table[QueryResultSchema]:
        """
        Query ``DocumentStore`` for statistics about indexed documents. It returns the number
        of indexed texts, time of last modification, and time of last indexing of input document.
        """

        # DocumentStore statistics computation
        @pw.udf
        def format_stats(counts, last_modified, last_indexed) -> pw.Json:
            if counts is not None:
                response = {
                    "file_count": counts,
                    "last_modified": last_modified,
                    "last_indexed": last_indexed,
                }
            else:
                response = {
                    "file_count": 0,
                    "last_modified": None,
                    "last_indexed": None,
                }
            return pw.Json(response)

        info_results = info_queries.join_left(self.stats, id=info_queries.id).select(
            result=format_stats(
                pw.right.count, pw.right.last_modified, pw.right.last_indexed
            )
        )
        return info_results

    @staticmethod
    def merge_filters(queries: pw.Table):
        queries = queries.without(
            *DocumentStore.FilterSchema.__columns__.keys()
        ) + queries.select(
            metadata_filter=_get_jmespath_filter(
                pw.this.metadata_filter, pw.this.filepath_globpattern
            )
        )
        return queries

    @pw.table_transformer
    def inputs_query(
        self, input_queries: pw.Table[InputsQuerySchema]
    ) -> pw.Table[InputsResultSchema]:
        """
        Query ``DocumentStore`` for the list of input documents.
        """
        # TODO: compare this approach to first joining queries to dicuments, then filtering,
        # then grouping to get each response.
        # The "dumb" tuple approach has more work precomputed for an all inputs query
        all_metas = self.progress_table.reduce(
            metadatas=pw.reducers.tuple(pw.this.metadata),
            is_parsed=pw.reducers.tuple(pw.this.is_parsed),
        )

        input_queries = self.merge_filters(input_queries)

        @pw.udf
        def format_inputs(
            metadatas: list[pw.Json] | None,
            metadata_filter: str | None,
            return_status: bool,
            is_parsed: list[bool] | None,
        ) -> list[pw.Json]:
            metadatas = metadatas if metadatas is not None else []
            is_parsed = is_parsed if is_parsed is not None else []
            assert metadatas is not None
            assert is_parsed is not None

            def remove_id(m):
                metadata_dict = m.as_dict()
                del metadata_dict["_file_id"]
                return pw.Json(metadata_dict)

            metadatas = [remove_id(m) for m in metadatas]
            if metadata_filter:
                metadatas = [
                    m
                    for m in metadatas
                    if jmespath.search(
                        metadata_filter, m.as_dict(), options=_knn_lsh._glob_options
                    )
                ]

            if return_status:
                metadatas = [
                    pw.Json(
                        {
                            "_indexing_status": (
                                IndexingStatus.INDEXED
                                if status
                                else IndexingStatus.INGESTED
                            ),
                            **m.as_dict(),
                        }
                    )
                    for (m, status) in zip(metadatas, is_parsed)
                ]

            return metadatas

        input_results = input_queries.join_left(all_metas, id=input_queries.id).select(
            all_metas.metadatas,
            input_queries.metadata_filter,
            input_queries.return_status,
            all_metas.is_parsed,
        )
        input_results = input_results.select(
            result=format_inputs(
                pw.this.metadatas,
                pw.this.metadata_filter,
                pw.this.return_status,
                pw.this.is_parsed,
            )
        )
        return input_results

    @pw.table_transformer
    def retrieve_query(
        self, retrieval_queries: pw.Table[RetrieveQuerySchema]
    ) -> pw.Table[QueryResultSchema]:
        """
        Query ``DocumentStore`` for the list of closest texts to a given ``query``.
        """

        # Relevant document search
        retrieval_queries = self.merge_filters(retrieval_queries)

        retrieval_results = retrieval_queries + self._retriever.query_as_of_now(
            retrieval_queries.query,
            number_of_matches=retrieval_queries.k,
            metadata_filter=retrieval_queries.metadata_filter,
        ).select(
            result=pw.coalesce(pw.right.text, ()),  # replace None results with []
            metadata=pw.coalesce(pw.right.metadata, ()),
            score=pw.coalesce(pw.right[_SCORE], ()),
        )

        retrieval_results = retrieval_results.select(
            result=pw.apply_with_type(
                lambda x, y, z: pw.Json(
                    sorted(
                        [
                            {"text": res, "metadata": metadata, "dist": -score}
                            for res, metadata, score in zip(x, y, z)
                        ],
                        key=lambda x: x["dist"],  # type: ignore
                    )
                ),
                pw.Json,
                pw.this.result,
                pw.this.metadata,
                pw.this.score,
            )
        )

        return retrieval_results

    @property
    def index(self) -> DataIndex:
        return self._retriever


class SlidesDocumentStore(DocumentStore):
    """
    Document store for the ``slide-search`` application.
    Builds a document indexing pipeline and starts an HTTP REST server.

    Adds to the ``DocumentStore`` a new method ``parsed_documents`` a set of
    documents metadata after the parsing and document post processing stages.
    """

    excluded_response_metadata = ["b64_image"]

    @pw.table_transformer
    def parsed_documents_query(
        self,
        parse_docs_queries: pw.Table[DocumentStore.InputsQuerySchema],
    ) -> pw.Table:
        """
        Query the SlidesDocumentStore for the list of documents with the associated
        metadata after the parsing stage.
        """
        docs = self.parsed_docs

        all_metas = docs.reduce(metadatas=pw.reducers.tuple(pw.this.metadata))

        parse_docs_queries = self.merge_filters(parse_docs_queries)

        @pw.udf
        def format_inputs(
            metadatas: list[pw.Json] | None,
            metadata_filter: str | None,
        ) -> list[pw.Json]:
            metadatas = metadatas if metadatas is not None else []
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

        input_results = parse_docs_queries.join_left(
            all_metas, id=parse_docs_queries.id
        ).select(
            all_metas.metadatas,
            parse_docs_queries.metadata_filter,
        )
        input_results = input_results.select(
            result=format_inputs(pw.this.metadatas, pw.this.metadata_filter)
        )
        return input_results


class DocumentStoreClient:
    """
    A client you can use to query DocumentStore.

    Please provide either the ``"url"``, or ``"host"`` and ``"port"``.

    Args:
        host: host on which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/document_store#pathway.xpacks.llm.document_store.DocumentStore>`_ listens
        port: port on which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/document_store#pathway.xpacks.llm.document_store.DocumentStore>`_ listens
        url: url at which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/document_store#pathway.xpacks.llm.document_store.DocumentStore>`_ listens
        timeout: timeout for the post requests in seconds
    """  # noqa

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        url: str | None = None,
        timeout: int | None = 15,
        additional_headers: dict | None = None,
    ):
        err = "Either (`host` and `port`) or `url` must be provided, but not both."
        if url is not None:
            if host or port:
                raise ValueError(err)
            self.url = url
        else:
            if host is None:
                raise ValueError(err)
            port = port or 80
            self.url = f"http://{host}:{port}"

        self.timeout = timeout
        self.additional_headers = additional_headers or {}

    def query(
        self,
        query: str,
        k: int = 3,
        metadata_filter: str | None = None,
        filepath_globpattern: str | None = None,
    ) -> list[dict]:
        """
        Perform a query to the vector store and fetch results.

        Args:
            query:
            k: number of documents to be returned
            metadata_filter: optional string representing the metadata filtering query
                in the JMESPath format. The search will happen only for documents
                satisfying this filtering.
            filepath_globpattern: optional glob pattern specifying which documents
                will be searched for this query.
        """

        data = {"query": query, "k": k}
        if metadata_filter is not None:
            data["metadata_filter"] = metadata_filter
        if filepath_globpattern is not None:
            data["filepath_globpattern"] = filepath_globpattern
        url = self.url + "/v1/retrieve"
        response = requests.post(
            url,
            data=json.dumps(data),
            headers=self._get_request_headers(),
            timeout=self.timeout,
        )

        responses = response.json()
        return sorted(responses, key=lambda x: x["dist"])

    # Make an alias
    __call__ = query

    def get_vectorstore_statistics(self):
        """Fetch basic statistics about the vector store."""

        url = self.url + "/v1/statistics"
        response = requests.post(
            url,
            json={},
            headers=self._get_request_headers(),
            timeout=self.timeout,
        )
        responses = response.json()
        return responses

    def get_input_files(
        self,
        metadata_filter: str | None = None,
        filepath_globpattern: str | None = None,
        return_status: bool = False,
    ):
        """
        Fetch information on documents in the the vector store.

        Args:
            metadata_filter: optional string representing the metadata filtering query
                in the JMESPath format. The search will happen only for documents
                satisfying this filtering.
            filepath_globpattern: optional glob pattern specifying which documents
                will be searched for this query.
            return_status: flag telling whether `_indexing_status` should be returned
                for each document
        """
        url = self.url + "/v1/inputs"
        response = requests.post(
            url,
            json={
                "metadata_filter": metadata_filter,
                "filepath_globpattern": filepath_globpattern,
                "return_status": return_status,
            },
            headers=self._get_request_headers(),
            timeout=self.timeout,
        )
        responses = response.json()
        return responses

    def _get_request_headers(self):
        request_headers = {"Content-Type": "application/json"}
        request_headers.update(self.additional_headers)
        return request_headers

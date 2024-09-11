# Copyright © 2024 Pathway

"""
Pathway Document Store for processing and indexing documents.

The document store reads source documents and build a vector index over them, and exposes
multiple methods for querying.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Iterable

import jmespath

import pathway as pw
import pathway.xpacks.llm.parsers
import pathway.xpacks.llm.splitters
from pathway.stdlib.indexing.data_index import _SCORE, DataIndex
from pathway.stdlib.indexing.retrievers import AbstractRetrieverFactory
from pathway.stdlib.ml.classifiers import _knn_lsh

from ._utils import _unwrap_udf

if TYPE_CHECKING:
    import langchain_core.documents
    import langchain_core.embeddings
    import llama_index.core.schema


class DocumentStore:
    """
    Builds a document indexing pipeline for processing documents and querying closest documents
    to a query according to a specified index.

    Args:
        - docs: pathway tables typically coming out of connectors which contain source documents.
        - retriever_factory: factory for building an index, which will be provided
            texts by the ``DocumentStore``.
        - parser: callable that parses file contents into a list of documents.
        - splitter: callable that splits long documents.
        - doc_post_processors: optional list of callables that modify parsed files and metadata.
            any callable takes two arguments (text: str, metadata: dict) and returns them as a tuple.
    """

    def __init__(
        self,
        docs: pw.Table | Iterable[pw.Table],
        retriever_factory: AbstractRetrieverFactory,
        parser: Callable[[bytes], list[tuple[str, dict]]] | pw.UDF | None = None,
        splitter: Callable[[str], list[tuple[str, dict]]] | pw.UDF | None = None,
        doc_post_processors: (
            list[Callable[[str, dict], tuple[str, dict]] | pw.UDF] | None
        ) = None,
    ):
        self.docs = docs

        self.retriever_factory = retriever_factory

        self.parser: Callable[[bytes], list[tuple[str, dict]]] = _unwrap_udf(
            parser if parser is not None else pathway.xpacks.llm.parsers.ParseUtf8()
        )
        self.doc_post_processors = []

        if doc_post_processors:
            self.doc_post_processors = [
                _unwrap_udf(processor)
                for processor in doc_post_processors
                if processor is not None
            ]

        self.splitter = _unwrap_udf(
            splitter
            if splitter is not None
            else pathway.xpacks.llm.splitters.null_splitter
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
            - docs: pathway tables typically coming out of connectors which contain source documents
            - retriever_factory: factory for building an index, which will be provided
                texts by the ``DocumentStore``.
            - parser: callable that parses file contents into a list of documents
            - splitter: Langchaing component for splitting documents into parts
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
            - docs: pathway tables typically coming out of connectors which contain source documents
            - retriever_factory: factory for building an index, which will be provided
                texts by the ``DocumentStore``.
            - transformations: list of LlamaIndex components.
            - parser: callable that parses file contents into a list of documents
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

        def node_to_pathway(x: list[BaseNode]) -> list[tuple[str, dict]]:
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
        text: bytes
        metadata: pw.Json

    class _DocumentSchema(pw.Schema):
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

    InputsQuerySchema = FilterSchema

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

    # Applies udf on (docs.text, docs.metadata), then flattens list and extracts column
    # from json
    # It assumes that `processor` takes two arguments and returns a list of dicts with
    # keys "text" and "metadata"
    def _apply_processor(self, docs: pw.Table, processor: pw.UDF) -> pw.Table:
        processed_docs = (
            docs.select(data=processor(pw.this.text, pw.this.metadata))
            .flatten(pw.this.data)
            .select(
                text=pw.unwrap(pw.this.data["text"].as_str()),
                metadata=pw.this.data["metadata"],
            )
        )
        return processed_docs

    def parse_documents(
        self, input_docs: pw.Table[_RawDocumentSchema]
    ) -> pw.Table[_DocumentSchema]:
        @pw.udf
        def parse_doc(data: bytes, metadata: pw.Json) -> list[dict]:
            rets = self.parser(data)
            metadata_dict = metadata.as_dict()
            return [
                dict(text=ret[0], metadata={**metadata_dict, **ret[1]}) for ret in rets
            ]

        return self._apply_processor(input_docs, parse_doc)

    def post_process_docs(
        self, parsed_docs: pw.Table[_DocumentSchema]
    ) -> pw.Table[_DocumentSchema]:
        @pw.udf
        def post_proc_docs(text: str, metadata: pw.Json) -> list[dict]:
            metadata_dict = metadata.as_dict()
            for processor in self.doc_post_processors:
                text, metadata_dict = processor(text, metadata_dict)

            return [dict(text=text, metadata=metadata)]

        return self._apply_processor(parsed_docs, post_proc_docs)

    def split_docs(self, post_processed_docs: pw.Table) -> pw.Table:
        @pw.udf
        def split_doc(text: str, metadata: pw.Json) -> list[dict]:
            rets = self.splitter(text)
            return [
                dict(text=ret[0], metadata={**metadata.as_dict(), **ret[1]})
                for ret in rets
            ]

        return self._apply_processor(post_processed_docs, split_doc)

    def build_pipeline(self):

        if isinstance(self.docs, pw.Table):
            docs = self.docs
        else:
            docs_list = list(self.docs)
            if len(docs_list) == 0:
                raise ValueError(
                    """Please provide at least one data source, e.g. read files from disk:
pw.io.fs.read('./sample_docs', format='binary', mode='static', with_metadata=True)
"""
                )
            elif len(docs_list) == 1:
                (docs,) = self.docs
            else:
                docs = docs_list[0].concat_reindex(*docs_list[1:])

        self.input_docs = docs.select(text=pw.this.data, metadata=pw.this._metadata)
        self.parsed_docs = self.parse_documents(self.input_docs)
        self.post_processed_docs = self.post_process_docs(self.parsed_docs)
        self.chunked_docs = self.split_docs(self.post_processed_docs)

        self._retriever = self.retriever_factory.build_index(
            self.chunked_docs.text,
            self.chunked_docs,
            metadata_column=self.chunked_docs.metadata,
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
        @pw.udf
        def _get_jmespath_filter(
            metadata_filter: str, filepath_globpattern: str
        ) -> str | None:
            ret_parts = []
            if metadata_filter:
                metadata_filter = (
                    metadata_filter.replace("'", r"\'")
                    .replace("`", "'")
                    .replace('"', "")
                )
                ret_parts.append(f"({metadata_filter})")
            if filepath_globpattern:
                ret_parts.append(f"globmatch('{filepath_globpattern}', path)")
            if ret_parts:
                return " && ".join(ret_parts)
            return None

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
        self, input_queries: pw.Table[InputsQuerySchema]  # type: ignore
    ) -> pw.Table[InputsResultSchema]:
        """
        Query ``DocumentStore`` for the list of input documents.
        """
        # TODO: compare this approach to first joining queries to dicuments, then filtering,
        # then grouping to get each response.
        # The "dumb" tuple approach has more work precomputed for an all inputs query
        all_metas = self.input_docs.reduce(
            metadatas=pw.reducers.tuple(pw.this.metadata)
        )

        input_queries = self.merge_filters(input_queries)

        @pw.udf
        def format_inputs(
            metadatas: list[pw.Json] | None, metadata_filter: str | None
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

            return metadatas

        input_results = input_queries.join_left(all_metas, id=input_queries.id).select(
            all_metas.metadatas, input_queries.metadata_filter
        )
        input_results = input_results.select(
            result=format_inputs(pw.this.metadatas, pw.this.metadata_filter)
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

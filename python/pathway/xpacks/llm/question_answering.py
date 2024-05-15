# Copyright © 2024 Pathway
import json
from enum import Enum

import pathway as pw
from pathway.internals import ColumnReference, Table
from pathway.stdlib.indexing import DataIndex
from pathway.xpacks.llm import Doc, llms, prompts
from pathway.xpacks.llm.llms import prompt_chat_single_qa
from pathway.xpacks.llm.prompts import prompt_qa_geometric_rag
from pathway.xpacks.llm.vector_store import VectorStoreServer


@pw.udf
def _limit_documents(documents: list[str], k: int) -> list[str]:
    return documents[:k]


_answer_not_known = "I could not find an answer."
_answer_not_known_open_source = "No information available."


def _query_chat_strict_json(chat: pw.UDF, t: Table) -> pw.Table:

    t += t.select(
        prompt=prompt_qa_geometric_rag(
            t.query, t.documents, _answer_not_known_open_source, strict_prompt=True
        )
    )
    answer = t.select(answer=chat(prompt_chat_single_qa(t.prompt)))

    @pw.udf
    def extract_answer(response: str) -> str:
        response = response.strip()  # mistral-7b occasionally puts empty spaces
        json_start, json_finish = response.find("{"), response.find(
            "}"
        )  # remove unparsable part, mistral sometimes puts `[sources]` after the json

        unparsed_json = response[json_start : json_finish + 1]
        answer_dict = json.loads(unparsed_json)
        return " ".join(answer_dict.values())

    answer = answer.select(answer=extract_answer(pw.this.answer))

    @pw.udf
    def check_no_information(pred: str) -> bool:
        return "No information" in pred

    answer = answer.select(
        answer=pw.if_else(check_no_information(pw.this.answer), None, pw.this.answer)
    )
    return answer


def _query_chat_gpt(chat: pw.UDF, t: Table) -> pw.Table:
    t += t.select(
        prompt=prompt_qa_geometric_rag(t.query, t.documents, _answer_not_known)
    )
    answer = t.select(answer=chat(prompt_chat_single_qa(t.prompt)))

    answer = answer.select(
        answer=pw.if_else(pw.this.answer == _answer_not_known, None, pw.this.answer)
    )
    return answer


def _query_chat(chat: pw.UDF, t: Table, strict_prompt: bool) -> pw.Table:
    if strict_prompt:
        return _query_chat_strict_json(chat, t)
    else:
        return _query_chat_gpt(chat, t)


def _query_chat_with_k_documents(
    chat: pw.UDF, k: int, t: pw.Table, strict_prompt: bool
) -> pw.Table:
    limited_documents = t.select(
        pw.this.query, documents=_limit_documents(t.documents, k)
    )
    result = _query_chat(chat, limited_documents, strict_prompt)
    return result


def answer_with_geometric_rag_strategy(
    questions: ColumnReference,
    documents: ColumnReference,
    llm_chat_model: pw.UDF,
    n_starting_documents: int,
    factor: int,
    max_iterations: int,
    strict_prompt: bool = False,
) -> ColumnReference:
    """
    Function for querying LLM chat while providing increasing number of documents until an answer
    is found. Documents are taken from `documents` argument. Initially first `n_starting_documents` documents
    are embedded in the query. If the LLM chat fails to find an answer, the number of documents
    is multiplied by `factor` and the question is asked again.

    Args:
        questions (ColumnReference[str]): Column with questions to be asked to the LLM chat.
        documents (ColumnReference[list[str]]): Column with documents to be provided along
             with a question to the LLM chat.
        llm_chat_model: Chat model which will be queried for answers
        n_starting_documents: Number of documents embedded in the first query.
        factor: Factor by which a number of documents increases in each next query, if
            an answer is not found.
        max_iterations: Number of times to ask a question, with the increasing number of documents.
        strict_prompt: If LLM should be instructed strictly to return json.
            Increases performance in small open source models, not needed in OpenAI GPT models.

    Returns:
        A column with answers to the question. If answer is not found, then None is returned.

    Example:

    >>> import pandas as pd
    >>> import pathway as pw
    >>> from pathway.xpacks.llm.llms import OpenAIChat
    >>> from pathway.xpacks.llm.question_answering import answer_with_geometric_rag_strategy
    >>> chat = OpenAIChat()
    >>> df = pd.DataFrame(
    ...     {
    ...         "question": ["How do you connect to Kafka from Pathway?"],
    ...         "documents": [
    ...             [
    ...                 "`pw.io.csv.read reads a table from one or several files with delimiter-separated values.",
    ...                 "`pw.io.kafka.read` is a seneralized method to read the data from the given topic in Kafka.",
    ...             ]
    ...         ],
    ...     }
    ... )
    >>> t = pw.debug.table_from_pandas(df)
    >>> answers = answer_with_geometric_rag_strategy(t.question, t.documents, chat, 1, 2, 2)
    """
    n_documents = n_starting_documents
    t = Table.from_columns(query=questions, documents=documents)
    t = t.with_columns(answer=None)
    for _ in range(max_iterations):
        rows_without_answer = t.filter(pw.this.answer.is_none())
        results = _query_chat_with_k_documents(
            llm_chat_model, n_documents, rows_without_answer, strict_prompt
        )
        new_answers = rows_without_answer.with_columns(answer=results.answer)
        t = t.update_rows(new_answers)
        n_documents *= factor
    return t.answer


def answer_with_geometric_rag_strategy_from_index(
    questions: ColumnReference,
    index: DataIndex,
    documents_column: str | ColumnReference,
    llm_chat_model: pw.UDF,
    n_starting_documents: int,
    factor: int,
    max_iterations: int,
    metadata_filter: pw.ColumnExpression | None = None,
    strict_prompt: bool = False,
) -> ColumnReference:
    """
    Function for querying LLM chat while providing increasing number of documents until an answer
    is found. Documents are taken from `index`. Initially first `n_starting_documents` documents
    are embedded in the query. If the LLM chat fails to find an answer, the number of documents
    is multiplied by `factor` and the question is asked again.

    Args:
        questions (ColumnReference[str]): Column with questions to be asked to the LLM chat.
        index: Index from which closest documents are obtained.
        documents_column: name of the column in table passed to index, which contains documents.
        llm_chat_model: Chat model which will be queried for answers
        n_starting_documents: Number of documents embedded in the first query.
        factor: Factor by which a number of documents increases in each next query, if
            an answer is not found.
        max_iterations: Number of times to ask a question, with the increasing number of documents.
        strict_prompt: If LLM should be instructed strictly to return json.
            Increases performance in small open source models, not needed in OpenAI GPT models.

    Returns:
        A column with answers to the question. If answer is not found, then None is returned.
    """
    max_documents = n_starting_documents * (factor ** (max_iterations - 1))

    if isinstance(documents_column, ColumnReference):
        documents_column_name = documents_column.name
    else:
        documents_column_name = documents_column

    query_context = questions.table + index.query(
        questions,
        number_of_matches=max_documents,
        collapse_rows=True,
        metadata_filter=metadata_filter,
    ).select(
        documents_list=pw.this[documents_column_name],
    )

    return answer_with_geometric_rag_strategy(
        questions,
        query_context.documents_list,
        llm_chat_model,
        n_starting_documents,
        factor,
        max_iterations,
        strict_prompt=strict_prompt,
    )


class AIResponseType(Enum):
    SHORT = "short"
    LONG = "long"


@pw.udf
def _filter_document_metadata(
    docs: pw.Json | list[pw.Json] | list[Doc], metadata_keys: list[str] = ["path"]
) -> list[Doc]:
    """Filter context document metadata to keep the keys in the
    provided `metadata_keys` list.

    Works on both ColumnReference and list of pw.Json."""
    if isinstance(docs, pw.Json):
        doc_ls: list[Doc] = docs.as_list()
    elif isinstance(docs, list) and all([isinstance(dc, dict) for dc in docs]):
        doc_ls = docs  # type: ignore
    elif all([isinstance(dc, pw.Json) for dc in docs]):
        doc_ls = [dc.as_dict() for dc in docs]  # type: ignore
    else:
        raise ValueError(
            """`docs` argument is not instance of (pw.Json | list[pw.Json] | list[Doc]).
                         Please check your pipeline. Using `pw.reducers.tuple` may help."""
        )

    if len(doc_ls) == 1 and isinstance(doc_ls[0], list | tuple):  # unpack if needed
        doc_ls = doc_ls[0]

    filtered_docs = []
    for doc in doc_ls:
        filtered_doc = {"text": doc["text"]}
        for key in metadata_keys:
            if key in doc.get("metadata", {}):
                assert isinstance(doc["metadata"], dict)
                metadata_dict: dict = doc["metadata"]
                filtered_doc[key] = metadata_dict[key]

        filtered_docs.append(filtered_doc)

    return filtered_docs


class BaseRAGQuestionAnswerer:
    """
    Builds the logic and the API for basic RAG application.

    Base class to build RAG app with Pathway vector store and Pathway components.
    Gives the freedom to choose between two question answering strategies,
    short (concise), and long (detailed) response, that can be set during the post request.
    Allows for LLM agnosticity with freedom to choose from proprietary or open-source LLMs.

    Args:
        llm: LLM instance for question answering. See https://pathway.com/developers/api-docs/pathway-xpacks-llm/llms for available models.
        indexer: Indexing object for search & retrieval to be used for context augmentation.
        default_llm_name: Default LLM model to be used in queries, only used if `model` parameter in post request is not specified.
            Omitting or setting this to `None` will default to the model name set during LLM's initialization.

        short_prompt_template: Template for document question answering with short response.
            A pw.udf function is expected. Defaults to `pathway.xpacks.llm.prompts.prompt_short_qa`.
        long_prompt_template: Template for document question answering with long response.
            A pw.udf function is expected. Defaults to `pathway.xpacks.llm.prompts.prompt_qa`.
        summarize_template: Template for text summarization. Defaults to `pathway.xpacks.llm.prompts.prompt_summarize`.
        search_topk: Top k parameter for the retrieval. Adjusts number of chunks in the context.


    Example:
    >>> import pathway as pw  # doctest: +SKIP
    >>> from pathway.xpacks.llm import embedders, splitters, llms, parsers  # doctest: +SKIP
    >>> from pathway.xpacks.llm.vector_store import VectorStoreServer  # doctest: +SKIP
    >>> from pathway.udfs import DiskCache, ExponentialBackoffRetryStrategy  # doctest: +SKIP
    >>> from pathway.xpacks.llm.question_answering import BaseRAGQuestionAnswerer  # doctest: +SKIP
    >>> my_folder = pw.io.fs.read(
    ...     path="/PATH/TO/MY/DATA/*",  # replace with your folder
    ...     format="binary",
    ...     with_metadata=True)  # doctest: +SKIP
    >>> sources = [my_folder]  # doctest: +SKIP
    >>> app_host = "0.0.0.0"  # doctest: +SKIP
    >>> app_port = 8000  # doctest: +SKIP
    >>> parser = parsers.ParseUnstructured()  # doctest: +SKIP
    >>> text_splitter = splitters.TokenCountSplitter(max_tokens=400)  # doctest: +SKIP
    >>> embedder = embedders.OpenAIEmbedder(cache_strategy=DiskCache())  # doctest: +SKIP
    >>> vector_server = VectorStoreServer(  # doctest: +SKIP
    ...     *sources,
    ...     embedder=embedder,
    ...     splitter=text_splitter,
    ...     parser=parser,
    ... )
    >>> chat = llms.OpenAIChat(  # doctest: +SKIP
    ...     model=DEFAULT_GPT_MODEL,
    ...     retry_strategy=ExponentialBackoffRetryStrategy(max_retries=6),
    ...     cache_strategy=DiskCache(),
    ...     temperature=0.05,
    ... )
    >>> app = BaseRAGQuestionAnswerer(  # doctest: +SKIP
    ...     llm=chat,
    ...     indexer=vector_server,
    ... )
    >>> app.build_server(host=app_host, port=app_port)  # doctest: +SKIP
    >>> app.run_server()  # doctest: +SKIP
    """  # noqa: E501

    def __init__(
        self,
        llm: pw.UDF,
        indexer: VectorStoreServer,
        *,
        default_llm_name: str | None = None,
        short_prompt_template: pw.UDF = prompts.prompt_short_qa,
        long_prompt_template: pw.UDF = prompts.prompt_qa,
        summarize_template: pw.UDF = prompts.prompt_summarize,
        search_topk: int = 6,
    ) -> None:

        self.llm = llm
        self.indexer = indexer

        if default_llm_name is None:
            default_llm_name = llm.kwargs.get("model", None)  # type: ignore

        self._init_schemas(default_llm_name)

        self.short_prompt_template = short_prompt_template
        self.long_prompt_template = long_prompt_template
        self.summarize_template = summarize_template

        self.search_topk = search_topk

    def _init_schemas(self, default_llm_name: str | None = None) -> None:
        """Initialize API schemas with optional and non-optional arguments."""

        class PWAIQuerySchema(pw.Schema):
            prompt: str
            filters: str | None = pw.column_definition(default_value=None)
            model: str | None = pw.column_definition(default_value=default_llm_name)
            response_type: str = pw.column_definition(
                default_value=AIResponseType.SHORT.value
            )

        class SummarizeQuerySchema(pw.Schema):
            text_list: list[str]
            model: str | None = pw.column_definition(default_value=default_llm_name)

        self.PWAIQuerySchema = PWAIQuerySchema
        self.SummarizeQuerySchema = SummarizeQuerySchema

    @pw.table_transformer
    def pw_ai_query(self, pw_ai_queries: pw.Table) -> pw.Table:
        """Main function for RAG applications that answer questions
        based on available information."""

        pw_ai_results = pw_ai_queries + self.indexer.retrieve_query(
            pw_ai_queries.select(
                metadata_filter=pw.this.filters,
                filepath_globpattern=pw.cast(str | None, None),
                query=pw.this.prompt,
                k=self.search_topk,
            )
        ).select(
            docs=pw.this.result,
        )

        pw_ai_results = pw_ai_results.select(
            *pw.this, filtered_docs=_filter_document_metadata(pw.this.docs)
        )

        pw_ai_results += pw_ai_results.select(
            rag_prompt=pw.if_else(
                pw.this.response_type == AIResponseType.SHORT.value,
                self.short_prompt_template(pw.this.prompt, pw.this.filtered_docs),
                self.long_prompt_template(pw.this.prompt, pw.this.filtered_docs),
            )
        )

        pw_ai_results += pw_ai_results.select(
            result=self.llm(
                llms.prompt_chat_single_qa(pw.this.rag_prompt),
                model=pw.this.model,
            )
        )
        return pw_ai_results

    @pw.table_transformer
    def summarize_query(self, summarize_queries: pw.Table) -> pw.Table:
        """Function for summarizing given texts."""

        summarize_results = summarize_queries.select(
            pw.this.model,
            prompt=self.summarize_template(pw.this.text_list),
        )
        summarize_results += summarize_results.select(
            result=self.llm(
                llms.prompt_chat_single_qa(pw.this.prompt),
                model=pw.this.model,
            )
        )
        return summarize_results

    # connect http endpoint to output writer
    def serve(self, route, schema, handler, webserver, **additional_endpoint_kwargs):

        queries, writer = pw.io.http.rest_connector(
            webserver=webserver,
            route=route,
            schema=schema,
            autocommit_duration_ms=50,
            delete_completed_queries=True,
            **additional_endpoint_kwargs,
        )
        writer(handler(queries))

    def build_server(
        self,
        host: str,
        port: int,
        **rest_kwargs,
    ) -> None:
        """Adds HTTP connectors to input tables, connects them with table transformers."""

        webserver = pw.io.http.PathwayWebserver(host=host, port=port)

        self.serve(
            "/v1/retrieve",
            self.indexer.RetrieveQuerySchema,
            self.indexer.retrieve_query,
            webserver,
            **rest_kwargs,
        )
        self.serve(
            "/v1/statistics",
            self.indexer.StatisticsQuerySchema,
            self.indexer.statistics_query,
            webserver,
            **rest_kwargs,
        )
        self.serve(
            "/v1/pw_list_documents",
            self.indexer.InputsQuerySchema,
            self.indexer.inputs_query,
            webserver,
            **rest_kwargs,
        )
        self.serve(
            "/v1/pw_ai_answer",
            self.PWAIQuerySchema,
            self.pw_ai_query,
            webserver,
            **rest_kwargs,
        )
        self.serve(
            "/v1/pw_ai_summary",
            self.SummarizeQuerySchema,
            self.summarize_query,
            webserver,
            **rest_kwargs,
        )

    def run_server(
        self,
        with_cache: bool = True,
        cache_backend: (
            pw.persistence.Backend | None
        ) = pw.persistence.Backend.filesystem("./Cache"),
        *args,
        **kwargs,
    ):
        """Start the app with cache configs. Enabling persistence will cache the embedding,
        and LLM requests between the runs."""

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
            *args,
            **kwargs,
        )


class AdaptiveRAGQuestionAnswerer(BaseRAGQuestionAnswerer):
    def __init__(
        self,
        llm: pw.UDF,
        indexer: VectorStoreServer,
        *,
        default_llm_name: str | None = None,
        short_prompt_template: pw.UDF = prompts.prompt_short_qa,
        long_prompt_template: pw.UDF = prompts.prompt_qa,
        summarize_template: pw.UDF = prompts.prompt_summarize,
        n_starting_documents: int = 2,
        factor: int = 2,
        max_iterations: int = 4,
        strict_prompt: bool = False,
    ) -> None:
        super().__init__(
            llm,
            indexer,
            default_llm_name=default_llm_name,
            short_prompt_template=short_prompt_template,
            long_prompt_template=long_prompt_template,
            summarize_template=summarize_template,
        )
        self.n_starting_documents = n_starting_documents
        self.factor = factor
        self.max_iterations = max_iterations
        self.strict_prompt = strict_prompt

    @pw.table_transformer
    def pw_ai_query(self, pw_ai_queries: pw.Table) -> pw.Table:
        """Create RAG response with adaptive retrieval."""

        index = self.indexer

        result = pw_ai_queries.select(
            *pw.this,
            result=answer_with_geometric_rag_strategy_from_index(
                pw_ai_queries.prompt,
                index,  # type: ignore
                "data",  # knn index returns result in this column
                self.llm,
                n_starting_documents=self.n_starting_documents,
                factor=self.factor,
                max_iterations=self.max_iterations,
                strict_prompt=self.strict_prompt,
                metadata_filter=pw_ai_queries.filters,
            ),
        )

        return result

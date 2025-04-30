# Copyright © 2024 Pathway
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable
from warnings import warn

import requests

import pathway as pw
from pathway.internals import ColumnReference, Table, udfs
from pathway.stdlib.indexing import DataIndex
from pathway.xpacks.llm import Doc, llms, prompts
from pathway.xpacks.llm.document_store import (
    DocumentStore,
    SlidesDocumentStore,
    _get_jmespath_filter,
)
from pathway.xpacks.llm.llms import BaseChat, prompt_chat_single_qa
from pathway.xpacks.llm.vector_store import (
    SlidesVectorStoreServer,
    VectorStoreClient,
    VectorStoreServer,
)

if TYPE_CHECKING:
    from pathway.xpacks.llm.servers import QARestServer, QASummaryRestServer


@pw.udf
def _limit_documents(documents: list[pw.Json], k: int) -> list[pw.Json]:
    return documents[:k]


_answer_not_known = "I could not find an answer."
_answer_not_known_open_source = "No information available."


class BaseContextProcessor(ABC):
    """Base class for formatting documents to LLM context.

    Abstract method ``docs_to_context`` defines the behavior for converting documents to context.
    """

    def maybe_unwrap_docs(self, docs: pw.Json | list[pw.Json] | list[Doc]):
        if isinstance(docs, pw.Json):
            doc_ls: list[Doc] = docs.as_list()
        elif isinstance(docs, list) and all([isinstance(dc, dict) for dc in docs]):
            doc_ls = docs  # type: ignore
        elif all([isinstance(doc, pw.Json) for doc in docs]):
            doc_ls = [doc.as_dict() for doc in docs]  # type: ignore
        else:
            raise ValueError(
                """`docs` argument is not instance of (pw.Json | list[pw.Json] | list[Doc]).
                            Please check your pipeline. Using `pw.reducers.tuple` may help."""
            )

        if len(doc_ls) == 1 and isinstance(doc_ls[0], list | tuple):  # unpack if needed
            doc_ls = doc_ls[0]

        return doc_ls

    def apply(self, docs: pw.Json | list[pw.Json] | list[Doc]) -> str:
        unwrapped_docs = self.maybe_unwrap_docs(docs)
        return self.docs_to_context(unwrapped_docs)

    @abstractmethod
    def docs_to_context(self, docs: list[dict] | list[Doc]) -> str: ...

    def as_udf(self) -> pw.UDF:
        return pw.udf(self.apply)


@dataclass
class SimpleContextProcessor(BaseContextProcessor):
    """Context processor that filters metadata fields and joins the documents."""

    context_metadata_keys: list[str] = field(default_factory=lambda: ["path"])
    context_joiner: str = "\n\n"

    def simplify_context_metadata(self, docs: list[Doc]) -> list[Doc]:
        filtered_docs = []
        for doc in docs:
            filtered_doc = {"text": doc["text"]}
            doc_metadata: dict = doc.get("metadata", {})  # type: ignore

            for key in self.context_metadata_keys:

                if key in doc_metadata:
                    filtered_doc[key] = doc_metadata[key]

            filtered_docs.append(filtered_doc)

        return filtered_docs

    def docs_to_context(self, docs: list[dict] | list[Doc]) -> str:
        docs = self.simplify_context_metadata(docs)

        context = self.context_joiner.join(
            [json.dumps(doc, ensure_ascii=False) for doc in docs]
        )

        return context


@pw.udf
def _prepare_RAG_response(
    response: str, docs: list[dict], return_context_docs: bool
) -> pw.Json:
    api_response: dict = {"response": response}
    if return_context_docs:
        api_response["context_docs"] = docs

    return pw.Json(api_response)


def _get_RAG_prompt_udf(prompt_template: str | Callable[[str, str], str] | pw.UDF):
    if isinstance(prompt_template, pw.UDF) or callable(prompt_template):
        verified_template: prompts.BasePromptTemplate = (
            prompts.RAGFunctionPromptTemplate(function_template=prompt_template)
        )
    elif isinstance(prompt_template, str):
        verified_template = prompts.RAGPromptTemplate(template=prompt_template)
    else:
        raise ValueError(
            f"Prompt template is not of expected type. Got: {type(prompt_template)}."
        )

    return verified_template.as_udf()


def _get_context_processor_udf(
    context_processor: (
        BaseContextProcessor | Callable[[list[dict] | list[Doc]], str] | pw.UDF
    )
) -> pw.UDF:
    if isinstance(context_processor, BaseContextProcessor):
        return context_processor.as_udf()
    elif isinstance(context_processor, pw.UDF):
        return context_processor
    elif callable(context_processor):
        return pw.udf(context_processor)
    else:
        raise ValueError(
            "Context processor must be type of one of the following: \
            ~BaseContextProcessor | Callable[[list[dict] | list[Doc]], str] | ~pw.UDF"
        )


def _query_chat(
    chat: BaseChat,
    t: Table,
    prompt_udf: pw.UDF,
    no_answer_string: str,
    context_processor: pw.UDF,
) -> pw.Table:
    t = t.with_columns(context=context_processor(t.documents))
    t += t.select(prompt=prompt_udf(t.context, t.query))
    answer = t.select(answer=chat(prompt_chat_single_qa(t.prompt)))

    answer = answer.select(
        answer=pw.if_else(pw.this.answer == no_answer_string, None, pw.this.answer)
    )
    return answer


def _query_chat_with_k_documents(
    chat: BaseChat,
    k: int,
    t: pw.Table,
    prompt_udf: pw.UDF,
    no_answer_string: str,
    context_processor: pw.UDF,
) -> pw.Table:
    limited_documents = t.select(
        pw.this.query, documents=_limit_documents(t.documents, k)
    )
    result = _query_chat(
        chat, limited_documents, prompt_udf, no_answer_string, context_processor
    ).with_columns(documents=limited_documents.documents)
    return result


def answer_with_geometric_rag_strategy(
    questions: ColumnReference,
    documents: ColumnReference,
    llm_chat_model: BaseChat,
    *,
    prompt_template: (
        str | Callable[[str, str], str] | pw.UDF
    ) = prompts.prompt_qa_geometric_rag,
    no_answer_string: str = "No information found.",
    context_processor: (
        BaseContextProcessor | Callable[[list[dict] | list[Doc]], str] | pw.UDF
    ) = SimpleContextProcessor(),
    n_starting_documents: int = 2,
    factor: int = 2,
    max_iterations: int = 4,
    return_context_docs: pw.ColumnExpression | bool = False,
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
        prompt_template: Template for document question answering with short response.
            Either string template, callable or a pw.udf function is expected.
            Defaults to ``pathway.xpacks.llm.prompts.prompt_qa``.
            String template needs to have ``context`` and ``query`` placeholders in curly brackets ``{}``.
            For Adaptive RAG to work, prompt needs to instruct, to return `no_answer_string`
            when information is not found.
        no_answer_string: string that will be returned by the LLM when information is not found.
        context_processor: Utility for representing the fetched documents to the LLM. Callable,
            UDF or ``BaseContextProcessor`` is expected.
            Defaults to ``SimpleContextProcessor`` that keeps the 'path' metadata
            and joins the documents with double new lines.
        n_starting_documents: Number of documents embedded in the first query.
        factor: Factor by which a number of documents increases in each next query, if
            an answer is not found.
        max_iterations: Number of times to ask a question, with the increasing number of documents.
        return_context_docs: A flag indicating whether context documents are to be returned
            in the answer.

    Returns:
        A column with answers to the question. The answer is returned as a dictionary
        that has a key ``"response"`` with the answer as a string. Additionally, if
        ``return_context_docs == True``, then it has additionally a key ``"context_docs"``
        with the list of documents that were used as a context for the question.

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
    >>> answers = answer_with_geometric_rag_strategy(t.question, t.documents, chat)
    """

    @pw.udf
    def make_json(docs: list[str]) -> list[dict]:
        return [{"text": doc} for doc in docs]

    prompt_udf = _get_RAG_prompt_udf(prompt_template)
    n_documents = n_starting_documents
    t = Table.from_columns(query=questions, documents=documents)
    t = t.with_columns(
        answer=None,
        documents_used=None,
        return_context_docs=return_context_docs,
        documents=make_json(documents),
    )
    context_processor = _get_context_processor_udf(context_processor)
    for _ in range(max_iterations):
        rows_without_answer = t.filter(pw.this.answer.is_none())
        results = _query_chat_with_k_documents(
            llm_chat_model,
            n_documents,
            rows_without_answer,
            prompt_udf,
            no_answer_string,
            context_processor=context_processor,
        )
        new_answers = rows_without_answer.with_columns(
            answer=results.answer, documents_used=results.documents
        )
        t = t.update_rows(new_answers)
        n_documents *= factor

    t = t.select(
        answer=pw.if_else(
            pw.this.answer.is_none(),
            _prepare_RAG_response(
                no_answer_string, pw.this.documents, pw.this.return_context_docs
            ),
            _prepare_RAG_response(
                pw.this.answer, pw.this.documents_used, pw.this.return_context_docs
            ),
        )
    )

    return t.answer


def answer_with_geometric_rag_strategy_from_index(
    questions: ColumnReference,
    index: DataIndex,
    documents_column: str | ColumnReference,
    llm_chat_model: BaseChat,
    *,
    prompt_template: (
        str | Callable[[str, str], str] | pw.UDF
    ) = prompts.prompt_qa_geometric_rag,
    no_answer_string: str = "No information found.",
    context_processor: (
        BaseContextProcessor | Callable[[list[dict] | list[Doc]], str] | pw.UDF
    ) = SimpleContextProcessor(),
    n_starting_documents: int = 2,
    factor: int = 2,
    max_iterations: int = 4,
    return_context_docs: pw.ColumnExpression | bool = False,
    metadata_filter: pw.ColumnExpression | None = None,
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
        prompt_template: Template for document question answering with short response.
            Either string template, callable or a pw.udf function is expected.
            Defaults to ``pathway.xpacks.llm.prompts.prompt_qa``.
            String template needs to have ``context`` and ``query`` placeholders in curly brackets ``{}``.
            For Adaptive RAG to work, prompt needs to instruct, to return `no_answer_string`
            when information is not found.
        no_answer_string: string that will be returned by the LLM when information is not found.
        context_processor: Utility for representing the fetched documents to the LLM. Callable,
            UDF or ``BaseContextProcessor`` is expected.
            Defaults to ``SimpleContextProcessor`` that keeps the 'path' metadata
            and joins the documents with double new lines.
        n_starting_documents: Number of documents embedded in the first query.
        factor: Factor by which a number of documents increases in each next query, if
            an answer is not found.
        max_iterations: Number of times to ask a question, with the increasing number of documents.
        return_context_docs: A flag indicating whether context documents are to be returned
            in the answer.
        metadata_filter: Metadata filter used when retrieving documents from the index.

    Returns:
        A column with answers to the question. The answer is returned as a dictionary
        that has a key ``"response"`` with the answer as a string. Additionally, if
        ``return_context_docs == True``, then it has additionally a key ``"context_docs"``
        with the list of documents that were used as a context for the question.
    """
    max_documents = n_starting_documents * (factor ** (max_iterations - 1))

    if isinstance(documents_column, ColumnReference):
        documents_column_name = documents_column.name
    else:
        documents_column_name = documents_column

    query_context = questions.table + index.query_as_of_now(
        questions,
        number_of_matches=max_documents,
        collapse_rows=True,
        metadata_filter=_get_jmespath_filter(metadata_filter, ""),
    ).select(
        documents_list=pw.coalesce(pw.this[documents_column_name], ()),
    )

    return answer_with_geometric_rag_strategy(
        questions,
        query_context.documents_list,
        llm_chat_model,
        n_starting_documents=n_starting_documents,
        factor=factor,
        max_iterations=max_iterations,
        return_context_docs=return_context_docs,
        prompt_template=prompt_template,
        no_answer_string=no_answer_string,
        context_processor=context_processor,
    )


class BaseQuestionAnswerer:
    AnswerQuerySchema: type[pw.Schema] = pw.Schema
    RetrieveQuerySchema: type[pw.Schema] = pw.Schema
    StatisticsQuerySchema: type[pw.Schema] = pw.Schema
    InputsQuerySchema: type[pw.Schema] = pw.Schema

    @abstractmethod
    def answer_query(self, pw_ai_queries: pw.Table) -> pw.Table: ...

    @abstractmethod
    def retrieve(self, retrieve_queries: pw.Table) -> pw.Table: ...

    @abstractmethod
    def statistics(self, statistics_queries: pw.Table) -> pw.Table: ...

    @abstractmethod
    def list_documents(self, list_documents_queries: pw.Table) -> pw.Table: ...


class SummaryQuestionAnswerer(BaseQuestionAnswerer):
    SummarizeQuerySchema: type[pw.Schema] = pw.Schema

    @abstractmethod
    def summarize_query(self, summarize_queries: pw.Table) -> pw.Table: ...


class BaseRAGQuestionAnswerer(SummaryQuestionAnswerer):
    """
    Builds the logic and the API for basic RAG application.

    Base class to build RAG app with Pathway vector store and Pathway components.
    Allows for LLM agnosticity with freedom to choose from proprietary or open-source LLMs.

    Args:
        llm: LLM instance for question answering. See https://pathway.com/developers/api-docs/pathway-xpacks-llm/llms for available models.
        indexer: Indexing object for search & retrieval to be used for context augmentation.
        default_llm_name: Default LLM model to be used in queries, only used if ``model`` parameter in post request is not specified.
            Omitting or setting this to ``None`` will default to the model name set during LLM's initialization.
        prompt_template: Template for document question answering with short response.
            Either string template, callable or a pw.udf function is expected.
            Defaults to ``pathway.xpacks.llm.prompts.prompt_qa``.
            String template needs to have ``context`` and ``query`` placeholders in curly brackets ``{}``.
        context_processor: Utility for representing the fetched documents to the LLM. Callable, UDF or ``BaseContextProcessor`` is expected.
            Defaults to ``SimpleContextProcessor`` that keeps the 'path' metadata and joins the documents with double new lines.
        summarize_template: Template for text summarization. Defaults to ``pathway.xpacks.llm.prompts.prompt_summarize``.
        search_topk: Top k parameter for the retrieval. Adjusts number of chunks in the context.


    Example:

    >>> import pathway as pw  # doctest: +SKIP
    >>> from pathway.xpacks.llm import embedders, splitters, llms, parsers  # doctest: +SKIP
    >>> from pathway.xpacks.llm.vector_store import VectorStoreServer  # doctest: +SKIP
    >>> from pathway.udfs import DiskCache, ExponentialBackoffRetryStrategy  # doctest: +SKIP
    >>> from pathway.xpacks.llm.question_answering import BaseRAGQuestionAnswerer  # doctest: +SKIP
    >>> from pathway.xpacks.llm.servers import QASummaryRestServer # doctest: +SKIP
    >>> my_folder = pw.io.fs.read(
    ...     path="/PATH/TO/MY/DATA/*",  # replace with your folder
    ...     format="binary",
    ...     with_metadata=True)  # doctest: +SKIP
    >>> sources = [my_folder]  # doctest: +SKIP
    >>> app_host = "0.0.0.0"  # doctest: +SKIP
    >>> app_port = 8000  # doctest: +SKIP
    >>> parser = parsers.UnstructuredParser()  # doctest: +SKIP
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
    >>> prompt_template = "Answer the question. Context: {context}. Question: {query}"  # doctest: +SKIP
    >>> rag = BaseRAGQuestionAnswerer(  # doctest: +SKIP
    ...     llm=chat,
    ...     indexer=vector_server,
    ...     prompt_template=prompt_template,
    ... )
    >>> app = QASummaryRestServer(app_host, app_port, rag)  # doctest: +SKIP
    >>> app.run()  # doctest: +SKIP
    """  # noqa: E501

    def __init__(
        self,
        llm: BaseChat,
        indexer: VectorStoreServer | DocumentStore,
        *,
        default_llm_name: str | None = None,
        prompt_template: str | Callable[[str, str], str] | pw.UDF = prompts.prompt_qa,
        context_processor: (
            BaseContextProcessor | Callable[[list[dict] | list[Doc]], str] | pw.UDF
        ) = SimpleContextProcessor(),
        summarize_template: pw.UDF = prompts.prompt_summarize,
        search_topk: int = 6,
    ) -> None:

        self.llm = llm
        self.indexer = indexer

        if default_llm_name is None:
            default_llm_name = llm.model

        self._init_schemas(default_llm_name)

        self.prompt_udf = _get_RAG_prompt_udf(prompt_template)

        if isinstance(context_processor, BaseContextProcessor):
            self.docs_to_context_transformer = context_processor.as_udf()
        elif isinstance(context_processor, pw.UDF):
            self.docs_to_context_transformer = context_processor
        elif callable(context_processor):
            self.docs_to_context_transformer = pw.udf(context_processor)
        else:
            raise ValueError(
                "Context processor must be type of one of the following: \
                             ~BaseContextProcessor | Callable[[list[dict] | list[Doc]], str] | ~pw.UDF"
            )

        self._pending_endpoints: list[tuple] = []

        self.summarize_template = summarize_template
        self.search_topk = search_topk
        self.server: None | QASummaryRestServer = None

    def _init_schemas(self, default_llm_name: str | None = None) -> None:
        """Initialize API schemas with optional and non-optional arguments."""

        class PWAIQuerySchema(pw.Schema):
            prompt: str
            filters: str | None = pw.column_definition(default_value=None)
            model: str | None = pw.column_definition(default_value=default_llm_name)
            return_context_docs: bool = pw.column_definition(default_value=False)

        class SummarizeQuerySchema(pw.Schema):
            text_list: list[str]
            model: str | None = pw.column_definition(default_value=default_llm_name)

        self.AnswerQuerySchema = PWAIQuerySchema
        self.SummarizeQuerySchema = SummarizeQuerySchema
        self.RetrieveQuerySchema = self.indexer.RetrieveQuerySchema
        self.StatisticsQuerySchema = self.indexer.StatisticsQuerySchema
        self.InputsQuerySchema = self.indexer.InputsQuerySchema

    @pw.table_transformer
    def answer_query(self, pw_ai_queries: pw.Table) -> pw.Table:
        """Answer a question based on the available information."""

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

        pw_ai_results += pw_ai_results.select(
            context=self.docs_to_context_transformer(pw.this.docs)
        )

        pw_ai_results += pw_ai_results.select(
            rag_prompt=self.prompt_udf(pw.this.context, pw.this.prompt)
        )

        pw_ai_results += pw_ai_results.select(
            response=self.llm(
                llms.prompt_chat_single_qa(pw.this.rag_prompt),
                model=pw.this.model,
            )
        )

        pw_ai_results = pw_ai_results.await_futures()

        pw_ai_results += pw_ai_results.select(
            result=_prepare_RAG_response(
                pw.this.response, pw.this.docs, pw.this.return_context_docs
            )
        )

        return pw_ai_results

    def pw_ai_query(self, pw_ai_queries: pw.Table) -> pw.Table:
        warn(
            "pw_ai_query method is deprecated. Its content has been moved to answer_query method.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.answer_query(pw_ai_queries)

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

    @pw.table_transformer
    def retrieve(self, retrieve_queries: pw.Table) -> pw.Table:
        """
        Retrieve documents from the index.
        """
        return self.indexer.retrieve_query(retrieve_queries)

    @pw.table_transformer
    def statistics(self, statistics_queries: pw.Table) -> pw.Table:
        """
        Get statistics about indexed files.
        """
        return self.indexer.statistics_query(statistics_queries)

    @pw.table_transformer
    def list_documents(self, list_documents_queries: pw.Table) -> pw.Table:
        """
        Get list of documents from the retriever.
        """
        return self.indexer.inputs_query(list_documents_queries)

    def build_server(
        self,
        host: str,
        port: int,
        **rest_kwargs,
    ):
        """Adds HTTP connectors to input tables, connects them with table transformers."""
        # circular import
        from pathway.xpacks.llm.servers import QASummaryRestServer

        self.server = QASummaryRestServer(host, port, self, **rest_kwargs)

        # register awaiting endpoints
        for (
            route,
            schema,
            callable_func,
            retry_strategy,
            cache_strategy,
            additional_endpoint_kwargs,
        ) in self._pending_endpoints:
            self.server.serve_callable(
                route=route,
                schema=schema,
                callable_func=callable_func,
                retry_strategy=retry_strategy,
                cache_strategy=cache_strategy,
                **additional_endpoint_kwargs,
            )
        self._pending_endpoints.clear()

    def serve_callable(
        self,
        route: str,
        schema: type[pw.Schema] | None = None,
        retry_strategy: udfs.AsyncRetryStrategy | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        **additional_endpoint_kwargs,
    ):
        """Serve additional endpoints by wrapping callables.
        Expects an endpoint route. Schema is optional, adding schema type will enforce the
            webserver to check arguments.
        Beware that if Schema is not set, incorrect types may cause runtime error.

        Example:

        >>> @rag_app.serve_callable(route="/agent")  # doctest: +SKIP
        ... async def some_func(user_query: str) -> str:
        ...     # define your agent, or custom RAG using any framework or plain Python
        ...     # ...
        ...     messages = [{"role": "user", "content": user_query}]
        ...     result = agent.invoke(messages)
        ...     return result
        """

        def decorator(callable_func):

            if self.server is None:
                self._pending_endpoints.append(
                    (
                        route,
                        schema,
                        callable_func,
                        retry_strategy,
                        cache_strategy,
                        additional_endpoint_kwargs,
                    )
                )
                warn(
                    "Adding an endpoint while webserver is not built, \
                    it will be registered when `build_server` is called."
                )
            else:
                self.server.serve_callable(
                    route=route,
                    schema=schema,
                    callable_func=callable_func,
                    retry_strategy=retry_strategy,
                    cache_strategy=cache_strategy,
                    **additional_endpoint_kwargs,
                )
            return callable_func

        return decorator

    def run_server(self, *args, **kwargs):
        if self.server is None:
            raise ValueError(
                "HTTP server is not built, initialize it with `build_server`"
            )
        self.server.run(*args, **kwargs)


class AdaptiveRAGQuestionAnswerer(BaseRAGQuestionAnswerer):
    """
    Builds the logic and the API for adaptive RAG application.

    It allows to build a RAG app with Pathway vector store and Pathway components.
    Gives the freedom to choose between two question answering strategies,
    short (concise), and long (detailed) response, that can be set during the post request.
    Allows for LLM agnosticity with freedom to choose from proprietary or open-source LLMs.

    It differs from :py:class:`~pathway.xpacks.llm.question_answering.BaseRAGQuestionAnswerer`
    in adaptive choosing the number of chunks used as a context of a question.
    First, only ``n_starting_documents`` chunks are used,
    and then the number is increased until an answer is found.

    Args:
        llm: LLM instance for question answering. See https://pathway.com/developers/api-docs/pathway-xpacks-llm/llms for available models.
        indexer: Indexing object for search & retrieval to be used for context augmentation.
        default_llm_name: Default LLM model to be used in queries, only used if ``model`` parameter in post request is not specified.
            Omitting or setting this to ``None`` will default to the model name set during LLM's initialization.
        prompt_template: Template for document question answering with short response.
            Either string template, callable or a pw.udf function is expected.
            Defaults to ``pathway.xpacks.llm.prompts.prompt_qa``.
            String template needs to have ``context`` and ``query`` placeholders in curly brackets ``{}``.
            For Adaptive RAG to work, prompt needs to instruct, to return `no_answer_string`
            when information is not found.
        no_answer_string: string that will be returned by the LLM when information is not found.
        context_processor: Utility for representing the fetched documents to the LLM. Callable, UDF or ``BaseContextProcessor`` is expected.
            Defaults to ``SimpleContextProcessor`` that keeps the 'path' metadata and joins the documents with double new lines.
        summarize_template: Template for text summarization. Defaults to ``pathway.xpacks.llm.prompts.prompt_summarize``.
        n_starting_documents: Number of documents embedded in the first query.
        factor: Factor by which a number of documents increases in each next query, if
            an answer is not found.
        max_iterations: Number of times to ask a question, with the increasing number of documents.


    Example:

    >>> import pathway as pw  # doctest: +SKIP
    >>> from pathway.xpacks.llm import embedders, splitters, llms, parsers  # doctest: +SKIP
    >>> from pathway.xpacks.llm.vector_store import VectorStoreServer  # doctest: +SKIP
    >>> from pathway.udfs import DiskCache, ExponentialBackoffRetryStrategy  # doctest: +SKIP
    >>> from pathway.xpacks.llm.question_answering import AdaptiveRAGQuestionAnswerer  # doctest: +SKIP
    >>> my_folder = pw.io.fs.read(
    ...     path="/PATH/TO/MY/DATA/*",  # replace with your folder
    ...     format="binary",
    ...     with_metadata=True)  # doctest: +SKIP
    >>> sources = [my_folder]  # doctest: +SKIP
    >>> app_host = "0.0.0.0"  # doctest: +SKIP
    >>> app_port = 8000  # doctest: +SKIP
    >>> parser = parsers.UnstructuredParser()  # doctest: +SKIP
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
    >>> app = AdaptiveRAGQuestionAnswerer(  # doctest: +SKIP
    ...     llm=chat,
    ...     indexer=vector_server,
    ... )
    >>> app.build_server(host=app_host, port=app_port)  # doctest: +SKIP
    >>> app.run_server()  # doctest: +SKIP
    """  # noqa: E501

    def __init__(
        self,
        llm: BaseChat,
        indexer: VectorStoreServer | DocumentStore,
        *,
        default_llm_name: str | None = None,
        prompt_template: (
            str | Callable[[str, str], str] | pw.UDF
        ) = prompts.prompt_qa_geometric_rag,
        no_answer_string: str = "No information found.",
        context_processor: (
            BaseContextProcessor | Callable[[list[dict] | list[Doc]], str] | pw.UDF
        ) = SimpleContextProcessor(),
        summarize_template: pw.UDF = prompts.prompt_summarize,
        n_starting_documents: int = 2,
        factor: int = 2,
        max_iterations: int = 4,
    ) -> None:
        super().__init__(
            llm,
            indexer,
            default_llm_name=default_llm_name,
            summarize_template=summarize_template,
            context_processor=context_processor,
        )
        self.prompt_template = prompt_template
        self.n_starting_documents = n_starting_documents
        self.factor = factor
        self.max_iterations = max_iterations
        self.no_answer_string = no_answer_string

    @pw.table_transformer
    def answer_query(self, pw_ai_queries: pw.Table) -> pw.Table:
        """Create RAG response with adaptive retrieval."""

        index = self.indexer.index
        if isinstance(self.indexer, VectorStoreServer):
            data_column_name = "data"
        else:
            data_column_name = "text"

        result = pw_ai_queries.select(
            result=answer_with_geometric_rag_strategy_from_index(
                pw_ai_queries.prompt,
                index,
                data_column_name,  # index returns result in this column
                self.llm,
                n_starting_documents=self.n_starting_documents,
                factor=self.factor,
                max_iterations=self.max_iterations,
                prompt_template=self.prompt_template,
                no_answer_string=self.no_answer_string,
                context_processor=self.docs_to_context_transformer,
                metadata_filter=pw_ai_queries.filters,
                return_context_docs=pw_ai_queries.return_context_docs,
            ),
        )

        return result


class DeckRetriever(BaseQuestionAnswerer):
    """
    Builds the logic for the Retriever of slides.

    Args:
        indexer: document store for parsing and indexing slides.
        search_topk: Number of slides to be returned by the `answer_query` method.
    """

    excluded_response_metadata = ["b64_image"]

    def __init__(
        self,
        indexer: SlidesDocumentStore | SlidesVectorStoreServer,
        *,
        search_topk: int = 6,
    ) -> None:
        self.indexer = indexer
        self._init_schemas()
        self.search_topk = search_topk

        self.server: None | QARestServer = None
        self._pending_endpoints: list[tuple] = []

    def _init_schemas(
        self,
    ) -> None:
        class PWAIQuerySchema(pw.Schema):
            prompt: str
            filters: str | None = pw.column_definition(default_value=None)

        self.AnswerQuerySchema = PWAIQuerySchema
        self.RetrieveQuerySchema = self.indexer.RetrieveQuerySchema
        self.StatisticsQuerySchema = self.indexer.StatisticsQuerySchema
        self.InputsQuerySchema = self.indexer.InputsQuerySchema

    @pw.table_transformer
    def answer_query(self, pw_ai_queries: pw.Table) -> pw.Table:
        """Return slides similar to the given query."""

        pw_ai_results = pw_ai_queries + self.indexer.retrieve_query(
            pw_ai_queries.select(
                metadata_filter=pw.this.filters,
                filepath_globpattern=None,
                query=pw.this.prompt,
                k=self.search_topk,
            )
        ).select(
            docs=pw.this.result,
        )

        @pw.udf
        def _format_results(docs: pw.Json) -> pw.Json:
            docs_ls = docs.as_list()

            for docs_dc in docs_ls:
                metadata: dict = docs_dc["metadata"]

                for metadata_key in self.excluded_response_metadata:
                    metadata.pop(metadata_key, None)

                docs_dc["metadata"] = metadata

            return pw.Json(docs_ls)

        pw_ai_results += pw_ai_results.select(result=_format_results(pw.this.docs))

        return pw_ai_results

    @pw.table_transformer
    def retrieve(self, retrieve_queries: pw.Table) -> pw.Table:
        return self.indexer.retrieve_query(retrieve_queries)

    @pw.table_transformer
    def statistics(self, statistics_queries: pw.Table) -> pw.Table:
        return self.indexer.statistics_query(statistics_queries)

    @pw.table_transformer
    def list_documents(self, list_documents_queries: pw.Table) -> pw.Table:
        return self.indexer.parsed_documents_query(list_documents_queries)

    def build_server(
        self,
        host: str,
        port: int,
        **rest_kwargs,
    ):
        warn(
            "build_server method is deprecated. Instead, use explicitly a server from pw.xpacks.llm.servers.",
            DeprecationWarning,
            stacklevel=2,
        )
        # circular import
        from pathway.xpacks.llm.servers import QARestServer

        self.server = QARestServer(host, port, self, **rest_kwargs)

    def run_server(self, *args, **kwargs):
        warn(
            "run_server method is deprecated. Instead, use explicitly a server from pw.xpacks.llm.servers.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.server is None:
            raise ValueError(
                "HTTP server is not built, initialize it with `build_server`"
            )
        self.server.run(*args, **kwargs)


def send_post_request(
    url: str, data: dict, headers: dict = {}, timeout: int | None = None
):
    response = requests.post(url, json=data, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


class RAGClient:
    """
    Connector for interacting with the Pathway RAG applications.
    Either (`host` and `port`) or `url` must be set.

    Args:
        host: The host of the RAG service.
        port: The port of the RAG service.
        url: The URL of the RAG service.
        timeout: Timeout for requests in seconds. Defaults to 90.
        additional_headers: Additional headers for the requests.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        url: str | None = None,
        timeout: int | None = 90,
        additional_headers: dict | None = None,
    ):
        err = "Either (`host` and `port`) or `url` must be provided, but not both."
        if url is not None:
            if host is not None or port is not None:
                raise ValueError(err)
            self.url = url
        else:
            if host is None:
                raise ValueError(err)
            port = port or 80

            protocol = "https" if port == 443 else "http"
            self.url = f"{protocol}://{host}:{port}"

        self.timeout = timeout
        self.additional_headers = additional_headers or {}

        self.index_client = VectorStoreClient(
            url=self.url,
            timeout=self.timeout,
            additional_headers=self.additional_headers,
        )

    def retrieve(
        self,
        query: str,
        k: int = 3,
        metadata_filter: str | None = None,
        filepath_globpattern: str | None = None,
    ):
        """
        Retrieve closest documents from the vector store based on a query.

        Args:
            query: The query string.
            k: The number of results to retrieve.
            metadata_filter: Optional metadata filter for the documents. Defaults to `None`, which
                means there will be no filter.
            filepath_globpattern: Glob pattern for file paths.
        """
        return self.index_client.query(
            query=query,
            k=k,
            metadata_filter=metadata_filter,
            filepath_globpattern=filepath_globpattern,
        )

    def statistics(
        self,
    ):
        """
        Retrieve stats from the vector store.
        """
        return self.index_client.get_vectorstore_statistics()

    def answer(
        self,
        prompt: str,
        filters: str | None = None,
        model: str | None = None,
        return_context_docs: bool | None = None,
    ) -> dict:
        """
        Return RAG answer based on a given prompt and optional filter.

        Args:
            prompt: Question to be asked.
            filters: Optional metadata filter for the documents. Defaults to ``None``, which
                means there will be no filter.
            model: Optional LLM model. If ``None``, app default will be used by the server.
        """
        api_url = f"{self.url}/v2/answer"
        payload = {
            "prompt": prompt,
        }

        if filters:
            payload["filters"] = filters

        if model:
            payload["model"] = model

        if return_context_docs is not None:
            payload["return_context_docs"] = return_context_docs  # type: ignore

        response = send_post_request(api_url, payload, self.additional_headers)
        return response

    def pw_ai_answer(self, *args, **kwargs) -> dict:
        warn(
            "`pw_ai_answer` is being deprecated, please use `answer` instead.",
            DeprecationWarning,
        )
        return self.answer(*args, **kwargs)

    def summarize(
        self,
        text_list: list[str],
        model: str | None = None,
    ):
        """
        Summarize a list of texts.

        Args:
            text_list: List of texts to summarize.
            model: Optional LLM model. If ``None``, app default will be used by the server.
        """
        api_url = f"{self.url}/v2/summarize"
        payload: dict = {
            "text_list": text_list,
        }

        if model:
            payload["model"] = model

        response = send_post_request(api_url, payload, self.additional_headers)
        return response

    def pw_ai_summary(self, *args, **kwargs):
        warn(
            "`pw_ai_summary` is being deprecated, please use `summarize` instead.",
            DeprecationWarning,
        )
        return self.summarize(*args, **kwargs)

    def list_documents(self, filters: str | None = None, keys: list[str] = ["path"]):
        """
        List indexed documents from the document store with optional filtering.

        Args:
            filters: Optional metadata filter for the documents.
            keys: List of metadata keys to be included in the response.
                Defaults to ``["path"]``. Setting to ``None`` will retrieve all available metadata.
        """
        api_url = f"{self.url}/v2/list_documents"
        payload: dict[str, Any] = {"return_status": True}

        if filters:
            payload["metadata_filter"] = filters

        response: list[dict] = send_post_request(
            api_url, payload, self.additional_headers
        )

        if response:
            if keys:
                result = [{k: v for k, v in dc.items() if k in keys} for dc in response]
            else:
                result = response
        else:
            result = []
        return result

    def pw_list_documents(self, *args, **kwargs):
        warn(
            "`pw_list_documents` is being deprecated, please use `list_documents` instead.",
            DeprecationWarning,
        )
        return self.list_documents(*args, **kwargs)

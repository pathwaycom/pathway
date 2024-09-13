import threading
from typing import Callable

import pathway as pw
from pathway.xpacks.llm.document_store import DocumentStore
from pathway.xpacks.llm.question_answering import (
    BaseQuestionAnswerer,
    SummaryQuestionAnswerer,
)


class BaseRestServer:
    def __init__(
        self,
        host: str,
        port: int,
        **rest_kwargs,
    ):
        self.webserver = pw.io.http.PathwayWebserver(host=host, port=port)

    def serve(
        self,
        route: str,
        schema: type[pw.Schema],
        handler: Callable[[pw.Table], pw.Table],
        **additional_endpoint_kwargs,
    ):

        queries, writer = pw.io.http.rest_connector(
            webserver=self.webserver,
            route=route,
            schema=schema,
            autocommit_duration_ms=50,
            delete_completed_queries=False,
            **additional_endpoint_kwargs,
        )
        writer(handler(queries))

    def run(
        self,
        threaded: bool = False,
        with_cache: bool = True,
        cache_backend: (
            pw.persistence.Backend | None
        ) = pw.persistence.Backend.filesystem("./Cache"),
        *args,
        **kwargs,
    ):
        """Start the app with cache configs. Enabling persistence will cache the embedding,
        and LLM requests between the runs."""

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
                *args,
                **kwargs,
            )

        if threaded:
            t = threading.Thread(target=run)
            t.start()
            return t
        else:
            run()


class DocumentStoreServer(BaseRestServer):
    """
    Creates a REST Server for answering queries to a given instance of ``DocumentStore``.
    It exposes three endpoints:
    - ``/v1/retrieve`` which is answered using ``retrieve`` method,
    - ``/v1/statistics`` which is answered using ``statistics`` method,
    - ``/v1/inputs`` which is answered using ``list_documents`` method,

    Args:
        - host: host on which server will run
        - port: port on which server will run
        - document_store: instance of ``DocumentStore`` which is used
            to answer queries received in the endpoints.
        - rest_kwargs: optional kwargs to be passed to ``pw.io.http.rest_connector``
    """

    def __init__(
        self,
        host: str,
        port: int,
        document_store: DocumentStore,
        **rest_kwargs,
    ):
        super().__init__(host, port, **rest_kwargs)

        rest_kwargs = {"methods": ("GET", "POST"), **rest_kwargs}

        self.serve(
            "/v1/retrieve",
            document_store.RetrieveQuerySchema,
            document_store.retrieve_query,
            **rest_kwargs,
        )
        self.serve(
            "/v1/statistics",
            document_store.StatisticsQuerySchema,
            document_store.statistics_query,
            **rest_kwargs,
        )

        self.serve(
            "/v1/inputs",
            document_store.InputsQuerySchema,
            document_store.inputs_query,
            **rest_kwargs,
        )


class QARestServer(BaseRestServer):
    """
    Creates a REST Server for answering queries to a given instance of ``BaseQuestionAnswerer``.
    It exposes four endpoints:
    - ``/v1/retrieve`` which is answered using ``retrieve`` method,
    - ``/v1/statistics`` which is answered using ``statistics`` method,
    - ``/v1/pw_list_documents`` which is answered using ``list_documents`` method,
    - ``/v1/pw_ai_answer`` which is answered using ``answer_query`` method,

    Args:
        - host: host on which server will run
        - port: port on which server will run
        - rag_question_answerer: instance of ``BaseQuestionAnswerer`` which is used
            to answer queries received in the endpoints.
        - rest_kwargs: optional kwargs to be passed to ``pw.io.http.rest_connector``
    """

    def __init__(
        self,
        host: str,
        port: int,
        rag_question_answerer: BaseQuestionAnswerer,
        **rest_kwargs,
    ):
        super().__init__(host, port, **rest_kwargs)

        self.serve(
            "/v1/retrieve",
            rag_question_answerer.RetrieveQuerySchema,
            rag_question_answerer.retrieve,
            **rest_kwargs,
        )
        self.serve(
            "/v1/statistics",
            rag_question_answerer.StatisticsQuerySchema,
            rag_question_answerer.statistics,
            **rest_kwargs,
        )

        self.serve(
            "/v1/pw_list_documents",
            rag_question_answerer.InputsQuerySchema,
            rag_question_answerer.list_documents,
            **rest_kwargs,
        )
        self.serve(
            "/v1/pw_ai_answer",
            rag_question_answerer.AnswerQuerySchema,
            rag_question_answerer.answer_query,
            **rest_kwargs,
        )


class QASummaryRestServer(QARestServer):
    """
    Creates a REST Server for answering queries to a given instance of ``SummaryQuestionAnswerer``.
    It exposes five endpoints:
    - ``/v1/retrieve`` which is answered using ``retrieve`` method,
    - ``/v1/statistics`` which is answered using ``statistics`` method,
    - ``/v1/pw_list_documents`` which is answered using ``list_documents`` method,
    - ``/v1/pw_ai_answer`` which is answered using ``answer`` method,
    - ``/v1/pw_ai_summary`` which is answered using ``summarize_query`` method.

    Args:
        - host: host on which server will run
        - port: port on which server will run
        - rag_question_answerer: instance of ``SummaryQuestionAnswerer`` which is used
            to answer queries received in the endpoints.
        - rest_kwargs: optional kwargs to be passed to ``pw.io.http.rest_connector``
    """

    def __init__(
        self,
        host: str,
        port: int,
        rag_question_answerer: SummaryQuestionAnswerer,
        **rest_kwargs,
    ):
        super().__init__(host, port, rag_question_answerer, **rest_kwargs)

        self.serve(
            "/v1/pw_ai_summary",
            rag_question_answerer.SummarizeQuerySchema,
            rag_question_answerer.summarize_query,
            **rest_kwargs,
        )

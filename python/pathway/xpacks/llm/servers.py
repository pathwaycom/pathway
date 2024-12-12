import threading
from typing import Callable

import pathway as pw
from pathway.internals import udfs
from pathway.internals.udfs.utils import coerce_async
from pathway.xpacks.llm.document_store import DocumentStore
from pathway.xpacks.llm.question_answering import (
    BaseQuestionAnswerer,
    SummaryQuestionAnswerer,
)

from ._utils import get_func_arg_names


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
        **kwargs,
    ):
        """
        Start the server. Enabling persistence will cache the UDFs
        for which ``cache_strategy`` is set.

        Args:
            threaded: if True, the server will be run in a new thread.
            with_cache: if True, caching will be enabled for the UDFs for which ``cache_strategy``
                is set.
            cache_backend: backend used for caching. Only relevant if ``with_cache`` is
                set to True.
            **kwargs: optional kwargs to be passed to ``pw.run``.
        """

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
        host: host on which server will run
        port: port on which server will run
        document_store: instance of ``DocumentStore`` which is used
            to answer queries received in the endpoints.
        rest_kwargs: optional kwargs to be passed to ``pw.io.http.rest_connector``
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
        host: host on which server will run
        port: port on which server will run
        rag_question_answerer: instance of ``BaseQuestionAnswerer`` which is used
            to answer queries received in the endpoints.
        rest_kwargs: optional kwargs to be passed to ``pw.io.http.rest_connector``
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
        host: host on which server will run
        port: port on which server will run
        rag_question_answerer: instance of ``SummaryQuestionAnswerer`` which is used
            to answer queries received in the endpoints.
        rest_kwargs: optional kwargs to be passed to ``pw.io.http.rest_connector``
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

    def serve_callable(
        self,
        route: str,
        schema: type[pw.Schema] | None,
        callable_func: Callable,
        retry_strategy: udfs.AsyncRetryStrategy | None,
        cache_strategy: udfs.CacheStrategy | None,
        **additional_endpoint_kwargs,
    ):
        if schema is None:
            args = get_func_arg_names(callable_func)
            schema_type_maps = {i: pw.PyObjectWrapper for i in args}
            schema = pw.schema_from_types(**schema_type_maps)

        def func_to_transformer(fn):
            HTTP_CONN_RESPONSE_KEY = "result"

            async_fn = coerce_async(fn)

            class FuncAsyncTransformer(
                pw.AsyncTransformer, output_schema=pw.schema_from_types(result=dict)
            ):

                async def invoke(self, *args, **kwargs) -> dict:
                    args = tuple(
                        (
                            arg.value
                            if isinstance(arg, (pw.Json, pw.PyObjectWrapper))
                            else arg
                        )
                        for arg in args
                    )
                    kwargs = {
                        k: (
                            v.value
                            if isinstance(v, (pw.Json, pw.PyObjectWrapper))
                            else v
                        )
                        for k, v in kwargs.items()
                    }

                    result = await async_fn(*args, **kwargs)

                    return {HTTP_CONN_RESPONSE_KEY: result}

            def table_transformer(table: pw.Table) -> pw.Table:
                return (
                    FuncAsyncTransformer(input_table=table)
                    .with_options(
                        cache_strategy=cache_strategy,
                        retry_strategy=retry_strategy,
                    )
                    .successful
                )

            return table_transformer

        self.serve(
            route,
            schema,
            handler=func_to_transformer(callable_func),
            **additional_endpoint_kwargs,
        )

        return callable_func

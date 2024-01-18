# Copyright © 2024 Pathway

import asyncio
import json
import logging
import threading
from collections.abc import Callable
from typing import Any
from uuid import uuid4
from warnings import warn

from aiohttp import web

import pathway.internals as pw
import pathway.io as io
from pathway.internals.api import Pointer, unsafe_make_pointer
from pathway.internals.runtime_type_check import check_arg_types


class PathwayWebserver:
    """
    The basic configuration class for ``pw.io.http.rest_connector``.

    It contains essential information about the host and the port on which the
    webserver should run and accept queries.

    Args:
        host: TCP/IP host or a sequence of hosts for the created endpoint.
        port: Port for the created endpoint.
    """

    _host: str
    _port: int
    _tasks: dict[Any, Any]
    _loop: asyncio.AbstractEventLoop
    _app: web.Application
    _is_launched: bool

    def __init__(self, host, port):
        self._host = host
        self._port = port

        self._tasks = {}
        self._loop = asyncio.new_event_loop()
        self._app = web.Application()
        self._is_launched = False
        self._app_start_mutex = threading.Lock()

    def _register_endpoint(self, route, handler) -> None:
        self._app.add_routes([web.post(route, handler)])

    def _run(self) -> None:
        self._app_start_mutex.acquire()
        if not self._is_launched:
            self._is_launched = True
            self._app_start_mutex.release()
            web.run_app(
                self._app,
                host=self._host,
                port=self._port,
                loop=self._loop,
                handle_signals=False,
            )
        else:
            self._app_start_mutex.release()


class RestServerSubject(io.python.ConnectorSubject):
    _webserver: PathwayWebserver
    _schema: type[pw.Schema]
    _delete_completed_queries: bool
    _format: str

    def __init__(
        self,
        webserver: PathwayWebserver,
        route: str,
        schema: type[pw.Schema],
        delete_completed_queries: bool,
        format: str = "raw",
    ) -> None:
        super().__init__()
        self._webserver = webserver
        self._tasks = webserver._tasks
        self._schema = schema
        self._delete_completed_queries = delete_completed_queries
        self._format = format

        webserver._register_endpoint(route, self.handle)

    def run(self):
        self._webserver._run()

    async def handle(self, request: web.Request):
        id = unsafe_make_pointer(uuid4().int)

        if self._format == "raw":
            payload = {"query": await request.text()}
        elif self._format == "custom":
            try:
                payload = await request.json()
                query_params = request.query
                for param, value in query_params.items():
                    if param not in payload:
                        payload[param] = value
            except json.decoder.JSONDecodeError:
                raise web.HTTPBadRequest(reason="payload is not a valid json")

        self._verify_payload(payload)

        event = asyncio.Event()
        data = json.dumps(payload).encode()

        self._tasks[id] = {
            "event": event,
            "result": "-PENDING-",
        }

        self._add(id, data)
        response = await self._fetch_response(id, event)
        if self._delete_completed_queries:
            self._remove(id, data)
        return web.json_response(status=200, data=response, dumps=pw.Json.dumps)

    async def _fetch_response(self, id, event) -> Any:
        await event.wait()
        task = self._tasks.pop(id)
        return task["result"]

    def _verify_payload(self, payload: dict):
        defaults = self._schema.default_values()
        for column in self._schema.keys():
            if column not in payload and column not in defaults:
                raise web.HTTPBadRequest(reason=f"`{column}` is required")

    @property
    def _deletions_enabled(self) -> bool:
        return self._delete_completed_queries

    def _is_finite(self):
        return False


@check_arg_types
def rest_connector(
    host: str | None = None,
    port: int | str | None = None,
    *,
    webserver: PathwayWebserver | None = None,
    route: str = "/",
    schema: type[pw.Schema] | None = None,
    autocommit_duration_ms=1500,
    keep_queries: bool | None = None,
    delete_completed_queries: bool | None = None,
) -> tuple[pw.Table, Callable]:
    """
    Runs a lightweight HTTP server and inputs a collection from the HTTP endpoint,
    configured by the parameters of this method.

    On the output, the method provides a table and a callable, which needs to accept
    the result table of the computation, which entries will be tracked and put into
    respective request's responses.

    Args:
        webserver: configuration object containing host and port information. You only \
need to create only one instance of this class per single host-port pair;
        route: route which will be listened to by the web server;
        schema: schema of the resulting table;
        autocommit_duration_ms: the maximum time between two commits. Every
          autocommit_duration_ms milliseconds, the updates received by the connector are
          committed and pushed into Pathway's computation graph;
        keep_queries: whether to keep queries after processing; defaults to False. [deprecated]
        delete_completed_queries: whether to send a deletion entry after the query is processed.
          Allows to remove it from the system if it is stored by operators such as ``join`` or ``groupby``;

    Returns:
        table: the table read;
        response_writer: a callable, where the result table should be provided. The \
result table must contain columns `query_id` corresponding to the primary key of an \
object from the input table and `result`, corresponding to the endpoint's return value.

    Example:

    Let's consider the following example: there is a collection of words that are \
received through HTTP REST endpoint `/uppercase` located at `127.0.0.1`, port `9999`. \
The Pathway program processes this table by converting these words to the upper case. \
This conversion result must be provided to the user on the output.

    Then, you can proceed with the following REST connector configuration code.

    First, the schema and the webserver object need to be created:

    >>> import pathway as pw
    >>> class WordsSchema(pw.Schema):
    ...     word: str
    ...
    >>>
    >>> webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=9999)

    Then, the endpoint that inputs this collection can be configured:

    >>> words, response_writer = pw.io.http.rest_connector(
    ...     webserver=webserver,
    ...     route="/uppercase",
    ...     schema=WordsSchema,
    ... )

    Finally, you can define the logic that takes the input table `words`, calculates
    the result in the form of a table, and provides it for the endpoint's output:

    >>> uppercase_words = words.select(
    ...     query_id=words.id,
    ...     result=pw.apply(lambda x: x.upper(), pw.this.word)
    ... )
    >>> response_writer(uppercase_words)

    Please note that you don't need to create another web server object if you need to \
have more than one endpoint running on the same host and port. For example, if you need \
to create another endpoint that converts words to lower case, in the same way, you \
need to reuse the existing `webserver` object. That is, the configuration would start \
with:

    >>> words_for_lowercase, response_writer_for_lowercase = pw.io.http.rest_connector(
    ...     webserver=webserver,
    ...     route="/lowercase",
    ...     schema=WordsSchema,
    ... )
    """

    if delete_completed_queries is None:
        if keep_queries is None:
            warn(
                "delete_completed_queries arg of rest_connector should be set explicitly."
                + " It will soon be required."
            )
            delete_completed_queries = True
        else:
            warn(
                "DEPRECATED: keep_queries arg of rest_connector is deprecated,"
                + " use delete_completed_queries with an opposite meaning instead."
            )
            delete_completed_queries = not keep_queries

    if schema is None:
        format = "raw"
        schema = pw.schema_builder({"query": pw.column_definition()})
    else:
        format = "custom"

    if webserver is None:
        if host is None or port is None:
            raise ValueError(
                "If webserver object isn't specified, host and port must be present"
            )
        if isinstance(port, str):
            port = int(port)
        # warn(
        #    "The `host` and `port` arguments are deprecated. Please use `webserver` "
        #    "instead.",
        #    DeprecationWarning,
        #    stacklevel=2,
        # )
        webserver = PathwayWebserver(host, port)
    else:
        if host is not None or port is not None:
            raise ValueError(
                "If webserver object is specified, host and port shouldn't be set"
            )

    tasks = webserver._tasks
    input_table = io.python.read(
        subject=RestServerSubject(
            webserver=webserver,
            route=route,
            schema=schema,
            delete_completed_queries=delete_completed_queries,
            format=format,
        ),
        schema=schema,
        format="json",
        autocommit_duration_ms=autocommit_duration_ms,
    )

    def response_writer(responses: pw.Table):
        def on_change(key: Pointer, row: dict[str, Any], time: int, is_addition: bool):
            if not is_addition:
                return

            task = tasks.get(key, None)

            if task is None:
                if delete_completed_queries:
                    logging.info(
                        "Query response has changed. It probably indicates an error in the pipeline."
                    )
                return

            def set_task():
                task["result"] = row["result"]
                task["event"].set()

            webserver._loop.call_soon_threadsafe(set_task)

        io.subscribe(table=responses, on_change=on_change)

    return input_table, response_writer

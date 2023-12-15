# Copyright © 2023 Pathway

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any
from uuid import uuid4
from warnings import warn

from aiohttp import web

import pathway.internals as pw
import pathway.io as io
from pathway.internals.api import Pointer, unsafe_make_pointer


class RestServerSubject(io.python.ConnectorSubject):
    _host: str
    _port: int
    _loop: asyncio.AbstractEventLoop
    _delete_completed_queries: bool

    def __init__(
        self,
        host: str,
        port: int,
        route: str,
        loop: asyncio.AbstractEventLoop,
        tasks: dict[Any, Any],
        schema: type[pw.Schema],
        delete_completed_queries: bool,
        format: str = "raw",
    ) -> None:
        super().__init__()
        self._host = host
        self._port = port
        self._route = route
        self._loop = loop
        self._tasks = tasks
        self._schema = schema
        self._delete_completed_queries = delete_completed_queries
        self._format = format

    def run(self):
        app = web.Application()
        app.add_routes([web.post(self._route, self.handle)])
        web.run_app(
            app, host=self._host, port=self._port, loop=self._loop, handle_signals=False
        )

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


def rest_connector(
    host: str,
    port: int,
    *,
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
        host: TCP/IP host or a sequence of hosts for the created endpoint;
        port: port for the created endpoint;
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
        response_writer: a callable, where the result table should be provided.
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

    loop = asyncio.new_event_loop()
    tasks: dict[Any, Any] = {}

    if schema is None:
        format = "raw"
        schema = pw.schema_builder({"query": pw.column_definition()})
    else:
        format = "custom"

    input_table = io.python.read(
        subject=RestServerSubject(
            host=host,
            port=port,
            route=route,
            loop=loop,
            tasks=tasks,
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

            loop.call_soon_threadsafe(set_task)

        io.subscribe(table=responses, on_change=on_change)

    return input_table, response_writer

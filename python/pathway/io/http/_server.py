# Copyright © 2023 Pathway

import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional, Tuple, Type
from uuid import uuid4

from aiohttp import web

import pathway.internals as pw
import pathway.io as io
from pathway.internals.api import BasePointer, unsafe_make_pointer


class RestServerSubject(io.python.ConnectorSubject):
    _host: str
    _port: int
    _loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        host: str,
        port: int,
        loop: asyncio.AbstractEventLoop,
        tasks: Dict[Any, Any],
        schema: Type[pw.Schema],
        format: str = "raw",
    ) -> None:
        super().__init__()
        self._host = host
        self._port = port
        self._loop = loop
        self._tasks = tasks
        self._schema = schema
        self._format = format

    def run(self):
        app = web.Application()
        app.add_routes([web.post("/", self.handle)])
        web.run_app(
            app, host=self._host, port=self._port, loop=self._loop, handle_signals=False
        )

    async def handle(self, request):
        id = unsafe_make_pointer(uuid4().int)

        if self._format == "raw":
            payload = {"query": await request.text()}
        elif self._format == "custom":
            try:
                payload = await request.json()
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
        self._remove(id, data)
        return web.json_response(status=200, data=response)

    async def _fetch_response(self, id, event) -> Any:
        await event.wait()
        task = self._tasks.pop(id)
        return task["result"]

    def _verify_payload(self, payload: dict):
        for column in self._schema.keys():
            if column not in payload:
                raise web.HTTPBadRequest(reason=f"`{column}` is required")


def rest_connector(
    host: str,
    port: int,
    schema: Optional[Type[pw.Schema]] = None,
    autocommit_duration_ms=1500,
) -> Tuple[pw.Table, Callable]:
    loop = asyncio.new_event_loop()
    tasks: Dict[Any, Any] = {}

    if schema is None:
        format = "raw"
        schema = pw.schema_builder({"query": pw.column_definition()})
    else:
        format = "custom"

    input_table = io.python.read(
        subject=RestServerSubject(
            host=host, port=port, loop=loop, tasks=tasks, schema=schema, format=format
        ),
        schema=schema,
        format="json",
        autocommit_duration_ms=autocommit_duration_ms,
    )

    def response_writer(responses: pw.Table):
        def on_change(
            key: BasePointer, row: Dict[str, Any], time: int, is_addition: bool
        ):
            if not is_addition:
                return

            task = tasks.get(key, None)

            if task is None:
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

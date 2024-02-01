# Copyright © 2024 Pathway

import asyncio
import json
import logging
import threading
from collections.abc import Callable
from typing import Any, Sequence
from uuid import uuid4
from warnings import warn

import aiohttp_cors
import yaml
from aiohttp import web

import pathway.internals as pw
import pathway.io as io
from pathway.internals import api
from pathway.internals.api import Pointer, unsafe_make_pointer
from pathway.internals.dtype import unoptionalize
from pathway.internals.runtime_type_check import check_arg_types

# There is no OpenAPI type for 'any', so it's excluded from the dict
# 'object' and 'array' are excluded because there is schema for the nested
# objects or array items
_ENGINE_TO_OPENAPI_TYPE = {
    api.PathwayType.INT: "number",
    api.PathwayType.STRING: "string",
    api.PathwayType.BOOL: "boolean",
    api.PathwayType.FLOAT: "number",
    api.PathwayType.POINTER: "string",
    api.PathwayType.DATE_TIME_NAIVE: "string",
    api.PathwayType.DATE_TIME_UTC: "string",
    api.PathwayType.DURATION: "string",
    api.PathwayType.BYTES: "bytes",
}

_ENGINE_TO_OPENAPI_FORMAT = {
    api.PathwayType.INT: "int64",
    api.PathwayType.FLOAT: "double",
}

# Which column in schema corresponds to the payload when the
# input format for the endpoint is 'raw'
QUERY_SCHEMA_COLUMN = "query"


class PathwayWebserver:
    """
    The basic configuration class for ``pw.io.http.rest_connector``.

    It contains essential information about the host and the port on which the
    webserver should run and accept queries.

    Args:
        host: TCP/IP host or a sequence of hosts for the created endpoint.
        port: Port for the created endpoint.
        with_schema_endpoint: If set to True, the server will also provide ``/_schema`` \
endpoint containing Open API 3.0.3 schema for the handlers generated with \
``pw.io.http.rest_connector`` calls.
        with_cors: If set to True, the server will allow cross-origin requests on the \
added endpoints.
    """

    _host: str
    _port: int
    _tasks: dict[Any, Any]
    _loop: asyncio.AbstractEventLoop
    _app: web.Application
    _is_launched: bool

    def __init__(self, host, port, with_schema_endpoint=True, with_cors=False):
        self._host = host
        self._port = port

        self._tasks = {}
        self._loop = asyncio.new_event_loop()
        self._app = web.Application()
        self._registered_routes = {}
        if with_cors:
            self._cors = aiohttp_cors.setup(self._app)
        else:
            self._cors = None
        self._is_launched = False
        self._app_start_mutex = threading.Lock()
        self._openapi_description = {
            "openapi": "3.0.3",
            "info": {
                "title": "Pathway-generated openapi description",
                "version": "1.0.0",
            },
            "paths": {},
            "servers": [{"url": f"http://{host}:{port}/"}],
        }
        if with_schema_endpoint:
            self._add_endpoint_to_app("GET", "/_schema", self._schema_handler)

    def _add_endpoint_to_app(self, method, route, handler):
        if route not in self._registered_routes:
            app_resource = self._app.router.add_resource(route)
            if self._cors is not None:
                app_resource = self._cors.add(app_resource)
            self._registered_routes[route] = app_resource

        app_resource_endpoint = self._registered_routes[route].add_route(
            method, handler
        )
        if self._cors is not None:
            self._cors.add(
                app_resource_endpoint,
                {
                    "*": aiohttp_cors.ResourceOptions(
                        expose_headers="*", allow_headers="*"
                    )
                },
            )

    async def _schema_handler(self, request: web.Request):
        format = request.query.get("format", "yaml")
        if format == "json":
            return web.json_response(
                status=200,
                data=self.openapi_description_json,
                dumps=pw.Json.dumps,
            )
        elif format != "yaml":
            raise web.HTTPBadRequest(
                reason=f"Unknown format: '{format}'. Supported formats: 'json', 'yaml'"
            )

        return web.Response(
            status=200,
            text=self.openapi_description,
            content_type="text/x-yaml",
        )

    def _construct_openapi_plaintext_schema(self, schema) -> dict:
        query_column = schema.columns().get(QUERY_SCHEMA_COLUMN)
        if query_column is None:
            raise ValueError(
                "'raw' endpoint input format requires 'value' column in schema"
            )
        openapi_type = _ENGINE_TO_OPENAPI_TYPE.get(query_column, "string")
        openapi_format = _ENGINE_TO_OPENAPI_FORMAT.get(query_column)
        description = {
            "type": openapi_type,
        }
        if openapi_format:
            description["format"] = openapi_format
        if query_column.has_default_value():
            description["default"] = query_column.default_value

        return description

    def _construct_openapi_get_request_schema(self, schema) -> list:
        parameters = []
        for name, props in schema.columns().items():
            field_description = {
                "in": "query",
                "name": name,
                "required": not props.has_default_value(),
            }
            openapi_type = _ENGINE_TO_OPENAPI_TYPE.get(props.dtype.map_to_engine())
            if openapi_type:
                field_description["schema"] = {
                    "type": openapi_type,
                }
            parameters.append(field_description)

        return parameters

    def _construct_openapi_json_schema(self, schema) -> dict:
        properties = {}
        required = []
        additional_properties = False

        for name, props in schema.columns().items():
            openapi_type = _ENGINE_TO_OPENAPI_TYPE.get(props.dtype.map_to_engine())
            if openapi_type is None:
                # not something we can clearly define the type for, so it will be
                # read as an additional property
                additional_properties = True
                continue

            field_description = {
                "type": openapi_type,
            }

            if not props.has_default_value():
                required.append(name)
            else:
                field_description["default"] = props.default_value

            openapi_format = _ENGINE_TO_OPENAPI_FORMAT.get(props.dtype.map_to_engine())
            if openapi_format is not None:
                field_description["format"] = openapi_format

            properties[name] = field_description

        result = {
            "type": "object",
            "properties": properties,
            "additionalProperties": additional_properties,
        }
        if required:
            result["required"] = required

        return result

    def _register_endpoint(self, route, handler, format, schema, methods) -> None:
        simple_responses_dict = {
            "200": {
                "description": "OK",
            },
            "400": {
                "description": "The request is incorrect. Please check if "
                "it complies with the auto-generated and Pathway input "
                "table schemas"
            },
        }

        self._openapi_description["paths"][route] = {}  # type: ignore[index]
        for method in methods:
            self._add_endpoint_to_app(method, route, handler)

            if method == "GET":
                content = {
                    "parameters": self._construct_openapi_get_request_schema(schema),
                    "responses": simple_responses_dict,
                }
            elif format == "raw":
                content = {
                    "text/plain": {
                        "schema": self._construct_openapi_plaintext_schema(schema)
                    }
                }
            elif format == "custom":
                content = {
                    "application/json": {
                        "schema": self._construct_openapi_json_schema(schema)
                    }
                }
            else:
                raise ValueError(f"Unknown endpoint input format: {format}")

            if method != "GET":
                self._openapi_description["paths"][route][method.lower()] = {  # type: ignore[index]
                    "requestBody": {
                        "content": content,
                    },
                    "responses": simple_responses_dict,
                }
            else:
                self._openapi_description["paths"][route]["get"] = content  # type: ignore[index]

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

    @property
    def openapi_description_json(self) -> dict:
        """
        Returns Open API description for the added set of endpoints in JSON format.
        """
        return self._openapi_description

    @property
    def openapi_description(self):
        """
        Returns Open API description for the added set of endpoints in yaml format.
        """
        return yaml.dump(self.openapi_description_json)


class RestServerSubject(io.python.ConnectorSubject):
    _webserver: PathwayWebserver
    _schema: type[pw.Schema]
    _delete_completed_queries: bool
    _format: str

    def __init__(
        self,
        webserver: PathwayWebserver,
        route: str,
        methods: Sequence[str],
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

        webserver._register_endpoint(route, self.handle, format, schema, methods)

    def run(self):
        self._webserver._run()

    async def handle(self, request: web.Request):
        id = unsafe_make_pointer(uuid4().int)

        if self._format == "raw":
            payload = {QUERY_SCHEMA_COLUMN: await request.text()}
        elif self._format == "custom":
            try:
                payload = await request.json()
            except json.decoder.JSONDecodeError:
                payload = {}
            query_params = request.query
            for param, value in query_params.items():
                if param not in payload:
                    payload[param] = value

        self._verify_payload(payload)
        self._cast_types_to_schema(payload)

        event = asyncio.Event()
        data = pw.Json.dumps(payload).encode()

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

    def _cast_types_to_schema(self, payload: dict):
        dtypes = self._schema._dtypes()
        for column, dtype in dtypes.items():
            if column not in payload:
                continue
            try:
                exact_type = unoptionalize(dtype).typehint
                payload[column] = exact_type(payload[column])
            except Exception:
                logging.exception(
                    f"Failed to cast column '{column}' to type '{exact_type}'"
                )

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
    methods: Sequence[str] = ("POST",),
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
        methods: HTTP methods that this endpoint will accept;
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
        warn(
            "The `host` and `port` arguments are deprecated. Please use `webserver` "
            "instead.",
            DeprecationWarning,
            stacklevel=2,
        )
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
            methods=methods,
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

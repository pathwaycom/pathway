# Copyright © 2024 Pathway

import asyncio
import copy
import json
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
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
from pathway.internals.table_subscription import subscribe as internal_subscribe
from pathway.internals.udfs.caches import CacheStrategy

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


class _LoggingContext:
    def __init__(self, request: web.Request, session_id: str):
        self.log = {
            "_type": "http_access",
            "method": request.method,
            "scheme": request.scheme,
            "scheme_with_forwarded": _request_scheme(request),
            "host": request.host,
            "route": str(request.rel_url),
            "content_type": request.headers.get("Content-Type"),
            "user_agent": request.headers.get("User-Agent"),
            "unix_timestamp": int(time.time()),
            "remote": request.remote,
            "session_id": session_id,
        }
        headers = []
        for header, value in request.headers.items():
            headers.append(
                {
                    "header": header,
                    "value": value,
                }
            )
        self.log["headers"] = headers
        self.request_start = time.time()

    def log_response(self, status: int):
        self.log["status"] = status
        time_elapsed = time.time() - self.request_start
        self.log["time_elapsed"] = "{:.3f}".format(time_elapsed)
        if status < 400:
            logging.info(json.dumps(self.log))
        else:
            logging.error(json.dumps(self.log))


class EndpointExamples:
    """
    Examples for endpoint documentation.
    """

    def __init__(self):
        self.examples_by_id = {}

    def add_example(self, id, summary, values):
        """
        Adds an example to the collection.

        Args:
            id: Short and unique ID for the example. It is used for naming the example
                within the Open API schema. By using `default` as an ID, you can set the example
                default for the readers, while users will be able to select another ones via the
                dropdown menu.
            summary: Human-readable summary of the example, describing what is shown.
                It is shown in the automatically generated dropdown menu.
            values: The key-value dictionary, a mapping from the fields described in
                schema to their values in the example.

        Returns:
            None
        """
        if id in self.examples_by_id:
            raise ValueError(f"Duplicate example id: {id}")
        self.examples_by_id[id] = {
            "summary": summary,
            "value": values,
        }
        return self

    def _openapi_description(self):
        return self.examples_by_id


class EndpointDocumentation:
    """
    The settings for the automatic OpenAPI v3 docs generation for an endpoint.

    Args:
        summary: Short endpoint description shown as a hint in the endpoints list.
        description: Comprehensive description for the endpoint.
        tags: Tags for grouping the endpoints.
        method_types: If set, Pathway will document only the given method types. This
            way, one can exclude certain endpoints and methods from being documented.
    """

    DEFAULT_RESPONSES_DESCRIPTION = {
        "200": {
            "description": "OK",
        },
        "400": {
            "description": "The request is incorrect. Please check if "
            "it complies with the auto-generated and Pathway input "
            "table schemas"
        },
    }

    def __init__(
        self,
        *,
        summary: str | None = None,
        description: str | None = None,
        tags: Sequence[str] | None = None,
        method_types: Sequence[str] | None = None,
        examples: EndpointExamples | None = None,
    ):
        self.summary = summary
        self.description = description
        self.tags = tags
        self.method_types = None
        if method_types is not None:
            self.method_types = set([x.upper() for x in method_types])
        self.examples = examples

    def generate_docs(self, format, method, schema) -> dict:
        if not self._is_method_exposed(method):
            return {}
        if method.upper() == "GET":
            # Get requests receive parameters from CGI, so their schema description
            # is a bit different from the POST / PUT / PATCH
            endpoint_description = {
                "parameters": self._construct_openapi_get_request_schema(schema),
                # disable yaml optimization to avoid
                # "instance type (string) does not match any allowed primitive type"
                # error from openapi validator
                "responses": copy.deepcopy(self.DEFAULT_RESPONSES_DESCRIPTION),
            }
        else:
            if format == "raw":
                content_header = "text/plain"
                openapi_schema = self._construct_openapi_plaintext_schema(schema)
            elif format == "custom":
                content_header = "application/json"
                openapi_schema = self._construct_openapi_json_schema(schema)
            else:
                raise ValueError(f"Unknown endpoint input format: {format}")
            schema_and_examples = {"schema": openapi_schema}
            if self.examples:
                schema_and_examples["examples"] = self.examples._openapi_description()
            content_description = {content_header: schema_and_examples}
            endpoint_description = {
                "requestBody": {
                    "content": content_description,
                },
                # disable yaml optimization to avoid
                # "instance type (string) does not match any allowed primitive type"
                # error from openapi validator
                "responses": copy.deepcopy(self.DEFAULT_RESPONSES_DESCRIPTION),
            }

        if self.tags is not None:
            endpoint_description["tags"] = list(self.tags)
        if self.description is not None:
            endpoint_description["description"] = self.description
        if self.summary is not None:
            endpoint_description["summary"] = self.summary

        return {method.lower(): endpoint_description}

    def _is_method_exposed(self, method):
        return self.method_types is None or method.upper() in self.method_types

    def _add_optional_traits_if_present(self, field_description, props):
        if props.example is not None:
            field_description["example"] = props.example
        if props.description is not None:
            field_description["description"] = props.description

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
        self._add_optional_traits_if_present(description, query_column)

        return description

    def _construct_openapi_get_request_schema(self, schema) -> list:
        parameters = []
        for name, props in schema.columns().items():
            field_description = {
                "in": "query",
                "name": name,
                "required": not props.has_default_value(),
            }
            self._add_optional_traits_if_present(field_description, props)
            openapi_type = _ENGINE_TO_OPENAPI_TYPE.get(
                unoptionalize(props.dtype).to_engine()
            )
            if openapi_type:
                field_description["schema"] = {
                    "type": openapi_type,
                }
            else:
                # Get request params without type make schema invalid
                field_description["schema"] = {"type": "string"}
            parameters.append(field_description)

        return parameters

    def _construct_openapi_json_schema(self, schema) -> dict:
        properties = {}
        required = []
        additional_properties = False

        for name, props in schema.columns().items():
            openapi_type = _ENGINE_TO_OPENAPI_TYPE.get(
                unoptionalize(props.dtype).to_engine()
            )
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

            self._add_optional_traits_if_present(field_description, props)
            openapi_format = _ENGINE_TO_OPENAPI_FORMAT.get(props.dtype.to_engine())
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


def _request_scheme(request: web.Request):
    """
    Get request scheme taking into account the forwarded headers.
    """
    scheme_headers = [
        "X-Forwarded-Proto",
        "X-Scheme",
        "X-Forwarded-Scheme",
    ]
    request_schemes = [
        "http",
        "https",
    ]
    for header in scheme_headers:
        header_value = request.headers.get(header)
        if header_value is None:
            continue
        header_value = header_value.lower()
        if header_value in request_schemes:
            return header_value

    # fallback, doesn't work for forwarded scenarios
    return request.scheme


class PathwayWebserver:
    """
    The basic configuration class for ``pw.io.http.rest_connector``.

    It contains essential information about the host and the port on which the
    webserver should run and accept queries.

    Args:
        host: TCP/IP host or a sequence of hosts for the created endpoint.
        port: Port for the created endpoint.
        with_schema_endpoint: If set to True, the server will also provide ``/_schema``
            endpoint containing Open API 3.0.3 schema for the handlers generated with
            ``pw.io.http.rest_connector`` calls.
        with_cors: If set to True, the server will allow cross-origin requests on the
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
        self._openapi_description = OrderedDict(
            {
                "openapi": "3.0.3",
                "info": {
                    "title": "Pathway-generated openapi description",
                    "version": "1.0.0",
                },
                "paths": {},
                "servers": [{"url": f"http://{host}:{port}/"}],
            }
        )
        if with_schema_endpoint:
            self._add_endpoint_to_app("GET", "/_schema", self._schema_handler)

    def _add_endpoint_to_app(self, method, route, handler):
        handler = self._wrap_handler_with_logger(handler)
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

    def _wrap_handler_with_logger(
        self, handler_method: Callable[[web.Request], Awaitable[web.Response]]
    ):
        async def wrapped_handler(request: web.Request):
            session_id = "uuid-" + str(uuid4())
            logging_context = _LoggingContext(request, session_id)
            try:
                headers = request.headers.copy()  # type:ignore
                headers["X-Pathway-Session"] = session_id
                request = request.clone(headers=headers)
                response = await handler_method(request)
            except web.HTTPError as http_error:
                logging_context.log_response(status=http_error.status_code)
                raise
            except Exception:
                logging.exception("Error in HTTP handler")
                # the server framework translates all non-native
                # exceptions into responses with code 500 so we use it
                logging_context.log_response(status=500)
                raise
            logging_context.log_response(response.status)
            return response

        return wrapped_handler

    async def _schema_handler(self, request: web.Request):
        origin = f"{_request_scheme(request)}://{request.host}"
        format = request.query.get("format", "yaml")
        if format == "json":
            return web.json_response(
                status=200,
                data=self.openapi_description_json(origin),
                dumps=pw.Json.dumps,
            )
        elif format != "yaml":
            raise web.HTTPBadRequest(
                reason=f"Unknown format: '{format}'. Supported formats: 'json', 'yaml'"
            )

        return web.Response(
            status=200,
            text=self.openapi_description(origin),
            content_type="text/x-yaml",
        )

    def _register_endpoint(
        self, route, handler, format, schema, methods, documentation
    ) -> None:
        endpoint_docs = {}
        for method in methods:
            self._add_endpoint_to_app(method, route, handler)
            method_docs = documentation.generate_docs(format, method, schema)
            if method_docs:
                endpoint_docs.update(method_docs)
        if endpoint_docs:
            self._openapi_description["paths"][route] = endpoint_docs  # type: ignore[index]

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

    def openapi_description_json(self, origin) -> dict:
        """
        Returns Open API description for the added set of endpoints in JSON format.
        """
        result = copy.deepcopy(self._openapi_description)
        result["servers"] = [{"url": origin}]

        return result

    def openapi_description(self, origin):
        """
        Returns Open API description for the added set of endpoints in yaml format.
        """
        return yaml.dump(dict(self.openapi_description_json(origin)), sort_keys=False)


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
        request_validator: Callable | None = None,
        documentation: EndpointDocumentation = EndpointDocumentation(),
        cache_strategy: CacheStrategy | None = None,
    ) -> None:
        super().__init__(datasource_name="rest-connector")
        self._webserver = webserver
        self._tasks = webserver._tasks
        self._schema = schema
        self._delete_completed_queries = delete_completed_queries
        self._format = format
        self._request_validator = request_validator
        self._cache_strategy = cache_strategy
        self._request_processor = self._create_request_processor()

        webserver._register_endpoint(
            route, self.handle, format, schema, methods, documentation
        )

    def run(self):
        self._webserver._run()

    async def handle(self, request: web.Request):
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
        logging.info(
            json.dumps(
                {
                    "_type": "request_payload",
                    "session_id": request.headers.get("X-Pathway-Session"),
                    "payload": payload,
                }
            )
        )
        self._verify_payload(payload)
        if self._request_validator:
            try:
                validator_ret = self._request_validator(payload, request.headers)
                if validator_ret is not None:
                    raise Exception(validator_ret)
            except Exception as e:
                record = {
                    "_type": "validator_rejected_http_request",
                    "error": str(e),
                    "payload": payload,
                }
                logging.error(json.dumps(record))
                raise web.HTTPBadRequest(reason=str(e))

        result = await self._request_processor(
            json.dumps(payload, sort_keys=True, ensure_ascii=False)
        )
        return copy.deepcopy(result)

    def _create_request_processor(self):
        async def inner(payload_encoded: str) -> web.Response:
            id = unsafe_make_pointer(uuid4().int)
            payload = json.loads(payload_encoded)
            self._cast_types_to_schema(payload)
            event = asyncio.Event()

            self._tasks[id] = {
                "event": event,
                "result": "-PENDING-",
            }

            self._add_inner(id, payload)
            response = await self._fetch_response(id, event)
            if self._delete_completed_queries:
                self._remove_inner(id, payload)
            if response is api.ERROR:
                return web.json_response(status=500)
            return web.json_response(status=200, data=response, dumps=pw.Json.dumps)

        if not self._cache_strategy:
            return inner

        return self._cache_strategy.wrap_async(inner)

    async def _fetch_response(self, id, event) -> Any:
        await event.wait()
        task = self._tasks.pop(id)
        return task["result"]

    def _cast_types_to_schema(self, payload: dict):
        dtypes = self._schema._dtypes()
        for column, dtype in dtypes.items():
            if payload.get(column) is None:
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
    documentation: EndpointDocumentation = EndpointDocumentation(),
    keep_queries: bool | None = None,
    delete_completed_queries: bool | None = None,
    request_validator: Callable | None = None,
    cache_strategy: CacheStrategy | None = None,
) -> tuple[pw.Table, Callable]:
    """
    Runs a lightweight HTTP server and inputs a collection from the HTTP endpoint,
    configured by the parameters of this method.

    On the output, the method provides a table and a callable, which needs to accept
    the result table of the computation, which entries will be tracked and put into
    respective request's responses.

    Args:
        webserver: configuration object containing host and port information. You only
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
        request_validator: a callable that can verify requests. A return value of `None` accepts payload.
          Any other returned value is treated as error and used as the response. Any exception is
          caught and treated as validation failure.
        cache_strategy: one of available request caching strategies or None if no caching is required.
          If enabled, caches responses for the requests with the same ``schema``-defined payload.

    Returns:
        tuple: A tuple containing two elements. The table read and a ``response_writer``,
        a callable, where the result table should be provided. The ``id`` column of the result
        table must contain the primary keys of the objects from the input
        table and a ``result`` column, corresponding to the endpoint's return value.

    Example:

    Let's consider the following example: there is a collection of words that are
    received through HTTP REST endpoint `/uppercase` located at `127.0.0.1`, port `9999`.
    The Pathway program processes this table by converting these words to the upper case.
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

    Please note that you don't need to create another web server object if you need to
    have more than one endpoint running on the same host and port. For example, if you need
    to create another endpoint that converts words to lower case, in the same way, you
    need to reuse the existing `webserver` object. That is, the configuration would start
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
            request_validator=request_validator,
            documentation=documentation,
            cache_strategy=cache_strategy,
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

        internal_subscribe(
            table=responses,
            on_change=on_change,
            skip_errors=False,
            skip_persisted_batch=True,
        )

    return input_table, response_writer

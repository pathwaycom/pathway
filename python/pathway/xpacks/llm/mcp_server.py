# Copyright 2025 Pathway
from __future__ import annotations

import copy
import json
import threading
import warnings
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from dataclasses import dataclass, field
from inspect import Parameter, Signature
from typing import TYPE_CHECKING, Any, Callable, Literal

from aiohttp import web

import pathway as pw
import pathway.io as io
from pathway.internals import api, dtype
from pathway.internals.config import _check_entitlements
from pathway.internals.helpers import _no_default_value_marker, _Undefined
from pathway.internals.udfs.caches import CacheStrategy
from pathway.io.http._server import PathwayServer, ServerSubject
from pathway.optional_import import optional_imports

if TYPE_CHECKING:
    with optional_imports("xpack-llm"):
        from fastmcp import FastMCP


def _generate_handler_signature(schema: type[pw.Schema]):
    params: list[Parameter] = []
    annotations: dict[str, type] = {}

    if schema is not None:
        for name, column in schema.columns().items():
            param_name = name
            param_type = column.dtype
            default = (
                column.default_value
                if column.default_value is not _no_default_value_marker
                else Parameter.empty
            )

            # Workaround for hitting max recursion depth during type hint evaluation
            # triggered inside FastMCP tool.
            typehint = dict if param_type is dtype.JSON else param_type.typehint

            params.append(
                Parameter(
                    param_name,
                    Parameter.KEYWORD_ONLY,
                    annotation=typehint,
                    default=default,
                )
            )
            annotations[param_name] = typehint

    return Signature(params), annotations


class _McpServerSubject(ServerSubject):
    def __init__(
        self,
        webserver: PathwayServer,
        route: str,
        schema: type[pw.Schema],
        delete_completed_queries: bool,
        cache_strategy: CacheStrategy | None = None,
        title: str | None = None,
        description: str | None = None,
        output_schema: dict[str, Any] | None | _Undefined = _no_default_value_marker,
        annotations: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ):
        super().__init__(
            webserver,
            route,
            methods=[],
            schema=schema,
            delete_completed_queries=delete_completed_queries,
            cache_strategy=cache_strategy,
            title=title,
            description=description,
            output_schema=output_schema,
            annotations=annotations,
            meta=meta,
        )

    async def handle(self, **kwargs):
        result = await self._request_processor(
            json.dumps(kwargs, sort_keys=True, ensure_ascii=False)
        )
        return copy.deepcopy(result)

    def _process_response(self, response):
        if response is api.ERROR:
            raise RuntimeError
        if isinstance(response, pw.Json):
            return response.value
        else:
            return response

    def _verify_payload(self, payload: dict):
        defaults = self._schema.default_values()
        for column in self._schema.keys():
            if column not in payload and column not in defaults:
                raise ValueError(
                    f"Column `{column}` is required but not provided in the payload."
                )


@dataclass
class McpConfig:
    """A configuration class for the :py:class:`~pathway.xpacks.llm.mcp_server.McpServer`."""

    name: str = "pathway-mcp-server"
    transport: Literal["streamable-http", "stdio"] = "streamable-http"
    host: str | None = None
    port: int | None = None

    def __post_init__(self):
        if self.transport not in ["streamable-http", "stdio"]:
            raise ValueError(
                f"Invalid transport type: {self.transport}. "
                "Must be one of 'streamable-http' or 'stdio'."
            )
        if self.transport == "stdio":
            warnings.warn(
                "The 'stdio' transport is unstable and experimental. "
                "It may change or be removed in future releases.",
                stacklevel=2,
            )
            if self.host is not None or self.port is not None:
                raise ValueError(
                    "Host and port cannot be set when transport is 'stdio'."
                )
        elif self.transport == "streamable-http":
            if self.host is None or self.port is None:
                raise ValueError(
                    "Host and port must be set when transport is 'streamable-http'."
                )

    @property
    def runner_kwargs(self) -> dict[str, Any]:
        if self.transport == "streamable-http":
            return {
                "transport": self.transport,
                "host": self.host,
                "port": self.port,
            }
        return {"transport": self.transport}


class McpServable(ABC):
    """
    Abstract base class for objects that can be registered
    with an :py:class:`~pathway.xpacks.llm.mcp_server.McpServer`.

    Implement this class to define a component compatible
    with :py:class:`~pathway.xpacks.llm.mcp_server.PathwayMcp`.
    """

    @abstractmethod
    def register_mcp(self, server: McpServer) -> None:
        pass


class McpServer(PathwayServer):
    """
    A server implementing MCP (Model Context Protocol).

    It is encouraged to use the py:class:`~pathway.xpacks.llm.mcp_server.PathwayMcp` class for easier configuration.
    """

    _fastmcp: FastMCP
    _thread: threading.Thread | None
    _start_mutex: threading.Lock
    _config: McpConfig

    _instances: dict[str, McpServer] = {}

    def __init__(self, config: McpConfig):
        super().__init__()
        _check_entitlements("xpack-llm-mcp")
        with optional_imports("xpack-llm"):
            from fastmcp import FastMCP

        self._config = config
        self._fastmcp = FastMCP(config.name)
        self._start_mutex = threading.Lock()
        self._thread = None

    @classmethod
    def get(cls, config: McpConfig) -> McpServer:
        """
        Returns an instance of the MCP server with the given configuration.
        """
        if config.name not in cls._instances:
            cls._instances[config.name] = cls(config)
        return cls._instances[config.name]

    def _register_endpoint(
        self,
        route: str,
        handler: Callable[[web.Request], Awaitable[web.Response]],
        *,
        schema: type[pw.Schema],
        title: str | None = None,
        description: str | None = None,
        output_schema: dict[str, Any] | None | _Undefined = _no_default_value_marker,
        annotations: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if isinstance(output_schema, _Undefined):
            with optional_imports("xpack-llm"):
                from fastmcp.utilities.types import NotSet

        async def wrapper(*args, **kwargs):
            return await handler(*args, **kwargs)

        wrapper.__signature__, wrapper.__annotations__ = (  # type:ignore[attr-defined]
            _generate_handler_signature(schema)
        )

        self._fastmcp.tool(
            wrapper,
            name=route,
            title=title,
            description=description,
            output_schema=(
                output_schema if not isinstance(output_schema, _Undefined) else NotSet
            ),
            annotations=annotations,
            meta=meta,
        )

    def _run(self):
        with self._start_mutex:
            if self._thread is None or not self._thread.is_alive():

                def run_mcp():
                    self._fastmcp.run(**self._config.runner_kwargs)

                self._thread = threading.Thread(target=run_mcp, daemon=True)
                self._thread.start()
                self._loop.run_forever()

    def tool(
        self,
        name: str,
        *,
        request_handler: Callable,
        schema: type[pw.Schema],
        delete_completed_queries: bool = False,
        cache_strategy: CacheStrategy | None = None,
        title: str | None = None,
        description: str | None = None,
        output_schema: dict[str, Any] | None | _Undefined = _no_default_value_marker,
        annotations: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        autocommit_duration_ms: int | None = 50,
    ) -> None:
        """
        Registers a callable as an MCP tool.

        Args:
            name: Tool name exposed by the MCP server.
            request_handler: Callable to process queries.
            schema: Query schema, used to generate tool input schema.
            delete_completed_queries: Delete completed queries, default False.
            cache_strategy: Optional caching strategy.
            title: Optional tool title for display purposes, `name` is used if `None`.
            description: Optional tool description, `request_handler` docstring is used if `None`.
            output_schema: Optional output schema.
            annotations: Optional specialized metadata like `readOnlyHint`, `idempotentHint`, ...
            meta: Optional tool metadata.
            autocommit_duration_ms: Maximum time between two commits in milliseconds.
        """
        subject = _McpServerSubject(
            webserver=self,
            route=name,
            schema=schema,
            delete_completed_queries=delete_completed_queries,
            cache_strategy=cache_strategy,
            title=title if title is not None else name,
            description=(
                description if description is not None else request_handler.__doc__
            ),
            output_schema=output_schema,
            annotations=annotations,
            meta=meta,
        )

        input_table = io.python.read(
            subject=subject,
            schema=schema,
            format="json",
            autocommit_duration_ms=autocommit_duration_ms,
        )

        response_writer = self._get_response_writer(delete_completed_queries)
        response_writer(request_handler(input_table))


@dataclass(frozen=True)
class PathwayMcp:
    """
    A configuration class that simplifies the definition of MCP servers with compatible servables.

    Args:
        transport: The transport protocol used by the server.
            The 'stdio' transport is unstable and experimental. It may change or be removed in future releases.
            Defaults to "streamable-http".
        host The hostname or IP address to bind the server to. If None, uses default binding.
        port: The port number to bind the server to. If None, uses default port.
        name: The name of the MCP server instance. Defaults to "pathway-mcp-server".
        serve: A list of :py:class:`~pathway.xpacks.llm.mcp_server.McpServable`
            objects to register with the MCP server.
    """

    transport: Literal["streamable-http", "stdio"] = "streamable-http"
    host: str | None = None
    port: int | None = None
    name: str = "pathway-mcp-server"
    serve: list[McpServable] = field(default_factory=list)

    def __post_init__(self):
        server = McpServer.get(
            McpConfig(
                name=self.name,
                transport=self.transport,
                host=self.host,
                port=self.port,
            )
        )
        for servable in self.serve:
            servable.register_mcp(server)

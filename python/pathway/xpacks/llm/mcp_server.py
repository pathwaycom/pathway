# Copyright 2025 Pathway
from __future__ import annotations

import copy
import json
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from inspect import Parameter, Signature
from typing import Any, Callable, Literal

import pathway as pw
import pathway.io as io
from pathway.internals import api
from pathway.internals.config import _check_entitlements
from pathway.internals.schema import _no_default_value_marker
from pathway.internals.udfs.caches import CacheStrategy
from pathway.io.http._server import PathwayServer, ServerSubject
from pathway.optional_import import optional_imports


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
            params.append(
                Parameter(
                    param_name,
                    Parameter.KEYWORD_ONLY,
                    annotation=param_type.typehint,
                    default=default,
                )
            )
            annotations[param_name] = param_type.typehint

    return Signature(params), annotations


class _McpServerSubject(ServerSubject):
    def __init__(
        self,
        webserver,
        route,
        schema,
        delete_completed_queries,
        cache_strategy=None,
    ):
        super().__init__(
            webserver,
            route,
            methods=[],
            schema=schema,
            delete_completed_queries=delete_completed_queries,
            cache_strategy=cache_strategy,
        )

    async def handle(self, **kwargs):
        result = await self._request_processor(
            json.dumps(kwargs, sort_keys=True, ensure_ascii=False)
        )
        return copy.deepcopy(result)

    def _process_response(self, response):
        if response is api.ERROR:
            raise RuntimeError
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
    name: str = "pathway-mcp-server"
    transport: Literal["streamable-http", "stdio"] = "stdio"
    host: str | None = None
    port: int | None = None

    def __post_init__(self):
        if self.transport not in ["streamable-http", "stdio"]:
            raise ValueError(
                f"Invalid transport type: {self.transport}. "
                "Must be one of 'streamable-http' or 'stdio'."
            )
        if self.transport == "stdio":
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
    @abstractmethod
    def register_mcp(self, server: McpServer) -> None:
        pass


class McpServer(PathwayServer):
    _fastmcp: Any
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
        if config.name not in cls._instances:
            cls._instances[config.name] = cls(config)
        return cls._instances[config.name]

    def _register_endpoint(
        self,
        route,
        handler,
        *,
        schema=None,
        **kwargs,
    ):
        async def wrapper(*args, **kwargs):
            return await handler(*args, **kwargs)

        wrapper.__signature__, wrapper.__annotations__ = (  # type:ignore[attr-defined]
            _generate_handler_signature(schema)
        )

        self._fastmcp.tool(wrapper, name=route)

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
        autocommit_duration_ms=50,
        delete_completed_queries: bool = False,
        cache_strategy: CacheStrategy | None = None,
    ) -> None:
        subject = _McpServerSubject(
            webserver=self,
            route=name,
            schema=schema,
            delete_completed_queries=delete_completed_queries,
            cache_strategy=cache_strategy,
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
    transport: Literal["streamable-http", "stdio"] = "stdio"
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

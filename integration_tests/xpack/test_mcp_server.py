import multiprocessing as mp
import time
from contextlib import asynccontextmanager

import pytest
from fastmcp import Client

import pathway as pw
from pathway.tests.utils import run_all
from pathway.xpacks.llm.mcp_server import McpConfig, McpServable, McpServer, PathwayMcp


@asynccontextmanager
async def spawn_mcp(target, port, *, name=None, args=()):
    proc = mp.Process(target=target, name=name, args=args, daemon=True)
    proc.start()

    try:
        url = f"http://localhost:{port}/mcp/"
        async with await _ready_client(url) as client:
            yield client
    finally:
        if proc.is_alive():
            proc.terminate()
        proc.join(timeout=2)


async def _ready_client(
    base_url: str, timeout_s: float = 10.0, interval_s: float = 0.1
):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            client = Client(base_url)

            async with client:
                await client.ping()

            return client
        except RuntimeError:
            time.sleep(interval_s)
    raise TimeoutError(f"Server at {base_url} did not become ready in {timeout_s}s")


@pytest.fixture
def tcp_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.mark.asyncio
async def test_list_mcp_tools(tcp_port):
    def pipeline():
        class InputSchema(pw.Schema):
            msg: str

        class OutputSchema(pw.Schema):
            result: str

        def handler(request: pw.Table[InputSchema]) -> pw.Table[OutputSchema]:
            @pw.udf
            def inner(msg: str) -> str:
                return msg.upper()

            return request.select(result=inner(request.msg))

        server = McpServer.get(
            McpConfig(
                name="test_server",
                transport="streamable-http",
                host="localhost",
                port=tcp_port,
            )
        )
        server.tool(
            "test-tool-1",
            request_handler=handler,
            schema=InputSchema,
        )
        server.tool(
            "test-tool-2",
            request_handler=handler,
            schema=InputSchema,
        )
        run_all()

    async with spawn_mcp(pipeline, tcp_port) as client:
        tools = await client.list_tools()
        assert len(tools) == 2
        assert {tool.name for tool in tools} == {"test-tool-1", "test-tool-2"}


@pytest.mark.asyncio
async def test_mcp_unstructured_content(tcp_port):
    def pipeline():
        class InputSchema(pw.Schema):
            msg: str
            repeat: int
            capitalize: bool

        class OutputSchema(pw.Schema):
            result: pw.Json

        def handler(request: pw.Table[InputSchema]) -> pw.Table[OutputSchema]:
            @pw.udf
            def inner(msg: str, repeat: int, capitalize: bool) -> str:
                return msg.upper() * repeat if capitalize else msg * repeat

            return request.select(
                result=inner(request.msg, request.repeat, request.capitalize)
            )

        server = McpServer.get(
            McpConfig(
                name="test_server",
                transport="streamable-http",
                host="localhost",
                port=tcp_port,
            )
        )
        server.tool(
            "test-tool",
            request_handler=handler,
            schema=InputSchema,
        )
        run_all()

    async with spawn_mcp(pipeline, tcp_port) as client:
        response = await client.call_tool(
            "test-tool",
            {"msg": "foo", "repeat": 2, "capitalize": False},
        )
        assert response.content[0].text == "foofoo"
        assert response.structured_content is None

        response = await client.call_tool(
            "test-tool",
            {"msg": "NaN", "repeat": 3, "capitalize": True},
        )
        assert response.content[0].text == "NANNANNAN"


@pytest.mark.asyncio
async def test_mcp_structured_content(tcp_port):
    def pipeline():
        class InputSchema(pw.Schema):
            msg: str
            repeat: int
            capitalize: bool

        class OutputSchema(pw.Schema):
            result: pw.Json

        def handler(request: pw.Table[InputSchema]) -> pw.Table[OutputSchema]:
            @pw.udf
            def inner(msg: str, repeat: int, capitalize: bool) -> dict:
                text = msg.upper() if capitalize else msg
                return {"text": text * repeat}

            return request.select(
                result=inner(request.msg, request.repeat, request.capitalize)
            )

        server = McpServer.get(
            McpConfig(
                name="test_server",
                transport="streamable-http",
                host="localhost",
                port=tcp_port,
            )
        )
        server.tool(
            "test-tool",
            request_handler=handler,
            schema=InputSchema,
            output_schema={
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                },
                "required": ["text"],
            },
        )
        run_all()

    async with spawn_mcp(pipeline, tcp_port) as client:
        response = await client.call_tool(
            "test-tool",
            {"msg": "foo", "repeat": 2, "capitalize": False},
        )
        assert response.structured_content == {"text": "foofoo"}

        response = await client.call_tool(
            "test-tool",
            {"msg": "NaN", "repeat": 3, "capitalize": True},
        )
        assert response.structured_content == {"text": "NANNANNAN"}


@pytest.mark.asyncio
async def test_pathway_mcp(tcp_port):

    class Greet(McpServable):
        class Input(pw.Schema):
            name: str = pw.column_definition(default_value="Manul")

        def handler(self, request: pw.Table) -> pw.Table:
            @pw.udf
            def inner(name: str) -> dict:
                return {"greeting": f"Hello, {name}!"}

            return request.select(result=inner(request.name))

        def register_mcp(self, server):
            server.tool(
                "greet",
                request_handler=self.handler,
                schema=self.Input,
                output_schema={
                    "type": "object",
                    "properties": {
                        "greeting": {"type": "string"},
                    },
                    "required": ["greeting"],
                },
            )

    class MinMax(McpServable):
        class Input(pw.Schema):
            a: int
            b: int

        def _min(self, request: pw.Table) -> pw.Table:
            @pw.udf
            def inner(a, b) -> int:
                return min(a, b)

            return request.select(result=inner(request.a, request.b))

        def _max(self, request: pw.Table) -> pw.Table:
            @pw.udf
            def inner(a, b) -> int:
                return max(a, b)

            return request.select(result=inner(request.a, request.b))

        def register_mcp(self, server):
            server.tool(
                "min",
                request_handler=self._min,
                schema=self.Input,
            )
            server.tool(
                "max",
                request_handler=self._max,
                schema=self.Input,
            )

    def pipeline():
        PathwayMcp(
            transport="streamable-http",
            host="localhost",
            port=tcp_port,
            name="test_server",
            serve=[
                Greet(),
                MinMax(),
            ],
        )
        run_all()

    async with spawn_mcp(pipeline, tcp_port) as client:
        list_tools = await client.list_tools()
        assert {tool.name for tool in list_tools} == {"greet", "min", "max"}

        greet_response = await client.call_tool("greet", {"name": "Alice"})
        assert greet_response.structured_content == {"greeting": "Hello, Alice!"}
        default_response = await client.call_tool("greet", {})
        assert default_response.structured_content == {"greeting": "Hello, Manul!"}

        min_response = await client.call_tool("min", {"a": 10, "b": 20})
        assert min_response.content[0].text == "10"

        max_response = await client.call_tool("max", {"a": 10, "b": 20})
        assert max_response.content[0].text == "20"


@pytest.mark.asyncio
async def test_json_input(tcp_port):
    def pipeline():
        class Input(pw.Schema):
            data: pw.Json

        def handler(request: pw.Table[Input]) -> pw.Table:
            @pw.udf
            def _max(data: pw.Json) -> int:
                return max(data["a"].as_int(), data["b"].as_int())

            return request.select(result=_max(request.data))

        server = McpServer.get(
            McpConfig(
                name="test_server",
                transport="streamable-http",
                host="localhost",
                port=tcp_port,
            )
        )
        server.tool("max", request_handler=handler, schema=Input)

        run_all()

    async with spawn_mcp(pipeline, tcp_port) as client:
        response = await client.call_tool("max", {"data": {"a": 10, "b": 20}})
        assert response.content[0].text == "20"

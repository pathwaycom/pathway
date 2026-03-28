import asyncio
import json
import pathlib
import threading
import time
from types import TracebackType
from uuid import uuid4

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import CsvLinesNumberChecker, wait_result_with_checker

RABBITMQ_STREAM_URI = "rabbitmq-stream://guest:guest@rabbitmq:5552/"
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5552
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
WAIT_TIMEOUT_SECS = 30


class RabbitmqStreamManager:
    """Context manager that creates and cleans up a RabbitMQ stream for testing."""

    def __init__(self, uri: str, stream_name: str):
        self.uri = uri
        self.stream_name = stream_name
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()

    def __enter__(self):
        fut = asyncio.run_coroutine_threadsafe(self._create_stream(), self._loop)
        fut.result()
        return self

    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ):
        fut = asyncio.run_coroutine_threadsafe(self._cleanup(), self._loop)
        fut.result()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    async def _create_stream(self):
        from rstream import Producer

        self._producer = await Producer.create(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            username=RABBITMQ_USER,
            password=RABBITMQ_PASSWORD,
        )
        try:
            await self._producer.create_stream(self.stream_name)
        except Exception:
            pass  # stream may already exist

    async def _cleanup(self):
        try:
            await self._producer.delete_stream(self.stream_name)
        except Exception:
            pass
        await self._producer.close()

    def send(self, message: str) -> None:
        fut = asyncio.run_coroutine_threadsafe(
            self._send_async(message),
            self._loop,
        )
        fut.result()

    async def _send_async(self, message: str) -> None:
        await self._producer.send(
            self.stream_name,
            message.encode(),
        )

    def send_with_properties(self, message: str, properties: dict[str, str]) -> None:
        """Send a message with AMQP 1.0 application properties."""
        fut = asyncio.run_coroutine_threadsafe(
            self._send_with_properties_async(message, properties),
            self._loop,
        )
        fut.result()

    async def _send_with_properties_async(
        self, message: str, properties: dict[str, str]
    ) -> None:
        from rstream import AMQPMessage

        amqp_message = AMQPMessage(
            body=message.encode(),
            application_properties=properties,
        )
        await self._producer.send(
            self.stream_name,
            amqp_message,
        )


def run_identity_program(
    input_file: pathlib.Path,
    output_file: pathlib.Path,
    stream_name: str,
    new_entries: list[str],
) -> None:
    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name):
        G.clear()
        table = pw.io.plaintext.read(input_file, mode="static")
        pw.io.rabbitmq.write(
            table,
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            format="json",
        )

        class InputSchema(pw.Schema):
            data: str

        table_reread = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            schema=InputSchema,
            format="json",
        )
        pw.io.csv.write(table_reread, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, len(new_entries)),
            WAIT_TIMEOUT_SECS,
        )


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_simple(tmp_path: pathlib.Path):
    stream_name = f"rmq-{uuid4()}"
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\n")
    run_identity_program(
        input_file,
        output_file,
        stream_name,
        ["one", "two", "three", "four"],
    )


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_write_json(tmp_path: pathlib.Path):
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name) as mgr:
        G.clear()

        class InputSchema(pw.Schema):
            name: str
            age: int

        # Send test messages
        mgr.send('{"name": "Alice", "age": 30}')
        mgr.send('{"name": "Bob", "age": 25}')

        table = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            schema=InputSchema,
            format="json",
        )
        pw.io.csv.write(table, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 2),
            WAIT_TIMEOUT_SECS,
        )


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_raw_format(tmp_path: pathlib.Path):
    """Test reading messages in raw binary format."""
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name) as mgr:
        G.clear()

        mgr.send("hello raw")
        mgr.send("world raw")

        table = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            format="raw",
        )
        pw.io.csv.write(table, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 2),
            WAIT_TIMEOUT_SECS,
        )


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_plaintext_format(tmp_path: pathlib.Path):
    """Test reading messages in plaintext format."""
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name) as mgr:
        G.clear()

        mgr.send("hello plaintext")
        mgr.send("world plaintext")

        table = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            format="plaintext",
        )
        pw.io.csv.write(table, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 2),
            WAIT_TIMEOUT_SECS,
        )


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_metadata(tmp_path: pathlib.Path):
    """Test with_metadata=True produces _metadata column with offset and stream_name."""
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name) as mgr:
        G.clear()

        mgr.send("message1")
        mgr.send("message2")

        table = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            format="plaintext",
            with_metadata=True,
        )
        pw.io.csv.write(table, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 2),
            WAIT_TIMEOUT_SECS,
        )

        # Verify metadata column contents
        import pandas as pd

        result = pd.read_csv(output_file)
        assert "_metadata" in result.columns, "Expected _metadata column"
        for _, row in result.iterrows():
            metadata = json.loads(row["_metadata"])
            assert "offset" in metadata
            assert "stream_name" in metadata
            assert metadata["stream_name"] == stream_name


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_key_via_application_properties(tmp_path: pathlib.Path):
    """Test key extraction from AMQP 1.0 application_properties."""
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name) as mgr:
        G.clear()

        # Send messages with "key" in application_properties
        mgr.send_with_properties("payload1", {"key": "key_a"})
        mgr.send_with_properties("payload2", {"key": "key_b"})

        table = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            format="plaintext",
        )
        pw.io.csv.write(table, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 2),
            WAIT_TIMEOUT_SECS,
        )

        # Verify key column contains the app property values
        import pandas as pd

        result = pd.read_csv(output_file)
        assert "key" in result.columns, "Expected key column"
        keys = sorted(result["key"].tolist())
        assert keys == ["key_a", "key_b"]


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_static_mode(tmp_path: pathlib.Path):
    """Test mode='static' reads existing messages and stops."""
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name) as mgr:
        # Pre-populate the stream before starting the reader
        for i in range(3):
            mgr.send(f'{{"value": "msg{i}"}}')

        # Give time for messages to be committed
        time.sleep(1)

        G.clear()

        class InputSchema(pw.Schema):
            value: str

        table = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            schema=InputSchema,
            format="json",
            mode="static",
        )
        pw.io.csv.write(table, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 3),
            WAIT_TIMEOUT_SECS,
        )


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_write_with_key(tmp_path: pathlib.Path):
    """Test writing messages with key parameter stores key in application_properties."""
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name):
        G.clear()

        table = pw.debug.table_from_markdown(
            """
            name   | age
            Alice  | 30
            Bob    | 25
            """
        )

        # Write with key=name column
        pw.io.rabbitmq.write(
            table,
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            format="json",
            key=table.name,
        )

        # Read back with key support
        class InputSchema(pw.Schema):
            name: str
            age: int

        table_reread = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            schema=InputSchema,
            format="json",
        )
        pw.io.csv.write(table_reread, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 2),
            WAIT_TIMEOUT_SECS,
        )


@pytest.mark.flaky(reruns=5)
def test_rabbitmq_write_with_headers(tmp_path: pathlib.Path):
    """Test writing messages with headers parameter stores in application_properties."""
    stream_name = f"rmq-{uuid4()}"
    output_file = tmp_path / "output.txt"

    with RabbitmqStreamManager(RABBITMQ_STREAM_URI, stream_name):
        G.clear()

        table = pw.debug.table_from_markdown(
            """
            name   | age
            Alice  | 30
            Bob    | 25
            """
        )

        # Write with headers
        pw.io.rabbitmq.write(
            table,
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            format="json",
            headers=[table.name],
        )

        # Read back to verify messages were written
        class InputSchema(pw.Schema):
            name: str
            age: int

        table_reread = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_name,
            schema=InputSchema,
            format="json",
        )
        pw.io.csv.write(table_reread, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 2),
            WAIT_TIMEOUT_SECS,
        )

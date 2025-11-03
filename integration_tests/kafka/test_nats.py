import asyncio
import json
import pathlib
import threading
from types import TracebackType
from uuid import uuid4

import pytest
from nats.aio.client import Client as NATS

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    FileLinesNumberChecker,
    wait_result_with_checker,
)

from .utils import check_keys_in_file

NATS_SERVER_URI = "nats://nats:4222/"
JETSTREAM_SERVER_URI = "nats://nats-js:4222/"


class JetStreamManager:
    def __init__(
        self,
        server: str,
        stream: str,
        subjects: list[str],
        storage: str = "file",
    ):
        self.server = server
        self.stream_name = stream
        self.subjects = subjects or [">"]
        self.storage = storage

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()

        fut = asyncio.run_coroutine_threadsafe(
            self._connect_to_nats_server(), self._loop
        )
        fut.result()

    async def _connect_to_nats_server(self) -> None:
        self._nats = NATS()
        await self._nats.connect(servers=[self.server])
        self._jetstream = self._nats.jetstream()

    def __enter__(self):
        fut = asyncio.run_coroutine_threadsafe(self._ensure_stream(), self._loop)
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

    async def _ensure_stream(self):
        try:
            await self._jetstream.stream_info(self.stream_name)
        except Exception:
            await self._jetstream.add_stream(
                name=self.stream_name,
                subjects=self.subjects,
                storage=self.storage,
            )

    async def _cleanup(self):
        try:
            await self._jetstream.delete_stream(self.stream_name)
        except Exception:
            pass
        await self._nats.close()

    def send(self, subject: str, message: str) -> None:
        fut = asyncio.run_coroutine_threadsafe(
            self._jetstream.publish(subject, message.encode()),
            self._loop,
        )
        fut.result()

    def create_consumer(self, subject: str) -> str:
        durable_name = str(uuid4())
        fut = asyncio.run_coroutine_threadsafe(
            self._jetstream.pull_subscribe(subject, durable=durable_name),
            self._loop,
        )
        fut.result()
        return durable_name


def run_identity_program(
    input_file: pathlib.Path,
    output_file: pathlib.Path,
    nats_topic: str,
    new_entries: list[str],
    persistence_config: pw.persistence.Config | None = None,
    with_jetstream: bool = False,
) -> None:
    def inner(uri: str, jetstream_stream_name: str | None) -> None:
        G.clear()
        table = pw.io.plaintext.read(input_file, mode="static")
        pw.io.nats.write(
            table,
            uri=uri,
            topic=nats_topic,
            format="json",
            jetstream_stream_name=jetstream_stream_name,
        )

        class InputSchema(pw.Schema):
            data: str

        table_reread = pw.io.nats.read(
            uri=uri,
            topic=nats_topic,
            schema=InputSchema,
            format="json",
            jetstream_stream_name=jetstream_stream_name,
        )
        pw.io.csv.write(table_reread, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, len(new_entries)),
            30,
            kwargs={"persistence_config": persistence_config},
        )

    if with_jetstream:
        stream_name = f"stream-{nats_topic}"
        with JetStreamManager(
            JETSTREAM_SERVER_URI, stream=stream_name, subjects=[nats_topic]
        ):
            inner(JETSTREAM_SERVER_URI, stream_name)
    else:
        inner(NATS_SERVER_URI, None)


@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize("with_jetstream", [False, True])
def test_nats_simple(with_jetstream: bool, tmp_path: pathlib.Path):
    nats_topic = f"nats-{uuid4()}"
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\n")
    run_identity_program(
        input_file,
        output_file,
        nats_topic,
        ["one", "two", "three", "four"],
        with_jetstream=with_jetstream,
    )


@pytest.mark.parametrize("with_jetstream", [False, True])
@pytest.mark.flaky(reruns=5)
def test_nats_persistence(with_jetstream: bool, tmp_path: pathlib.Path):
    nats_topic = f"nats-{uuid4()}"
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"

    config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage")
    )

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\n")
    run_identity_program(
        input_file,
        output_file,
        nats_topic,
        ["one", "two", "three", "four"],
        config,
        with_jetstream=with_jetstream,
    )
    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\n")
    run_identity_program(
        input_file,
        output_file,
        nats_topic,
        ["five", "six"],
        config,
        with_jetstream=with_jetstream,
    )
    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine")
    run_identity_program(
        input_file,
        output_file,
        nats_topic,
        ["seven", "eight", "nine"],
        config,
        with_jetstream=with_jetstream,
    )


@pytest.mark.parametrize("output_format", ["json", "plaintext"])
@pytest.mark.parametrize("with_jetstream", [False, True])
@pytest.mark.flaky(reruns=3)
def test_nats_dynamic_topics(
    with_jetstream: bool, tmp_path: pathlib.Path, output_format: str
):
    input_path = tmp_path / "input.jsonl"
    output_path_1 = tmp_path / "output_1.jsonl"
    output_path_2 = tmp_path / "output_2.jsonl"
    dynamic_topic_1 = str(uuid4())
    dynamic_topic_2 = str(uuid4())
    with open(input_path, "w") as f:
        f.write(json.dumps({"k": "0", "v": "foo", "t": dynamic_topic_1}))
        f.write("\n")
        f.write(json.dumps({"k": "1", "v": "bar", "t": dynamic_topic_2}))
        f.write("\n")
        f.write(json.dumps({"k": "2", "v": "baz", "t": dynamic_topic_1}))
        f.write("\n")

    def inner(uri: str) -> None:

        class InputSchema(pw.Schema):
            k: str = pw.column_definition(primary_key=True)
            v: str
            t: str

        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        if output_format == "json":
            write_kwargs = {"format": "json"}
        elif output_format == "plaintext":
            write_kwargs = {
                "format": "plaintext",
                "value": table.k,
            }
        else:
            raise RuntimeError(f"Unknown output format: {output_format}")
        pw.io.nats.write(
            table,
            uri=uri,
            topic=table.select(topic="NatsTopic." + pw.this.t)["topic"],
            **write_kwargs,
        )
        stream_1 = pw.io.nats.read(
            uri=uri,
            topic=f"NatsTopic.{dynamic_topic_1}",
            format="plaintext",
        )
        stream_2 = pw.io.nats.read(
            uri=uri,
            topic=f"NatsTopic.{dynamic_topic_2}",
            format="plaintext",
        )
        pw.io.jsonlines.write(stream_1, output_path_1)
        pw.io.jsonlines.write(stream_2, output_path_2)
        wait_result_with_checker(
            FileLinesNumberChecker(output_path_1, 2).add_path(output_path_2, 1), 30
        )

        check_keys_in_file(
            output_path_1,
            output_format,
            {"0", "2"},
            {"k", "v", "t", "time", "diff", "topic"},
        )
        check_keys_in_file(
            output_path_2,
            output_format,
            {"1"},
            {"k", "v", "t", "time", "diff", "topic"},
        )

    if with_jetstream:
        stream_name = f"stream-{dynamic_topic_1}-{dynamic_topic_2}"
        with JetStreamManager(
            JETSTREAM_SERVER_URI,
            stream=stream_name,
            subjects=[f"NatsTopic.{dynamic_topic_1}", f"NatsTopic.{dynamic_topic_2}"],
        ):
            inner(JETSTREAM_SERVER_URI)
    else:
        inner(NATS_SERVER_URI)


@pytest.mark.parametrize("with_external_consumer", [False, True])
def test_jetstream_reader(with_external_consumer: bool, tmp_path: pathlib.Path):
    output_path = tmp_path / "output.jsonl"
    stream_name = str(uuid4())
    subject_name = f"NatsTopic.{stream_name}"

    def run_simple_read(
        durable_consumer_name: str | None,
        interval_start: int,
        interval_finish: int,
    ):
        n_expected_entries = interval_finish - interval_start
        G.clear()
        table = pw.io.nats.read(
            uri=JETSTREAM_SERVER_URI,
            topic=subject_name,
            format="plaintext",
            jetstream_stream_name=stream_name,
            durable_consumer_name=durable_consumer_name,
        )
        pw.io.jsonlines.write(table, output_path)
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, n_expected_entries), 30
        )
        with open(output_path, "r") as f:
            for row in f:
                row_id = int(json.loads(row)["data"])
                assert interval_start <= row_id <= interval_finish

    with JetStreamManager(
        JETSTREAM_SERVER_URI,
        stream=stream_name,
        subjects=[subject_name],
    ) as js:
        consumer_name = (
            js.create_consumer(subject_name) if with_external_consumer else None
        )
        for i in range(10):
            js.send(subject_name, f"{i}")
        run_simple_read(consumer_name, 0, 10)
        run_simple_read(consumer_name, 0, 0)
        for i in range(10, 15):
            js.send(subject_name, f"{i}")
        run_simple_read(consumer_name, 10, 15)

        # Create another durable consumer, and since it's a new one,
        # it reads everything
        new_consumer_name = js.create_consumer(subject_name)
        run_simple_read(new_consumer_name, 0, 15)

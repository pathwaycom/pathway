# Copyright © 2026 Pathway

"""NATS JetStream reader + persistence: no message may be lost across a hard restart.

The guarantee under test: every message published to a JetStream-backed topic
that a persisted pipeline subscribes to must eventually appear in the pipeline
output, even if the pipeline process is SIGKILLed at an arbitrary moment and
restarted from its persistent storage. Duplicates are allowed (at-least-once),
gaps are not.

Unlike MQTT, the offline window is safe here even without deferred
acknowledgements: the durable consumer keeps its position at the broker. The
loss window specific to JetStream is between reading a message and covering it
with a durable checkpoint - the reader acknowledges on read, so a crash inside
that window discards the messages forever.

The test uses the ``nats-js`` service of the docker-compose environment. For a
local run outside of docker-compose, point ``PATHWAY_NATS_JETSTREAM_URI`` at
your own JetStream-enabled server (``nats-server -js``).
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
from uuid import uuid4

import pytest

nats = pytest.importorskip("nats")

JETSTREAM_URI = os.environ.get("PATHWAY_NATS_JETSTREAM_URI", "nats://nats-js:4222/")

IDENTITY_PROGRAM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "pw_nats_identity_program.py"
)
SNAPSHOT_INTERVAL_MS = 1000
SUBSCRIPTION_TIMEOUT_SEC = 60
COMPLETION_TIMEOUT_SEC = 90


class JetStreamPublisher:
    """A synchronous facade over nats-py, which is asyncio-only."""

    def __init__(self, uri, stream_name, topic):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._uri = uri
        self._stream_name = stream_name
        self._topic = topic
        self._call(self._connect())

    def _call(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(
            timeout=30
        )

    async def _connect(self):
        self._connection = await nats.connect(self._uri)
        self._jetstream = self._connection.jetstream()
        await self._jetstream.add_stream(name=self._stream_name, subjects=[self._topic])

    def publish(self, payload: str):
        # jetstream publish resolves on the broker's PubAck, so a returned
        # publish is durably accepted by the stream.
        self._call(self._jetstream.publish(self._topic, payload.encode()))

    def stop(self):
        self._call(self._connection.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)


class PipelineRun:
    """One run of the identity pipeline subprocess."""

    def __init__(self, tmp_path, topic, stream_name, run_index):
        self.output_path = tmp_path / f"output-{run_index}.jsonl"
        self._log_path = tmp_path / f"pw-log-{run_index}.txt"
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        with open(self._log_path, "wb") as log_file:
            self._process = subprocess.Popen(
                [
                    sys.executable,
                    IDENTITY_PROGRAM_PATH,
                    "--uri",
                    JETSTREAM_URI,
                    "--topic",
                    topic,
                    "--stream",
                    stream_name,
                    "--output",
                    str(self.output_path),
                    "--pstorage",
                    str(tmp_path / "pstorage"),
                    "--snapshot-interval-ms",
                    str(SNAPSHOT_INTERVAL_MS),
                ],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )

    def received_payloads(self) -> set[str]:
        payloads = set()
        try:
            with open(self.output_path) as f:
                for line in f:
                    if not line.endswith("\n"):
                        continue  # partially flushed last line
                    payloads.add(json.loads(line)["data"])
        except FileNotFoundError:
            pass
        return payloads

    def assert_alive(self):
        exit_code = self._process.poll()
        assert exit_code is None, (
            f"the pipeline exited prematurely with code {exit_code}; "
            f"log tail:\n{self.log_tail()}"
        )

    def sigkill(self):
        os.kill(self._process.pid, signal.SIGKILL)
        self._process.wait(timeout=60)

    def stop(self):
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait(timeout=60)

    def log_tail(self) -> str:
        try:
            with open(self._log_path) as f:
                return "".join(f.readlines()[-30:])
        except FileNotFoundError:
            return "<no log file>"


def _wait_until_received(run, publisher, marker, timeout):
    """Publish `marker` repeatedly until it shows up in the run's output."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run.assert_alive()
        publisher.publish(marker)
        if marker in run.received_payloads():
            return
        time.sleep(0.5)
    raise AssertionError(
        f"marker {marker!r} did not reach the output in {timeout} seconds; "
        f"log tail:\n{run.log_tail()}"
    )


def test_nats_jetstream_messages_survive_sigkill_restart(tmp_path):
    suffix = uuid4().hex
    stream_name = f"persistence-{suffix}"
    topic = f"persistence.{suffix}"
    publisher = JetStreamPublisher(JETSTREAM_URI, stream_name, topic)

    # Run 0: start the pipeline and wait until it demonstrably receives data.
    run0 = PipelineRun(tmp_path, topic, stream_name, run_index=0)
    try:
        _wait_until_received(run0, publisher, "warmup", SUBSCRIPTION_TIMEOUT_SEC)

        # Publish a continuous stream and SIGKILL the pipeline mid-flight, so
        # that some delivered messages have not made it into a checkpoint yet.
        for i in range(1500):
            publisher.publish(f"msg-{i:05d}")
        # Let the pipeline ingest (but not necessarily checkpoint) the tail.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if len(run0.received_payloads()) >= 500:
                break
            time.sleep(0.1)
        run0.assert_alive()
    except Exception:
        run0.stop()
        publisher.stop()
        raise
    run0.sigkill()

    # While the pipeline is down, the stream continues. This window is covered
    # by the durable consumer's position at the broker, not by the deferred
    # acknowledgements.
    for i in range(1500, 2000):
        publisher.publish(f"msg-{i:05d}")

    # Run 1: restart from the same persistent storage and wait until the
    # pipeline has caught up with a fresh marker.
    run1 = PipelineRun(tmp_path, topic, stream_name, run_index=1)
    try:
        _wait_until_received(run1, publisher, "final-marker", COMPLETION_TIMEOUT_SEC)

        expected = {f"msg-{i:05d}" for i in range(2000)}
        received = run0.received_payloads() | run1.received_payloads()

        # Give the late tail a chance: everything the broker still has pending
        # should be delivered shortly after the final marker.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not expected <= received:
            time.sleep(0.5)
            received = run0.received_payloads() | run1.received_payloads()

        missing = sorted(expected - received)
        assert not missing, (
            f"{len(missing)} of {len(expected)} JetStream messages were lost "
            f"across the SIGKILL restart, e.g. {missing[:10]}"
        )
    finally:
        run1.stop()
        publisher.stop()

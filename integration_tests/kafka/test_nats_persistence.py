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
import os
import threading
from uuid import uuid4

import pytest

nats = pytest.importorskip("nats")

from .utils import (  # noqa: E402
    IdentityPipelineRun,
    assert_messages_survive_sigkill_restart,
)

JETSTREAM_URI = os.environ.get("PATHWAY_NATS_JETSTREAM_URI", "nats://nats-js:4222/")

IDENTITY_PROGRAM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "pw_nats_identity_program.py"
)


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


def test_nats_jetstream_messages_survive_sigkill_restart(tmp_path):
    suffix = uuid4().hex
    stream_name = f"persistence-{suffix}"
    topic = f"persistence.{suffix}"
    publisher = JetStreamPublisher(JETSTREAM_URI, stream_name, topic)

    def make_run(run_index):
        return IdentityPipelineRun(
            tmp_path,
            IDENTITY_PROGRAM_PATH,
            [
                "--uri",
                JETSTREAM_URI,
                "--topic",
                topic,
                "--stream",
                stream_name,
            ],
            run_index,
        )

    try:
        assert_messages_survive_sigkill_restart(
            make_run, publisher.publish, "JetStream messages"
        )
    finally:
        publisher.stop()

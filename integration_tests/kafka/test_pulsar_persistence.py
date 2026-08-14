# Copyright © 2026 Pathway

"""Pulsar reader + persistence: no message may be lost across a hard restart.

The guarantee under test: every message published to a topic that a persisted
pipeline reads must eventually appear in the pipeline output, even if the
pipeline process is SIGKILLed at an arbitrary moment and restarted from its
persistent storage.

With persistence the connector reads in the partition-reader mode: the
per-partition positions of the delivered messages live in the checkpoint, and
a restart re-reads every partition right after its last checkpointed position.
The uncheckpointed tail (delivered but not yet covered by a checkpoint at the
moment of the kill) is re-read from the topic itself, and the offline window
is covered the same way — the messages stay in the topic and the restarted
pipeline picks them up from the recorded positions. No broker-side
subscription state is involved.

The test uses the ``pulsar`` service of the docker-compose environment. For a
local run outside of docker-compose, point ``PULSAR_HOST`` / ``PULSAR_PORT``
at your own broker (e.g. ``bin/pulsar standalone``).
"""

import os
from uuid import uuid4

import pytest

pulsar = pytest.importorskip("pulsar")

from .utils import (  # noqa: E402
    PULSAR_SERVICE_URI,
    IdentityPipelineRun,
    assert_messages_survive_sigkill_restart,
)

IDENTITY_PROGRAM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "pw_pulsar_identity_program.py"
)


class PulsarPublisher:
    def __init__(self, uri, topic):
        self._client = pulsar.Client(
            uri, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Warn)
        )
        self._producer = self._client.create_producer(topic)

    def publish(self, payload: str):
        # `send` resolves on the broker's acknowledgement, so a returned
        # publish is durably accepted by the topic.
        self._producer.send(payload.encode())

    def stop(self):
        self._producer.close()
        self._client.close()


def test_pulsar_messages_survive_sigkill_restart(tmp_path):
    suffix = uuid4().hex
    topic = f"persistence-{suffix}"
    publisher = PulsarPublisher(PULSAR_SERVICE_URI, topic)

    def make_run(run_index):
        return IdentityPipelineRun(
            tmp_path,
            IDENTITY_PROGRAM_PATH,
            ["--uri", PULSAR_SERVICE_URI, "--topic", topic],
            run_index,
        )

    try:
        assert_messages_survive_sigkill_restart(make_run, publisher.publish, "messages")
    finally:
        publisher.stop()

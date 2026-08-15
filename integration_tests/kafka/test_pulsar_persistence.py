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
import signal
import subprocess
import sys
import time
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


WRITER_PROGRAM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "pw_pulsar_writer_program.py"
)


def test_pulsar_written_messages_survive_sigkill_restart(tmp_path):
    """Every row a persisted writing pipeline is given must reach the topic,
    even if the process is SIGKILLed while the writing is in flight.

    The output connector delivers at-least-once: the messages of a minibatch
    are pushed to the broker and their receipts awaited before the sink's time
    is committed, so a restart from the persistent storage resends whatever the
    checkpoint does not cover yet. Duplicates are therefore allowed by the
    contract, and missing rows are not.
    """
    n_rows = 150_000
    topic = f"persistence-writer-{uuid4().hex}"
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    with open(input_dir / "rows.txt", "w") as f:
        for i in range(n_rows):
            f.write(f"row-{i:06d}-" + "p" * 200 + "\n")

    client = pulsar.Client(
        PULSAR_SERVICE_URI, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Warn)
    )
    # The subscription is created before the pipeline starts, so nothing that
    # is published can escape the verification.
    consumer = client.subscribe(
        topic,
        subscription_name="writer-persistence-verifier",
        initial_position=pulsar.InitialPosition.Earliest,
    )
    received: set[str] = set()

    def collect(budget_seconds: float) -> None:
        deadline = time.monotonic() + budget_seconds
        while time.monotonic() < deadline:
            try:
                message = consumer.receive(timeout_millis=500)
            except Exception:
                continue
            consumer.acknowledge(message)
            received.add(message.data().decode()[:10])

    def start_writer() -> subprocess.Popen:
        return subprocess.Popen(
            [
                sys.executable,
                WRITER_PROGRAM_PATH,
                "--uri",
                PULSAR_SERVICE_URI,
                "--topic",
                topic,
                "--input-dir",
                str(input_dir),
                "--pstorage",
                str(tmp_path / "pstorage"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    process = start_writer()
    try:
        # Kill the writer once the topic has some of the rows but not all of
        # them, so that the crash really lands in the middle of the writing.
        deadline = time.monotonic() + 180
        killed_after = None
        while time.monotonic() < deadline:
            collect(0.5)
            if 1000 < len(received) < n_rows - 1000:
                os.kill(process.pid, signal.SIGKILL)
                killed_after = len(received)
                break
        assert killed_after is not None, (
            f"the writer never reached the middle of its work: "
            f"{len(received)} of {n_rows} rows delivered"
        )
        process.wait(timeout=60)
        collect(3.0)

        process = start_writer()
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline and len(received) < n_rows:
            collect(2.0)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=60)
        collect(3.0)
        consumer.close()
        client.close()

    missing = [f"row-{i:06d}" for i in range(n_rows) if f"row-{i:06d}" not in received]
    assert not missing, f"{len(missing)} rows never reached the topic: {missing[:5]}"

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
import requests

pulsar = pytest.importorskip("pulsar")

from .utils import (  # noqa: E402
    PULSAR_ADMIN_URL,
    PULSAR_SERVICE_URI,
    SIGKILL_COMPLETION_TIMEOUT_SEC,
    SIGKILL_SUBSCRIPTION_TIMEOUT_SEC,
    IdentityPipelineRun,
    assert_messages_survive_sigkill_restart,
    make_pulsar_client,
)

IDENTITY_PROGRAM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "pw_pulsar_identity_program.py"
)


class PulsarPublisher:
    def __init__(self, uri, topic):
        self._client = make_pulsar_client(uri)
        self._topic = topic
        self._producer = self._client.create_producer(topic)

    def publish(self, payload: str):
        # `send` resolves on the broker's acknowledgement, so a returned
        # publish is durably accepted by the topic.
        self._producer.send(payload.encode())

    def reconnect(self):
        # A producer resolves the partitions of its topic when it is created,
        # so publishing into partitions added later needs a fresh one.
        self._producer.close()
        self._producer = self._client.create_producer(self._topic)

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


def test_pulsar_restart_reads_the_partitions_added_while_it_was_down(tmp_path):
    """A reader is attached to the partitions the topic had when it started
    and reports an expansion instead of skipping the new partitions silently.
    The remedy it recommends — restart the pipeline — must then work: the
    restarted run reads the partitions added while it was down, and resumes
    the old ones from their checkpointed positions."""
    suffix = uuid4().hex
    topic = f"expansion-{suffix}"
    response = requests.put(
        f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/{topic}/partitions",
        json=2,
        timeout=60,
    )
    response.raise_for_status()
    publisher = PulsarPublisher(PULSAR_SERVICE_URI, topic)

    def make_run(run_index):
        return IdentityPipelineRun(
            tmp_path,
            IDENTITY_PROGRAM_PATH,
            ["--uri", PULSAR_SERVICE_URI, "--topic", topic],
            run_index,
        )

    before = {f"before-{i:03d}" for i in range(20)}
    after = {f"after-{i:03d}" for i in range(40)}
    try:
        for payload in sorted(before):
            publisher.publish(payload)
        run0 = make_run(0)
        try:
            deadline = time.monotonic() + SIGKILL_SUBSCRIPTION_TIMEOUT_SEC
            while time.monotonic() < deadline and run0.received_payloads() != before:
                time.sleep(0.2)
            assert (
                run0.received_payloads() == before
            ), f"the first run did not read the topic; log tail:\n{run0.log_tail()}"
        finally:
            run0.stop()

        response = requests.post(
            f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/{topic}/partitions",
            json=4,
            timeout=60,
        )
        response.raise_for_status()
        publisher.reconnect()
        for payload in sorted(after):
            publisher.publish(payload)

        run1 = make_run(1)
        try:
            deadline = time.monotonic() + SIGKILL_COMPLETION_TIMEOUT_SEC
            while time.monotonic() < deadline:
                if (
                    before | after
                    <= run0.received_payloads() | run1.received_payloads()
                ):
                    break
                time.sleep(0.5)
            received = run0.received_payloads() | run1.received_payloads()
            missing = sorted((before | after) - received)
            assert not missing, (
                f"{len(missing)} messages were never read after the expansion, "
                f"e.g. {missing[:10]}; log tail:\n{run1.log_tail()}"
            )
        finally:
            run1.stop()
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

    client = make_pulsar_client(PULSAR_SERVICE_URI)
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

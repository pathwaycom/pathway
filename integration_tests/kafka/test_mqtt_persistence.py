# Copyright © 2026 Pathway

"""MQTT reader + persistence: no QoS1 message may be lost across a hard restart.

The guarantee under test: every message published with QoS 1 to a topic that a
persisted pipeline subscribes to must eventually appear in the pipeline output,
even if the pipeline process is SIGKILLed at an arbitrary moment and restarted
from its persistent storage. Duplicates are allowed (at-least-once), gaps are
not.

The test uses the dedicated ``mqtt-persistent`` broker of the docker-compose
environment, which lifts mosquitto's in-flight/queued limits (see
``.jenkins/integration_tests/docker-compose-integration.yml``). For a local run
outside of docker-compose, point ``PATHWAY_MQTT_BASE_ROUTE`` at your own broker,
e.g. ``mqtt://127.0.0.1:1883?client_id=$CLIENT_ID``; the broker must allow
enough in-flight and queued messages per client (for mosquitto:
``max_inflight_messages 0``, ``max_queued_messages 0``).
"""

import json
import os
import signal
import subprocess
import sys
import time
from uuid import uuid4

import pytest

pytest.importorskip("paho.mqtt.client")
import paho.mqtt.client as mqtt  # noqa: E402

from .utils import MQTT_PERSISTENT_BASE_ROUTE  # noqa: E402

BASE_ROUTE = os.environ.get("PATHWAY_MQTT_BASE_ROUTE", MQTT_PERSISTENT_BASE_ROUTE)

IDENTITY_PROGRAM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "pw_mqtt_identity_program.py"
)
SNAPSHOT_INTERVAL_MS = 1000
SUBSCRIPTION_TIMEOUT_SEC = 60
COMPLETION_TIMEOUT_SEC = 90


def _broker_host_port() -> tuple[str, int]:
    location = BASE_ROUTE.split("://", 1)[1].split("?", 1)[0].rstrip("/")
    host, _, port = location.partition(":")
    return host, int(port or "1883")


class Publisher:
    def __init__(self):
        host, port = _broker_host_port()
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"publisher-{uuid4()}",
            protocol=mqtt.MQTTv311,
        )
        self._client.connect(host, port)
        self._client.loop_start()

    def publish(self, topic: str, payload: str):
        info = self._client.publish(topic, payload, qos=1)
        info.wait_for_publish(timeout=10)
        assert info.is_published(), f"message {payload!r} was not confirmed by broker"

    def stop(self):
        self._client.loop_stop()
        self._client.disconnect()


@pytest.fixture
def publisher():
    publisher = Publisher()
    yield publisher
    publisher.stop()


class PipelineRun:
    """One run of the identity pipeline subprocess."""

    def __init__(self, tmp_path, reader_uri, topic, run_index):
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
                    reader_uri,
                    "--topic",
                    topic,
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


def _wait_until_received(run, publisher, topic, marker, timeout):
    """Publish `marker` repeatedly until it shows up in the run's output.

    Publishing repeatedly is the only reliable way to know the reader is
    subscribed and flowing: a marker published before the subscription was
    set up would never be delivered.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run.assert_alive()
        publisher.publish(topic, marker)
        if marker in run.received_payloads():
            return
        time.sleep(0.5)
    raise AssertionError(
        f"marker {marker!r} did not reach the output in {timeout} seconds; "
        f"log tail:\n{run.log_tail()}"
    )


def test_mqtt_qos1_messages_survive_sigkill_restart(tmp_path, publisher):
    topic = f"persistence/{uuid4()}"
    # The reader's client id must be stable across the two runs (the broker
    # session holding the unacknowledged messages is bound to it) and unique
    # across the test session (the broker is shared between xdist workers).
    reader_uri = BASE_ROUTE.replace("$CLIENT_ID", f"reader-{uuid4()}")

    # Run 0: start the pipeline and wait until it demonstrably receives data.
    run0 = PipelineRun(tmp_path, reader_uri, topic, run_index=0)
    try:
        _wait_until_received(run0, publisher, topic, "warmup", SUBSCRIPTION_TIMEOUT_SEC)

        # Publish a continuous stream and SIGKILL the pipeline mid-flight, so
        # that some delivered messages have not made it into a checkpoint yet.
        for i in range(1500):
            publisher.publish(topic, f"msg-{i:05d}")
        # Let the pipeline ingest (but not necessarily checkpoint) the tail.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if len(run0.received_payloads()) >= 500:
                break
            time.sleep(0.1)
        run0.assert_alive()
    except Exception:
        run0.stop()
        raise
    run0.sigkill()

    # While the pipeline is down, the stream continues.
    for i in range(1500, 2000):
        publisher.publish(topic, f"msg-{i:05d}")

    # Run 1: restart from the same persistent storage and wait until the
    # pipeline has caught up with a fresh marker.
    run1 = PipelineRun(tmp_path, reader_uri, topic, run_index=1)
    try:
        _wait_until_received(
            run1, publisher, topic, "final-marker", COMPLETION_TIMEOUT_SEC
        )

        expected = {f"msg-{i:05d}" for i in range(2000)}
        received = run0.received_payloads() | run1.received_payloads()

        # Give the late tail a chance: everything the broker still has queued
        # should be delivered shortly after the final marker.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not expected <= received:
            time.sleep(0.5)
            received = run0.received_payloads() | run1.received_payloads()

        missing = sorted(expected - received)
        assert not missing, (
            f"{len(missing)} of {len(expected)} QoS1 messages were lost across "
            f"the SIGKILL restart, e.g. {missing[:10]}"
        )
    finally:
        run1.stop()

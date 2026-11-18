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

import os
from uuid import uuid4

import pytest

pytest.importorskip("paho.mqtt.client")
import paho.mqtt.client as mqtt  # noqa: E402

from .utils import (  # noqa: E402
    MQTT_PERSISTENT_BASE_ROUTE,
    IdentityPipelineRun,
    assert_messages_survive_sigkill_restart,
)

BASE_ROUTE = os.environ.get("PATHWAY_MQTT_BASE_ROUTE", MQTT_PERSISTENT_BASE_ROUTE)

IDENTITY_PROGRAM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "pw_mqtt_identity_program.py"
)


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


def test_mqtt_qos1_messages_survive_sigkill_restart(tmp_path, publisher):
    topic = f"persistence/{uuid4()}"
    # The reader's client id must be stable across the two runs (the broker
    # session holding the unacknowledged messages is bound to it) and unique
    # across the test session (the broker is shared between xdist workers).
    reader_uri = BASE_ROUTE.replace("$CLIENT_ID", f"reader-{uuid4()}")

    def make_run(run_index):
        return IdentityPipelineRun(
            tmp_path,
            IDENTITY_PROGRAM_PATH,
            ["--uri", reader_uri, "--topic", topic],
            run_index,
        )

    assert_messages_survive_sigkill_restart(
        make_run,
        lambda payload: publisher.publish(topic, payload),
        "QoS1 messages",
    )

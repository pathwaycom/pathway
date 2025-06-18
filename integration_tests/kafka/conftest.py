# Copyright © 2024 Pathway

from __future__ import annotations

import pytest

from .utils import KafkaTestContext, MqttTestContext


@pytest.fixture
def kafka_context():
    kafka_context = KafkaTestContext()
    yield kafka_context
    kafka_context.teardown()


@pytest.fixture
def mqtt_context():
    mqtt_context = MqttTestContext()
    yield mqtt_context

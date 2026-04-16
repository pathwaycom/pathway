# Copyright © 2026 Pathway

from __future__ import annotations

import pytest

from .utils import (
    KINESIS_ENDPOINT_URL,
    KafkaTestContext,
    KinesisTestContext,
    MqttTestContext,
    RabbitmqTestContext,
)


@pytest.fixture
def kafka_context():
    kafka_context = KafkaTestContext()
    yield kafka_context
    kafka_context.teardown()


@pytest.fixture
def mqtt_context():
    mqtt_context = MqttTestContext()
    yield mqtt_context


@pytest.fixture
def rabbitmq_context():
    ctx = RabbitmqTestContext()
    yield ctx
    ctx.teardown()


@pytest.fixture
def kinesis_context(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", KINESIS_ENDPOINT_URL)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "placeholder")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "placeholder")
    kinesis_context = KinesisTestContext()
    yield kinesis_context

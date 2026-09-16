# Copyright © 2026 Pathway

from __future__ import annotations

import gc
import os

import pytest

from .utils import (
    KINESIS_BULK_ENDPOINT_URL,
    KINESIS_ENDPOINT_URL,
    KafkaTestContext,
    KinesisTestContext,
    MqttTestContext,
    PulsarTestContext,
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
def pulsar_context():
    ctx = PulsarTestContext()
    yield ctx
    ctx.teardown()


def _kinesis_context(monkeypatch, endpoint_url: str):
    # The connector under test picks the emulator up from the environment.
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint_url)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "placeholder")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "placeholder")
    return KinesisTestContext(endpoint_url=endpoint_url)


@pytest.fixture
def kinesis_context(monkeypatch):
    yield _kinesis_context(monkeypatch, KINESIS_ENDPOINT_URL)


@pytest.fixture
def kinesis_bulk_context(monkeypatch):
    """The emulator reserved for the million-record test: kinesalite is
    single-threaded, and that test's traffic would queue every other Kinesis
    test's request behind it (a 25s wait was seen on a loaded CI node)."""
    yield _kinesis_context(monkeypatch, KINESIS_BULK_ENDPOINT_URL)


def _keep_native_client_objects_out_of_forked_children():
    """Makes the forks of this suite (a pipeline in a child process) safe
    with a live pulsar-client in the parent.

    The pulsar-client is a C++ library with its own threads. Its Python
    wrappers (Client, Producer, Consumer) run the C++ destructors when they
    are freed, and a destructor stops the library's executors: it joins
    threads and destroys condition variables. After a fork only the forking
    thread exists in the child, so a wrapper that the child's garbage
    collector happens to free — one a previous test left in a reference
    cycle, collected when the child allocates enough — blocks forever inside
    `pulsar::ExecutorService::~ExecutorService` (seen with py-spy: a
    single-threaded child sitting in pthread_cond_destroy before it even
    started the engine or installed its SIGTERM handler). Such a child
    delivers nothing and the test fails with an empty output.

    Hence, before every fork, the parent collects its garbage itself (its
    threads are all there, the destructors complete) and freezes the
    surviving objects into the permanent generation, so the child's collector
    never touches them; the parent thaws after the fork.
    """

    def before_fork():
        gc.collect()
        gc.freeze()

    os.register_at_fork(before=before_fork, after_in_parent=gc.unfreeze)


_keep_native_client_objects_out_of_forked_children()

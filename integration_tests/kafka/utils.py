# Copyright © 2026 Pathway

import asyncio
import dataclasses
import json
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterable
from typing import Mapping
from uuid import uuid4

import boto3
import requests
from botocore.exceptions import ConnectionError as BotoConnectionError
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import (
    KafkaError,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)
from kafka.producer.future import FutureRecordMetadata
from rstream import AMQPMessage, Producer
from rstream.exceptions import (
    ClientError,
    InternalError,
    LeaderNotAvailable,
    StreamAlreadyExists,
    StreamDoesNotExist,
    StreamNotAvailable,
)

KAFKA_SETTINGS = {"bootstrap_servers": "kafka:9092"}
MQTT_BASE_ROUTE = "mqtt://mqtt:1883?client_id=$CLIENT_ID"
# A dedicated broker with the in-flight/queued limits lifted, required by the
# persistence tests (see the `mqtt-persistent` service in
# `.jenkins/integration_tests/docker-compose-integration.yml`).
MQTT_PERSISTENT_BASE_ROUTE = "mqtt://mqtt-persistent:1883?client_id=$CLIENT_ID"
SCHEMA_REGISTRY_BASE_ROUTE = "http://schema-registry:8081"
# How long to keep retrying a schema-registry request while the service is
# still coming up (connection refused or a transient 5xx).
SCHEMA_REGISTRY_READY_TIMEOUT = 60.0
SCHEMA_REGISTRY_RETRY_INTERVAL = 0.5
KINESIS_ENDPOINT_URL = "http://kinesis:4567"

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5552"))
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_STREAM_URI = (
    f"rabbitmq-stream://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}"
    f"@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
)

# How long to keep retrying transient broker errors while bringing a stream up.
# The CI healthcheck only waits for the stream port to accept TCP, but the
# broker needs longer before it can complete the protocol handshake and elect a
# stream leader — and when every connector stack (kafka, kinesis, nats, mqtt,
# rabbitmq) plus the pathway engines share one host, that window stretches well
# past the single-digit seconds it takes on an idle node. A failed setup here is
# always transient, so the timeout is generous on purpose.
RABBITMQ_READY_TIMEOUT = 90.0

# How long to wait for a single freshly created stream to elect a leader before
# giving up on it and recreating it (see _ensure_stream). The stream coordinator
# occasionally leaves a stream leaderless under load; recreating nudges it to
# redo the election, whereas re-querying the same wedged stream never recovers.
RABBITMQ_STREAM_READY_ATTEMPT = 15.0

# Polling interval while waiting for a transient broker error to clear.
RABBITMQ_RETRY_INTERVAL = 0.2

# Timeout for a synchronous (confirmed) publish. Generous so a busy broker has
# time to confirm rather than failing a test on a transient slow round-trip.
RABBITMQ_SEND_TIMEOUT = 30

# Broker errors that are transient during startup / under load and clear on
# their own if we retry:
#   * ConnectionError    — TCP port open but the connection was refused/reset;
#   * TimeoutError       — TCP connected, but the protocol handshake or a
#     request timed out (rstream's request timeout) before the broker answered;
#   * ClientError        — generic rstream client error, e.g. the connection
#     dropped mid-request;
#   * StreamDoesNotExist — create's metadata not yet visible to the broker we
#     queried;
#   * StreamNotAvailable — stream known to the coordinator, but its underlying
#     resources aren't up yet (response_code 6);
#   * LeaderNotAvailable — leader election still in progress (leader_ref 65535).
#   * InternalError      — the stream coordinator hit an internal error while
#     servicing the request (response_code 15); seen on create_stream while the
#     broker is still warming up or under load, and clears on retry.
RABBITMQ_TRANSIENT_ERRORS = (
    ConnectionError,
    TimeoutError,
    ClientError,
    InternalError,
    StreamDoesNotExist,
    StreamNotAvailable,
    LeaderNotAvailable,
)


def random_topic_name():
    return f"integration-tests-{uuid4()}"


# Like RABBITMQ_READY_TIMEOUT, but for Kafka: connecting to the broker and
# creating/deleting topics can fail transiently while the cluster is still
# settling at startup (no broker reachable yet, controller not elected, request
# timed out). On a shared CI host every connector stack contends for the same
# resources, so that window is generous on purpose: observed brownouts (the
# broker starved of CPU/IO by dozens of parallel workers) last minutes, and
# admin requests burn up to 30s each before timing out, so a 90s budget was
# exhausted by just a few attempts. Matches _TOPIC_SETTLE_HARD_TIMEOUT below.
KAFKA_READY_TIMEOUT = 240.0
KAFKA_RETRY_INTERVAL = 0.2


def _kafka_retry(operation, deadline: float, *, ignore: tuple = ()):
    """Run a Kafka admin/client call, retrying transient broker errors until the
    deadline. ``ignore`` lists error types whose occurrence means the desired
    end state is already reached (e.g. the topic already exists) — those return
    quietly instead of being retried or raised. Every other ``KafkaError`` is
    treated as transient: at startup these are connection/controller hiccups
    that clear on their own (``NoBrokersAvailable``, ``NotControllerError``,
    ``NodeNotReadyError``, ``RequestTimedOutError``)."""
    while True:
        try:
            return operation()
        except ignore:
            return None
        except KafkaError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(KAFKA_RETRY_INTERVAL)


class KafkaTestContext:
    _producer: KafkaProducer
    _admin: KafkaAdminClient
    _input_topic: str
    _output_topic: str

    def __init__(self) -> None:
        # The constructors connect eagerly and raise NoBrokersAvailable (a
        # KafkaError) if the broker is not reachable yet, which is transient at
        # startup — retry until the broker accepts connections.
        deadline = time.monotonic() + KAFKA_READY_TIMEOUT
        self._producer = _kafka_retry(
            lambda: KafkaProducer(
                bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"]
            ),
            deadline,
        )
        self._admin = _kafka_retry(
            lambda: KafkaAdminClient(
                bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"],
            ),
            deadline,
        )
        self._input_topic = random_topic_name()
        self._output_topic = random_topic_name()
        self._created_topics: set[str] = set()
        # Records produced by ``send`` whose delivery has not been confirmed
        # yet. ``KafkaProducer.send`` is asynchronous; the per-record outcome
        # lands on these futures, which ``flush`` then resolves (see ``flush``).
        self._pending_sends: list[FutureRecordMetadata] = []

        self._create_topic(self.input_topic)
        self._create_topic(self.output_topic)

    def create_additional_topic(self) -> str:
        topic_name = random_topic_name()
        self._create_topic(topic_name)
        return topic_name

    # Kafka topic creation and deletion are processed asynchronously by the
    # controller and only then propagated to every broker's metadata cache.
    # Returning before that has settled races: a producer can write into a
    # topic the cluster does not fully know yet, and — worst of all — deleting
    # and recreating a topic under the same name (set_input_topic_partitions)
    # can have the create applied before the still-pending delete, so the late
    # delete wipes the freshly created topic. Producers then write into a topic
    # that silently vanishes and the test sees an empty topic. Waiting for each
    # operation to settle in the cluster metadata makes the sequence
    # deterministic.
    #
    # The settle budget counts only *answered* metadata polls: like
    # KAFKA_READY_TIMEOUT it is generous because on a shared CI host the
    # controller can lag far behind an acknowledged create/delete while dozens
    # of workers hammer the broker. A poll that fails with a KafkaError says
    # nothing about the topic — the broker could not be asked (admin requests
    # time out after 30s and reconnects back off up to 30s, so a broker
    # brownout starves the whole window with just a couple of attempts). Such
    # polls must not eat the settle budget, otherwise a transient brownout
    # surfaces as a misleading "topic did not appear": every failed poll
    # extends the deadline so that at least _TOPIC_SETTLE_ERROR_GRACE of
    # answered polling remains once the broker responds again, with
    # _TOPIC_SETTLE_HARD_TIMEOUT capping the total wait so a genuinely dead
    # broker still fails loudly (and with the real error attached).
    _TOPIC_SETTLE_TIMEOUT = 90.0
    _TOPIC_SETTLE_INTERVAL = 0.2
    _TOPIC_SETTLE_ERROR_GRACE = 30.0
    _TOPIC_SETTLE_HARD_TIMEOUT = 240.0

    def _topic_partition_count(self, name: str) -> int | None:
        """Number of partitions ``name`` currently has in the cluster metadata,
        or ``None`` if the topic is absent."""
        for topic in self._admin.describe_topics([name]):
            # kafka-python keys the topic name as "name" (3.x) or "topic"
            # (<=2.x); accept either so this works across client versions.
            if topic.get("name", topic.get("topic")) != name:
                continue
            # A non-zero error code (e.g. 3, UNKNOWN_TOPIC_OR_PARTITION) or an
            # empty partition list means the topic is not (yet) a live topic.
            if topic.get("error_code", 0) != 0 or not topic.get("partitions"):
                return None
            return len(topic["partitions"])
        return None

    def _wait_for_topic_metadata(self, name: str, predicate, description: str) -> None:
        start = time.monotonic()
        deadline = start + self._TOPIC_SETTLE_TIMEOUT
        hard_deadline = start + self._TOPIC_SETTLE_HARD_TIMEOUT
        answered = 0
        failed = 0
        last_count: int | None = None
        last_error: KafkaError | None = None
        while True:
            try:
                last_count = self._topic_partition_count(name)
            except KafkaError as error:
                # Only genuine broker/metadata errors are transient here (the
                # cluster is still in flux); those polls carry no information
                # about the topic, so they extend the deadline (see the comment
                # on the constants above). Programming errors (e.g. an
                # unexpected response shape) are NOT swallowed — they propagate
                # immediately instead of masquerading as a timeout.
                failed += 1
                last_error = error
                deadline = max(
                    deadline,
                    min(
                        time.monotonic() + self._TOPIC_SETTLE_ERROR_GRACE,
                        hard_deadline,
                    ),
                )
            else:
                answered += 1
                if predicate(last_count):
                    return
            if time.monotonic() >= deadline:
                elapsed = time.monotonic() - start
                raise TimeoutError(
                    f"topic {name!r} {description} after {elapsed:.1f}s: "
                    f"{answered} metadata poll(s) answered "
                    f"(last saw partition count {last_count}), "
                    f"{failed} failed (last error: {last_error!r})"
                ) from last_error
            time.sleep(self._TOPIC_SETTLE_INTERVAL)

    def _create_topic(self, name: str, num_partitions: int = 1) -> None:
        # The create request itself (not just the metadata settle below) can hit
        # a transient controller error while the cluster is in flux; retry it. A
        # topic that already exists (e.g. a retried create whose first response
        # was lost) is the desired end state, so treat it as success.
        deadline = time.monotonic() + KAFKA_READY_TIMEOUT
        _kafka_retry(
            lambda: self._admin.create_topics(
                [
                    NewTopic(
                        name=name,
                        num_partitions=num_partitions,
                        replication_factor=1,
                    )
                ]
            ),
            deadline,
            ignore=(TopicAlreadyExistsError,),
        )
        self._created_topics.add(name)
        self._wait_for_topic_metadata(
            name,
            lambda count: count == num_partitions,
            f"did not appear with {num_partitions} partition(s) in time",
        )

    def _delete_topic(self, name: str) -> None:
        # As with create, retry transient controller errors. A topic that is
        # already gone is the desired end state.
        deadline = time.monotonic() + KAFKA_READY_TIMEOUT
        _kafka_retry(
            lambda: self._admin.delete_topics(topics=[name]),
            deadline,
            ignore=(UnknownTopicOrPartitionError,),
        )
        self._wait_for_topic_metadata(
            name, lambda count: count is None, "was not deleted in time"
        )

    def send(
        self,
        message: str | tuple[str | bytes | None, str | bytes | None],
        topic: str | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        topic = topic or self._input_topic

        if isinstance(message, tuple):
            (key, value) = message
        else:
            (key, value) = str(uuid4()), message

        if isinstance(key, str):
            key = key.encode()
        if isinstance(value, str):
            value = value.encode()

        self._pending_sends.append(
            self._producer.send(
                topic,
                key=key,
                value=value,
                headers=headers,
            )
        )

    # Generous upper bound on how long a single buffered record may take to be
    # confirmed once flushed; only trips if a delivery wedges, so a test fails
    # loudly instead of hanging.
    _SEND_SETTLE_TIMEOUT = 60.0

    def flush(self) -> None:
        """Deliver every buffered record and surface any delivery failure.

        ``KafkaProducer.send`` is asynchronous and ``KafkaProducer.flush`` only
        waits for the in-flight batches to drain — it does *not* raise when a
        record was permanently rejected (e.g. its topic disappeared). The error
        sits unread on the per-record future, so a dropped message would
        otherwise masquerade as silent data loss in whichever test produced it,
        surfacing far away as a confusing "missing rows" assertion. Resolve
        every future here so a failed delivery fails the test loudly, at the
        point of production."""
        self._producer.flush()
        pending, self._pending_sends = self._pending_sends, []
        for future in pending:
            # Already flushed, so this resolves immediately; the timeout only
            # guards against a future that somehow never completes.
            future.get(timeout=self._SEND_SETTLE_TIMEOUT)

    def set_input_topic_partitions(self, num_partitions: int):
        self._delete_topic(self._input_topic)
        self._create_topic(self._input_topic, num_partitions)

    def fill(
        self,
        messages: Iterable[str | tuple[str, str]],
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        for msg in messages:
            self.send(msg, headers=headers)
        self.flush()

    def read_topic(self, topic, poll_timeout_ms: int = 1000) -> list[ConsumerRecord]:
        consumer = KafkaConsumer(
            auto_offset_reset="earliest",
            bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"],
            enable_auto_commit=False,
            # kafka-python defaults ``receive_message_max_bytes`` (the maximum
            # frame it will accept) to 1 MB, while ``fetch_max_bytes`` defaults to
            # 50 MB — so the consumer asks for up to 50 MB but rejects any fetch
            # response larger than 1 MB with ``InvalidReceiveError: Invalid frame
            # length``. Topics that hold more than ~1 MB (e.g. the backfilling
            # input topic, which accumulates across runs) then become unreadable.
            # Raise the accept limit to match the broker's allowance so whole-topic
            # drains succeed.
            receive_message_max_bytes=100_000_000,
        )
        try:
            # Assign every partition explicitly and drain each one up to the end
            # offset captured at the start, instead of subscribing and stopping
            # on empty polls. Subscription needs a consumer-group rebalance before
            # the first fetch; an early empty poll can then arrive before any
            # partition is assigned, and breaking there under-reads the topic.
            # With many partitions this makes callers that rebuild expected state
            # from the topic (e.g. the backfilling wordcount checker) miss
            # messages entirely. Reading to the per-partition end offset is
            # deterministic and drains the topic completely.
            deadline = time.monotonic() + 60.0
            partitions = consumer.partitions_for_topic(topic)
            while partitions is None and time.monotonic() < deadline:
                partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return []
            tps = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tps)
            consumer.seek_to_beginning(*tps)
            beginning_offsets = consumer.beginning_offsets(tps)
            end_offsets = consumer.end_offsets(tps)

            # Completion is tracked by the offset of the last consumed message,
            # not by ``consumer.position()``: after ``seek_to_beginning`` the
            # latter blocks indefinitely resolving the pending offset reset. A
            # partition is drained once a message at its last offset
            # (``end_offset - 1``) has been read; partitions that hold no
            # messages are already complete.
            pending = {tp for tp in tps if end_offsets[tp] > beginning_offsets[tp]}

            messages: list[ConsumerRecord] = []
            # The deadline guards against a partition whose end offset is somehow
            # never reached, so the test fails loudly instead of hanging forever.
            while pending and time.monotonic() < deadline:
                poll_result = consumer.poll(poll_timeout_ms)
                for topic_partition, new_messages in poll_result.items():
                    assert (
                        topic_partition.topic == topic
                    ), "Poller returns messages from an unexpected topic"
                    messages += new_messages
                    if (
                        new_messages
                        and new_messages[-1].offset >= end_offsets[topic_partition] - 1
                    ):
                        pending.discard(topic_partition)
            return messages
        finally:
            consumer.close()

    def read_output_topic(
        self,
        poll_timeout_ms: int = 1000,
        expected_headers=("pathway_time", "pathway_diff"),
    ) -> list[ConsumerRecord]:
        messages = self.read_topic(self._output_topic, poll_timeout_ms)
        for message in messages:
            headers = {header_key for header_key, _ in message.headers}
            for header in expected_headers:
                assert header in headers, headers
        return messages

    def read_input_topic(self, poll_timeout_ms: int = 1000) -> list[ConsumerRecord]:
        return self.read_topic(self._input_topic, poll_timeout_ms)

    def teardown(self) -> None:
        for topic in self._created_topics:
            self._delete_topic(topic)
        self._producer.close()
        self._admin.close()

    @property
    def input_topic(self) -> str:
        return self._input_topic

    @property
    def output_topic(self) -> str:
        return self._output_topic

    def default_rdkafka_settings(self) -> dict:
        return {
            "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
            "auto.offset.reset": "beginning",
            "group.id": str(uuid4()),
        }

    def __repr__(self) -> str:
        return f"<{type(self).__qualname__} input_topic={self.input_topic!r} output_topic={self.output_topic!r}>"


class MqttTestContext:
    topic: str
    reader_connection_string: str
    writer_connection_string: str

    def __init__(self) -> None:
        topic = str(uuid4())
        self.topic = topic
        reader_client_id = f"reader-{str(uuid4())}"
        writer_client_id = f"writer-{str(uuid4())}"
        self.reader_connection_string = MQTT_BASE_ROUTE.replace(
            "$CLIENT_ID", reader_client_id
        )
        self.writer_connection_string = MQTT_BASE_ROUTE.replace(
            "$CLIENT_ID", writer_client_id
        )


@dataclasses.dataclass(frozen=True)
class KinesisRecord:
    key: str
    value: bytes


@dataclasses.dataclass(frozen=True)
class KinesisShard:
    shard_id: str
    is_open: bool
    hash_range_start: int
    hash_range_end: int


class KinesisTestContext:
    stream_name: str

    # Same idea as the Kafka/RabbitMQ timeouts: the Kinesis endpoint may not
    # accept connections the instant the container starts, and CreateStream can
    # be throttled (LimitExceededException) when several streams are created at
    # once. Both clear on their own, so retry until the deadline.
    _READY_TIMEOUT = 90.0
    _RETRY_INTERVAL = 0.5

    def __init__(self, stream_name: str | None = None) -> None:
        self.stream_name = stream_name or str(uuid4())
        self.kinesis = boto3.client(
            "kinesis",
            region_name="us-east-1",
            endpoint_url=KINESIS_ENDPOINT_URL,
            aws_access_key_id="placeholder",
            aws_secret_access_key="placeholder",
        )
        self._create_stream(shard_count=1)

    def recreate(self, shard_count: int) -> None:
        self.stream_name = str(uuid4())
        self._create_stream(shard_count=shard_count)

    def _create_stream(self, shard_count: int) -> None:
        # CreateStream and the activation wait can both transiently fail while
        # the endpoint is starting up or throttling concurrent creates; retry
        # until the deadline. A stream that already exists is the desired end
        # state (a retried create whose first response was lost).
        deadline = time.monotonic() + self._READY_TIMEOUT
        while True:
            try:
                self.kinesis.create_stream(
                    StreamName=self.stream_name, ShardCount=shard_count
                )
                break
            except self.kinesis.exceptions.ResourceInUseException:
                break
            except (
                BotoConnectionError,
                self.kinesis.exceptions.LimitExceededException,
            ):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(self._RETRY_INTERVAL)
        self._wait_stream_to_activate(deadline)

    def send_record(self, record: KinesisRecord) -> None:
        self.kinesis.put_record(
            StreamName=self.stream_name,
            PartitionKey=record.key,
            Data=record.value,
        )

    def list_shards_and_statuses(self) -> list[KinesisShard]:
        shards: list[KinesisShard] = []

        resp = self.kinesis.list_shards(StreamName=self.stream_name)
        for sh in resp.get("Shards", []):
            shard_id = sh["ShardId"]
            is_open = "EndingSequenceNumber" not in sh["SequenceNumberRange"]
            start = int(sh["HashKeyRange"]["StartingHashKey"])
            end = int(sh["HashKeyRange"]["EndingHashKey"])
            shards.append(
                KinesisShard(
                    shard_id=shard_id,
                    is_open=is_open,
                    hash_range_start=start,
                    hash_range_end=end,
                )
            )

        return shards

    def split_shard(self, shard: KinesisShard) -> None:
        n_expected_shards_after_split = len(self.list_shards_and_statuses()) + 2

        hash_range_midpoint = (
            shard.hash_range_start
            + (shard.hash_range_end - shard.hash_range_start) // 2
        )
        self.kinesis.split_shard(
            StreamName=self.stream_name,
            ShardToSplit=shard.shard_id,
            NewStartingHashKey=str(hash_range_midpoint),
        )

        for _ in range(100):
            try:
                shards = self.list_shards_and_statuses()
            except (
                BotoConnectionError,
                self.kinesis.exceptions.ResourceNotFoundException,
            ):
                time.sleep(1.0)
                continue
            shard_count_as_expected = len(shards) == n_expected_shards_after_split
            source_shard_status_is_updated = False
            for new_shard in shards:
                if new_shard.shard_id == shard.shard_id and not new_shard.is_open:
                    source_shard_status_is_updated = True
            if source_shard_status_is_updated and shard_count_as_expected:
                return
            time.sleep(1.0)

        raise RuntimeError("failed to wait for the target shards state after split")

    def merge_shards(self, shard_1: KinesisShard, shard_2: KinesisShard) -> None:
        if shard_1.hash_range_end + 1 != shard_2.hash_range_start:
            raise ValueError("shards are not adjacent")
        n_expected_shards_after_split = len(self.list_shards_and_statuses()) + 1

        self.kinesis.merge_shards(
            StreamName=self.stream_name,
            ShardToMerge=shard_1.shard_id,
            AdjacentShardToMerge=shard_2.shard_id,
        )

        for _ in range(100):
            try:
                shards = self.list_shards_and_statuses()
            except (
                BotoConnectionError,
                self.kinesis.exceptions.ResourceNotFoundException,
            ):
                time.sleep(1.0)
                continue
            shard_count_as_expected = len(shards) == n_expected_shards_after_split
            shard_1_status_is_updated = False
            shard_2_status_is_updated = False
            for new_shard in shards:
                if new_shard.shard_id == shard_1.shard_id and not new_shard.is_open:
                    shard_1_status_is_updated = True
                if new_shard.shard_id == shard_2.shard_id and not new_shard.is_open:
                    shard_2_status_is_updated = True
            if (
                shard_1_status_is_updated
                and shard_2_status_is_updated
                and shard_count_as_expected
            ):
                return
            time.sleep(1.0)

        raise RuntimeError("failed to wait for the target shards state after merge")

    def read_shard_records(self, shard_id) -> list[KinesisRecord]:
        iterator_resp = self.kinesis.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )

        shard_iterator = iterator_resp["ShardIterator"]
        result = []
        while shard_iterator:
            recs = self.kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
            for r in recs["Records"]:
                result.append(KinesisRecord(key=r["PartitionKey"], value=r["Data"]))
            shard_iterator = recs.get("NextShardIterator")
            if not recs["Records"]:
                break

        return result

    def _wait_stream_to_activate(self, deadline: float | None = None) -> None:
        if deadline is None:
            deadline = time.monotonic() + self._READY_TIMEOUT
        stream_status = None
        while True:
            try:
                desc = self.kinesis.describe_stream(StreamName=self.stream_name)
                stream_status = desc["StreamDescription"]["StreamStatus"]
            except (
                BotoConnectionError,
                self.kinesis.exceptions.ResourceNotFoundException,
            ):
                # The stream's metadata is not visible yet right after creation;
                # keep polling rather than failing.
                stream_status = None
            if stream_status == "ACTIVE":
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Kinesis stream {self.stream_name!r} did not become ACTIVE "
                    f"in time (last status: {stream_status})"
                )
            time.sleep(self._RETRY_INTERVAL)


def create_schema_in_registry(
    column_types: Mapping[str, str], required_columns: list[str]
) -> str:
    properties = {}
    for name, type_ in column_types.items():
        assert name not in properties
        properties[name] = {
            "type": type_,
        }
    schema_subject = str(uuid.uuid4())
    schema_basic = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Kafka Integration Tests Schema",
        "type": "object",
        "properties": properties,
        "required": required_columns,
        "additionalProperties": False,
    }
    payload = {"schemaType": "JSON", "schema": json.dumps(schema_basic)}
    # The schema-registry container may still be starting (connection refused)
    # or briefly returning 5xx while it comes up; both are transient, so retry
    # until the deadline. A 4xx is a real client error and is raised at once.
    deadline = time.monotonic() + SCHEMA_REGISTRY_READY_TIMEOUT
    while True:
        try:
            response = requests.post(
                f"{SCHEMA_REGISTRY_BASE_ROUTE}/subjects/{schema_subject}/versions",
                headers={
                    "Content-Type": "application/vnd.schemaregistry.v1+json",
                },
                json=payload,
            )
            response.raise_for_status()
            return schema_subject
        except requests.HTTPError:
            if response.status_code < 500 or time.monotonic() >= deadline:
                raise
        except (requests.ConnectionError, requests.Timeout):
            if time.monotonic() >= deadline:
                raise
        time.sleep(SCHEMA_REGISTRY_RETRY_INTERVAL)


def check_keys_in_file(
    path: pathlib.Path,
    output_format: str,
    expected_keys: set[str],
    expected_columns: set[str],
):
    keys = set()
    with open(path, "r") as f:
        for message in f:
            message = json.loads(message)["data"]
            if output_format == "json":
                value = json.loads(message)
                keys.add(value["k"])
                assert value.keys() == expected_columns
            else:
                keys.add(message)
        assert keys == expected_keys


class RabbitmqTestContext:
    """Creates a temporary RabbitMQ stream and provides helpers to send messages."""

    def __init__(self):
        self.stream_name = f"rmq-{uuid4()}"
        self.uri = RABBITMQ_STREAM_URI
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._run(self._create_stream())

    def teardown(self):
        self._run(self._cleanup())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def _run(self, coro):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result()

    @staticmethod
    def _new_producer() -> Producer:
        return Producer(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            username=RABBITMQ_USER,
            password=RABBITMQ_PASSWORD,
        )

    async def _create_stream(self):
        deadline = time.monotonic() + RABBITMQ_READY_TIMEOUT
        self._producer = await self._connect_producer(deadline)
        await self._ensure_stream(self.stream_name, deadline)

    # The CI healthcheck only waits for the stream port to accept TCP. A TCP
    # connection can therefore succeed while the broker still can't complete the
    # stream protocol handshake — the connection then resets or the handshake
    # request times out. Both are transient at startup, so retry with a fresh
    # producer (a half-started one can't be reused) until the deadline.
    async def _connect_producer(self, deadline: float) -> Producer:
        while True:
            producer = self._new_producer()
            try:
                await producer.start()
                return producer
            except RABBITMQ_TRANSIENT_ERRORS:
                try:
                    await producer.close()
                except Exception:
                    pass
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(RABBITMQ_RETRY_INTERVAL)

    # Create a stream and wait until it is fully usable. create_stream returns
    # before RabbitMQ has propagated the stream's metadata and elected a leader,
    # and under load the create request itself can be lost to a transient error.
    #
    # Crucially, re-querying the leader is not always enough on its own: under
    # load the stream coordinator can leave a freshly created stream permanently
    # without a leader (leader_ref 65535, with queries returning
    # StreamDoesNotExist forever). No amount of re-querying *that* stream will
    # fix it — which is why the previous retry-only loop still timed out at
    # setup. So if a stream does not become ready within
    # RABBITMQ_STREAM_READY_ATTEMPT we delete it and create a brand-new one,
    # repeating until the overall deadline; recreating forces the coordinator to
    # redo the leader election.
    async def _ensure_stream(self, name: str, deadline: float):
        while True:
            await self._create_stream_once(name, deadline)
            attempt_deadline = min(
                deadline, time.monotonic() + RABBITMQ_STREAM_READY_ATTEMPT
            )
            if await self._wait_until_stream_ready(name, attempt_deadline):
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"RabbitMQ stream {name!r} never elected a leader within "
                    f"{RABBITMQ_READY_TIMEOUT}s"
                )
            # Leader election for this stream is wedged; drop it and retry with a
            # fresh stream of the same name.
            await self._safe_delete_stream(name)
            await asyncio.sleep(RABBITMQ_RETRY_INTERVAL)

    # Issue a single create_stream, retrying only transient errors. A stream that
    # already exists (e.g. recreated after a wedged election) is fine.
    async def _create_stream_once(self, name: str, deadline: float):
        while True:
            try:
                await self._producer.create_stream(name)
                return
            except StreamAlreadyExists:
                return
            except RABBITMQ_TRANSIENT_ERRORS:
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(RABBITMQ_RETRY_INTERVAL)

    async def _safe_delete_stream(self, name: str):
        try:
            await self._producer.delete_stream(name)
        except Exception:
            pass

    # Query the stream's leader until it answers (the stream is then usable) or
    # the attempt deadline fires. Returns True if the stream became ready and
    # False if the deadline passed — the caller decides whether to recreate the
    # stream or give up. Querying can yield any transient error
    # (StreamDoesNotExist while metadata propagates, StreamNotAvailable while
    # resources start, LeaderNotAvailable while a leader is elected) or a
    # connection-level blip on the locator.
    async def _wait_until_stream_ready(self, name: str, deadline: float) -> bool:
        while True:
            try:
                client = await self._producer.default_client
                await client.query_leader_and_replicas(name)
                return True
            except RABBITMQ_TRANSIENT_ERRORS:
                if time.monotonic() >= deadline:
                    return False
                await asyncio.sleep(RABBITMQ_RETRY_INTERVAL)

    async def _cleanup(self):
        # Close the producer first so any buffered messages are flushed
        # while the stream still exists. Otherwise the flush that runs
        # inside close() races against delete_stream and fails with
        # StreamDoesNotExist.
        try:
            await self._producer.close()
        except Exception:
            pass
        # Reconnect with a fresh producer to delete the stream. Best-effort:
        # leftover streams accumulate on the broker across the session and add
        # to coordinator load, so retry the connection briefly rather than give
        # up on the first transient blip. Keep the window short — teardown must
        # not hang if the broker has actually gone away.
        try:
            cleanup_producer = await self._connect_producer(time.monotonic() + 15.0)
        except Exception:
            return
        try:
            await cleanup_producer.delete_stream(self.stream_name)
        except Exception:
            pass
        finally:
            try:
                await cleanup_producer.close()
            except Exception:
                pass

    def create_stream(self, name: str) -> None:
        self._run(self._create_stream_by_name(name))

    async def _create_stream_by_name(self, name: str):
        await self._ensure_stream(name, time.monotonic() + RABBITMQ_READY_TIMEOUT)

    def delete_stream(self, name: str) -> None:
        self._run(self._delete_stream_by_name(name))

    async def _delete_stream_by_name(self, name: str):
        try:
            await self._producer.delete_stream(name)
        except Exception:
            pass

    def send(self, message: str) -> None:
        self._run(self._send_async(message))

    async def _send_async(self, message: str) -> None:
        amqp_message = AMQPMessage(body=message.encode())
        # send_wait publishes synchronously and waits for the broker's
        # confirmation, so the message is durably committed — in its own stream
        # chunk, with the server-side timestamp set — before the caller
        # proceeds. The buffered send() flushes only every
        # default_batch_publishing_delay (3 s by default), which under load
        # races with timestamp-sensitive tests and can pack messages sent
        # seconds apart into a single chunk, defeating per-message timestamp
        # filtering (test_rabbitmq_start_from_timestamp).
        await self._producer.send_wait(
            self.stream_name, amqp_message, timeout=RABBITMQ_SEND_TIMEOUT
        )


PULSAR_HOST = os.environ.get("PULSAR_HOST", "pulsar")
PULSAR_PORT = int(os.environ.get("PULSAR_PORT", "6650"))
PULSAR_SERVICE_URI = f"pulsar://{PULSAR_HOST}:{PULSAR_PORT}"

# How long a verification consumer waits for a single message before deciding
# that no further messages arrive.
PULSAR_RECEIVE_TIMEOUT_MS = 30000


class PulsarTestContext:
    """Provides a unique topic and pulsar-client helpers for the tests.

    The topic is not created explicitly: Pulsar auto-creates topics on the
    first producer or subscription, which is also how the connector under test
    interacts with the broker.
    """

    def __init__(self):
        # The `pulsar` package (pulsar-client) is imported lazily so that the
        # remaining suites of this directory don't depend on it.
        import pulsar

        self._pulsar = pulsar
        self.topic = f"pulsar-{uuid4()}"
        self.service_uri = PULSAR_SERVICE_URI
        self._client = pulsar.Client(
            self.service_uri, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Warn)
        )

    def create_partitioned_topic(self, partitions: int) -> str:
        """Creates a fresh partitioned topic over the admin API and returns
        its name. Pulsar auto-creates only non-partitioned topics, so the
        partitioned ones must be created explicitly."""
        topic = f"pulsar-partitioned-{uuid4()}"
        response = requests.put(
            f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/"
            f"{topic}/partitions",
            json=partitions,
            timeout=60,
        )
        response.raise_for_status()
        return topic

    def teardown(self):
        self._client.close()

    def send(
        self,
        message: str,
        topic: str | None = None,
        properties: Mapping[str, str] | None = None,
        key: str | None = None,
    ) -> None:
        producer = self._client.create_producer(topic or self.topic)
        try:
            # `send` waits for the broker's acknowledgement, so the message is
            # durably accepted (with its publish timestamp assigned) before
            # the caller proceeds.
            producer.send(
                message.encode(),
                properties=dict(properties) if properties is not None else None,
                partition_key=key,
            )
        finally:
            producer.close()

    def read_messages(self, expected_count: int, topic: str | None = None) -> list:
        """Reads `expected_count` messages from the earliest position.

        Returns the raw pulsar-client messages, so the callers can inspect
        payloads, properties and partition keys. Fails if the expected number
        of messages doesn't arrive in time.
        """
        consumer = self._client.subscribe(
            topic or self.topic,
            subscription_name=f"verifier-{uuid4()}",
            initial_position=self._pulsar.InitialPosition.Earliest,
        )
        messages: list = []
        try:
            while len(messages) < expected_count:
                message = consumer.receive(timeout_millis=PULSAR_RECEIVE_TIMEOUT_MS)
                consumer.acknowledge(message)
                messages.append(message)
        finally:
            consumer.close()
        return messages


PULSAR_ADMIN_URL = os.environ.get("PULSAR_ADMIN_URL", "http://pulsar:8080")

PULSAR_AUTH_HOST = os.environ.get("PULSAR_AUTH_HOST", "pulsar-auth")
PULSAR_AUTH_PORT = int(os.environ.get("PULSAR_AUTH_PORT", "6650"))
PULSAR_AUTH_SERVICE_URI = f"pulsar://{PULSAR_AUTH_HOST}:{PULSAR_AUTH_PORT}"

# Static test-only JWT for the token-authenticated broker (the `pulsar-auth`
# service of the docker-compose environment). The broker is configured with
# the matching secret key; both were generated with `bin/pulsar tokens ...`.
PULSAR_AUTH_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwYXRod2F5LXRlc3QifQ"
    ".nQ-ly33j_vVTCvmTWoW9AcdLuygrNKXG2-oIrz5OmY8"
)


# --- The shared SIGKILL persistence scenario ------------------------------
#
# Used by the MQTT, NATS JetStream and Pulsar persistence tests. Each test
# provides a broker-specific publisher and an identity-pipeline subprocess
# (a small pathway program that copies the topic to a JSONL file with
# persistence enabled); the scenario itself — warm up, publish a stream,
# SIGKILL mid-flight, publish more while the pipeline is down, restart from
# the same persistent storage and verify that nothing was lost — is the same
# for every broker.

SIGKILL_SNAPSHOT_INTERVAL_MS = 1000
SIGKILL_SUBSCRIPTION_TIMEOUT_SEC = 60
SIGKILL_COMPLETION_TIMEOUT_SEC = 90


class IdentityPipelineRun:
    """One run of an identity pipeline subprocess.

    The subprocess is expected to accept ``--output``, ``--pstorage`` and
    ``--snapshot-interval-ms`` (appended here); the broker-specific arguments
    are passed through ``program_args``.
    """

    def __init__(self, tmp_path, program_path, program_args, run_index):
        self.output_path = tmp_path / f"output-{run_index}.jsonl"
        self._log_path = tmp_path / f"pw-log-{run_index}.txt"
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        with open(self._log_path, "wb") as log_file:
            self._process = subprocess.Popen(
                [
                    sys.executable,
                    program_path,
                    *program_args,
                    "--output",
                    str(self.output_path),
                    "--pstorage",
                    str(tmp_path / "pstorage"),
                    "--snapshot-interval-ms",
                    str(SIGKILL_SNAPSHOT_INTERVAL_MS),
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


def _wait_until_received(run, publish, marker, timeout):
    """Publish `marker` repeatedly until it shows up in the run's output.

    Publishing repeatedly is the only reliable way to know the reader is
    subscribed and flowing: a marker published before the subscription was
    set up may never be delivered.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run.assert_alive()
        publish(marker)
        if marker in run.received_payloads():
            return
        time.sleep(0.5)
    raise AssertionError(
        f"marker {marker!r} did not reach the output in {timeout} seconds; "
        f"log tail:\n{run.log_tail()}"
    )


def assert_messages_survive_sigkill_restart(make_run, publish, loss_description):
    """Drives the shared SIGKILL at-least-once scenario.

    Args:
        make_run: ``make_run(run_index) -> IdentityPipelineRun`` — starts one
            run of the identity pipeline; the consecutive runs must share the
            persistent storage.
        publish: ``publish(payload: str)`` — durably publishes one message to
            the topic the pipeline reads; must return only after the broker
            has accepted the message.
        loss_description: what to call the lost messages in the failure text,
            e.g. ``"QoS1 messages"``.
    """
    # Run 0: start the pipeline and wait until it demonstrably receives data.
    run0 = make_run(0)
    try:
        _wait_until_received(run0, publish, "warmup", SIGKILL_SUBSCRIPTION_TIMEOUT_SEC)

        # Publish a continuous stream and SIGKILL the pipeline mid-flight, so
        # that some delivered messages have not made it into a checkpoint yet.
        for i in range(1500):
            publish(f"msg-{i:05d}")
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

    # While the pipeline is down, the topic continues to receive data. This
    # window must be covered by the broker-side reading position (a durable
    # subscription, consumer or session), not by the pipeline's checkpoints.
    for i in range(1500, 2000):
        publish(f"msg-{i:05d}")

    # Run 1: restart from the same persistent storage and wait until the
    # pipeline has caught up with a fresh marker.
    run1 = make_run(1)
    try:
        _wait_until_received(
            run1, publish, "final-marker", SIGKILL_COMPLETION_TIMEOUT_SEC
        )

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
            f"{len(missing)} of {len(expected)} {loss_description} were lost "
            f"across the SIGKILL restart, e.g. {missing[:10]}; "
            f"log tail:\n{run1.log_tail()}"
        )
    finally:
        run1.stop()

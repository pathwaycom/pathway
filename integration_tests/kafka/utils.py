# Copyright © 2026 Pathway

import asyncio
import dataclasses
import json
import os
import pathlib
import threading
import time
import uuid
from collections.abc import Iterable
from typing import Mapping
from uuid import uuid4

import boto3
import requests
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from rstream import AMQPMessage, Producer
from rstream.exceptions import (
    ClientError,
    LeaderNotAvailable,
    StreamAlreadyExists,
    StreamDoesNotExist,
    StreamNotAvailable,
)

KAFKA_SETTINGS = {"bootstrap_servers": "kafka:9092"}
MQTT_BASE_ROUTE = "mqtt://mqtt:1883?client_id=$CLIENT_ID"
SCHEMA_REGISTRY_BASE_ROUTE = "http://schema-registry:8081"
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
RABBITMQ_TRANSIENT_ERRORS = (
    ConnectionError,
    TimeoutError,
    ClientError,
    StreamDoesNotExist,
    StreamNotAvailable,
    LeaderNotAvailable,
)


def random_topic_name():
    return f"integration-tests-{uuid4()}"


class KafkaTestContext:
    _producer: KafkaProducer
    _admin: KafkaAdminClient
    _input_topic: str
    _output_topic: str

    def __init__(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"]
        )
        self._admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"],
        )
        self._input_topic = random_topic_name()
        self._output_topic = random_topic_name()
        self._created_topics: set[str] = set()

        self._create_topic(self.input_topic)
        self._create_topic(self.output_topic)

    def create_additional_topic(self) -> str:
        topic_name = random_topic_name()
        self._create_topic(topic_name)
        return topic_name

    def _create_topic(self, name: str, num_partitions: int = 1) -> None:
        self._admin.create_topics(
            [NewTopic(name=name, num_partitions=num_partitions, replication_factor=1)]
        )
        self._created_topics.add(name)

    def _delete_topic(self, name: str) -> None:
        self._admin.delete_topics(topics=[name])

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

        self._producer.send(
            topic,
            key=key,
            value=value,
            headers=headers,
        )

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
        self._producer.flush()

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

    def __init__(self, stream_name: str | None = None) -> None:
        self.stream_name = stream_name or str(uuid4())
        self.kinesis = boto3.client(
            "kinesis",
            region_name="us-east-1",
            endpoint_url=KINESIS_ENDPOINT_URL,
            aws_access_key_id="placeholder",
            aws_secret_access_key="placeholder",
        )
        self.kinesis.create_stream(StreamName=self.stream_name, ShardCount=1)
        self._wait_stream_to_activate()

    def recreate(self, shard_count: int) -> None:
        self.stream_name = str(uuid4())
        self.kinesis.create_stream(StreamName=self.stream_name, ShardCount=shard_count)
        self._wait_stream_to_activate()

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
            shards = self.list_shards_and_statuses()
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
            shards = self.list_shards_and_statuses()
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

    def _wait_stream_to_activate(self) -> None:
        stream_status = None
        for _ in range(100):
            desc = self.kinesis.describe_stream(StreamName=self.stream_name)
            stream_status = desc["StreamDescription"]["StreamStatus"]
            if stream_status == "ACTIVE":
                break
            time.sleep(1)
        assert stream_status == "ACTIVE"


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
    response = requests.post(
        f"{SCHEMA_REGISTRY_BASE_ROUTE}/subjects/{schema_subject}/versions",
        headers={
            "Content-Type": "application/vnd.schemaregistry.v1+json",
        },
        json=payload,
    )
    response.raise_for_status()
    return schema_subject


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

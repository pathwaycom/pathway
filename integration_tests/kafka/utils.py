# Copyright © 2024 Pathway

import dataclasses
import json
import pathlib
import time
import uuid
from collections.abc import Iterable
from typing import Mapping
from uuid import uuid4

import boto3
import requests
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.consumer.fetcher import ConsumerRecord

KAFKA_SETTINGS = {"bootstrap_servers": "kafka:9092"}
MQTT_BASE_ROUTE = "mqtt://mqtt:1883?client_id=$CLIENT_ID"
SCHEMA_REGISTRY_BASE_ROUTE = "http://schema-registry:8081"
KINESIS_ENDPOINT_URL = "http://kinesis:4567"


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
        self, message: str | tuple[str | bytes | None, str | bytes | None], topic=None
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

        self._producer.send(topic, key=key, value=value)

    def set_input_topic_partitions(self, num_partitions: int):
        self._delete_topic(self._input_topic)
        self._create_topic(self._input_topic, num_partitions)

    def fill(self, messages: Iterable[str | tuple[str, str]]) -> None:
        for msg in messages:
            self.send(msg)
        self._producer.flush()

    def read_topic(self, topic, poll_timeout_ms: int = 1000) -> list[ConsumerRecord]:
        consumer = KafkaConsumer(
            topic,
            auto_offset_reset="earliest",
            bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"],
        )
        messages = []
        while True:
            poll_result = consumer.poll(poll_timeout_ms)
            if not poll_result:
                break
            for topic_partition, new_messages in poll_result.items():
                assert (
                    topic_partition.topic == topic
                ), "Poller returns messages from an unexpected topic"
                messages += new_messages
        return messages

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

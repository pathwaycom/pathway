# Copyright © 2024 Pathway

import json
import pathlib
import uuid
from collections.abc import Iterable
from typing import Mapping
from uuid import uuid4

import requests
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.consumer.fetcher import ConsumerRecord

KAFKA_SETTINGS = {"bootstrap_servers": "kafka:9092"}
MQTT_BASE_ROUTE = "mqtt://mqtt:1883?client_id=$CLIENT_ID"
SCHEMA_REGISTRY_BASE_ROUTE = "http://schema-registry:8081"


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
        self._input_topic = f"integration-tests-{uuid4()}"
        self._output_topic = f"integration-tests-{uuid4()}"
        self._create_topic(self.input_topic)
        self._create_topic(self.output_topic)

    def _create_topic(self, name: str, num_partitions: int = 1) -> None:
        self._admin.create_topics(
            [NewTopic(name=name, num_partitions=num_partitions, replication_factor=1)]
        )

    def _delete_topic(self, name: str) -> None:
        self._admin.delete_topics(topics=[name])

    def send(self, message: str | tuple[str, str]) -> None:
        if isinstance(message, tuple):
            (key, value) = message
        else:
            (key, value) = str(uuid4()), message
        self._producer.send(self.input_topic, key=key.encode(), value=value.encode())

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
        self._delete_topic(self.input_topic)
        self._delete_topic(self.output_topic)
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

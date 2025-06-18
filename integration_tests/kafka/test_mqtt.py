import json
from uuid import uuid4

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    FileLinesNumberChecker,
    wait_result_with_checker,
)

from .utils import MQTT_BASE_ROUTE, check_keys_in_file


def run_identity_program(
    input_file,
    output_file,
    qos,
    mqtt_topic,
    mqtt_reader_uri,
    mqtt_writer_uri,
    new_entries,
    persistence_config=None,
):
    with open(input_file, "a") as f:
        for entry in new_entries:
            f.write(entry)
            f.write("\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.mqtt.write(
        table,
        uri=mqtt_writer_uri,
        topic=mqtt_topic,
        format="json",
        qos=qos,
    )

    class InputSchema(pw.Schema):
        data: str

    table_reread = pw.io.mqtt.read(
        uri=mqtt_reader_uri,
        topic=mqtt_topic,
        schema=InputSchema,
        format="json",
        qos=qos,
    )
    pw.io.csv.write(table_reread, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, len(new_entries)),
        30,
        kwargs={"persistence_config": persistence_config},
    )


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_mqtt_simple(tmp_path, mqtt_context, qos):
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.txt"
    run_identity_program(
        input_path,
        output_path,
        qos,
        mqtt_context.topic,
        mqtt_context.reader_connection_string,
        mqtt_context.writer_connection_string,
        ["one", "two", "three", "four", "five"],
    )


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_mqtt_dynamic_topic(tmp_path, qos):
    input_path = tmp_path / "input.txt"
    dynamic_topic_1 = str(uuid4())
    dynamic_topic_2 = str(uuid4())
    with open(input_path, "w") as f:
        f.write(json.dumps({"k": "0", "v": "foo", "t": dynamic_topic_1}))
        f.write("\n")
        f.write(json.dumps({"k": "1", "v": "bar", "t": dynamic_topic_2}))
        f.write("\n")
        f.write(json.dumps({"k": "2", "v": "baz", "t": dynamic_topic_1}))
        f.write("\n")

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: str
        t: str

    writer_uri = MQTT_BASE_ROUTE.replace("$CLIENT_ID", f"writer-{str(uuid4())}")
    table = pw.io.jsonlines.read(input_path, schema=InputSchema)
    pw.io.mqtt.write(
        table,
        uri=writer_uri,
        topic=table.select(topic="MqttTopic-" + pw.this.t).topic,
        format="json",
        qos=qos,
    )

    output_path_1 = tmp_path / "output_1.txt"
    output_path_2 = tmp_path / "output_2.txt"
    reader_uri_1 = MQTT_BASE_ROUTE.replace("$CLIENT_ID", f"reader-{str(uuid4())}")
    reader_uri_2 = MQTT_BASE_ROUTE.replace("$CLIENT_ID", f"reader-{str(uuid4())}")
    stream_1 = pw.io.mqtt.read(
        uri=reader_uri_1,
        topic=f"MqttTopic-{dynamic_topic_1}",
        format="plaintext",
        qos=qos,
    )
    stream_2 = pw.io.mqtt.read(
        uri=reader_uri_2,
        topic=f"MqttTopic-{dynamic_topic_2}",
        format="plaintext",
        qos=qos,
    )
    pw.io.jsonlines.write(stream_1, output_path_1)
    pw.io.jsonlines.write(stream_2, output_path_2)
    wait_result_with_checker(
        FileLinesNumberChecker(output_path_1, 2).add_path(output_path_2, 1), 30
    )
    check_keys_in_file(
        output_path_1,
        "json",
        {"0", "2"},
        {"k", "v", "t", "time", "diff", "topic"},
    )
    check_keys_in_file(
        output_path_2, "json", {"1"}, {"k", "v", "t", "time", "diff", "topic"}
    )

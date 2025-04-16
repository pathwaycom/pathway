# Copyright © 2024 Pathway

import base64
import json
import pathlib
import threading
import time
import uuid

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    expect_csv_checker,
    wait_result_with_checker,
)

from .utils import KafkaTestContext


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.flaky(reruns=3)
def test_kafka_raw(with_metadata, tmp_path, kafka_context):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
        with_metadata=with_metadata,
    )

    pw.io.csv.write(table, tmp_path / "output.csv")

    wait_result_with_checker(
        expect_csv_checker(
            """
            data
            foo
            bar
            """,
            tmp_path / "output.csv",
            usecols=["data"],
            index_col=["data"],
        ),
        10,
    )


@pytest.mark.flaky(reruns=3)
def test_kafka_static_mode(tmp_path, kafka_context):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
        mode="static",
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    pw.run()
    result = set()
    with open(tmp_path / "output.jsonl", "r") as f:
        for row in f:
            result.add(json.loads(row)["data"])
    assert result == set({"foo", "bar"})


@pytest.mark.flaky(reruns=3)
def test_kafka_message_metadata(tmp_path, kafka_context):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
        with_metadata=True,
    )
    output_path = tmp_path / "output.jsonl"

    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 10)

    offsets = set()
    with open(output_path, "r") as f:
        for row in f:
            data = json.loads(row)
            metadata = data["_metadata"]
            assert metadata["topic"] == kafka_context.input_topic
            assert "partition" in metadata
            assert "offset" in metadata
            offsets.add(metadata["offset"])

    assert len(offsets) == 2


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.flaky(reruns=3)
def test_kafka_json(tmp_path, kafka_context, with_metadata):
    kafka_context.fill(
        [
            json.dumps({"k": 0, "v": "foo"}),
            json.dumps({"k": 1, "v": "bar"}),
            json.dumps({"k": 2, "v": "baz"}),
        ]
    )

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        schema=InputSchema,
        with_metadata=with_metadata,
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, tmp_path / "output.csv")

    wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            0    | foo
            1    | bar
            2    | baz
            """,
            tmp_path / "output.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
    )


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.flaky(reruns=3)
def test_kafka_csv(tmp_path, kafka_context, with_metadata):
    kafka_context.fill(
        [
            "k,v",
            "0,foo",
            "1,bar",
            "2,baz",
        ]
    )

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="csv",
        schema=InputSchema,
        with_metadata=with_metadata,
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, tmp_path / "output.csv")

    wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            0    | foo
            1    | bar
            2    | baz
            """,
            tmp_path / "output.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
    )


@pytest.mark.flaky(reruns=3)
def test_kafka_simple_wrapper_bytes_io(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.simple_read(
        kafka_context.default_rdkafka_settings()["bootstrap.servers"],
        kafka_context.input_topic,
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    wait_result_with_checker(FileLinesNumberChecker(tmp_path / "output.jsonl", 2), 10)

    # check that reread will have all these messages again
    G.clear()
    table = pw.io.kafka.simple_read(
        kafka_context.default_rdkafka_settings()["bootstrap.servers"],
        kafka_context.input_topic,
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    wait_result_with_checker(FileLinesNumberChecker(tmp_path / "output.jsonl", 2), 10)

    # Check output type, bytes should be rendered as an array
    with open(tmp_path / "output.jsonl", "r") as f:
        for row in f:
            row_parsed = json.loads(row)
            assert isinstance(row_parsed["data"], str)
            decoded = base64.b64decode(row_parsed["data"])
            assert decoded in (b"foo", b"bar")


@pytest.mark.flaky(reruns=3)
def test_kafka_simple_wrapper_plaintext_io(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.simple_read(
        kafka_context.default_rdkafka_settings()["bootstrap.servers"],
        kafka_context.input_topic,
        format="plaintext",
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    wait_result_with_checker(FileLinesNumberChecker(tmp_path / "output.jsonl", 2), 10)

    # check that reread will have all these messages again
    G.clear()
    table = pw.io.kafka.simple_read(
        kafka_context.default_rdkafka_settings()["bootstrap.servers"],
        kafka_context.input_topic,
        format="plaintext",
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    wait_result_with_checker(FileLinesNumberChecker(tmp_path / "output.jsonl", 2), 10)

    # Check output type, parsed plaintext should be a string
    with open(tmp_path / "output.jsonl", "r") as f:
        for row in f:
            row_parsed = json.loads(row)
            assert isinstance(row_parsed["data"], str)
            assert row_parsed["data"] == "foo" or row_parsed["data"] == "bar"


@pytest.mark.flaky(reruns=3)
def test_kafka_output(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    input_path = tmp_path / "input"
    with open(input_path, "w") as f:
        f.write("foo\nbar\n")

    table = pw.io.plaintext.read(
        str(input_path),
        mode="static",
    )
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
    )
    pw.run()

    output_topic_contents = kafka_context.read_output_topic()
    assert len(output_topic_contents) == 2


@pytest.mark.flaky(reruns=3)
def test_kafka_raw_bytes_output(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    input_path = tmp_path / "input"
    input_path.mkdir()
    (input_path / "foo").write_text("foo")
    (input_path / "bar").write_text("bar")

    table = pw.io.fs.read(
        input_path,
        mode="static",
        format="binary",
    )
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
        format="raw",
    )
    pw.run()

    output_topic_contents = kafka_context.read_output_topic()
    assert len(output_topic_contents) == 2


def get_test_binary_table(tmp_path):
    input_path = tmp_path / "input"
    input_path.mkdir()
    (input_path / "foo").write_text("foo")
    (input_path / "bar").write_text("bar")

    return pw.io.fs.read(
        input_path,
        mode="static",
        format="binary",
        with_metadata=True,
    )


@pytest.mark.flaky(reruns=3)
@pytest.mark.parametrize(
    "key",
    [None, "data"],
)
@pytest.mark.parametrize(
    "headers",
    [
        [],
        ["data"],
        ["data", "_metadata"],
    ],
)
def test_kafka_raw_bytes_output_select_index(
    key, headers, tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    def construct_raw_write_argument(table, name):
        if name is None:
            return None
        return table[name]

    def get_expected_headers(headers):
        expected_headers = ["pathway_time", "pathway_diff"]
        expected_headers.extend(headers)
        return expected_headers

    table = get_test_binary_table(tmp_path)
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
        format="raw",
        value=table.data,
        key=construct_raw_write_argument(table, key),
        headers=[construct_raw_write_argument(table, header) for header in headers],
    )
    pw.run()
    output_topic_contents = kafka_context.read_output_topic(
        expected_headers=get_expected_headers(headers)
    )
    assert len(output_topic_contents) == 2


@pytest.mark.flaky(reruns=3)
def test_kafka_output_rename_headers(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    table = get_test_binary_table(tmp_path)
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
        format="raw",
        key=pw.this.data,
        value=pw.this.data,
        headers=[*table.select(foo=pw.this.data, bar=pw.this._metadata)],
    )
    pw.run()
    output_topic_contents = kafka_context.read_output_topic(
        expected_headers=["pathway_time", "pathway_diff", "foo", "bar"]
    )
    assert len(output_topic_contents) == 2


@pytest.mark.flaky(reruns=3)
def test_kafka_plaintext_output(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    input_path = tmp_path / "input"
    input_path.mkdir()
    (input_path / "foo").write_text("foo")
    (input_path / "bar").write_text("bar")

    table = pw.io.fs.read(
        input_path,
        mode="static",
        format="plaintext",
    )
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
        format="plaintext",
    )
    pw.run()

    output_topic_contents = kafka_context.read_output_topic()
    assert len(output_topic_contents) == 2


@pytest.mark.flaky(reruns=3)
def test_kafka_recovery(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    persistent_storage_path = tmp_path / "PStorage"

    kafka_context.fill(
        [
            json.dumps({"k": 0, "v": "foo"}),
            json.dumps({"k": 1, "v": "bar"}),
            json.dumps({"k": 2, "v": "baz"}),
        ]
    )

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        schema=pw.schema_builder(
            columns={
                "k": pw.column_definition(dtype=int, primary_key=True),
                "v": pw.column_definition(dtype=str),
            }
        ),
        autocommit_duration_ms=100,
        name="1",
    )

    pw.io.csv.write(table, tmp_path / "output.csv")

    wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            0    | foo
            1    | bar
            2    | baz
            """,
            tmp_path / "output.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
        kwargs={
            "persistence_config": pw.persistence.Config(
                pw.persistence.Backend.filesystem(persistent_storage_path),
            ),
        },
    )
    G.clear()

    # fill doesn't replace the messages, so we append 3 new ones
    kafka_context.fill(
        [
            json.dumps({"k": 3, "v": "foofoo"}),
            json.dumps({"k": 4, "v": "barbar"}),
            json.dumps({"k": 5, "v": "bazbaz"}),
        ]
    )

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        schema=pw.schema_builder(
            columns={
                "k": pw.column_definition(dtype=int, primary_key=True),
                "v": pw.column_definition(dtype=str),
            }
        ),
        autocommit_duration_ms=100,
        name="1",
    )

    pw.io.csv.write(table, tmp_path / "output_backfilled.csv")
    wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            3    | foofoo
            4    | barbar
            5    | bazbaz
            """,
            tmp_path / "output_backfilled.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
        target=pw.run,
        kwargs={
            "persistence_config": pw.persistence.Config(
                pw.persistence.Backend.filesystem(persistent_storage_path),
            ),
        },
    )


@pytest.mark.flaky(reruns=3)
def test_start_from_timestamp_ms_seek_to_middle(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    kafka_context.fill(["foo", "bar"])
    time.sleep(10)
    start_from_timestamp_ms = (int(time.time()) - 5) * 1000
    kafka_context.fill(["qqq", "www"])

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
        start_from_timestamp_ms=start_from_timestamp_ms,
    )

    pw.io.csv.write(table, tmp_path / "output.csv")

    wait_result_with_checker(
        expect_csv_checker(
            """
            data
            qqq
            www
            """,
            tmp_path / "output.csv",
            usecols=["data"],
            index_col=["data"],
        ),
        10,
    )


@pytest.mark.flaky(reruns=3)
def test_start_from_timestamp_ms_seek_to_beginning(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    kafka_context.fill(["foo", "bar"])
    start_from_timestamp_ms = (int(time.time()) - 3600) * 1000

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
        start_from_timestamp_ms=start_from_timestamp_ms,
    )

    pw.io.csv.write(table, tmp_path / "output.csv")

    wait_result_with_checker(
        expect_csv_checker(
            """
            data
            foo
            bar
            """,
            tmp_path / "output.csv",
            usecols=["data"],
            index_col=["data"],
        ),
        10,
    )


@pytest.mark.flaky(reruns=3)
def test_start_from_timestamp_ms_seek_to_end(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    kafka_context.fill(["foo", "bar"])
    time.sleep(10)
    start_from_timestamp_ms = int(time.time() - 5) * 1000

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
        start_from_timestamp_ms=start_from_timestamp_ms,
    )

    def stream_inputs():
        for i in range(10):
            kafka_context.fill([str(i)])
            time.sleep(1)

    t = threading.Thread(target=stream_inputs, daemon=True)
    t.run()

    pw.io.csv.write(table, tmp_path / "output.csv")

    wait_result_with_checker(
        expect_csv_checker(
            """
            data
            0
            1
            2
            3
            4
            5
            6
            7
            8
            9
            """,
            tmp_path / "output.csv",
            usecols=["data"],
            index_col=["data"],
        ),
        30,
    )


@pytest.mark.flaky(reruns=3)
def test_kafka_json_key(tmp_path, kafka_context):
    input_path = tmp_path / "input.jsonl"
    with open(input_path, "w") as f:
        f.write(json.dumps({"k": 0, "v": "foo"}))
        f.write("\n")
        f.write(json.dumps({"k": 1, "v": "bar"}))
        f.write("\n")
        f.write(json.dumps({"k": 2, "v": "baz"}))
        f.write("\n")

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
        format="json",
        key=table["v"],
        headers=[table["k"], table["v"]],
    )
    pw.run()
    output_topic_contents = kafka_context.read_output_topic()
    for message in output_topic_contents:
        key = message.key
        value = json.loads(message.value)
        assert value["v"].encode("utf-8") == key
        assert "k" in value
        headers = {}
        for header_key, header_value in message.headers:
            headers[header_key] = header_value
        assert headers["k"] == str(value["k"]).encode("utf-8")
        assert headers["v"] == f'"{value["v"]}"'.encode("utf-8")


@pytest.mark.parametrize("output_format", ["json", "plaintext"])
@pytest.mark.flaky(reruns=3)
def test_kafka_dynamic_topics(tmp_path, kafka_context, output_format):
    input_path = tmp_path / "input.jsonl"
    dynamic_topic_1 = str(uuid.uuid4())
    dynamic_topic_2 = str(uuid.uuid4())
    kafka_context._create_topic(f"KafkaTopic.{dynamic_topic_1}")
    kafka_context._create_topic(f"KafkaTopic.{dynamic_topic_2}")
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

    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
    if output_format == "json":
        write_kwargs = {"format": "json"}
    elif output_format == "plaintext":
        write_kwargs = {
            "format": "plaintext",
            "key": table.k,
            "value": table.v,
        }
    else:
        raise RuntimeError(f"Unknown output format: {output_format}")
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=table.select(topic="KafkaTopic." + pw.this.t)["topic"],
        **write_kwargs,
    )
    pw.run()

    def check_keys_in_topic(topic_name, expected_keys):
        keys = set()
        output_topic_contents = kafka_context.read_topic(topic_name)
        for message in output_topic_contents:
            if output_format == "json":
                value = json.loads(message.value)
                keys.add(value["k"])
                assert value.keys() == {"k", "v", "t", "time", "diff", "topic"}
            else:
                keys.add(message.key.decode("utf-8"))
        assert keys == expected_keys

    check_keys_in_topic(f"KafkaTopic.{dynamic_topic_1}", {"0", "2"})
    check_keys_in_topic(f"KafkaTopic.{dynamic_topic_2}", {"1"})

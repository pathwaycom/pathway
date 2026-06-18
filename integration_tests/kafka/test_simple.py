# Copyright © 2026 Pathway

import base64
import datetime
import inspect
import json
import pathlib
import threading
import time
import uuid
import warnings

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    expect_csv_checker,
    wait_result_with_checker,
)

from .utils import (
    KAFKA_SETTINGS,
    SCHEMA_REGISTRY_BASE_ROUTE,
    KafkaTestContext,
    create_schema_in_registry,
)


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


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize("input_format", ["plaintext", "raw"])
@pytest.mark.flaky(reruns=3)
def test_kafka_key_parsing(input_format, with_metadata, tmp_path, kafka_context):
    context = [
        ("1", "one"),
        ("2", "two"),
        ("3", "three"),
        ("4", None),
        (None, "five"),
    ]
    kafka_context.fill(context)

    output_path = tmp_path / "output.jsonl"
    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format=input_format,
        autocommit_duration_ms=100,
        with_metadata=with_metadata,
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    parsed_values = []
    with open(output_path, "r") as f:
        for row in f:
            data = json.loads(row)
            key = data["key"]
            value = data["data"]
            if input_format == "raw" and key is not None:
                key = base64.b64decode(key).decode("utf-8")
            if input_format == "raw" and value is not None:
                value = base64.b64decode(value).decode("utf-8")
            parsed_values.append((key, value))
    parsed_values.sort(key=lambda data: str(data[0]))
    context.sort(key=lambda data: str(data[0]))
    assert parsed_values == context


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
    test_kafka_foo_message_headers = [
        ("X-Sender-ID", b"pathway-integration-test"),
        ("X-Trace-ID", b"a8acf0a5-009f-4035-9aca-834bc85929f9"),
        ("X-Trace-ID", b"7a21cee9-c081-4d64-add1-06e2e5e592d6"),
        ("X-Origin", b""),
        ("X-Signature", bytes([0, 255, 128, 10])),
    ]
    test_kafka_bar_message_headers = [
        ("X-Sender-ID", b"pathway-integration-test"),
        ("X-Trace-ID", b"ee6e3017-d77f-43d9-abf6-c33bd51e27ef"),
        ("X-Trace-ID", b"092565ae-aa1e-406c-a53f-d2c4d6f2397c"),
        ("X-Trace-ID", b"1d0ae9e7-8cac-40d8-9072-3d1a919a2fef"),
        ("X-Origin", b"Server"),
        ("X-Signature", bytes([0, 255, 128, 10, 17])),
    ]

    def check_headers(parsed: list[list[str]], original: list[tuple[str, bytes]]):
        decoded_headers = []
        for key, value in parsed:
            decoded_value = base64.b64decode(value)
            decoded_headers.append((key, decoded_value))
        decoded_headers.sort()
        original.sort()
        assert decoded_headers == original

    kafka_context.fill(["foo"], headers=test_kafka_foo_message_headers)
    kafka_context.fill(["bar"], headers=test_kafka_bar_message_headers)

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

            assert "headers" in metadata
            headers = metadata["headers"]
            if data["data"] == "foo":
                check_headers(headers, test_kafka_foo_message_headers)
            elif data["data"] == "bar":
                check_headers(headers, test_kafka_bar_message_headers)
            else:
                raise ValueError(f"unknown message data: {data['data']}")

    assert len(offsets) == 2


# Python client for Kafka doesn't allow null header body, while it's still allowed
# by the protocol. Hence we test it, but differently.
def test_null_header(tmp_path, kafka_context):
    output_path = tmp_path / "output.jsonl"
    kafka_context.fill(
        [
            json.dumps({"k": 0, "hdr": "foo"}),
            json.dumps(
                {"k": 1, "hdr": None}
            ),  # We output this as a header having no value
            json.dumps({"k": 2, "hdr": "bar"}),
        ]
    )

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        hdr: str | None

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        mode="static",
        schema=InputSchema,
    )
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
        format="json",
        headers=[pw.this.hdr],
    )
    pw.run()
    G.clear()

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.output_topic,
        format="json",
        mode="static",
        schema=InputSchema,
        with_metadata=True,
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    n_rows = 0
    with open(output_path, "r") as f:
        for row in f:
            data = json.loads(row)
            key = data["k"]
            metadata = data["_metadata"]
            headers = [
                h for h in metadata["headers"] if not h[0].startswith("pathway_")
            ]
            assert len(headers) == 1
            header_key, header_value = headers[0]
            header_value = (
                base64.b64decode(header_value) if header_value is not None else None
            )
            assert header_key == "hdr"
            if key == 0:
                assert header_value == b"foo"
            elif key == 1:
                assert header_value is None
            elif key == 2:
                assert header_value == b"bar"
            else:
                raise ValueError(f"unknown key: {key}")
            n_rows += 1

    assert n_rows == 3


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
def test_kafka_json_key_parsing(tmp_path, kafka_context, with_metadata):
    context = [
        (json.dumps({"k": 0}), json.dumps({"v": "foo"})),
        (json.dumps({"k": 1}), json.dumps({"v": "bar"})),
        (json.dumps({"k": 2}), json.dumps({"v": "baz"})),
    ]
    kafka_context.fill(context)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True, source_component="key")
        v: str = pw.column_definition(primary_key=True, source_component="payload")

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
def test_kafka_json_key_jsonpaths(tmp_path, kafka_context, with_metadata):
    context = [
        (json.dumps({"k": {"l": 0, "m": 3}}), json.dumps({"v": {"vv": "foo"}})),
        (json.dumps({"k": {"l": 1, "m": 4}}), json.dumps({"v": {"vv": "bar"}})),
        (json.dumps({"k": {"l": 2, "m": 5}}), json.dumps({"v": {"vv": "baz"}})),
    ]
    kafka_context.fill(context)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True, source_component="key")
        v: str = pw.column_definition(primary_key=True, source_component="payload")

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        schema=InputSchema,
        with_metadata=with_metadata,
        autocommit_duration_ms=100,
        json_field_paths={"k": "/k/l", "v": "/v/vv"},
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
@pytest.mark.parametrize("unparsable_value", ["abracadabra", None])
@pytest.mark.flaky(reruns=3)
def test_kafka_json_data_only_in_key(
    tmp_path, unparsable_value, kafka_context, with_metadata
):
    context = [
        (json.dumps({"k": 0, "v": "foo"}), unparsable_value),
        (json.dumps({"k": 1, "v": "bar"}), unparsable_value),
        (json.dumps({"k": 2, "v": "baz"}), unparsable_value),
    ]
    kafka_context.fill(context)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True, source_component="key")
        v: str = pw.column_definition(primary_key=True, source_component="key")

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
        assert headers["v"] == value["v"].encode("utf-8")


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


@pytest.mark.flaky(reruns=3)
def test_kafka_registry(tmp_path, kafka_context):
    schema_subject = create_schema_in_registry(
        column_types={
            "key": "integer",
            "value": "string",
            "time": "integer",
            "diff": "integer",
        },
        required_columns=["key", "value", "time", "diff"],
    )

    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output.jsonl"
    raw_output_path = tmp_path / "output_raw.jsonl"
    input_entries = [
        {"key": 1, "value": "one"},
        {"key": 2, "value": "two"},
    ]

    with open(input_path, "w") as f:
        for entry in input_entries:
            f.write(json.dumps(entry))
            f.write("\n")

    class TableSchema(pw.Schema):
        key: int
        value: str

    table = pw.io.jsonlines.read(input_path, schema=TableSchema)

    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.input_topic,
        format="json",
        schema_registry_settings=pw.io.kafka.SchemaRegistrySettings(
            urls=[SCHEMA_REGISTRY_BASE_ROUTE],
            timeout=datetime.timedelta(seconds=5),
        ),
        subject=schema_subject,
    )

    table_reread = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        schema=TableSchema,
        autocommit_duration_ms=100,
        schema_registry_settings=pw.io.kafka.SchemaRegistrySettings(
            urls=[SCHEMA_REGISTRY_BASE_ROUTE],
            timeout=datetime.timedelta(seconds=5),
        ),
    )
    table_raw = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="raw",
    )

    pw.io.jsonlines.write(table_reread, output_path)
    pw.io.jsonlines.write(table_raw, raw_output_path)
    wait_result_with_checker(
        FileLinesNumberChecker(output_path, 2).add_path(raw_output_path, 2), 30
    )
    output_entries = []
    with open(output_path, "r") as f:
        for line in f:
            data = json.loads(line)
            output_entries.append(
                {
                    "key": data["key"],
                    "value": data["value"],
                }
            )
    output_entries.sort(key=lambda x: x["key"])
    assert output_entries == input_entries

    # Send the data encoded by the registry as a key, while keeping the value as empty.
    # Check that value parsing works.
    additional_topic = kafka_context.create_additional_topic()
    with open(raw_output_path, "r") as f:
        for line in f:
            data = json.loads(line)
            encoded_message = base64.b64decode(data["data"])
            kafka_context.send(message=(encoded_message, None), topic=additional_topic)
    # Confirm delivery before the static read below, which snapshots the topic
    # end offsets at startup: an unflushed send could land after that snapshot
    # and be missed.
    kafka_context.flush()

    class KeyTableSchema(pw.Schema):
        key: int = pw.column_definition(source_component="key")
        value: str = pw.column_definition(source_component="key")

    G.clear()
    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=additional_topic,
        format="json",
        schema=KeyTableSchema,
        schema_registry_settings=pw.io.kafka.SchemaRegistrySettings(
            urls=[SCHEMA_REGISTRY_BASE_ROUTE],
            timeout=datetime.timedelta(seconds=5),
        ),
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    roundtrip_entries = []
    with open(output_path, "r") as f:
        for line in f:
            data = json.loads(line)
            roundtrip_entries.append(
                {
                    "key": data["key"],
                    "value": data["value"],
                }
            )
    roundtrip_entries.sort(key=lambda x: x["key"])
    assert roundtrip_entries == input_entries


@pytest.mark.flaky(reruns=3)
def test_kafka_simple_read_returns_key_and_data_columns(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """pw.io.kafka.simple_read produces a table with both a ``key`` and a
    ``data`` column (the same as ``pw.io.kafka.read``), and its docstring must
    describe exactly that — without referring to a ``value_column`` argument
    (which this function does not accept) or to a ``single column`` table."""
    kafka_context.fill([("k1", "v1"), ("k2", "v2")])

    table = pw.io.kafka.simple_read(
        kafka_context.default_rdkafka_settings()["bootstrap.servers"],
        kafka_context.input_topic,
        format="plaintext",
    )

    columns = set(table._columns.keys())
    assert columns == {"key", "data"}, (
        f"simple_read should produce a table with both 'key' and 'data' columns, "
        f"got {columns}"
    )

    sig = inspect.signature(pw.io.kafka.simple_read)
    assert "value_column" not in sig.parameters, (
        "simple_read docstring mentions 'value_column' but no such parameter exists; "
        "the docstring must not advertise non-existent parameters."
    )

    docstring = pw.io.kafka.simple_read.__doc__ or ""
    assert "value_column" not in docstring, (
        "simple_read docstring still references 'value_column', which is not a "
        "parameter of this function."
    )
    assert "single column" not in docstring.lower(), (
        "simple_read docstring still claims the table has a 'single column'; "
        "the actual table has both 'key' and 'data' columns."
    )

    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 10)

    with open(output_path, "r") as f:
        for row in f:
            parsed = json.loads(row)
            assert "key" in parsed and parsed["key"] is not None
            assert "data" in parsed and parsed["data"] is not None


def test_kafka_topic_list_uses_user_visible_warning_class(kafka_context):
    """When ``topic`` is passed as a list, pw.io.kafka.read must warn using a
    user-facing category (``DeprecationWarning`` or ``UserWarning``).
    ``SyntaxWarning`` is reserved by the interpreter for dubious source syntax
    and must not be used for a runtime API deprecation."""

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        table = pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=[kafka_context.input_topic, "second-ignored"],
            format="plaintext",
            mode="static",
        )
        # Ensure the table was actually constructed.
        assert "data" in table._columns

        topic_warnings = [
            w
            for w in captured
            if "topic" in str(w.message).lower() and "list" in str(w.message).lower()
        ]
        assert topic_warnings, "Expected a warning about 'topic' being a list"
        for w in topic_warnings:
            assert w.category is not SyntaxWarning, (
                "Warning class for 'topic'-as-list should not be SyntaxWarning "
                "(SyntaxWarning is reserved by the language for dubious syntax)."
            )
            assert issubclass(
                w.category, (DeprecationWarning, UserWarning)
            ), f"Unexpected warning class: {w.category.__name__}"


def test_kafka_topic_names_warning_message_is_accurate(kafka_context):
    """When the deprecated ``topic_names`` kwarg is used without ``topic``, its
    first element is actually used as the topic, so the deprecation warning
    must say so (pointing at ``topic`` as the replacement) rather than
    claiming the value ``will be ignored``."""

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        table = pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_names=[kafka_context.input_topic],
            format="plaintext",
            mode="static",
        )
        assert "data" in table._columns
        topic_names_warnings = [w for w in captured if "topic_names" in str(w.message)]
        assert topic_names_warnings, "Expected a warning about 'topic_names'"
        for w in topic_names_warnings:
            message = str(w.message).lower()
            # The warning must not claim the value is ignored: its first
            # element is actually used as the topic.
            assert "will be ignored" not in message, (
                f"The deprecation warning for 'topic_names' must not claim it "
                f"is ignored when its first element is actually used as the topic. "
                f"Got: {w.message!r}"
            )


@pytest.mark.flaky(reruns=3)
def test_kafka_metadata_empty_header_body_is_distinct_from_absent(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """The ``read`` metadata preserves the Kafka-protocol distinction between
    an absent header body and an empty one: an absent body becomes ``null``,
    while an empty body (``b""``) is encoded as an empty string. The ``read``
    docstring must describe this behavior accurately.
    """
    headers_with_empty = [("X-Empty", b""), ("X-Filled", b"value")]
    kafka_context.fill(["payload"], headers=headers_with_empty)

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
        with_metadata=True,
    )
    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 10)

    with open(output_path, "r") as f:
        row = json.loads(f.readline())
    headers = {key: value for key, value in row["_metadata"]["headers"]}
    assert "X-Empty" in headers
    # Empty body must NOT be null – it is an empty base64 string. The doc must
    # match this behavior.
    assert (
        headers["X-Empty"] is not None
    ), "Empty header body should produce empty string, not null (per actual impl)"
    assert (
        headers["X-Empty"] == ""
    ), f"Expected empty string, got {headers['X-Empty']!r}"

    # Verify the docstring describes this behavior. Earlier wording said
    # "non-empty"; the corrected text should refer to the body being absent
    # / not present, not "non-empty".
    import re

    docstring = pw.io.kafka.read.__doc__ or ""
    normalized = re.sub(r"\s+", " ", docstring)
    assert "header body is non-empty" not in normalized, (
        "read() docstring still claims null happens when the body is "
        "'non-empty', which contradicts the actual implementation: "
        "an empty body is encoded as an empty string, only an absent "
        "body becomes null."
    )


def test_kafka_write_value_without_format_error_uses_default_format_name(
    kafka_context: KafkaTestContext,
):
    """When ``pw.io.kafka.write`` rejects ``value=`` for a non raw/plaintext
    format and the user did not pass ``format=`` explicitly, the error must
    name the actual default format (``json``) rather than ``None``."""

    class InputSchema(pw.Schema):
        s: str

    table = pw.debug.table_from_markdown("s\nfoo\n", schema=InputSchema)
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            value=table.s,
        )
    msg = str(exc_info.value)
    assert "None" not in msg, (
        f"Error message must not say 'None format' — the user did not pass "
        f"format=None. Got: {msg!r}"
    )
    assert "json" in msg, (
        f"Error message should mention the actual default format ('json'). "
        f"Got: {msg!r}"
    )


@pytest.mark.flaky(reruns=3)
def test_kafka_write_sort_by_works(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """``pw.io.kafka.write`` honors its documented ``sort_by`` parameter: the
    column reference is taken against the table passed to ``write`` and the
    produced messages are emitted in that order. Internally ``write`` rebuilds
    the table via ``table.select(...)``, so the connector must remap the
    ``sort_by`` columns onto the rebuilt table."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        a: int

    table = pw.debug.table_from_markdown(
        """
        k | a
        1 | 30
        2 | 10
        3 | 20
        """,
        schema=InputSchema,
    )
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
        format="json",
        sort_by=[table.a],
    )
    pw.run()

    out = kafka_context.read_output_topic()
    assert len(out) == 3, f"Expected 3 messages, got {len(out)}"
    payloads = [json.loads(m.value) for m in out]
    a_values = [p["a"] for p in payloads]
    assert a_values == sorted(a_values), (
        f"With sort_by=[table.a], the messages must be ordered by 'a'. "
        f"Got: {a_values}"
    )


@pytest.mark.flaky(reruns=3)
def test_kafka_write_topic_name_collides_with_header_name(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """When a header is aliased to the same output name as the ``topic_name``
    column (e.g. ``topic_name=table.t`` and
    ``headers=[*table.select(t=table.other)]``), the two distinct columns
    would collide on a single extracted slot. ``pw.io.kafka.write`` must reject
    this with a clear error instead of silently dropping one column's value.
    """

    class InputSchema(pw.Schema):
        s: bytes = pw.column_definition(primary_key=True)
        t: str
        other: str

    table = pw.debug.table_from_pandas(
        __import__("pandas").DataFrame(
            {
                "s": [b"data"],
                "t": [kafka_context.output_topic],
                "other": ["hdr_value"],
            }
        ),
        schema=InputSchema,
    )
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=table.t,
            format="raw",
            value=table.s,
            headers=[*table.select(t=table.other)],
        )
    msg = str(exc_info.value).lower()
    assert "'t'" in msg or "name 't'" in msg or "shares" in msg or "topic" in msg, (
        f"Error must explain the name collision (the header 't' clashes "
        f"with the topic-name column 't'). Got: {exc_info.value!r}"
    )


def test_kafka_write_user_header_collides_with_reserved_pathway_headers(
    kafka_context: KafkaTestContext,
):
    """``pw.io.kafka.write`` always emits the reserved Kafka headers
    ``pathway_time`` and ``pathway_diff``, so a user header named
    ``pathway_time`` or ``pathway_diff`` would produce a duplicate header key.
    The connector must reject such a header up front rather than emitting an
    ambiguous message."""

    class InputSchema(pw.Schema):
        s: bytes = pw.column_definition(primary_key=True)
        pathway_time: bytes

    table = pw.debug.table_from_pandas(
        __import__("pandas").DataFrame(
            {"s": [b"data"], "pathway_time": [b"user_time"]}
        ),
        schema=InputSchema,
    )
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="raw",
            value=table.s,
            headers=[table.pathway_time],
        )
    msg = str(exc_info.value).lower()
    assert (
        "pathway_time" in msg or "pathway_diff" in msg or "reserved" in msg
    ), f"Error must mention the reserved header name. Got: {exc_info.value!r}"


def test_kafka_simple_read_internal_kwarg_clear_error():
    """``simple_read`` builds ``rdkafka_settings`` itself, so passing
    ``rdkafka_settings=`` to it is unsupported and must be rejected with an
    actionable error that names ``simple_read`` / ``rdkafka_settings`` and
    points the user at ``read`` — not an opaque ``got multiple values for
    keyword argument`` error from a function the user did not call."""

    with pytest.raises((TypeError, ValueError)) as exc_info:
        pw.io.kafka.simple_read(
            KAFKA_SETTINGS["bootstrap_servers"],
            "whatever",
            rdkafka_settings={"security.protocol": "PLAINTEXT"},
        )
    msg = str(exc_info.value).lower()
    assert "simple_read" in msg or "rdkafka_settings" in msg, (
        f"Error must reference simple_read or rdkafka_settings; "
        f"got {exc_info.value!r}"
    )
    assert "multiple values" not in msg, (
        f"Should not surface the raw 'multiple values' error; "
        f"got {exc_info.value!r}"
    )


def test_kafka_parallel_readers_zero_or_negative_is_rejected(kafka_context):
    """``parallel_readers`` is the number of reader copies, so it must be
    positive. Zero or negative values must be rejected up front with a clear,
    actionable error that names ``parallel_readers`` — not silently produce an
    idle pipeline or leak a low-level ``OverflowError``."""

    rdkafka_settings = kafka_context.default_rdkafka_settings()
    for bad in (0, -1, -10):
        with pytest.raises((ValueError, TypeError)) as exc_info:
            pw.io.kafka.read(
                rdkafka_settings=rdkafka_settings,
                topic=kafka_context.input_topic,
                format="plaintext",
                mode="static",
                parallel_readers=bad,
            )
        msg = str(exc_info.value).lower()
        assert (
            "parallel_readers" in msg or "positive" in msg
        ), f"Error must mention parallel_readers; got {exc_info.value!r}"
        # Make sure we're not surfacing the rust OverflowError verbatim
        assert (
            "overflow" not in msg
        ), f"Should not leak OverflowError text; got {exc_info.value!r}"


@pytest.mark.flaky(reruns=3)
def test_kafka_static_read_reads_all_partitions_when_parallel_readers_below_workers(
    tmp_path, kafka_context, monkeypatch
):
    """A static read whose ``parallel_readers`` limit is below the worker count
    must still read every partition. In static mode partitions are sharded by
    hand across only the workers that actually run a reader; sharding by the raw
    worker count instead would assign some partitions to workers that never run
    a reader, silently dropping their messages."""

    monkeypatch.setenv("PATHWAY_THREADS", "8")
    kafka_context.set_input_topic_partitions(8)
    kafka_context.fill([f"message_{i}" for i in range(400)])

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        mode="static",
        parallel_readers=2,
    )
    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    # Compare against what actually landed in the topic (not the generated
    # input), so a message the producer dropped doesn't masquerade as a read
    # loss. Any partition skipped by the reader would drop ~1/8 of the messages.
    expected = {
        message.value.decode("utf-8") for message in kafka_context.read_input_topic()
    }
    assert len(expected) > 350, "most produced messages should be in the topic"

    seen = set()
    with open(output_path) as f:
        for row in f:
            seen.add(json.loads(row)["data"])

    assert seen == expected


def test_kafka_autocommit_duration_ms_must_be_positive(kafka_context):
    """``autocommit_duration_ms`` is the maximum time between two commits, so
    it must be positive. Zero or negative values must be rejected up front
    with an error that names the parameter, rather than producing a pipeline
    that never commits."""

    rdkafka_settings = kafka_context.default_rdkafka_settings()
    for bad in (0, -1, -100):
        with pytest.raises(ValueError) as exc_info:
            pw.io.kafka.read(
                rdkafka_settings=rdkafka_settings,
                topic=kafka_context.input_topic,
                format="plaintext",
                mode="static",
                autocommit_duration_ms=bad,
            )
        msg = str(exc_info.value).lower()
        assert (
            "autocommit_duration_ms" in msg
        ), f"Error must mention the parameter; got {exc_info.value!r}"


def test_kafka_start_from_timestamp_ms_negative_rejected(kafka_context):
    """``start_from_timestamp_ms`` is a Unix timestamp in milliseconds, so it
    must be non-negative (negative values are pre-epoch and unsupported by
    Kafka). Such values must be rejected up front with an error that names the
    parameter, rather than surfacing as confusing seek errors at runtime."""

    rdkafka_settings = kafka_context.default_rdkafka_settings()
    for bad in (-1, -1000, -(10**12)):
        with pytest.raises(ValueError) as exc_info:
            pw.io.kafka.read(
                rdkafka_settings=rdkafka_settings,
                topic=kafka_context.input_topic,
                format="plaintext",
                mode="static",
                start_from_timestamp_ms=bad,
            )
        msg = str(exc_info.value).lower()
        assert (
            "start_from_timestamp_ms" in msg or "timestamp" in msg
        ), f"Error must mention the parameter; got {exc_info.value!r}"


def test_kafka_max_backlog_size_zero_or_negative_is_rejected(kafka_context):
    """``max_backlog_size`` limits the number of entries kept in processing, so
    it must be positive: ``0`` would let no entry be in flight (deadlocking the
    reader) and negative values are meaningless. Both must be rejected up front
    rather than producing a hung pipeline."""

    rdkafka_settings = kafka_context.default_rdkafka_settings()
    for bad in (0, -1, -100):
        with pytest.raises((ValueError, TypeError)) as exc_info:
            pw.io.kafka.read(
                rdkafka_settings=rdkafka_settings,
                topic=kafka_context.input_topic,
                format="plaintext",
                mode="static",
                max_backlog_size=bad,
            )
        msg = str(exc_info.value).lower()
        assert (
            "max_backlog_size" in msg or "positive" in msg or "negative" in msg
        ), f"Error must mention max_backlog_size; got {exc_info.value!r}"


@pytest.mark.flaky(reruns=3)
def test_kafka_write_json_schema_collides_with_pathway_metadata_fields(
    kafka_context: KafkaTestContext,
):
    """``pw.io.kafka.write`` with ``format='json'`` always adds the ``time``
    and ``diff`` fields to every message, so a user table column named
    ``time`` or ``diff`` would produce duplicate JSON keys and silent data
    loss. The connector must reject the conflict up front with an error that
    names the offending field and explains the reservation.
    """

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        time: str

    table = pw.debug.table_from_markdown(
        """
        k | time
        1 | user_time_value
        """,
        schema=InputSchema,
    )
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="json",
        )
    msg = str(exc_info.value).lower()
    assert (
        "time" in msg or "diff" in msg
    ), f"Error must mention the conflicting field name. Got: {exc_info.value!r}"
    assert "json" in msg or "format" in msg or "reserved" in msg, (
        f"Error must explain that JSON output reserves these names. "
        f"Got: {exc_info.value!r}"
    )


def test_kafka_schema_registry_settings_only_for_json_in_read(kafka_context):
    """``schema_registry_settings`` only has an effect for ``format='json'`` on
    read, so combining it with ``format='raw'`` or ``'plaintext'`` must raise
    an error that names the schema registry, rather than being silently
    ignored."""

    settings = pw.io.kafka.SchemaRegistrySettings(
        urls=[SCHEMA_REGISTRY_BASE_ROUTE],
        timeout=datetime.timedelta(seconds=5),
    )
    for bad_format in ("raw", "plaintext"):
        with pytest.raises(ValueError) as exc_info:
            pw.io.kafka.read(
                rdkafka_settings=kafka_context.default_rdkafka_settings(),
                topic=kafka_context.input_topic,
                format=bad_format,
                mode="static",
                schema_registry_settings=settings,
            )
        msg = str(exc_info.value).lower()
        assert (
            "schema_registry" in msg or "registry" in msg
        ), f"Error must mention schema_registry; got {exc_info.value!r}"


def test_kafka_schema_registry_settings_only_for_json_in_write(
    kafka_context: KafkaTestContext,
):
    """``schema_registry_settings`` only has an effect for ``format='json'`` on
    the writer (Confluent JSON Schema), so combining it with ``format='raw'``
    or ``'plaintext'`` must raise an error that names the schema registry,
    rather than being silently ignored."""

    class InputSchema(pw.Schema):
        s: bytes

    table = pw.debug.table_from_pandas(
        __import__("pandas").DataFrame({"s": [b"data"]}), schema=InputSchema
    )
    settings = pw.io.kafka.SchemaRegistrySettings(
        urls=[SCHEMA_REGISTRY_BASE_ROUTE],
        timeout=datetime.timedelta(seconds=5),
    )
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="raw",
            value=table.s,
            schema_registry_settings=settings,
            subject="subj",
        )
    msg = str(exc_info.value).lower()
    assert (
        "schema_registry" in msg or "registry" in msg
    ), f"Error must mention schema_registry_settings; got {exc_info.value!r}"
    assert (
        "json" in msg or "raw" in msg or "format" in msg
    ), f"Error must mention the format/json. Got {exc_info.value!r}"


@pytest.mark.flaky(reruns=3)
def test_kafka_topic_names_string_value_handled_or_rejected(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """When the deprecated ``topic_names`` kwarg is passed as a bare string,
    the connector must use the whole string as the topic name (not its first
    character) or raise a clear error — never silently slice it into a
    single-character topic.
    """
    kafka_context.fill(["foo", "bar"])

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        try:
            table = pw.io.kafka.read(
                rdkafka_settings=kafka_context.default_rdkafka_settings(),
                topic_names=kafka_context.input_topic,
                format="plaintext",
                mode="static",
                autocommit_duration_ms=100,
            )
        except (TypeError, ValueError) as e:
            msg = str(e).lower()
            assert "topic_names" in msg or "list" in msg, (
                f"If 'topic_names' as a string is rejected, the error must "
                f"explain it, got: {e!r}"
            )
            return

    # If accepted, the ENTIRE string must be used as the topic name. We can
    # only verify that by actually reading: if we silently took the first
    # character, the read would target a non-existent topic and produce 0
    # rows or fail.
    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)
    pw.run()
    with open(output_path) as f:
        rows = [json.loads(line) for line in f]
    payloads = sorted(row["data"] for row in rows)
    assert payloads == ["bar", "foo"], (
        f"'topic_names' as a string must be used as the full topic, but "
        f"only got payloads {payloads!r} (the connector probably split it "
        f"by character or read from the wrong topic)."
    )


def test_kafka_write_delimiter_only_for_dsv(kafka_context: KafkaTestContext):
    """``delimiter`` only applies to the ``dsv`` format, so passing a
    non-default delimiter with ``format='json'``, ``'plaintext'`` or ``'raw'``
    must be rejected with an error that names ``delimiter``, rather than being
    silently accepted with no effect."""

    class InputSchema(pw.Schema):
        s: str

    table = pw.debug.table_from_pandas(
        __import__("pandas").DataFrame({"s": ["x"]}), schema=InputSchema
    )
    for bad_format in ("json", "plaintext", "raw"):
        with pytest.raises(ValueError) as exc_info:
            kwargs = {}
            if bad_format in ("plaintext", "raw"):
                kwargs["value"] = table.s
            pw.io.kafka.write(
                table,
                rdkafka_settings=kafka_context.default_rdkafka_settings(),
                topic_name=kafka_context.output_topic,
                format=bad_format,
                delimiter="|",
                **kwargs,
            )
        msg = str(exc_info.value).lower()
        assert (
            "delimiter" in msg
        ), f"Error must mention 'delimiter'; got {exc_info.value!r}"


def test_kafka_write_schema_registry_settings_without_subject_rejected(kafka_context):
    """``pw.io.kafka.write`` with ``schema_registry_settings=...`` requires a
    ``subject``. The pair must be validated up front, at construction time,
    with an error that names both ``subject`` and the schema registry —
    mirroring the opposite direction (subject without registry) — rather than
    deferring the failure to ``pw.run()``."""

    class InputSchema(pw.Schema):
        s: str

    table = pw.debug.table_from_pandas(
        __import__("pandas").DataFrame({"s": ["x"]}), schema=InputSchema
    )
    settings = pw.io.kafka.SchemaRegistrySettings(
        urls=[SCHEMA_REGISTRY_BASE_ROUTE],
        timeout=datetime.timedelta(seconds=5),
    )
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="json",
            schema_registry_settings=settings,
            # subject is intentionally None
        )
    msg = str(exc_info.value).lower()
    assert "subject" in msg, f"Error must mention 'subject'; got {exc_info.value!r}"
    assert (
        "schema_registry" in msg or "registry" in msg
    ), f"Error must mention schema_registry; got {exc_info.value!r}"


def test_kafka_read_autogenerate_key_with_json_rejected(kafka_context):
    """``autogenerate_key`` only applies to the ``raw`` and ``plaintext``
    formats, so passing ``autogenerate_key=True`` with ``format='json'`` must
    be rejected up front with an error that names the parameter, rather than
    being silently ignored."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.input_topic,
            format="json",
            schema=InputSchema,
            mode="static",
            autogenerate_key=True,
        )
    msg = str(exc_info.value).lower()
    assert (
        "autogenerate_key" in msg
    ), f"Error must mention 'autogenerate_key'; got {exc_info.value!r}"


def test_kafka_read_json_field_paths_unknown_column_rejected(kafka_context):
    """``json_field_paths`` maps schema field names to JSON paths, so an entry
    naming a field that is not in the schema is a typo and must be rejected
    with an error that names the unknown field, rather than silently doing
    nothing."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.input_topic,
            format="json",
            schema=InputSchema,
            mode="static",
            json_field_paths={"inner": "/foo"},
        )
    msg = str(exc_info.value).lower()
    assert (
        "inner" in msg or "schema" in msg or "field" in msg
    ), f"Error must mention the unknown field. Got: {exc_info.value!r}"


def test_kafka_read_json_field_paths_for_metadata_rejected(kafka_context):
    """The connector populates the ``_metadata`` column itself (it does not
    come from the JSON message body), so a ``json_field_paths`` entry for
    ``_metadata`` can never take effect and must be rejected up front with an
    error that names ``_metadata``."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.input_topic,
            format="json",
            schema=InputSchema,
            mode="static",
            with_metadata=True,
            json_field_paths={"_metadata": "/foo"},
        )
    msg = str(exc_info.value).lower()
    assert "_metadata" in msg, f"Error must mention '_metadata'; got {exc_info.value!r}"


def test_kafka_read_json_field_paths_invalid_pointer_rejected(kafka_context):
    """``json_field_paths`` values are JSON Pointers (RFC 6901), which must be
    empty or start with ``/``. A path without the leading slash (e.g.
    ``{'inner': 'inner'}``) is invalid and must be rejected up front with an
    error that explains the JSON Pointer requirement, rather than silently
    resolving to a missing column.
    """

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        inner: str

    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.input_topic,
            format="json",
            schema=InputSchema,
            mode="static",
            json_field_paths={"inner": "inner"},
        )
    msg = str(exc_info.value).lower()
    assert (
        "json" in msg or "pointer" in msg or "rfc" in msg or "/" in msg
    ), f"Error must explain the JSON pointer requirement; got {exc_info.value!r}"


def test_kafka_write_rdkafka_settings_must_have_bootstrap_servers():
    """``pw.io.kafka.write`` requires a ``bootstrap.servers`` entry in
    ``rdkafka_settings`` so the producer can locate a broker; an empty dict
    must be rejected up front with an actionable error (the writer-side
    counterpart of the same requirement on ``read``)."""

    class InputSchema(pw.Schema):
        s: str

    table = pw.debug.table_from_pandas(
        __import__("pandas").DataFrame({"s": ["x"]}), schema=InputSchema
    )
    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings={},
            topic_name="x",
            format="plaintext",
        )
    msg = str(exc_info.value).lower()
    assert "bootstrap" in msg or "rdkafka_settings" in msg, (
        f"Error must mention 'bootstrap.servers' or rdkafka_settings; "
        f"got {exc_info.value!r}"
    )


def test_kafka_simple_read_read_only_new_with_timestamp_rejected():
    """``read_only_new=True`` and ``start_from_timestamp_ms`` both control the
    starting position and contradict each other, so combining them in
    ``simple_read`` must be rejected with an error that names both
    parameters, rather than silently letting one override the other."""

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.simple_read(
            KAFKA_SETTINGS["bootstrap_servers"],
            "whatever",
            read_only_new=True,
            start_from_timestamp_ms=12345,
        )
    msg = str(exc_info.value).lower()
    assert (
        "read_only_new" in msg
    ), f"Error must mention 'read_only_new'; got {exc_info.value!r}"
    assert (
        "start_from_timestamp_ms" in msg or "timestamp" in msg
    ), f"Error must mention 'start_from_timestamp_ms'; got {exc_info.value!r}"


def test_kafka_simple_read_static_with_read_only_new_rejected():
    """``simple_read(read_only_new=True, mode='static')`` is contradictory:
    ``read_only_new=True`` consumes only entries arriving after start-up,
    while ``mode='static'`` reads the already-present messages and stops. The
    combination must be rejected with an error that names ``read_only_new``
    and ``static``, rather than silently returning zero rows."""

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.simple_read(
            KAFKA_SETTINGS["bootstrap_servers"],
            "whatever",
            read_only_new=True,
            mode="static",
        )
    msg = str(exc_info.value).lower()
    assert (
        "read_only_new" in msg
    ), f"Error must mention 'read_only_new'; got {exc_info.value!r}"
    assert "static" in msg, f"Error must mention 'static' mode; got {exc_info.value!r}"


def test_kafka_write_topic_name_str_must_be_non_empty(kafka_context):
    """An empty ``topic_name`` is never valid in Kafka, so ``pw.io.kafka.write``
    must reject it up front with an error that names the topic, rather than
    deferring to a failure deep inside librdkafka."""

    class InputSchema(pw.Schema):
        s: str

    table = pw.debug.table_from_pandas(
        __import__("pandas").DataFrame({"s": ["x"]}), schema=InputSchema
    )
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name="",
            format="plaintext",
        )
    msg = str(exc_info.value).lower()
    assert (
        "topic" in msg
    ), f"Error must mention 'topic_name'/'topic'; got {exc_info.value!r}"


def test_kafka_read_rdkafka_settings_bootstrap_servers_must_be_non_empty():
    """An empty ``bootstrap.servers`` value cannot be resolved into a broker
    address even when the key is present, so ``pw.io.kafka.read`` must reject
    it up front with an error that names ``bootstrap.servers``."""

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings={"bootstrap.servers": ""},
            topic="x",
            mode="static",
            format="plaintext",
        )
    msg = str(exc_info.value).lower()
    assert (
        "bootstrap" in msg
    ), f"Error must mention 'bootstrap.servers'; got {exc_info.value!r}"


def test_kafka_read_rdkafka_settings_must_have_bootstrap_servers():
    """``pw.io.kafka.read`` requires a ``bootstrap.servers`` entry in
    ``rdkafka_settings`` so the consumer can locate a broker; an empty dict
    must be rejected up front with an actionable error, rather than surfacing
    a cryptic runtime error from librdkafka."""

    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings={},
            topic="whatever",
            mode="static",
            format="plaintext",
        )
    msg = str(exc_info.value).lower()
    assert "bootstrap" in msg or "rdkafka_settings" in msg, (
        f"Error must mention 'bootstrap.servers' or rdkafka_settings; "
        f"got {exc_info.value!r}"
    )


def test_kafka_read_topic_name_typo_not_swallowed():
    """``pw.io.kafka.read`` takes ``topic`` while ``pw.io.kafka.write`` takes
    ``topic_name``, so passing ``topic_name=`` to ``read`` is a common copy
    mistake. ``read`` must surface an error that explicitly mentions
    ``topic_name`` (pointing at ``topic`` as the right name), rather than the
    generic ``Missing topic name specification``."""

    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
                "group.id": "test",
            },
            format="plaintext",
            mode="static",
            topic_name="my-topic",
        )
    msg = str(exc_info.value).lower()
    assert "topic_name" in msg, (
        f"Error must mention the 'topic_name' typo so the user can correct "
        f"it. Got: {exc_info.value!r}"
    )


def test_kafka_schema_registry_header_must_be_strings():
    """``SchemaRegistryHeader.key`` and ``.value`` are typed as ``str``, so
    non-string values must be rejected at construction time with an error
    that names the field/type, rather than failing later inside the engine
    with an opaque message."""

    with pytest.raises((TypeError, ValueError)) as exc_info:
        pw.io.kafka.SchemaRegistryHeader(key=123, value="val")  # type: ignore[arg-type]
    msg = str(exc_info.value).lower()
    assert (
        "key" in msg or "string" in msg or "str" in msg
    ), f"Error must mention the field/type. Got: {exc_info.value!r}"

    with pytest.raises((TypeError, ValueError)) as exc_info:
        pw.io.kafka.SchemaRegistryHeader(key="X", value=42)  # type: ignore[arg-type]
    msg = str(exc_info.value).lower()
    assert (
        "value" in msg or "string" in msg or "str" in msg
    ), f"Error must mention the field/type. Got: {exc_info.value!r}"


def test_kafka_schema_registry_settings_string_fields_must_be_strings():
    """``token_authorization``, ``username``, ``password`` and ``proxy`` are
    all typed as ``str | None``, so non-string values must be rejected at
    construction time with an error that names the offending field, rather
    than surfacing later from the HTTP/auth layer."""

    for kwarg_name in ("token_authorization", "username", "password", "proxy"):
        with pytest.raises(TypeError) as exc_info:
            pw.io.kafka.SchemaRegistrySettings(
                urls=["http://x"],
                **{kwarg_name: 123},  # type: ignore[arg-type]
            )
        msg = str(exc_info.value).lower()
        assert kwarg_name in msg or "str" in msg, (
            f"Error must mention {kwarg_name!r} or 'str'; " f"got {exc_info.value!r}"
        )


def test_kafka_schema_registry_settings_urls_must_be_list():
    """``urls`` must be a ``list[str]``. Passing a single string by mistake
    (``urls='http://x'`` instead of ``urls=['http://x']``) must be rejected
    with an error that names ``urls`` / ``list``, rather than being iterated
    character-by-character into single-letter URLs."""

    with pytest.raises((TypeError, ValueError)) as exc_info:
        pw.io.kafka.SchemaRegistrySettings(urls="http://x")  # type: ignore[arg-type]
    msg = str(exc_info.value).lower()
    assert (
        "urls" in msg or "list" in msg
    ), f"Error must mention 'urls'/'list'; got {exc_info.value!r}"


def test_kafka_schema_registry_settings_url_must_be_non_empty():
    """``SchemaRegistrySettings.urls`` items are individual HTTP endpoints, so
    an empty string is never a valid URL and must be rejected at construction
    time with an error that names ``urls``, rather than surfacing as a runtime
    DNS/HTTP error far from the configuration site."""

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.SchemaRegistrySettings(urls=["", "http://x"])
    msg = str(exc_info.value).lower()
    assert "url" in msg, f"Error must mention 'urls'; got {exc_info.value!r}"


def test_kafka_schema_registry_settings_timeout_type():
    """``timeout`` is typed as ``datetime.timedelta | None``, so an ``int``
    (a common "5 seconds" mistake) must be rejected with a clear type error
    that names ``timeout`` and the expected ``timedelta`` type, rather than a
    confusing comparison error."""

    with pytest.raises(TypeError) as exc_info:
        pw.io.kafka.SchemaRegistrySettings(urls=["http://x"], timeout=5)  # type: ignore[arg-type]
    msg = str(exc_info.value).lower()
    assert "timeout" in msg, f"Error must mention 'timeout'; got {exc_info.value!r}"
    assert (
        "timedelta" in msg
    ), f"Error must mention the expected timedelta type; got {exc_info.value!r}"


def test_kafka_schema_registry_settings_timeout_must_be_positive():
    """``SchemaRegistrySettings.timeout`` is a network-request timeout, so it
    must be a positive duration; a zero or negative ``timedelta`` has no
    useful meaning and must be rejected at construction time with an error
    that names ``timeout``."""

    import datetime as _dt

    for bad in (
        _dt.timedelta(seconds=0),
        _dt.timedelta(seconds=-1),
        _dt.timedelta(microseconds=-1),
    ):
        with pytest.raises((ValueError, TypeError)) as exc_info:
            pw.io.kafka.SchemaRegistrySettings(urls=["http://x"], timeout=bad)
        msg = str(exc_info.value).lower()
        assert (
            "timeout" in msg
        ), f"Error must mention 'timeout'. Got: {exc_info.value!r}"


def test_kafka_schema_registry_settings_headers_must_be_typed():
    """``SchemaRegistrySettings.headers`` is typed as
    ``list[SchemaRegistryHeader] | None``, so plain tuples must be rejected at
    construction time with an error that names the expected type, rather than
    surfacing later as an opaque ``AttributeError`` from ``to_engine``."""

    with pytest.raises((TypeError, ValueError)) as exc_info:
        pw.io.kafka.SchemaRegistrySettings(
            urls=["http://x"],
            headers=[("key", "value")],  # type: ignore[list-item]
        )
    msg = str(exc_info.value)
    assert (
        "header" in msg.lower() or "SchemaRegistryHeader" in msg
    ), f"Error must mention the expected type. Got: {exc_info.value!r}"
    # Should not surface AttributeError text from a delayed access.
    assert "AttributeError" not in msg, (
        f"Should be a clear validation error, not the late AttributeError. "
        f"Got: {exc_info.value!r}"
    )


def test_kafka_schema_registry_settings_token_and_password_rejected():
    """``SchemaRegistrySettings`` exposes two distinct authentication
    mechanisms — ``token_authorization`` and ``username``/``password`` — which
    are mutually exclusive. Combining them must be rejected with an error that
    names the conflicting fields, rather than producing an ambiguous
    configuration."""

    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.SchemaRegistrySettings(
            urls=["http://x"],
            token_authorization="token",
            username="u",
            password="p",
        )
    msg = str(exc_info.value).lower()
    assert (
        "token" in msg
    ), f"Error must mention 'token_authorization'. Got: {exc_info.value!r}"
    assert (
        "username" in msg or "password" in msg
    ), f"Error must mention 'username'/'password'. Got: {exc_info.value!r}"


def test_kafka_schema_registry_settings_password_without_username_rejected():
    """``SchemaRegistrySettings`` requires ``username`` whenever ``password``
    is set, so ``password`` without ``username`` must be rejected with an
    error that names both fields."""

    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.SchemaRegistrySettings(urls=["http://x"], password="pwd")
    msg = str(exc_info.value).lower()
    assert (
        "username" in msg and "password" in msg
    ), f"Error must mention both 'username' and 'password'. Got: {exc_info.value!r}"


def test_kafka_schema_registry_settings_empty_urls_rejected():
    """``SchemaRegistrySettings`` needs at least one URL to reach the registry,
    so ``urls=[]`` must be rejected at construction time with an error that
    names ``urls``."""

    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.SchemaRegistrySettings(urls=[])
    msg = str(exc_info.value).lower()
    assert "url" in msg, f"Error must mention 'urls'. Got: {exc_info.value!r}"


def test_kafka_write_subject_without_schema_registry_settings_is_rejected(
    kafka_context: KafkaTestContext,
):
    """``subject`` only has an effect together with ``schema_registry_settings``
    (the Confluent Schema Registry), so passing it without the settings must
    be rejected with an error that names both ``subject`` and the schema
    registry, rather than being silently accepted."""

    class InputSchema(pw.Schema):
        s: str

    table = pw.debug.table_from_markdown("s\nfoo\n", schema=InputSchema)
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="json",
            subject="my-subject",
        )
    msg = str(exc_info.value).lower()
    assert "subject" in msg, f"Error must mention 'subject'; got {exc_info.value!r}"
    assert "schema_registry" in msg or "registry" in msg, (
        f"Error must explain that schema_registry_settings is missing; "
        f"got {exc_info.value!r}"
    )


def test_kafka_write_rejects_duplicate_header_names(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """When two distinct columns are aliased to the same header name,
    ``pw.io.kafka.write`` must reject the request with a clear error that names
    the duplicated header, rather than silently keeping only the first column
    and dropping the other's value."""

    table = get_test_binary_table(tmp_path)
    duplicated_headers = [
        *table.select(dup=table.data),
        *table.select(dup=table._metadata),
    ]
    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="raw",
            value=table.data,
            headers=duplicated_headers,
        )
    msg = str(exc_info.value).lower()
    assert (
        "header" in msg
    ), f"Error message must mention headers; got: {exc_info.value!r}"
    assert "dup" in msg or "duplicate" in msg, (
        f"Error message must reference the duplicate header name or the "
        f"duplication condition; got: {exc_info.value!r}"
    )


@pytest.mark.flaky(reruns=3)
def test_kafka_user_metadata_column_without_with_metadata_rejected(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """``_metadata`` is a reserved connector column name: the engine forces any
    column named ``_metadata`` to be sourced from connector metadata. A user
    schema that declares its own ``_metadata`` field without
    ``with_metadata=True`` must therefore be rejected with a clear error, so
    that the user's data cannot be silently shadowed by Pathway's metadata.
    """

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        _metadata: str

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.input_topic,
            format="json",
            schema=InputSchema,
            mode="static",
        )
    msg = str(exc_info.value).lower()
    assert "_metadata" in msg, (
        f"Error must mention '_metadata' as the reserved column name. "
        f"Got: {exc_info.value!r}"
    )
    assert "reserved" in msg or "with_metadata" in msg, (
        f"Error must explain that '_metadata' is reserved. " f"Got: {exc_info.value!r}"
    )


def test_kafka_with_metadata_conflicts_with_user_metadata_column(kafka_context):
    """Passing ``with_metadata=True`` together with a schema that already
    declares a ``_metadata`` column must fail with a clear ``ValueError`` that
    mentions ``_metadata`` / ``with_metadata``, rather than a bare
    ``AssertionError`` with no message."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        _metadata: int

    with pytest.raises((ValueError, TypeError)) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.input_topic,
            format="json",
            schema=InputSchema,
            with_metadata=True,
            mode="static",
        )
    msg = str(exc_info.value)
    assert msg, (
        "Expected a clear error message, but got an empty assertion. "
        "The user should be told that the schema already has '_metadata' "
        "while with_metadata=True wants to add it."
    )
    # Should mention either '_metadata' or 'with_metadata' so the user can act.
    assert "metadata" in msg.lower(), (
        f"Error message should mention '_metadata'/'with_metadata' "
        f"to help the user. Got: {msg!r}"
    )


def test_kafka_raw_format_with_schema_error_mentions_actual_format(kafka_context):
    """When ``format='raw'`` is passed together with an unsupported argument
    like ``schema``, the resulting error must name the format the user
    actually passed (``raw``), not a hardcoded ``plaintext``."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)

    with pytest.raises(ValueError) as exc_info:
        pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.input_topic,
            format="raw",
            schema=InputSchema,
            mode="static",
        )
    msg = str(exc_info.value)
    assert (
        "raw" in msg
    ), f"Error message for format='raw' should mention 'raw', got: {msg!r}"
    assert "plaintext" not in msg, (
        f"Error message for format='raw' must not hardcode 'plaintext', "
        f"got: {msg!r}"
    )


# ---------------------------------------------------------------------------
# pw.io.kafka.read must reject an empty topic name at construction time.
#
# An empty topic is never valid in Kafka, so the reader should fail fast with
# an actionable ValueError — symmetrically with pw.io.kafka.write, which
# already rejects ``topic_name=""`` — rather than deferring to librdkafka,
# which only surfaces an opaque ``Failed to fetch topic metadata`` at runtime.
# ---------------------------------------------------------------------------


def test_kafka_read_topic_empty_string_rejected():
    with pytest.raises(ValueError, match="'topic'.*non-empty"):
        pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
                "group.id": "bug-bounty",
            },
            topic="",
            format="raw",
        )


def test_kafka_read_topic_list_with_empty_string_rejected():
    with pytest.raises(ValueError, match="'topic'.*non-empty"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pw.io.kafka.read(
                rdkafka_settings={
                    "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
                    "group.id": "bug-bounty",
                },
                topic=[""],
                format="raw",
            )


def test_kafka_simple_read_topic_empty_string_rejected():
    with pytest.raises(ValueError, match="'topic'.*non-empty"):
        pw.io.kafka.simple_read(KAFKA_SETTINGS["bootstrap_servers"], "")


def test_kafka_read_topic_empty_list_rejected():
    with pytest.raises(ValueError, match="'topic'.*non-empty"):
        pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
                "group.id": "bug-bounty",
            },
            topic=[],
            format="raw",
        )


# ---------------------------------------------------------------------------
# pw.io.kafka.read(format="raw", with_metadata=True) returns raw bytes for
# both ``key`` and ``data`` while also populating the ``_metadata`` column.
#
# The ``raw`` format must preserve the payload and key as bytes (no UTF-8
# decoding), and it must coexist with the auto-generated ``_metadata`` column,
# which carries ``topic``, ``partition``, ``offset``, ``timestamp_millis`` and
# ``headers``.
# ---------------------------------------------------------------------------


@pytest.mark.flaky(reruns=3)
def test_kafka_raw_with_metadata_populates_all_fields(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    payload = bytes(
        [0, 255, 128, 10, 17]
    )  # deliberately non-UTF-8 to expose any plaintext shortcut
    kafka_context.send(("my-key", payload))
    kafka_context.flush()

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="raw",
        mode="static",
        with_metadata=True,
        autocommit_duration_ms=100,
    )
    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    rows = [json.loads(line) for line in output_path.read_text().splitlines() if line]
    assert len(rows) == 1
    row = rows[0]
    assert set(row.keys()) >= {"key", "data", "_metadata"}

    # ``data`` is bytes — JSON encodes bytes as a base64 string.
    assert isinstance(row["data"], str)
    assert base64.b64decode(row["data"]) == payload

    assert isinstance(row["key"], str)
    assert base64.b64decode(row["key"]) == b"my-key"

    meta = row["_metadata"]
    assert meta["topic"] == kafka_context.input_topic
    assert isinstance(meta["partition"], int)
    assert isinstance(meta["offset"], int)
    assert "timestamp_millis" in meta
    assert "headers" in meta


# ---------------------------------------------------------------------------
# When ``start_from_timestamp_ms`` is set, the reader needs
# ``auto.offset.reset="earliest"`` so a lazy-seek miss falls back to the start
# rather than skipping data, and it overrides any conflicting user value to
# achieve that. Because the override discards an explicit user setting, the
# reader must warn the user that their ``auto.offset.reset`` value is being
# ignored — and must stay silent when the user's value already agrees.
# ---------------------------------------------------------------------------


def test_kafka_read_auto_offset_reset_override_warns():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
                "group.id": "test-group",
                "auto.offset.reset": "latest",
            },
            topic="anything",
            start_from_timestamp_ms=0,
            format="raw",
        )
    matching = [w for w in caught if "auto.offset.reset" in str(w.message)]
    assert matching, (
        "When 'start_from_timestamp_ms' is set, the user's "
        "'auto.offset.reset' value is silently overridden to 'earliest'. "
        "Pathway must warn that the setting is being ignored. "
        f"Got warnings: {[str(w.message) for w in caught]}"
    )


def test_kafka_read_auto_offset_reset_no_warning_when_matching():
    # If the user explicitly set 'earliest' (the value Pathway needs for the
    # lazy-seek to work), no override happens and no warning should fire.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
                "group.id": "test-group",
                "auto.offset.reset": "earliest",
            },
            topic="anything",
            start_from_timestamp_ms=0,
            format="raw",
        )
    matching = [w for w in caught if "auto.offset.reset" in str(w.message)]
    assert not matching, (
        "No warning expected when user's 'auto.offset.reset' already matches "
        f"Pathway's required value, got: {[str(w.message) for w in matching]}"
    )


# ---------------------------------------------------------------------------
# pw.io.kafka.read requires a non-empty 'group.id' in rdkafka_settings.
#
# The reader uses ``subscribe`` (not ``assign``), which librdkafka refuses to
# perform without a configured consumer group. The requirement must be
# validated up front — like ``bootstrap.servers`` — with an actionable
# ValueError, instead of failing deep inside ``pw.run()`` with an opaque
# ``Subscription to Kafka topic failed: Local: Unknown group`` error. The
# write path uses a producer, which has no consumer group, so it must not
# require ``group.id``.
# ---------------------------------------------------------------------------


def test_kafka_read_rdkafka_settings_must_have_group_id():
    with pytest.raises(ValueError, match="group.id"):
        pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
            },
            topic="any",
            format="raw",
        )


def test_kafka_read_rdkafka_settings_group_id_empty_string_rejected():
    with pytest.raises(ValueError, match="group.id"):
        pw.io.kafka.read(
            rdkafka_settings={
                "bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"],
                "group.id": "",
            },
            topic="any",
            format="raw",
        )


def test_kafka_write_does_not_need_group_id():
    # Producers don't belong to a consumer group, so write() must not require
    # 'group.id'. This guards against an over-broad fix that would also touch
    # the write path.
    class T2Schema(pw.Schema):
        v: bytes

    class Source(pw.io.python.ConnectorSubject):
        def run(self) -> None:
            pass

    t = pw.io.python.read(Source(), schema=T2Schema)
    pw.io.kafka.write(
        t,
        rdkafka_settings={"bootstrap.servers": KAFKA_SETTINGS["bootstrap_servers"]},
        topic_name="some-topic",
        format="raw",
    )


# ---------------------------------------------------------------------------
# pw.io.kafka.read(mode="static", start_from_timestamp_ms=<future>) must
# finish promptly when no message matches the requested timestamp.
#
# When the requested timestamp is past every message in the topic, each
# partition's seek target resolves to its high watermark (the
# next-to-be-produced offset), so there is nothing left to consume. Static
# mode must recognize the partition as fully consumed and return right away,
# rather than polling fruitlessly until the static-mode polling budget (~30s)
# is exhausted.
# ---------------------------------------------------------------------------


@pytest.mark.flaky(reruns=3)
def test_kafka_static_mode_with_future_timestamp_finishes_promptly(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    kafka_context.fill(["foo", "bar"])
    future_ts = (int(time.time()) + 3600) * 1000  # 1 hour in the future

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        mode="static",
        autocommit_duration_ms=100,
        start_from_timestamp_ms=future_ts,
    )
    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)

    start = time.monotonic()
    pw.run()
    elapsed = time.monotonic() - start

    # No messages match a future timestamp.
    assert output_path.read_text().strip() == ""
    # Must not stall for the full polling window.
    assert elapsed < 15, (
        f"Static mode with a future 'start_from_timestamp_ms' must return "
        f"promptly when no message matches, but pw.run() took {elapsed:.2f}s."
    )


# ---------------------------------------------------------------------------
# pw.io.kafka.write must reject an empty ``subject`` string at construction
# time.
#
# An empty string is never a valid Confluent Schema Registry subject, so it
# must be rejected up front with an actionable ValueError — just like an empty
# topic name — instead of being forwarded to the registry encoder and
# surfacing as an opaque "subject not found" 404 at run time.
# ---------------------------------------------------------------------------


def test_kafka_write_subject_empty_string_rejected(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    class T(pw.Schema):
        v: int

    class Source(pw.io.python.ConnectorSubject):
        def run(self) -> None:
            pass

    t = pw.io.python.read(Source(), schema=T)
    with pytest.raises(ValueError, match="subject"):
        pw.io.kafka.write(
            t,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="json",
            schema_registry_settings=pw.io.kafka.SchemaRegistrySettings(
                urls=[SCHEMA_REGISTRY_BASE_ROUTE]
            ),
            subject="",
        )

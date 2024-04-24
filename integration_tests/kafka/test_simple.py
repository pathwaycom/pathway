# Copyright © 2024 Pathway

import json
import pathlib

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    expect_csv_checker,
    wait_result_with_checker,
)

from .utils import KafkaTestContext


@pytest.mark.flaky(reruns=3)
def test_kafka_raw(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

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
def test_kafka_json(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
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
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

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
def test_kafka_csv(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    kafka_context.fill(
        [
            "k,v",
            "0,foo",
            "1,bar",
            "2,baz",
        ]
    )

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="csv",
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

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
        "kafka:9092",
        kafka_context.input_topic,
    )
    pw.io.jsonlines.write(table, str(tmp_path / "output.jsonl"))
    wait_result_with_checker(FileLinesNumberChecker(tmp_path / "output.jsonl", 2), 10)

    # check that reread will have all these messages again
    G.clear()
    table = pw.io.kafka.simple_read(
        "kafka:9092",
        kafka_context.input_topic,
    )
    pw.io.jsonlines.write(table, str(tmp_path / "output.jsonl"))
    wait_result_with_checker(FileLinesNumberChecker(tmp_path / "output.jsonl", 2), 10)

    # Check output type, bytes should be rendered as an array
    with open(tmp_path / "output.jsonl", "r") as f:
        for row in f:
            row_parsed = json.loads(row)
            assert isinstance(row_parsed["data"], list)
            assert row_parsed["data"] == list(b"foo") or row_parsed["data"] == list(
                b"bar"
            )


@pytest.mark.flaky(reruns=3)
def test_kafka_simple_wrapper_plaintext_io(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.simple_read(
        "kafka:9092",
        kafka_context.input_topic,
        format="plaintext",
    )
    pw.io.jsonlines.write(table, str(tmp_path / "output.jsonl"))
    wait_result_with_checker(FileLinesNumberChecker(tmp_path / "output.jsonl", 2), 10)

    # check that reread will have all these messages again
    G.clear()
    table = pw.io.kafka.simple_read(
        "kafka:9092",
        kafka_context.input_topic,
        format="plaintext",
    )
    pw.io.jsonlines.write(table, str(tmp_path / "output.jsonl"))
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
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
        persistent_id="1",
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

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
            "persistence_config": pw.persistence.Config.simple_config(
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
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
        persistent_id="1",
    )

    pw.io.csv.write(table, str(tmp_path / "output_backfilled.csv"))
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
            "persistence_config": pw.persistence.Config.simple_config(
                pw.persistence.Backend.filesystem(persistent_storage_path),
            ),
        },
    )

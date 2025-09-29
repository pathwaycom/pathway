import json
import os
import threading
import time
import uuid

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    wait_result_with_checker,
    write_lines,
)

from .utils import KinesisRecord, KinesisTestContext


@pytest.mark.parametrize(
    "scenario",
    [
        ["no-op", ("split", 0), ("split", 1), ("merge", 0), ("split", 1)],
        ["no-op", ("split", 0), ("merge", 0), ("split", 0), ("merge", 0)],
        ["no-op", ("split", 0), ("split", 1), ("merge", 0), ("merge", 0)],
        [
            "no-op",
            ("split", 0),
            ("split", 1),
            ("split", 2),
            ("merge", 2),
            ("merge", 0),
            ("merge", 0),
        ],
    ],
)
def test_persistence(scenario, kinesis_context, tmp_path):
    output_path = tmp_path / "output.jsonl"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(tmp_path / "PStorage")
    )

    # We don't control how Kinesis splits records into shards, so we just send enough
    # to be sure that at least some have landed into every active shard
    n_records_per_run = 30
    next_counter_value = 0

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    def run():
        G.clear()
        expected_keys = set()
        for _ in range(n_records_per_run):
            nonlocal next_counter_value
            message = json.dumps(
                {
                    "key": next_counter_value,
                    "value": "a" * next_counter_value,
                }
            )
            kinesis_context.send_record(
                KinesisRecord(
                    key=str(next_counter_value), value=message.encode("utf-8")
                )
            )
            expected_keys.add(next_counter_value)
            next_counter_value += 1

        table = pw.io.kinesis.read(
            kinesis_context.stream_name, format="json", schema=InputSchema
        )
        pw.io.jsonlines.write(table, output_path)
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, n_records_per_run),
            30,
            kwargs={"persistence_config": persistence_config},
        )

        read_keys = set()
        with open(output_path, "r") as f:
            for row in f:
                row_parsed = json.loads(row)
                read_keys.add(row_parsed["key"])
        assert read_keys == expected_keys

    run()
    for element in scenario:
        if element == "no-op":
            run()
            continue
        (operation, index) = element
        shards = kinesis_context.list_shards_and_statuses()
        shards.sort(key=lambda shard: shard.hash_range_start)
        active_shards = []
        for shard in shards:
            if shard.is_open:
                active_shards.append(shard)
        if operation == "split":
            kinesis_context.split_shard(active_shards[index])
        elif operation == "merge":
            kinesis_context.merge_shards(active_shards[index], active_shards[index + 1])
        else:
            raise ValueError(f"unknown item: {element}")
        run()


@pytest.mark.parametrize("format", ["plaintext", "raw", "json"])
@pytest.mark.parametrize("specify_key", [False, True])
def test_kinesis_output_simple(format, specify_key, kinesis_context):
    table = pw.debug.table_from_markdown(
        """
        | key | value
     1  |   1 | one
     2  |   2 | two
     3  |   3 | three
    """
    )

    extra_kwargs = {}
    if specify_key:
        extra_kwargs["partition_key"] = table.key
    if format != "json":
        extra_kwargs["data"] = table.value

    pw.io.kinesis.write(
        table,
        stream_name=kinesis_context.stream_name,
        format=format,
        **extra_kwargs,
    )
    pw.run()
    shards = kinesis_context.list_shards_and_statuses()
    assert len(shards) == 1

    expected_entry = {
        1: "one",
        2: "two",
        3: "three",
    }
    user_defined_values = expected_entry.values()

    records = kinesis_context.read_shard_records(shards[0].shard_id)
    for record in records:
        key_is_user_defined = record.key in {"1", "2", "3"}
        assert key_is_user_defined == specify_key
        if format == "json":
            data = json.loads(record.value.decode("utf-8"))
            assert expected_entry[data["key"]] == data["value"]
        elif format == "plaintext":
            assert record.value.decode("utf-8") in user_defined_values
        elif format == "raw":
            assert record.value.decode("utf-8") in user_defined_values


def test_kinesis_output_custom_topic(kinesis_context):
    second_context = KinesisTestContext()
    table_markdown = """
        | key | value | stream
     1  |   1 | one   | #0
     2  |   2 | two   | #1
     3  |   3 | three | #0
    """.replace(
        "#0", kinesis_context.stream_name
    ).replace(
        "#1", second_context.stream_name
    )
    table = pw.debug.table_from_markdown(table_markdown)
    pw.io.kinesis.write(
        table,
        stream_name=table.stream,
        partition_key=table.key,
        data=table.value,
        format="raw",
    )
    pw.run()

    shards = kinesis_context.list_shards_and_statuses()
    assert len(shards) == 1
    records = kinesis_context.read_shard_records(shards[0].shard_id)
    records.sort(key=lambda record: record.key)
    assert records == [
        KinesisRecord("1", "one".encode("utf-8")),
        KinesisRecord("3", "three".encode("utf-8")),
    ]

    shards = second_context.list_shards_and_statuses()
    assert len(shards) == 1
    records = second_context.read_shard_records(shards[0].shard_id)
    assert records == [
        KinesisRecord("2", "two".encode("utf-8")),
    ]


def test_kinesis_io_much_data(kinesis_context, tmp_path):
    # The whole round-trip takes ~120 seconds. The main problem is the speed of the
    # emulator, as the get-records request may take up to 1.5s
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.txt"
    kinesis_context.recreate(shard_count=32)

    n_total_entries = 1_000_000
    with open(input_path, "w") as f:
        for _ in range(n_total_entries):
            f.write(str(uuid.uuid4()))
            f.write("\n")
    table = pw.io.plaintext.read(input_path, mode="static")
    pw.io.kinesis.write(
        table, kinesis_context.stream_name, format="raw", data=table.data
    )
    pw.run()

    G.clear()
    table = pw.io.kinesis.read(kinesis_context.stream_name)
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, n_total_entries), 60)


@pytest.mark.parametrize("shard_count", [1, 4])
def test_kinesis_input_streaming(shard_count, kinesis_context, tmp_path):
    if shard_count != 1:
        kinesis_context.recreate(shard_count)
    output_path = tmp_path / "output.jsonl"
    n_total_entries = 20

    def stream_inputs():
        for key in range(n_total_entries):
            kinesis_context.send_record(
                KinesisRecord(
                    key=str(key),
                    value=uuid.uuid4().bytes,
                )
            )
            time.sleep(0.4)

    table = pw.io.kinesis.read(stream_name=kinesis_context.stream_name)
    pw.io.jsonlines.write(table, output_path)

    streaming_thread = threading.Thread(target=stream_inputs, daemon=True)
    streaming_thread.start()
    wait_result_with_checker(FileLinesNumberChecker(output_path, n_total_entries), 30)


@pytest.mark.parametrize("shard_count", [1, 4])
def test_kinesis_output_streaming(shard_count, kinesis_context, tmp_path):
    if shard_count != 1:
        kinesis_context.recreate(shard_count)
    inputs_path = tmp_path / "input"
    os.mkdir(inputs_path)
    n_total_entries = 20

    def stream_inputs():
        for key in range(n_total_entries):
            input_path = inputs_path / str(key)
            write_lines(input_path, str(key))
            time.sleep(0.4)

    table = pw.io.plaintext.read(inputs_path)
    pw.io.kinesis.write(table, stream_name=kinesis_context.stream_name, format="raw")

    streaming_thread = threading.Thread(target=stream_inputs, daemon=True)
    streaming_thread.start()

    class KinesisMessageCountChecker:

        def __init__(self, kinesis_context: KinesisTestContext, expected_entries: int):
            self.kinesis_context = kinesis_context
            self.expected_entries = expected_entries
            self.shards = kinesis_context.list_shards_and_statuses()

        def __call__(self):
            total_messages = 0
            for shard in self.shards:
                total_messages += len(
                    self.kinesis_context.read_shard_records(shard.shard_id)
                )
            return total_messages == self.expected_entries

    wait_result_with_checker(
        KinesisMessageCountChecker(kinesis_context, n_total_entries), 30
    )

# Copyright © 2026 Pathway

import json
import pathlib
import threading
import time
from uuid import uuid4

import pandas as pd
import pytest
import requests

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    FileLinesNumberChecker,
    wait_result_with_checker,
)

from .utils import (
    PULSAR_ADMIN_URL,
    PULSAR_AUTH_SERVICE_URI,
    PULSAR_AUTH_TOKEN,
    PULSAR_SERVICE_URI,
)

WAIT_TIMEOUT_SECS = 30


# --- Parametrized read/write test ---


@pytest.mark.parametrize("format", ["plaintext", "raw", "json"])
@pytest.mark.parametrize("mode", ["streaming", "static"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_read_write(pulsar_context, tmp_path, format, mode):
    output_file = tmp_path / "output.jsonl"
    n_messages = 3

    class JsonSchema(pw.Schema):
        name: str
        age: int

    schema = JsonSchema if format == "json" else None

    if format == "json":
        payloads = [json.dumps({"name": f"user-{i}", "age": 20 + i}) for i in range(3)]
    else:
        payloads = [f"message-{i}" for i in range(3)]

    for payload in payloads:
        pulsar_context.send(payload)

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format=format,
        schema=schema,
        mode=mode,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)

    if mode == "static":
        pw.run()
    else:
        wait_result_with_checker(
            FileLinesNumberChecker(output_file, n_messages), WAIT_TIMEOUT_SECS
        )

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert len(lines) == n_messages
    if format == "json":
        assert {(line["name"], line["age"]) for line in lines} == {
            (f"user-{i}", 20 + i) for i in range(3)
        }
    elif format == "plaintext":
        expected = {f"message-{i}" for i in range(3)}
        assert {line["data"] for line in lines} == expected


@pytest.mark.flaky(reruns=3)
def test_pulsar_write_then_read_roundtrip(pulsar_context, tmp_path):
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"
    entries = ["one", "two", "three", "four"]
    with open(input_file, "w") as f:
        f.write("\n".join(entries) + "\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.pulsar.write(table, PULSAR_SERVICE_URI, pulsar_context.topic, format="json")

    class InputSchema(pw.Schema):
        data: str

    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        schema=InputSchema,
        format="json",
        autocommit_duration_ms=100,
    )
    pw.io.csv.write(table_reread, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, len(entries)), WAIT_TIMEOUT_SECS
    )


def test_pulsar_static_empty_topic(pulsar_context, tmp_path):
    output_file = tmp_path / "output.jsonl"

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
    )
    pw.io.jsonlines.write(table, output_file)
    pw.run()

    assert output_file.read_text() == ""


@pytest.mark.flaky(reruns=3)
def test_pulsar_headers_and_key(pulsar_context, tmp_path):
    input_file = tmp_path / "input.jsonl"
    rows = [
        {"key": "front-door", "temperature": 21, "note": "ok"},
        {"key": "back-door", "temperature": 22, "note": "warm"},
    ]
    with open(input_file, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    class InputSchema(pw.Schema):
        key: str
        temperature: int
        note: str

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="json",
        key=table.key,
        headers=[table.temperature, table.note],
    )
    pw.run()

    messages = pulsar_context.read_messages(expected_count=len(rows))
    received = {}
    for message in messages:
        payload = json.loads(message.data())
        received[payload["key"]] = message

    assert set(received) == {"front-door", "back-door"}
    for row in rows:
        message = received[row["key"]]
        assert message.partition_key() == row["key"]
        properties = message.properties()
        assert properties["pathway_diff"] == "1"
        assert "pathway_time" in properties
        assert json.loads(properties["temperature"]) == row["temperature"]
        assert json.loads(properties["note"]) == row["note"]


@pytest.mark.flaky(reruns=3)
def test_pulsar_dynamic_topics(pulsar_context, tmp_path):
    input_path = tmp_path / "input.jsonl"
    output_path_1 = tmp_path / "output_1.jsonl"
    output_path_2 = tmp_path / "output_2.jsonl"
    dynamic_topic_1 = f"pulsar-{uuid4()}"
    dynamic_topic_2 = f"pulsar-{uuid4()}"
    with open(input_path, "w") as f:
        f.write(json.dumps({"k": "0", "v": "foo", "t": dynamic_topic_1}) + "\n")
        f.write(json.dumps({"k": "1", "v": "bar", "t": dynamic_topic_2}) + "\n")
        f.write(json.dumps({"k": "2", "v": "baz", "t": dynamic_topic_1}) + "\n")

    class InputSchema(pw.Schema):
        k: str
        v: str
        t: str

    G.clear()
    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
    pw.io.pulsar.write(table, PULSAR_SERVICE_URI, topic=table.t, format="json")
    pw.run()

    class OutputSchema(pw.Schema):
        k: str
        v: str

    G.clear()
    table_1 = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        dynamic_topic_1,
        schema=OutputSchema,
        format="json",
        mode="static",
    )
    table_2 = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        dynamic_topic_2,
        schema=OutputSchema,
        format="json",
        mode="static",
    )
    pw.io.jsonlines.write(table_1, output_path_1)
    pw.io.jsonlines.write(table_2, output_path_2)
    pw.run()

    lines_1 = [json.loads(line) for line in output_path_1.read_text().splitlines()]
    lines_2 = [json.loads(line) for line in output_path_2.read_text().splitlines()]
    assert {(line["k"], line["v"]) for line in lines_1} == {("0", "foo"), ("2", "baz")}
    assert {(line["k"], line["v"]) for line in lines_2} == {("1", "bar")}
    # The topic-name column must not be a part of the payload.
    assert all("t" not in line for line in lines_1 + lines_2)


@pytest.mark.flaky(reruns=3)
def test_pulsar_start_from_end(pulsar_context, tmp_path):
    output_file = tmp_path / "output.jsonl"
    for i in range(3):
        pulsar_context.send(f"old-{i}")

    def send_new_messages():
        # Give the pipeline the time to create the subscription positioned at
        # the tail of the topic; anything published afterwards must be seen.
        time.sleep(5.0)
        for i in range(2):
            pulsar_context.send(f"new-{i}")

    sender = threading.Thread(target=send_new_messages)
    sender.start()
    try:
        G.clear()
        table = pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            pulsar_context.topic,
            format="plaintext",
            start_from="end",
            autocommit_duration_ms=100,
        )
        pw.io.jsonlines.write(table, output_file)
        wait_result_with_checker(
            FileLinesNumberChecker(output_file, 2), WAIT_TIMEOUT_SECS
        )
    finally:
        sender.join()

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == {"new-0", "new-1"}


@pytest.mark.flaky(reruns=3)
def test_pulsar_start_from_timestamp(pulsar_context, tmp_path):
    output_file = tmp_path / "output.jsonl"
    for i in range(3):
        pulsar_context.send(f"old-{i}")
    time.sleep(2.0)
    cutoff_timestamp_ms = int(time.time() * 1000)
    time.sleep(2.0)
    for i in range(3):
        pulsar_context.send(f"new-{i}")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
        start_from="timestamp",
        start_from_timestamp_ms=cutoff_timestamp_ms,
    )
    pw.io.jsonlines.write(table, output_file)
    pw.run()

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == {"new-0", "new-1", "new-2"}


@pytest.mark.flaky(reruns=3)
def test_pulsar_types_roundtrip(pulsar_context, tmp_path):
    """All types serializable to JSON survive the write-read roundtrip intact."""
    output_file = tmp_path / "output.jsonl"

    class TypesSchema(pw.Schema):
        int_field: int
        float_field: float
        bool_field: bool
        str_field: str
        list_field: list[int]
        json_field: pw.Json

    rows = [
        {
            "int_field": 42,
            "float_field": -2.5,
            "bool_field": True,
            "str_field": "héllo wörld",
            "list_field": [1, 2, 3],
            "json_field": {"nested": {"a": 1}, "arr": [True, None, "x"]},
        }
    ]
    input_file = tmp_path / "input.jsonl"
    with open(input_file, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=TypesSchema, mode="static")
    pw.io.pulsar.write(table, PULSAR_SERVICE_URI, pulsar_context.topic, format="json")
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        schema=TypesSchema,
        format="json",
        mode="static",
    )
    pw.io.jsonlines.write(table_reread, output_file)
    pw.run()

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert len(lines) == len(rows)
    result = lines[0]
    for field, expected_value in rows[0].items():
        assert result[field] == expected_value, field


@pytest.mark.flaky(reruns=3)
def test_pulsar_persistence_no_rereading(pulsar_context, tmp_path: pathlib.Path):
    """Restarts of a persisted pipeline continue after the processed prefix.

    Three consecutive runs against the same growing input file and the same
    persistent storage: each run must deliver exactly the new entries, so
    nothing is lost and nothing is duplicated across graceful restarts.
    """
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"
    # A short snapshot interval, so the deferred broker acknowledgements
    # (sent only once a checkpoint covers the messages) go out promptly
    # after the phase's data is processed.
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage"),
        snapshot_interval_ms=200,
    )

    def run_identity_program(new_entries: list[str]) -> None:
        G.clear()
        table = pw.io.plaintext.read(input_file, mode="static")
        pw.io.pulsar.write(
            table, PULSAR_SERVICE_URI, pulsar_context.topic, format="json"
        )

        class InputSchema(pw.Schema):
            data: str

        table_reread = pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            pulsar_context.topic,
            schema=InputSchema,
            format="json",
            autocommit_duration_ms=100,
        )
        pw.io.csv.write(table_reread, output_file)

        # The double check keeps the pipeline alive for a while after the
        # expected result is reached. This closes an at-least-once race
        # between the phases: the phase's last messages are acknowledged to
        # the broker only after a checkpoint covers them, and killing the
        # pipeline right away could leave them unacknowledged — the broker
        # would then redeliver them to the next phase, which expects exactly
        # its own new entries. The extra interval both lets the final
        # checkpoint's acknowledgements go out and verifies that no duplicate
        # rows arrive in the meantime.
        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, len(new_entries)),
            WAIT_TIMEOUT_SECS,
            double_check_interval=2.0,
            kwargs={"persistence_config": persistence_config},
        )

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\n")
    run_identity_program(["one", "two", "three", "four"])

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\n")
    run_identity_program(["five", "six"])

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine")
    run_identity_program(["seven", "eight", "nine"])


# --- Authentication tests (the `pulsar-auth` broker: JWT token required) ---


@pytest.mark.flaky(reruns=3)
def test_pulsar_token_authentication(tmp_path):
    """A token-authenticated broker accepts reads and writes with the token."""
    topic = f"pulsar-{uuid4()}"
    auth = pw.io.pulsar.TokenAuthentication(PULSAR_AUTH_TOKEN)
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.jsonl"
    entries = ["one", "two", "three"]
    with open(input_file, "w") as f:
        f.write("\n".join(entries) + "\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.pulsar.write(
        table, PULSAR_AUTH_SERVICE_URI, topic, format="plaintext", auth=auth
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_AUTH_SERVICE_URI,
        topic,
        format="plaintext",
        mode="static",
        auth=auth,
    )
    pw.io.jsonlines.write(table_reread, output_file)
    pw.run()

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == set(entries)


def test_pulsar_authentication_required(tmp_path):
    """The token-authenticated broker rejects a connection without a token."""
    output_file = tmp_path / "output.jsonl"

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_AUTH_SERVICE_URI,
        f"pulsar-{uuid4()}",
        format="plaintext",
        mode="static",
    )
    pw.io.jsonlines.write(table, output_file)
    with pytest.raises(Exception, match="[Pp]ulsar"):
        pw.run()


@pytest.mark.parametrize(
    "subscription_type", ["reader", "shared", "key_shared", "exclusive", "failover"]
)
@pytest.mark.flaky(reruns=3)
def test_pulsar_subscription_types(pulsar_context, tmp_path, subscription_type):
    """Every supported subscription type delivers the full message set."""
    output_file = tmp_path / "output.jsonl"
    n_messages = 3
    for i in range(n_messages):
        pulsar_context.send(f"message-{i}")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        subscription_type=subscription_type,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    wait_result_with_checker(
        FileLinesNumberChecker(output_file, n_messages), WAIT_TIMEOUT_SECS
    )

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == {f"message-{i}" for i in range(n_messages)}


# --- Stabilization tests: previously uncovered paths ---


@pytest.mark.flaky(reruns=3)
def test_pulsar_multiple_workers_shared_read(pulsar_context, tmp_path, monkeypatch):
    """A multi-worker pipeline reads the whole topic through one shared
    subscription: the broker distributes the messages between the workers and
    no message is lost or duplicated."""
    output_file = tmp_path / "output.jsonl"
    n_messages = 200
    for i in range(n_messages):
        pulsar_context.send(f"message-{i:04d}")

    monkeypatch.setenv("PATHWAY_THREADS", "4")
    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    wait_result_with_checker(
        FileLinesNumberChecker(output_file, n_messages), WAIT_TIMEOUT_SECS
    )

    payloads = [
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    ]
    assert len(payloads) == n_messages
    assert set(payloads) == {f"message-{i:04d}" for i in range(n_messages)}


@pytest.mark.flaky(reruns=3)
def test_pulsar_multiple_workers_static_read(pulsar_context, tmp_path, monkeypatch):
    """A multi-worker pipeline in static mode reads the whole topic. The
    static read goes through a single exclusive consumer, so only one worker
    may connect to the broker — the other workers must stay idle instead of
    joining the subscription and being rejected with ConsumerBusy."""
    output_file = tmp_path / "output.jsonl"
    n_messages = 50
    for i in range(n_messages):
        pulsar_context.send(f"message-{i:02d}")

    monkeypatch.setenv("PATHWAY_THREADS", "4")
    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    pw.run()

    payloads = [
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    ]
    assert len(payloads) == n_messages
    assert set(payloads) == {f"message-{i:02d}" for i in range(n_messages)}


@pytest.mark.parametrize("subscription_type", ["exclusive", "failover"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_multiple_workers_single_consumer_subscription(
    pulsar_context, tmp_path, subscription_type, monkeypatch
):
    """A multi-worker pipeline with a single-consumer subscription type reads
    the whole topic through one worker. The other workers must stay idle: a
    second consumer joining an exclusive subscription would be rejected by
    the broker with ConsumerBusy and fail the pipeline."""
    output_file = tmp_path / "output.jsonl"
    n_messages = 50
    for i in range(n_messages):
        pulsar_context.send(f"message-{i:02d}")

    monkeypatch.setenv("PATHWAY_THREADS", "4")
    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        subscription_type=subscription_type,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    wait_result_with_checker(
        FileLinesNumberChecker(output_file, n_messages), WAIT_TIMEOUT_SECS
    )

    payloads = [
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    ]
    assert len(payloads) == n_messages
    assert set(payloads) == {f"message-{i:02d}" for i in range(n_messages)}


@pytest.mark.parametrize("mode", ["streaming", "static"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_partitioned_topic(pulsar_context, tmp_path, mode):
    """Reading a partitioned topic delivers the messages of every partition;
    in static mode the per-partition boundaries terminate the read exactly
    once each partition is drained."""
    topic = pulsar_context.create_partitioned_topic(partitions=3)
    output_file = tmp_path / "output.jsonl"
    n_messages = 30
    # Distinct keys spread the messages across the partitions.
    for i in range(n_messages):
        pulsar_context.send(f"message-{i:02d}", topic=topic, key=f"key-{i % 7}")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        topic,
        format="plaintext",
        mode=mode,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    if mode == "static":
        pw.run()
    else:
        wait_result_with_checker(
            FileLinesNumberChecker(output_file, n_messages), WAIT_TIMEOUT_SECS
        )

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == {f"message-{i:02d}" for i in range(n_messages)}


@pytest.mark.flaky(reruns=3)
def test_pulsar_streaming_start_from_timestamp(pulsar_context, tmp_path):
    """The publish-timestamp filter applies in the streaming mode as well:
    only the messages published at or after the timestamp are delivered."""
    output_file = tmp_path / "output.jsonl"
    for i in range(3):
        pulsar_context.send(f"old-{i}")
    time.sleep(2.0)
    cutoff_timestamp_ms = int(time.time() * 1000)
    time.sleep(2.0)
    for i in range(3):
        pulsar_context.send(f"new-{i}")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        start_from="timestamp",
        start_from_timestamp_ms=cutoff_timestamp_ms,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    wait_result_with_checker(FileLinesNumberChecker(output_file, 3), WAIT_TIMEOUT_SECS)

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == {"new-0", "new-1", "new-2"}


def test_pulsar_raw_binary_roundtrip(pulsar_context, tmp_path):
    """Non-UTF8 binary payloads survive the write/read roundtrip untouched."""
    binary_payloads = [bytes([0, 255, 254, 1, 2, 128]), b"\x89PNG\r\n\x1a\n"]
    for payload in binary_payloads:
        producer = pulsar_context._client.create_producer(pulsar_context.topic)
        producer.send(payload)
        producer.close()

    output_file = tmp_path / "output.jsonl"
    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="raw",
        mode="static",
    )
    pw.io.jsonlines.write(table, output_file)
    pw.run()

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert len(lines) == len(binary_payloads)


@pytest.mark.flaky(reruns=3)
def test_pulsar_write_publishes_rows_that_fill_a_batch(pulsar_context, tmp_path):
    """Rows of a few kilobytes are published normally. The producer groups the
    messages into batches, and a batch is a single Pulsar message on the wire,
    so the batching must stay within the broker's message size limit no matter
    how large the individual rows are — otherwise the broker drops the
    connection and the whole write is lost."""
    n_messages = 1200  # x 10 KB: far above the broker's 5 MB default per message
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.jsonl"
    with open(input_file, "w") as f:
        for i in range(n_messages):
            f.write(f"{i:06d}" + "x" * 10_000 + "\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.pulsar.write(
        table, PULSAR_SERVICE_URI, pulsar_context.topic, format="plaintext"
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
    )
    pw.io.jsonlines.write(table_reread, output_file)
    pw.run()

    assert len(output_file.read_text().splitlines()) == n_messages


@pytest.mark.flaky(reruns=3)
def test_pulsar_write_large_batch_backpressure(pulsar_context, tmp_path):
    """A minibatch far larger than the writer's in-flight window is delivered
    completely: the backpressure drain frees the window without stalling the
    producer batching."""
    n_messages = 60_000  # exceeds MAX_IN_FLIGHT_SENDS (50k) in one minibatch
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.jsonl"
    with open(input_file, "w") as f:
        for i in range(n_messages):
            f.write(f"message-{i:05d}\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.pulsar.write(
        table, PULSAR_SERVICE_URI, pulsar_context.topic, format="plaintext"
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
    )
    pw.io.jsonlines.write(table_reread, output_file)
    pw.run()

    assert len(output_file.read_text().splitlines()) == n_messages


def test_pulsar_invalid_token_rejected(tmp_path):
    """The token-authenticated broker rejects a syntactically valid but
    wrongly signed token."""
    output_file = tmp_path / "output.jsonl"
    bad_token = PULSAR_AUTH_TOKEN[:-4] + "AAAA"  # break the signature

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_AUTH_SERVICE_URI,
        f"pulsar-{uuid4()}",
        format="plaintext",
        mode="static",
        auth=pw.io.pulsar.TokenAuthentication(bad_token),
    )
    pw.io.jsonlines.write(table, output_file)
    with pytest.raises(Exception, match="[Pp]ulsar"):
        pw.run()


def test_pulsar_write_rejects_non_string_key(pulsar_context, tmp_path):
    """A key column that is neither a string nor UTF-8 bytes fails the write
    with a clear error instead of producing a corrupted partition key."""
    input_file = tmp_path / "input.jsonl"
    with open(input_file, "w") as f:
        f.write(json.dumps({"num_key": 5, "value": "foo"}) + "\n")

    class InputSchema(pw.Schema):
        num_key: int
        value: str

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    with pytest.raises(Exception):
        pw.io.pulsar.write(
            table,
            PULSAR_SERVICE_URI,
            pulsar_context.topic,
            format="json",
            key=table.num_key,
        )
        pw.run()


def test_pulsar_write_rejects_non_utf8_key(pulsar_context, tmp_path):
    """Pulsar stores partition keys as strings, so a binary key column whose
    bytes are not valid UTF-8 cannot be published. Such a key must fail with an
    error naming the key and its encoding requirement, rather than with a bare
    decoding failure that gives no hint about which column is at fault."""
    input_file = tmp_path / "input.txt"
    input_file.write_text("one\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    table = table.with_columns(
        binary_key=pw.apply_with_type(lambda _: b"\xff\xfe", bytes, pw.this.data)
    )
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        value=table.data,
        key=table.binary_key,
    )
    with pytest.raises(Exception) as exception_info:
        pw.run()
    message = str(exception_info.value)
    assert "key" in message.lower(), message
    assert "utf-8" in message.lower(), message


def test_pulsar_rejects_empty_names():
    """A name that is only ever passed on to the broker — the subscription of a
    read, the producer name of a write — must be rejected when the connector is
    created if it is empty. An empty subscription name in particular buys a
    pipeline that subscribes to nothing and delivers nothing forever, which is
    indistinguishable from an idle topic; both cases usually come from an
    unset environment variable, and the earlier they surface the better."""
    G.clear()
    with pytest.raises(ValueError, match="subscription_name"):
        pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            "some-topic",
            format="plaintext",
            subscription_name="",
        )

    G.clear()
    table = pw.debug.table_from_markdown(
        """
        data
        one
        """
    )
    with pytest.raises(ValueError, match="producer_name"):
        pw.io.pulsar.write(
            table,
            PULSAR_SERVICE_URI,
            "some-topic",
            format="plaintext",
            producer_name="",
        )


def test_pulsar_start_from_validation():
    """Contradictory start_from arguments are rejected at graph-build time."""
    G.clear()
    with pytest.raises(ValueError, match="start_from_timestamp_ms is required"):
        pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            "some-topic",
            format="plaintext",
            start_from="timestamp",
        )
    with pytest.raises(ValueError, match="must not be set"):
        pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            "some-topic",
            format="plaintext",
            start_from="beginning",
            start_from_timestamp_ms=123,
        )
    # -1 is the engine's internal sentinel for start_from="end"; an explicit
    # negative timestamp must raise instead of silently reading from the end.
    with pytest.raises(ValueError, match="must be a non-negative"):
        pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            "some-topic",
            format="plaintext",
            start_from="timestamp",
            start_from_timestamp_ms=-1,
        )


# --- Message key tests ---


@pytest.mark.parametrize(
    "reader_kwargs",
    [
        {"subscription_type": "reader"},
        {"subscription_type": "shared", "subscription_name": "unload-durable-sub"},
    ],
    ids=["reader_mode", "durable_subscription"],
)
@pytest.mark.flaky(reruns=3)
def test_pulsar_read_survives_topic_unload_without_duplicates(
    pulsar_context, tmp_path, reader_kwargs
):
    """Unloading a topic is a routine broker operation — it happens on
    rebalancing, on namespace bundle moves and on broker restarts — and it
    drops the connections of everyone attached to the topic. The reading
    mechanisms that track their position durably, the partition reader on the
    client side and a named subscription on the broker side, must come back
    from it having delivered every message exactly once."""
    output_file = tmp_path / "output.jsonl"
    before = [f"before-{i:03d}" for i in range(20)]
    after = [f"after-{i:03d}" for i in range(15)]
    for message in before:
        pulsar_context.send(message)

    def unload_and_publish_more():
        deadline = time.monotonic() + WAIT_TIMEOUT_SECS
        while time.monotonic() < deadline:
            if output_file.exists() and len(
                output_file.read_text().splitlines()
            ) >= len(before):
                break
            time.sleep(0.5)
        response = requests.put(
            f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/"
            f"{pulsar_context.topic}/unload",
            timeout=60,
        )
        response.raise_for_status()
        time.sleep(3.0)
        for message in after:
            pulsar_context.send(message)

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        autocommit_duration_ms=100,
        **reader_kwargs,
    )
    pw.io.jsonlines.write(table, output_file)
    worker = threading.Thread(target=unload_and_publish_more)
    worker.start()
    try:
        wait_result_with_checker(
            FileLinesNumberChecker(output_file, len(before) + len(after)),
            WAIT_TIMEOUT_SECS * 2,
        )
    finally:
        worker.join()

    payloads = [
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    ]
    assert sorted(payloads) == sorted(before + after)


@pytest.mark.parametrize("input_format", ["plaintext", "raw"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_read_message_key(pulsar_context, input_format):
    """The raw and plaintext formats expose the partition key of every message
    in the ``key`` column next to the payload in ``data``; a message published
    without a partition key gets ``None``."""
    context = [("1", "one"), ("2", "two"), ("3", "three")]
    for key, value in context:
        pulsar_context.send(value, key=key)
    pulsar_context.send("keyless")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format=input_format,
        mode="static",
    )
    pandas_table = pw.debug.table_to_pandas(table)

    assert len(pandas_table) == len(context) + 1
    for key, value in context:
        if input_format != "plaintext":
            key = key.encode("utf-8")  # type: ignore
            value = value.encode("utf-8")  # type: ignore
        row = pandas_table.loc[pandas_table["key"] == key, ["data"]].iloc[0]
        assert (row == pd.Series({"data": value})).all()
    keyless_payload = "keyless" if input_format == "plaintext" else b"keyless"
    keyless_rows = pandas_table.loc[pandas_table["data"] == keyless_payload]
    assert len(keyless_rows) == 1
    assert keyless_rows.iloc[0]["key"] is None


@pytest.mark.flaky(reruns=3)
def test_pulsar_read_derives_row_keys_from_message_keys(pulsar_context):
    """The primary key of a row follows the partition key of the message: the
    messages sharing a partition key land under one primary key, so a topic
    with repeating keys produces fewer primary keys than messages. With
    autogenerate_key=True every message becomes a row of its own instead."""
    pulsar_context.send("a", key="key-0")
    pulsar_context.send("b", key="shared")
    pulsar_context.send("c", key="shared")

    def rows_per_primary_key(autogenerate_key: bool) -> list[int]:
        G.clear()
        table = pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            pulsar_context.topic,
            format="plaintext",
            mode="static",
            autogenerate_key=autogenerate_key,
        )
        # Grouping by the row id, rather than reading the table directly,
        # keeps the check meaningful when several rows share a primary key.
        counts = table.groupby(pw.this.id).reduce(cnt=pw.reducers.count())
        return sorted(pw.debug.table_to_pandas(counts)["cnt"].tolist())

    assert rows_per_primary_key(autogenerate_key=False) == [1, 2]
    assert rows_per_primary_key(autogenerate_key=True) == [1, 1, 1]


@pytest.mark.flaky(reruns=3)
def test_pulsar_message_key_write_read_roundtrip(pulsar_context, tmp_path):
    """The key written by pw.io.pulsar.write's ``key`` parameter comes back in
    the ``key`` column of pw.io.pulsar.read."""
    input_file = tmp_path / "input.jsonl"
    rows = [
        {"sensor": "front-door", "reading": "21"},
        {"sensor": "back-door", "reading": "22"},
    ]
    with open(input_file, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    class InputSchema(pw.Schema):
        sensor: str
        reading: str

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        key=table.sensor,
        value=table.reading,
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
    )
    pandas_table = pw.debug.table_to_pandas(table_reread)
    assert {(row["key"], row["data"]) for _, row in pandas_table.iterrows()} == {
        (row["sensor"], row["reading"]) for row in rows
    }


@pytest.mark.flaky(reruns=3)
def test_pulsar_read_autogenerate_key(pulsar_context):
    """With autogenerate_key=True every message gets its own autogenerated
    primary key, so the messages sharing a partition key stay separate rows;
    the ``key`` column still carries the partition key."""
    for i in range(3):
        pulsar_context.send(f"message-{i}", key="shared-key")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
        autogenerate_key=True,
    )
    pandas_table = pw.debug.table_to_pandas(table)
    assert len(pandas_table) == 3
    assert set(pandas_table["key"]) == {"shared-key"}
    assert set(pandas_table["data"]) == {f"message-{i}" for i in range(3)}


def test_pulsar_read_autogenerate_key_with_json_rejected():
    """``autogenerate_key`` only applies to the ``raw`` and ``plaintext``
    formats, so passing it with ``format='json'`` must be rejected up front
    with an error that names the parameter."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)

    G.clear()
    with pytest.raises(ValueError, match="autogenerate_key"):
        pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            "some-topic",
            format="json",
            schema=InputSchema,
            mode="static",
            autogenerate_key=True,
        )


# --- Metadata tests ---


@pytest.mark.parametrize("mode", ["streaming", "static"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_read_with_metadata(pulsar_context, tmp_path, mode):
    """with_metadata=True adds a _metadata column describing the message: the
    topic, the partition, the message id components, the publish and event
    timestamps, the producer name, the ordering key and the properties —
    through both reading mechanisms."""
    import base64

    output_file = tmp_path / "output.jsonl"
    event_ts_ms = 1_700_000_000_000
    producer = pulsar_context._client.create_producer(
        pulsar_context.topic, producer_name="external-producer"
    )
    try:
        producer.send(
            b"payload-0",
            partition_key="k0",
            ordering_key="entity-7",
            properties={"origin": "external"},
            event_timestamp=event_ts_ms,
        )
    finally:
        producer.close()

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode=mode,
        with_metadata=True,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    if mode == "static":
        pw.run()
    else:
        wait_result_with_checker(
            FileLinesNumberChecker(output_file, 1), WAIT_TIMEOUT_SECS
        )

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert len(lines) == 1
    row = lines[0]
    assert row["key"] == "k0"
    assert row["data"] == "payload-0"
    metadata = row["_metadata"]
    assert pulsar_context.topic in metadata["topic"]
    assert metadata["partition"] == -1
    assert metadata["batch_index"] == -1
    assert metadata["ledger_id"] >= 0 and metadata["entry_id"] >= 0
    assert metadata["publish_time_millis"] > 0
    assert metadata["event_time_millis"] == event_ts_ms
    assert metadata["producer_name"] == "external-producer"
    assert metadata["ordering_key"] == base64.b64encode(b"entity-7").decode()
    assert metadata["properties"] == {"origin": "external"}


@pytest.mark.parametrize("mode", ["streaming", "static"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_read_with_metadata_reports_real_partition(
    pulsar_context, tmp_path, mode
):
    """On a partitioned topic the ``partition`` field of the metadata carries
    the index of the partition the message was read from — the ``-1`` of the
    non-partitioned case is not reported instead. The index is checked against
    the physical topic name of the same metadata entry, so the assertion holds
    whichever partitions the keys happen to hash into."""
    topic = pulsar_context.create_partitioned_topic(partitions=3)
    output_file = tmp_path / "output.jsonl"
    n_messages = 30
    for i in range(n_messages):
        pulsar_context.send(f"message-{i:02d}", topic=topic, key=f"key-{i}")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        topic,
        format="plaintext",
        mode=mode,
        with_metadata=True,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    if mode == "static":
        pw.run()
    else:
        wait_result_with_checker(
            FileLinesNumberChecker(output_file, n_messages), WAIT_TIMEOUT_SECS
        )

    partitions = set()
    for line in output_file.read_text().splitlines():
        metadata = json.loads(line)["_metadata"]
        partition_from_topic = int(metadata["topic"].rsplit("-partition-", 1)[1])
        assert metadata["partition"] == partition_from_topic, metadata
        partitions.add(metadata["partition"])
    assert partitions, "no messages were read"
    assert -1 not in partitions


@pytest.mark.flaky(reruns=3)
def test_pulsar_write_read_roundtrip_metadata(pulsar_context, tmp_path):
    """The metadata of the messages produced by pw.io.pulsar.write is visible
    to a metadata-enabled reader: the producer name carries the per-worker
    suffix and the headers arrive as properties alongside pathway_time and
    pathway_diff."""
    input_file = tmp_path / "input.jsonl"
    output_file = tmp_path / "output.jsonl"
    with open(input_file, "w") as f:
        f.write(json.dumps({"word": "hello", "note": "greeting"}) + "\n")

    class InputSchema(pw.Schema):
        word: str
        note: str

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        value=table.word,
        headers=[table.note],
        producer_name="metadata-writer",
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
        with_metadata=True,
    )
    pw.io.jsonlines.write(table_reread, output_file)
    pw.run()

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert len(lines) == 1
    metadata = lines[0]["_metadata"]
    assert metadata["producer_name"] == "metadata-writer-0"
    assert json.loads(metadata["properties"]["note"]) == "greeting"
    assert metadata["properties"]["pathway_diff"] == "1"
    assert "pathway_time" in metadata["properties"]


# --- Event-time tests ---


@pytest.mark.parametrize("column_kind", ["int", "datetime_utc"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_write_event_time_from_column(pulsar_context, tmp_path, column_kind):
    """The event_time column of pw.io.pulsar.write becomes the native
    event_time of the produced messages, for both supported column types:
    integer milliseconds and UTC datetimes."""
    input_file = tmp_path / "input.jsonl"
    output_file = tmp_path / "output.jsonl"
    rows = [
        {"name": "first", "ts_ms": 1_700_000_000_000},
        {"name": "second", "ts_ms": 1_700_000_060_000},
    ]
    with open(input_file, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    class InputSchema(pw.Schema):
        name: str
        ts_ms: int

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    if column_kind == "datetime_utc":
        table = table.with_columns(
            event_ts=table.ts_ms.dt.utc_from_timestamp(unit="ms")
        )
        event_time_column = table.event_ts
    else:
        event_time_column = table.ts_ms
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        value=table.name,
        event_time=event_time_column,
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
        with_metadata=True,
    )
    pw.io.jsonlines.write(table_reread, output_file)
    pw.run()

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    received = {line["data"]: line["_metadata"]["event_time_millis"] for line in lines}
    assert received == {row["name"]: row["ts_ms"] for row in rows}


@pytest.mark.flaky(reruns=3)
def test_pulsar_write_event_time_from_engine(pulsar_context, tmp_path):
    """pw.io.ENGINE_TIME sets the native event_time of the messages to the
    engine (minibatch) time of the update — exactly the value the messages
    carry in the pathway_time property."""
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.jsonl"
    input_file.write_text("one\ntwo\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        event_time=pw.io.ENGINE_TIME,
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
        with_metadata=True,
    )
    pw.io.jsonlines.write(table_reread, output_file)
    pw.run()

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert len(lines) == 2
    for line in lines:
        metadata = line["_metadata"]
        assert metadata["event_time_millis"] == int(
            metadata["properties"]["pathway_time"]
        )
        # The engine time of pw.io.* data is a wall-clock UNIX timestamp.
        assert metadata["event_time_millis"] > 1_500_000_000_000


def test_pulsar_write_rejects_naive_event_time(pulsar_context, tmp_path):
    """A timezone-naive datetime column is rejected as the event time with an
    error suggesting an explicit UTC conversion, instead of silently assuming
    a timezone."""
    input_file = tmp_path / "input.jsonl"
    input_file.write_text(json.dumps({"name": "x", "ts_ms": 1}) + "\n")

    class InputSchema(pw.Schema):
        name: str
        ts_ms: int

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    table = table.with_columns(
        naive_ts=table.ts_ms.dt.from_timestamp(unit="ms"),
    )
    with pytest.raises(ValueError, match="naive"):
        pw.io.pulsar.write(
            table,
            PULSAR_SERVICE_URI,
            pulsar_context.topic,
            format="plaintext",
            value=table.name,
            event_time=table.naive_ts,
        )


def test_pulsar_write_rejects_negative_event_time(pulsar_context, tmp_path):
    """A negative event-time value fails the write with a clear error instead
    of silently producing a message with a bogus timestamp."""
    input_file = tmp_path / "input.jsonl"
    input_file.write_text(json.dumps({"name": "x", "ts_ms": -5}) + "\n")

    class InputSchema(pw.Schema):
        name: str
        ts_ms: int

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        value=table.name,
        event_time=table.ts_ms,
    )
    with pytest.raises(Exception, match="event time"):
        pw.run()


# --- Ordering-key tests ---


@pytest.mark.flaky(reruns=3)
def test_pulsar_write_ordering_key(pulsar_context, tmp_path):
    """The ordering_key column of pw.io.pulsar.write becomes the ordering key
    of the produced messages, independent of the partition key; rows without
    it configured leave the ordering key unset."""
    input_file = tmp_path / "input.jsonl"
    rows = [
        {"tenant": "acme", "user": "alice", "action": "login"},
        {"tenant": "acme", "user": "bob", "action": "logout"},
    ]
    with open(input_file, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    class InputSchema(pw.Schema):
        tenant: str
        user: str
        action: str

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="json",
        key=table.tenant,
        ordering_key=table.user,
    )
    pw.run()

    messages = pulsar_context.read_messages(expected_count=len(rows))
    received = {}
    for message in messages:
        payload = json.loads(message.data())
        received[payload["user"]] = message
    assert set(received) == {"alice", "bob"}
    for row in rows:
        message = received[row["user"]]
        assert message.partition_key() == row["tenant"]
        assert message.ordering_key() == row["user"]


def test_pulsar_write_rejects_non_string_ordering_key(pulsar_context, tmp_path):
    """An ordering_key column that is neither a string nor bytes is rejected
    with a clear error instead of producing a corrupted ordering key."""
    input_file = tmp_path / "input.jsonl"
    with open(input_file, "w") as f:
        f.write(json.dumps({"num_key": 5, "value": "foo"}) + "\n")

    class InputSchema(pw.Schema):
        num_key: int
        value: str

    G.clear()
    table = pw.io.jsonlines.read(input_file, schema=InputSchema, mode="static")
    with pytest.raises(ValueError, match="ordering key"):
        pw.io.pulsar.write(
            table,
            PULSAR_SERVICE_URI,
            pulsar_context.topic,
            format="json",
            ordering_key=table.num_key,
        )


# --- Compacted-topic and producer-name tests ---


def _compact_topic_and_wait(topic: str, timeout_sec: float = 60.0) -> None:
    """Triggers compaction of the topic over the admin API and waits until it
    completes."""
    compaction_url = (
        f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/{topic}/compaction"
    )
    response = requests.put(compaction_url, timeout=60)
    response.raise_for_status()
    deadline = time.monotonic() + timeout_sec
    while True:
        status = requests.get(compaction_url, timeout=60).json()["status"]
        if status == "SUCCESS":
            return
        assert status in ("NOT_RUN", "RUNNING"), f"compaction failed: {status}"
        assert time.monotonic() < deadline, "compaction did not finish in time"
        time.sleep(0.5)


@pytest.mark.flaky(reruns=3)
def test_pulsar_read_compacted_returns_latest_state_per_key(pulsar_context, tmp_path):
    """With read_compacted=True the compacted part of the topic delivers only
    the latest message of every partition key, not the full history."""
    output_file = tmp_path / "output.jsonl"
    pulsar_context.send("stale-front", key="front-door")
    pulsar_context.send("fresh-front", key="front-door")
    pulsar_context.send("fresh-back", key="back-door")
    _compact_topic_and_wait(pulsar_context.topic)

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        subscription_type="exclusive",
        read_compacted=True,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    # The double check keeps the pipeline running after two lines arrive and
    # verifies that the compacted-away message never shows up as a third.
    wait_result_with_checker(
        FileLinesNumberChecker(output_file, 2),
        WAIT_TIMEOUT_SECS,
        double_check_interval=2.0,
    )

    lines = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert {(line["key"], line["data"]) for line in lines} == {
        ("front-door", "fresh-front"),
        ("back-door", "fresh-back"),
    }


def test_pulsar_read_compacted_validation():
    """read_compacted is limited to the streaming single-consumer
    subscriptions; every other combination is rejected at graph-build time
    with a clear error."""
    G.clear()
    for kwargs in [
        {},  # the default subscription type is "shared"
        {"subscription_type": "key_shared"},
        {"subscription_type": "reader"},
        {"subscription_type": "exclusive", "mode": "static"},
    ]:
        with pytest.raises(ValueError, match="read_compacted"):
            pw.io.pulsar.read(
                PULSAR_SERVICE_URI,
                "some-topic",
                format="plaintext",
                read_compacted=True,
                **kwargs,
            )


@pytest.mark.flaky(reruns=3)
def test_pulsar_write_producer_name_visible_to_broker(pulsar_context, tmp_path):
    """The producer_name parameter names the producers on the broker: the
    messages carry the producer name (with the per-worker suffix), which the
    admin API reports."""
    input_file = tmp_path / "input.txt"
    input_file.write_text("one\n")
    # A durable subscription to peek through, created before the write so it
    # covers the written message.
    consumer = pulsar_context._client.subscribe(
        pulsar_context.topic,
        subscription_name="peek-sub",
        initial_position=pulsar_context._pulsar.InitialPosition.Earliest,
    )
    consumer.close()

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        producer_name="pathway-test-writer",
    )
    pw.run()

    response = requests.get(
        f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/"
        f"{pulsar_context.topic}/subscription/peek-sub/position/1",
        timeout=60,
    )
    response.raise_for_status()
    assert response.headers["X-Pulsar-producer-name"] == "pathway-test-writer-0"


# --- Compression tests ---


@pytest.mark.parametrize("compression", ["lz4", "zlib", "zstd"])
@pytest.mark.parametrize("mode", ["streaming", "static"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_compressed_write_read_roundtrip(
    pulsar_context, tmp_path, compression, mode
):
    """Messages written with every supported compression codec are read back
    intact, through both reading mechanisms (the static mode reads through the
    partition readers, the streaming mode through a shared subscription), and
    the official Pulsar client decompresses them as well."""
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.jsonl"
    entries = [f"payload-{i}-{'x' * 100}" for i in range(5)]
    with open(input_file, "w") as f:
        f.write("\n".join(entries) + "\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.pulsar.write(
        table,
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        compression=compression,
    )
    pw.run()

    G.clear()
    table_reread = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode=mode,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table_reread, output_file)
    if mode == "static":
        pw.run()
    else:
        wait_result_with_checker(
            FileLinesNumberChecker(output_file, len(entries)), WAIT_TIMEOUT_SECS
        )

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == set(entries)

    # Interoperability: the official client decodes our compressed messages.
    messages = pulsar_context.read_messages(expected_count=len(entries))
    assert {message.data().decode() for message in messages} == set(entries)


@pytest.mark.parametrize("codec_name", ["LZ4", "ZLib", "ZSTD"])
@pytest.mark.flaky(reruns=3)
def test_pulsar_read_externally_compressed_messages(
    pulsar_context, tmp_path, codec_name
):
    """Messages compressed by a foreign producer (the official Pulsar client)
    are decompressed transparently by the connector."""
    import pulsar as pulsar_client

    output_file = tmp_path / "output.jsonl"
    entries = [f"external-{i}-{'y' * 100}" for i in range(5)]
    producer = pulsar_context._client.create_producer(
        pulsar_context.topic,
        compression_type=getattr(pulsar_client.CompressionType, codec_name),
    )
    try:
        for entry in entries:
            producer.send(entry.encode())
    finally:
        producer.close()

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        mode="static",
    )
    pw.io.jsonlines.write(table, output_file)
    pw.run()

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == set(entries)


@pytest.mark.flaky(reruns=3)
def test_pulsar_compression_reduces_stored_size(pulsar_context, tmp_path):
    """Compression is applied on the wire, not just accepted as a parameter:
    the broker-side storage size of a topic with highly compressible payloads
    is a fraction of the size of the same payloads sent uncompressed."""
    input_file = tmp_path / "input.txt"
    n_messages = 200
    payload_size = 10_000
    entries = [f"{i:06d}-" + "z" * payload_size for i in range(n_messages)]
    with open(input_file, "w") as f:
        f.write("\n".join(entries) + "\n")
    uncompressed_topic = f"pulsar-{uuid4()}"
    compressed_topic = f"pulsar-{uuid4()}"

    for topic, compression in (
        (uncompressed_topic, None),
        (compressed_topic, "zstd"),
    ):
        G.clear()
        table = pw.io.plaintext.read(input_file, mode="static")
        pw.io.pulsar.write(
            table,
            PULSAR_SERVICE_URI,
            topic,
            format="plaintext",
            compression=compression,
        )
        pw.run()

    def topic_storage_size(topic: str) -> int:
        response = requests.get(
            f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/{topic}/stats",
            timeout=60,
        )
        response.raise_for_status()
        return response.json()["storageSize"]

    uncompressed_size = topic_storage_size(uncompressed_topic)
    compressed_size = topic_storage_size(compressed_topic)
    total_payload_size = sum(len(entry) for entry in entries)
    assert uncompressed_size >= total_payload_size // 2
    assert compressed_size < total_payload_size // 5
    assert compressed_size < uncompressed_size // 5


@pytest.mark.parametrize("codec", ["gzip", "snappy"])
def test_pulsar_write_rejects_unsupported_compression(pulsar_context, tmp_path, codec):
    """A compression codec the connector does not support — an unknown one, or
    snappy, whose framing in the client library is incompatible with the other
    Pulsar clients — is rejected when the sink is created, with an error naming
    the supported codecs, instead of silently sending the messages
    uncompressed."""
    input_file = tmp_path / "input.txt"
    input_file.write_text("one\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    with pytest.raises(Exception) as exception_info:
        pw.io.pulsar.write(
            table,
            PULSAR_SERVICE_URI,
            pulsar_context.topic,
            format="plaintext",
            compression=codec,
        )
    message = str(exception_info.value)
    assert "compression" in message, message
    for supported_codec in ("lz4", "zlib", "zstd"):
        assert supported_codec in message, message


# --- Partition-reader (Kafka-like) persistence tests ---


def test_pulsar_persistence_rejects_subscription_modes(tmp_path):
    """Persistence requires the partition-reader mode: a broker-side
    subscription cannot replay the messages a restarted pipeline needs."""
    output_file = tmp_path / "output.jsonl"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage")
    )

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        f"pulsar-{uuid4()}",
        format="plaintext",
        subscription_type="shared",
    )
    pw.io.jsonlines.write(table, output_file)
    with pytest.raises(Exception, match="cannot be used with persistence"):
        pw.run(persistence_config=persistence_config)


def test_pulsar_persistence_rejects_start_from_end(tmp_path):
    """Persistence rejects start_from="end": the "end" position would be
    re-resolved at every restart, so the messages published into a partition
    without a checkpointed position while the pipeline was down would be
    silently lost. An explicit start_from="timestamp" identifies the same
    starting point deterministically and must be used instead."""
    output_file = tmp_path / "output.jsonl"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage")
    )

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        f"pulsar-{uuid4()}",
        format="plaintext",
        start_from="end",
    )
    pw.io.jsonlines.write(table, output_file)
    with pytest.raises(Exception, match="cannot be used with persistence"):
        pw.run(persistence_config=persistence_config)


@pytest.mark.flaky(reruns=3)
def test_pulsar_reader_mode_streaming_roundtrip(pulsar_context, tmp_path):
    """The explicit partition-reader mode works for plain streaming reads."""
    output_file = tmp_path / "output.jsonl"
    n_messages = 5
    for i in range(n_messages):
        pulsar_context.send(f"message-{i}")

    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="plaintext",
        subscription_type="reader",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, output_file)
    wait_result_with_checker(
        FileLinesNumberChecker(output_file, n_messages), WAIT_TIMEOUT_SECS
    )

    payloads = {
        json.loads(line)["data"] for line in output_file.read_text().splitlines()
    }
    assert payloads == {f"message-{i}" for i in range(n_messages)}


@pytest.mark.flaky(reruns=3)
def test_pulsar_persistence_partitioned_topic(pulsar_context, tmp_path):
    """Persistent reading of a partitioned topic resumes every partition from
    its own checkpointed position: restarts deliver exactly the new messages
    of each partition."""
    topic = pulsar_context.create_partitioned_topic(partitions=3)
    output_file = tmp_path / "output.txt"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage"),
        snapshot_interval_ms=200,
    )

    def run_phase(new_messages: list[str]) -> None:
        for i, message in enumerate(new_messages):
            pulsar_context.send(message, topic=topic, key=f"key-{i % 5}")

        G.clear()
        table = pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            topic,
            format="plaintext",
            autocommit_duration_ms=100,
        )
        pw.io.csv.write(table, output_file)
        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, len(new_messages)),
            WAIT_TIMEOUT_SECS,
            double_check_interval=2.0,
            kwargs={"persistence_config": persistence_config},
        )

    run_phase([f"first-{i:02d}" for i in range(10)])
    run_phase([f"second-{i:02d}" for i in range(7)])
    run_phase([f"third-{i:02d}" for i in range(4)])


@pytest.mark.flaky(reruns=3)
def test_pulsar_persistence_multiple_workers(pulsar_context, tmp_path, monkeypatch):
    """Persistence with several workers: every worker resumes its partitions
    from the checkpoint, and restarts deliver exactly the new messages."""
    topic = pulsar_context.create_partitioned_topic(partitions=4)
    output_file = tmp_path / "output.txt"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage"),
        snapshot_interval_ms=200,
    )
    monkeypatch.setenv("PATHWAY_THREADS", "4")

    def run_phase(new_messages: list[str]) -> None:
        for i, message in enumerate(new_messages):
            pulsar_context.send(message, topic=topic, key=f"key-{i % 11}")

        G.clear()
        table = pw.io.pulsar.read(
            PULSAR_SERVICE_URI,
            topic,
            format="plaintext",
            autocommit_duration_ms=100,
        )
        pw.io.csv.write(table, output_file)
        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, len(new_messages)),
            WAIT_TIMEOUT_SECS,
            double_check_interval=2.0,
            kwargs={"persistence_config": persistence_config},
        )

    run_phase([f"first-{i:02d}" for i in range(20)])
    run_phase([f"second-{i:02d}" for i in range(12)])

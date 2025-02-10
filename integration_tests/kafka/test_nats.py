import json
import pathlib
from uuid import uuid4

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    FileLinesNumberChecker,
    wait_result_with_checker,
)

NATS_SERVER_URI = "nats://nats:4222/"


def run_identity_program(
    input_file, output_file, nats_topic, new_entries, persistence_config=None
):
    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.nats.write(
        table,
        uri=NATS_SERVER_URI,
        topic=nats_topic,
        format="json",
    )

    class InputSchema(pw.Schema):
        data: str

    table_reread = pw.io.nats.read(
        uri=NATS_SERVER_URI,
        topic=nats_topic,
        schema=InputSchema,
        format="json",
    )
    pw.io.csv.write(table_reread, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, len(new_entries)),
        30,
        kwargs={"persistence_config": persistence_config},
    )


@pytest.mark.flaky(reruns=5)
def test_nats_simple(tmp_path: pathlib.Path):
    nats_topic = f"nats-{uuid4()}"
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\n")
    run_identity_program(
        input_file, output_file, nats_topic, ["one", "two", "three", "four"]
    )


@pytest.mark.flaky(reruns=5)
def test_nats_persistence(tmp_path: pathlib.Path):
    nats_topic = f"nats-{uuid4()}"
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"

    config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage")
    )

    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\n")
    run_identity_program(
        input_file, output_file, nats_topic, ["one", "two", "three", "four"], config
    )
    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\n")
    run_identity_program(input_file, output_file, nats_topic, ["five", "six"], config)
    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine")
    run_identity_program(
        input_file, output_file, nats_topic, ["seven", "eight", "nine"], config
    )


@pytest.mark.parametrize("output_format", ["json", "plaintext"])
@pytest.mark.flaky(reruns=3)
def test_nats_dynamic_topics(tmp_path, output_format):
    input_path = tmp_path / "input.jsonl"
    output_path_1 = tmp_path / "output_1.jsonl"
    output_path_2 = tmp_path / "output_2.jsonl"
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

    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
    if output_format == "json":
        write_kwargs = {"format": "json"}
    elif output_format == "plaintext":
        write_kwargs = {
            "format": "plaintext",
            "value": table.k,
        }
    else:
        raise RuntimeError(f"Unknown output format: {output_format}")
    pw.io.nats.write(
        table,
        uri=NATS_SERVER_URI,
        topic=table.select(topic="NatsTopic." + pw.this.t)["topic"],
        **write_kwargs,
    )
    stream_1 = pw.io.nats.read(
        uri=NATS_SERVER_URI,
        topic=f"NatsTopic.{dynamic_topic_1}",
        format="plaintext",
    )
    stream_2 = pw.io.nats.read(
        uri=NATS_SERVER_URI,
        topic=f"NatsTopic.{dynamic_topic_2}",
        format="plaintext",
    )
    pw.io.jsonlines.write(stream_1, output_path_1)
    pw.io.jsonlines.write(stream_2, output_path_2)
    wait_result_with_checker(FileLinesNumberChecker(output_path_1, 2), 30)

    def check_keys_in_file(path: pathlib.Path, expected_keys: set[str]):
        keys = set()
        with open(path, "r") as f:
            for message in f:
                message = json.loads(message)["data"]
                if output_format == "json":
                    value = json.loads(message)
                    keys.add(value["k"])
                    assert value.keys() == {"k", "v", "t", "time", "diff", "topic"}
                else:
                    keys.add(message)
            assert keys == expected_keys

    check_keys_in_file(output_path_1, {"0", "2"})
    check_keys_in_file(output_path_2, {"1"})

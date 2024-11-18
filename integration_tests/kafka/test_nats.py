import pathlib
from uuid import uuid4

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import CsvLinesNumberChecker, wait_result_with_checker

NATS_SERVER_URI = "nats://nats:4222/"


def run(input_file, output_file, nats_topic, new_entries, persistence_config=None):
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
    run(input_file, output_file, nats_topic, ["one", "two", "three", "four"])


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
    run(input_file, output_file, nats_topic, ["one", "two", "three", "four"], config)
    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\n")
    run(input_file, output_file, nats_topic, ["five", "six"], config)
    with open(input_file, "w") as f:
        f.write("one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine")
    run(input_file, output_file, nats_topic, ["seven", "eight", "nine"], config)

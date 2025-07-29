# Copyright © 2024 Pathway

import json
import os
import pathlib

import pandas as pd
import pytest

import pathway as pw
from pathway.internals import api
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.parse_graph import G
from pathway.tests.utils import get_aws_s3_settings, write_lines

from .base import create_jsonlines, put_aws_object, read_jsonlines_fields


@pytest.mark.parametrize(
    "snapshot_access", [api.SnapshotAccess.FULL, api.SnapshotAccess.OFFSETS_ONLY]
)
def test_s3_backfilling(snapshot_access, tmp_path: pathlib.Path, s3_path: str):
    pathway_persistent_storage = tmp_path / "PStorage"
    s3_input_path = f"{s3_path}/input.csv"

    input_contents = "key,value\n1,Hello\n2,World"
    put_aws_object(s3_input_path, input_contents)
    table = pw.io.s3.read(
        s3_path,
        aws_s3_settings=get_aws_s3_settings(),
        schema=pw.schema_from_types(key=int, value=str),
        format="csv",
        mode="static",
        autocommit_duration_ms=1000,
        name="1",
    )
    pw.io.csv.write(table, str(tmp_path / "output.csv"))
    pw.run(
        monitoring_level=MonitoringLevel.NONE,
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(pathway_persistent_storage),
            snapshot_access=snapshot_access,
        ),
    )
    G.clear()

    input_contents = "key,value\n1,Hello\n2,World\n3,Bonjour\n4,Monde\n"
    put_aws_object(s3_input_path, input_contents)
    table = pw.io.s3.read(
        s3_path,
        aws_s3_settings=get_aws_s3_settings(),
        schema=pw.schema_from_types(key=int, value=str),
        mode="static",
        format="csv",
        autocommit_duration_ms=1000,
        name="1",
    )
    pw.io.csv.write(table, str(tmp_path / "output_backfilled.csv"))
    pw.run(
        monitoring_level=MonitoringLevel.NONE,
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(pathway_persistent_storage),
            snapshot_access=snapshot_access,
        ),
    )
    G.clear()

    input_contents = "key,value\n1,Hello\n2,World\n3,Bonjour\n4,Monde\n5,Hola\n"
    s3_input_path_2 = f"{s3_path}/input_2.csv"
    input_contents_2 = "key,value\n6,Mundo"
    output_path = tmp_path / "output_final.csv"
    put_aws_object(s3_input_path, input_contents)
    put_aws_object(s3_input_path_2, input_contents_2)
    table = pw.io.s3.read(
        s3_path,
        aws_s3_settings=get_aws_s3_settings(),
        schema=pw.schema_from_types(key=int, value=str),
        mode="static",
        format="csv",
        autocommit_duration_ms=1000,
        name="1",
    )
    pw.io.csv.write(table, str(output_path))
    pw.run(
        monitoring_level=MonitoringLevel.NONE,
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(pathway_persistent_storage),
            snapshot_access=snapshot_access,
        ),
    )

    model_output_contents = "key,value\n5,Hola\n6,Mundo"
    model_output_path = tmp_path / "expected_output.csv"
    write_lines(model_output_path, model_output_contents)

    result = pd.read_csv(
        output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    expected = pd.read_csv(
        model_output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    assert result.equals(expected)


@pytest.mark.parametrize(
    "snapshot_access", [api.SnapshotAccess.FULL, api.SnapshotAccess.OFFSETS_ONLY]
)
def test_s3_json_read_and_recovery(
    snapshot_access, tmp_path: pathlib.Path, s3_path: str
):
    pstorage_s3_path = f"{s3_path}/PStorage"
    input_s3_path = f"{s3_path}/input"
    output_path = tmp_path / "output.json"

    def run_pw_program():
        class InputSchema(pw.Schema):
            key: int
            value: str

        G.clear()
        table = pw.io.s3.read(
            input_s3_path,
            aws_s3_settings=get_aws_s3_settings(),
            format="json",
            schema=InputSchema,
            mode="static",
            name="1",
        )
        pw.io.jsonlines.write(table, str(output_path))
        pw.run(
            monitoring_level=MonitoringLevel.NONE,
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.s3(
                    root_path=pstorage_s3_path,
                    bucket_settings=get_aws_s3_settings(),
                ),
                snapshot_access=snapshot_access,
            ),
        )

    input_contents = [
        {"key": 1, "value": "One"},
        {"key": 2, "value": "Two"},
    ]
    put_aws_object(
        f"{input_s3_path}/input_1.json",
        create_jsonlines(input_contents),
    )

    run_pw_program()
    output_contents = read_jsonlines_fields(output_path, ["key", "value"])
    output_contents.sort(key=lambda entry: entry["key"])
    assert output_contents == input_contents

    second_input_part = [
        {"key": 3, "value": "Three"},
        {"key": 4, "value": "Four"},
    ]
    input_contents += second_input_part
    put_aws_object(
        f"{input_s3_path}/input_1.json",
        create_jsonlines(input_contents),
    )

    run_pw_program()
    output_contents = read_jsonlines_fields(output_path, ["key", "value"])
    output_contents.sort(key=lambda entry: entry["key"])
    assert output_contents == second_input_part

    third_input_part = [
        {"key": 5, "value": "Five"},
        {"key": 6, "value": "Six"},
    ]
    input_contents += third_input_part
    put_aws_object(
        f"{input_s3_path}/input_2.json",
        create_jsonlines(third_input_part),
    )

    run_pw_program()
    output_contents = read_jsonlines_fields(output_path, ["key", "value"])
    output_contents.sort(key=lambda entry: entry["key"])
    assert output_contents == third_input_part


def test_s3_alternative_path(tmp_path: pathlib.Path, s3_path: str):
    input_s3_path = f"{s3_path}/input.csv"
    output_path = tmp_path / "output.csv"
    model_output_path = tmp_path / "model_output.csv"

    input_contents = "key,value\n1,Hello\n2,World"

    put_aws_object(input_s3_path, input_contents)
    write_lines(model_output_path, input_contents)

    table = pw.io.s3.read(
        f"s3://aws-integrationtest/{input_s3_path}",
        aws_s3_settings=pw.io.s3.AwsS3Settings(
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        schema=pw.schema_from_types(key=int, value=str),
        mode="static",
        format="csv",
        autocommit_duration_ms=1000,
    )

    pw.io.csv.write(table, str(output_path))
    pw.run()

    result = pd.read_csv(
        output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    expected = pd.read_csv(
        model_output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    assert result.equals(expected)


def test_s3_wrong_path(tmp_path: pathlib.Path, s3_path: str):
    input_s3_path = f"{s3_path}/input.csv"
    output_path = tmp_path / "output.csv"

    table = pw.io.s3.read(
        f"s3://aws-integrationtest/{input_s3_path}",
        aws_s3_settings=pw.io.s3.AwsS3Settings(
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        format="csv",
        schema=pw.schema_from_types(key=int, value=str),
        mode="static",
        autocommit_duration_ms=1000,
    )

    pw.io.csv.write(table, str(output_path))
    with pytest.raises(
        IOError,
        match="Failed to initialize S3 scanner: no objects to read",
    ):
        pw.run()


def test_s3_creds_from_profiles(tmp_path: pathlib.Path, s3_path: str, monkeypatch):
    monkeypatch.setenv("AWS_PROFILE", "aws-integrationtest")
    input_s3_path = f"{s3_path}/input.csv"
    output_path = tmp_path / "output.csv"
    model_output_path = tmp_path / "model_output.csv"

    input_contents = "key,value\n1,Hello\n2,World"

    put_aws_object(input_s3_path, input_contents)
    write_lines(model_output_path, input_contents)

    table = pw.io.s3.read(
        f"s3://aws-integrationtest/{input_s3_path}",
        aws_s3_settings=pw.io.s3.AwsS3Settings(region="eu-central-1"),
        schema=pw.schema_from_types(key=int, value=str),
        format="csv",
        mode="static",
        autocommit_duration_ms=1000,
    )

    pw.io.csv.write(table, str(output_path))
    pw.run()

    result = pd.read_csv(
        output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    expected = pd.read_csv(
        model_output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    assert result.equals(expected)


def test_s3_full_autodetect(tmp_path: pathlib.Path, s3_path: str):
    input_s3_path = f"{s3_path}/input.csv"
    output_path = tmp_path / "output.csv"
    model_output_path = tmp_path / "model_output.csv"

    input_contents = "key,value\n1,Hello\n2,World"

    put_aws_object(input_s3_path, input_contents)
    write_lines(model_output_path, input_contents)

    class InputSchema(pw.Schema):
        key: int
        value: str

    table = pw.io.s3.read(
        f"s3://aws-integrationtest/{input_s3_path}",
        format="csv",
        schema=InputSchema,
        mode="static",
    )

    pw.io.csv.write(table, str(output_path))
    pw.run()

    result = pd.read_csv(
        output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    expected = pd.read_csv(
        model_output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    assert result.equals(expected)


def run_csv_reader_with_path_filter(
    root_path: str, output_path: pathlib.Path, path_filter: str, expected_keys: set[int]
):
    class InputSchema(pw.Schema):
        key: int
        value: str

    G.clear()
    table = pw.io.s3.read(
        root_path,
        format="csv",
        mode="static",
        path_filter=path_filter,
        schema=InputSchema,
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    keys = set()
    with open(output_path, "r") as f:
        for row in f.readlines():
            row_parsed = json.loads(row)
            keys.add(row_parsed["key"])
    assert keys == expected_keys


def test_s3_objects_filter(tmp_path: pathlib.Path, s3_path: str):
    input_s3_path_csv = f"{s3_path}/input.csv"
    input_s3_path_json = f"{s3_path}/input.json"
    input_s3_path_dsv = f"{s3_path}/input.dsv"

    input_contents_csv = "key,value\n1,Hello\n2,World"
    input_contents_json = json.dumps({"key": 3, "value": "Bonjour"})
    input_contents_dsv = "key,value\n4,Another\n5,Test"

    output_path = tmp_path / "output.jsonl"
    put_aws_object(input_s3_path_csv, input_contents_csv)
    put_aws_object(input_s3_path_json, input_contents_json)
    put_aws_object(input_s3_path_dsv, input_contents_dsv)

    table = pw.io.s3.read(
        f"s3://aws-integrationtest/{s3_path}",
        format="plaintext_by_object",
        mode="static",
        path_filter="*.json",
        with_metadata=True,
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    n_rows = 0
    with open(output_path, "r") as f:
        for row in f.readlines():
            n_rows += 1
            row_parsed = json.loads(row)
            assert row_parsed["_metadata"]["path"].endswith(".json")
    assert n_rows == 1

    run_csv_reader_with_path_filter(
        f"s3://aws-integrationtest/{s3_path}", output_path, "*.csv", {1, 2}
    )

    run_csv_reader_with_path_filter(
        f"s3://aws-integrationtest/{s3_path}", output_path, "*.?sv", {1, 2, 4, 5}
    )


def test_s3_objects_filter_complex_path(tmp_path: pathlib.Path, s3_path: str):
    input_s3_path_csv = f"{s3_path}/one/two/three/input.csv"
    input_s3_path_json = f"{s3_path}/one/three/input.csv"
    input_s3_path_dsv = f"{s3_path}/one/two/five/input.csv"

    input_contents_csv = "key,value\n1,Hello\n2,World"
    input_contents_json = "key,value\n3,Bonjour"
    input_contents_dsv = "key,value\n4,Another\n5,Test"
    output_path = tmp_path / "output.jsonl"

    put_aws_object(input_s3_path_csv, input_contents_csv)
    put_aws_object(input_s3_path_json, input_contents_json)
    put_aws_object(input_s3_path_dsv, input_contents_dsv)

    run_csv_reader_with_path_filter(
        f"s3://aws-integrationtest/{s3_path}",
        output_path,
        "*/one/*/*/*.csv",
        {1, 2, 4, 5},
    )

    run_csv_reader_with_path_filter(
        f"s3://aws-integrationtest/{s3_path}",
        output_path,
        "*/one/*/three/*.csv",
        {1, 2},
    )

    run_csv_reader_with_path_filter(
        f"s3://aws-integrationtest/{s3_path}",
        output_path,
        "*/one/two/*/*.csv",
        {1, 2, 4, 5},
    )

    run_csv_reader_with_path_filter(
        f"s3://aws-integrationtest/{s3_path}",
        output_path,
        "*/one/*.csv",
        {1, 2, 3, 4, 5},
    )

    run_csv_reader_with_path_filter(
        f"s3://aws-integrationtest/{s3_path}",
        output_path,
        "*/five/*.csv",
        {4, 5},
    )

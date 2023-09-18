# Copyright © 2023 Pathway

import json
import os
import pathlib
import time
import uuid

import boto3
import pandas as pd

import pathway as pw
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.parse_graph import G
from pathway.tests.utils import write_lines


def put_aws_object(path, contents):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id="AKIAX67C7K343BP4QUWN",
        aws_secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
    )
    s3_client.put_object(
        Bucket="aws-integrationtest",
        Key=path,
        Body=contents,
    )


def put_minio_object(path, contents):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id="iqrK7CK8iUURqs1cB2wd",
        aws_secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
        endpoint_url="https://minio-api.deploys.pathway.com",
    )
    s3_client.put_object(
        Bucket="minio-integrationtest",
        Key=path,
        Body=contents,
    )


def create_jsonlines(input_dicts):
    return "\n".join([json.dumps(value) for value in input_dicts])


def read_jsonlines_fields(path, keys_to_extract):
    result = []
    with open(path, "r") as f:
        for row in f:
            original_json = json.loads(row)
            processed_json = {}
            for key in keys_to_extract:
                processed_json[key] = original_json[key]
            result.append(processed_json)
    return result


def test_s3_read_write(tmp_path: pathlib.Path):
    input_s3_path = "integration_tests/test_s3_read_write/input.csv"
    output_path = tmp_path / "output.csv"
    model_output_path = tmp_path / "model_output.csv"

    input_contents = "key,value\n1,Hello\n2,World"

    put_aws_object(input_s3_path, input_contents)
    write_lines(model_output_path, input_contents)

    table = pw.io.s3_csv.read(
        input_s3_path,
        aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key="AKIAX67C7K343BP4QUWN",
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        value_columns=["key", "value"],
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


def test_minio_read_write(tmp_path: pathlib.Path):
    class InputSchema(pw.Schema):
        key: int
        value: str

    input_s3_path = "integration_tests/test_minio_read_write/input.csv"
    output_path = tmp_path / "output.csv"
    model_output_path = tmp_path / "model_output.csv"

    input_contents = "key,value\n1,Hello\n2,World"

    put_minio_object(input_s3_path, input_contents)
    write_lines(model_output_path, input_contents)

    table = pw.io.minio.read(
        input_s3_path,
        minio_settings=pw.io.minio.MinIOSettings(
            bucket_name="minio-integrationtest",
            access_key="iqrK7CK8iUURqs1cB2wd",
            secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
            endpoint="minio-api.deploys.pathway.com",
        ),
        format="csv",
        schema=InputSchema,
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


def test_s3_backfilling(tmp_path: pathlib.Path):
    pathway_persistent_storage = tmp_path / "PStorage"
    s3_folder_path = "integration_tests/test_s3_backfilling/{}".format(time.time())
    s3_input_path = s3_folder_path + "/input.csv"

    input_contents = "key,value\n1,Hello\n2,World"
    put_aws_object(s3_input_path, input_contents)
    table = pw.io.s3_csv.read(
        s3_folder_path,
        aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key="AKIAX67C7K343BP4QUWN",
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        value_columns=["key", "value"],
        mode="static",
        autocommit_duration_ms=1000,
        persistent_id="1",
    )
    pw.io.csv.write(table, str(tmp_path / "output.csv"))
    pw.run(
        monitoring_level=MonitoringLevel.NONE,
        persistence_config=pw.io.PersistenceConfig.single_backend(
            pw.io.PersistentStorageBackend.filesystem(pathway_persistent_storage),
        ),
    )
    G.clear()

    input_contents = "key,value\n1,Hello\n2,World\n3,Bonjour\n4,Monde"
    put_aws_object(s3_input_path, input_contents)
    table = pw.io.s3_csv.read(
        s3_folder_path,
        aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key="AKIAX67C7K343BP4QUWN",
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        value_columns=["key", "value"],
        mode="static",
        autocommit_duration_ms=1000,
        persistent_id="1",
    )
    pw.io.csv.write(table, str(tmp_path / "output_backfilled.csv"))
    pw.run(
        monitoring_level=MonitoringLevel.NONE,
        persistence_config=pw.io.PersistenceConfig.single_backend(
            pw.io.PersistentStorageBackend.filesystem(pathway_persistent_storage),
        ),
    )
    G.clear()

    input_contents = "key,value\n1,Hello\n2,World\n3,Bonjour\n4,Monde\n5,Hola"
    s3_input_path_2 = s3_folder_path + "/input_2.csv"
    input_contents_2 = "key,value\n6,Mundo"
    output_path = tmp_path / "output_final.csv"
    put_aws_object(s3_input_path, input_contents)
    put_aws_object(s3_input_path_2, input_contents_2)
    table = pw.io.s3_csv.read(
        s3_folder_path,
        aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key="AKIAX67C7K343BP4QUWN",
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        value_columns=["key", "value"],
        mode="static",
        autocommit_duration_ms=1000,
        persistent_id="1",
    )
    pw.io.csv.write(table, str(output_path))
    pw.run(
        monitoring_level=MonitoringLevel.NONE,
        persistence_config=pw.io.PersistenceConfig.single_backend(
            pw.io.PersistentStorageBackend.filesystem(pathway_persistent_storage),
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


def test_s3_json_read_and_recovery(tmp_path: pathlib.Path):
    pathway_persistent_storage = str(tmp_path / "PStorage")
    input_s3_path = "integration_tests/test_s3_json_read_write/{}".format(time.time())
    output_path = tmp_path / "output.json"

    def run_pw_program():
        class InputSchema(pw.Schema):
            key: int
            value: str

        G.clear()
        table = pw.io.s3.read(
            input_s3_path,
            aws_s3_settings=pw.io.s3.AwsS3Settings(
                bucket_name="aws-integrationtest",
                access_key="AKIAX67C7K343BP4QUWN",
                secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
                region="eu-central-1",
            ),
            format="json",
            schema=InputSchema,
            mode="static",
            persistent_id="1",
        )
        pw.io.jsonlines.write(table, str(output_path))
        pw.run(
            monitoring_level=MonitoringLevel.NONE,
            persistence_config=pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(pathway_persistent_storage),
            ),
        )

    input_contents = [
        {"key": 1, "value": "One"},
        {"key": 2, "value": "Two"},
    ]
    put_aws_object(
        os.path.join(input_s3_path, "input_1.json"),
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
        os.path.join(input_s3_path, "input_1.json"),
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
        os.path.join(input_s3_path, "input_2.json"),
        create_jsonlines(third_input_part),
    )

    run_pw_program()
    output_contents = read_jsonlines_fields(output_path, ["key", "value"])
    output_contents.sort(key=lambda entry: entry["key"])
    assert output_contents == third_input_part


def test_s3_bytes_read(tmp_path: pathlib.Path):
    input_path = (
        f"integration_tests/test_s3_bytes_read/{time.time()}-{uuid.uuid4()}/input.txt"
    )
    input_full_contents = "abc\n\ndef\nghi\njkl"
    output_path = tmp_path / "output.json"

    put_aws_object(input_path, input_full_contents)
    table = pw.io.s3.read(
        input_path,
        aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key="AKIAX67C7K343BP4QUWN",
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        format="binary",
        mode="static",
        autocommit_duration_ms=1000,
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    with open(output_path, "r") as f:
        result = json.load(f)
        assert result["data"] == [ord(c) for c in input_full_contents]


def test_s3_empty_bytes_read(tmp_path: pathlib.Path):
    base_path = (
        f"integration_tests/test_s3_empty_bytes_read/{time.time()}-{uuid.uuid4()}/"
    )

    put_aws_object(base_path + "input", "")
    put_aws_object(base_path + "input2", "")

    table = pw.io.s3.read(
        base_path,
        aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key="AKIAX67C7K343BP4QUWN",
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        ),
        format="binary",
        mode="static",
        autocommit_duration_ms=1000,
    )

    rows = []

    def on_change(key, row, time, is_addition):
        rows.append(row)

    def on_end(*args, **kwargs):
        pass

    pw.io.subscribe(table, on_change=on_change, on_end=on_end)
    pw.run()

    assert (
        rows
        == [
            {
                "data": b"",
            }
        ]
        * 2
    )

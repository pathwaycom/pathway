# Copyright © 2024 Pathway

import os
import threading
import time

import pandas as pd
import pytest
from deltalake import DeltaTable, write_deltalake

import pathway as pw
from pathway.tests.utils import CsvLinesNumberChecker, wait_result_with_checker

from .base import MINIO_BUCKET_NAME, S3_BUCKET_NAME


def get_deltalake_connection_options(storage_type):
    options = {
        "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
    }
    if storage_type == "s3":
        options.update(
            {
                "AWS_ACCESS_KEY_ID": os.environ["AWS_S3_ACCESS_KEY"],
                "AWS_SECRET_ACCESS_KEY": os.environ["AWS_S3_SECRET_ACCESS_KEY"],
                "AWS_REGION": "eu-central-1",
                "AWS_BUCKET_NAME": S3_BUCKET_NAME,
            }
        )
    elif storage_type == "minio":
        options.update(
            {
                "AWS_ACCESS_KEY_ID": os.environ["MINIO_S3_ACCESS_KEY"],
                "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
                "AWS_BUCKET_NAME": MINIO_BUCKET_NAME,
                "AWS_ENDPOINT_URL": "https://minio-api.deploys.pathway.com",
            }
        )
    else:
        raise RuntimeError(
            f"Unknown storage type: {storage_type}."
            'Only "s3" and "minio" are supported.'
        )
    return options


def write_deltalake_with_auth(storage_type, s3_path, chunk, **kwargs):
    write_deltalake(
        s3_path,
        chunk,
        storage_options=get_deltalake_connection_options(storage_type),
        **kwargs,
    )


@pytest.mark.parametrize(
    "credentials",
    [
        pw.io.s3.AwsS3Settings(
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            bucket_name=S3_BUCKET_NAME,
            region="eu-central-1",
        ),
        pw.io.minio.MinIOSettings(
            bucket_name=MINIO_BUCKET_NAME,
            access_key=os.environ["MINIO_S3_ACCESS_KEY"],
            secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
            endpoint="minio-api.deploys.pathway.com",
        ),
        None,
    ],
)
def test_streaming_from_deltalake(credentials, tmp_path, s3_path):
    if isinstance(credentials, pw.io.minio.MinIOSettings):
        lake_path = f"s3://{MINIO_BUCKET_NAME}/{s3_path}/"
        storage_type = "minio"
    else:
        lake_path = f"s3://{S3_BUCKET_NAME}/{s3_path}/"
        storage_type = "s3"
    output_path = tmp_path / "output.csv"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    data = [{"k": 0, "v": ""}]
    df = pd.DataFrame(data).set_index("k")
    write_deltalake_with_auth(storage_type, lake_path, df, mode="append")

    def create_new_versions(start_idx, end_idx):
        for idx in range(start_idx, end_idx):
            data = [{"k": idx, "v": "a" * idx}]
            df = pd.DataFrame(data).set_index("k")
            write_deltalake_with_auth(storage_type, lake_path, df, mode="append")
            time.sleep(1.0)

    t = threading.Thread(target=create_new_versions, args=(1, 10))
    t.start()
    table = pw.io.deltalake.read(
        lake_path,
        schema=InputSchema,
        autocommit_duration_ms=10,
        s3_connection_settings=credentials,
    )
    pw.io.csv.write(table, output_path)
    wait_result_with_checker(CsvLinesNumberChecker(output_path, 10), 60)


@pytest.mark.parametrize(
    "credentials",
    [
        pw.io.s3.AwsS3Settings(
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            bucket_name=S3_BUCKET_NAME,
            region="eu-central-1",
        ),
        pw.io.minio.MinIOSettings(
            bucket_name=MINIO_BUCKET_NAME,
            access_key=os.environ["MINIO_S3_ACCESS_KEY"],
            secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
            endpoint="minio-api.deploys.pathway.com",
        ),
        None,
    ],
)
@pytest.mark.parametrize("min_commit_frequency", [None, 60_000])
def test_output(credentials, min_commit_frequency, tmp_path, s3_path):
    input_path = tmp_path / "input.csv"
    if isinstance(credentials, pw.io.minio.MinIOSettings):
        output_s3_path = f"s3://{MINIO_BUCKET_NAME}/{s3_path}/"
        storage_type = "minio"
    else:
        output_s3_path = f"s3://{S3_BUCKET_NAME}/{s3_path}/"
        storage_type = "s3"
    input_contents = "key,value\n1,Hello\n2,World"

    with open(input_path, "w") as f:
        f.write(input_contents)

    class InputSchema(pw.Schema):
        key: int
        value: str

    table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    pw.io.deltalake.write(
        table,
        output_s3_path,
        s3_connection_settings=credentials,
        min_commit_frequency=min_commit_frequency,
    )
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    delta_table = DeltaTable(
        output_s3_path, storage_options=get_deltalake_connection_options(storage_type)
    )
    pd_table_from_delta = delta_table.to_pandas()
    assert pd_table_from_delta.shape[0] == 2


@pytest.mark.parametrize(
    "credentials",
    [
        pw.io.s3.AwsS3Settings(
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            bucket_name=S3_BUCKET_NAME,
            region="eu-central-1",
        ),
        pw.io.minio.MinIOSettings(
            bucket_name=MINIO_BUCKET_NAME,
            access_key=os.environ["MINIO_S3_ACCESS_KEY"],
            secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
            endpoint="minio-api.deploys.pathway.com",
        ),
        None,
    ],
)
def test_input(credentials, tmp_path, s3_path):
    if isinstance(credentials, pw.io.minio.MinIOSettings):
        input_s3_path = f"s3://{MINIO_BUCKET_NAME}/{s3_path}/"
        storage_type = "minio"
    else:
        input_s3_path = f"s3://{S3_BUCKET_NAME}/{s3_path}/"
        storage_type = "s3"
    output_path = tmp_path / "output.csv"

    data = [{"k": 1, "v": "one"}, {"k": 2, "v": "two"}, {"k": 3, "v": "three"}]
    original = pd.DataFrame(data).set_index("k")
    write_deltalake_with_auth(storage_type, input_s3_path, original)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.deltalake.read(
        input_s3_path, InputSchema, mode="static", s3_connection_settings=credentials
    )
    pw.io.csv.write(table, output_path)
    pw.run()

    final = pd.read_csv(output_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    assert final.equals(original)

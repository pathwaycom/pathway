# Copyright © 2024 Pathway

import os

import pytest
from deltalake import DeltaTable

import pathway as pw


@pytest.mark.parametrize(
    "credentials",
    [
        pw.io.s3.AwsS3Settings(
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            bucket_name="aws-integrationtest",
            region="eu-central-1",
        ),
        None,
    ],
)
@pytest.mark.parametrize("min_commit_frequency", [None, 60_000])
def test_output(credentials, min_commit_frequency, tmp_path, s3_path):
    input_path = tmp_path / "input.csv"
    output_s3_path = f"s3://aws-integrationtest/{s3_path}/"
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

    delta_table = DeltaTable(output_s3_path)
    pd_table_from_delta = delta_table.to_pandas()
    assert pd_table_from_delta.shape[0] == 2

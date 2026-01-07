# Copyright © 2026 Pathway

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Generator

import boto3
import pytest

import pathway as pw
from pathway.internals import parse_graph
from pathway.tests.utils import SerializationTestHelper

CREDENTIALS_DIR = Path(os.getenv("CREDENTIALS_DIR", default=Path(__file__).parent))
TESTS_BUCKET = "aws-integrationtest"


@pytest.fixture(autouse=True)
def parse_graph_teardown():
    yield
    parse_graph.G.clear()


@pytest.fixture(scope="session")
def root_s3_path() -> str:
    return f"integration_tests/{time.time()}-{uuid.uuid4()}"


@pytest.fixture
def s3_path(
    request: pytest.FixtureRequest, root_s3_path: str
) -> Generator[str, None, None]:
    node_name = request.node.name
    path = f"{root_s3_path}/{node_name}/{uuid.uuid4()}"
    yield path

    # Remove the artifacts created by test under the path
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_S3_SECRET_ACCESS_KEY"),
    )
    response = s3.list_objects_v2(Bucket=TESTS_BUCKET, Prefix=path)
    if "Contents" not in response:
        return
    objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
    s3.delete_objects(Bucket=TESTS_BUCKET, Delete={"Objects": objects_to_delete})
    while response.get("IsTruncated"):
        response = s3.list_objects_v2(
            Bucket=TESTS_BUCKET,
            Prefix=path,
            ContinuationToken=response["NextContinuationToken"],
        )
        if "Contents" not in response:
            break
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3.delete_objects(Bucket=TESTS_BUCKET, Delete={"Objects": objects_to_delete})


@pytest.fixture(autouse=True)
def disable_monitoring(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PATHWAY_MONITORING_SERVER", raising=False)
    monkeypatch.delenv("PATHWAY_DETAILED_MONITORING_DIR", raising=False)
    pw.set_monitoring_config()


@pytest.fixture
def credentials_dir() -> Path:
    return CREDENTIALS_DIR


@pytest.fixture
def serialization_tester():
    return SerializationTestHelper()

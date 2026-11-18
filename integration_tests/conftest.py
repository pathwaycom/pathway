# Copyright © 2026 Pathway

from __future__ import annotations

import os
import re
import time
import uuid
from pathlib import Path
from typing import Generator

import boto3
import pytest
from click.testing import CliRunner

import pathway as pw
from pathway.internals import parse_graph
from pathway.tests.utils import (
    PortBlockRegistry,
    SerializationTestHelper,
    make_port_fixture,
)

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
    # pytest test IDs for parametrized tests contain `[`/`]` (e.g.
    # `test_snapshot_mode[1-2]`). deltalake-core 0.31's DataFusion integration
    # builds a synthetic `delta-rs://<scheme>-<host>-<path>` URL where `/` in
    # the path is replaced by `-`, so any path char that isn't valid in a URL
    # *host* panics with `InvalidDomainCharacter`. Brackets specifically
    # trigger it on the 2nd+ pass over a snapshot-mode table. Strip them (and
    # anything else that could end up in the synthetic URL's host segment) so
    # we don't hit the upstream bug.
    #
    # Critical for runs with --count.
    node_name = re.sub(r"[^A-Za-z0-9._-]", "_", request.node.name)
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


# Ports come from the registry rather than from `bind(("", 0))`: a port the
# kernel picks that way is released again before the test uses it, and it comes
# from the ephemeral range, where an outgoing connection can take it meanwhile.
tcp_port = make_port_fixture()
_TWO_PORT_REGISTRY = PortBlockRegistry(block_size=2)


@pytest.fixture
def two_free_ports() -> Generator[list[int], None, None]:
    first_port = _TWO_PORT_REGISTRY.acquire()
    try:
        yield [first_port, first_port + 1]
    finally:
        _TWO_PORT_REGISTRY.release(first_port)


@pytest.fixture
def runner():
    return CliRunner()

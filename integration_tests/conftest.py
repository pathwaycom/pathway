# Copyright © 2024 Pathway

from __future__ import annotations

import time
import uuid

import pytest

from pathway.internals import parse_graph


@pytest.fixture(autouse=True)
def parse_graph_teardown():
    yield
    parse_graph.G.clear()


@pytest.fixture(scope="session")
def root_s3_path() -> str:
    return f"integration_tests/{time.time()}-{uuid.uuid4()}"


@pytest.fixture
def s3_path(request: pytest.FixtureRequest, root_s3_path: str) -> str:
    node_name = request.node.name
    return f"{root_s3_path}/{node_name}/{uuid.uuid4()}"


@pytest.fixture(autouse=True)
def disable_monitoring(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PATHWAY_MONITORING_SERVER", raising=False)

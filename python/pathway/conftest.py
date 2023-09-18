# Copyright © 2023 Pathway

from __future__ import annotations

import pytest

from pathway.internals import parse_graph


@pytest.fixture(autouse=True)
def parse_graph_teardown():
    yield
    parse_graph.G.clear()


@pytest.fixture(autouse=True)
def environment_variables(monkeypatch):
    monkeypatch.setenv("KAFKA_USERNAME", "pathway")
    monkeypatch.setenv("KAFKA_PASSWORD", "Pallas'sCat")
    monkeypatch.setenv("BEARER_TOKEN", "42")
    monkeypatch.setenv("MINIO_S3_ACCESS_KEY", "Otocolobus")
    monkeypatch.setenv("MINIO_S3_SECRET_ACCESS_KEY", "manul")
    monkeypatch.setenv("S3_ACCESS_KEY", "Otocolobus")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "manul")
    monkeypatch.setenv("DO_S3_ACCESS_KEY", "Otocolobus")
    monkeypatch.setenv("DO_S3_SECRET_ACCESS_KEY", "manul")
    monkeypatch.setenv("WASABI_S3_ACCESS_KEY", "Otocolobus")
    monkeypatch.setenv("WASABI_S3_SECRET_ACCESS_KEY", "manul")
    monkeypatch.setenv("OVH_S3_ACCESS_KEY", "Otocolobus")
    monkeypatch.setenv("OVH_S3_SECRET_ACCESS_KEY", "manul")

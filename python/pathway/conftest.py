# Copyright © 2024 Pathway

from __future__ import annotations

import os
from collections.abc import Generator
from warnings import warn

import pytest

from pathway.internals import parse_graph


@pytest.fixture(autouse=True)
def parse_graph_teardown() -> Generator[None, None, None]:
    yield
    parse_graph.G.clear()


@pytest.fixture(autouse=True)
def environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
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


environment_stash_key = pytest.StashKey[dict[str, str]]()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    item.stash[environment_stash_key] = os.environ.copy()


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item: pytest.Item) -> None:
    saved_env = item.stash[environment_stash_key]
    new_env = os.environ.copy()

    if new_env != saved_env:
        os.environ.update(saved_env)
        for key in new_env.keys() - saved_env.keys():
            del os.environ[key]

    # XXX
    new_persistent_storage = new_env.pop("PATHWAY_PERSISTENT_STORAGE", None)
    saved_persistent_storage = saved_env.pop("PATHWAY_PERSISTENT_STORAGE", None)
    if new_persistent_storage != saved_persistent_storage:
        warn(
            "The test changed PATHWAY_PERSISTENT_STORAGE in the environment.\n"
            "The tests should NOT change the environment, please fix this!\n"
            "Restoring the old value for now."
        )

    assert saved_env == new_env, "environment changed during the test run"

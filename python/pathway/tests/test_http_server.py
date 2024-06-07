# Copyright © 2024 Pathway

from __future__ import annotations

import os
import time
import urllib

import pytest

import pathway as pw
from pathway.internals import graph_runner
from pathway.internals.parse_graph import G
from pathway.tests.utils import T


@pytest.mark.xdist_group(name="http_server_tests")
def http_server_status(_, max_retries: int = 1) -> int:
    port = os.environ.get("PATHWAY_MONITORING_HTTP_PORT", "20000")

    for n_attempt in range(max_retries):
        time.sleep(2**n_attempt * 0.1)
        try:
            with urllib.request.urlopen(f"http://localhost:{port}/status") as response:
                return response.status
        except urllib.error.URLError:
            continue
    return -1


@pytest.mark.xdist_group(name="http_server_tests")
def test_http_server_runs_when_enabled():
    table = T(
        """
            | foo
        1   | 42
        """
    )

    response_code = table.select(
        response_code=pw.apply_async(http_server_status, table.foo, max_retries=4)
    )

    updates_stream = graph_runner.GraphRunner(
        G, with_http_server=True, monitoring_level=pw.MonitoringLevel.NONE
    ).run_tables(response_code)[0]
    assert updates_stream[0].values[0] == 200


@pytest.mark.xfail(reason="fails randomly")
def test_http_server_doesnt_run_when_disabled():
    table = T(
        """
            | foo
        1   | 42
        """
    )

    response_code = table.select(
        response_code=pw.apply_async(http_server_status, table.foo)
    )

    updates_stream = graph_runner.GraphRunner(
        G, with_http_server=False, monitoring_level=pw.MonitoringLevel.NONE
    ).run_tables(response_code)[0]
    assert updates_stream[0].values[0] == -1

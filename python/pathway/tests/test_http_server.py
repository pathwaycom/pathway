# Copyright © 2026 Pathway

from __future__ import annotations

import http.client
import os
import time
import urllib

import pytest

import pathway as pw
from pathway.internals import graph_runner
from pathway.internals.parse_graph import G
from pathway.tests.utils import T, make_port_fixture

# A port derived from the PR number can overlap a Kubernetes NodePort. Reserve
# a separate port for each test, outside the default NodePort range.
port = make_port_fixture(block_size=16, range_start=40000, range_end=50000)


@pytest.fixture(autouse=True)
def configure_monitoring_port(monkeypatch: pytest.MonkeyPatch, port: int) -> None:
    monkeypatch.setenv("PATHWAY_MONITORING_HTTP_PORT", str(port))


def http_server_status(_, max_retries: int = 1) -> int:
    port = os.environ.get("PATHWAY_MONITORING_HTTP_PORT", "20000")

    for n_attempt in range(max_retries):
        time.sleep(2**n_attempt * 0.1)
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/status", timeout=2
            ) as response:
                return response.status
        except urllib.error.HTTPError:
            raise
        except urllib.error.URLError:
            continue
    return -1


def test_http_server_runs_when_enabled():
    table = T(
        """
            | foo
        1   | 42
        """
    )

    response_code = table.select(
        response_code=pw.apply_async(http_server_status, table.foo, 4)
    )

    updates_stream = graph_runner.GraphRunner(
        G, with_http_server=True, monitoring_level=pw.MonitoringLevel.NONE
    ).run_tables(response_code)[0]
    assert updates_stream[0].values[0] == 200


def test_http_server_routes_and_keepalive_shutdown(port: int):
    connections = []

    def check_routes(_: int) -> int:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connections.append(connection)
        deadline = time.monotonic() + 10
        while True:
            try:
                connection.connect()
                break
            except ConnectionRefusedError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

        for route, status, content_type in [
            ("/status", 200, "application/json"),
            (
                "/metrics",
                200,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            ),
            ("/missing", 404, None),
        ]:
            connection.request("GET", route)
            response = connection.getresponse()
            assert response.status == status
            assert response.getheader("Content-Type") == content_type
            response.read()

        # Leave the connection idle: stopping the graph must close keepalive
        # connections and release the listening port for the next graph.
        assert connection.sock is not None
        return 1

    table = T("foo\n42")
    response_code = table.select(response_code=pw.apply(check_routes, table.foo))
    try:
        for _ in range(2):
            updates_stream = graph_runner.GraphRunner(
                G, with_http_server=True, monitoring_level=pw.MonitoringLevel.NONE
            ).run_tables(response_code)[0]
            assert updates_stream[0].values[0] == 1
            connection = connections[-1]
            assert connection.sock.recv(1) == b""
    finally:
        for connection in connections:
            connection.close()


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

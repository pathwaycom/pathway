import os
import pathlib
import threading
import time

import pytest
import requests

import pathway as pw
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    wait_result_with_checker,
    xfail_on_darwin,
)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
def test_server(tmp_path: pathlib.Path):
    port = int(os.environ.get("PATHWAY_MONITORING_HTTP_PORT", "20000")) + 10000
    output_path = tmp_path / "output.csv"

    class InputSchema(pw.Schema):
        query: str
        user: str

    def logic(queries: pw.Table) -> pw.Table:
        return queries.select(
            query_id=queries.id, result=pw.apply(lambda x: x.upper(), pw.this.query)
        )

    def target():
        time.sleep(5)
        requests.post(
            f"http://127.0.0.1:{port}",
            json={"query": "one", "user": "sergey"},
        ).raise_for_status()
        requests.post(
            f"http://127.0.0.1:{port}",
            json={"query": "two", "user": "sergey"},
        ).raise_for_status()

    queries, response_writer = pw.io.http.rest_connector(
        host="127.0.0.1", port=port, schema=InputSchema
    )
    responses = logic(queries)
    response_writer(responses)
    pw.io.csv.write(queries, output_path)

    t = threading.Thread(target=target, daemon=True)
    t.start()
    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 4), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
def test_server_customization(tmp_path: pathlib.Path):
    port = int(os.environ.get("PATHWAY_MONITORING_HTTP_PORT", "20000")) + 10001
    output_path = tmp_path / "output.csv"

    class InputSchema(pw.Schema):
        query: str
        user: str

    def logic(queries: pw.Table) -> pw.Table:
        return queries.select(
            query_id=queries.id, result=pw.apply(lambda x: x.upper(), pw.this.query)
        )

    def target():
        time.sleep(5)
        requests.post(
            f"http://127.0.0.1:{port}/endpoint?user=sergey",
            json={"query": "one"},
        ).raise_for_status()
        requests.post(
            f"http://127.0.0.1:{port}/endpoint?user=sergey",
            json={"query": "two"},
        ).raise_for_status()

    queries, response_writer = pw.io.http.rest_connector(
        host="127.0.0.1", port=port, schema=InputSchema, route="/endpoint"
    )
    responses = logic(queries)
    response_writer(responses)
    pw.io.csv.write(queries, output_path)

    t = threading.Thread(target=target, daemon=True)
    t.start()
    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 4), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
def test_server_schema_customization(tmp_path: pathlib.Path):
    port = int(os.environ.get("PATHWAY_MONITORING_HTTP_PORT", "20000")) + 10002
    output_path = tmp_path / "output.csv"

    class InputSchema(pw.Schema):
        query: str
        user: str = pw.column_definition(default_value="manul")

    def logic(queries: pw.Table) -> pw.Table:
        return queries.select(
            query_id=queries.id, result=pw.apply(lambda x: x.upper(), pw.this.query)
        )

    def target():
        time.sleep(5)
        requests.post(
            f"http://127.0.0.1:{port}/",
            json={"query": "one"},
        ).raise_for_status()
        requests.post(
            f"http://127.0.0.1:{port}/",
            json={"query": "two"},
        ).raise_for_status()

    queries, response_writer = pw.io.http.rest_connector(
        host="127.0.0.1", port=port, schema=InputSchema
    )
    responses = logic(queries)
    response_writer(responses)
    pw.io.csv.write(queries, output_path)

    t = threading.Thread(target=target, daemon=True)
    t.start()
    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 4), 30)

# Copyright © 2026 Pathway

from __future__ import annotations

import base64
import json
import multiprocessing
import socket
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

import pathway as pw
from pathway.engine import EngineError


@pytest.fixture(autouse=True)
def _single_writer(monkeypatch):
    # These assertions inspect retries of one writer, not worker sharding.
    monkeypatch.setenv("PATHWAY_THREADS", "1")


def _serve_put_records_proxy(endpoint_url, fault, batches, ready, stop):

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            action = self.headers["X-Amz-Target"].split(".")[-1]
            payload = json.loads(body)
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower() not in ("host", "content-length", "connection")
            }
            if action == "PutRecords":
                records = payload["Records"]
                batches.append(records)
                inject_fault = fault.startswith("permanent") or len(batches) == 1
                if inject_fault and "network" in fault:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                if inject_fault and records:
                    # Keep the last record pending; actually store the others.
                    accepted = records[:-1]
                    if accepted:
                        accepted_payload = dict(payload, Records=accepted)
                        response = requests.post(
                            endpoint_url,
                            json=accepted_payload,
                            headers=headers,
                            timeout=10,
                        )
                        response.raise_for_status()
                        result = response.json()["Records"]
                    else:
                        result = []
                    result.append(
                        {
                            "ErrorCode": "ProvisionedThroughputExceededException",
                            "ErrorMessage": "The record exceeded the shard write rate",
                        }
                    )
                    self._respond(
                        200,
                        json.dumps(
                            {"FailedRecordCount": 1, "Records": result}
                        ).encode(),
                    )
                    return
            response = requests.post(
                endpoint_url, data=body, headers=headers, timeout=10
            )
            self._respond(response.status_code, response.content)

        def _respond(self, status, body):
            self.send_response(status)
            self.send_header("Content-Type", "application/x-amz-json-1.1")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.timeout = 0.2
    ready.put(server.server_port)
    try:
        while not stop.is_set():
            server.handle_request()
    finally:
        server.server_close()


@contextmanager
def _put_records_proxy(endpoint_url, fault):
    """Run the proxy outside the GIL held while the engine constructs the writer."""
    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager:
        requests_received = manager.list()
        ready = context.Queue()
        stop = context.Event()
        process = context.Process(
            target=_serve_put_records_proxy,
            args=(endpoint_url, fault, requests_received, ready, stop),
        )
        process.start()
        batches: list[list[dict[str, str]]] = []
        try:
            port = ready.get(timeout=30)
            yield f"http://127.0.0.1:{port}", batches
        finally:
            stop.set()
            process.join(timeout=10)
            if process.is_alive():
                process.terminate()
                process.join()
                raise RuntimeError("Kinesis proxy did not stop")
            batches.extend(requests_received)
            ready.close()
            assert process.exitcode == 0


def _write_rows(stream_name, count):
    table = pw.debug.table_from_markdown(
        " | sequence\n" + "\n".join(f"{index + 1} | {index}" for index in range(count))
    )
    pw.io.kinesis.write(table, stream_name, format="json")
    pw.run()


def _stored_sequences(context):
    shards = context.list_shards_and_statuses()
    return sorted(
        json.loads(record.value)["sequence"]
        for shard in shards
        for record in context.read_shard_records(shard.shard_id)
    )


@pytest.mark.parametrize("count", [2, 501])
def test_partial_acceptance_retries_only_pending_records(
    kinesis_context, monkeypatch, count
):
    with _put_records_proxy(kinesis_context.endpoint_url, "partial") as (url, batches):
        monkeypatch.setenv("AWS_ENDPOINT_URL", url)
        _write_rows(kinesis_context.stream_name, count)

    assert _stored_sequences(kinesis_context) == list(range(count))
    assert len(batches[1]) == 1
    assert batches[1] == batches[0][-1:]


def test_transport_recovery_preserves_pending_records(kinesis_context, monkeypatch):
    with _put_records_proxy(kinesis_context.endpoint_url, "network") as (url, batches):
        monkeypatch.setenv("AWS_ENDPOINT_URL", url)
        # Exercise the connector's retry, rather than recovery inside the AWS SDK.
        monkeypatch.setenv("AWS_MAX_ATTEMPTS", "1")
        _write_rows(kinesis_context.stream_name, 2)

    assert _stored_sequences(kinesis_context) == [0, 1]
    assert batches == [batches[0], batches[0]]


@pytest.mark.parametrize(
    "fault, pending_count", [("permanent_network", 2), ("permanent_partial", 1)]
)
def test_undelivered_records_report_the_pending_count(
    kinesis_context, monkeypatch, fault, pending_count
):
    with _put_records_proxy(kinesis_context.endpoint_url, fault) as (url, batches):
        monkeypatch.setenv("AWS_ENDPOINT_URL", url)
        monkeypatch.setenv("AWS_MAX_ATTEMPTS", "1")
        with pytest.raises(
            EngineError, match=f"{pending_count} items haven't been saved"
        ):
            _write_rows(kinesis_context.stream_name, 2)

    assert len(batches) == 5
    assert all(batches)
    assert _stored_sequences(kinesis_context) == sorted(
        json.loads(base64.b64decode(record["Data"]))["sequence"]
        for record in (batches[0][:-1] if fault == "permanent_partial" else [])
    )


def test_restart_delivers_rows_after_failed_output(
    kinesis_context, monkeypatch, tmp_path
):
    from pathway.internals.parse_graph import G

    class InputSchema(pw.Schema):
        sequence: int

    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"sequence": 0}\n{"sequence": 1}\n')
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(tmp_path / "state")
    )

    def run():
        table = pw.io.jsonlines.read(
            input_path, schema=InputSchema, mode="static", name="input"
        )
        pw.io.kinesis.write(table, kinesis_context.stream_name, name="output")
        pw.run(persistence_config=persistence_config)

    with _put_records_proxy(kinesis_context.endpoint_url, "permanent_network") as (
        url,
        _batches,
    ):
        monkeypatch.setenv("AWS_ENDPOINT_URL", url)
        monkeypatch.setenv("AWS_MAX_ATTEMPTS", "1")
        with pytest.raises(EngineError, match="2 items haven't been saved"):
            run()

    assert _stored_sequences(kinesis_context) == []
    G.clear()
    monkeypatch.setenv("AWS_ENDPOINT_URL", kinesis_context.endpoint_url)
    run()
    assert _stored_sequences(kinesis_context) == [0, 1]

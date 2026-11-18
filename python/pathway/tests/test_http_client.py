# Copyright © 2026 Pathway

import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
import requests

import pathway as pw
from pathway.io.http import RetryPolicy, _common
from pathway.io.http._common import Sender
from pathway.io.http._streaming import HttpStreamingSubject
from pathway.tests.utils import assert_table_equality_wo_index


@pytest.fixture
def http_server():
    class Server(ThreadingHTTPServer):
        daemon_threads = True

        def __init__(self):
            self.connections = 0
            self.received = []
            self.routes = {}
            self.lock = threading.Lock()
            super().__init__(("127.0.0.1", 0), Handler)

        def get_request(self):
            connection = super().get_request()
            self.connections += 1
            return connection

        @property
        def url(self):
            return f"http://127.0.0.1:{self.server_port}"

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            with self.server.lock:
                self.server.received.append((self.path, dict(self.headers), body))
                responses = self.server.routes.get(self.path, [])
                status, headers, body = (
                    responses.pop(0) if responses else (200, {}, b"")
                )
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_POST = do_GET

        def log_message(self, *args):
            pass

    server = Server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture
def senders():
    created = []

    def create(n_retries=2, retry_policy=None):
        sender = Sender(
            request_method="GET",
            n_retries=n_retries,
            retry_policy=retry_policy or RetryPolicy(1000, 2, 0),
            connect_timeout_ms=5000,
            request_timeout_ms=5000,
            allow_redirects=True,
            retry_codes=(503,),
        )
        created.append(sender)
        return sender

    yield create
    for sender in created:
        sender.close()


@pytest.fixture
def retry_delays(monkeypatch):
    delays = []
    monkeypatch.setattr(_common, "time", SimpleNamespace(sleep=delays.append))
    return delays


@pytest.fixture
def sessions(monkeypatch):
    created = []

    class Session(requests.Session):
        def __init__(self):
            super().__init__()
            self.closed = False
            self.threads = set()
            created.append(self)

        def request(self, *args, **kwargs):
            self.threads.add(threading.get_ident())
            return super().request(*args, **kwargs)

        def close(self):
            self.closed = True
            super().close()

    monkeypatch.setattr(requests, "Session", Session)
    monkeypatch.setattr(requests.sessions, "Session", Session)
    return created


def test_retry_backoff_starts_over_for_each_request(http_server, senders, retry_delays):
    policy = RetryPolicy(1000, 2, 0)
    first, second = senders(retry_policy=policy), senders(retry_policy=policy)
    http_server.routes["/"] = [(503, {}, b""), (503, {}, b""), (200, {}, b"ok")] * 3
    for sender in (first, first, second):
        with sender.send(http_server.url) as response:
            assert response.content == b"ok"
    assert retry_delays == [1, 2, 1, 2, 1, 2]
    assert policy.wait_duration_before_retry() == 1


@pytest.mark.parametrize("n_retries", [0, 2])
@pytest.mark.parametrize("status", [401, 503])
@pytest.mark.parametrize("stream", [False, True])
def test_no_delay_after_final_attempt(
    http_server, senders, retry_delays, sessions, n_retries, status, stream
):
    http_server.routes["/"] = [(status, {}, b"error")] * (n_retries + 1)
    with pytest.raises(requests.exceptions.HTTPError, match=str(status)) as exc_info:
        senders(n_retries=n_retries).send(http_server.url, stream=stream)
    assert exc_info.value.response.status_code == status
    assert exc_info.value.response.url == http_server.url + "/"
    assert exc_info.value.response.raw.closed
    assert all(session.closed for session in sessions)
    expected_retries = n_retries if status == 503 else 0
    assert len(http_server.received) == expected_retries + 1
    assert retry_delays == [1, 2][:expected_retries]


@pytest.mark.parametrize("recover", [False, True])
def test_connect_timeout_retries(
    http_server, senders, retry_delays, monkeypatch, sessions, recover
):
    request = requests.Session.request
    attempts = 0

    def timeout_then_request(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts <= 2 or not recover:
            raise requests.exceptions.ConnectTimeout()
        return request(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "request", timeout_then_request)
    sender = senders()
    if recover:
        with sender.send(http_server.url) as response:
            assert response.status_code == 200
    else:
        with pytest.raises(requests.exceptions.ConnectTimeout):
            sender.send(http_server.url)
        assert all(session.closed for session in sessions)
    assert attempts == 3
    assert retry_delays == [1, 2]


def test_connection_pool_reuses_tcp_connection(http_server, senders, sessions):
    sender = senders()
    for _ in range(10):
        with sender.send(http_server.url) as response:
            assert response.status_code == 200
    assert http_server.connections == 1
    sender.close()
    assert len(sessions) == 1
    assert sessions[0].closed
    # A graph can be run again after its previous subscription has finished.
    sender.send(http_server.url).close()
    assert http_server.connections == 2


def test_session_cookies_are_isolated_between_attempts(
    http_server, senders, retry_delays
):
    http_server.routes["/redirect"] = [
        (302, {"Location": "/", "Set-Cookie": "token=redirect; Path=/"}, b"")
    ]
    http_server.routes["/retry"] = [
        (503, {"Set-Cookie": "token=retry; Path=/"}, b""),
        (200, {}, b""),
    ]
    sender = senders()
    for path in ("/redirect", "/", "/retry"):
        sender.send(http_server.url + path).close()
    sender.send(http_server.url, headers={"Cookie": "token=explicit"}).close()
    cookies = [headers.get("Cookie") for _, headers, _ in http_server.received]
    assert cookies == [None, "token=redirect", None, None, None, "token=explicit"]


def test_retried_streaming_response_is_closed(
    http_server, senders, retry_delays, monkeypatch
):
    http_server.routes["/"] = [(503, {}, b""), (200, {}, b"data\n")]
    closed = []
    close = requests.Response.close

    def record_close(self):
        closed.append(self.status_code)
        close(self)

    monkeypatch.setattr(requests.Response, "close", record_close)
    with senders().send(http_server.url, stream=True) as response:
        assert closed == [503]
        assert list(response.iter_lines()) == [b"data"]
    assert closed == [503, 200]


def test_sessions_are_owned_by_individual_threads(http_server, senders, sessions):
    sender = senders()
    barrier = threading.Barrier(2)

    def send():
        barrier.wait(timeout=5)
        for _ in range(3):
            sender.send(http_server.url).close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(lambda _: send(), range(2)))
    assert http_server.connections == 2
    assert len(sessions) == 2
    assert all(len(session.threads) == 1 for session in sessions)
    assert sessions[0].threads != sessions[1].threads
    sender.close()
    assert all(session.closed for session in sessions)


@pytest.mark.parametrize("workers", [1, 2])
def test_http_write_reuses_and_closes_subscription_session(
    http_server, sessions, monkeypatch, workers
):
    monkeypatch.setenv("PATHWAY_THREADS", str(workers))

    class Schema(pw.Schema):
        value: int

    table = pw.debug.table_from_rows(Schema, [(i,) for i in range(32)])
    pw.io.http.write(
        table,
        http_server.url,
        method="POST",
        format="custom",
        request_payload_template="{table.value}",
    )
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    assert sorted(int(body) for _, _, body in http_server.received) == list(range(32))
    # Subscriptions consolidate output on one worker, even in a parallel graph.
    assert http_server.connections == 1
    assert len(sessions) == 1
    assert all(session.closed and len(session.threads) == 1 for session in sessions)


def test_http_read_closes_session(http_server, sessions):
    http_server.routes["/"] = [(200, {}, b"one\ntwo\n")]
    table = pw.io.http.read(http_server.url, format="raw")

    class Schema(pw.Schema):
        data: str

    expected = pw.debug.table_from_rows(Schema, [("one",), ("two",)])
    assert_table_equality_wo_index(table, expected)
    assert len(sessions) == 1
    assert sessions[0].closed


@pytest.mark.parametrize("direction", ["read", "write"])
@pytest.mark.parametrize("status", [401, 503])
@pytest.mark.parametrize("n_retries", [0, 2])
def test_http_status_errors_reach_pipeline(
    http_server, sessions, retry_delays, direction, status, n_retries
):
    # A valid data payload must not hide an unsuccessful HTTP status.
    http_server.routes["/"] = [(status, {}, b'{"value": 42}\n')] * (n_retries + 1)

    class Schema(pw.Schema):
        value: int

    mapped_lines = []

    def response_mapper(line):
        mapped_lines.append(line)
        return line

    kwargs = dict(n_retries=n_retries, retry_policy=RetryPolicy(1000, 2, 0))
    if direction == "read":
        table = pw.io.http.read(
            http_server.url, schema=Schema, response_mapper=response_mapper, **kwargs
        )
        pw.io.null.write(table)
    else:
        table = pw.debug.table_from_rows(Schema, [(42,)])
        pw.io.http.write(table, http_server.url, **kwargs)

    with pytest.raises(requests.exceptions.HTTPError, match=str(status)):
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    expected_retries = n_retries if status == 503 else 0
    assert len(http_server.received) == expected_retries + 1
    assert retry_delays == [1, 2][:expected_retries]
    assert mapped_lines == []
    assert len(sessions) == 1
    assert sessions[0].closed


def test_streaming_mapper_failure_closes_response_and_session(
    http_server, senders, sessions, monkeypatch
):
    http_server.routes["/"] = [(200, {}, b"one\ntwo\n")]
    closed = []
    close = requests.Response.close

    def record_close(self):
        closed.append(self)
        close(self)

    def fail(line):
        raise ValueError("mapper failed")

    monkeypatch.setattr(requests.Response, "close", record_close)
    subject = HttpStreamingSubject(
        url=http_server.url,
        sender=senders(),
        headers=None,
        payload=None,
        delimiter=None,
        response_mapper=fail,
    )
    with pytest.raises(ValueError, match="mapper failed"):
        subject.run()
    assert len(closed) == 1
    assert closed[0].raw.closed
    assert sessions[0].closed

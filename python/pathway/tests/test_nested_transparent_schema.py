# Copyright © 2026 Pathway

import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import pathway as pw
from pathway.tests.utils import (
    FileLinesNumberChecker,
    assert_table_equality_wo_index,
    needs_multiprocessing_fork,
    wait_result_with_checker,
)


class Leaf(pw.Schema):
    price: float
    data: bytes
    currency: str = pw.column_definition(default_value="USD")


class Input(pw.Schema):
    payload: Leaf


def test_nested_python_defaults_nulls_errors_and_deletions():
    schema = pw.schema_builder(
        {
            "number": pw.column_definition(dtype=int, primary_key=True),
            "payload": pw.column_definition(
                dtype=Leaf | None, default_value={"price": 1, "data": b"ok"}
            ),
        }
    )

    class Subject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next(number=1)
            self.next(number=2, payload=None)
            self.next(number=3, payload={"price": "bad", "data": "AQI="})
            row = {"number": 4, "payload": {"price": 2, "data": "AQI="}}
            self.next(**row)
            self.commit()
            self.delete(**row)

    table = pw.io.python.read(Subject(), schema=schema).remove_errors()
    result = table.select(
        number=table.number,
        price=table.payload.price,
        data=table.payload.data,
        currency=table.payload.currency,
    )
    expected = pw.schema_from_types(
        number=int, price=float | None, data=bytes | None, currency=str | None
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected, [(1, 1.0, b"ok", "USD"), (2, None, None, None)]
        ),
        terminate_on_error=False,
    )


def test_nested_http_json():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            content = b'{"payload":{"price":12.5,"data":"AQI="}}\n'
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        table = pw.io.http.read(
            f"http://127.0.0.1:{server.server_port}/", schema=Input, format="json"
        )
        result = table.select(
            price=table.payload.price,
            data=table.payload.data,
            currency=table.payload.currency,
        )
        assert_table_equality_wo_index(
            result,
            pw.debug.table_from_rows(
                pw.schema_from_types(price=float, data=bytes, currency=str),
                [(12.5, b"\x01\x02", "USD")],
            ),
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@needs_multiprocessing_fork
def test_nested_sqlite_json(tmp_path):
    database = tmp_path / "nested.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE orders (payload TEXT)")
        connection.executemany(
            "INSERT INTO orders VALUES (?)",
            [
                (json.dumps({"price": 12.5, "data": "AQI="}),),
                (json.dumps({"price": "bad", "data": "AQI="}),),
            ],
        )
    table = pw.io.sqlite.read(
        database, "orders", Input, autocommit_duration_ms=10
    ).remove_errors()
    output = tmp_path / "result.jsonl"
    pw.io.jsonlines.write(
        table.select(
            price=table.payload.price,
            data=table.payload.data,
            currency=table.payload.currency,
        ),
        output,
    )
    wait_result_with_checker(
        FileLinesNumberChecker(output, 1), 30, kwargs={"terminate_on_error": False}
    )
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert [(row["price"], row["data"], row["currency"]) for row in rows] == [
        (12.5, "AQI=", "USD")
    ]


def test_nested_python_invalid_default_rejected_before_start():
    class Bad(pw.Schema):
        price: float = pw.column_definition(default_value="bad")

    class Subject(pw.io.python.ConnectorSubject):
        def run(self):
            pytest.fail("must fail before starting the source")

    with pytest.raises(ValueError, match="/payload/price"):
        pw.io.python.read(Subject(), schema=pw.schema_from_types(payload=Bad))

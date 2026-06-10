# Copyright © 2026 Pathway

"""Round-trip parsing coverage for the Elasticsearch input connector.

For every Pathway type that can round-trip through Elasticsearch, this writes the
value with ``pw.io.elasticsearch.write``, reads it back with
``pw.io.elasticsearch.read`` in both ``static`` and ``streaming`` modes, mirrors
the result into a second index, and asserts the value survived the trip.

The check compares the two indices document-by-document. Index ``A`` is produced
by ``write(input)`` and index ``B`` by ``write(read(A))``; because both go
through the same serializer, a faithful reader yields ``A == B`` regardless of
how each type is encoded in JSON (base64 for ``bytes``, ISO strings for
datetimes, ``{"shape", "elements"}`` for ndarrays, …). A type that the reader
misreads re-serializes differently and breaks the equality. ``static`` mode
additionally type-checks the parsed values directly via a Python observer.
"""

import time
from typing import Any

import numpy as np
import pandas as pd
from utils import (
    ELASTICSEARCH_URL,
    SimpleObject,
    _make_type_check_observer,
    elasticsearch_now_ms as _now_ms,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import run, wait_result_with_checker

# The reader requires a numeric timestamp column (it watermarks and orders by it)
# and a sortable id column; both are mapped explicitly. ``dynamic=False`` keeps
# the ``item`` payload in ``_source`` without indexing it, so a single index can
# hold ``item`` values of any JSON shape without a mapping conflict.
_INDEX_PROPERTIES = {"pkey": {"type": "long"}, "ts": {"type": "long"}}

# A wide window so streaming-mode rows stay inside the overlap re-read until they
# are delivered, independent of how slow the test host is.
_MAX_TRANSACTION_DURATION = pd.Timedelta(seconds=60).to_pytimedelta()

_AUTH = pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin")


def _create_table(item_type: Any, input_rows: list[dict]) -> pw.Table:
    class InputSchema(pw.Schema):
        pkey: int
        ts: int
        item: item_type

    return pw.debug.table_from_rows(
        InputSchema,
        [tuple(row.values()) for row in input_rows],
    )


def _create_ndarray_table(item_type: Any, input_rows: list[dict]) -> pw.Table:
    class InputSchema(pw.Schema):
        pkey: int
        ts: int
        item: Any

    return pw.debug.table_from_rows(
        InputSchema,
        [tuple(row.values()) for row in input_rows],
    ).update_types(item=item_type)


def _read_schema(item_type: Any) -> type[pw.Schema]:
    # No primary key: the connector keys the table by ``id_column`` (``pkey``).
    return pw.schema_builder(
        columns={
            "pkey": pw.column_definition(dtype=int),
            "ts": pw.column_definition(dtype=int),
            "item": pw.column_definition(dtype=item_type),
        }
    )


def _strip_meta(source: dict) -> dict:
    # ``time``/``diff`` are appended by the writer and differ run-to-run.
    return {k: v for k, v in source.items() if k not in ("time", "diff")}


def _assert_indices_match(
    es, index_a: str, index_b: str, expected_count: int, *, timeout: float = 30.0
) -> None:
    # Both indices are expected to settle at exactly ``expected_count`` documents
    # (A is written-and-refreshed, B is confirmed by the caller). Under heavy
    # parallel load a ``_refresh`` + ``_search`` issued right after a burst of
    # writes can momentarily report fewer docs, so poll until both reach the
    # expected count before comparing contents rather than reading once.
    deadline = time.monotonic() + timeout
    while True:
        rows_a = es.get_all_sources(index_a)
        rows_b = es.get_all_sources(index_b)
        if len(rows_a) == expected_count and len(rows_b) == expected_count:
            break
        if time.monotonic() >= deadline:
            assert (
                len(rows_a) == expected_count
            ), f"index A: {len(rows_a)} != {expected_count}"
            assert (
                len(rows_b) == expected_count
            ), f"index B: {len(rows_b)} != {expected_count}"
        time.sleep(0.5)
    by_key_a = {src["pkey"]: _strip_meta(src) for src in rows_a}
    by_key_b = {src["pkey"]: _strip_meta(src) for src in rows_b}
    assert by_key_a == by_key_b, f"\nA: {by_key_a}\nB: {by_key_b}"


class _IndexCountChecker:
    def __init__(self, es, index_name: str, expected_count: int):
        self.es = es
        self.index_name = index_name
        self.expected_count = expected_count

    def __call__(self) -> bool:
        try:
            return self.es.document_count(self.index_name) == self.expected_count
        except Exception:
            return False


def _input_rows(items: list[Any], *, start_pkey: int = 0) -> list[dict]:
    base_ts = _now_ms()
    return [
        {"pkey": start_pkey + index, "ts": base_ts + index, "item": item}
        for index, item in enumerate(items)
    ]


def _test_elasticsearch_static(
    es, item_type: Any, items: list[Any], create_table: Any
) -> None:
    G.clear()
    input_rows = _input_rows(items)
    index_a = es.create_index(
        es.generate_index_name(), _INDEX_PROPERTIES, dynamic=False
    )
    index_b = es.create_index(
        es.generate_index_name(), _INDEX_PROPERTIES, dynamic=False
    )

    pw.io.elasticsearch.write(
        create_table(item_type, input_rows), ELASTICSEARCH_URL, _AUTH, index_a
    )
    run()
    es.refresh(index_a)

    G.clear()
    read_table = pw.io.elasticsearch.read(
        host=ELASTICSEARCH_URL,
        auth=_AUTH,
        index_name=index_a,
        schema=_read_schema(item_type),
        timestamp_column="ts",
        id_column="pkey",
        max_transaction_duration=_MAX_TRANSACTION_DURATION,
        mode="static",
    )
    observer, type_errors = _make_type_check_observer(item_type)
    pw.io.elasticsearch.write(read_table, ELASTICSEARCH_URL, _AUTH, index_b)
    pw.io.python.write(read_table, observer)
    # The static reader emits Finished after the snapshot, so run() terminates.
    run()

    assert not type_errors, "\n".join(type_errors)
    _assert_indices_match(es, index_a, index_b, len(items))


def _test_elasticsearch_streaming(
    es, item_type: Any, items: list[Any], create_table: Any
) -> None:
    G.clear()
    input_rows = _input_rows(items)
    index_a = es.create_index(
        es.generate_index_name(), _INDEX_PROPERTIES, dynamic=False
    )
    index_b = es.create_index(
        es.generate_index_name(), _INDEX_PROPERTIES, dynamic=False
    )

    pw.io.elasticsearch.write(
        create_table(item_type, input_rows), ELASTICSEARCH_URL, _AUTH, index_a
    )
    run()
    es.refresh(index_a)

    G.clear()
    read_table = pw.io.elasticsearch.read(
        host=ELASTICSEARCH_URL,
        auth=_AUTH,
        index_name=index_a,
        schema=_read_schema(item_type),
        timestamp_column="ts",
        id_column="pkey",
        max_transaction_duration=_MAX_TRANSACTION_DURATION,
        mode="streaming",
        poll_interval=pd.Timedelta(milliseconds=200).to_pytimedelta(),
    )
    pw.io.elasticsearch.write(read_table, ELASTICSEARCH_URL, _AUTH, index_b)
    # Forks a process that runs the streaming graph; the parent polls index B
    # until every row has been mirrored, then terminates the worker.
    wait_result_with_checker(_IndexCountChecker(es, index_b, len(items)), 120)

    _assert_indices_match(es, index_a, index_b, len(items))


def _test_elasticsearch(
    es, item_type: Any, items: list[Any], create_table: Any = _create_table
) -> None:
    _test_elasticsearch_static(es, item_type, items, create_table)
    _test_elasticsearch_streaming(es, item_type, items, create_table)


def test_bool(elasticsearch):
    _test_elasticsearch(elasticsearch, bool, [True, False, True])


def test_int(elasticsearch):
    _test_elasticsearch(elasticsearch, int, [-1, 0, 1])


def test_int_boundary(elasticsearch):
    _test_elasticsearch(elasticsearch, int, [-(2**63), 2**63 - 1, 2**31 - 1, -(2**31)])


def test_float(elasticsearch):
    _test_elasticsearch(elasticsearch, float, [-1.5, 0.0, 3.14])


def test_str(elasticsearch):
    _test_elasticsearch(elasticsearch, str, ["hello", "", "world"])


def test_str_unicode(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        str,
        [
            "привет 世界 🔥",
            "line\nnewline\ttab",
            "quote'semicolon;null,./;'l][p!@#!@#!@#%$#$#]",
            "a" * 10_000,
        ],
    )


def test_bytes(elasticsearch):
    _test_elasticsearch(elasticsearch, bytes, [b"\xde\xad\xbe\xef", b"", b"\x00\xff"])


def test_pointer(elasticsearch):
    _test_elasticsearch(
        elasticsearch, pw.Pointer, [api.ref_scalar(42), api.ref_scalar(43)]
    )


def test_json(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.Json,
        [pw.Json({"key": "value"}), pw.Json([1, 2, 3]), pw.Json(42)],
    )


def test_json_nested(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.Json,
        [
            pw.Json({"nested": {"a": 1, "b": [1, 2, 3]}}),
            pw.Json({"key": "val,with,commas"}),
            pw.Json({}),
        ],
    )


def test_duration(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.Duration,
        [
            pd.Timedelta("0"),
            pd.Timedelta("1 days 2 seconds 3 us"),
            pd.Timedelta("-1 days"),
        ],
    )


def test_duration_negative_mixed(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.Duration,
        [
            pd.Timedelta("-1 days 2 hours 3 minutes"),
            pd.Timedelta("-2 days 30 minutes"),
        ],
    )


def test_duration_large(elasticsearch):
    _test_elasticsearch(elasticsearch, pw.Duration, [pd.Timedelta(days=365 * 100)])


def test_datetime_naive(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.DateTimeNaive,
        [
            pw.DateTimeNaive("2025-01-01T00:00:00"),
            pw.DateTimeNaive("2025-03-14T10:13:00.123000000"),
        ],
    )


def test_datetime_utc(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.DateTimeUtc,
        [
            pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
            pw.DateTimeUtc("2025-03-14T10:13:00.123000000+00:00"),
        ],
    )


def test_optional_int(elasticsearch):
    _test_elasticsearch(elasticsearch, int | None, [1, None, -1, None, 0])


def test_optional_str(elasticsearch):
    _test_elasticsearch(elasticsearch, str | None, ["hello", None, "", None, "world"])


def test_optional_float(elasticsearch):
    _test_elasticsearch(elasticsearch, float | None, [1.5, None, -2.5, None, 0.0])


def test_optional_bytes(elasticsearch):
    _test_elasticsearch(elasticsearch, bytes | None, [b"\x01\x02", None, b"", None])


def test_optional_duration(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.Duration | None,
        [pd.Timedelta("1 days 2 seconds"), None, pd.Timedelta("-3 hours")],
    )


def test_optional_datetime_naive(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.DateTimeNaive | None,
        [pw.DateTimeNaive("2025-01-01T00:00:00"), None],
    )


def test_optional_json(elasticsearch):
    _test_elasticsearch(
        elasticsearch, pw.Json | None, [pw.Json({"a": 1}), None, pw.Json([1, 2])]
    )


def test_bool_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[bool | None],
        [[True, False], [True, None], [None, False]],
    )


def test_int_array(elasticsearch):
    _test_elasticsearch(elasticsearch, list[int | None], [[1, 2], [3, None], [None, 4]])


def test_float_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch, list[float | None], [[1.1, 2.2], [3.3, None], [None, 4.4]]
    )


def test_str_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch, list[str | None], [["a", "b"], ["c", None], [None, "d"]]
    )


def test_str_array_with_backslash_and_quotes(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[str | None],
        [
            ['say "hello"', "path\\to\\file"],
            ['mixed \\"escaped\\"', None],
            ["C:\\Users\\test", 'quote"then\\slash'],
        ],
    )


def test_bytes_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[bytes],
        [[b"\xde\xad", b"\xbe\xef"], [b"\xab\xcd", b"\xef\x01"]],
    )


def test_pointer_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[pw.Pointer | None],
        [[api.ref_scalar(42), api.ref_scalar(43)], [None, None]],
    )


def test_json_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[pw.Json | None],
        [
            [pw.Json({"a": 1}), pw.Json([1, 2, 3])],
            [pw.Json("string"), None],
            [None, pw.Json(42)],
        ],
    )


def test_duration_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[pw.Duration | None],
        [
            [pd.Timedelta("0"), pd.Timedelta("1 days 2 seconds 3 us")],
            [pd.Timedelta("-1 days"), None],
            [None, pd.Timedelta("3 days 4 hours")],
        ],
    )


def test_2d_bool_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[list[bool]],
        [[[True, False], [False, True]], [[True, True], [False, False]]],
    )


def test_2d_int_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch, list[list[int]], [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
    )


def test_2d_float_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[list[float]],
        [[[1.1, 2.2], [3.3, 4.4]], [[5.5, 6.6], [7.7, 8.8]]],
    )


def test_2d_str_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[list[str]],
        [[["a", "b"], ["c", "d"]], [["w", "x"], ["y", "z"]]],
    )


def test_2d_json_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[list[pw.Json]],
        [
            [
                [pw.Json({"x": 1}), pw.Json({"y": 2})],
                [pw.Json([1, 2]), pw.Json([3, 4])],
            ],
            [[pw.Json("a"), pw.Json("b")], [pw.Json(True), pw.Json(False)]],
        ],
    )


def test_empty_list(elasticsearch):
    _test_elasticsearch(elasticsearch, list[int], [[]])


def test_empty_str_list(elasticsearch):
    _test_elasticsearch(elasticsearch, list[str | None], [[None, None]])


def test_tuple(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        tuple[int, str, bool],
        [(1, "hello", True), (2, "world", False), (-1, "", True)],
    )


def test_tuple_optional(elasticsearch):
    _test_elasticsearch(
        elasticsearch, tuple[int, str] | None, [(1, "hello"), None, (2, "world")]
    )


def test_pyobject_wrapper(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        pw.PyObjectWrapper[SimpleObject],
        [
            pw.wrap_py_object(SimpleObject(1)),
            pw.wrap_py_object(SimpleObject(2)),
            pw.wrap_py_object(SimpleObject(3)),
        ],
    )


def test_pyobject_wrapper_array(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        list[pw.PyObjectWrapper[SimpleObject]],
        [
            [pw.wrap_py_object(SimpleObject(1)), pw.wrap_py_object(SimpleObject(2))],
            [pw.wrap_py_object(SimpleObject(3)), pw.wrap_py_object(SimpleObject(4))],
        ],
    )


def test_int_ndarray_1d(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        np.ndarray[None, int],  # type: ignore
        [
            np.array([1, 2, 3], dtype=np.int64),
            np.array([-1, 0, 1], dtype=np.int64),
            np.array([], dtype=np.int64),
        ],
        _create_ndarray_table,
    )


def test_float_ndarray_1d(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        np.ndarray[None, float],  # type: ignore
        [
            np.array([1.1, 2.2, 3.3], dtype=np.float64),
            np.array([-1.5, 0.0, 3.14], dtype=np.float64),
            np.array([], dtype=np.float64),
        ],
        _create_ndarray_table,
    )


def test_int_ndarray_2d(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        np.ndarray[None, int],  # type: ignore
        [
            np.array([[1, 2], [3, 4]], dtype=np.int64),
            np.array([[5, 6], [7, 8]], dtype=np.int64),
        ],
        _create_ndarray_table,
    )


def test_float_ndarray_2d(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        np.ndarray[None, float],  # type: ignore
        [
            np.array([[1.1, 2.2], [3.3, 4.4]], dtype=np.float64),
            np.array([[5.5, 6.6], [7.7, 8.8]], dtype=np.float64),
        ],
        _create_ndarray_table,
    )


def test_int_ndarray_3d(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        np.ndarray[None, int],  # type: ignore
        [np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.int64)],
        _create_ndarray_table,
    )


def test_float_ndarray_3d(elasticsearch):
    _test_elasticsearch(
        elasticsearch,
        np.ndarray[None, float],  # type: ignore
        [
            np.array(
                [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]], dtype=np.float64
            )
        ],
        _create_ndarray_table,
    )

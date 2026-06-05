# Copyright © 2026 Pathway

"""Per-type parsing coverage for the ClickHouse output connector.

For every Pathway type the connector supports, this writes a column of that type
with ``pw.io.clickhouse.write`` and reads it back over ClickHouse's HTTP
interface, checking the value survived the trip. A homogeneous ``list[T]`` of a
scalar type is stored as ``Array(T)``; the unsupported array-like types
(heterogeneous tuples, ``np.ndarray``, and lists nested beyond one level or with
optional/bytes elements) are covered by a dedicated rejection test instead.
"""

import base64
import json
from typing import Any, get_args, get_origin

import numpy as np
import pandas as pd
import pytest
from utils import SimpleObject

import pathway as pw
from pathway.internals import api

WAIT_TIMEOUT_SECONDS = 60


def _is_pyobject_type(item_type: Any) -> bool:
    return get_origin(item_type) is pw.PyObjectWrapper or "PyObjectWrapper" in str(
        item_type
    )


def _split_optional(item_type: Any) -> tuple[Any, bool]:
    args = get_args(item_type)
    if args and type(None) in args:
        base = next(arg for arg in args if arg is not type(None))
        return base, True
    return item_type, False


def _select_columns(base: Any) -> list[str]:
    """SQL expressions read back for the ``item`` column, normalized so the
    value comes over HTTP in a JSON-friendly form regardless of its type."""
    if base is bytes:
        # A byte-valued String is not necessarily valid UTF-8, so it cannot be
        # returned in JSON directly; hex() makes it printable and lossless.
        return ["pkey", "hex(item) AS item"]
    if base in (pw.DateTimeNaive, pw.DateTimeUtc):
        return ["pkey", "toUnixTimestamp64Nano(item) AS item"]
    return ["pkey", "item"]


def _canon_input(value: Any, base: Any) -> Any:
    """Normalize an original (Pathway/Python) value to the canonical form used
    for comparison against the read-back value."""
    if value is None:
        return None
    if isinstance(value, pw.PyObjectWrapper):
        return value.value
    if isinstance(value, pw.Pointer):
        return str(value)
    if isinstance(value, pw.Json):
        return value.value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, pd.Timestamp):
        # pandas stores the instant as nanoseconds since the epoch; a naive
        # timestamp is interpreted as a UTC wall clock, matching the connector.
        return value.value
    if isinstance(value, pd.Timedelta):
        return value.value  # nanoseconds
    return value


def _canon_output(value: Any, base: Any) -> Any:
    """Normalize a value read back from ClickHouse over HTTP to the same
    canonical form as :func:`_canon_input`."""
    if value is None:
        return None
    if _is_pyobject_type(base):
        restored = api.deserialize(base64.b64decode(value))
        assert isinstance(
            restored, pw.PyObjectWrapper
        ), f"expecting PyObjectWrapper, got {type(restored)}"
        return restored.value
    if base is pw.Json:
        return json.loads(value)
    if base is bytes:
        return value.lower()  # hex() returns uppercase
    if base in (pw.DateTimeNaive, pw.DateTimeUtc, pw.Duration):
        return int(value)
    return value


def _compare(item_type: Any, input_rows: list[dict], output_rows: list[dict]) -> None:
    base, _ = _split_optional(item_type)
    input_rows = sorted(input_rows, key=lambda row: row["pkey"])
    output_rows = sorted(output_rows, key=lambda row: row["pkey"])

    canonical_input = [
        {"pkey": row["pkey"], "item": _canon_input(row["item"], base)}
        for row in input_rows
    ]
    canonical_output = [
        {"pkey": row["pkey"], "item": _canon_output(row["item"], base)}
        for row in output_rows
    ]

    if base is float:
        assert len(canonical_output) == len(canonical_input)
        for got, expected in zip(canonical_output, canonical_input):
            assert got["pkey"] == expected["pkey"]
            if expected["item"] is None:
                assert got["item"] is None
            else:
                assert got["item"] == pytest.approx(expected["item"])
    else:
        assert (
            canonical_output == canonical_input
        ), f"\ninput:  {canonical_input}\noutput: {canonical_output}"


def _create_table(item_type: Any, input_rows: list[dict]) -> pw.Table:
    class InputSchemaWithPkey(pw.Schema):
        pkey: int = pw.column_definition(primary_key=True)
        item: item_type

    return pw.debug.table_from_rows(
        InputSchemaWithPkey,
        [tuple(row.values()) for row in input_rows],
    )


def _roundtrip(clickhouse, item_type: Any, items: list[Any]) -> None:
    from utils import EntryCountChecker

    from pathway.tests.utils import wait_result_with_checker

    base, _ = _split_optional(item_type)
    input_rows = [{"pkey": index, "item": item} for index, item in enumerate(items)]

    with clickhouse.temp_table() as table_name:
        table = _create_table(item_type, input_rows)
        pw.io.clickhouse.write(
            table,
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            init_mode="create_if_not_exists",
        )
        checker = EntryCountChecker(
            len(items), clickhouse, table_name=table_name, column_names=["pkey"]
        )
        wait_result_with_checker(checker, WAIT_TIMEOUT_SECONDS)
        output_rows = clickhouse.get_table_contents(table_name, _select_columns(base))
        _compare(item_type, input_rows, output_rows)


def test_bool(clickhouse):
    _roundtrip(clickhouse, bool, [True, False, True])


def test_int(clickhouse):
    _roundtrip(clickhouse, int, [-1, 0, 1])


def test_int_boundary(clickhouse):
    _roundtrip(clickhouse, int, [-(2**63), 2**63 - 1, 2**31 - 1, -(2**31)])


def test_float(clickhouse):
    _roundtrip(clickhouse, float, [-1.5, 0.0, 3.14, 1e-300, 1e300])


def test_str(clickhouse):
    _roundtrip(clickhouse, str, ["hello", "", "world"])


def test_str_unicode(clickhouse):
    _roundtrip(
        clickhouse,
        str,
        [
            "привет 世界 🔥",
            "line\nnewline\ttab",
            "quote'semicolon;null,./;'l][p!@#!@#!@#%$#$#]",
            "a" * 10_000,
        ],
    )


def test_bytes(clickhouse):
    _roundtrip(clickhouse, bytes, [b"\xde\xad\xbe\xef", b"", b"\x00\xff"])


def test_pointer(clickhouse):
    _roundtrip(clickhouse, pw.Pointer, [api.ref_scalar(42), api.ref_scalar(43)])


def test_json(clickhouse):
    _roundtrip(
        clickhouse,
        pw.Json,
        [pw.Json({"key": "value"}), pw.Json([1, 2, 3]), pw.Json(42)],
    )


def test_json_nested(clickhouse):
    _roundtrip(
        clickhouse,
        pw.Json,
        [
            pw.Json({"nested": {"a": 1, "b": [1, 2, 3]}}),
            pw.Json({"key": "val,with,commas"}),
            pw.Json({}),
        ],
    )


def test_duration(clickhouse):
    _roundtrip(
        clickhouse,
        pw.Duration,
        [
            pd.Timedelta("0"),
            pd.Timedelta("1 days 2 seconds 3 us"),
            pd.Timedelta("-1 days"),
        ],
    )


def test_duration_negative_mixed(clickhouse):
    _roundtrip(
        clickhouse,
        pw.Duration,
        [
            pd.Timedelta("-1 days 2 hours 3 minutes"),
            pd.Timedelta("-2 days 30 minutes"),
        ],
    )


def test_datetime_naive(clickhouse):
    _roundtrip(
        clickhouse,
        pw.DateTimeNaive,
        [
            pw.DateTimeNaive("2025-01-01T00:00:00"),
            pw.DateTimeNaive("2025-03-14T10:13:00.123000"),
            pw.DateTimeNaive("1969-12-31T23:59:59.999999000"),
        ],
    )


def test_datetime_utc(clickhouse):
    _roundtrip(
        clickhouse,
        pw.DateTimeUtc,
        [
            pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
            pw.DateTimeUtc("2025-03-14T10:13:00.123000+00:00"),
        ],
    )


def test_pyobject_wrapper(clickhouse):
    _roundtrip(
        clickhouse,
        pw.PyObjectWrapper[SimpleObject],
        [
            pw.wrap_py_object(SimpleObject(1)),
            pw.wrap_py_object(SimpleObject(2)),
            pw.wrap_py_object(SimpleObject(3)),
        ],
    )


def test_optional_int(clickhouse):
    _roundtrip(clickhouse, int | None, [1, None, -1, None, 0])


def test_optional_float(clickhouse):
    _roundtrip(clickhouse, float | None, [1.5, None, -2.5, None, 0.0])


def test_optional_bool(clickhouse):
    _roundtrip(clickhouse, bool | None, [True, None, False, None])


def test_optional_str(clickhouse):
    _roundtrip(clickhouse, str | None, ["hello", None, "", None, "world"])


def test_optional_bytes(clickhouse):
    _roundtrip(clickhouse, bytes | None, [b"\x01\x02", None, b"", None])


def test_optional_pointer(clickhouse):
    _roundtrip(clickhouse, pw.Pointer | None, [api.ref_scalar(7), None])


def test_optional_json(clickhouse):
    _roundtrip(clickhouse, pw.Json | None, [pw.Json({"a": 1}), None, pw.Json([1, 2])])


def test_optional_duration(clickhouse):
    _roundtrip(
        clickhouse,
        pw.Duration | None,
        [pd.Timedelta("1 days 2 seconds"), None, pd.Timedelta("-3 hours")],
    )


def test_optional_datetime_naive(clickhouse):
    _roundtrip(
        clickhouse,
        pw.DateTimeNaive | None,
        [pw.DateTimeNaive("2025-01-01T00:00:00"), None],
    )


def test_optional_datetime_utc(clickhouse):
    _roundtrip(
        clickhouse,
        pw.DateTimeUtc | None,
        [pw.DateTimeUtc("2025-01-01T00:00:00+00:00"), None],
    )


@pytest.mark.parametrize(
    "item_type, value",
    [
        # A heterogeneous tuple maps to a ClickHouse Tuple, which the
        # native-protocol client cannot build. (A fixed-length tuple of identical
        # element types *is* supported, as an Array — see the tuple tests below.)
        (tuple[int, str], (1, "a")),
        # A homogeneous tuple whose element type is itself unsupported as an Array
        # element (here ``bytes``) is rejected too.
        (tuple[bytes, bytes], (b"a", b"b")),
        # Only one level of array nesting is supported: a list whose element is
        # itself a list, an optional, bytes, or a datetime cannot be built as a
        # full-fidelity Array column.
        (list[list[int]], [[1, 2], [3]]),
        (list[int | None], [1, None, 3]),
        (list[bytes], [b"a", b"b"]),
        (list[pw.DateTimeUtc], [pw.DateTimeUtc("2025-01-01T00:00:00+00:00")]),
        # Only one-dimensional, element-typed numeric ndarrays are supported.
        (
            np.ndarray[tuple[int, int], np.dtype[np.float64]],
            np.array([[1.0, 2.0], [3.0, 4.0]]),
        ),
        (np.ndarray, np.array([1.0, 2.0, 3.0])),  # unknown dimensionality
    ],
)
def test_clickhouse_rejects_unsupported_type(clickhouse, item_type, value):
    """Types the ClickHouse output connector cannot represent must surface a clear
    error rather than silently dropping data. This includes heterogeneous tuples
    and lists nested beyond one level (or whose elements are optional/bytes).
    """
    table = _create_table(item_type, [{"pkey": 0, "item": value}])
    with clickhouse.temp_table() as table_name:
        pw.io.clickhouse.write(
            table,
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            init_mode="create_if_not_exists",
        )
        with pytest.raises(Exception, match="(?i)unsupported type"):
            pw.run()


def test_list_int(clickhouse):
    _roundtrip(clickhouse, list[int], [[1, 2, 3], [], [-5]])


def test_list_float(clickhouse):
    _roundtrip(clickhouse, list[float], [[1.5, -2.5], [0.0], []])


def test_list_str(clickhouse):
    _roundtrip(clickhouse, list[str], [["a", "b"], [""], ["x", "y", "z"]])


def test_list_bool(clickhouse):
    _roundtrip(clickhouse, list[bool], [[True, False, True], [], [False]])


def _write_single_row_list(clickhouse, item_type, value, read_columns):
    """Writes a one-row table with an ``item`` column of ``item_type`` holding
    ``value``, waits for it, and returns the read-back row using ``read_columns``
    (SQL expressions). Used for list element types whose round-trip needs a
    per-element read-back transform."""
    from utils import EntryCountChecker

    from pathway.tests.utils import wait_result_with_checker

    seed = pw.debug.table_from_rows(
        type("S", (pw.Schema,), {"__annotations__": {"pkey": int}}), [(0,)]
    )
    udf = pw.udf(lambda pkey: value, return_type=item_type)
    table = seed.select(pkey=pw.this.pkey, item=udf(pw.this.pkey))
    with clickhouse.temp_table() as table_name:
        pw.io.clickhouse.write(
            table,
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            init_mode="create_if_not_exists",
        )
        checker = EntryCountChecker(
            1, clickhouse, table_name=table_name, column_names=["pkey"]
        )
        wait_result_with_checker(checker, WAIT_TIMEOUT_SECONDS)
        return clickhouse.get_table_contents(table_name, read_columns)[0]


def test_list_duration(clickhouse):
    """A ``list[Duration]`` stores as ``Array(Int64)`` of nanoseconds."""
    durations = [pd.Timedelta("1 days 2 seconds 3 us"), pd.Timedelta("-3 hours")]
    row = _write_single_row_list(clickhouse, list[pw.Duration], durations, ["item"])
    assert row["item"] == [d.value for d in durations]


def test_ndarray_1d_float(clickhouse):
    """A one-dimensional float ndarray stores as ``Array(Float64)`` and
    round-trips element-by-element (the vector / embedding case)."""
    vector = np.array([1.5, -2.5, 0.0, 3.25])
    row = _write_single_row_list(
        clickhouse,
        np.ndarray[tuple[int], np.dtype[np.float64]],
        vector,
        ["item"],
    )
    assert row["item"] == vector.tolist()


def test_ndarray_1d_int(clickhouse):
    """A one-dimensional int ndarray stores as ``Array(Int64)``."""
    vector = np.array([1, -2, 3, 0], dtype=np.int64)
    row = _write_single_row_list(
        clickhouse,
        np.ndarray[tuple[int], np.dtype[np.int64]],
        vector,
        ["item"],
    )
    assert row["item"] == vector.tolist()


def test_tuple_homogeneous_int(clickhouse):
    """A fixed-length tuple of ints stores as ``Array(Int64)`` (the tuple length
    is not preserved — it becomes a variable-length array)."""
    value = (1, -2, 3)
    row = _write_single_row_list(clickhouse, tuple[int, int, int], value, ["item"])
    assert row["item"] == list(value)


def test_tuple_homogeneous_float(clickhouse):
    """A fixed-length tuple of floats stores as ``Array(Float64)`` (e.g. a
    fixed-size vector)."""
    value = (1.5, -2.5)
    row = _write_single_row_list(clickhouse, tuple[float, float], value, ["item"])
    assert row["item"] == list(value)


def test_tuple_homogeneous_str(clickhouse):
    """A fixed-length tuple of strings stores as ``Array(String)``."""
    value = ("a", "b", "c")
    row = _write_single_row_list(clickhouse, tuple[str, str, str], value, ["item"])
    assert row["item"] == list(value)

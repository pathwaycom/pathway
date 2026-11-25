# Copyright © 2026 Pathway

import base64
import datetime
import decimal
import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from deltalake import write_deltalake

import pathway as pw
from pathway.internals import api
from pathway.tests.utils import assert_table_equality_wo_index


@pytest.fixture(params=[False, True], ids=["parquet", "arrow-backfill"])
def lake_reader(request, tmp_path):
    def read(rows, arrow_type, schema):
        location = tmp_path / "lake"
        write_deltalake(
            str(location),
            pa.Table.from_pylist(
                [{"key": index, "one": row} for index, row in enumerate(rows)],
                schema=pa.schema([("key", pa.int64()), ("one", arrow_type)]),
            ),
        )
        schema = pw.schema_builder(
            {
                "key": pw.column_definition(dtype=int, primary_key=True),
                "one": pw.column_definition(dtype=schema),
            }
        )
        return pw.io.deltalake.read(
            location,
            schema=schema,
            mode="static",
            _backfilling_thresholds=(
                [api.BackfillingThreshold(field="key", comparison_op=">=", threshold=0)]
                if request.param
                else None
            ),
        )

    return read


@pytest.mark.parametrize(
    "dtype,arrow_type,value,expected",
    [
        (bool, pa.bool_(), True, True),
        (int, pa.int64(), 2**63 - 1, 2**63 - 1),
        (float, pa.int32(), 42, 42.0),
        (float, pa.float64(), 1.25, 1.25),
        (str, pa.string(), "hello π", "hello π"),
        (bytes, pa.binary(), b"\x00\xff", b"\x00\xff"),
        (bytes, pa.binary(), b"", b""),
        (str, pa.decimal128(10, 2), decimal.Decimal("1.25"), "1.25"),
        (
            pw.DateTimeUtc,
            pa.timestamp("us", tz="UTC"),
            datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc),
            pd.Timestamp("2024-01-02T00:00:00Z"),
        ),
        (
            pw.DateTimeNaive,
            pa.timestamp("us"),
            datetime.datetime(2024, 1, 2),
            pd.Timestamp("2024-01-02"),
        ),
        (
            pw.DateTimeNaive,
            pa.date32(),
            datetime.date(2024, 1, 2),
            pd.Timestamp("2024-01-02"),
        ),
        (pw.Duration, pa.int64(), -123, pd.Timedelta(-123, unit="us")),
        (pw.Pointer, pa.string(), str(api.ref_scalar(42)), api.ref_scalar(42)),
        (
            np.ndarray[tuple[int], np.dtype[np.int64]],
            pa.list_(pa.int64()),
            [1, -2],
            np.array([1, -2]),
        ),
        (
            np.ndarray[tuple[int], np.dtype[np.int64]],
            pa.list_(pa.int64()),
            [],
            np.array([], dtype=np.int64),
        ),
        (
            np.ndarray[tuple[int, int], np.dtype[np.float64]],
            pa.list_(pa.list_(pa.float64())),
            [[1, 2.5]],
            np.array([[1, 2.5]]),
        ),
        (
            np.ndarray[tuple[()], np.dtype[np.float64]],
            pa.float64(),
            1.25,
            np.array(1.25),
        ),
        (pw.Json, pa.string(), '{"a":1}', pw.Json('{"a":1}')),
    ],
)
def test_nested_lake_native_leaves(lake_reader, dtype, arrow_type, value, expected):
    schema = pw.schema_from_types(two=pw.schema_from_types(three=dtype))
    nested_type = pa.struct([("two", pa.struct([("three", arrow_type)]))])
    table = lake_reader([{"two": {"three": value}}], nested_type, schema)
    projected = table.select(two=table.one.two)
    result = table.select(
        direct=table.one.two.three,
        bracket=table.one["two"]["three"],
        projected=projected.two.three,
    )
    expected_schema = pw.schema_from_types(direct=dtype, bracket=dtype, projected=dtype)
    assert result.schema.typehints() == expected_schema.typehints()
    assert projected.schema.typehints() == {"two": pw.Json}
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(expected_schema, [(expected, expected, expected)]),
    )


def test_nested_lake_defaults_maps_and_null(lake_reader):
    class Leaf(pw.Schema):
        price: float
        content: bytes
        currency: str = pw.column_definition(default_value="USD")

    schema = pw.schema_from_types(two=Leaf)
    native_type = pa.map_(
        pa.string(),
        pa.struct(
            [("content", pa.binary()), ("price", pa.float64()), ("extra", pa.int64())]
        ),
    )
    table = lake_reader(
        [{"two": {"price": 12.5, "content": b"ok", "extra": 7}}, None],
        native_type,
        schema | None,
    )
    result = table.select(
        price=table.one.two.price,
        content=table.one.two.content,
        currency=table.one.two.currency,
        raw=table.one.two,
    )
    expected = pw.schema_from_types(
        price=float | None,
        content=bytes | None,
        currency=str | None,
        raw=pw.Json | None,
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected,
            [
                (
                    12.5,
                    b"ok",
                    "USD",
                    pw.Json(
                        {
                            "content": "b2s=",
                            "price": 12.5,
                            "extra": 7,
                            "currency": "USD",
                        }
                    ),
                ),
                (None, None, None, None),
            ],
        ),
    )


@pytest.mark.parametrize("value", ["bad", None])
def test_nested_lake_invalid_leaf(lake_reader, value, caplog):
    table = lake_reader(
        [{"two": {"three": value}}],
        pa.struct([("two", pa.struct([("three", pa.string())]))]),
        pw.schema_from_types(two=pw.schema_from_types(three=float)),
    )
    table = table.remove_errors()
    result = table.select(value=table.one.two.three)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(pw.schema_from_types(value=float), []),
        terminate_on_error=False,
    )
    assert "/one/two/three" in caplog.text


def test_nested_lake_python_wrapper(lake_reader):
    value = pw.wrap_py_object({"key": [1, 2]})
    encoded = base64.b64decode(json.loads(api.value_to_json(value)))
    table = lake_reader(
        [{"value": encoded}],
        pa.struct([("value", pa.binary())]),
        pw.schema_from_types(value=pw.PyObjectWrapper),
    )
    result = table.select(value=table.one.value)

    @pw.udf
    def get_key(value: pw.PyObjectWrapper) -> int:
        return value.value["key"][1]

    extracted = result.select(value=get_key(result.value))
    assert_table_equality_wo_index(
        extracted, pw.debug.table_from_rows(pw.schema_from_types(value=int), [(2,)])
    )


def test_nested_lake_plain_json_native_struct(lake_reader):
    table = lake_reader(
        [{"text": "ok", "content": b"ok"}],
        pa.struct([("text", pa.string()), ("content", pa.binary())]),
        pw.Json,
    )
    assert_table_equality_wo_index(
        table.select(value=table.one),
        pw.debug.table_from_rows(
            pw.schema_from_types(value=pw.Json),
            [(pw.Json({"text": "ok", "content": "b2s="}),)],
        ),
    )


def test_nested_lake_list_and_tuple(lake_reader):
    class Leaf(pw.Schema):
        pair: tuple[int, str]
        fallback: int = pw.column_definition(default_value=7)

    native_type = pa.list_(
        pa.struct([("pair", pa.struct([("a", pa.int64()), ("b", pa.string())]))])
    )
    table = lake_reader([[{"pair": {"a": 2, "b": "ok"}}], []], native_type, list[Leaf])
    result = table.select(
        first=table.one[0].pair[0],
        second=table.one[0].pair[1],
        default=table.one[0].fallback,
    )
    expected = pw.schema_from_types(
        first=int | None, second=str | None, default=int | None
    )
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(expected, [(2, "ok", 7), (None, None, None)])
    )


def test_nested_lake_rejects_ragged_array(lake_reader, caplog):
    schema = pw.schema_from_types(
        array=np.ndarray[tuple[int, int], np.dtype[np.float64]]
    )
    table = lake_reader(
        [{"array": [[1], [2, 3]]}],
        pa.struct([("array", pa.list_(pa.list_(pa.float64())))]),
        schema,
    )
    table = table.remove_errors()
    result = table.select(value=table.one)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(pw.schema_from_types(value=pw.Json), []),
        terminate_on_error=False,
    )
    assert "/one/array" in caplog.text


def test_nested_lake_rejects_non_string_map_keys(lake_reader, caplog):
    table = lake_reader([[(1, 7)]], pa.map_(pa.int64(), pa.int64()), pw.Json)
    table = table.remove_errors()
    result = table.select(value=table.one)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(pw.schema_from_types(value=pw.Json), []),
        terminate_on_error=False,
    )
    assert "string keys" in caplog.text


def test_nested_lake_reuses_inferred_map_metadata(lake_reader):
    inner = pw.schema_builder(
        {
            "two": pw.column_definition(
                dtype=pw.Json,
                _json_schema={"type": "map", "values": {"type": "bytes"}},
            )
        }
    )
    table = lake_reader(
        [{"two": {"eur": b"\x00\xff"}}],
        pa.struct([("two", pa.map_(pa.string(), pa.binary()))]),
        inner,
    )
    result = table.select(value=table.one.two.eur, missing=table.one.two.absent)
    expected = pw.schema_from_types(value=bytes | None, missing=bytes | None)
    assert result.schema.typehints() == expected.typehints()
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(expected, [(b"\x00\xff", None)])
    )

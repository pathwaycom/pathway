# Copyright © 2026 Pathway

"""Nested leaves use the JSON connector's encodings and decode on field access.

Type              JSON representation
bytes             base64 string
DateTimeNaive      timestamp string without a timezone
DateTimeUtc        timestamp string with a timezone
Duration          integer nanoseconds
Pointer           Pathway pointer string
numeric ndarray   object with shape and flat elements
PyObjectWrapper   base64-encoded Pathway serialization
"""

import base64
import csv
import json
import pickle

import numpy as np
import pandas as pd
import pytest

import pathway as pw
from pathway.internals import api, dtype as dt
from pathway.internals.parse_graph import G
from pathway.tests.utils import assert_table_equality_wo_index, run_all

INT_ARRAY = np.ndarray[tuple[int], np.dtype[np.int64]]
FLOAT_ARRAY = np.ndarray[tuple[int, int], np.dtype[np.float64]]

ENCODED_LEAVES = [
    pytest.param(bytes, b"\x00\x01\xff", id="bytes"),
    pytest.param(bytes, b"", id="empty-bytes"),
    pytest.param(
        pw.DateTimeNaive, pd.Timestamp("2026-11-04T12:34:56.123456789"), id="naive"
    ),
    pytest.param(
        pw.DateTimeUtc,
        pd.Timestamp("2026-11-04T12:34:56.123456789+02:00"),
        id="utc",
    ),
    pytest.param(pw.Duration, pd.Timedelta(-123456789, unit="ns"), id="duration"),
    pytest.param(pw.Pointer, api.ref_scalar(42), id="pointer"),
    pytest.param(INT_ARRAY, np.array([1, -2, 3]), id="int-array"),
    pytest.param(FLOAT_ARRAY, np.array([[1.25, 2.5], [3.0, -4.0]]), id="float-array"),
    pytest.param(INT_ARRAY, np.array([], dtype=np.int64), id="empty-array"),
    pytest.param(
        np.ndarray[None, np.dtype[np.int64]],
        np.array([[[1, 2], [3, 4]]]),
        id="unknown-rank-array",
    ),
    pytest.param(
        np.ndarray[tuple[()], np.dtype[np.float64]], np.array(1.5), id="scalar-array"
    ),
]


@pytest.fixture(params=["json", "csv", "python"])
def read_nested(tmp_path, request):
    def read(schema, rows):
        if request.param == "python":

            class Subject(pw.io.python.ConnectorSubject):
                def run(self):
                    for row in rows:
                        self.next(**row)

            return pw.io.python.read(Subject(), schema=schema)
        if request.param == "json":
            path = tmp_path / "nested.jsonl"
            path.write_text("\n".join(map(json.dumps, rows)) + "\n")
            return pw.io.jsonlines.read(path, schema=schema, mode="static")
        path = tmp_path / "nested.csv"
        columns = list(schema.column_names())
        with path.open("w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(
                [json.dumps(row[name]) if name in row else "" for name in columns]
                for row in rows
            )
        return pw.io.csv.read(path, schema=schema, mode="static")

    return read


@pytest.mark.parametrize("dtype,value", ENCODED_LEAVES)
def test_nested_json_types_roundtrip_through_connector(
    tmp_path, read_nested, dtype, value
):
    # The writer supplies the wire format; the reader must recover the original value.
    source = pw.debug.table_from_rows(pw.schema_from_types(three=dtype), [(value,)])
    output = tmp_path / "encoded.jsonl"
    pw.io.jsonlines.write(source, output)
    run_all()
    encoded = json.loads(output.read_text())["three"]
    G.clear()

    leaf = pw.schema_from_types(three=dtype)
    middle = pw.schema_from_types(two=leaf)
    schema = pw.schema_from_types(one=middle)
    schema = pw.schema_from_dict(
        **json.loads(json.dumps(schema.to_json_serializable_dict()))
    )
    table = read_nested(schema, [{"one": {"two": {"three": encoded}}}])
    projected = table.select(two=table.one.two)
    result = table.select(
        direct=table.one.two.three,
        bracket=table.one["two"]["three"],
        projected=projected.two.three,
        raw=table.one,
        object_=table.one.two,
    )
    for name in ("direct", "bracket", "projected"):
        assert result.schema[name].dtype == dt.wrap(dtype)
    assert result.schema["raw"].dtype == dt.JSON
    assert result.schema["object_"].dtype == dt.JSON
    expected = pw.debug.table_from_rows(
        pw.schema_from_types(
            direct=dtype, bracket=dtype, projected=dtype, raw=pw.Json, object_=pw.Json
        ),
        [
            (
                value,
                value,
                value,
                pw.Json({"two": {"three": encoded}}),
                pw.Json({"three": encoded}),
            )
        ],
    )
    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize("dtype,value", ENCODED_LEAVES)
def test_nested_json_types_optional_containers_and_defaults(read_nested, dtype, value):
    inner = pw.schema_builder(
        {
            "two": pw.column_definition(dtype=dtype | None),
            "fallback": pw.column_definition(dtype=dtype, default_value=value),
            "values": pw.column_definition(dtype=list[dtype]),
            "pair": pw.column_definition(dtype=tuple[dtype, int]),
        }
    )
    schema = pw.schema_from_types(one=inner | None)
    encoded = json.loads(api.value_to_json(value))
    table = read_nested(
        schema,
        [
            {"one": {"two": encoded, "values": [encoded], "pair": [encoded, 1]}},
            {"one": {"two": None, "values": [], "pair": [encoded, 2]}},
            {"one": None},
        ],
    )
    result = table.select(
        two=table.one.two,
        fallback=table.one.fallback,
        first=table.one.values[0],
        pair=table.one.pair[0],
    )
    expected_schema = pw.schema_from_types(
        two=dtype | None, fallback=dtype | None, first=dtype | None, pair=dtype | None
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (value, value, value, value),
                (None, value, None, value),
                (None, None, None, None),
            ],
        ),
    )


@pytest.mark.parametrize("dtype,value", ENCODED_LEAVES)
def test_nested_json_typed_values_in_object_defaults(read_nested, dtype, value):
    inner = pw.schema_from_types(two=dtype)
    middle = pw.schema_builder(
        {"child": pw.column_definition(dtype=inner, default_value={"two": value})}
    )
    schema = pw.schema_builder(
        {
            "one": pw.column_definition(
                dtype=middle, default_value={"child": {"two": value}}
            )
        }
    )
    schema = pw.schema_from_dict(
        **json.loads(json.dumps(schema.to_json_serializable_dict()))
    )
    table = read_nested(schema, [{}, {"one": {}}])
    result = table.select(two=table.one.child.two)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(pw.schema_from_types(two=dtype), [(value,), (value,)]),
    )


@pytest.mark.parametrize(
    "dtype,invalid",
    [
        (bytes, "%%%"),
        (bytes, 123),
        (bytes, None),
        (pw.DateTimeNaive, "2026-11-04T12:00:00Z"),
        (pw.DateTimeUtc, "2026-11-04T12:00:00"),
        (pw.Duration, "1s"),
        (pw.Duration, 1.5),
        (pw.Pointer, "not-a-pointer"),
        (INT_ARRAY, {}),
        (INT_ARRAY, {"shape": [2]}),
        (INT_ARRAY, {"elements": [1, 2]}),
        (INT_ARRAY, {"shape": [2], "elements": [1]}),
        (INT_ARRAY, {"shape": [-1], "elements": []}),
        (INT_ARRAY, {"shape": [1, 1], "elements": [1]}),
        (INT_ARRAY, {"shape": [1], "elements": [1.5]}),
        (pw.PyObjectWrapper, "%%%"),
        # An encoded engine value of another kind is not a wrapped Python object.
        (pw.PyObjectWrapper, base64.b64encode(api.serialize(42)).decode()),
    ],
)
def test_nested_json_invalid_encoded_defaults_report_full_path(
    read_nested, dtype, invalid
):
    leaf = pw.schema_builder(
        {"three": pw.column_definition(dtype=dtype, default_value=invalid)}
    )
    middle = pw.schema_from_types(two=leaf)
    schema = pw.schema_from_types(one=middle)
    with pytest.raises(ValueError, match="/one/two/three"):
        read_nested(schema, [])


@pytest.mark.parametrize("serializer", [None, pickle])
def test_nested_json_py_object_wrapper_access_and_defaults(read_nested, serializer):
    wrapped = pw.wrap_py_object({"number": 42}, serializer=serializer)
    encoded = base64.b64encode(api.serialize(wrapped)).decode()
    inner = pw.schema_builder(
        {
            "two": pw.column_definition(dtype=pw.PyObjectWrapper | None),
            "fallback": pw.column_definition(
                dtype=pw.PyObjectWrapper, default_value=wrapped
            ),
        }
    )
    table = read_nested(
        pw.schema_from_types(one=inner),
        [{"one": {"two": encoded}}, {"one": {"two": None}}],
    )
    decoded = table.select(two=table.one.two, fallback=table.one.fallback)
    assert decoded.schema["two"].dtype == dt.Optional(dt.ANY_PY_OBJECT_WRAPPER)

    @pw.udf
    def get_number(value: pw.PyObjectWrapper | None) -> int | None:
        return None if value is None else value.value["number"]

    result = decoded.select(
        two=get_number(decoded.two), fallback=get_number(decoded.fallback)
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(two=int | None, fallback=int | None),
            [(42, 42), (None, 42)],
        ),
    )


def test_nested_json_bad_encoded_input_is_an_error_value(read_nested):
    inner = pw.schema_from_types(two=bytes)
    table = read_nested(
        pw.schema_from_types(one=inner),
        [{"one": {"two": "%%%"}}, {"one": {"two": "AQI="}}],
    )
    result = table.remove_errors().select(two=pw.this.one.two)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(pw.schema_from_types(two=bytes), [(b"\x01\x02",)]),
        terminate_on_error=False,
    )


def test_nested_json_ndarray_supports_native_indexing_and_arithmetic(read_nested):
    table = read_nested(
        pw.schema_from_types(one=pw.schema_from_types(two=FLOAT_ARRAY)),
        [{"one": {"two": {"shape": [2, 2], "elements": [1, 2.5, 3, 4]}}}],
    )
    result = table.select(value=table.one.two[0][1] + 1)
    assert result.schema.typehints() == {"value": float}
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(pw.schema_from_types(value=float), [(3.5,)])
    )

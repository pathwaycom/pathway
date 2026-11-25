# Copyright © 2026 Pathway

"""Native Avro records/maps normalize to the nested JSON codecs.

bytes/fixed -> base64; logical timestamps/date -> ISO strings;
long/time-micros duration -> nanoseconds; arrays -> shape/elements.
Opaque JSON leaves keep the ordinary Avro structural conversion.
"""

import decimal
import doctest
import json
from typing import Any

import numpy as np
import pandas as pd
import pytest

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import assert_table_equality_wo_index, run_all

from .test_pulsar import publish_avro_datums
from .utils import PULSAR_SERVICE_URI

INT_ARRAY = np.ndarray[tuple[int], np.dtype[np.int64]]
FLOAT_ARRAY = np.ndarray[tuple[int, int], np.dtype[np.float64]]


def record(name, fields):
    return {
        "type": "record",
        "name": name,
        "fields": [{"name": key, "type": dtype} for key, dtype in fields.items()],
    }


def read(pulsar_context, schema):
    return pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="avro",
        schema=schema,
        mode="static",
    )


@pytest.mark.parametrize(
    "dtype,avro_type,value,expected",
    [
        (bool, "boolean", True, True),
        (int, "long", 2**63 - 1, 2**63 - 1),
        (float, "int", 42, 42.0),
        (float, "double", 1.5, 1.5),
        (str, "string", "hello π", "hello π"),
        (bytes, "bytes", b"\x00\xff", b"\x00\xff"),
        (bytes, "bytes", b"", b""),
        (bytes, {"type": "fixed", "name": "Binary", "size": 2}, b"ab", b"ab"),
        (
            str,
            {"type": "enum", "name": "State", "symbols": ["ready"]},
            "ready",
            "ready",
        ),
        (
            str,
            {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2},
            decimal.Decimal("1.25"),
            "1.25",
        ),
        (
            pw.DateTimeUtc,
            {"type": "long", "logicalType": "timestamp-millis"},
            123,
            pd.Timestamp("1970-01-01T00:00:00.123Z"),
        ),
        (
            pw.DateTimeUtc,
            {"type": "long", "logicalType": "timestamp-micros"},
            123456,
            pd.Timestamp("1970-01-01T00:00:00.123456Z"),
        ),
        (
            pw.DateTimeNaive,
            {"type": "long", "logicalType": "local-timestamp-micros"},
            123456,
            pd.Timestamp("1970-01-01T00:00:00.123456"),
        ),
        (
            pw.DateTimeNaive,
            {"type": "int", "logicalType": "date"},
            1,
            pd.Timestamp("1970-01-02"),
        ),
        (pw.Duration, "long", -123, pd.Timedelta(-123, unit="us")),
        (
            pw.Duration,
            {"type": "int", "logicalType": "time-millis"},
            123,
            pd.Timedelta(123, unit="ms"),
        ),
        (pw.Pointer, "string", str(api.ref_scalar(42)), api.ref_scalar(42)),
        (INT_ARRAY, {"type": "array", "items": "long"}, [1, -2], np.array([1, -2])),
        (
            INT_ARRAY,
            {"type": "array", "items": "long"},
            [],
            np.array([], dtype=np.int64),
        ),
        (
            FLOAT_ARRAY,
            {"type": "array", "items": {"type": "array", "items": "double"}},
            [[1, 2.5]],
            np.array([[1, 2.5]]),
        ),
        (np.ndarray[tuple[()], np.dtype[np.float64]], "double", 1.5, np.array(1.5)),
        (pw.Json, "string", '{"x":1}', pw.Json('{"x":1}')),
        (type(None), "null", None, None),
    ],
)
def test_nested_avro_native_leaves(pulsar_context, dtype, avro_type, value, expected):
    definition = record(
        "Row", {"one": record("Outer", {"two": record("Inner", {"three": avro_type})})}
    )
    publish_avro_datums(
        pulsar_context, definition, [{"one": {"two": {"three": value}}}]
    )
    schema = pw.schema_from_types(
        one=pw.schema_from_types(two=pw.schema_from_types(three=dtype))
    )
    schema = pw.schema_from_dict(
        **json.loads(json.dumps(schema.to_json_serializable_dict()))
    )
    table = read(pulsar_context, schema)
    projected = table.select(two=table.one.two)
    result = table.select(
        direct=table.one.two.three,
        bracket=table.one["two"]["three"],
        projected=projected.two.three,
        raw=table.one.two,
    )
    expected_schema = pw.schema_from_types(
        direct=dtype, bracket=dtype, projected=dtype, raw=pw.Json
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (
                    expected,
                    expected,
                    expected,
                    pw.Json({"three": json.loads(api.value_to_json(expected))}),
                )
            ],
        ),
    )


def test_nested_avro_maps_lists_tuples_defaults_and_errors(pulsar_context):
    class Leaf(pw.Schema):
        three: bytes
        fallback: bytes = pw.column_definition(default_value=b"ok")

    schema = pw.schema_from_types(
        number=int, one=list[Leaf | None], pair=tuple[Leaf, int]
    )
    definition = record(
        "Row",
        {
            "number": "int",
            "one": {
                "type": "array",
                "items": ["null", {"type": "map", "values": "bytes"}],
            },
            "pair": {
                "type": "array",
                "items": [{"type": "map", "values": "bytes"}, "int"],
            },
        },
    )
    publish_avro_datums(
        pulsar_context,
        definition,
        [
            {
                "number": 1,
                "one": [{"three": b"abc", "extra": b"x"}, None],
                "pair": [{"three": b"def"}, 7],
            },
            {"number": 2, "one": [{}], "pair": [{"three": b"def"}, 7]},
            {"number": 3, "one": [{"three": b"abc"}], "pair": [7, {"three": b"wrong"}]},
        ],
    )
    table = read(pulsar_context, schema).remove_errors()
    result = table.select(
        number=table.number,
        value=table.one[0].three,
        fallback=table.one[0].fallback,
        null=table.one[1].three,
        pair=table.pair[0].three,
        extra=table.one[0].extra,
    )
    expected_schema = pw.schema_from_types(
        number=int,
        value=bytes | None,
        fallback=bytes | None,
        null=bytes | None,
        pair=bytes,
        extra=pw.Json,
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema, [(1, b"abc", b"ok", None, b"def", pw.Json("eA=="))]
        ),
        terminate_on_error=False,
    )


def test_nested_avro_python_wrapper_and_default(pulsar_context):
    wrapped = pw.wrap_py_object({"answer": 42})

    class Leaf(pw.Schema):
        data: pw.PyObjectWrapper
        fallback: pw.PyObjectWrapper = pw.column_definition(default_value=wrapped)

    definition = record("Row", {"one": record("Inner", {"data": "bytes"})})
    publish_avro_datums(
        pulsar_context, definition, [{"one": {"data": api.serialize(wrapped)}}]
    )
    table = read(pulsar_context, pw.schema_from_types(one=Leaf))
    result = table.select(
        value=pw.apply_with_type(lambda x: x.value["answer"], int, table.one.data),
        fallback=pw.apply_with_type(
            lambda x: x.value["answer"], int, table.one.fallback
        ),
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(value=int, fallback=int), [(42, 42)]
        ),
    )


def test_nested_avro_writer_json_string_roundtrip(pulsar_context):
    class Leaf(pw.Schema):
        data: bytes
        array: FLOAT_ARRAY
        at: pw.DateTimeUtc

    definition = record(
        "Row",
        {
            "one": record(
                "Inner",
                {
                    "data": "bytes",
                    "array": {
                        "type": "array",
                        "items": {"type": "array", "items": "double"},
                    },
                    "at": {"type": "long", "logicalType": "timestamp-millis"},
                },
            )
        },
    )
    publish_avro_datums(
        pulsar_context,
        definition,
        [{"one": {"data": b"abc", "array": [[1, 2.5]], "at": 123}}],
    )
    schema = pw.schema_from_types(one=Leaf)
    table = read(pulsar_context, schema)
    output_topic = pulsar_context.topic + "-roundtrip"
    pw.io.pulsar.write(table, PULSAR_SERVICE_URI, output_topic, format="avro")
    run_all()
    G.clear()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI, output_topic, format="avro", schema=schema, mode="static"
    )
    result = table.select(data=table.one.data, array=table.one.array, at=table.one.at)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(data=bytes, array=FLOAT_ARRAY, at=pw.DateTimeUtc),
            [(b"abc", np.array([[1, 2.5]]), pd.Timestamp("1970-01-01T00:00:00.123Z"))],
        ),
    )


def test_nested_avro_empty_topic_and_invalid_default(pulsar_context):
    class Leaf(pw.Schema):
        price: float

    publish_avro_datums(
        pulsar_context, record("Row", {"one": record("Inner", {"price": "double"})}), []
    )
    table = read(pulsar_context, pw.schema_from_types(one=Leaf))
    assert_table_equality_wo_index(
        table.select(price=table.one.price),
        pw.debug.table_from_rows(pw.schema_from_types(price=float), []),
    )
    invalid = pw.schema_builder(
        {"price": pw.column_definition(dtype=float, default_value="bad")}
    )
    with pytest.raises(ValueError, match="/one/price"):
        read(pulsar_context, pw.schema_from_types(one=invalid))


def test_nested_avro_read_docstring(pulsar_context, monkeypatch):
    definition = record(
        "Row", {"payload": record("Payload", {"price": "double", "content": "bytes"})}
    )
    publish_avro_datums(
        pulsar_context,
        definition,
        [{"payload": {"price": 12.5, "content": b"\x01\x02"}}],
    )
    reader = pw.io.pulsar.read
    examples = doctest.DocTestParser().get_examples(reader.__doc__)
    start = next(
        i
        for i, example in enumerate(examples)
        if example.source.startswith("class Payload")
    )

    def read_test_topic(*args, **kwargs):
        return reader(PULSAR_SERVICE_URI, pulsar_context.topic, **kwargs)

    monkeypatch.setattr(pw.io.pulsar, "read", read_test_topic)
    namespace: dict[str, Any] = {"pw": pw, "__name__": __name__}
    for example in examples[start:]:
        exec(example.source, namespace)
        if example.source.startswith("result ="):
            break
    expected_schema = pw.schema_from_types(price=float, content=bytes, currency=str)
    assert namespace["result"].schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        namespace["result"],
        pw.debug.table_from_rows(expected_schema, [(12.5, b"\x01\x02", "USD")]),
    )


@pytest.mark.parametrize("whole_column", [False, True])
def test_nested_avro_schema_evolution_uses_pathway_defaults(
    pulsar_context, whole_column
):
    class Leaf(pw.Schema):
        content: bytes = pw.column_definition(default_value=b"pathway")

    schema = pw.schema_builder(
        {
            "number": pw.column_definition(dtype=int),
            "one": pw.column_definition(
                dtype=Leaf | None, default_value={"content": b"column"}
            ),
        }
    )
    old_row: dict[str, Any]
    if whole_column:
        old = record("Row", {"number": "int"})
        new = record(
            "Row",
            {"number": "int", "one": ["null", record("Inner", {"content": "bytes"})]},
        )
        new["fields"][1]["default"] = None
        old_row = {"number": 1}
        expected_old = b"column"
    else:
        old = record("Row", {"number": "int", "one": record("Inner", {})})
        inner = record("Inner", {"content": "bytes"})
        inner["fields"][0]["default"] = "writer"
        new = record("Row", {"number": "int", "one": inner})
        old_row = {"number": 1, "one": {}}
        expected_old = b"pathway"
    publish_avro_datums(pulsar_context, old, [old_row])
    publish_avro_datums(
        pulsar_context, new, [{"number": 2, "one": {"content": b"new"}}]
    )
    table = read(pulsar_context, schema)
    result = table.select(number=table.number, content=table.one.content)
    expected_schema = pw.schema_from_types(number=int, content=bytes | None)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(expected_schema, [(1, expected_old), (2, b"new")]),
    )


@pytest.mark.parametrize(
    "avro_type,value,dtype,expected",
    [
        ("boolean", True, bool, True),
        ("long", 42, int, 42),
        ("double", 1.25, float, 1.25),
        ("string", "hello", str, "hello"),
        ("bytes", b"\x00\xff", bytes, b"\x00\xff"),
        ({"type": "fixed", "name": "Binary", "size": 2}, b"ab", bytes, b"ab"),
        (
            {"type": "enum", "name": "State", "symbols": ["ready"]},
            "ready",
            str,
            "ready",
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2},
            decimal.Decimal("1.25"),
            str,
            "1.25",
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            123,
            pw.DateTimeUtc,
            pd.Timestamp(123, unit="us", tz="UTC"),
        ),
        (
            {"type": "long", "logicalType": "local-timestamp-micros"},
            123,
            pw.DateTimeNaive,
            pd.Timestamp(123, unit="us"),
        ),
        (
            {"type": "int", "logicalType": "date"},
            1,
            pw.DateTimeNaive,
            pd.Timestamp("1970-01-02"),
        ),
        (
            {"type": "long", "logicalType": "time-micros"},
            123,
            pw.Duration,
            pd.Timedelta(123, unit="us"),
        ),
        (["null", "double"], None, float | None, None),
        (["long", "string"], "42", pw.Json, pw.Json("42")),
        ("null", None, type(None), None),
    ],
)
def test_nested_avro_inferred_leaves(pulsar_context, avro_type, value, dtype, expected):
    definition = record(
        "Row", {"one": record("Outer", {"two": record("Inner", {"three": avro_type})})}
    )
    publish_avro_datums(
        pulsar_context, definition, [{"one": {"two": {"three": value}}}]
    )
    table = read(pulsar_context, None)
    restored = pw.schema_from_dict(
        **json.loads(json.dumps(table.schema.to_json_serializable_dict()))
    )
    assert restored["one"].json_schema == table.schema["one"].json_schema
    projected = table.select(two=table.one.two)
    assert projected.schema.typehints() == {"two": pw.Json}
    result = table.select(value=table.one.two.three, projected=projected.two.three)
    schema = pw.schema_from_types(value=dtype, projected=dtype)
    assert result.schema.typehints() == schema.typehints()
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(schema, [(expected, expected)])
    )


@pytest.mark.parametrize("nullable", [False, True])
def test_nested_avro_inferred_maps_and_arrays(pulsar_context, nullable):
    leaf = record("Leaf", {"price": "double", "content": "bytes"})
    definition = record(
        "Row",
        {
            "key": "string",
            "by_name": {"type": "map", "values": leaf},
            "items": {"type": "array", "items": "Leaf"},
        },
    )
    rows: list[dict[str, Any]] = [
        {
            "key": "eur",
            "by_name": {"eur": {"price": 12.5, "content": b"ok"}},
            "items": [{"price": 1.5, "content": b"item"}],
        }
    ]
    if nullable:
        for field in definition["fields"][1:]:
            field["type"] = ["null", field["type"]]
        rows.append({"key": "eur", "by_name": None, "items": None})
    publish_avro_datums(pulsar_context, definition, rows)
    table = read(pulsar_context, None)
    assert table.schema.typehints() == {
        "key": str,
        "by_name": pw.Json | None if nullable else pw.Json,
        "items": pw.Json | None if nullable else pw.Json,
    }
    assert table.select(value=table.by_name.eur).schema.typehints() == {
        "value": pw.Json | None
    }
    result = table.select(
        content=table.by_name[table.key].content,
        price=table.by_name.eur.price,
        missing=table.by_name.absent.price,
        item=table.items[0].price,
    )
    expected = pw.schema_from_types(
        content=bytes | None,
        price=float | None,
        missing=float | None,
        item=float | None,
    )
    assert result.schema.typehints() == expected.typehints()
    expected_rows: list[tuple[bytes | None, float | None, None, float | None]] = [
        (b"ok", 12.5, None, 1.5)
    ]
    if nullable:
        expected_rows.append((None, None, None, None))
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(expected, expected_rows)
    )


@pytest.mark.parametrize("whole_column", [False, True])
def test_nested_avro_inferred_evolution_defaults(pulsar_context, whole_column):
    leaf = record("Leaf", {"content": "bytes"})
    leaf["fields"][0]["default"] = "ÿ"
    new = record("Row", {"number": "long", "one": leaf})
    new["fields"][1]["default"] = {"content": "ÿ"}
    if whole_column:
        old = record("Row", {"number": "long"})
        old_row: dict[str, Any] = {"number": 1}
    else:
        old = record("Row", {"number": "long", "one": record("Leaf", {})})
        old_row = {"number": 1, "one": {}}
    publish_avro_datums(pulsar_context, old, [old_row])
    publish_avro_datums(
        pulsar_context, new, [{"number": 2, "one": {"content": b"new"}}]
    )
    table = read(pulsar_context, None)
    result = table.select(number=table.number, content=table.one.content)
    expected = pw.schema_from_types(number=int, content=bytes)
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(expected, [(1, b"\xff"), (2, b"new")])
    )


def test_nested_avro_inferred_recursive_record(pulsar_context):
    node = record("Node", {"value": "long", "next": ["null", "Node"]})
    node["fields"][1]["default"] = None
    publish_avro_datums(
        pulsar_context,
        record("Row", {"one": node}),
        [{"one": {"value": 1, "next": {"value": 2, "next": None}}}],
    )
    table = read(pulsar_context, None)
    result = table.select(
        value=table.one.value, next_value=table.one.next.value.as_int()
    )
    expected = pw.schema_from_types(value=int, next_value=int | None)
    assert result.schema.typehints() == expected.typehints()
    assert_table_equality_wo_index(result, pw.debug.table_from_rows(expected, [(1, 2)]))


def test_nested_json_registry_inference(pulsar_context):
    import pulsar

    definition = record(
        "Row",
        {
            "one": record(
                "Inner",
                {
                    "at": {"type": "long", "logicalType": "timestamp-micros"},
                    "by_name": {"type": "map", "values": "double"},
                },
            )
        },
    )

    class RawJsonSchema(pulsar.schema.Schema):
        def __init__(self):
            super().__init__(None, pulsar._pulsar.SchemaType.JSON, definition, "JSON")

        def encode(self, data):
            return json.dumps(data).encode()

        def decode(self, data):
            return json.loads(data)

    producer = pulsar_context._client.create_producer(
        pulsar_context.topic, schema=RawJsonSchema()
    )
    try:
        producer.send({"one": {"at": 123, "by_name": {"USD": 1.5}}})
    finally:
        producer.close()
    table = pw.io.pulsar.read(
        PULSAR_SERVICE_URI,
        pulsar_context.topic,
        format="json",
        schema=None,
        mode="static",
    )
    result = table.select(at=table.one.at, price=table.one.by_name.USD)
    expected = pw.schema_from_types(at=int, price=float | None)
    assert result.schema.typehints() == expected.typehints()
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(expected, [(123, 1.5)])
    )


def test_nested_avro_inferred_read_docstring(pulsar_context, monkeypatch):
    publish_avro_datums(
        pulsar_context,
        record(
            "Row",
            {"payload": record("Payload", {"price": "double", "content": "bytes"})},
        ),
        [{"payload": {"price": 12.5, "content": b"ok"}}],
    )
    reader = pw.io.pulsar.read
    examples = doctest.DocTestParser().get_examples(reader.__doc__)
    start = next(
        i
        for i, example in enumerate(examples)
        if example.source.startswith("inferred =")
    )

    def read_test_topic(*args, **kwargs):
        return reader(PULSAR_SERVICE_URI, pulsar_context.topic, **kwargs)

    monkeypatch.setattr(pw.io.pulsar, "read", read_test_topic)
    namespace: dict[str, Any] = {"pw": pw}
    for example in examples[start:]:
        exec(example.source, namespace)
        if example.source.startswith("inferred_result ="):
            break
    expected = pw.schema_from_types(price=float, content=bytes)
    assert namespace["inferred_result"].schema.typehints() == expected.typehints()
    assert_table_equality_wo_index(
        namespace["inferred_result"],
        pw.debug.table_from_rows(expected, [(12.5, b"ok")]),
    )

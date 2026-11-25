# Copyright © 2026 Pathway

"""Native BSON leaves normalize to the JSON codec used by nested field access.

Binary -> base64; DateTime -> timestamp string; Int64 duration milliseconds ->
nanoseconds; numeric arrays -> shape/elements; ObjectId/Decimal128/Regex -> str;
Timestamp -> seconds. JSON strings written by Pathway use the JSON codec directly.
"""

import datetime
import doctest
import json
import pickle
from typing import Any

import numpy as np
import pandas as pd
import pytest
from bson import Binary, Decimal128, Int64, ObjectId, Regex, Timestamp
from utils import MONGODB_BASE_NAME, MONGODB_CONNECTION_STRING

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    assert_table_equality_wo_index,
    run_all,
    wait_result_with_checker,
)

pytestmark = pytest.mark.xdist_group("mongodb")

INT_ARRAY = np.ndarray[tuple[int], np.dtype[np.int64]]
FLOAT_ARRAY = np.ndarray[tuple[int, int], np.dtype[np.float64]]
OID = ObjectId("507f1f77bcf86cd799439011")


def read_documents(mongodb, schema, documents):
    collection = mongodb.generate_collection_name()
    if documents:
        mongodb.client[MONGODB_BASE_NAME][collection].insert_many(documents)
    return read_collection(schema, collection)


def read_collection(schema, collection):
    return pw.io.mongodb.read(
        MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=collection,
        schema=schema,
        mode="static",
    )


@pytest.mark.parametrize(
    "dtype,native,expected",
    [
        pytest.param(bool, True, True, id="bool"),
        pytest.param(int, 42, 42, id="int32"),
        pytest.param(int, Int64(2**63 - 1), 2**63 - 1, id="int64"),
        pytest.param(float, 42, 42.0, id="int-to-float"),
        pytest.param(float, 1.5, 1.5, id="float"),
        pytest.param(str, "hello π", "hello π", id="string"),
        pytest.param(bytes, Binary(b"\x00\x01\xff"), b"\x00\x01\xff", id="bytes"),
        pytest.param(bytes, Binary(b""), b"", id="empty-bytes"),
        pytest.param(bytes, Binary(b"abc", subtype=128), b"abc", id="binary-subtype"),
        pytest.param(
            pw.DateTimeNaive,
            datetime.datetime(2026, 11, 6, 12, 30, 0, 123000),
            pd.Timestamp("2026-11-06T12:30:00.123"),
            id="naive",
        ),
        pytest.param(
            pw.DateTimeUtc,
            datetime.datetime(2026, 11, 6, 12, 30, tzinfo=datetime.timezone.utc),
            pd.Timestamp("2026-11-06T12:30:00Z"),
            id="utc",
        ),
        pytest.param(
            pw.Duration, Int64(-1234), pd.Timedelta(-1234, unit="ms"), id="duration"
        ),
        pytest.param(
            pw.Pointer, str(api.ref_scalar(42)), api.ref_scalar(42), id="pointer"
        ),
        pytest.param(INT_ARRAY, [1, -2, 3], np.array([1, -2, 3]), id="int-array"),
        pytest.param(
            FLOAT_ARRAY,
            [[1, 2.5], [3, 4]],
            np.array([[1, 2.5], [3, 4]]),
            id="float-array",
        ),
        pytest.param(INT_ARRAY, [], np.array([], dtype=np.int64), id="empty-array"),
        pytest.param(INT_ARRAY, 7, np.array([7]), id="scalar-as-vector"),
        pytest.param(
            np.ndarray[None, np.dtype[np.int64]],
            [[[1, 2]]],
            np.array([[[1, 2]]]),
            id="unknown-rank",
        ),
        pytest.param(
            np.ndarray[tuple[()], np.dtype[np.float64]],
            1.5,
            np.array(1.5),
            id="zero-rank",
        ),
        pytest.param(str, OID, str(OID), id="object-id"),
        pytest.param(str, Decimal128("1.25"), "1.25", id="decimal"),
        pytest.param(str, Regex("abc", "i"), "/abc/i", id="regex"),
        pytest.param(int, Timestamp(123, 7), 123, id="timestamp"),
        pytest.param(
            pw.Json, {"x": [1, None]}, pw.Json({"x": [1, None]}), id="opaque-json"
        ),
        pytest.param(pw.Json, '{"x": 1}', pw.Json('{"x": 1}'), id="opaque-json-string"),
        pytest.param(type(None), None, None, id="null"),
    ],
)
def test_nested_bson_native_leaves(mongodb, dtype, native, expected):
    inner = pw.schema_from_types(three=dtype)
    schema = pw.schema_from_types(one=pw.schema_from_types(two=inner))
    schema = pw.schema_from_dict(
        **json.loads(json.dumps(schema.to_json_serializable_dict()))
    )
    table = read_documents(mongodb, schema, [{"one": {"two": {"three": native}}}])
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


def test_nested_bson_optional_lists_tuples_defaults_and_extra_fields(mongodb):
    class Leaf(pw.Schema):
        three: bytes | None
        fallback: bytes = pw.column_definition(default_value=b"ok")

    object_ = pw.schema_from_types(two=Leaf, items=list[Leaf], pair=tuple[Leaf, str])
    schema = pw.schema_from_types(number=int, one=object_ | None)
    table = read_documents(
        mongodb,
        schema,
        [
            {
                "number": 1,
                "one": {
                    "two": {"three": Binary(b"abc")},
                    "items": [{"three": None}],
                    "pair": [{"three": Binary(b"def")}, "x"],
                    "extra": {"keep": True},
                },
            },
            {"number": 2, "one": None},
        ],
    )
    result = table.select(
        number=table.number,
        direct=table.one.two.three,
        fallback=table.one.two.fallback,
        item=table.one.items[0].three,
        missing=table.one.items[1].three,
        pair=table.one.pair[0].three,
        extra=table.one.extra,
    )
    expected_schema = pw.schema_from_types(
        number=int,
        direct=bytes | None,
        fallback=bytes | None,
        item=bytes | None,
        missing=bytes | None,
        pair=bytes | None,
        extra=pw.Json,
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (1, b"abc", b"ok", None, None, b"def", pw.Json({"keep": True})),
                (2, None, None, None, None, None, pw.Json.NULL),
            ],
        ),
    )


@pytest.mark.parametrize("serializer", [None, pickle])
def test_nested_bson_wrappers_and_defaults(mongodb, serializer):
    wrapped = pw.wrap_py_object({"number": 42}, serializer=serializer)
    leaf = pw.schema_builder(
        {
            "three": pw.column_definition(dtype=pw.PyObjectWrapper),
            "fallback": pw.column_definition(
                dtype=pw.PyObjectWrapper, default_value=wrapped
            ),
        }
    )
    schema = pw.schema_from_types(one=leaf)
    table = read_documents(
        mongodb, schema, [{"one": {"three": Binary(api.serialize(wrapped))}}]
    )
    result = table.select(
        three=pw.apply_with_type(lambda x: x.value["number"], int, table.one.three),
        fallback=pw.apply_with_type(
            lambda x: x.value["number"], int, table.one.fallback
        ),
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(three=int, fallback=int), [(42, 42)]
        ),
    )


def test_nested_bson_column_defaults_and_errors(mongodb):
    leaf = pw.schema_from_types(three=bytes)
    schema = pw.schema_builder(
        {
            "number": pw.column_definition(dtype=int),
            "one": pw.column_definition(
                dtype=leaf | None, default_value={"three": b"ok"}
            ),
        }
    )
    table = read_documents(
        mongodb,
        schema,
        [
            {"number": 1},
            {"number": 2, "one": None},
            {"number": 3, "one": {"three": "base64-is-not-BSON-binary"}},
            {"number": 4, "one": {}},
            {"number": 5, "one": "{"},
        ],
    )
    result = table.remove_errors().select(
        number=pw.this.number, value=pw.this.one.three
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(number=int, value=bytes | None),
            [(1, b"ok"), (2, None)],
        ),
        terminate_on_error=False,
    )


def test_nested_bson_writer_roundtrip(mongodb):
    schema = pw.schema_from_types(
        one=pw.schema_from_types(data=bytes, array=FLOAT_ARRAY, at=pw.DateTimeUtc)
    )
    table = read_documents(
        mongodb,
        schema,
        [
            {
                "one": {
                    "data": Binary(b"abc"),
                    "array": [[1, 2], [3, 4]],
                    "at": datetime.datetime(2026, 11, 6),
                }
            }
        ],
    )
    collection = mongodb.generate_collection_name()
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=collection,
        output_table_type="snapshot",
    )
    run_all()
    stored = mongodb.get_full_collection(collection)
    assert len(stored) == 1 and isinstance(stored[0]["one"], str)
    assert json.loads(stored[0]["one"])["data"] == "YWJj"
    G.clear()
    table = read_collection(schema, collection)
    result = table.select(data=table.one.data, array=table.one.array, at=table.one.at)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(data=bytes, array=FLOAT_ARRAY, at=pw.DateTimeUtc),
            [
                (
                    b"abc",
                    np.array([[1.0, 2.0], [3.0, 4.0]]),
                    pd.Timestamp("2026-11-06T00:00:00Z"),
                )
            ],
        ),
    )


def test_nested_bson_empty_collection(mongodb):
    table = read_documents(
        mongodb, pw.schema_from_types(one=pw.schema_from_types(two=float)), []
    )
    assert_table_equality_wo_index(
        table.select(value=table.one.two),
        pw.debug.table_from_rows(pw.schema_from_types(value=float), []),
    )


def test_nested_bson_opaque_and_extra_fields_keep_plain_json_conversion(mongodb):
    payload = {
        "binary": Binary(b"abc"),
        "oid": OID,
        "at": datetime.datetime(2026, 11, 6),
    }
    schema = pw.schema_from_types(
        plain=pw.Json,
        missing=pw.Json | None,
        typed=pw.schema_from_types(opaque=pw.Json, number=int),
    )
    table = read_documents(
        mongodb,
        schema,
        [
            {
                "plain": payload,
                "typed": {"opaque": payload, "extra": payload, "number": 1},
            }
        ],
    )
    result = table.select(
        opaque=table.typed.opaque == table.plain,
        extra=table.typed.extra == table.plain,
        missing=table.missing,
    )
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(opaque=bool, extra=bool, missing=pw.Json | None),
            [(True, True, None)],
        ),
    )


def test_nested_bson_invalid_default_is_rejected_before_reading():
    leaf = pw.schema_builder(
        {"two": pw.column_definition(dtype=bytes, default_value="%%%")}
    )
    with pytest.raises(ValueError, match="/one/two"):
        read_collection(pw.schema_from_types(one=leaf), "unused")


def test_nested_bson_read_docstring_example(mongodb, monkeypatch):
    reader = pw.io.mongodb.read
    collection = mongodb.generate_collection_name()
    mongodb.client[MONGODB_BASE_NAME][collection].insert_one(
        {"payload": {"price": 12.5, "content": Binary(b"\x01\x02")}}
    )
    examples = doctest.DocTestParser().get_examples(reader.__doc__)
    start = next(
        i
        for i, example in enumerate(examples)
        if example.source.startswith("class Payload")
    )

    def read_from_test_collection(*args, **kwargs):
        kwargs.update(database=MONGODB_BASE_NAME, collection=collection)
        return reader(MONGODB_CONNECTION_STRING, **kwargs)

    monkeypatch.setattr(pw.io.mongodb, "read", read_from_test_collection)
    namespace: dict[str, Any] = {"pw": pw, "__name__": __name__}
    for example in examples[start:]:
        exec(example.source, namespace)
        if example.source.startswith("result ="):
            break
    schema = pw.schema_from_types(price=float, content=bytes, currency=str)
    assert namespace["result"].schema.typehints() == schema.typehints()
    assert_table_equality_wo_index(
        namespace["result"],
        pw.debug.table_from_rows(schema, [(12.5, b"\x01\x02", "USD")]),
    )


def test_nested_bson_json_text_keeps_empty_array_shape(mongodb):
    table = read_documents(
        mongodb,
        pw.schema_from_types(one=pw.schema_from_types(two=FLOAT_ARRAY)),
        [
            {"one": '{"two": {"shape": [0, 3], "elements": []}}'},
        ],
    )
    assert_table_equality_wo_index(
        table.select(value=table.one.two),
        pw.debug.table_from_rows(
            pw.schema_from_types(value=FLOAT_ARRAY),
            [(np.empty((0, 3)),)],
        ),
    )


def test_nested_bson_streaming_updates_and_deletes(mongodb):
    from test_mongodb_parsing import (
        MongoDBRowCountChecker,
        _start_mongodb_stream_worker,
    )

    input_collection = mongodb.generate_collection_name()
    output_collection = mongodb.generate_collection_name()
    source = mongodb.client[MONGODB_BASE_NAME][input_collection]
    source.insert_one({"_id": "row", "one": {"two": Binary(b"abc")}})
    worker = _start_mongodb_stream_worker(
        input_collection,
        output_collection,
        """
class Leaf(pw.Schema):
    two: bytes
class InputSchemaWithPkey(pw.Schema):
    one: Leaf
""",
    )
    try:
        for count in [1, 3, 4]:
            wait_result_with_checker(
                MongoDBRowCountChecker(mongodb, output_collection, count, worker),
                90,
                target=None,
            )
            if count == 1:
                source.replace_one(
                    {"_id": "row"}, {"_id": "row", "one": {"two": Binary(b"def")}}
                )
            elif count == 3:
                source.delete_one({"_id": "row"})
        rows = mongodb.get_full_collection(output_collection)
        assert sorted((json.loads(row["one"])["two"], row["diff"]) for row in rows) == [
            ("YWJj", -1),
            ("YWJj", 1),
            ("ZGVm", -1),
            ("ZGVm", 1),
        ]
    finally:
        worker.terminate()

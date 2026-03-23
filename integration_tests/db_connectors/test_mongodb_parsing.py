from typing import Any

import pandas as pd
import pytest
from utils import (
    MONGODB_BASE_NAME,
    MONGODB_CONNECTION_STRING,
    MongoDBContext,
    SimpleObject,
    _compare_input_and_output,
    _make_type_check_observer,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import ExceptionAwareThread, run, wait_result_with_checker

pytestmark = pytest.mark.xdist_group("mongodb")


class MongoDBRowCountChecker:

    def __init__(
        self, mongodb: MongoDBContext, collection_name: str, n_rows_expected: int
    ):
        self.mongodb = mongodb
        self.collection_name = collection_name
        self.n_rows_expected = n_rows_expected

    def __call__(self) -> bool:
        try:
            rows = self.mongodb.get_full_collection(self.collection_name)
        except Exception as e:
            print("Exception:", e)
            return False
        print(f"Rows seen: {len(rows)} Rows expected: {self.n_rows_expected}")
        return len(rows) == self.n_rows_expected


def _create_table(ItemType: type, input_rows: list[dict]) -> pw.Table:
    class InputSchema(pw.Schema):
        pkey: int
        item: ItemType  # type: ignore

    return pw.debug.table_from_rows(
        InputSchema,
        [tuple(row.values()) for row in input_rows],
    )


def _test_mongodb_static(
    mongodb, ItemType: type, items: list[Any], create_table: Any = _create_table
) -> str:
    input_rows = []
    for index, item in enumerate(items):
        input_rows.append(
            {
                "pkey": index,
                "item": item,
            }
        )

    input_table_name = mongodb.generate_collection_name()
    table = create_table(ItemType, input_rows)
    InputSchemaWithPkey = table.schema

    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=input_table_name,
        output_table_type="stream_of_changes",
    )
    run()

    G.clear()
    output_table_name = mongodb.generate_collection_name()
    table = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=input_table_name,
        schema=InputSchemaWithPkey,
        mode="static",
    )
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=output_table_name,
        output_table_type="stream_of_changes",
    )
    observer, type_errors = _make_type_check_observer(ItemType)
    pw.io.python.write(table, observer)
    run()

    assert not type_errors, "\n".join(type_errors)

    output_rows = mongodb.get_collection(
        output_table_name, InputSchemaWithPkey.column_names()
    )
    _compare_input_and_output(
        ItemType,
        input_rows,
        output_rows,
        timestamp_precision=1000000,
        timezone_supported=False,
    )
    return input_table_name


def _test_mongodb_streaming(
    mongodb,
    ItemType: type,
    items: list[Any],
    create_table: Any = _create_table,
):
    input_table_name = _test_mongodb_static(mongodb, ItemType, items, create_table)
    output_table_name = mongodb.generate_collection_name()

    class InputSchemaWithPkey(pw.Schema):
        pkey: int
        item: ItemType  # type: ignore

    input_rows = []
    for index, item in enumerate(items):
        input_rows.append(
            {
                "pkey": len(items) + index,
                "item": item,
            }
        )

    def streaming_target():
        for index, row in enumerate(input_rows):
            wait_result_with_checker(
                MongoDBRowCountChecker(
                    mongodb, input_table_name, index + len(input_rows)
                ),
                30,
                target=None,
            )

            G.clear()
            table = create_table(ItemType, [row])
            pw.io.mongodb.write(
                table,
                connection_string=MONGODB_CONNECTION_STRING,
                database=MONGODB_BASE_NAME,
                collection=input_table_name,
                output_table_type="stream_of_changes",
            )
            run()

    streaming_thread = ExceptionAwareThread(target=streaming_target, daemon=True)
    streaming_thread.start()

    G.clear()
    table = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=input_table_name,
        schema=InputSchemaWithPkey,
        mode="streaming",
    )
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=output_table_name,
        output_table_type="stream_of_changes",
    )
    observer, type_errors = _make_type_check_observer(ItemType)
    pw.io.python.write(table, observer)

    # Fails with fork
    pathway_thread = ExceptionAwareThread(target=run, daemon=True)
    pathway_thread.start()

    wait_result_with_checker(
        MongoDBRowCountChecker(mongodb, output_table_name, 2 * len(input_rows)),
        60,
        target=None,
    )
    streaming_thread.join()

    assert not type_errors, "\n".join(type_errors)

    output_rows = mongodb.get_full_collection(output_table_name)

    # add the initial snapshot items for comparison
    for index, item in enumerate(items):
        input_rows.append({"pkey": index, "item": item})
    input_rows.sort(key=lambda i: i["pkey"])
    assert len(input_rows) == len(output_rows)
    _compare_input_and_output(
        ItemType,
        input_rows,
        output_rows,
        timestamp_precision=1000000,
        timezone_supported=False,
    )


def test_bool(mongodb):
    _test_mongodb_streaming(mongodb, bool, [True, False, True])


def test_int(mongodb):
    _test_mongodb_streaming(mongodb, int, [-1, 0, 1])


def test_int_boundary(mongodb):
    _test_mongodb_streaming(
        mongodb,
        int,
        [-(2**63), 2**63 - 1, 2**31 - 1, -(2**31)],
    )


def test_float(mongodb):
    _test_mongodb_streaming(mongodb, float, [-1.5, 0.0, 3.14])


def test_str(mongodb):
    _test_mongodb_streaming(mongodb, str, ["hello", "", "world"])


def test_str_unicode(mongodb):
    _test_mongodb_streaming(
        mongodb,
        str,
        [
            "привет 世界 🔥",
            "line\nnewline\ttab",
            "quote'semicolon;null,./;'l][p!@#!@#!@#%$#$#]",
            "a" * 10_000,
        ],
    )


def test_bytes(mongodb):
    _test_mongodb_streaming(mongodb, bytes, [b"\xde\xad\xbe\xef", b"", b"\x00\xff"])


def test_pointer(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.Pointer,
        [api.ref_scalar(42), api.ref_scalar(43)],
    )


def test_json(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.Json,
        [
            pw.Json({"key": "value"}),
            pw.Json([1, 2, 3]),
            pw.Json(42),
        ],
    )


def test_json_nested(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.Json,
        [
            pw.Json({"nested": {"a": 1, "b": [1, 2, 3]}}),
            pw.Json({"key": "val,with,commas"}),
            pw.Json({}),
        ],
    )


def test_duration(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.Duration,
        [
            pd.Timedelta("0"),
            pd.Timedelta("1 days 2 seconds 3 us"),
            pd.Timedelta("-1 days"),
        ],
    )


def test_duration_negative_mixed(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.Duration,
        [
            pd.Timedelta("-1 days 2 hours 3 minutes"),
            pd.Timedelta("-2 days 30 minutes"),
        ],
    )


def test_duration_large(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.Duration,
        [pd.Timedelta(days=365 * 100)],
    )


def test_datetime_naive(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.DateTimeNaive,
        [
            pw.DateTimeNaive("2025-01-01T00:00:00"),
            pw.DateTimeNaive("2025-03-14T10:13:00.123000000"),
        ],
    )


def test_datetime_utc(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.DateTimeUtc,
        [
            pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
            pw.DateTimeUtc("2025-03-14T10:13:00.123000000+00:00"),
        ],
    )


def test_bool_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[bool | None],
        [[True, False], [True, None], [None, False]],
    )


def test_int_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[int | None],
        [[1, 2], [3, None], [None, 4]],
    )


def test_float_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[float | None],
        [[1.1, 2.2], [3.3, None], [None, 4.4]],
    )


def test_str_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[str | None],
        [["a", "b"], ["c", None], [None, "d"]],
    )


def test_str_array_with_backslash_and_quotes(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[str | None],
        [
            ['say "hello"', "path\\to\\file"],
            ['mixed \\"escaped\\"', None],
            ["C:\\Users\\test", 'quote"then\\slash'],
        ],
    )


def test_bytes_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[bytes],
        [[b"\xde\xad", b"\xbe\xef"], [b"\xab\xcd", b"\xef\x01"]],
    )


def test_pointer_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[pw.Pointer | None],
        [[api.ref_scalar(42), api.ref_scalar(43)], [None, None]],
    )


def test_json_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[pw.Json | None],
        [
            [pw.Json({"a": 1}), pw.Json([1, 2, 3])],
            [pw.Json("string"), None],
            [None, pw.Json(42)],
        ],
    )


def test_json_array_with_commas_in_values(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[pw.Json | None],
        [
            [pw.Json({"a": 1, "b": 2, "c": 3}), pw.Json({"x": [1, 2, 3]})],
            [pw.Json({"nested": {"key": "val,with,commas"}}), None],
        ],
    )


def test_duration_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[pw.Duration | None],
        [
            [pd.Timedelta("0"), pd.Timedelta("1 days 2 seconds 3 us")],
            [pd.Timedelta("-1 days"), None],
            [None, pd.Timedelta("3 days 4 hours")],
        ],
    )


def test_2d_bool_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[list[bool]],
        [
            [[True, False], [False, True]],
            [[True, True], [False, False]],
        ],
    )


def test_2d_int_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[list[int]],
        [
            [[1, 2], [3, 4]],
            [[5, 6], [7, 8]],
        ],
    )


def test_2d_float_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[list[float]],
        [
            [[1.1, 2.2], [3.3, 4.4]],
            [[5.5, 6.6], [7.7, 8.8]],
        ],
    )


def test_2d_str_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[list[str]],
        [
            [["a", "b"], ["c", "d"]],
            [["w", "x"], ["y", "z"]],
        ],
    )


def test_2d_duration_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[list[pw.Duration]],
        [
            [
                [pd.Timedelta("0"), pd.Timedelta("1 days")],
                [
                    pd.Timedelta("2 days 3 seconds"),
                    pd.Timedelta("4 days 5 microseconds"),
                ],
            ],
            [
                [pd.Timedelta("-1 days"), pd.Timedelta("3 hours")],
                [pd.Timedelta("7 days"), pd.Timedelta("10 days 12 seconds")],
            ],
        ],
    )


def test_2d_json_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[list[pw.Json]],
        [
            [
                [pw.Json({"x": 1}), pw.Json({"y": 2})],
                [pw.Json([1, 2]), pw.Json([3, 4])],
            ],
            [
                [pw.Json("a"), pw.Json("b")],
                [pw.Json(True), pw.Json(False)],
            ],
        ],
    )


def test_empty_list(mongodb):
    _test_mongodb_streaming(mongodb, list[int], [[]])


def test_empty_str_list(mongodb):
    _test_mongodb_streaming(mongodb, list[str | None], [[None, None]])


def test_pyobject_wrapper(mongodb):
    _test_mongodb_streaming(
        mongodb,
        pw.PyObjectWrapper[SimpleObject],
        [
            pw.wrap_py_object(SimpleObject(1)),
            pw.wrap_py_object(SimpleObject(2)),
            pw.wrap_py_object(SimpleObject(3)),
        ],
    )


def test_pyobject_wrapper_array(mongodb):
    _test_mongodb_streaming(
        mongodb,
        list[pw.PyObjectWrapper[SimpleObject]],
        [
            [pw.wrap_py_object(SimpleObject(1)), pw.wrap_py_object(SimpleObject(2))],
            [pw.wrap_py_object(SimpleObject(3)), pw.wrap_py_object(SimpleObject(4))],
        ],
    )

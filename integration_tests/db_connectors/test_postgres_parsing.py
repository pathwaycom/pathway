from typing import Any, get_args

import numpy as np
import pandas as pd
import pytest
from utils import POSTGRES_SETTINGS, SimpleObject, _make_type_check_observer

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import ExceptionAwareThread, run, wait_result_with_checker

pytestmark = pytest.mark.xdist_group("postgres")


class PostgresRowCountChecker:

    def __init__(self, postgres, table_name: str, n_rows_expected: int):
        self.postgres = postgres
        self.table_name = table_name
        self.n_rows_expected = n_rows_expected

    def __call__(self):
        try:
            rows = self.postgres.get_table_contents(self.table_name, ["pkey", "item"])
        except Exception:
            return False
        return len(rows) == self.n_rows_expected


def _compare_input_and_output(
    ItemType: type, input_rows: list[dict], output_rows: list[dict]
):
    def normalize_input(value):
        if isinstance(value, pw.Pointer):
            return str(value)
        if isinstance(value, pw.Json):
            return value.value
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, pd.Timedelta):
            return value.value // 1000
        if isinstance(value, np.ndarray):
            return normalize_input(value.tolist())
        if hasattr(value, "_create_with_serializer"):
            return value.value
        if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict)):
            return [normalize_input(v) for v in value]
        return value

    def normalize_output(value, ItemType: type):
        if hasattr(ItemType, "_create_with_serializer"):
            value = api.deserialize(value)
            assert isinstance(
                value, pw.PyObjectWrapper
            ), f"expecting PyObjectWrapper, got {type(value)}"
            return value.value
        if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict)):
            args = get_args(ItemType)
            nested_arg = None
            for arg in args:
                if arg is not None:
                    nested_arg = arg
                    break
            return [normalize_output(v, nested_arg) for v in value]  # type: ignore
        return value

    output_rows.sort(key=lambda item: item["pkey"])

    for input_row in input_rows:
        input_row["item"] = normalize_input(input_row["item"])

    for output_row in output_rows:
        output_row["item"] = normalize_output(output_row["item"], ItemType)

    assert output_rows == input_rows


def _create_table(ItemType: type, input_rows: list[Any]) -> pw.Table:

    class InputSchemaWithPkey(pw.Schema):
        pkey: int = pw.column_definition(primary_key=True)
        item: ItemType  # type: ignore

    table = pw.debug.table_from_rows(
        InputSchemaWithPkey,
        [tuple(row.values()) for row in input_rows],
    )

    return table


def _create_ndarray_table(ItemType: type, input_rows: list[Any]) -> pw.Table:
    class InputSchemaWithPkey(pw.Schema):
        pkey: int = pw.column_definition(primary_key=True)
        item: Any

    table = pw.debug.table_from_rows(
        InputSchemaWithPkey,
        [tuple(row.values()) for row in input_rows],
    ).update_types(item=ItemType)

    return table


def _test_postgres_streaming(
    postgres, ItemType: type, items: list[Any], create_table: Any = _create_table
):
    input_table_name = _test_postgres_static(postgres, ItemType, items, create_table)
    output_table_name = postgres.random_table_name()

    class InputSchemaWithPkey(pw.Schema):
        pkey: int = pw.column_definition(primary_key=True)
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
                PostgresRowCountChecker(
                    postgres, output_table_name, index + len(input_rows)
                ),
                30,
                target=None,
            )

            G.clear()
            table = create_table(ItemType, [row])
            pw.io.postgres.write(table, POSTGRES_SETTINGS, input_table_name)
            run()

    streaming_thread = ExceptionAwareThread(target=streaming_target, daemon=True)
    streaming_thread.start()

    with postgres.publication(input_table_name) as publication_name:
        table = pw.io.postgres.read(
            POSTGRES_SETTINGS,
            input_table_name,
            InputSchemaWithPkey,
            mode="streaming",
            is_append_only=True,
            publication_name=publication_name,
        )
        pw.io.postgres.write(
            table,
            POSTGRES_SETTINGS,
            output_table_name,
            init_mode="create_if_not_exists",
        )
        observer, type_errors = _make_type_check_observer(ItemType)
        pw.io.python.write(table, observer)
        wait_result_with_checker(
            PostgresRowCountChecker(postgres, output_table_name, 2 * len(items)), 60
        )
        streaming_thread.join()

        assert not type_errors, "\n".join(type_errors)

        output_rows = postgres.get_table_contents(
            output_table_name, InputSchemaWithPkey.column_names()
        )

        # add the initial snapshot items for comparison
        for index, item in enumerate(items):
            input_rows.append(
                {
                    "pkey": index,
                    "item": item,
                }
            )
        input_rows.sort(key=lambda i: i["pkey"])
        _compare_input_and_output(ItemType, input_rows, output_rows)


def _test_postgres_static(
    postgres, ItemType: type, items: list[Any], create_table: Any = _create_table
) -> str:
    input_rows = []
    for index, item in enumerate(items):
        input_rows.append(
            {
                "pkey": index,
                "item": item,
            }
        )

    input_table_name = postgres.random_table_name()
    table = create_table(ItemType, input_rows)
    InputSchemaWithPkey = table.schema

    pw.io.postgres.write(
        table, POSTGRES_SETTINGS, input_table_name, init_mode="create_if_not_exists"
    )
    run()

    G.clear()
    output_table_name = postgres.random_table_name()
    table = pw.io.postgres.read(
        POSTGRES_SETTINGS, input_table_name, InputSchemaWithPkey, mode="static"
    )
    pw.io.postgres.write(
        table, POSTGRES_SETTINGS, output_table_name, init_mode="create_if_not_exists"
    )

    observer, type_errors = _make_type_check_observer(ItemType)
    pw.io.python.write(table, observer)
    run()

    assert not type_errors, "\n".join(type_errors)

    output_rows = postgres.get_table_contents(
        output_table_name, InputSchemaWithPkey.column_names()
    )
    _compare_input_and_output(ItemType, input_rows, output_rows)
    return input_table_name


def test_empty_structures(postgres):
    _test_postgres_streaming(postgres, list[int], [[]])
    _test_postgres_streaming(postgres, list[str | None], [[None, None]])
    _test_postgres_streaming(
        postgres,
        np.ndarray[None, float],  # type: ignore
        [np.array([], dtype=float)],
        _create_ndarray_table,
    )


def test_large_nested_array(postgres):
    large_array = np.arange(64, dtype=int).reshape(2, 2, 2, 2, 2, 2)

    _test_postgres_streaming(
        postgres,
        np.ndarray[None, int],  # type: ignore
        [large_array],
        _create_ndarray_table,
    )


def test_int_boundary(postgres):
    _test_postgres_streaming(
        postgres,
        int,
        [
            -(2**63),
            2**63 - 1,
            2**31 - 1,
            -(2**31),
        ],
    )


def test_str_unicode(postgres):
    _test_postgres_streaming(
        postgres,
        str,
        [
            "привет 世界 🔥",
            "line\nnewline\ttab",
            "quote'semicolon;null,./;'l][p!@#!@#!@#%$#$#]",
            "a" * 10_000,
        ],
    )


def test_duration_negative_mixed(postgres):
    _test_postgres_streaming(
        postgres,
        pw.Duration,
        [
            pd.Timedelta("-1 days 2 hours 3 minutes"),
            pd.Timedelta("-2 days 30 minutes"),
        ],
    )


def test_tuple_single_element(postgres):
    _test_postgres_streaming(
        postgres,
        tuple[int],
        [(42,), (0,), (-1,)],
    )


def test_json_array_with_commas_in_values(postgres):
    _test_postgres_streaming(
        postgres,
        list[pw.Json | None],
        [
            [pw.Json({"a": 1, "b": 2, "c": 3}), pw.Json({"x": [1, 2, 3]})],
            [pw.Json({"nested": {"key": "val,with,commas"}}), None],
        ],
    )


def test_str_array_with_backslash_and_quotes(postgres):
    _test_postgres_streaming(
        postgres,
        list[str | None],
        [
            ['say "hello"', "path\\to\\file"],
            ['mixed \\"escaped\\"', None],
            ["C:\\Users\\test", 'quote"then\\slash'],
        ],
    )


def test_duration_large_months(postgres):
    _test_postgres_streaming(
        postgres,
        pw.Duration,
        [pd.Timedelta(days=365 * 100)],
    )


def test_bool(postgres):
    _test_postgres_streaming(postgres, bool, [True, False])


def test_bool_array(postgres):
    _test_postgres_streaming(
        postgres, list[bool | None], [[True, False], [True, None], [None, False]]
    )


def test_2d_bool_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[bool]],
        [
            [
                [True, False],
                [False, True],
            ],
            [
                [True, True],
                [False, False],
            ],
        ],
    )


def test_int(postgres):
    _test_postgres_streaming(postgres, int, [-1, 0, 1])


def test_int_array(postgres):
    _test_postgres_streaming(postgres, list[int | None], [[1, 2], [3, None], [None, 4]])


def test_2d_int_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[int]],
        [
            [[1, 2], [3, 4]],
            [[5, 6], [7, 8]],
        ],
    )


def test_float(postgres):
    _test_postgres_streaming(postgres, float, [-1.5, 0.0, 3.14])


def test_float_array(postgres):
    _test_postgres_streaming(
        postgres, list[float | None], [[1.1, 2.2], [3.3, None], [None, 4.4]]
    )


def test_2d_float_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[float]],
        [
            [[1.1, 2.2], [3.3, 4.4]],
            [[5.5, 6.6], [7.7, 8.8]],
        ],
    )


def test_str(postgres):
    _test_postgres_streaming(postgres, str, ["hello", "", "world"])


def test_str_array(postgres):
    _test_postgres_streaming(
        postgres, list[str | None], [["a", "b"], ["c", None], [None, "d"]]
    )


def test_2d_str_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[str]],
        [
            [["a", "b"], ["c", "d"]],
            [["w", "x"], ["y", "z"]],
        ],
    )


def test_bytes(postgres):
    _test_postgres_streaming(postgres, bytes, [b"\xde\xad\xbe\xef", b"", b"\x00\xff"])


def test_bytes_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[bytes],
        [[b"\xde\xad", b"\xbe\xef"], [b"\xab\xcd", b"\xef\x01"]],
    )


def test_2d_bytes_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[bytes]],
        [
            [
                [b"\xde\xad", b"\xbe\xef"],
                [b"\xab\xcd", b"\xef\x01"],
            ],
            [
                [b"\x00\x01", b"\x02\x03"],
                [b"\xff\xfe", b"\xfd\xfc"],
            ],
        ],
    )


def test_pointer(postgres):
    _test_postgres_streaming(
        postgres, pw.Pointer, [api.ref_scalar(42), api.ref_scalar(43)]
    )


def test_pointer_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[pw.Pointer | None],
        [[api.ref_scalar(42), api.ref_scalar(43)], [None, None]],
    )


def test_2d_pointer_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[pw.Pointer]],
        [
            [
                [api.ref_scalar(1), api.ref_scalar(2)],
                [api.ref_scalar(3), api.ref_scalar(4)],
            ],
            [
                [api.ref_scalar(5), api.ref_scalar(6)],
                [api.ref_scalar(7), api.ref_scalar(8)],
            ],
        ],
    )


def test_json(postgres):
    _test_postgres_streaming(
        postgres,
        pw.Json,
        [
            pw.Json({"key": "value"}),
            pw.Json([1, 2, 3]),
            pw.Json(42),
        ],
    )


def test_json_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[pw.Json | None],
        [
            [
                pw.Json({"a": 1}),
                pw.Json([1, 2, 3]),
            ],
            [
                pw.Json("string"),
                None,
            ],
            [
                None,
                pw.Json(42),
            ],
        ],
    )


def test_2d_json_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[pw.Json]],
        [
            [
                [
                    pw.Json({"x": 1}),
                    pw.Json({"y": 2}),
                ],
                [
                    pw.Json([1, 2]),
                    pw.Json([3, 4]),
                ],
            ],
            [
                [
                    pw.Json("a"),
                    pw.Json("b"),
                ],
                [
                    pw.Json(True),
                    pw.Json(False),
                ],
            ],
        ],
    )


def test_duration(postgres):
    _test_postgres_streaming(
        postgres,
        pw.Duration,
        [
            pd.Timedelta("0"),
            pd.Timedelta("1 days 2 seconds 3 us"),
            pd.Timedelta("-1 days"),
        ],
    )


def test_duration_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[pw.Duration | None],
        [
            [
                pd.Timedelta("0"),
                pd.Timedelta("1 days 2 seconds 3 us"),
            ],
            [
                pd.Timedelta("-1 days"),
                None,
            ],
            [
                None,
                pd.Timedelta("3 days 4 hours"),
            ],
        ],
    )


def test_2d_duration_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[pw.Duration]],
        [
            [
                [
                    pd.Timedelta("0"),
                    pd.Timedelta("1 days"),
                ],
                [
                    pd.Timedelta("2 days 3 seconds"),
                    pd.Timedelta("4 days 5 microseconds"),
                ],
            ],
            [
                [
                    pd.Timedelta("-1 days"),
                    pd.Timedelta("3 hours"),
                ],
                [
                    pd.Timedelta("7 days"),
                    pd.Timedelta("10 days 12 seconds"),
                ],
            ],
        ],
    )


def test_datetime_naive(postgres):
    _test_postgres_streaming(
        postgres,
        pw.DateTimeNaive,
        [
            pw.DateTimeNaive("2025-01-01T00:00:00"),
            pw.DateTimeNaive("2025-03-14T10:13:00.123456789"),
        ],
    )


def test_datetime_naive_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[pw.DateTimeNaive | None],
        [
            [
                pw.DateTimeNaive("2025-01-01T00:00:00"),
                pw.DateTimeNaive("2025-01-02T12:34:56.123456789"),
            ],
            [
                pw.DateTimeNaive("2025-03-14T10:13:00"),
                None,
            ],
            [
                None,
                pw.DateTimeNaive("2026-06-01T23:59:59"),
            ],
        ],
    )


def test_2d_datetime_naive_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[pw.DateTimeNaive]],
        [
            [
                [
                    pw.DateTimeNaive("2025-01-01T00:00:00"),
                    pw.DateTimeNaive("2025-01-02T12:00:00"),
                ],
                [
                    pw.DateTimeNaive("2025-03-14T10:13:00.123456789"),
                    pw.DateTimeNaive("2025-04-01T08:30:00"),
                ],
            ],
            [
                [
                    pw.DateTimeNaive("2026-01-01T00:00:00"),
                    pw.DateTimeNaive("2026-01-02T00:00:00"),
                ],
                [
                    pw.DateTimeNaive("2026-02-01T00:00:00"),
                    pw.DateTimeNaive("2026-03-01T00:00:00"),
                ],
            ],
        ],
    )


def test_datetime_utc(postgres):
    _test_postgres_streaming(
        postgres,
        pw.DateTimeUtc,
        [
            pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
            pw.DateTimeUtc("2025-03-14T10:13:00.123456000+00:00"),
        ],
    )


def test_datetime_utc_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[pw.DateTimeUtc | None],
        [
            [
                pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
                pw.DateTimeUtc("2025-01-02T12:34:56.123456000+00:00"),
            ],
            [
                pw.DateTimeUtc("2025-03-14T10:13:00+00:00"),
                None,
            ],
            [
                None,
                pw.DateTimeUtc("2026-06-01T23:59:59+00:00"),
            ],
        ],
    )


def test_2d_datetime_utc_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[pw.DateTimeUtc]],
        [
            [
                [
                    pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
                    pw.DateTimeUtc("2025-01-02T12:00:00+00:00"),
                ],
                [
                    pw.DateTimeUtc("2025-03-14T10:13:00.123456000+00:00"),
                    pw.DateTimeUtc("2025-04-01T08:30:00+00:00"),
                ],
            ],
            [
                [
                    pw.DateTimeUtc("2026-01-01T00:00:00+00:00"),
                    pw.DateTimeUtc("2026-01-02T00:00:00+00:00"),
                ],
                [
                    pw.DateTimeUtc("2026-02-01T00:00:00+00:00"),
                    pw.DateTimeUtc("2026-03-01T00:00:00+00:00"),
                ],
            ],
        ],
    )


def test_tuple_homogeneous(postgres):
    _test_postgres_streaming(
        postgres,
        tuple[int, int, int],
        [
            (1, 2, 3),
            (4, 5, 6),
            (7, 8, 9),
        ],
    )


def test_tuple_homogeneous_optional(postgres):
    _test_postgres_streaming(
        postgres,
        tuple[int, int, int] | None,  # type: ignore
        [
            (1, 2, 3),
            None,
            (4, 5, 6),
        ],
    )


def test_tuple_nested_homogeneous(postgres):
    _test_postgres_streaming(
        postgres,
        tuple[tuple[int, int], tuple[int, int]],
        [
            ((1, 2), (3, 4)),
            ((5, 6), (7, 8)),
            ((9, 10), (11, 12)),
        ],
    )


def test_tuple_nested_homogeneous_optional(postgres):
    _test_postgres_streaming(
        postgres,
        tuple[tuple[int, int], tuple[int, int]] | None,  # type: ignore
        [
            ((1, 2), (3, 4)),
            None,
            ((5, 6), (7, 8)),
        ],
    )


def test_ndarray_int(postgres):
    _test_postgres_streaming(
        postgres,
        np.ndarray[None, int],  # type: ignore
        [
            np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=int),
        ],
        _create_ndarray_table,
    )


def test_ndarray_float(postgres):
    _test_postgres_streaming(
        postgres,
        np.ndarray[None, float],  # type: ignore
        [
            np.array([[[1.1, 2.2], [3.3, 4.4]], [[5.5, 6.6], [7.7, 8.8]]], dtype=float),
        ],
        _create_ndarray_table,
    )


def test_pyobject_wrapper(postgres):
    _test_postgres_streaming(
        postgres,
        pw.PyObjectWrapper[SimpleObject],
        [
            pw.wrap_py_object(SimpleObject(1)),
            pw.wrap_py_object(SimpleObject(2)),
            pw.wrap_py_object(SimpleObject(3)),
        ],
    )


def test_pyobject_wrapper_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[pw.PyObjectWrapper[SimpleObject]],
        [
            [
                pw.wrap_py_object(SimpleObject(1)),
                pw.wrap_py_object(SimpleObject(2)),
            ],
            [
                pw.wrap_py_object(SimpleObject(3)),
                pw.wrap_py_object(SimpleObject(4)),
            ],
        ],
    )


def test_2d_pyobject_wrapper_array(postgres):
    _test_postgres_streaming(
        postgres,
        list[list[pw.PyObjectWrapper[SimpleObject]]],
        [
            [
                [
                    pw.wrap_py_object(SimpleObject(1)),
                    pw.wrap_py_object(SimpleObject(2)),
                ],
                [
                    pw.wrap_py_object(SimpleObject(3)),
                    pw.wrap_py_object(SimpleObject(4)),
                ],
            ]
        ],
    )

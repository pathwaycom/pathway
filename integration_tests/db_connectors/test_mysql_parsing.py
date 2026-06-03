# Copyright © 2026 Pathway

"""Round-trip parsing coverage for the MySQL input connector.

For every Pathway type that can round-trip through MySQL (the scalar types the
output connector supports — MySQL has no array types, so lists/tuples/ndarrays
are intentionally out of scope), this writes the value with
``pw.io.mysql.write``, reads it back with ``pw.io.mysql.read`` in both
``static`` and ``streaming`` modes, mirrors it into a second MySQL table, and
checks the value survived the trip.
"""

import datetime
import json
from typing import Any, get_args, get_origin

import pandas as pd
import pytest
from utils import (
    MYSQL_CONNECTION_STRING,
    MySQLContext,
    SimpleObject,
    binlog_reader_access,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import ExceptionAwareThread, run, wait_result_with_checker

pytestmark = pytest.mark.xdist_group("mysql")


def _is_pyobject_type(item_type: Any) -> bool:
    return get_origin(item_type) is pw.PyObjectWrapper or "PyObjectWrapper" in str(
        item_type
    )


def _is_json_type(item_type: Any) -> bool:
    args = get_args(item_type) or (item_type,)
    return item_type is pw.Json or pw.Json in args


def _canon_input(value: Any) -> Any:
    """Normalize an original (Pathway/Python) value to the canonical form used
    for comparison."""
    if value is None:
        return None
    if isinstance(value, pw.PyObjectWrapper):
        return value.value
    if isinstance(value, pw.Pointer):
        return str(value)
    if isinstance(value, pw.Json):
        return value.value
    if isinstance(value, pd.Timestamp):
        moment = value.to_pydatetime()
        if moment.tzinfo is not None:
            moment = moment.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return moment
    if isinstance(value, pd.Timedelta):
        return value.value // 1000  # nanoseconds -> microseconds
    return value


def _canon_output(value: Any, item_type: Any) -> Any:
    """Normalize a value read back from MySQL (raw mysql.connector value) to the
    same canonical form as :func:`_canon_input`."""
    if value is None:
        return None
    if isinstance(value, datetime.timedelta):
        # MySQL TIME(6) -> datetime.timedelta; convert to total microseconds.
        return (
            value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
        )
    if _is_pyobject_type(item_type):
        restored = api.deserialize(bytes(value))
        assert isinstance(
            restored, pw.PyObjectWrapper
        ), f"expecting PyObjectWrapper, got {type(restored)}"
        return restored.value
    if _is_json_type(item_type):
        if isinstance(value, (bytes, bytearray)):
            value = bytes(value).decode("utf-8")
        return json.loads(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return value


def _compare_input_and_output(
    item_type: Any, input_rows: list[dict], output_rows: list[dict]
) -> None:
    output_rows = sorted(output_rows, key=lambda row: row["pkey"])
    input_rows = sorted(input_rows, key=lambda row: row["pkey"])

    canonical_input = [
        {"pkey": row["pkey"], "item": _canon_input(row["item"])} for row in input_rows
    ]
    canonical_output = [
        {"pkey": row["pkey"], "item": _canon_output(row["item"], item_type)}
        for row in output_rows
    ]
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


def _serialize_item_for_mysql(value: Any) -> Any:
    """Convert a Pathway/Python value to a form mysql.connector can bind into the
    column ``pw.io.mysql.write`` created for it (mirrors the writer's
    serialization, so a directly inserted row reads back identically)."""
    if value is None:
        return None
    if isinstance(value, pw.PyObjectWrapper):
        return api.serialize(value)  # type: ignore[arg-type]
    if isinstance(value, pw.Pointer):
        return str(value)
    if isinstance(value, pw.Json):
        return json.dumps(value.value)
    if isinstance(value, pd.Timestamp):
        moment = value.to_pydatetime()
        if moment.tzinfo is not None:
            moment = moment.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return moment
    if isinstance(value, pd.Timedelta):
        return value.to_pytimedelta()
    return value


class MysqlRowCountChecker:
    def __init__(self, mysql: MySQLContext, table_name: str, n_rows_expected: int):
        self.mysql = mysql
        self.table_name = table_name
        self.n_rows_expected = n_rows_expected

    def __call__(self) -> bool:
        try:
            rows = self.mysql.get_table_contents(self.table_name, ["pkey", "item"])
        except Exception:
            return False
        return len(rows) == self.n_rows_expected


def _test_mysql_static(mysql: MySQLContext, item_type: Any, items: list[Any]) -> str:
    G.clear()
    input_rows = [{"pkey": index, "item": item} for index, item in enumerate(items)]

    input_table_name = mysql.random_table_name()
    table = _create_table(item_type, input_rows)
    input_schema = table.schema

    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name=input_table_name,
        init_mode="create_if_not_exists",
    )
    run()

    G.clear()
    output_table_name = mysql.random_table_name()
    read_table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING,
        input_table_name,
        input_schema,
        mode="static",
    )
    pw.io.mysql.write(
        read_table,
        MYSQL_CONNECTION_STRING,
        table_name=output_table_name,
        init_mode="create_if_not_exists",
    )
    # The static reader emits ReadResult::Finished after the snapshot, so
    # pw.run() terminates on its own.
    run()

    output_rows = mysql.get_table_contents(output_table_name, ["pkey", "item"])
    _compare_input_and_output(item_type, input_rows, output_rows)
    return input_table_name


def _test_mysql_streaming(
    mysql: MySQLContext, item_type: Any, items: list[Any]
) -> None:
    input_table_name = _test_mysql_static(mysql, item_type, items)
    mysql.grant_replication_privileges()

    output_table_name = mysql.random_table_name()

    class InputSchemaWithPkey(pw.Schema):
        pkey: int = pw.column_definition(primary_key=True)
        item: item_type

    # New rows for the streaming phase; pkey is offset by len(items) to avoid
    # colliding with the rows the static phase already wrote.
    streaming_rows = [
        {"pkey": len(items) + index, "item": item} for index, item in enumerate(items)
    ]

    def streaming_target():
        # Separate connection so this thread's cursor does not race with the main
        # thread's MysqlRowCountChecker.
        thread_mysql = MySQLContext()
        try:
            for index, row in enumerate(streaming_rows):
                # Wait for the snapshot (len(items) rows) plus every previously
                # inserted streaming row to land in the output before inserting
                # more.
                wait_result_with_checker(
                    MysqlRowCountChecker(
                        thread_mysql, output_table_name, index + len(items)
                    ),
                    60,
                    target=None,
                )
                # The input table was created by pw.io.mysql.write, which
                # appends non-nullable `time`/`diff` columns; the binlog reader
                # only maps `pkey` and `item`, so any valid metadata values work
                # here.
                thread_mysql.insert_row(
                    input_table_name,
                    {
                        "pkey": row["pkey"],
                        "item": _serialize_item_for_mysql(row["item"]),
                        "time": 0,
                        "diff": 1,
                    },
                )
        finally:
            thread_mysql.close()

    # Hold the binlog reader lock for the whole tailing phase so the
    # server-global PURGE BINARY LOGS in the persistence-purge test cannot
    # delete the log file this reader is resuming from (see
    # ``binlog_reader_access`` in utils.py).
    with binlog_reader_access():
        streaming_thread = ExceptionAwareThread(target=streaming_target, daemon=True)
        streaming_thread.start()

        G.clear()
        read_table = pw.io.mysql.read(
            MYSQL_CONNECTION_STRING,
            input_table_name,
            InputSchemaWithPkey,
            mode="streaming",
        )
        pw.io.mysql.write(
            read_table,
            MYSQL_CONNECTION_STRING,
            table_name=output_table_name,
            init_mode="create_if_not_exists",
        )
        # Forks a process that runs pw.run() on the streaming graph; the parent
        # polls until the output table holds both the snapshot and the streamed
        # rows.
        wait_result_with_checker(
            MysqlRowCountChecker(mysql, output_table_name, 2 * len(items)),
            120,
        )
        streaming_thread.join()

    output_rows = mysql.get_table_contents(output_table_name, ["pkey", "item"])

    all_input_rows = list(streaming_rows)
    for index, item in enumerate(items):
        all_input_rows.append({"pkey": index, "item": item})
    _compare_input_and_output(item_type, all_input_rows, output_rows)


def _test_mysql(mysql: MySQLContext, item_type: Any, items: list[Any]) -> None:
    if not mysql.binlog_available():
        pytest.skip("row-based binary logging is not enabled on the MySQL server")
    _test_mysql_streaming(mysql, item_type, items)


def test_bool(mysql):
    _test_mysql(mysql, bool, [True, False, True])


def test_int(mysql):
    _test_mysql(mysql, int, [-1, 0, 1])


def test_int_boundary(mysql):
    _test_mysql(mysql, int, [-(2**63), 2**63 - 1, 2**31 - 1, -(2**31)])


def test_float(mysql):
    _test_mysql(mysql, float, [-1.5, 0.0, 3.14])


def test_str(mysql):
    _test_mysql(mysql, str, ["hello", "", "world"])


def test_str_unicode(mysql):
    _test_mysql(
        mysql,
        str,
        [
            "привет 世界 🔥",
            "line\nnewline\ttab",
            "quote'semicolon;null,./;'l][p!@#!@#!@#%$#$#]",
            "a" * 10_000,
        ],
    )


def test_bytes(mysql):
    _test_mysql(mysql, bytes, [b"\xde\xad\xbe\xef", b"", b"\x00\xff"])


def test_pointer(mysql):
    _test_mysql(mysql, pw.Pointer, [api.ref_scalar(42), api.ref_scalar(43)])


def test_json(mysql):
    _test_mysql(
        mysql,
        pw.Json,
        [pw.Json({"key": "value"}), pw.Json([1, 2, 3]), pw.Json(42)],
    )


def test_json_nested(mysql):
    _test_mysql(
        mysql,
        pw.Json,
        [
            pw.Json({"nested": {"a": 1, "b": [1, 2, 3]}}),
            pw.Json({"key": "val,with,commas"}),
            pw.Json({}),
        ],
    )


def test_duration(mysql):
    _test_mysql(
        mysql,
        pw.Duration,
        [
            pd.Timedelta("0"),
            pd.Timedelta("1 days 2 seconds 3 us"),
            pd.Timedelta("-1 days"),
        ],
    )


def test_duration_negative_mixed(mysql):
    _test_mysql(
        mysql,
        pw.Duration,
        [
            pd.Timedelta("-1 days 2 hours 3 minutes"),
            pd.Timedelta("-2 days 30 minutes"),
        ],
    )


def test_datetime_naive(mysql):
    _test_mysql(
        mysql,
        pw.DateTimeNaive,
        [
            pw.DateTimeNaive("2025-01-01T00:00:00"),
            pw.DateTimeNaive("2025-03-14T10:13:00.123000"),
        ],
    )


def test_datetime_utc(mysql):
    _test_mysql(
        mysql,
        pw.DateTimeUtc,
        [
            pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
            pw.DateTimeUtc("2025-03-14T10:13:00.123000+00:00"),
        ],
    )


def test_optional_int(mysql):
    _test_mysql(mysql, int | None, [1, None, -1, None, 0])


def test_optional_str(mysql):
    _test_mysql(mysql, str | None, ["hello", None, "", None, "world"])


def test_optional_float(mysql):
    _test_mysql(mysql, float | None, [1.5, None, -2.5, None, 0.0])


def test_optional_bytes(mysql):
    _test_mysql(mysql, bytes | None, [b"\x01\x02", None, b"", None])


def test_optional_duration(mysql):
    _test_mysql(
        mysql,
        pw.Duration | None,
        [pd.Timedelta("1 days 2 seconds"), None, pd.Timedelta("-3 hours")],
    )


def test_optional_datetime_naive(mysql):
    _test_mysql(
        mysql,
        pw.DateTimeNaive | None,
        [pw.DateTimeNaive("2025-01-01T00:00:00"), None],
    )


def test_optional_json(mysql):
    _test_mysql(
        mysql,
        pw.Json | None,
        [pw.Json({"a": 1}), None, pw.Json([1, 2])],
    )


def test_pyobject_wrapper(mysql):
    _test_mysql(
        mysql,
        pw.PyObjectWrapper[SimpleObject],
        [
            pw.wrap_py_object(SimpleObject(1)),
            pw.wrap_py_object(SimpleObject(2)),
            pw.wrap_py_object(SimpleObject(3)),
        ],
    )

# Copyright © 2026 Pathway

import json
import os
import subprocess
import sys
import textwrap
import types as builtin_types
import typing
from typing import Any, Union, get_args, get_origin

import numpy as np
import pandas as pd
import pytest
from utils import (
    MSSQL_CONNECTION_STRING,
    MssqlContext,
    SimpleObject,
    _compare_input_and_output,
    _make_type_check_observer,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import ExceptionAwareThread, run, wait_result_with_checker

pytestmark = pytest.mark.xdist_group("mssql")

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

# Script run in a subprocess for CDC streaming tests.  Using subprocess.Popen
# (not fork) avoids inheriting Rust/tokio global state that pw.run() leaves
# behind in the test process.
_MSSQL_CDC_SCRIPT = textwrap.dedent(
    """\
    import json
    import random
    import sys
    import time

    cfg = json.loads(sys.argv[1])
    sys.path.insert(0, cfg["test_dir"])

    import pathway as pw
    from pathway.internals.parse_graph import G
    from pathway.tests.utils import run

    exec(cfg["schema_code"], globals())

    # Retry the entire pipeline on transient TCP source-port exhaustion
    # (EADDRNOTAVAIL).  Under heavy CI parallelism, the host occasionally
    # cannot allocate an ephemeral source port for tiberius's connect, and
    # the engine surfaces it as 'Cannot assign requested address'.  The CDC
    # writer is in ``create_if_not_exists`` mode, so a retried run that hits
    # an existing output table just reuses it.
    max_attempts = 5
    for attempt in range(max_attempts):
        G.clear()
        table = pw.io.mssql.read(
            cfg["connection_string"],
            cfg["input_table_name"],
            InputSchemaWithPkey,
            mode="streaming",
        )
        pw.io.mssql.write(
            table,
            cfg["connection_string"],
            table_name=cfg["output_table_name"],
            init_mode="create_if_not_exists",
        )
        try:
            run()
            sys.exit(0)
        except Exception as e:
            if (
                "Cannot assign requested address" in str(e)
                and attempt < max_attempts - 1
            ):
                time.sleep(2.0 * (attempt + 1) + random.uniform(0, 1.0))
                continue
            raise
    """
)


def _type_to_str(t: Any) -> str:
    """Return a Python source expression for *t* suitable for exec'd schema code."""
    if t is type(None):
        return "None"
    if t is typing.Any:
        return "Any"
    if t in (int, float, bool, str, bytes):
        return t.__name__
    if t is pw.Pointer:
        return "pw.Pointer"
    if t is pw.Json:
        return "pw.Json"
    if t is pw.Duration:
        return "pw.Duration"
    if t is pw.DateTimeNaive:
        return "pw.DateTimeNaive"
    if t is pw.DateTimeUtc:
        return "pw.DateTimeUtc"

    origin = get_origin(t)
    args = get_args(t)

    if origin is Union or (
        hasattr(builtin_types, "UnionType") and isinstance(t, builtin_types.UnionType)
    ):
        return " | ".join(_type_to_str(a) for a in args)

    if origin is list:
        return f"list[{_type_to_str(args[0])}]"

    if origin is tuple:
        return f"tuple[{', '.join(_type_to_str(a) for a in args)}]"

    if origin is np.ndarray:
        dims_arg, scalar_arg = args
        scalar_str = _type_to_str(scalar_arg)
        if dims_arg is None:
            return f"np.ndarray[None, {scalar_str}]"
        return f"np.ndarray[{_type_to_str(dims_arg)}, {scalar_str}]"

    # pw.PyObjectWrapper[X]
    if origin is pw.PyObjectWrapper and args:
        return f"pw.PyObjectWrapper[{args[0].__name__}]"

    if hasattr(t, "__name__"):
        return t.__name__
    return repr(t)


def _generate_schema_code(ItemType: Any) -> str:
    """Return Python source that defines InputSchemaWithPkey for the given type."""
    item_type_str = _type_to_str(ItemType)
    lines = []
    # Import inner class of PyObjectWrapper if it comes from utils
    origin = get_origin(ItemType)
    if origin is np.ndarray:
        lines.append("import numpy as np")
        lines.append("from typing import Any")
    if origin is pw.PyObjectWrapper:
        args = get_args(ItemType)
        if args:
            inner = args[0]
            module = getattr(inner, "__module__", "builtins")
            if not module.startswith(("builtins", "pathway")):
                lines.append(f"from utils import {inner.__name__}")
    if lines:
        lines.append("")
    lines += [
        "class InputSchemaWithPkey(pw.Schema):",
        "    pkey: int = pw.column_definition(primary_key=True)",
        f"    item: {item_type_str}",
    ]
    return "\n".join(lines)


def _start_mssql_cdc_worker(
    input_table_name: str,
    output_table_name: str,
    schema_code: str,
) -> subprocess.Popen:
    cfg = json.dumps(
        {
            "connection_string": MSSQL_CONNECTION_STRING,
            "input_table_name": input_table_name,
            "output_table_name": output_table_name,
            "schema_code": schema_code,
            "test_dir": _TEST_DIR,
        }
    )
    import tempfile

    log_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".log", prefix="mssql_cdc_", delete=False
    )
    log_path = log_file.name
    log_file.close()
    print(f"[CDC subprocess log: {log_path}]", flush=True)
    proc = subprocess.Popen(
        [sys.executable, "-c", _MSSQL_CDC_SCRIPT, cfg],
        env=os.environ,
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
    )
    return proc


def _to_json_serializable(value: Any) -> Any:
    """Recursively convert value to a JSON-serializable form matching Pathway's
    MSSQL list/tuple serialization (serialize_value_to_json in data_format.rs)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, pw.Pointer):
        return str(value)
    if isinstance(value, pw.Json):
        return value.value  # the underlying JSON-compatible Python object
    if isinstance(value, pd.Timedelta):
        return value.value  # nanoseconds (serialize_value_to_json uses nanoseconds)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_to_json_serializable(v) for v in value]
    return value


def _serialize_item_for_mssql(value: Any) -> Any:
    """Convert a Pathway/Python value to a form pymssql can bind, matching what
    the Pathway MSSQL writer (bind_value in mssql.rs) produces."""
    if value is None:
        return None
    if isinstance(value, pw.PyObjectWrapper):
        return api.serialize(value)  # type: ignore[arg-type]
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str, bytes)):
        return value
    if isinstance(value, pw.Pointer):
        return str(value)
    if isinstance(value, pw.Json):
        return json.dumps(value.value)
    if isinstance(value, pd.Timedelta):
        return value.value // 1000  # nanoseconds → microseconds (bind_value uses µs)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.ndarray):
        return json.dumps(
            {"shape": list(value.shape), "elements": value.flatten().tolist()}
        )
    if isinstance(value, (list, tuple)):
        return json.dumps(_to_json_serializable(value))
    return value


class MssqlRowCountChecker:

    def __init__(self, mssql: MssqlContext, table_name: str, n_rows_expected: int):
        self.mssql = mssql
        self.table_name = table_name
        self.n_rows_expected = n_rows_expected

    def __call__(self) -> bool:
        try:
            rows = self.mssql.get_table_contents(self.table_name, ["pkey", "item"])
        except Exception:
            return False
        return len(rows) == self.n_rows_expected


def _create_table(ItemType: Any, input_rows: list[dict]) -> pw.Table:
    class InputSchemaWithPkey(pw.Schema):
        pkey: int = pw.column_definition(primary_key=True)
        item: ItemType

    return pw.debug.table_from_rows(
        InputSchemaWithPkey,
        [tuple(row.values()) for row in input_rows],
    )


def _create_ndarray_table(ItemType: Any, input_rows: list[dict]) -> pw.Table:
    """Like _create_table but uses schema_builder for ndarray type annotations.

    Cannot use a class-body annotation directly because pw.debug.table_from_rows
    validates schema-declared types, and ndarray dimension constraints (e.g.
    n_dim=0 from np.ndarray[None, int]) would reject valid arrays.
    schema_builder skips that class-body parse path while still preserving
    primary_key, which update_types() would silently drop.
    """
    schema = pw.schema_builder(
        {
            "pkey": pw.column_definition(dtype=int, primary_key=True),
            "item": pw.column_definition(dtype=ItemType),
        }
    )
    return pw.debug.table_from_rows(
        schema,
        [tuple(row.values()) for row in input_rows],
    )


def _test_mssql_static(
    mssql: MssqlContext,
    ItemType: Any,
    items: list[Any],
    create_table: Any = _create_table,
) -> str:
    G.clear()
    input_rows = [{"pkey": i, "item": item} for i, item in enumerate(items)]

    input_table_name = mssql.random_table_name()
    table = create_table(ItemType, input_rows)
    InputSchemaWithPkey = table.schema

    # Write test data to MSSQL; terminates because the source is a static debug table.
    pw.io.mssql.write(
        table,
        MSSQL_CONNECTION_STRING,
        table_name=input_table_name,
        init_mode="create_if_not_exists",
    )
    run()

    # Build a new graph that reads the data back and writes it to a second table.
    G.clear()
    output_table_name = mssql.random_table_name()
    table = pw.io.mssql.read(
        MSSQL_CONNECTION_STRING,
        input_table_name,
        InputSchemaWithPkey,
        mode="static",
    )
    pw.io.mssql.write(
        table,
        MSSQL_CONNECTION_STRING,
        table_name=output_table_name,
        init_mode="create_if_not_exists",
    )
    observer, type_errors = _make_type_check_observer(ItemType)
    pw.io.python.write(table, observer)

    # The static MSSQL reader now returns ReadResult::Finished after the snapshot,
    # so pw.run() terminates naturally — no subprocess needed.
    run()

    assert not type_errors, "\n".join(type_errors)

    output_rows = mssql.get_table_contents(
        output_table_name, list(InputSchemaWithPkey.column_names())
    )
    _compare_input_and_output(ItemType, input_rows, output_rows)
    return input_table_name


def _test_mssql_streaming(
    mssql: MssqlContext,
    ItemType: Any,
    items: list[Any],
    create_table: Any = _create_table,
) -> None:
    input_table_name = _test_mssql_static(mssql, ItemType, items, create_table)

    # Enable CDC on the input table so the streaming (CDC) reader can use it.
    mssql.enable_cdc(input_table_name)
    # Speed up the CDC capture job for tests: pollinginterval=0 means no sleep
    # between scan cycles (~500 ms effective latency instead of the 5 s default).
    # execute_sql retries on 1205 — sp_cdc_change_job touches msdb job metadata
    # and competes with concurrent CDC traffic under xdist parallelism.
    # drain_status_rows clears the proc's status output so the next statement
    # on this connection isn't refused with "Statement(s) could not be prepared".
    mssql.execute_sql(
        "EXEC sys.sp_cdc_change_job @job_type = N'capture', @pollinginterval = 0",
        drain_status_rows=True,
    )

    output_table_name = mssql.random_table_name()

    class InputSchemaWithPkey(pw.Schema):
        pkey: int = pw.column_definition(primary_key=True)
        item: ItemType

    # New rows for the streaming phase; pkey is offset by len(items) to avoid
    # collisions with the rows already written by the static phase.
    streaming_rows = [
        {"pkey": len(items) + i, "item": item} for i, item in enumerate(items)
    ]

    def streaming_target():
        # Use a separate connection so this thread's cursor doesn't race with
        # the main thread's MssqlRowCountChecker polling the same mssql cursor.
        thread_mssql = MssqlContext()
        for index, row in enumerate(streaming_rows):
            # Wait until the output has (index + len(items)) rows before
            # inserting the next row.  At index=0 this waits for the CDC
            # snapshot (the initial len(items) rows) to land in the output.
            wait_result_with_checker(
                MssqlRowCountChecker(
                    thread_mssql, output_table_name, index + len(items)
                ),
                60,
                target=None,
            )
            # The input table was created by pw.io.mssql.write which adds
            # non-nullable `time` and `diff` columns.  The CDC reader only
            # selects `pkey` and `item`, so any valid values work here.
            thread_mssql.insert_row(
                input_table_name,
                {
                    "pkey": row["pkey"],
                    "item": _serialize_item_for_mssql(row["item"]),
                    "time": 0,
                    "diff": 1,
                },
            )

    # Start the CDC reader subprocess first so it has time to connect and deliver
    # the snapshot rows before streaming_target starts polling for them.
    proc = _start_mssql_cdc_worker(
        input_table_name,
        output_table_name,
        _generate_schema_code(ItemType),
    )

    streaming_thread = ExceptionAwareThread(target=streaming_target, daemon=True)
    streaming_thread.start()

    try:
        wait_result_with_checker(
            MssqlRowCountChecker(mssql, output_table_name, 2 * len(items)),
            120,
            target=None,
        )
        streaming_thread.join()
    finally:
        proc.terminate()
        proc.wait()

    output_rows = mssql.get_table_contents(
        output_table_name, list(InputSchemaWithPkey.column_names())
    )

    # Combine snapshot rows (pkey 0..N-1) and streaming rows (pkey N..2N-1)
    # for the final comparison.
    all_input_rows = streaming_rows.copy()
    for i, item in enumerate(items):
        all_input_rows.append({"pkey": i, "item": item})
    all_input_rows.sort(key=lambda r: r["pkey"])

    _compare_input_and_output(ItemType, all_input_rows, output_rows)


def test_bool(mssql):
    _test_mssql_streaming(mssql, bool, [True, False, True])


def test_int(mssql):
    _test_mssql_streaming(mssql, int, [-1, 0, 1])


def test_int_boundary(mssql):
    _test_mssql_streaming(
        mssql,
        int,
        [-(2**63), 2**63 - 1, 2**31 - 1, -(2**31)],
    )


def test_float(mssql):
    _test_mssql_streaming(mssql, float, [-1.5, 0.0, 3.14])


def test_str(mssql):
    _test_mssql_streaming(mssql, str, ["hello", "", "world"])


def test_str_unicode(mssql):
    _test_mssql_streaming(
        mssql,
        str,
        [
            "привет 世界 🔥",
            "line\nnewline\ttab",
            "quote'semicolon;null,./;'l][p!@#!@#!@#%$#$#]",
            "a" * 10_000,
        ],
    )


def test_bytes(mssql):
    _test_mssql_streaming(mssql, bytes, [b"\xde\xad\xbe\xef", b"", b"\x00\xff"])


def test_pointer(mssql):
    _test_mssql_streaming(
        mssql,
        pw.Pointer,
        [api.ref_scalar(42), api.ref_scalar(43)],
    )


def test_json(mssql):
    _test_mssql_streaming(
        mssql,
        pw.Json,
        [
            pw.Json({"key": "value"}),
            pw.Json([1, 2, 3]),
            pw.Json(42),
        ],
    )


def test_json_nested(mssql):
    _test_mssql_streaming(
        mssql,
        pw.Json,
        [
            pw.Json({"nested": {"a": 1, "b": [1, 2, 3]}}),
            pw.Json({"key": "val,with,commas"}),
            pw.Json({}),
        ],
    )


def test_duration(mssql):
    _test_mssql_streaming(
        mssql,
        pw.Duration,
        [
            pd.Timedelta("0"),
            pd.Timedelta("1 days 2 seconds 3 us"),
            pd.Timedelta("-1 days"),
        ],
    )


def test_duration_negative_mixed(mssql):
    _test_mssql_streaming(
        mssql,
        pw.Duration,
        [
            pd.Timedelta("-1 days 2 hours 3 minutes"),
            pd.Timedelta("-2 days 30 minutes"),
        ],
    )


def test_datetime_naive(mssql):
    _test_mssql_streaming(
        mssql,
        pw.DateTimeNaive,
        [
            pw.DateTimeNaive("2025-01-01T00:00:00"),
            pw.DateTimeNaive("2025-03-14T10:13:00.123000000"),
        ],
    )


def test_datetime_utc(mssql):
    _test_mssql_streaming(
        mssql,
        pw.DateTimeUtc,
        [
            pw.DateTimeUtc("2025-01-01T00:00:00+00:00"),
            pw.DateTimeUtc("2025-03-14T10:13:00.123000000+00:00"),
        ],
    )


def test_optional_int(mssql):
    _test_mssql_streaming(
        mssql,
        int | None,
        [1, None, -1, None, 0],
    )


def test_optional_str(mssql):
    _test_mssql_streaming(
        mssql,
        str | None,
        ["hello", None, "", None, "world"],
    )


def test_bool_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[bool | None],
        [[True, False], [True, None], [None, False]],
    )


def test_int_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[int | None],
        [[1, 2], [3, None], [None, 4]],
    )


def test_float_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[float | None],
        [[1.1, 2.2], [3.3, None], [None, 4.4]],
    )


def test_str_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[str | None],
        [["a", "b"], ["c", None], [None, "d"]],
    )


def test_str_array_with_backslash_and_quotes(mssql):
    _test_mssql_streaming(
        mssql,
        list[str | None],
        [
            ['say "hello"', "path\\to\\file"],
            ['mixed \\"escaped\\"', None],
            ["C:\\Users\\test", 'quote"then\\slash'],
        ],
    )


def test_pointer_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[pw.Pointer | None],
        [[api.ref_scalar(42), api.ref_scalar(43)], [None, None]],
    )


def test_json_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[pw.Json | None],
        [
            [pw.Json({"a": 1}), pw.Json([1, 2, 3])],
            [pw.Json("string"), None],
            [None, pw.Json(42)],
        ],
    )


def test_json_array_with_commas_in_values(mssql):
    _test_mssql_streaming(
        mssql,
        list[pw.Json | None],
        [
            [pw.Json({"a": 1, "b": 2, "c": 3}), pw.Json({"x": [1, 2, 3]})],
            [pw.Json({"nested": {"key": "val,with,commas"}}), None],
        ],
    )


def test_2d_bool_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[list[bool]],
        [
            [[True, False], [False, True]],
            [[True, True], [False, False]],
        ],
    )


def test_2d_int_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[list[int]],
        [
            [[1, 2], [3, 4]],
            [[5, 6], [7, 8]],
        ],
    )


def test_2d_float_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[list[float]],
        [
            [[1.1, 2.2], [3.3, 4.4]],
            [[5.5, 6.6], [7.7, 8.8]],
        ],
    )


def test_2d_str_array(mssql):
    _test_mssql_streaming(
        mssql,
        list[list[str]],
        [
            [["a", "b"], ["c", "d"]],
            [["w", "x"], ["y", "z"]],
        ],
    )


def test_2d_json_array(mssql):
    _test_mssql_streaming(
        mssql,
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


def test_empty_list(mssql):
    _test_mssql_streaming(mssql, list[int], [[]])


def test_pyobject_wrapper(mssql):
    _test_mssql_streaming(
        mssql,
        pw.PyObjectWrapper[SimpleObject],
        [
            pw.wrap_py_object(SimpleObject(1)),
            pw.wrap_py_object(SimpleObject(2)),
            pw.wrap_py_object(SimpleObject(3)),
        ],
    )


def test_tuple(mssql):
    _test_mssql_streaming(
        mssql,
        tuple[int, str, bool],
        [(1, "hello", True), (2, "world", False), (-1, "", True)],
    )


def test_int_ndarray_unspecified_dim(mssql):
    _test_mssql_streaming(
        mssql,
        np.ndarray[typing.Any, int],  # type: ignore
        [
            np.array([1, 2, 3], dtype=np.int64),
            np.array([[1, 2], [3, 4]], dtype=np.int64),
        ],
        _create_ndarray_table,
    )


def test_float_ndarray_unspecified_dim(mssql):
    _test_mssql_streaming(
        mssql,
        np.ndarray[typing.Any, float],  # type: ignore
        [
            np.array([1.1, 2.2, 3.3], dtype=np.float64),
            np.array([[1.1, 2.2], [3.3, 4.4]], dtype=np.float64),
        ],
        _create_ndarray_table,
    )


def test_int_ndarray_1d(mssql):
    _test_mssql_streaming(
        mssql,
        np.ndarray[tuple[int], int],  # type: ignore
        [
            np.array([1, 2, 3], dtype=np.int64),
            np.array([-1, 0, 1], dtype=np.int64),
        ],
        _create_ndarray_table,
    )


def test_float_ndarray_1d(mssql):
    _test_mssql_streaming(
        mssql,
        np.ndarray[tuple[int], float],  # type: ignore
        [
            np.array([1.1, 2.2, 3.3], dtype=np.float64),
            np.array([-1.5, 0.0, 3.14], dtype=np.float64),
        ],
        _create_ndarray_table,
    )


def test_int_ndarray_2d(mssql):
    _test_mssql_streaming(
        mssql,
        np.ndarray[tuple[int, int], int],  # type: ignore
        [
            np.array([[1, 2], [3, 4]], dtype=np.int64),
            np.array([[5, 6], [7, 8]], dtype=np.int64),
        ],
        _create_ndarray_table,
    )


def test_float_ndarray_2d(mssql):
    _test_mssql_streaming(
        mssql,
        np.ndarray[tuple[int, int], float],  # type: ignore
        [
            np.array([[1.1, 2.2], [3.3, 4.4]], dtype=np.float64),
            np.array([[5.5, 6.6], [7.7, 8.8]], dtype=np.float64),
        ],
        _create_ndarray_table,
    )

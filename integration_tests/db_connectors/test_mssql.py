# Copyright © 2026 Pathway

import datetime
import json
import multiprocessing
import pathlib
import random
import threading
import time
import traceback
import uuid

import pandas as pd
import pytest
from utils import (
    MSSQL_CONNECTION_STRING,
    MSSQL_DB_HOST,
    MSSQL_DB_PASSWORD,
    MSSQL_DB_PORT,
    MSSQL_DB_USER,
    SimpleObject,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    read_jsonlines,
    wait_result_with_checker,
)

pytestmark = pytest.mark.xdist_group("mssql")


@pytest.mark.parametrize("output_table_type", ["stream_of_changes", "snapshot"])
@pytest.mark.parametrize("init_mode", ["default", "create_if_not_exists", "replace"])
def test_mssql_write_outputs(output_table_type, init_mode, mssql):
    table_name = mssql.random_table_name()

    class InputSchema(pw.Schema):
        row_id: int = pw.column_definition(primary_key=True)
        flag: bool
        age: float
        title: str

    def run(offset: int):
        G.clear()
        table = pw.demo.generate_custom_stream(
            value_generators={
                "row_id": lambda x: x + offset,
                "flag": lambda x: (x + offset) % 2 == 0,
                "age": lambda x: x + offset + 0.5,
                "title": lambda x: f"Record {x + offset}",
            },
            schema=InputSchema,
            nb_rows=5,
            autocommit_duration_ms=100,
        )
        extra_kwargs = {}
        if output_table_type == "snapshot":
            extra_kwargs["primary_key"] = [table.row_id]
        pw.io.mssql.write(
            table,
            MSSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode=init_mode,
            output_table_type=output_table_type,
            **extra_kwargs,
        )
        pw.run()

    if init_mode == "default":
        with pytest.raises(pw.engine.EngineError):
            run(0)
        add_special_fields = output_table_type == "stream_of_changes"
        table_name = mssql.create_table(
            InputSchema, add_special_fields=add_special_fields
        )

    run(0)
    contents = mssql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
    contents.sort(key=lambda item: item["row_id"])
    assert contents == [
        {"row_id": 0, "flag": True, "age": 0.5, "title": "Record 0"},
        {"row_id": 1, "flag": False, "age": 1.5, "title": "Record 1"},
        {"row_id": 2, "flag": True, "age": 2.5, "title": "Record 2"},
        {"row_id": 3, "flag": False, "age": 3.5, "title": "Record 3"},
        {"row_id": 4, "flag": True, "age": 4.5, "title": "Record 4"},
    ]

    run(5)
    contents = mssql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
    contents.sort(key=lambda item: item["row_id"])
    if init_mode == "replace":
        assert contents == [
            {"row_id": 5, "flag": False, "age": 5.5, "title": "Record 5"},
            {"row_id": 6, "flag": True, "age": 6.5, "title": "Record 6"},
            {"row_id": 7, "flag": False, "age": 7.5, "title": "Record 7"},
            {"row_id": 8, "flag": True, "age": 8.5, "title": "Record 8"},
            {"row_id": 9, "flag": False, "age": 9.5, "title": "Record 9"},
        ]
    else:
        assert contents == [
            {"row_id": 0, "flag": True, "age": 0.5, "title": "Record 0"},
            {"row_id": 1, "flag": False, "age": 1.5, "title": "Record 1"},
            {"row_id": 2, "flag": True, "age": 2.5, "title": "Record 2"},
            {"row_id": 3, "flag": False, "age": 3.5, "title": "Record 3"},
            {"row_id": 4, "flag": True, "age": 4.5, "title": "Record 4"},
            {"row_id": 5, "flag": False, "age": 5.5, "title": "Record 5"},
            {"row_id": 6, "flag": True, "age": 6.5, "title": "Record 6"},
            {"row_id": 7, "flag": False, "age": 7.5, "title": "Record 7"},
            {"row_id": 8, "flag": True, "age": 8.5, "title": "Record 8"},
            {"row_id": 9, "flag": False, "age": 9.5, "title": "Record 9"},
        ]


@pytest.mark.parametrize("are_types_optional", [False, True])
@pytest.mark.parametrize("init_mode", ["create_if_not_exists", "replace"])
def test_mssql_different_types(init_mode, are_types_optional, mssql):
    table_name = mssql.random_table_name()

    if are_types_optional:

        class InputSchema(pw.Schema):
            a: str | None
            b: float | None
            c: bool | None
            d: bytes | None
            e: api.Pointer | None
            f: pw.Json | None
            g: str
            h: str
            i: pw.PyObjectWrapper[SimpleObject] | None
            j: pw.Duration | None
            k: int | None

    else:

        class InputSchema(pw.Schema):  # type:ignore
            a: str
            b: float
            c: bool
            d: bytes
            e: api.Pointer
            f: pw.Json
            g: str
            h: str
            i: pw.PyObjectWrapper[SimpleObject]
            j: pw.Duration
            k: int

    rows = [
        {
            "a": "foo",
            "b": 1.5,
            "c": False,
            "d": bytes([0xFF, 0xFE, 0xC0, 0xC1]),
            "e": api.ref_scalar(42),
            "f": {"foo": "bar", "baz": 123},
            "g": "2025-03-14T10:13:00",
            "h": "2025-04-23T10:13:00+00:00",
            "i": pw.wrap_py_object(SimpleObject("test")),
            "j": pd.Timedelta("4 days 2 seconds 123 us"),
            "k": 42,
        }
    ]

    table = pw.debug.table_from_rows(
        InputSchema,
        [tuple(row.values()) for row in rows],
    ).with_columns(
        g=pw.this.g.dt.strptime("%Y-%m-%dT%H:%M:%S", contains_timezone=False),
        h=pw.this.h.dt.strptime("%Y-%m-%dT%H:%M:%S%z", contains_timezone=True),
    )
    if are_types_optional:
        table = table.update_types(
            g=pw.DateTimeNaive | None,
            h=pw.DateTimeUtc | None,
        )

    pw.io.mssql.write(
        table,
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode=init_mode,
    )
    pw.run()

    result = mssql.get_table_contents(table_name, InputSchema.column_names())

    for row in result:
        obj = api.deserialize(bytes(row["i"]))
        assert isinstance(
            obj, pw.PyObjectWrapper
        ), f"expecting PyObjectWrapper, got {type(obj)}"
        row["i"] = obj.value

    expected_result = {
        "a": "foo",
        "b": 1.5,
        "c": False,
        "d": bytes([0xFF, 0xFE, 0xC0, 0xC1]),
        "e": "^Z5QKEQCDK9ZZ6TSYV0PM0G92JC",
        "f": {"foo": "bar", "baz": 123},
        "g": datetime.datetime(2025, 3, 14, 10, 13),
        "h": datetime.datetime(2025, 4, 23, 10, 13, tzinfo=datetime.timezone.utc),
        "i": SimpleObject("test"),
        "j": pd.Timedelta("4 days 2 seconds 123 us"),
        "k": 42,
    }
    assert len(result) == 1
    for key, expected_value in expected_result.items():
        our_value = result[0][key]
        if key == "f":
            our_value = json.loads(our_value)
        elif key == "j":
            our_value = pd.Timedelta(microseconds=our_value)
        assert our_value == expected_value, key

    external_schema = mssql.get_table_schema(table_name)
    assert external_schema["a"].type_name == "nvarchar"
    assert external_schema["b"].type_name == "float"
    assert external_schema["c"].type_name == "bit"
    assert external_schema["d"].type_name == "varbinary"
    assert external_schema["e"].type_name == "nvarchar"
    assert external_schema["f"].type_name == "nvarchar"
    assert external_schema["g"].type_name == "datetime2"
    assert external_schema["h"].type_name == "datetimeoffset"
    assert external_schema["i"].type_name == "varbinary"
    assert external_schema["j"].type_name == "bigint"
    assert external_schema["k"].type_name == "bigint"
    for column_name, column_props in external_schema.items():
        if column_name in ("time", "diff"):
            assert not column_props.is_nullable
        else:
            assert column_props.is_nullable == are_types_optional, column_name


def test_mssql_snapshot_overwrite_by_key(mssql):
    words = ["one", "two", "three", "one", "four", "two", "one", "one", "two", "four"]

    class InputSchema(pw.Schema):
        word: str

    table = pw.demo.generate_custom_stream(
        value_generators={"word": lambda index: words[index]},
        schema=InputSchema,
        nb_rows=10,
        autocommit_duration_ms=100,
    )
    result = table.groupby(table.word).reduce(table.word, count=pw.reducers.count())
    table_name = mssql.create_table(result.schema, add_special_fields=False)
    pw.io.mssql.write(
        result,
        MSSQL_CONNECTION_STRING,
        table_name=table_name,
        output_table_type="snapshot",
        primary_key=[result.word],
    )
    pw.run()

    assert len(mssql.get_table_contents(table_name, ["word", "count"])) == 4


def test_mssql_overwrites_old_snapshot(mssql):
    table_name = mssql.random_table_name()

    class InputSchema(pw.Schema):
        row_id: int = pw.column_definition(primary_key=True)
        flag: bool
        age: float
        title: str

    def run(offset: int):
        G.clear()
        table = pw.demo.generate_custom_stream(
            value_generators={
                "row_id": lambda x: (x + offset) % 10,
                "flag": lambda x: (x + offset) % 2 == 0,
                "age": lambda x: x + offset + 0.5,
                "title": lambda x: f"Record {x + offset}",
            },
            schema=InputSchema,
            nb_rows=5,
            autocommit_duration_ms=100,
        )
        pw.io.mssql.write(
            table,
            MSSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.row_id],
        )
        pw.run()

    run(0)
    contents = mssql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
    contents.sort(key=lambda item: item["row_id"])
    assert contents == [
        {"row_id": 0, "flag": True, "age": 0.5, "title": "Record 0"},
        {"row_id": 1, "flag": False, "age": 1.5, "title": "Record 1"},
        {"row_id": 2, "flag": True, "age": 2.5, "title": "Record 2"},
        {"row_id": 3, "flag": False, "age": 3.5, "title": "Record 3"},
        {"row_id": 4, "flag": True, "age": 4.5, "title": "Record 4"},
    ]

    run(7)
    contents = mssql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
    contents.sort(key=lambda item: item["row_id"])
    assert contents == [
        {"row_id": 0, "flag": True, "age": 10.5, "title": "Record 10"},
        {"row_id": 1, "flag": False, "age": 11.5, "title": "Record 11"},
        {"row_id": 2, "flag": True, "age": 2.5, "title": "Record 2"},
        {"row_id": 3, "flag": False, "age": 3.5, "title": "Record 3"},
        {"row_id": 4, "flag": True, "age": 4.5, "title": "Record 4"},
        {"row_id": 7, "flag": False, "age": 7.5, "title": "Record 7"},
        {"row_id": 8, "flag": True, "age": 8.5, "title": "Record 8"},
        {"row_id": 9, "flag": False, "age": 9.5, "title": "Record 9"},
    ]


def test_mssql_composite_snapshot_key(mssql):
    table_name = mssql.random_table_name()

    class InputSchema(pw.Schema):
        x: int = pw.column_definition(primary_key=True)
        y: int = pw.column_definition(primary_key=True)
        flag: bool
        age: float
        title: str

    def run(offset: int):
        G.clear()
        table = pw.demo.generate_custom_stream(
            value_generators={
                "x": lambda x: (x + offset) % 2,
                "y": lambda x: (x + offset) % 4 - (x + offset) % 2,
                "flag": lambda x: (x + offset) % 2 == 0,
                "age": lambda x: x + offset + 0.5,
                "title": lambda x: f"Record {x + offset}",
            },
            schema=InputSchema,
            nb_rows=16,
            autocommit_duration_ms=100,
        )
        pw.io.mssql.write(
            table,
            MSSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.x, table.y],
        )
        pw.run()

    run(0)
    contents = mssql.get_table_contents(table_name, ["x", "y", "flag", "age", "title"])
    contents.sort(key=lambda item: item["x"] + item["y"])
    assert contents == [
        {"x": 0, "y": 0, "flag": True, "age": 12.5, "title": "Record 12"},
        {"x": 1, "y": 0, "flag": False, "age": 13.5, "title": "Record 13"},
        {"x": 0, "y": 2, "flag": True, "age": 14.5, "title": "Record 14"},
        {"x": 1, "y": 2, "flag": False, "age": 15.5, "title": "Record 15"},
    ]


@pytest.mark.parametrize("malformed_primary_key", [None, []])
def test_mssql_no_snapshot_key(malformed_primary_key, mssql):
    table_name = mssql.random_table_name()

    class InputSchema(pw.Schema):
        x: int
        y: int

    table = pw.demo.generate_custom_stream(
        value_generators={
            "x": lambda x: x * 2,
            "y": lambda x: x * 3,
        },
        schema=InputSchema,
        nb_rows=16,
        autocommit_duration_ms=100,
    )

    with pytest.raises(
        ValueError,
        match="primary key field names must be specified for a snapshot mode",
    ):
        pw.io.mssql.write(
            table,
            MSSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=malformed_primary_key,
        )
        pw.run()


def test_mssql_single_column_snapshot_mode(mssql):
    table_name = mssql.random_table_name()

    class InputSchema(pw.Schema):
        x: int = pw.column_definition(primary_key=True)

    table = pw.demo.generate_custom_stream(
        value_generators={"x": lambda x: x},
        schema=InputSchema,
        nb_rows=5,
        autocommit_duration_ms=100,
    )
    pw.io.mssql.write(
        table,
        MSSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode="create_if_not_exists",
        output_table_type="snapshot",
        primary_key=[table.x],
    )
    pw.run()
    contents = mssql.get_table_contents(table_name, ["x"])
    contents.sort(key=lambda item: item["x"])
    assert contents == [{"x": 0}, {"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]


def test_mssql_read_basic(mssql, tmp_path):
    """Test basic read from an MSSQL table."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  sensor_name NVARCHAR(100) NOT NULL PRIMARY KEY,"
        f"  temperature FLOAT NOT NULL,"
        f"  humidity FLOAT NOT NULL"
        f")"
    )
    mssql.insert_row(
        table_name, {"sensor_name": "A", "temperature": 22.5, "humidity": 45.0}
    )
    mssql.insert_row(
        table_name, {"sensor_name": "B", "temperature": 23.1, "humidity": 42.3}
    )
    mssql.insert_row(
        table_name, {"sensor_name": "C", "temperature": 21.0, "humidity": 50.2}
    )

    class SensorSchema(pw.Schema):
        sensor_name: str = pw.column_definition(primary_key=True)
        temperature: float
        humidity: float

    output_path = tmp_path / "output.jsonl"

    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=table_name,
        schema=SensorSchema,
        mode="static",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, str(output_path))

    def run_pw():
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    t = threading.Thread(target=run_pw, daemon=True)
    t.start()

    wait_result_with_checker(
        FileLinesNumberChecker(output_path, 3), timeout_sec=30, target=None
    )
    records = [json.loads(line) for line in output_path.read_text().splitlines()]
    sensor_names = sorted([r["sensor_name"] for r in records])
    assert sensor_names == ["A", "B", "C"]


def test_mssql_read_write_roundtrip(mssql, tmp_path):
    """Test reading from MSSQL, transforming, and writing back."""
    input_table = mssql.random_table_name()
    output_table = mssql.random_table_name()

    mssql.execute_sql(
        f"CREATE TABLE {input_table} ("
        f"  name NVARCHAR(100) NOT NULL PRIMARY KEY,"
        f"  value FLOAT NOT NULL,"
        f"  score TINYINT NOT NULL"
        f")"
    )
    mssql.insert_row(input_table, {"name": "x", "value": 10.0, "score": 1})
    mssql.insert_row(input_table, {"name": "y", "value": 20.0, "score": 2})
    mssql.insert_row(input_table, {"name": "z", "value": 30.0, "score": 255})

    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        value: float
        score: int

    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=input_table,
        schema=InputSchema,
        mode="static",
        autocommit_duration_ms=100,
    )

    # Double the values
    transformed = table.select(
        name=pw.this.name, value=pw.this.value * 2, score=pw.this.score
    )

    pw.io.mssql.write(
        transformed,
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=output_table,
        init_mode="create_if_not_exists",
    )

    def run_pw():
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    t = threading.Thread(target=run_pw, daemon=True)
    t.start()

    def _output_table_has_three_rows() -> bool:
        try:
            return (
                len(mssql.get_table_contents(output_table, ["name", "value", "score"]))
                >= 3
            )
        except Exception:
            return False

    wait_result_with_checker(_output_table_has_three_rows, timeout_sec=30, target=None)
    contents = mssql.get_table_contents(output_table, ["name", "value", "score"])
    contents.sort(key=lambda item: item["name"])
    assert len(contents) == 3
    assert contents[0]["name"] == "x"
    assert contents[0]["value"] == 20.0
    assert contents[0]["score"] == 1
    assert contents[1]["name"] == "y"
    assert contents[1]["value"] == 40.0
    assert contents[1]["score"] == 2
    assert contents[2]["name"] == "z"
    assert contents[2]["value"] == 60.0
    assert contents[2]["score"] == 255


def _start_mssql_cdc_streaming_reader(
    table_name: str,
    schema: type[pw.Schema],
    output_path: pathlib.Path,
) -> multiprocessing.Process:
    """Start ``pw.io.mssql.read(mode='streaming') -> jsonlines.write`` in a
    forked process.

    A forked subprocess (instead of a daemon thread) is mandatory: the engine
    keeps polling CDC indefinitely, and the test must be able to *stop* it
    before the fixture cleanup drops the table.  With a daemon thread there
    was no way to signal the engine to quit — the cleanup dropped the table
    while the thread was still polling, and the engine then thrashed CDC
    queries against a non-existent capture instance for as long as the thread
    survived (every retry opening a fresh TCP connection).  Across xdist
    workers that thrash exhausted the host's ephemeral source-port range and
    starved unrelated tests of MSSQL connections, which is what was tipping
    the persistence tests over their 60 s checker timeout.

    Caller is responsible for terminating + joining the returned process.
    """

    def worker():
        G.clear()
        table = pw.io.mssql.read(
            connection_string=MSSQL_CONNECTION_STRING,
            table_name=table_name,
            schema=schema,
            mode="streaming",
            autocommit_duration_ms=100,
        )
        pw.io.jsonlines.write(table, str(output_path))
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    proc = multiprocessing.Process(target=worker, daemon=True)
    proc.start()
    return proc


def _stop_mssql_cdc_streaming_reader(proc: multiprocessing.Process) -> None:
    """Reverse of ``_start_mssql_cdc_streaming_reader``: SIGTERM, fall back to
    SIGKILL if the engine doesn't exit promptly.  Always called in a finally
    so a failing assertion still drains the engine before fixture teardown.
    """
    proc.terminate()
    proc.join(timeout=10)
    if proc.is_alive():
        proc.kill()
        proc.join()


def test_mssql_cdc_read_inserts(mssql, tmp_path):
    """Test CDC reader detects inserts in real-time."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    mssql.enable_cdc(table_name)
    mssql.insert_row(table_name, {"id": 1, "name": "Alice"})
    mssql.insert_row(table_name, {"id": 2, "name": "Bob"})

    class TestSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        name: str

    output_path = tmp_path / "cdc_output.jsonl"
    proc = _start_mssql_cdc_streaming_reader(table_name, TestSchema, output_path)
    try:
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 2), timeout_sec=30, target=None
        )
        mssql.insert_row(table_name, {"id": 3, "name": "Charlie"})
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 3), timeout_sec=30, target=None
        )
        records = [json.loads(line) for line in output_path.read_text().splitlines()]
        names = sorted(r["name"] for r in records)
        assert "Charlie" in names
    finally:
        _stop_mssql_cdc_streaming_reader(proc)


def test_mssql_cdc_read_updates(mssql, tmp_path):
    """Test CDC reader detects updates (emits delete + insert)."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  value NVARCHAR(100) NOT NULL"
        f")"
    )
    mssql.enable_cdc(table_name)
    mssql.insert_row(table_name, {"id": 1, "value": "original"})

    class TestSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "cdc_update_output.jsonl"
    proc = _start_mssql_cdc_streaming_reader(table_name, TestSchema, output_path)
    try:
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 1), timeout_sec=30, target=None
        )
        # An UPDATE under "all update old" emits two CDC events — pre-image
        # (delete) and post-image (insert) — so the file grows from 1 to 3 lines.
        mssql.execute_sql(f"UPDATE {table_name} SET value = 'updated' WHERE id = 1")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 3), timeout_sec=30, target=None
        )
        records = [json.loads(line) for line in output_path.read_text().splitlines()]
        values = [r["value"] for r in records]
        assert "updated" in values
    finally:
        _stop_mssql_cdc_streaming_reader(proc)


def test_mssql_cdc_read_deletes(mssql, tmp_path):
    """Test CDC reader detects deletes."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    mssql.enable_cdc(table_name)
    mssql.insert_row(table_name, {"id": 1, "name": "ToDelete"})
    mssql.insert_row(table_name, {"id": 2, "name": "ToKeep"})

    class TestSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        name: str

    output_path = tmp_path / "cdc_delete_output.jsonl"
    proc = _start_mssql_cdc_streaming_reader(table_name, TestSchema, output_path)
    try:
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 2), timeout_sec=30, target=None
        )
        mssql.execute_sql(f"DELETE FROM {table_name} WHERE id = 1")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 3), timeout_sec=30, target=None
        )
    finally:
        _stop_mssql_cdc_streaming_reader(proc)


def test_mssql_read_reserved_word_columns(mssql, tmp_path):
    """Regression: column names that are SQL reserved words must be bracket-quoted
    in the snapshot SELECT. Without the fix the generated query is
    ``SELECT key,order FROM [dbo].[table]`` which SQL Server rejects."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE [{table_name}] ("
        f"  [key] NVARCHAR(100) NOT NULL PRIMARY KEY,"
        f"  [order] BIGINT NOT NULL"
        f")"
    )
    mssql.execute_sql(
        f"INSERT INTO [{table_name}] ([key], [order]) VALUES ('alpha', 1)"
    )
    mssql.execute_sql(f"INSERT INTO [{table_name}] ([key], [order]) VALUES ('beta', 2)")

    class ReservedSchema(pw.Schema):
        key: str = pw.column_definition(primary_key=True)
        order: int

    output_path = tmp_path / "output.jsonl"
    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=table_name,
        schema=ReservedSchema,
        mode="static",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, str(output_path))
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    lines = [ln for ln in output_path.read_text().strip().split("\n") if ln]
    records = [json.loads(ln) for ln in lines]
    assert sorted((r["key"], r["order"]) for r in records) == [
        ("alpha", 1),
        ("beta", 2),
    ]


def test_mssql_snapshot_write_reserved_word_columns(mssql):
    """Regression: column names that are SQL reserved words must be bracket-quoted
    in the MERGE statement used for snapshot upserts. Without the fix the generated
    MERGE contains bare ``key`` and ``order`` identifiers which SQL Server rejects."""
    table_name = mssql.random_table_name()

    class ReservedSchema(pw.Schema):
        key: str = pw.column_definition(primary_key=True)
        order: int

    table = pw.debug.table_from_rows(
        ReservedSchema,
        [("alpha", 1), ("beta", 2)],
    )
    pw.io.mssql.write(
        table,
        MSSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode="create_if_not_exists",
        output_table_type="snapshot",
        primary_key=[table.key],
    )
    pw.run()

    mssql.execute_sql(f"SELECT [key], [order] FROM [{table_name}] ORDER BY [key]")
    rows = list(mssql.cursor.fetchall())
    assert rows == [("alpha", 1), ("beta", 2)]


def test_mssql_write_null_non_string_columns(mssql):
    """Check that NULL values round-trip correctly for every non-string optional
    column type. The write path currently binds all NULLs as Option::<String>::None
    (a typed NVARCHAR NULL in TDS); this test exposes whether SQL Server accepts
    that for BIGINT, FLOAT, BIT, and VARBINARY columns."""
    table_name = mssql.random_table_name()

    class NullableSchema(pw.Schema):
        row_id: int = pw.column_definition(primary_key=True)
        int_val: int | None
        float_val: float | None
        bool_val: bool | None
        bytes_val: bytes | None

    table = pw.debug.table_from_rows(
        NullableSchema,
        [
            (1, 42, 3.14, True, b"\x01\x02"),  # all fields populated
            (2, None, None, None, None),  # all optional fields null
        ],
    )
    pw.io.mssql.write(
        table,
        MSSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode="create_if_not_exists",
    )
    pw.run()

    mssql.execute_sql(
        f"SELECT [row_id],[int_val],[float_val],[bool_val],[bytes_val] "
        f"FROM [{table_name}] ORDER BY [row_id]"
    )
    db_rows = list(mssql.cursor.fetchall())
    assert len(db_rows) == 2

    r1_id, r1_int, r1_float, r1_bool, r1_bytes = db_rows[0]
    assert r1_id == 1
    assert r1_int == 42
    assert abs(r1_float - 3.14) < 1e-6
    assert r1_bool
    assert r1_bytes == b"\x01\x02"

    r2_id, r2_int, r2_float, r2_bool, r2_bytes = db_rows[1]
    assert r2_id == 2
    assert r2_int is None
    assert r2_float is None
    assert r2_bool is None
    assert r2_bytes is None


def test_mssql_streaming_requires_cdc_on_table(tmp_path, mssql):
    """Streaming mode on a table without `sp_cdc_enable_table` must fail with
    the specific `CdcNotEnabledOnTable` error.  The database does have CDC
    enabled (docker init runs `sp_cdc_enable_db`), so this exercises the
    table-level branch of `probe_cdc_availability`."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    # Deliberately NOT calling mssql.enable_cdc(table_name).

    error_path = tmp_path / "error.log"
    output_path = tmp_path / "output.jsonl"
    p = multiprocessing.Process(
        target=_mssql_persistence_worker,
        kwargs={
            "connection_string": MSSQL_CONNECTION_STRING,
            "table_name": table_name,
            "output_path": str(output_path),
            "mode": "streaming",
            # Any persistence_config; we only want to observe the probe error.
            "persistence_config": pw.persistence.Config(
                backend=pw.persistence.Backend.filesystem(tmp_path / "PStorage")
            ),
            "error_path": str(error_path),
        },
    )
    p.start()
    p.join(timeout=60)
    if p.is_alive():
        p.terminate()
        p.join()
        raise AssertionError("worker should have failed fast, not hung")
    assert p.exitcode not in (None, 0)
    error_text = error_path.read_text() if error_path.exists() else ""
    assert (
        "CDC is not enabled on table" in error_text
    ), f"expected CdcNotEnabledOnTable error, got:\n{error_text}"


def test_mssql_streaming_requires_cdc_on_database(tmp_path, mssql):
    """Streaming mode against a database where `sp_cdc_enable_db` was never
    run must fail with `CdcNotEnabledOnDatabase`.  We can't disable CDC on
    the shared `testdb` without breaking every other test, so the test
    creates a throw-away database, connects to *that*, and tears it down in
    a finally."""
    db_name = f"nocdc_{uuid.uuid4().hex[:12]}"
    import pymssql

    # CREATE/ALTER/DROP DATABASE all hold heavy locks on master.sys.databases;
    # under concurrent CDC test load they're occasionally elected as deadlock
    # victims.  The retry-aware ``execute_sql`` reruns the statement on 1205,
    # which is what SQL Server's deadlock-victim contract asks for.
    mssql.execute_sql(f"CREATE DATABASE {db_name}")
    try:
        # Create a table inside the new DB via its own connection.
        db_conn = pymssql.connect(
            server=MSSQL_DB_HOST,
            port=MSSQL_DB_PORT,
            user=MSSQL_DB_USER,
            password=MSSQL_DB_PASSWORD,
            database=db_name,
            autocommit=True,
            tds_version="7.3",
        )
        db_cur = db_conn.cursor()
        db_cur.execute(
            "CREATE TABLE dbo.t ("
            " id INT PRIMARY KEY,"
            " name NVARCHAR(100) NOT NULL"
            ")"
        )
        db_cur.execute("INSERT INTO dbo.t VALUES (1, 'x')")
        db_conn.close()

        no_cdc_connection_string = (
            f"Server=tcp:{MSSQL_DB_HOST},{MSSQL_DB_PORT};"
            f"Database={db_name};"
            f"User Id={MSSQL_DB_USER};Password={MSSQL_DB_PASSWORD};"
            f"TrustServerCertificate=true"
        )

        error_path = tmp_path / "error.log"
        output_path = tmp_path / "output.jsonl"
        p = multiprocessing.Process(
            target=_mssql_persistence_worker,
            kwargs={
                "connection_string": no_cdc_connection_string,
                "table_name": "t",
                "output_path": str(output_path),
                "mode": "streaming",
                "persistence_config": pw.persistence.Config(
                    backend=pw.persistence.Backend.filesystem(tmp_path / "PStorage")
                ),
                "error_path": str(error_path),
            },
        )
        p.start()
        p.join(timeout=60)
        if p.is_alive():
            p.terminate()
            p.join()
            raise AssertionError("worker should have failed fast, not hung")
        assert p.exitcode not in (None, 0)
        error_text = error_path.read_text() if error_path.exists() else ""
        assert (
            "CDC is not enabled on the current database" in error_text
        ), f"expected CdcNotEnabledOnDatabase error, got:\n{error_text}"
    finally:
        mssql.execute_sql(
            f"ALTER DATABASE {db_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
        )
        mssql.execute_sql(f"DROP DATABASE {db_name}")


def test_mssql_read_custom_schema_name(mssql, tmp_path):
    """The `schema_name` parameter must be threaded through to both the
    `SELECT` against the table and the `OBJECT_ID` lookup used by CDC
    queries.  This test creates a non-`dbo` schema, puts a table in it,
    and reads it back in static mode."""
    schema_name = f"s_{uuid.uuid4().hex[:8]}"
    table_name = mssql.random_table_name()
    mssql.execute_sql(f"CREATE SCHEMA {schema_name}")
    try:
        mssql.execute_sql(
            f"CREATE TABLE {schema_name}.{table_name} ("
            f"  id INT PRIMARY KEY,"
            f"  name NVARCHAR(100) NOT NULL"
            f")"
        )
        mssql.execute_sql(
            f"INSERT INTO {schema_name}.{table_name} VALUES "
            f"(1, N'alpha'), (2, N'beta')"
        )

        class TableSchema(pw.Schema):
            id: int = pw.column_definition(primary_key=True)
            name: str

        output_path = tmp_path / "output.jsonl"
        G.clear()
        table = pw.io.mssql.read(
            connection_string=MSSQL_CONNECTION_STRING,
            table_name=table_name,
            schema=TableSchema,
            schema_name=schema_name,
            mode="static",
            autocommit_duration_ms=100,
        )
        pw.io.jsonlines.write(table, str(output_path))
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        rows = sorted((r["id"], r["name"]) for r in read_jsonlines(output_path))
        assert rows == [(1, "alpha"), (2, "beta")]
    finally:
        # Drop the schema's objects before the schema itself.  Both DROPs go
        # through the deadlock-retry helper — concurrent xdist workers fight
        # on system-table locks, and a bare ``DROP TABLE`` would surface the
        # 1205 deadlock victim error as a test failure even though the test
        # body succeeded.
        mssql.drop_table(table_name, schema_name=schema_name)
        mssql.drop_schema(schema_name)


# ---------------------------------------------------------------------------
# CDC persistence tests.
#
# The MSSQL CDC reader persists the last consumed LSN as an MssqlCdcLsn
# offset.  On restart seek() picks the LSN up, skips the full table snapshot,
# and replays the CDC window (saved_lsn, current_max_lsn] as a single
# NewSource/FinishedSource block.  The same mechanism works in both reader
# modes: in streaming mode the worker keeps running after the catch-up, in
# static mode it emits the delta and terminates.
# ---------------------------------------------------------------------------


def _mssql_persistence_worker(
    connection_string: str,
    table_name: str,
    output_path: str,
    mode: str,
    persistence_config: pw.persistence.Config,
    error_path: str | None = None,
) -> None:
    """Build the CDC-read pipeline used by the persistence tests.

    Invoked both directly by `test_mssql_cdc_persistence` (static mode, which
    terminates on its own) and via `wait_result_with_checker` +
    `multiprocessing.Process` (streaming mode, which runs until killed).  It
    must therefore be importable at module scope — the forked child
    re-invokes the same callable by reference.

    Retries ``pw.run`` on transient OS-level TCP source-port exhaustion: under
    heavy CI parallelism (multiple xdist workers spawning Pathway subprocesses,
    each opening fresh connections to MSSQL), ``connect(2)`` occasionally
    returns ``EADDRNOTAVAIL`` because the ephemeral source-port range is
    saturated by sockets sitting in ``TIME_WAIT``.  Tiberius surfaces this as
    ``EngineError("Cannot assign requested address (os error 99)")`` — a short
    backoff lets the kernel recycle source ports.  Persistence makes the retry
    safe: if Run 1 dies before any rows are written, the persistence directory
    is empty and we re-emit the full snapshot; if Run 2 dies, the saved offset
    from Run 1 is still on disk and the retry resumes from there.

    If `error_path` is provided, any exception raised by `pw.run` after the
    last attempt is written to that file before being re-raised — this lets
    the expired-LSN test assert on the specific error message without having
    to tap into pytest's stderr capture, which doesn't cover multiprocessing
    child processes.
    """

    def _build_and_run():
        G.clear()

        class InputSchema(pw.Schema):
            id: int = pw.column_definition(primary_key=True)
            name: str

        table = pw.io.mssql.read(
            connection_string=connection_string,
            table_name=table_name,
            schema=InputSchema,
            mode=mode,
            autocommit_duration_ms=100,
            name="mssql_persistence_source",
        )
        pw.io.jsonlines.write(table, output_path)
        pw.run(
            persistence_config=persistence_config,
            monitoring_level=pw.MonitoringLevel.NONE,
        )

    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            _build_and_run()
            return
        except Exception as e:
            transient = "Cannot assign requested address" in str(e)
            if transient and attempt < max_attempts - 1:
                # The previous attempt may have written partial output.  Reset
                # so the test sees a clean output file from the successful run.
                try:
                    open(output_path, "w").close()
                except Exception:
                    pass
                time.sleep(2.0 * (attempt + 1) + random.uniform(0, 1.0))
                continue
            if error_path is not None:
                with open(error_path, "w") as f:
                    traceback.print_exc(file=f)
            raise


def _extract_row(r: dict) -> dict:
    return {"id": r["id"], "name": r["name"], "diff": r["diff"]}


def _sort_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (r["id"], r["name"], r["diff"]))


# Each plan:
#   initial   – rows inserted before run 1
#   changes   – (op, payload) tuples applied between runs; payload is a dict
#               interpreted per-op:
#                 insert  -> INSERT {id, name}
#                 delete  -> DELETE WHERE id = {id}
#                 update  -> UPDATE SET name = {name} WHERE id = {id}
#   run1_expected – expected jsonl rows after run 1 (all diff=+1 because the
#                   connector dumps the full table snapshot)
#   run2_expected – expected jsonl rows after run 2 (only the CDC delta)
_MSSQL_CDC_PERSISTENCE_PLANS = [
    pytest.param(
        {
            "initial": [(1, "Alice"), (2, "Bob")],
            "changes": [
                ("insert", {"id": 3, "name": "Charlie"}),
                ("insert", {"id": 4, "name": "Dana"}),
            ],
            "run1_expected": [
                {"id": 1, "name": "Alice", "diff": 1},
                {"id": 2, "name": "Bob", "diff": 1},
            ],
            "run2_expected": [
                {"id": 3, "name": "Charlie", "diff": 1},
                {"id": 4, "name": "Dana", "diff": 1},
            ],
        },
        id="inserts_only",
    ),
    pytest.param(
        {
            "initial": [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            "changes": [
                ("delete", {"id": 2}),
            ],
            "run1_expected": [
                {"id": 1, "name": "Alice", "diff": 1},
                {"id": 2, "name": "Bob", "diff": 1},
                {"id": 3, "name": "Charlie", "diff": 1},
            ],
            "run2_expected": [
                {"id": 2, "name": "Bob", "diff": -1},
            ],
        },
        id="deletes_only",
    ),
    pytest.param(
        {
            "initial": [(1, "Alice"), (2, "Bob")],
            "changes": [
                ("update", {"id": 1, "name": "Alicia"}),
            ],
            "run1_expected": [
                {"id": 1, "name": "Alice", "diff": 1},
                {"id": 2, "name": "Bob", "diff": 1},
            ],
            "run2_expected": [
                {"id": 1, "name": "Alice", "diff": -1},
                {"id": 1, "name": "Alicia", "diff": 1},
            ],
        },
        id="updates_only",
    ),
    pytest.param(
        {
            "initial": [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            "changes": [
                ("insert", {"id": 4, "name": "Dana"}),
                ("delete", {"id": 2}),
                ("update", {"id": 3, "name": "Chuck"}),
            ],
            "run1_expected": [
                {"id": 1, "name": "Alice", "diff": 1},
                {"id": 2, "name": "Bob", "diff": 1},
                {"id": 3, "name": "Charlie", "diff": 1},
            ],
            "run2_expected": [
                {"id": 2, "name": "Bob", "diff": -1},
                {"id": 3, "name": "Charlie", "diff": -1},
                {"id": 3, "name": "Chuck", "diff": 1},
                {"id": 4, "name": "Dana", "diff": 1},
            ],
        },
        id="mixed",
    ),
    pytest.param(
        {
            "initial": [(1, "Alice"), (2, "Bob")],
            "changes": [],
            "run1_expected": [
                {"id": 1, "name": "Alice", "diff": 1},
                {"id": 2, "name": "Bob", "diff": 1},
            ],
            "run2_expected": [],
        },
        id="no_changes",
    ),
]


def _apply_mssql_change(mssql, table_name: str, op: str, payload: dict) -> None:
    if op == "insert":
        mssql.insert_row(table_name, payload)
    elif op == "delete":
        mssql.execute_sql(f"DELETE FROM {table_name} WHERE id = {int(payload['id'])}")
    elif op == "update":
        name = str(payload["name"]).replace("'", "''")
        mssql.execute_sql(
            f"UPDATE {table_name} SET name = N'{name}' "
            f"WHERE id = {int(payload['id'])}"
        )
    else:
        raise AssertionError(f"unsupported op {op!r}")


# CDC change-table row count produced by each operation when the reader queries
# ``fn_cdc_get_all_changes`` with ``'all update old'`` (the mode used by the
# Rust connector): inserts and deletes contribute one row, updates contribute
# two — pre-image (op=3) and post-image (op=4).
_MSSQL_CT_ROWS_PER_OP = {"insert": 1, "delete": 1, "update": 2}


def _expected_ct_row_count(plan_changes: list[tuple[str, dict]]) -> int:
    return sum(_MSSQL_CT_ROWS_PER_OP[op] for op, _ in plan_changes)


def _mssql_current_max_lsn(mssql) -> bytes | None:
    mssql.execute_sql("SELECT sys.fn_cdc_get_max_lsn()")
    row = mssql.cursor.fetchone()
    if row is None or row[0] is None:
        return None
    return bytes(row[0])


def _run_mssql_streaming_pipeline(
    *,
    table_name: str,
    output_path: pathlib.Path,
    persistence_config: pw.persistence.Config,
    expected_lines: int,
    double_check_interval: float | None,
) -> None:
    """Start the streaming worker, wait for `expected_lines` to appear, then
    let `wait_result_with_checker` flush + terminate it."""
    if expected_lines == 0:
        # Pre-create the output file so the zero-lines checker can see it.
        output_path.touch()
    wait_result_with_checker(
        FileLinesNumberChecker(output_path, expected_lines),
        timeout_sec=60,
        double_check_interval=double_check_interval,
        target=_mssql_persistence_worker,
        kwargs={
            "connection_string": MSSQL_CONNECTION_STRING,
            "table_name": table_name,
            "output_path": str(output_path),
            "mode": "streaming",
            "persistence_config": persistence_config,
        },
    )


def _run_mssql_static_pipeline(
    *,
    table_name: str,
    output_path: pathlib.Path,
    persistence_config: pw.persistence.Config,
) -> None:
    """Run the static-mode pipeline to completion.  Static reads terminate on
    their own after the full snapshot (or resume delta) has been written, so
    a plain `pw.run` in a forked child is all we need — no polling."""
    p = multiprocessing.Process(
        target=_mssql_persistence_worker,
        kwargs={
            "connection_string": MSSQL_CONNECTION_STRING,
            "table_name": table_name,
            "output_path": str(output_path),
            "mode": "static",
            "persistence_config": persistence_config,
        },
    )
    p.start()
    p.join(timeout=60)
    if p.is_alive():
        p.terminate()
        p.join()
        raise AssertionError("static-mode worker did not terminate within 60 s")
    assert p.exitcode == 0, f"static-mode worker exited with code {p.exitcode}"


@pytest.mark.parametrize("mode", ["streaming", "static"])
@pytest.mark.parametrize("plan", _MSSQL_CDC_PERSISTENCE_PLANS)
def test_mssql_cdc_persistence(tmp_path, mssql, mode, plan):
    """Two-run CDC persistence test for pw.io.mssql.read.

    Run 1: full table snapshot is emitted (all diff=+1).
    Run 2: only the CDC delta since the persisted LSN is emitted.

    Verified in both reader modes — streaming (worker keeps running, polled
    via `wait_result_with_checker`) and static (worker terminates on its
    own once the delta is consumed)."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    mssql.enable_cdc(table_name)

    for row_id, name in plan["initial"]:
        mssql.insert_row(table_name, {"id": row_id, "name": name})

    # The Rust reader's ``load_snapshot`` persists an offset derived from
    # ``fn_cdc_get_max_lsn()`` (database-wide).  If the capture agent has not
    # yet processed our inserts, that max reflects only *other* xdist workers'
    # captured transactions — and the persisted offset can sit below the LSNs
    # of the rows we are about to insert between Run 1 and Run 2, so Run 2's
    # ``(saved_lsn, current_max_lsn]`` window misses them.  Wait until the
    # capture agent has actually produced a CT row for each of our inserts.
    if plan["initial"]:
        mssql.wait_for_capture_count(table_name, len(plan["initial"]))

    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )

    def run(output_path: pathlib.Path, expected: list[dict]) -> None:
        if mode == "streaming":
            _run_mssql_streaming_pipeline(
                table_name=table_name,
                output_path=output_path,
                persistence_config=persistence_config,
                expected_lines=len(expected),
                # In the no_changes plan we must prove the output stays
                # empty.  double_check_interval gives the engine a second
                # chance to (not) produce output before we declare success.
                double_check_interval=5.0 if not expected else None,
            )
        else:
            _run_mssql_static_pipeline(
                table_name=table_name,
                output_path=output_path,
                persistence_config=persistence_config,
            )

    # Run 1.
    output_path_1 = tmp_path / "output_1.jsonl"
    run(output_path_1, plan["run1_expected"])
    assert _sort_rows(
        [_extract_row(r) for r in read_jsonlines(output_path_1)]
    ) == _sort_rows(plan["run1_expected"]), f"Run 1: expected {plan['run1_expected']}"

    for op, payload in plan["changes"]:
        _apply_mssql_change(mssql, table_name, op, payload)

    if plan["changes"]:
        # Same rationale as the pre-Run-1 wait: we need the capture agent to
        # actually produce CT rows for each of our changes before Run 2 queries
        # ``fn_cdc_get_max_lsn()``.  Updates show up as two CT rows under
        # ``all update old`` (pre + post images); inserts and deletes as one.
        target_count = len(plan["initial"]) + _expected_ct_row_count(plan["changes"])
        mssql.wait_for_capture_count(table_name, target_count)

    # Run 2.
    output_path_2 = tmp_path / "output_2.jsonl"
    run(output_path_2, plan["run2_expected"])
    assert _sort_rows(
        [_extract_row(r) for r in read_jsonlines(output_path_2)]
    ) == _sort_rows(plan["run2_expected"]), f"Run 2: expected {plan['run2_expected']}"


def test_mssql_static_persistence_without_cdc_errors(tmp_path, mssql):
    """Static mode + persistence enabled + CDC *not* enabled on the table.

    Without CDC there is no LSN to persist, so the connector refuses the
    configuration at `seek` time — probing CDC with `cdc_required=true`
    surfaces the same `CdcNotEnabledOnTable` error a streaming-mode user
    would get.  The message points at `sp_cdc_enable_table`, which is the
    action either user has to take.
    """
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    # Deliberately NOT calling mssql.enable_cdc(table_name) — that's the
    # scenario under test.
    mssql.insert_row(table_name, {"id": 1, "name": "Alice"})

    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )

    output_path = tmp_path / "output.jsonl"
    error_path = tmp_path / "error.log"
    p = multiprocessing.Process(
        target=_mssql_persistence_worker,
        kwargs={
            "connection_string": MSSQL_CONNECTION_STRING,
            "table_name": table_name,
            "output_path": str(output_path),
            "mode": "static",
            "persistence_config": persistence_config,
            "error_path": str(error_path),
        },
    )
    p.start()
    p.join(timeout=60)
    if p.is_alive():
        p.terminate()
        p.join()
        raise AssertionError("worker should have failed fast, not hung")
    assert p.exitcode not in (None, 0), "persistence-without-CDC must fail"
    error_text = error_path.read_text() if error_path.exists() else ""
    assert (
        "CDC is not enabled on table" in error_text
    ), f"expected CdcNotEnabledOnTable error, got:\n{error_text}"


def test_mssql_cdc_expired_lsn(tmp_path, mssql):
    """A persisted LSN that predates the CDC retention window must surface
    a specific CdcLsnOutOfRetention error on restart so the user knows to
    drop the persistence directory.

    The test reproduces the scenario without patching persistence files: after
    run 1 is over we insert additional rows to advance the CDC max LSN, then
    call `sys.sp_cdc_cleanup_change_table` with that newer LSN as the low
    water mark.  The procedure moves the capture instance's `start_lsn`
    forward, so `fn_cdc_get_min_lsn` in run 2 reports a value strictly
    greater than the persisted LSN — exactly the condition the retention
    check catches.
    """
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    mssql.enable_cdc(table_name)
    mssql.insert_row(table_name, {"id": 1, "name": "Alice"})
    mssql.insert_row(table_name, {"id": 2, "name": "Bob"})

    # Same race as in ``test_mssql_cdc_persistence``: until the agent has
    # actually written CT rows for our inserts, the LSN Run 1 persists is
    # determined by other workers' captured transactions and may sit below
    # our own LSNs.
    mssql.wait_for_capture_count(table_name, 2)

    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )

    # Run 1: populate persistence with a valid LSN.
    output_path_1 = tmp_path / "output_1.jsonl"
    wait_result_with_checker(
        FileLinesNumberChecker(output_path_1, 2),
        timeout_sec=60,
        target=_mssql_persistence_worker,
        kwargs={
            "connection_string": MSSQL_CONNECTION_STRING,
            "table_name": table_name,
            "output_path": str(output_path_1),
            "mode": "streaming",
            "persistence_config": persistence_config,
        },
    )
    assert len(read_jsonlines(output_path_1)) == 2

    # The database-wide max LSN sampled *immediately after* Run 1 finished is
    # an UPPER bound on whatever LSN Run 1 persisted (Run 1 cannot persist a
    # future LSN).  Use it as the target for cleanup: driving the capture
    # instance's min_lsn strictly past this point guarantees that Run 2 will
    # see ``saved_lsn < fn_cdc_get_min_lsn(instance)``.  A pre-Run-1 snapshot
    # would be a *lower* bound and leave the race unsolved.
    post_run1_max = _mssql_current_max_lsn(mssql)
    assert post_run1_max is not None, "expected CDC activity after Run 1"

    # Generate additional CDC traffic so the cleanup target advances beyond
    # post_run1_max.  Then wait for this instance's agent to pick them up so
    # sp_cdc_cleanup_change_table has something to cleanup strictly past
    # post_run1_max.
    mssql.insert_row(table_name, {"id": 3, "name": "Charlie"})
    mssql.insert_row(table_name, {"id": 4, "name": "Dana"})
    mssql.wait_for_capture_count(table_name, 4)

    capture_instance = f"dbo_{table_name}"
    # Once all four CT rows are present, ``fn_cdc_get_max_lsn()`` is at least
    # the LSN of id=4's commit, which is strictly greater than
    # ``post_run1_max``.  Re-poll briefly to absorb the small window between
    # the CT INSERT and the lsn_time_mapping update on the same transaction.
    wait_result_with_checker(
        lambda: (_mssql_current_max_lsn(mssql) or b"") > post_run1_max,
        timeout_sec=10,
        step=0.1,
        target=None,
    )
    max_lsn = _mssql_current_max_lsn(mssql)
    assert max_lsn is not None and max_lsn > post_run1_max

    # Drive the capture instance's start_lsn forward.  ``sp_cdc_cleanup_change_table``
    # competes with the capture agent for system-table locks under concurrent
    # test load and is occasionally elected as the deadlock victim — execute_sql
    # retries on 1205.  drain_status_rows discards the proc's "(N rows
    # affected)" output that pymssql would otherwise leave pending and trip up
    # the next statement.
    mssql.execute_sql(
        "EXEC sys.sp_cdc_cleanup_change_table "
        "@capture_instance=%s, @low_water_mark=%s",
        (capture_instance, max_lsn),
        drain_status_rows=True,
    )

    # Verify: the new min_lsn is strictly greater than post_run1_max — hence
    # strictly greater than anything Run 1 could have persisted.
    def _min_lsn_advanced() -> bool:
        mssql.execute_sql("SELECT sys.fn_cdc_get_min_lsn(%s)", (capture_instance,))
        row = mssql.cursor.fetchone()
        return row is not None and row[0] is not None and bytes(row[0]) > post_run1_max

    wait_result_with_checker(_min_lsn_advanced, timeout_sec=30, step=0.25, target=None)

    # Run 2: must fail fast with the specific retention error, not hang or
    # silently produce an empty delta.  We bypass wait_result_with_checker here
    # because it asserts the child exited with code 0, which contradicts the
    # expected behavior of this test.
    output_path_2 = tmp_path / "output_2.jsonl"
    error_path = tmp_path / "run2_error.log"
    p2 = multiprocessing.Process(
        target=_mssql_persistence_worker,
        kwargs={
            "connection_string": MSSQL_CONNECTION_STRING,
            "table_name": table_name,
            "output_path": str(output_path_2),
            "mode": "streaming",
            "persistence_config": persistence_config,
            "error_path": str(error_path),
        },
    )
    p2.start()
    try:
        p2.join(timeout=60)
    finally:
        if p2.is_alive():
            p2.terminate()
            p2.join()
    error_text = error_path.read_text() if error_path.exists() else ""
    assert (
        p2.exitcode is not None and p2.exitcode != 0
    ), f"Run 2 should have failed with CdcLsnOutOfRetention.\n{error_text}"
    assert (
        "persisted CDC position is outside the SQL Server retention window"
        in error_text
    ), f"Expected CdcLsnOutOfRetention error, got:\n{error_text}"

# Copyright © 2026 Pathway

import datetime
import json
import re
import threading
import time

import pandas as pd
import pytest
from utils import (
    MSSQL_CONNECTION_STRING,
    MSSQL_DB_NAME,
    SimpleObject,
    is_mssql_reachable,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G

xfail_if_mssql_failed_to_start = pytest.mark.xfail(
    not is_mssql_reachable(), reason="mssql has failed to start"
)


@xfail_if_mssql_failed_to_start
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


@xfail_if_mssql_failed_to_start
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
        "h": datetime.datetime(2025, 4, 23, 10, 13),
        "i": SimpleObject("test"),
        "j": pd.Timedelta("4 days 2 seconds 123 us"),
        "k": 42,
    }
    assert len(result) == 1
    for key, expected_value in expected_result.items():
        our_value = result[0][key]
        if key == "f":
            our_value = json.loads(our_value)
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


@xfail_if_mssql_failed_to_start
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


@xfail_if_mssql_failed_to_start
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


@xfail_if_mssql_failed_to_start
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


@xfail_if_mssql_failed_to_start
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


@xfail_if_mssql_failed_to_start
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


@xfail_if_mssql_failed_to_start
def test_mssql_read_basic(mssql, tmp_path):
    """Test basic read from an MSSQL table."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  sensor_name NVARCHAR(100) NOT NULL,"
        f"  temperature FLOAT NOT NULL,"
        f"  humidity FLOAT NOT NULL"
        f")"
    )
    mssql.insert_row(table_name, {"sensor_name": "A", "temperature": 22.5, "humidity": 45.0})
    mssql.insert_row(table_name, {"sensor_name": "B", "temperature": 23.1, "humidity": 42.3})
    mssql.insert_row(table_name, {"sensor_name": "C", "temperature": 21.0, "humidity": 50.2})

    class SensorSchema(pw.Schema):
        sensor_name: str
        temperature: float
        humidity: float

    output_path = tmp_path / "output.jsonl"

    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=table_name,
        schema=SensorSchema,
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, str(output_path))

    def run_pw():
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    t = threading.Thread(target=run_pw, daemon=True)
    t.start()

    # Wait for output to appear
    deadline = time.time() + 30
    while time.time() < deadline:
        if output_path.exists():
            lines = output_path.read_text().strip().split("\n")
            if len(lines) >= 3:
                break
        time.sleep(0.5)

    assert output_path.exists()
    lines = output_path.read_text().strip().split("\n")
    assert len(lines) >= 3

    records = [json.loads(line) for line in lines]
    sensor_names = sorted([r["sensor_name"] for r in records])
    assert sensor_names == ["A", "B", "C"]


@xfail_if_mssql_failed_to_start
def test_mssql_read_write_roundtrip(mssql, tmp_path):
    """Test reading from MSSQL, transforming, and writing back."""
    input_table = mssql.random_table_name()
    output_table = mssql.random_table_name()

    mssql.execute_sql(
        f"CREATE TABLE {input_table} ("
        f"  name NVARCHAR(100) NOT NULL,"
        f"  value FLOAT NOT NULL"
        f")"
    )
    mssql.insert_row(input_table, {"name": "x", "value": 10.0})
    mssql.insert_row(input_table, {"name": "y", "value": 20.0})
    mssql.insert_row(input_table, {"name": "z", "value": 30.0})

    class InputSchema(pw.Schema):
        name: str
        value: float

    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=input_table,
        schema=InputSchema,
        autocommit_duration_ms=100,
    )

    # Double the values
    transformed = table.select(name=pw.this.name, value=pw.this.value * 2)

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

    # Wait for output table to be populated
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            contents = mssql.get_table_contents(output_table, ["name", "value"])
            if len(contents) >= 3:
                break
        except Exception:
            pass
        time.sleep(0.5)

    contents = mssql.get_table_contents(output_table, ["name", "value"])
    contents.sort(key=lambda item: item["name"])
    assert len(contents) == 3
    assert contents[0]["name"] == "x"
    assert contents[0]["value"] == 20.0
    assert contents[1]["name"] == "y"
    assert contents[1]["value"] == 40.0
    assert contents[2]["name"] == "z"
    assert contents[2]["value"] == 60.0


def _is_mssql_cdc_available():
    """Check if CDC is available on the MSSQL instance."""
    if not is_mssql_reachable():
        return False
    try:
        import pymssql

        conn = pymssql.connect(
            server="mssql",
            port=1433,
            user="sa",
            password="YourStrong!Passw0rd",
            database="testdb",
            autocommit=True,
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()"
        )
        row = cursor.fetchone()
        return row is not None and row[0] == 1
    except Exception:
        return False


xfail_if_mssql_cdc_unavailable = pytest.mark.xfail(
    not _is_mssql_cdc_available(),
    reason="mssql CDC is not available (requires Developer/Enterprise edition with CDC enabled)",
)


@xfail_if_mssql_cdc_unavailable
def test_mssql_cdc_read_inserts(mssql, tmp_path):
    """Test CDC reader detects inserts in real-time."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    # Enable CDC on the table
    mssql.execute_sql(
        f"EXEC sys.sp_cdc_enable_table "
        f"@source_schema=N'dbo', "
        f"@source_name=N'{table_name}', "
        f"@role_name=NULL"
    )

    # Insert initial data
    mssql.insert_row(table_name, {"id": 1, "name": "Alice"})
    mssql.insert_row(table_name, {"id": 2, "name": "Bob"})

    class TestSchema(pw.Schema):
        id: int
        name: str

    output_path = tmp_path / "cdc_output.jsonl"

    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=table_name,
        schema=TestSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, str(output_path))

    def run_pw():
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    t = threading.Thread(target=run_pw, daemon=True)
    t.start()

    # Wait for initial snapshot
    deadline = time.time() + 30
    while time.time() < deadline:
        if output_path.exists():
            lines = output_path.read_text().strip().split("\n")
            if len(lines) >= 2:
                break
        time.sleep(0.5)

    assert output_path.exists()
    lines = output_path.read_text().strip().split("\n")
    assert len(lines) >= 2

    # Insert more data — CDC should pick it up
    mssql.insert_row(table_name, {"id": 3, "name": "Charlie"})

    deadline = time.time() + 30
    while time.time() < deadline:
        lines = output_path.read_text().strip().split("\n")
        if len(lines) >= 3:
            break
        time.sleep(0.5)

    lines = output_path.read_text().strip().split("\n")
    records = [json.loads(line) for line in lines]
    names = sorted([r["name"] for r in records])
    assert "Charlie" in names


@xfail_if_mssql_cdc_unavailable
def test_mssql_cdc_read_updates(mssql, tmp_path):
    """Test CDC reader detects updates (emits delete + insert)."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  value NVARCHAR(100) NOT NULL"
        f")"
    )
    mssql.execute_sql(
        f"EXEC sys.sp_cdc_enable_table "
        f"@source_schema=N'dbo', "
        f"@source_name=N'{table_name}', "
        f"@role_name=NULL"
    )

    mssql.insert_row(table_name, {"id": 1, "value": "original"})

    class TestSchema(pw.Schema):
        id: int
        value: str

    output_path = tmp_path / "cdc_update_output.jsonl"

    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=table_name,
        schema=TestSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, str(output_path))

    def run_pw():
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    t = threading.Thread(target=run_pw, daemon=True)
    t.start()

    # Wait for initial snapshot
    deadline = time.time() + 30
    while time.time() < deadline:
        if output_path.exists():
            lines = output_path.read_text().strip().split("\n")
            if len(lines) >= 1:
                break
        time.sleep(0.5)

    # Update the row — CDC should emit delete (old) + insert (new)
    mssql.execute_sql(
        f"UPDATE {table_name} SET value = 'updated' WHERE id = 1"
    )

    # Wait for the update events
    deadline = time.time() + 30
    while time.time() < deadline:
        lines = output_path.read_text().strip().split("\n")
        if len(lines) >= 2:
            break
        time.sleep(0.5)

    lines = output_path.read_text().strip().split("\n")
    records = [json.loads(line) for line in lines]
    values = [r["value"] for r in records]
    assert "updated" in values


@xfail_if_mssql_cdc_unavailable
def test_mssql_cdc_read_deletes(mssql, tmp_path):
    """Test CDC reader detects deletes."""
    table_name = mssql.random_table_name()
    mssql.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  id INT PRIMARY KEY,"
        f"  name NVARCHAR(100) NOT NULL"
        f")"
    )
    mssql.execute_sql(
        f"EXEC sys.sp_cdc_enable_table "
        f"@source_schema=N'dbo', "
        f"@source_name=N'{table_name}', "
        f"@role_name=NULL"
    )

    mssql.insert_row(table_name, {"id": 1, "name": "ToDelete"})
    mssql.insert_row(table_name, {"id": 2, "name": "ToKeep"})

    class TestSchema(pw.Schema):
        id: int
        name: str

    output_path = tmp_path / "cdc_delete_output.jsonl"

    table = pw.io.mssql.read(
        connection_string=MSSQL_CONNECTION_STRING,
        table_name=table_name,
        schema=TestSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.jsonlines.write(table, str(output_path))

    def run_pw():
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    t = threading.Thread(target=run_pw, daemon=True)
    t.start()

    # Wait for initial snapshot
    deadline = time.time() + 30
    while time.time() < deadline:
        if output_path.exists():
            lines = output_path.read_text().strip().split("\n")
            if len(lines) >= 2:
                break
        time.sleep(0.5)

    initial_count = len(output_path.read_text().strip().split("\n"))

    # Delete a row
    mssql.execute_sql(f"DELETE FROM {table_name} WHERE id = 1")

    # Wait for the delete event
    deadline = time.time() + 30
    while time.time() < deadline:
        lines = output_path.read_text().strip().split("\n")
        if len(lines) > initial_count:
            break
        time.sleep(0.5)

    lines = output_path.read_text().strip().split("\n")
    assert len(lines) > initial_count

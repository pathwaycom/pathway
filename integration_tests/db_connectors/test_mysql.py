import datetime
import json
import re

import pandas as pd
import pytest
from utils import (
    MYSQL_CONNECTION_STRING,
    MYSQL_DB_NAME,
    SimpleObject,
    is_mysql_reachable,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G

# FIXME: the mysql Docker container used in the integration tests is unstable and
# sometimes is unavailable, even though the healthcheck passes
xfail_if_mysql_failed_to_start = pytest.mark.xfail(
    not is_mysql_reachable(), reason="mysql has failed to start"
)


@xfail_if_mysql_failed_to_start
@pytest.mark.parametrize("output_table_type", ["stream_of_changes", "snapshot"])
@pytest.mark.parametrize("init_mode", ["default", "create_if_not_exists", "replace"])
def test_outputs(output_table_type, init_mode, mysql):
    table_name = mysql.random_table_name()

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
        pw.io.mysql.write(
            table,
            MYSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode=init_mode,
            output_table_type=output_table_type,
            **extra_kwargs,
        )
        pw.run()

    if init_mode == "default":
        error_pattern = (
            "MySqlError {{ ERROR 1146 (42S02): Table '{}.{}' doesn't exist }}".format(
                MYSQL_DB_NAME, table_name
            )
        )
        with pytest.raises(pw.engine.EngineError, match=re.escape(error_pattern)):
            run(0)
        add_special_fields = output_table_type == "stream_of_changes"
        table_name = mysql.create_table(
            InputSchema, add_special_fields=add_special_fields
        )

    run(0)
    contents = mysql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
    contents.sort(key=lambda item: item["row_id"])
    assert contents == [
        {"row_id": 0, "flag": True, "age": 0.5, "title": "Record 0"},
        {"row_id": 1, "flag": False, "age": 1.5, "title": "Record 1"},
        {"row_id": 2, "flag": True, "age": 2.5, "title": "Record 2"},
        {"row_id": 3, "flag": False, "age": 3.5, "title": "Record 3"},
        {"row_id": 4, "flag": True, "age": 4.5, "title": "Record 4"},
    ]

    run(5)
    contents = mysql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
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


@xfail_if_mysql_failed_to_start
@pytest.mark.parametrize("are_types_optional", [False, True])
@pytest.mark.parametrize("init_mode", ["create_if_not_exists", "replace"])
def test_different_types_schema_and_serialization(init_mode, are_types_optional, mysql):
    table_name = mysql.random_table_name()

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
            l: pw.Duration | None

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
            l: pw.Duration

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
            "j": pd.Timedelta("4 days 2 seconds 123 us 456 ns"),
            "k": 42,
            "l": -pd.Timedelta("5 days 6 minutes 7 seconds 8 ms 9 us"),
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

    pw.io.mysql.write(
        table,
        connection_string=MYSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode=init_mode,
    )
    pw.run()

    result = mysql.get_table_contents(table_name, InputSchema.column_names())

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
        "l": -pd.Timedelta("5 days 6 minutes 7 seconds 8 ms 9 us"),
    }
    assert len(result) == 1
    for key, expected_value in expected_result.items():
        our_value = result[0][key]
        if key == "f":
            our_value = json.loads(our_value)
        assert our_value == expected_value, key

    external_schema = mysql.get_table_schema(table_name)
    assert external_schema["a"].type_name == "text"
    assert external_schema["b"].type_name == "double"
    assert external_schema["c"].type_name == "tinyint"
    assert external_schema["d"].type_name == "blob"
    assert external_schema["e"].type_name == "text"
    assert external_schema["f"].type_name == "json"
    assert external_schema["g"].type_name == "datetime"
    assert external_schema["h"].type_name == "datetime"
    assert external_schema["i"].type_name == "blob"
    assert external_schema["j"].type_name == "time"
    assert external_schema["k"].type_name == "bigint"
    assert external_schema["l"].type_name == "time"
    for column_name, column_props in external_schema.items():
        if column_name in ("time", "diff"):
            assert not column_props.is_nullable
        else:
            assert column_props.is_nullable == are_types_optional, column_name


@xfail_if_mysql_failed_to_start
def test_output_snapshot_overwrite_by_key(mysql):
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
    table_name = mysql.create_table(result.schema, add_special_fields=False)
    pw.io.mysql.write(
        result,
        MYSQL_CONNECTION_STRING,
        table_name=table_name,
        output_table_type="snapshot",
        primary_key=[result.word],
    )
    pw.run()

    assert len(mysql.get_table_contents(table_name, ["word", "count"])) == 4

    external_schema = mysql.get_table_schema(table_name)
    assert len(external_schema) == 2
    assert external_schema["word"].type_name == "varchar"
    assert external_schema["count"].type_name == "bigint"


@xfail_if_mysql_failed_to_start
def test_mysql_overwrites_old_snapshot(mysql):
    table_name = mysql.random_table_name()

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
        pw.io.mysql.write(
            table,
            MYSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.row_id],
        )
        pw.run()

    run(0)
    contents = mysql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
    contents.sort(key=lambda item: item["row_id"])
    assert contents == [
        {"row_id": 0, "flag": True, "age": 0.5, "title": "Record 0"},
        {"row_id": 1, "flag": False, "age": 1.5, "title": "Record 1"},
        {"row_id": 2, "flag": True, "age": 2.5, "title": "Record 2"},
        {"row_id": 3, "flag": False, "age": 3.5, "title": "Record 3"},
        {"row_id": 4, "flag": True, "age": 4.5, "title": "Record 4"},
    ]

    run(7)
    contents = mysql.get_table_contents(table_name, ["row_id", "flag", "age", "title"])
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


@xfail_if_mysql_failed_to_start
def test_mysql_composite_snapshot_key(mysql):
    table_name = mysql.random_table_name()

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
        pw.io.mysql.write(
            table,
            MYSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.x, table.y],
        )
        pw.run()

    run(0)
    contents = mysql.get_table_contents(table_name, ["x", "y", "flag", "age", "title"])
    contents.sort(key=lambda item: item["x"] + item["y"])
    assert contents == [
        {"x": 0, "y": 0, "flag": True, "age": 12.5, "title": "Record 12"},
        {"x": 1, "y": 0, "flag": False, "age": 13.5, "title": "Record 13"},
        {"x": 0, "y": 2, "flag": True, "age": 14.5, "title": "Record 14"},
        {"x": 1, "y": 2, "flag": False, "age": 15.5, "title": "Record 15"},
    ]


@xfail_if_mysql_failed_to_start
@pytest.mark.parametrize("malfomed_primary_key", [None, []])
def test_mysql_no_snapshot_key(malfomed_primary_key, mysql):
    table_name = mysql.random_table_name()

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
        pw.io.mysql.write(
            table,
            MYSQL_CONNECTION_STRING,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=malfomed_primary_key,
        )
        pw.run()


@xfail_if_mysql_failed_to_start
def test_mysql_single_column_snapshot_mode(mysql):
    table_name = mysql.random_table_name()

    class InputSchema(pw.Schema):
        x: int = pw.column_definition(primary_key=True)

    table = pw.demo.generate_custom_stream(
        value_generators={"x": lambda x: x},
        schema=InputSchema,
        nb_rows=5,
        autocommit_duration_ms=100,
    )
    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode="create_if_not_exists",
        output_table_type="snapshot",
        primary_key=[table.x],
    )
    pw.run()
    contents = mysql.get_table_contents(table_name, ["x"])
    contents.sort(key=lambda item: item["x"])
    assert contents == [{"x": 0}, {"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]

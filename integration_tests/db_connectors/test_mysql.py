import datetime
import json
import os
import re
import subprocess
import sys
import textwrap
import time

import pandas as pd
import pytest
from utils import (
    MYSQL_CONNECTION_STRING,
    MYSQL_DB_NAME,
    MySQLContext,
    SimpleObject,
    check_write_quotes_reserved_word_column_name,
    check_write_quotes_table_name_with_special_characters,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    ExceptionAwareThread,
    FileLinesNumberChecker,
    read_jsonlines,
    run,
    wait_result_with_checker,
)

pytestmark = pytest.mark.xdist_group("mysql")


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


def test_mysql_permanent_error_fails_fast(mysql):
    # A permanent failure -- here, writing to a table that does not exist
    # with init_mode="default" -- must surface promptly instead of being
    # retried with exponential backoff. Retrying a non-transient error
    # (missing table, bad SQL, a type or constraint violation) only adds
    # several seconds of pointless backoff before the inevitable failure.
    # This matches the Postgres and MSSQL writers, which retry transient
    # errors only.
    table_name = mysql.random_table_name()  # deliberately never created

    class InputSchema(pw.Schema):
        k: int

    table = pw.debug.table_from_rows(InputSchema, [(1,)])
    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode="default",
    )

    start = time.monotonic()
    with pytest.raises(pw.engine.EngineError, match="doesn't exist"):
        pw.run()
    elapsed = time.monotonic() - start
    # Three retries with exponential backoff add at least ~3.6s of pure
    # sleep; failing fast returns in well under a second. The 3.0s budget
    # sits comfortably between the two regimes.
    assert elapsed < 3.0, (
        f"permanent error surfaced after {elapsed:.2f}s; a non-transient "
        "failure must not be retried with backoff"
    )


@pytest.mark.parametrize("colliding_column", ["time", "diff", "TIME"])
def test_mysql_rejects_metadata_column_collision_in_stream_mode(colliding_column):
    # In stream_of_changes mode the writer appends ``time`` and ``diff``
    # metadata columns to the target table. A user schema column with one
    # of those names -- case-insensitively, since MySQL column names are
    # not case-sensitive -- would otherwise be emitted twice in the
    # generated DDL/INSERT and fail mid-run with an opaque MySQL
    # "Duplicate column name" error. Reject it up front with an actionable
    # message, the same way the Postgres connector does. No MySQL instance
    # is required: the check runs while the sink is being registered.
    schema = pw.schema_builder(
        {
            "key": pw.column_definition(dtype=int),
            colliding_column: pw.column_definition(dtype=int),
        }
    )
    table = pw.debug.table_from_rows(schema, [(1, 2)])

    with pytest.raises(ValueError, match="collide with"):
        pw.io.mysql.write(
            table,
            MYSQL_CONNECTION_STRING,
            table_name="irrelevant",
        )


@pytest.mark.parametrize("metadata_column", ["time", "diff"])
def test_mysql_allows_metadata_column_name_in_snapshot_mode(metadata_column):
    # Snapshot mode does not append the ``time`` / ``diff`` metadata
    # columns, so a user column with one of those names is perfectly valid
    # there and must not be rejected by the stream-mode collision check.
    schema = pw.schema_builder(
        {
            "key": pw.column_definition(dtype=int, primary_key=True),
            metadata_column: pw.column_definition(dtype=int),
        }
    )
    table = pw.debug.table_from_rows(schema, [(1, 2)])

    # Registering the sink must not raise; we do not call ``pw.run()`` so
    # no MySQL instance is required.
    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name="irrelevant",
        output_table_type="snapshot",
        primary_key=[table.key],
    )


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


def _mysql_quote_ident(name: str) -> str:
    """Escape a MySQL identifier with backticks."""
    return "`" + name.replace("`", "``") + "`"


def _mysql_write(table, *, table_name, init_mode):
    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode=init_mode,
    )
    pw.run()


def test_mysql_write_table_name_with_special_characters(mysql):
    """Shared regression for identifier quoting in SQL writers.
    Unverified in this harness — see
    ``check_write_quotes_table_name_with_special_characters`` in
    ``utils.py`` and the Postgres counterpart in ``test_postgres.py``."""
    check_write_quotes_table_name_with_special_characters(
        write=_mysql_write, db_context=mysql, quote_ident=_mysql_quote_ident
    )


def test_mysql_write_column_name_is_reserved_word(mysql):
    """Shared regression for identifier quoting in SQL writers.
    Unverified in this harness — see
    ``check_write_quotes_reserved_word_column_name`` in ``utils.py``
    and the Postgres counterpart in ``test_postgres.py``."""
    check_write_quotes_reserved_word_column_name(
        write=_mysql_write, db_context=mysql, quote_ident=_mysql_quote_ident
    )


# ---------------------------------------------------------------------------
# Input connector (pw.io.mysql.read) tests
# ---------------------------------------------------------------------------


class _RowCollector(pw.io.python.ConnectorObserver):
    """Collects the latest state per key from a Pathway table into ``self.rows``."""

    def __init__(self, key_name: str):
        self.key_name = key_name
        self.rows: dict = {}

    def on_change(self, key, row, time, is_addition):
        identity = row[self.key_name]
        if is_addition:
            self.rows[identity] = dict(row)
        else:
            self.rows.pop(identity, None)

    def on_end(self):
        pass


class _MysqlRowCountChecker:
    def __init__(self, mysql, table_name, n_rows_expected):
        self.mysql = mysql
        self.table_name = table_name
        self.n_rows_expected = n_rows_expected

    def __call__(self) -> bool:
        try:
            rows = self.mysql.get_table_contents(self.table_name, ["id"])
        except Exception:
            return False
        return len(rows) == self.n_rows_expected


class _MysqlContentChecker:
    def __init__(self, mysql, table_name, columns, expected):
        self.mysql = mysql
        self.table_name = table_name
        self.columns = columns
        self.expected = self._sort(expected)

    def _sort(self, rows):
        return sorted(rows, key=lambda row: tuple(row[c] for c in self.columns))

    def __call__(self) -> bool:
        try:
            rows = self.mysql.get_table_contents(self.table_name, self.columns)
        except Exception:
            return False
        return self._sort(rows) == self.expected


def _skip_if_no_binlog(mysql) -> None:
    if not mysql.binlog_available():
        pytest.skip("row-based binary logging is not enabled on the MySQL server")


def test_mysql_read_requires_primary_key():
    # No MySQL instance is required: the check runs while the connector is
    # being registered.
    class NoPkSchema(pw.Schema):
        a: int
        b: str

    with pytest.raises(ValueError, match="requires at least one primary key column"):
        pw.io.mysql.read(MYSQL_CONNECTION_STRING, "irrelevant", NoPkSchema)


def test_mysql_read_rejects_nullable_primary_key():
    class NullablePkSchema(pw.Schema):
        a: int | None = pw.column_definition(primary_key=True)
        b: str

    with pytest.raises(ValueError, match="nullable"):
        pw.io.mysql.read(MYSQL_CONNECTION_STRING, "irrelevant", NullablePkSchema)


def test_mysql_read_static_returns_all_rows(mysql):
    table_name = mysql.random_table_name()

    class InputSchema(pw.Schema):
        row_id: int = pw.column_definition(primary_key=True)
        flag: bool
        age: float
        title: str

    written = pw.debug.table_from_rows(
        InputSchema,
        [
            (0, True, 0.5, "Record 0"),
            (1, False, 1.5, "Record 1"),
            (2, True, 2.5, "Record 2"),
        ],
    )
    pw.io.mysql.write(
        written,
        MYSQL_CONNECTION_STRING,
        table_name=table_name,
        init_mode="create_if_not_exists",
    )
    run()

    G.clear()
    table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING, table_name, InputSchema, mode="static"
    )
    collector = _RowCollector("row_id")
    pw.io.python.write(table, collector)
    run()

    assert collector.rows == {
        0: {"row_id": 0, "flag": True, "age": 0.5, "title": "Record 0"},
        1: {"row_id": 1, "flag": False, "age": 1.5, "title": "Record 1"},
        2: {"row_id": 2, "flag": True, "age": 2.5, "title": "Record 2"},
    }


def test_mysql_read_static_parses_native_mysql_types(mysql):
    # Covers MySQL column types that the output connector never produces but
    # that the input connector parses into Pathway types: VARCHAR, DECIMAL,
    # TINYINT(1), DATE, DATETIME, JSON, INT.
    table_name = mysql.random_table_name()
    mysql.cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255),
            score DECIMAL(10, 2),
            active TINYINT(1),
            born DATE,
            created DATETIME,
            payload JSON,
            qty INT
        )
        """
    )
    mysql.insert_row(
        table_name,
        {
            "id": 1,
            "name": "alice",
            "score": 3.50,
            "active": 1,
            "born": "1990-05-17",
            "created": "2025-03-14 10:13:00",
            "payload": json.dumps({"a": 1, "b": [2, 3]}),
            "qty": 7,
        },
    )

    class NativeSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        name: str
        score: float
        active: bool
        born: pw.DateTimeNaive
        created: pw.DateTimeNaive
        payload: pw.Json
        qty: int

    table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING, table_name, NativeSchema, mode="static"
    )
    collector = _RowCollector("id")
    pw.io.python.write(table, collector)
    run()

    assert set(collector.rows.keys()) == {1}
    row = collector.rows[1]
    assert row["name"] == "alice"
    assert row["score"] == 3.5
    assert row["active"] is True
    assert row["born"] == pd.Timestamp("1990-05-17 00:00:00")
    assert row["created"] == pd.Timestamp("2025-03-14 10:13:00")
    assert row["payload"].value == {"a": 1, "b": [2, 3]}
    assert row["qty"] == 7


def test_mysql_read_static_table_not_found(mysql):
    table_name = mysql.random_table_name()  # never created

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING, table_name, InputSchema, mode="static"
    )
    pw.io.null.write(table)
    with pytest.raises(pw.engine.EngineError, match="was not found"):
        run()


def test_mysql_read_static_schema_column_missing(mysql):
    table_name = mysql.random_table_name()
    mysql.cursor.execute(
        f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, value VARCHAR(255))"
    )
    mysql.insert_row(table_name, {"id": 1, "value": "hello"})

    class MismatchedSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str
        absent_column: int

    table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING, table_name, MismatchedSchema, mode="static"
    )
    pw.io.null.write(table)
    with pytest.raises(pw.engine.EngineError, match="absent_column"):
        run()


def test_mysql_read_streaming_reflects_inserts_updates_deletes(mysql):
    _skip_if_no_binlog(mysql)
    mysql.grant_replication_privileges()

    input_table = mysql.random_table_name()
    output_table = mysql.random_table_name()
    mysql.cursor.execute(
        f"CREATE TABLE {input_table} "
        f"(id BIGINT PRIMARY KEY, name VARCHAR(255), value DOUBLE)"
    )
    for identity, name, value in [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)]:
        mysql.insert_row(input_table, {"id": identity, "name": name, "value": value})

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        name: str
        value: float

    def mutate():
        # A separate connection so this thread does not race with the main
        # thread's content checker.
        thread_mysql = MySQLContext()
        # Wait for the snapshot (3 rows) to be mirrored into the output before
        # changing the source.
        wait_result_with_checker(
            _MysqlRowCountChecker(thread_mysql, output_table, 3),
            60,
            target=None,
        )
        thread_mysql.cursor.execute(
            f"UPDATE {input_table} SET name='A', value=1.5 WHERE id=1"
        )
        thread_mysql.cursor.execute(f"DELETE FROM {input_table} WHERE id=2")
        thread_mysql.insert_row(input_table, {"id": 4, "name": "d", "value": 4.0})

    mutate_thread = ExceptionAwareThread(target=mutate, daemon=True)
    mutate_thread.start()

    G.clear()
    table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING, input_table, InputSchema, mode="streaming"
    )
    # Snapshot output mode mirrors the current state, so updates and deletes are
    # observable as the final table contents.
    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name=output_table,
        init_mode="create_if_not_exists",
        output_table_type="snapshot",
        primary_key=[table.id],
    )

    expected = [
        {"id": 1, "name": "A", "value": 1.5},
        {"id": 3, "name": "c", "value": 3.0},
        {"id": 4, "name": "d", "value": 4.0},
    ]
    wait_result_with_checker(
        _MysqlContentChecker(mysql, output_table, ["id", "name", "value"], expected),
        120,
    )
    mutate_thread.join()


def test_mysql_read_streaming_delivers_entire_large_single_statement(mysql):
    """A single statement that changes more rows than the per-block cap must be
    delivered in full.

    MySQL splits such a statement across several binlog rows events that share
    one table map (emitted once, valid until the rows event flagged STMT_END, and
    not re-sent if a later dump resumes mid-statement). The reader must therefore
    never commit a binlog position inside a statement; otherwise the rows past the
    cap would be silently dropped when the next poll resumes from that position.
    """
    _skip_if_no_binlog(mysql)
    mysql.grant_replication_privileges()

    input_table = mysql.random_table_name()
    output_table = mysql.random_table_name()
    mysql.cursor.execute(
        f"CREATE TABLE {input_table} (id BIGINT PRIMARY KEY, payload VARCHAR(255))"
    )
    # A sentinel row lets the inserting thread detect that the reader has finished
    # its snapshot startup and is tailing the binlog, so the big statement below
    # is captured via the binlog path (where the split happens), not the snapshot.
    mysql.insert_row(input_table, {"id": 0, "payload": "sentinel"})

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        payload: str

    # Comfortably above MAX_ROWS_PER_BLOCK (10_000). With the server's default
    # binlog_row_event_max_size this one INSERT is split into dozens of rows
    # events sharing a single table map, so a per-event cap would resume
    # mid-statement and lose the rows after the cap.
    n_rows = 15_000

    def insert_large_statement():
        thread_mysql = MySQLContext()
        # Wait until the snapshot (the sentinel) has been mirrored, i.e. the
        # reader is now tailing the binlog from a position before this insert.
        wait_result_with_checker(
            _MysqlRowCountChecker(thread_mysql, output_table, 1),
            60,
            target=None,
        )
        values = ", ".join(f"({i}, 'row-{i}')" for i in range(1, n_rows + 1))
        thread_mysql.cursor.execute(
            f"INSERT INTO {input_table} (id, payload) VALUES {values}"
        )

    insert_thread = ExceptionAwareThread(target=insert_large_statement, daemon=True)
    insert_thread.start()

    G.clear()
    table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING, input_table, InputSchema, mode="streaming"
    )
    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name=output_table,
        init_mode="create_if_not_exists",
        output_table_type="snapshot",
        primary_key=[table.id],
    )

    # The sentinel plus every row of the single large statement must arrive.
    wait_result_with_checker(
        _MysqlRowCountChecker(mysql, output_table, n_rows + 1),
        180,
    )
    insert_thread.join()


def test_mysql_read_streaming_composite_key_and_reordered_schema(mysql):
    """Streaming CDC with a composite primary key and a Pathway schema whose
    column order differs from the physical table order.

    Binlog row images are positional in physical table order; the connector maps
    them back to schema fields by *name*. Declaring the schema in a different
    order (and using a multi-column key) would silently misassign columns if the
    mapping were positional — this asserts it is not, and that composite keys are
    tracked correctly across inserts, updates, and deletes.
    """
    _skip_if_no_binlog(mysql)
    mysql.grant_replication_privileges()

    input_table = mysql.random_table_name()
    output_table = mysql.random_table_name()
    # Physical column order: region, id, label, amount; composite PK (region, id).
    mysql.cursor.execute(
        f"CREATE TABLE {input_table} ("
        f"region VARCHAR(50), id BIGINT, label VARCHAR(255), amount DOUBLE, "
        f"PRIMARY KEY (region, id))"
    )
    for region, identity, label, amount in [
        ("us", 1, "a", 1.0),
        ("us", 2, "b", 2.0),
        ("eu", 1, "c", 3.0),
    ]:
        mysql.insert_row(
            input_table,
            {"region": region, "id": identity, "label": label, "amount": amount},
        )

    # Schema declared in a DIFFERENT order than the physical table.
    class InputSchema(pw.Schema):
        amount: float
        label: str
        id: int = pw.column_definition(primary_key=True)
        region: str = pw.column_definition(primary_key=True)

    def mutate():
        thread_mysql = MySQLContext()
        wait_result_with_checker(
            _MysqlRowCountChecker(thread_mysql, output_table, 3), 60, target=None
        )
        thread_mysql.cursor.execute(
            f"UPDATE {input_table} SET label='A', amount=1.5 "
            f"WHERE region='us' AND id=1"
        )
        thread_mysql.cursor.execute(
            f"DELETE FROM {input_table} WHERE region='us' AND id=2"
        )
        thread_mysql.insert_row(
            input_table,
            {"region": "eu", "id": 2, "label": "d", "amount": 4.0},
        )

    # Pre-create the snapshot output table: a str/TEXT column cannot be an
    # auto-created MySQL primary key without a length, so a VARCHAR key column
    # must be provided by hand (as the pw.io.mysql.write docstring notes).
    mysql.cursor.execute(
        f"CREATE TABLE {output_table} ("
        f"region VARCHAR(50), id BIGINT, label VARCHAR(255), amount DOUBLE, "
        f"PRIMARY KEY (region, id))"
    )

    mutate_thread = ExceptionAwareThread(target=mutate, daemon=True)
    mutate_thread.start()

    G.clear()
    table = pw.io.mysql.read(
        MYSQL_CONNECTION_STRING, input_table, InputSchema, mode="streaming"
    )
    pw.io.mysql.write(
        table,
        MYSQL_CONNECTION_STRING,
        table_name=output_table,
        init_mode="default",
        output_table_type="snapshot",
        primary_key=[table.region, table.id],
    )

    expected = [
        {"region": "us", "id": 1, "label": "A", "amount": 1.5},
        {"region": "eu", "id": 1, "label": "c", "amount": 3.0},
        {"region": "eu", "id": 2, "label": "d", "amount": 4.0},
    ]
    wait_result_with_checker(
        _MysqlContentChecker(
            mysql, output_table, ["region", "id", "label", "amount"], expected
        ),
        120,
    )
    mutate_thread.join()


# ---------------------------------------------------------------------------
# Persistence (binlog-position resume) tests
#
# Each run is a fresh subprocess.Popen (not fork) so no Rust global state left
# by a prior pw.run() in the test process is inherited — mirrors the MongoDB
# streaming-persistence tests.
# ---------------------------------------------------------------------------
_MYSQL_STREAMING_WORKER_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys

    cfg = json.loads(sys.argv[1])

    import pathway as pw
    from pathway.internals.parse_graph import G
    from pathway.tests.utils import run

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        name: str
        value: float

    G.clear()
    table = pw.io.mysql.read(
        cfg["connection_string"],
        cfg["table_name"],
        InputSchema,
        mode="streaming",
        name="mysql_streaming_persistence_source",
    )
    pw.io.jsonlines.write(table, cfg["output_path"])
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(cfg["pstorage_path"])
    )
    run(persistence_config=persistence_config)
    """
)


def _start_mysql_streaming_worker(
    output_path, pstorage_path, table_name, log_path=None
):
    cfg = json.dumps(
        {
            "connection_string": MYSQL_CONNECTION_STRING,
            "table_name": table_name,
            "output_path": str(output_path),
            "pstorage_path": str(pstorage_path),
        }
    )
    log_file = open(log_path, "w") if log_path is not None else None
    return subprocess.Popen(
        [sys.executable, "-c", _MYSQL_STREAMING_WORKER_SCRIPT, cfg],
        env=os.environ,
        stdout=log_file,
        stderr=subprocess.STDOUT if log_file is not None else None,
    )


def _wait_and_terminate(checker, timeout_sec, proc, *, persistence_flush_sec=5.0):
    """Poll ``checker`` until it passes, give the engine time to flush
    persistence state, then terminate the subprocess."""
    start = time.monotonic()
    while True:
        time.sleep(0.1)
        if time.monotonic() - start >= timeout_sec:
            proc.terminate()
            proc.wait()
            raise AssertionError(
                f"Timed out after {timeout_sec}s. "
                f"{checker.provide_information_on_failure()}"
            )
        if checker():
            break
        if proc.poll() is not None:
            assert proc.returncode == 0, f"Worker exited early with {proc.returncode}"
            break
    time.sleep(persistence_flush_sec)
    proc.terminate()
    proc.wait()


def _extract_persistence_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "value": row["value"],
        "diff": row["diff"],
    }


def _sort_persistence_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (r["id"], r["value"], r["diff"]))


@pytest.mark.flaky(reruns=2)
def test_mysql_read_streaming_persistence(tmp_path, mysql):
    """Two-run binlog-position persistence test for streaming reads.

    Run 1 delivers the initial snapshot. After it is killed (and the binlog
    position persisted), the source is changed. Run 2 must resume from the saved
    position and deliver ONLY the delta — proving the snapshot is not re-read.
    """
    _skip_if_no_binlog(mysql)
    mysql.grant_replication_privileges()

    pstorage_path = tmp_path / "PStorage"
    input_table = mysql.random_table_name()
    mysql.cursor.execute(
        f"CREATE TABLE {input_table} "
        f"(id BIGINT PRIMARY KEY, name VARCHAR(255), value DOUBLE)"
    )
    for identity, name, value in [(1, "a", 1.0), (2, "b", 2.0)]:
        mysql.insert_row(input_table, {"id": identity, "name": name, "value": value})

    # --- Run 1: the initial snapshot must appear ---
    output_1 = tmp_path / "output_1.jsonl"
    p1 = _start_mysql_streaming_worker(output_1, pstorage_path, input_table)
    try:
        _wait_and_terminate(FileLinesNumberChecker(output_1, 2), 90, p1)
    finally:
        if p1.poll() is None:
            p1.terminate()
            p1.wait()
    run1 = _sort_persistence_rows(
        [_extract_persistence_row(r) for r in read_jsonlines(output_1)]
    )
    assert run1 == _sort_persistence_rows(
        [
            {"id": 1, "name": "a", "value": 1.0, "diff": 1},
            {"id": 2, "name": "b", "value": 2.0, "diff": 1},
        ]
    ), run1

    # Apply changes after Run 1 has persisted its position.
    mysql.cursor.execute(
        f"INSERT INTO {input_table} (id, name, value) VALUES (3, 'c', 3.0)"
    )
    mysql.cursor.execute(f"UPDATE {input_table} SET value=9.0 WHERE id=1")
    mysql.cursor.execute(f"DELETE FROM {input_table} WHERE id=2")

    # --- Run 2: only the delta since Run 1 must appear (no re-snapshot) ---
    output_2 = tmp_path / "output_2.jsonl"
    p2 = _start_mysql_streaming_worker(output_2, pstorage_path, input_table)
    try:
        _wait_and_terminate(FileLinesNumberChecker(output_2, 4), 90, p2)
    finally:
        if p2.poll() is None:
            p2.terminate()
            p2.wait()
    run2 = _sort_persistence_rows(
        [_extract_persistence_row(r) for r in read_jsonlines(output_2)]
    )
    assert run2 == _sort_persistence_rows(
        [
            {"id": 1, "name": "a", "value": 1.0, "diff": -1},  # update: old image
            {"id": 1, "name": "a", "value": 9.0, "diff": 1},  # update: new image
            {"id": 2, "name": "b", "value": 2.0, "diff": -1},  # delete
            {"id": 3, "name": "c", "value": 3.0, "diff": 1},  # insert
        ]
    ), run2


@pytest.mark.flaky(reruns=2)
def test_mysql_read_persistence_errors_when_binlog_purged(tmp_path, mysql):
    """Resuming from a position whose binary log has been purged must fail
    loudly, not silently re-snapshot.

    This guards the conservative-error contract: when the server's normal binary
    log expiry has dropped the file the saved offset points into, the connector
    raises a clear error instead of pretending it can resume.
    """
    _skip_if_no_binlog(mysql)
    mysql.grant_replication_privileges()

    pstorage_path = tmp_path / "PStorage"
    input_table = mysql.random_table_name()
    mysql.cursor.execute(
        f"CREATE TABLE {input_table} "
        f"(id BIGINT PRIMARY KEY, name VARCHAR(255), value DOUBLE)"
    )
    for identity, name, value in [(1, "a", 1.0), (2, "b", 2.0)]:
        mysql.insert_row(input_table, {"id": identity, "name": name, "value": value})

    # --- Run 1: persist a binlog position ---
    output_1 = tmp_path / "output_1.jsonl"
    p1 = _start_mysql_streaming_worker(output_1, pstorage_path, input_table)
    try:
        _wait_and_terminate(FileLinesNumberChecker(output_1, 2), 90, p1)
    finally:
        if p1.poll() is None:
            p1.terminate()
            p1.wait()

    # Rotate to a fresh binary log, then purge everything older — including the
    # file Run 1's saved position points into. (Requires elevated privileges.)
    mysql.execute_as_root("FLUSH BINARY LOGS")
    logs = mysql.execute_as_root("SHOW BINARY LOGS")
    newest_log = logs[-1][0]
    mysql.execute_as_root(f"PURGE BINARY LOGS TO '{newest_log}'")

    # --- Run 2: resume must fail loudly, not re-snapshot ---
    output_2 = tmp_path / "output_2.jsonl"
    log_2 = tmp_path / "worker2.log"
    p2 = _start_mysql_streaming_worker(output_2, pstorage_path, input_table, log_2)
    try:
        returncode = p2.wait(timeout=90)
    finally:
        if p2.poll() is None:
            p2.terminate()
            p2.wait()
    log_text = log_2.read_text()
    assert returncode != 0, f"worker should have failed; log:\n{log_text}"
    assert "no longer present" in log_text or "purged" in log_text, log_text

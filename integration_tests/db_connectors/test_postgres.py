import copy
import datetime
import json
import os
import threading
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from utils import (
    PGVECTOR_SETTINGS,
    POSTGRES_SETTINGS,
    POSTGRES_WITH_TLS_SETTINGS,
    ColumnProperties,
    RowCountChecker,
    SimpleObject,
    check_write_quotes_reserved_word_column_name,
    check_write_quotes_table_name_with_special_characters,
)

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import FileLinesNumberChecker, run, wait_result_with_checker

pytestmark = pytest.mark.xdist_group("postgres")

CREDENTIALS_DIR = Path(os.getenv("CREDENTIALS_DIR", default=Path(__file__).parent))


def test_psql_output_stream(tmp_path, postgres):
    class InputSchema(pw.Schema):
        name: str
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_table = postgres.create_table(InputSchema, add_special_fields=True)

    def _run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.postgres.write(table, POSTGRES_SETTINGS, output_table)
        run()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    _run(test_items)

    rows = postgres.get_table_contents(output_table, InputSchema.column_names())
    rows.sort(key=lambda item: (item["name"], item["available"]))
    assert rows == test_items

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    _run(new_test_items)

    rows = postgres.get_table_contents(
        output_table, InputSchema.column_names(), ("name", "available")
    )
    expected_rows = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert rows == expected_rows


# TODO: remove when pw.io.postgres.write_snapshot is fully deprecated.
def test_psql_output_snapshot_legacy(tmp_path, postgres):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_table = postgres.create_table(InputSchema, add_special_fields=True)

    def _run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.postgres.write_snapshot(table, POSTGRES_SETTINGS, output_table, ["name"])
        run()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    _run(test_items)

    rows = postgres.get_table_contents(output_table, InputSchema.column_names())
    rows.sort(key=lambda item: item["name"])
    assert rows == test_items

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    _run(new_test_items)

    rows = postgres.get_table_contents(output_table, InputSchema.column_names(), "name")

    expected_rows = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert rows == expected_rows


def test_psql_write_snapshot_no_primary_key(postgres):
    class InputSchema(pw.Schema):
        a: int

    table_name = postgres.random_table_name()

    with pytest.raises(
        ValueError,
        match="primary key field names must be specified for a snapshot mode",
    ):
        table = pw.debug.table_from_rows(InputSchema, [(1,), (2,), (3,)])
        pw.io.postgres.write(
            table,
            table_name=table_name,
            postgres_settings=POSTGRES_SETTINGS,
            output_table_type="snapshot",
            primary_key=[],
            init_mode="create_if_not_exists",
        )
        run()


def test_psql_write_snapshot_single_column(postgres):
    class InputSchema(pw.Schema):
        a: int

    table_name = postgres.random_table_name()

    for i in range(3):
        G.clear()
        table = pw.debug.table_from_rows(InputSchema, [(j + 1,) for j in range(i + 1)])
        pw.io.postgres.write(
            table,
            table_name=table_name,
            postgres_settings=POSTGRES_SETTINGS,
            output_table_type="snapshot",
            primary_key=[table.a],
            init_mode="create_if_not_exists",
        )
        run()

    result = postgres.get_table_contents(table_name, InputSchema.column_names(), ("a"))

    assert result == [{"a": 1}, {"a": 2}, {"a": 3}]


def test_psql_write_snapshot_only_primary_keys(postgres):
    class InputSchema(pw.Schema):
        a: int
        b: int

    table_name = postgres.random_table_name()

    for i in range(3):
        G.clear()
        table = pw.debug.table_from_rows(InputSchema, [(1, 1), (2, 2), (3, 3)])
        pw.io.postgres.write(
            table,
            table_name=table_name,
            postgres_settings=POSTGRES_SETTINGS,
            output_table_type="snapshot",
            primary_key=[table.a, table.b],
            init_mode="create_if_not_exists",
        )
        run()

    result = postgres.get_table_contents(
        table_name, InputSchema.column_names(), ("a", "b")
    )

    assert result == [{"a": 1, "b": 1}, {"a": 2, "b": 2}, {"a": 3, "b": 3}]


def write_snapshot(primary_key: list[str]):
    def _write_snapshot(table: pw.Table, /, **kwargs):
        primary_key_columns = []
        for column_name in primary_key:
            primary_key_columns.append(table[column_name])
        pw.io.postgres.write(
            table,
            **kwargs,
            output_table_type="snapshot",
            primary_key=primary_key_columns,
        )

    return _write_snapshot


@pytest.mark.parametrize("write_method", [pw.io.postgres.write, write_snapshot(["a"])])
def test_init_wrong_mode(write_method):
    class InputSchema(pw.Schema):
        a: str
        b: int

    rows = [
        ("foo", 1),
        ("bar", 2),
    ]

    table = pw.debug.table_from_rows(
        InputSchema,
        rows,
    )

    with pytest.raises(TypeError):
        write_method(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            init_mode="wrong_mode",
            table_name="non_existent_table",
        )


@pytest.mark.parametrize("write_method", [pw.io.postgres.write, write_snapshot(["a"])])
def test_init_default_rejects_missing_table(write_method):
    """``init_mode="default"`` issues no DDL and writes straight into the
    user's pre-existing table. A missing destination must be rejected at
    writer init with a clear "table does not exist" message — not surface
    as an opaque ``db error`` from the binary-``COPY`` fast path failing to
    resolve the destination column OIDs.
    """

    class InputSchema(pw.Schema):
        a: str
        b: int

    rows = [
        ("foo", 1),
        ("bar", 2),
    ]

    table = pw.debug.table_from_rows(
        InputSchema,
        rows,
    )

    write_method(
        table,
        postgres_settings=POSTGRES_SETTINGS,
        table_name="non_existent_table",
    )
    with pytest.raises(Exception, match=r"non_existent_table\" does not exist"):
        run()


@pytest.mark.parametrize("init_mode", ["create_if_not_exists", "replace"])
@pytest.mark.parametrize("write_method", [pw.io.postgres.write, write_snapshot(["a"])])
@pytest.mark.parametrize("are_types_optional", [False, True])
def test_different_types_schema_and_serialization(
    init_mode, write_method, are_types_optional, postgres
):
    table_name = postgres.random_table_name()

    if are_types_optional:

        class InputSchema(pw.Schema):
            # `a` is used as snapshot-mode primary_key by
            # `write_snapshot(["a"])` below; primary_key columns must
            # be non-nullable in snapshot mode.
            a: str
            b: float | None
            c: bool | None
            d: list[int] | None
            e: tuple[int, int, int] | None
            f: pw.Json | None
            g: str
            h: str
            i: pw.PyObjectWrapper[SimpleObject] | None
            j: pw.Duration | None
            k: list[str] | None
            l: list[pw.Duration] | None
            m: list[list[str]] | None
            n: Any
            o: Any

    else:

        class InputSchema(pw.Schema):  # type:ignore
            a: str
            b: float
            c: bool
            d: list[int]
            e: tuple[int, int, int]
            f: pw.Json
            g: str
            h: str
            i: pw.PyObjectWrapper[SimpleObject]
            j: pw.Duration
            k: list[str]
            l: list[pw.Duration]
            m: list[list[str]]
            n: Any
            o: Any

    rows = [
        {
            "a": "foo",
            "b": 1.5,
            "c": False,
            "d": [1, 2, 3],
            "e": (1, 2, 3),
            "f": {"foo": "bar", "baz": 123},
            "g": "2025-03-14T10:13:00",
            "h": "2025-04-23T10:13:00+00:00",
            "i": pw.wrap_py_object(SimpleObject("test")),
            "j": pd.Timedelta("4 days 2 seconds 123 us 456 ns"),
            "k": ["abc", "def", "ghi"],
            "l": [
                pd.Timedelta("4 days 2 seconds 123 us 456 ns"),
                pd.Timedelta("1 days 2 seconds 3 us 4 ns"),
            ],
            "m": [["a", "b"], ["c", "d"]],
            "n": np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=int),
            "o": np.array(
                [[[1.1, 2.2], [3.3, 4.4]], [[5.5, 6.6], [7.7, 8.8]]], dtype=float
            ),
        }
    ]

    table = (
        pw.debug.table_from_rows(
            InputSchema,
            [tuple(row.values()) for row in rows],
        )
        .with_columns(
            g=pw.this.g.dt.strptime("%Y-%m-%dT%H:%M:%S", contains_timezone=False),
            h=pw.this.h.dt.strptime("%Y-%m-%dT%H:%M:%S%z", contains_timezone=True),
        )
        .update_types(n=np.ndarray[None, int], o=np.ndarray[None, float])  # type: ignore
    )
    if are_types_optional:
        table = table.update_types(
            g=pw.DateTimeNaive | None,
            h=pw.DateTimeUtc | None,
            n=np.ndarray[None, int] | None,  # type: ignore
            o=np.ndarray[None, float] | None,  # type: ignore
        )

    write_method(
        table,
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        init_mode=init_mode,
    )
    run()

    result = postgres.get_table_contents(table_name, InputSchema.column_names())

    for row in result:
        obj = api.deserialize(bytes(row["i"]))
        assert isinstance(
            obj, pw.PyObjectWrapper
        ), f"expecting PyObjectWrapper, got {type(obj)}"
        row["i"] = obj.value

    expected_output_row = {
        "a": "foo",
        "b": 1.5,
        "c": False,
        "d": [1, 2, 3],
        "e": [1, 2, 3],
        "f": {"foo": "bar", "baz": 123},
        "g": datetime.datetime(2025, 3, 14, 10, 13),
        "h": datetime.datetime(2025, 4, 23, 10, 13, tzinfo=datetime.timezone.utc),
        "i": SimpleObject("test"),
        "j": pd.Timedelta("4 days 2 seconds 123 us").value // 1_000,
        "k": ["abc", "def", "ghi"],
        "l": [
            pd.Timedelta("4 days 2 seconds 123 us").value // 1_000,
            pd.Timedelta("1 days 2 seconds 3 us").value // 1_000,
        ],
        "m": [["a", "b"], ["c", "d"]],
        "n": np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=int),
        "o": np.array(
            [[[1.1, 2.2], [3.3, 4.4]], [[5.5, 6.6], [7.7, 8.8]]], dtype=float
        ),
    }

    result = result[0]
    assert np.array_equal(result.pop("n"), expected_output_row.pop("n"))
    assert np.array_equal(result.pop("o"), expected_output_row.pop("o"))

    assert result == expected_output_row
    external_schema = postgres.get_table_schema(table_name)
    assert external_schema["a"].type_name == "text"
    assert external_schema["b"].type_name == "double precision"
    assert external_schema["c"].type_name == "boolean"
    assert external_schema["d"].type_name == "array"
    assert external_schema["e"].type_name == "array"
    assert external_schema["f"].type_name == "jsonb"
    assert external_schema["g"].type_name == "timestamp without time zone"
    assert external_schema["h"].type_name == "timestamp with time zone"
    assert external_schema["i"].type_name == "bytea"
    assert external_schema["j"].type_name == "bigint"
    assert external_schema["k"].type_name == "array"
    assert external_schema["l"].type_name == "array"
    assert external_schema["m"].type_name == "array"
    assert external_schema["n"].type_name == "array"
    assert external_schema["o"].type_name == "array"
    for column_name, column_props in external_schema.items():
        if column_name in ("time", "diff"):
            assert not column_props.is_nullable
            continue
        if column_name == "a":
            # `a` is pinned to non-nullable in the schema above so it
            # can be used as the snapshot-mode primary key, so the
            # emitted CREATE TABLE always marks it NOT NULL regardless
            # of `are_types_optional`.
            assert not column_props.is_nullable
            continue
        assert column_props.is_nullable == are_types_optional, column_name

    class TestObserver(pw.io.python.ConnectorObserver):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.n_rows_processed = 0

        def on_change(
            self, key: pw.Pointer, row: dict[str, Any], time: int, is_addition: bool
        ) -> None:
            self.n_rows_processed += 1
            assert row["a"] == "foo"
            assert row["b"] == 1.5
            assert not row["c"]
            assert row["d"] == (1, 2, 3)
            assert row["e"] == (1, 2, 3)
            assert row["f"] == pw.Json({"foo": "bar", "baz": 123})
            assert row["g"] == datetime.datetime(2025, 3, 14, 10, 13)
            assert row["h"] == datetime.datetime(
                2025, 4, 23, 10, 13, tzinfo=datetime.timezone.utc
            )
            assert row["i"].value == SimpleObject("test")
            assert row["j"] == pd.Timedelta("4 days 2 seconds 123 us")
            assert row["k"] == ("abc", "def", "ghi")
            assert row["l"] == (
                pd.Timedelta("4 days 2 seconds 123 us"),
                pd.Timedelta("1 days 2 seconds 3 us"),
            )
            assert row["m"] == (("a", "b"), ("c", "d"))

    # `g` / `h` start as strings in the input rows (strptime-parsed
    # into DateTimeNaive / DateTimeUtc before writing), so the stored
    # PostgreSQL columns are TIMESTAMP / TIMESTAMPTZ. Reading back with
    # the original InputSchema would declare str for those columns —
    # validate_table_schema catches the mismatch. Use a dedicated
    # OutputSchema that mirrors the *stored* types instead.
    if are_types_optional:

        class OutputSchema(pw.Schema):
            a: str
            b: float | None
            c: bool | None
            d: list[int] | None
            e: tuple[int, int, int] | None
            f: pw.Json | None
            g: pw.DateTimeNaive | None
            h: pw.DateTimeUtc | None
            i: pw.PyObjectWrapper[SimpleObject] | None
            j: pw.Duration | None
            k: list[str] | None
            l: list[pw.Duration] | None
            m: list[list[str]] | None
            n: Any
            o: Any

    else:

        class OutputSchema(pw.Schema):  # type:ignore
            a: str
            b: float
            c: bool
            d: list[int]
            e: tuple[int, int, int]
            f: pw.Json
            g: pw.DateTimeNaive
            h: pw.DateTimeUtc
            i: pw.PyObjectWrapper[SimpleObject]
            j: pw.Duration
            k: list[str]
            l: list[pw.Duration]
            m: list[list[str]]
            n: Any
            o: Any

    observer = TestObserver()
    G.clear()
    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=OutputSchema,
        mode="static",
    )
    pw.io.python.write(table, observer)
    run()
    assert observer.n_rows_processed == 1


@pytest.mark.parametrize(
    "init_mode, expected",
    [
        pytest.param(
            "create_if_not_exists",
            [{"i": 0, "data": 0}, {"i": 1, "data": 1}, {"i": 2, "data": 2}],
            id="create_if_not_exists-appends",
        ),
        pytest.param(
            "replace",
            [{"i": 2, "data": 2}],
            id="replace-keeps-only-last",
        ),
    ],
)
@pytest.mark.parametrize("write_method", [pw.io.postgres.write, write_snapshot(["i"])])
def test_init_mode_accumulates_or_replaces(write_method, postgres, init_mode, expected):
    """``create_if_not_exists`` preserves prior rows across runs;
    ``replace`` drops and re-creates the table each run, leaving only
    the final batch. The table shape (BIGINT NOT NULL columns) is
    verified after ``replace`` since it is the only mode that
    guarantees Pathway owns the CREATE TABLE DDL."""
    table_name = postgres.random_table_name()

    class InputSchema(pw.Schema):
        i: int
        data: int

    for i in range(3):
        G.clear()
        table = pw.debug.table_from_rows(InputSchema, [(i, i)])
        write_method(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode=init_mode,
        )
        run()

    result = postgres.get_table_contents(table_name, InputSchema.column_names(), "i")
    assert result == expected

    if init_mode == "replace":
        external_schema = postgres.get_table_schema(table_name)
        assert external_schema["i"] == ColumnProperties(
            type_name="bigint", is_nullable=False
        )
        assert external_schema["data"] == ColumnProperties(
            type_name="bigint", is_nullable=False
        )


def test_psql_json_datetimes(postgres):
    class InputSchema(pw.Schema):
        a: pw.Json
        b: pw.PyObjectWrapper[dict]
        c: pw.PyObjectWrapper[dict]

    table_name = postgres.random_table_name()

    @pw.udf
    def to_json(obj: pw.PyObjectWrapper[dict]) -> pw.Json:
        return pw.Json(obj.value)

    @pw.udf
    def to_json_wrapped(obj) -> pw.Json:
        return pw.Json({k: pw.Json(v) for k, v in obj.value.items()})

    obj = {
        "dtn": datetime.datetime(2025, 3, 14, 10, 13),
        "dt": datetime.datetime(
            2025, 3, 14, 10, 13, microsecond=123456, tzinfo=datetime.timezone.utc
        ),
        "pdn": pd.Timestamp("2025-03-14"),
        "pd": pd.Timestamp("2025-03-14T00:00+00:00"),
        "pwn": pw.DateTimeNaive("2025-03-14T10:13:00.123456789"),
        "pw": pw.DateTimeUtc("2025-03-14T10:13:00.123456000+00:00"),
        "dur": pd.Timedelta("4 days 2 microseconds"),
    }
    rows = [{"a": obj, "b": pw.wrap_py_object(obj), "c": pw.wrap_py_object(obj)}]

    table = pw.debug.table_from_rows(
        InputSchema,
        [tuple(row.values()) for row in rows],
    ).select(a=pw.this.a, b=to_json(pw.this.b), c=to_json_wrapped(pw.this.c))

    pw.io.postgres.write(
        table,
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        init_mode="replace",
    )
    run()

    result = postgres.get_table_contents(table_name, InputSchema.column_names())[0]
    expected = {
        "dtn": "2025-03-14T10:13:00.000000000",
        "dt": "2025-03-14T10:13:00.123456000+00:00",
        "pdn": "2025-03-14T00:00:00.000000000",
        "pd": "2025-03-14T00:00:00.000000000+00:00",
        "pwn": "2025-03-14T10:13:00.123456789",
        "pw": "2025-03-14T10:13:00.123456000+00:00",
        "dur": 345600000002000,
    }

    assert result["a"] == expected
    assert result["b"] == expected
    assert result["c"] == expected


# TODO: remove when pw.io.postgres.write_snapshot is fully deprecated.
def test_psql_external_diff_column_legacy(tmp_path, postgres):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool
        external_diff: int

    input_path = tmp_path / "input.txt"
    output_table = postgres.create_table(InputSchema, add_special_fields=True)

    def _run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.postgres.write_snapshot(
            table,
            POSTGRES_SETTINGS,
            output_table,
            ["name"],
            _external_diff_column=table.external_diff,
        )
        run()

    test_items = [
        {
            "name": "Milk",
            "count": 500,
            "price": 1.5,
            "available": False,
            "external_diff": 1,
        },
        {
            "name": "Water",
            "count": 600,
            "price": 0.5,
            "available": True,
            "external_diff": 1,
        },
    ]
    _run(test_items)

    rows = postgres.get_table_contents(output_table, InputSchema.column_names())
    rows.sort(key=lambda item: (item["name"], item["available"]))
    assert rows == test_items

    # Also test that the junk data in the additional columns would not break deletion
    new_test_items = [
        {
            "name": "Milk",
            "count": -1,
            "price": -1.0,
            "available": True,
            "external_diff": -1,
        }
    ]
    _run(new_test_items)

    rows = postgres.get_table_contents(
        output_table, InputSchema.column_names(), ("name", "available")
    )
    expected_rows = [
        {
            "name": "Water",
            "count": 600,
            "price": 0.5,
            "available": True,
            "external_diff": 1,
        },
    ]
    assert rows == expected_rows


def test_pgvector_vectors(pgvector):
    class OutputSchema(pw.Schema):
        i: int
        a_vector: np.ndarray
        b_halfvec: np.ndarray

    output_table = pgvector.create_table(OutputSchema, add_special_fields=True)

    @pw.udf
    def make_array(a: int) -> np.ndarray:
        return np.ones(3) * a

    t = pw.debug.table_from_markdown(
        """
        i | a | b
        1 | 1 | 2
        2 | 3 | 4
        3 | 5 | 6
    """
    ).select(pw.this.i, a_vector=make_array(pw.this.a), b_halfvec=make_array(pw.this.b))

    pw.io.postgres.write(t, PGVECTOR_SETTINGS, output_table)
    run()

    rows = pgvector.get_table_contents(output_table, OutputSchema.column_names())
    expected = {
        1: {"a_vector": "[1,1,1]", "b_halfvec": "[2,2,2]"},
        2: {"a_vector": "[3,3,3]", "b_halfvec": "[4,4,4]"},
        3: {"a_vector": "[5,5,5]", "b_halfvec": "[6,6,6]"},
    }
    assert len(rows) == 3
    for row in rows:
        expected_row = expected.pop(row["i"])
        for name in ["a_vector", "b_halfvec"]:
            assert row[name] == expected_row[name]
            # assert np.all(np.isclose(row[name], expected_row[name])) # FIXME


def test_psql_external_diff_column(tmp_path, postgres):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool
        external_diff: int

    input_path = tmp_path / "input.txt"
    output_table = postgres.create_table(InputSchema, add_special_fields=False)

    def _run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.postgres.write(
            table,
            POSTGRES_SETTINGS,
            output_table,
            output_table_type="snapshot",
            primary_key=[table.name],
            _external_diff_column=table.external_diff,
        )
        run()

    test_items = [
        {
            "name": "Milk",
            "count": 500,
            "price": 1.5,
            "available": False,
            "external_diff": 1,
        },
        {
            "name": "Water",
            "count": 600,
            "price": 0.5,
            "available": True,
            "external_diff": 1,
        },
    ]
    _run(test_items)

    rows = postgres.get_table_contents(output_table, InputSchema.column_names())
    rows.sort(key=lambda item: (item["name"], item["available"]))
    assert rows == test_items

    # Also test that the junk data in the additional columns would not break deletion
    new_test_items = [
        {
            "name": "Milk",
            "count": -1,
            "price": -1.0,
            "available": True,
            "external_diff": -1,
        }
    ]
    _run(new_test_items)

    rows = postgres.get_table_contents(
        output_table, InputSchema.column_names(), ("name", "available")
    )
    expected_rows = [
        {
            "name": "Water",
            "count": 600,
            "price": 0.5,
            "available": True,
            "external_diff": 1,
        },
    ]
    assert rows == expected_rows


def test_psql_output_snapshot(tmp_path, postgres):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_table = postgres.create_table(InputSchema, add_special_fields=False)

    def _run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.postgres.write(
            table,
            POSTGRES_SETTINGS,
            output_table,
            output_table_type="snapshot",
            primary_key=[table.name],
        )
        run()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    _run(test_items)

    rows = postgres.get_table_contents(output_table, InputSchema.column_names())
    rows.sort(key=lambda item: item["name"])
    assert rows == test_items

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    _run(new_test_items)

    rows = postgres.get_table_contents(output_table, InputSchema.column_names(), "name")

    expected_rows = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert rows == expected_rows


@pytest.mark.parametrize(
    "sslmode", ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]
)
def test_tls_no_cert(postgres_with_tls, sslmode):
    class InputSchema(pw.Schema):
        data: str

    table = pw.demo.generate_custom_stream(
        value_generators={"data": lambda x: str(x + 1)},
        schema=InputSchema,
        nb_rows=5,
        input_rate=5,
        autocommit_duration_ms=10,
    )

    settings = copy.deepcopy(POSTGRES_WITH_TLS_SETTINGS)
    settings["sslmode"] = sslmode

    output_table = postgres_with_tls.create_table(InputSchema, add_special_fields=True)
    pw.io.postgres.write(
        table,
        postgres_settings=settings,
        table_name=output_table,
    )

    if sslmode == "disable":
        with pytest.raises(
            IOError,
            match="Failed to establish PostgreSQL connection",
        ):
            pw.run()
    elif sslmode == "verify-ca" or sslmode == "verify-full":
        with pytest.raises(
            IOError,
            match="Failed to establish PostgreSQL connection: "
            "ssl certificate is not provided",
        ):
            pw.run()
    else:
        pw.run()
        rows = postgres_with_tls.get_table_contents(
            output_table, InputSchema.column_names(), sort_by="data"
        )
        assert rows == [
            {"data": "1"},
            {"data": "2"},
            {"data": "3"},
            {"data": "4"},
            {"data": "5"},
        ]


@pytest.mark.parametrize("sslmode", ["verify-ca", "verify-full"])
def test_tls_with_cert(postgres_with_tls, sslmode):
    class InputSchema(pw.Schema):
        data: str

    table = pw.demo.generate_custom_stream(
        value_generators={"data": lambda x: str(x + 1)},
        schema=InputSchema,
        nb_rows=5,
        input_rate=5,
        autocommit_duration_ms=10,
    )

    settings = copy.deepcopy(POSTGRES_WITH_TLS_SETTINGS)
    settings["sslmode"] = sslmode
    settings["sslrootcert"] = str(CREDENTIALS_DIR / "ca.crt")

    output_table = postgres_with_tls.create_table(InputSchema, add_special_fields=True)
    pw.io.postgres.write(
        table,
        postgres_settings=settings,
        table_name=output_table,
    )
    pw.run()

    rows = postgres_with_tls.get_table_contents(
        output_table, InputSchema.column_names(), sort_by="data"
    )
    assert rows == [
        {"data": "1"},
        {"data": "2"},
        {"data": "3"},
        {"data": "4"},
        {"data": "5"},
    ]


@pytest.mark.parametrize("sslmode", ["verify-ca", "verify-full"])
def test_tls_with_incorrect_cert(tmp_path, postgres_with_tls, sslmode):
    cert_path = tmp_path / "ca.crt"
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "Bad Test CA"),
        ]
    )
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_subject)
        .issuer_name(ca_subject)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        )
        .not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30)
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    cert_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))

    class InputSchema(pw.Schema):
        data: str

    table = pw.demo.generate_custom_stream(
        value_generators={"data": lambda x: str(x + 1)},
        schema=InputSchema,
        nb_rows=5,
        input_rate=5,
        autocommit_duration_ms=10,
    )

    settings = copy.deepcopy(POSTGRES_WITH_TLS_SETTINGS)
    settings["sslmode"] = sslmode
    settings["sslrootcert"] = str(cert_path)

    output_table = postgres_with_tls.create_table(InputSchema, add_special_fields=True)
    pw.io.postgres.write(
        table,
        postgres_settings=settings,
        table_name=output_table,
    )
    with pytest.raises(
        IOError,
        match="Failed to establish PostgreSQL connection",
    ):
        pw.run()


@pytest.mark.parametrize("with_tls", [False, True])
def test_postgres_input(tmp_path, postgres, postgres_with_tls, with_tls):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        v_none: bool | None
        v_bool: bool
        v_int: int
        v_float: float
        v_string: str
        v_bytes: bytes
        v_int_array: np.ndarray[None, int]  # type: ignore
        v_float_array: np.ndarray[None, float]  # type: ignore
        v_datetime_naive: pw.DateTimeNaive
        v_datetime_utc: pw.DateTimeUtc
        v_duration: pw.Duration
        v_json: pw.Json
        v_pyobject: bytes
        v_int_list: list[list[int]]
        v_float_list: list[list[float]]
        v_string_matrix: list[list[str]]
        v_bytes_matrix: list[list[bytes]]

    if with_tls:
        postgres = postgres_with_tls
        postgres_settings = copy.deepcopy(POSTGRES_WITH_TLS_SETTINGS)
        postgres_settings["sslmode"] = "verify-ca"
        postgres_settings["sslrootcert"] = str(CREDENTIALS_DIR / "ca.crt")
    else:
        postgres_settings = POSTGRES_SETTINGS

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()
    create_table_sql = f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            v_none BOOLEAN,
            v_bool BOOLEAN,
            v_int BIGINT,
            v_float DOUBLE PRECISION,
            v_string TEXT,
            v_bytes BYTEA,
            v_int_array BIGINT[],
            v_float_array DOUBLE PRECISION[],
            v_datetime_naive TIMESTAMP,
            v_datetime_utc TIMESTAMPTZ,
            v_duration INTERVAL,
            v_json JSONB,
            v_pyobject BYTEA,
            v_int_list BIGINT[],
            v_float_list DOUBLE PRECISION[],
            v_string_matrix TEXT[],
            v_bytes_matrix BYTEA[]
        );
    """
    postgres.execute_sql(create_table_sql)

    with postgres.publication(table_name) as publication_name:
        json_value_dumped = json.dumps({"key": "value"})
        insert_row_sql = f"""
            INSERT INTO {table_name} (
                v_none,
                v_bool,
                v_int,
                v_float,
                v_string,
                v_bytes,
                v_int_array,
                v_float_array,
                v_datetime_naive,
                v_datetime_utc,
                v_duration,
                v_json,
                v_pyobject,
                v_int_list,
                v_float_list,
                v_string_matrix,
                v_bytes_matrix
            ) VALUES (
                null,
                TRUE,
                42,
                3.1415926535,
                'hello world',
                '\\xDEADBEEF'::bytea,
                ARRAY[1, 2, 3, 4],
                ARRAY[1.1, 2.2, 3.3],
                '2025-01-01 12:00:00',
                '2025-01-01 12:00:00+00',
                INTERVAL '1 hour 30 minutes',
                '{json_value_dumped}',
                '\\xABCDEF'::bytea,
                ARRAY[[1, 2], [3, 4]],
                ARRAY[[1.1, 2.2], [3.3, 4.4]],
                ARRAY[['a}},{{', '{{,,,,,,{{b'], ['c,,', ',,d']],
                ARRAY[
                    ['\\xDEAD'::bytea, '\\xBEEF'::bytea],
                    ['\\xABCD'::bytea, '\\xEF01'::bytea]
                ]
            );
        """
        postgres.execute_sql(insert_row_sql)

        table = pw.io.postgres.read(
            postgres_settings=postgres_settings,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            for i in range(5):
                wait_result_with_checker(
                    FileLinesNumberChecker(output_path, i + 1), 30, target=None
                )
                postgres.execute_sql(insert_row_sql)
            for i in range(5):
                wait_result_with_checker(
                    FileLinesNumberChecker(output_path, 6 + i), 30, target=None
                )
                delete_row_sql = f"DELETE FROM {table_name} WHERE id = {i + 1};"
                postgres.execute_sql(delete_row_sql)

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 11), 60)

    ids = set()
    with open(output_path, "r") as f:
        for row in f:
            data = json.loads(row)
            row_id = data.pop("id")
            data.pop("time")
            diff = data.pop("diff")
            if diff == 1:
                assert row_id not in ids
                ids.add(row_id)
            elif diff == -1:
                assert row_id in ids
                ids.remove(row_id)
            else:
                raise ValueError(f"unexpected diff: {diff}")
            assert data == {
                "v_none": None,
                "v_bool": True,
                "v_int": 42,
                "v_float": 3.1415926535,
                "v_string": "hello world",
                "v_bytes": "3q2+7w==",
                "v_int_array": {"shape": [4], "elements": [1, 2, 3, 4]},
                "v_float_array": {"shape": [3], "elements": [1.1, 2.2, 3.3]},
                "v_datetime_naive": "2025-01-01T12:00:00.000000000",
                "v_datetime_utc": "2025-01-01T12:00:00.000000000+0000",
                "v_duration": 5400000000000,
                "v_json": {"key": "value"},
                "v_pyobject": "q83v",
                "v_int_list": [[1, 2], [3, 4]],
                "v_float_list": [[1.1, 2.2], [3.3, 4.4]],
                "v_string_matrix": [["a},{", "{,,,,,,{b"], ["c,,", ",,d"]],
                "v_bytes_matrix": [["3q0=", "vu8="], ["q80=", "7wE="]],
            }
    assert len(ids) == 1


def test_postgres_snapshot_only(tmp_path, postgres):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            value TEXT NOT NULL
        );
    """
    )

    for i in range(5):
        postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('row_{i}');")

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert len(rows) == 5
    values = {r["value"] for r in rows}
    assert values == {f"row_{i}" for i in range(5)}
    assert all(r["diff"] == 1 for r in rows)


def test_postgres_streaming_inserts_only(tmp_path, postgres):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            value TEXT NOT NULL
        );
    """
    )

    with postgres.publication(table_name) as publication_name:
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            for i in range(10):
                postgres.execute_sql(
                    f"INSERT INTO {table_name} (value) VALUES ('row_{i}');"
                )
                wait_result_with_checker(
                    FileLinesNumberChecker(output_path, i + 1), 30, target=None
                )

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 10), 60)

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert len(rows) == 10
    assert all(r["diff"] == 1 for r in rows)
    ids = {r["id"] for r in rows}
    assert len(ids) == 10


def test_postgres_update_by_primary_key(tmp_path, postgres):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            value TEXT NOT NULL
        );
    """
    )
    with postgres.publication(table_name) as publication_name:
        postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('value');")

        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 1), 30, target=None
            )
            for i in range(10):
                postgres.execute_sql(
                    f"UPDATE {table_name} SET id = {i + 2} WHERE id = {i + 1};"
                )
                wait_result_with_checker(
                    FileLinesNumberChecker(output_path, 3 + 2 * i), 30, target=None
                )

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 21), 60)


def test_postgres_read_after_truncate(tmp_path, postgres):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            value TEXT NOT NULL
        );
    """
    )
    with postgres.publication(table_name) as publication_name:
        for i in range(5):
            postgres.execute_sql(
                f"INSERT INTO {table_name} (value) VALUES ('row_{i}');"
            )

        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 5), 30, target=None
            )
            postgres.execute_sql(f"TRUNCATE {table_name};")

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 10), 60)


@pytest.mark.parametrize(
    "pg_interval,expected_td",
    [
        ("0", datetime.timedelta(0)),
        ("1 microsecond", datetime.timedelta(microseconds=1)),
        ("1 millisecond", datetime.timedelta(milliseconds=1)),
        ("1 second", datetime.timedelta(seconds=1)),
        ("1 minute", datetime.timedelta(minutes=1)),
        ("1 hour", datetime.timedelta(hours=1)),
        ("1 day", datetime.timedelta(days=1)),
        ("7 days", datetime.timedelta(weeks=1)),
        ("-1 second", datetime.timedelta(seconds=-1)),
        ("-1 day", datetime.timedelta(days=-1)),
        ("1 hour 30 minutes", datetime.timedelta(hours=1, minutes=30)),
        (
            "2 days 12 hours 30 minutes",
            datetime.timedelta(days=2, hours=12, minutes=30),
        ),
    ],
)
def test_postgres_external_interval_parsing(
    tmp_path, postgres, pg_interval, expected_td
):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        duration: pw.Duration

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            duration INTERVAL NOT NULL
        );
        """
    )
    with postgres.publication(table_name) as publication_name:
        postgres.execute_sql(
            f"INSERT INTO {table_name} (id, duration) VALUES (1, INTERVAL '{pg_interval}');"
        )

        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 30)

    with open(output_path) as f:
        rows = [json.loads(line) for line in f]

    result = next(r for r in rows if r["diff"] == 1)
    expected_ns = int(expected_td.total_seconds() * 1_000_000_000)
    assert (
        result["duration"] == expected_ns
    ), f"INTERVAL '{pg_interval}': expected {expected_ns} ns, got {result['duration']}"


def test_postgres_read_table_without_primary_key_streaming(tmp_path, postgres):
    """Table without a primary key should raise an error in streaming mode too."""

    class InputSchema(pw.Schema):
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            value TEXT NOT NULL
        );
        """
    )

    with postgres.publication(table_name) as publication_name:
        postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('hello');")

        with pytest.raises(RuntimeError, match="no primary key"):
            table = pw.io.postgres.read(
                postgres_settings=POSTGRES_SETTINGS,
                table_name=table_name,
                schema=InputSchema,
                mode="streaming",
                publication_name=publication_name,
                autocommit_duration_ms=10,
            )
            pw.io.jsonlines.write(table, output_path)
            pw.run()


@pytest.mark.parametrize(
    "missing_columns, error_pattern",
    [
        pytest.param(
            {"nonexistent_column": "str"},
            "nonexistent_column",
            id="single-missing",
        ),
        pytest.param(
            {"missing_a": "str", "missing_b": "int"},
            "missing_a|missing_b",
            id="multiple-missing",
        ),
    ],
)
def test_postgres_read_rejects_missing_value_fields(
    tmp_path, postgres, missing_columns, error_pattern
):
    """A pw.Schema that names columns absent from the destination
    must be rejected with an error that lists every missing name —
    for both the single-missing and the multiple-missing cases."""

    annotations = {
        "id": int,
        "value": str,
        **{n: eval(t) for n, t in missing_columns.items()},
    }
    InputSchema = type(
        "InputSchema",
        (pw.Schema,),
        {
            "__annotations__": annotations,
            "id": pw.column_definition(primary_key=True),
        },
    )

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('hello');")

    with pytest.raises(RuntimeError, match=error_pattern):
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="static",
        )
        pw.io.jsonlines.write(table, output_path)
        pw.run()


def test_postgres_read_extra_columns_in_table_are_allowed(tmp_path, postgres):
    """Columns in the table that are NOT in the schema should be silently ignored."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str
        # 'extra_column' exists in the table but is not declared in the schema — that's fine

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            value TEXT NOT NULL,
            extra_column INT
        );
        """
    )
    postgres.execute_sql(
        f"INSERT INTO {table_name} (value, extra_column) VALUES ('hello', 42);"
    )

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert len(rows) == 1
    assert rows[0]["value"] == "hello"
    assert "extra_column" not in rows[0]


@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param("subset", id="subset-of-composite-pk"),
        pytest.param("superset", id="superset-of-single-pk"),
    ],
)
def test_postgres_read_primary_key_mismatch(tmp_path, postgres, scenario):
    """Streaming reader rejects a pw.Schema whose primary-key columns
    do not match the PG table's PRIMARY KEY constraint. Exercises
    both directions: declaring *fewer* PK columns than the table
    (subset: composite PG PK, single Pathway PK) and declaring
    *more* PK columns than the table (superset: single PG PK, two
    Pathway PK columns)."""

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    if scenario == "subset":

        class InputSchema(pw.Schema):
            id_a: int = pw.column_definition(primary_key=True)
            id_b: int
            value: str

        postgres.execute_sql(
            f"""
            CREATE TABLE {table_name} (
                id_a INT NOT NULL,
                id_b INT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (id_a, id_b)
            );
            """
        )
        error_pattern = "primary key mismatch|id_a|id_b"
    else:

        class InputSchema(pw.Schema):  # type: ignore[no-redef]
            id: int = pw.column_definition(primary_key=True)
            value: str = pw.column_definition(primary_key=True)

        postgres.execute_sql(
            f"""
            CREATE TABLE {table_name} (
                id BIGSERIAL PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('hello');")
        error_pattern = "primary key mismatch|value"

    with postgres.publication(table_name) as publication_name:
        with pytest.raises(RuntimeError, match=error_pattern):
            table = pw.io.postgres.read(
                postgres_settings=POSTGRES_SETTINGS,
                table_name=table_name,
                schema=InputSchema,
                mode="streaming",
                publication_name=publication_name,
            )
            pw.io.jsonlines.write(table, output_path)
            pw.run()


def test_postgres_read_correct_composite_primary_key(tmp_path, postgres):
    """Correctly specifying all composite PK columns should work fine."""

    class InputSchema(pw.Schema):
        id_a: int = pw.column_definition(primary_key=True)
        id_b: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id_a INT NOT NULL,
            id_b INT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (id_a, id_b)
        );
        """
    )
    postgres.execute_sql(
        f"INSERT INTO {table_name} (id_a, id_b, value) VALUES (1, 2, 'hello');"
    )

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert len(rows) == 1
    assert rows[0]["value"] == "hello"


def test_postgres_append_only_fails_on_deletes(tmp_path, postgres):
    """In append-only mode, DELETE events must be silently ignored."""

    class InputSchema(pw.Schema):
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            value TEXT NOT NULL PRIMARY KEY
        );
        """
    )

    with postgres.publication(table_name) as publication_name:
        for i in range(3):
            postgres.execute_sql(
                f"INSERT INTO {table_name} (value) VALUES ('row_{i}');"
            )

        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
            is_append_only=True,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 3), 30, target=None
            )
            # DELETE must trigger an explicit error
            postgres.execute_sql(f"DELETE FROM {table_name} WHERE value = 'row_0';")

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()

        with pytest.raises(
            api.EngineError, match="Modification of a non-append-only table"
        ):
            run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert all(
        r["diff"] == 1 for r in rows
    ), f"Expected all diff=1, got: {[r['diff'] for r in rows]}"
    values = {r["value"] for r in rows}
    assert values == {"row_0", "row_1", "row_2"}


def test_postgres_without_primary_key(tmp_path, postgres):
    """In append-only mode, DELETE events must be silently ignored."""

    class InputSchema(pw.Schema):
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            value TEXT NOT NULL
        );
        """
    )

    for i in range(3):
        postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('row_{i}');")

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
        autocommit_duration_ms=10,
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert all(
        r["diff"] == 1 for r in rows
    ), f"Expected all diff=1, got: {[r['diff'] for r in rows]}"
    values = {r["value"] for r in rows}
    assert values == {"row_0", "row_1", "row_2"}


def test_postgres_read_and_parse_extra_types(tmp_path, postgres):
    class InputSchema(pw.Schema):
        id: str = pw.column_definition(primary_key=True)
        birthday: pw.DateTimeNaive
        meeting_time: pw.Duration
        meeting_timetz: pw.Duration
        ip_addr: str
        network: str
        mac_addr: str
        object_id: int
        price: float
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id UUID PRIMARY KEY,
            birthday DATE NOT NULL,
            meeting_time TIME NOT NULL,
            meeting_timetz TIMETZ NOT NULL,
            ip_addr INET NOT NULL,
            network CIDR NOT NULL,
            mac_addr MACADDR NOT NULL,
            object_id OID NOT NULL,
            price NUMERIC(10, 4) NOT NULL,
            value TEXT NOT NULL
        );
        """
    )

    # Each row tests a different edge case combination
    rows_data = [
        # (id, birthday, meeting_time, meeting_timetz, ip_addr, network,
        # mac_addr, object_id, price, value, description)
        (
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "2025-03-14",
            "12:30:00",
            "12:30:00+02:00",  # positive timezone +02:00
            "192.168.1.1",  # IPv4 private
            "192.168.1.0/24",  # IPv4 CIDR
            "08:00:2b:01:02:03",
            12345,
            123.4567,
            "positive_tz",
        ),
        (
            "b1ffcd00-1d1c-5fa9-cc7e-7cc0ce491b22",
            "2025-03-14",
            "12:30:00",
            "12:30:00-05:00",  # negative timezone -05:00
            "10.0.0.1",  # IPv4 private class A
            "10.0.0.0/8",  # IPv4 CIDR /8
            "ff:ff:ff:ff:ff:ff",  # broadcast MAC
            99999,
            0.0001,  # very small price
            "negative_tz",
        ),
        (
            "c2ffcd00-1d1c-5fa9-cc7e-7cc0ce491b33",
            "2025-03-14",
            "00:00:00",  # midnight
            "00:00:00+00:00",  # UTC (zero offset)
            "2001:db8::1",  # IPv6 address
            "2001:db8::/32",  # IPv6 CIDR
            "00:00:00:00:00:00",  # zero MAC
            0,
            9999.9999,  # max price
            "ipv6_midnight_utc",
        ),
        (
            "d3ffcd00-1d1c-5fa9-cc7e-7cc0ce491b44",
            "1970-01-01",  # minimum DATE
            "23:59:59",  # end of day
            "23:59:59+14:00",  # maximum positive timezone +14:00
            "255.255.255.255",  # IPv4 broadcast
            "0.0.0.0/0",  # IPv4 default route
            "0a:0b:0c:0d:0e:0f",
            4294967295,  # max OID (2^32 - 1)
            1234.5678,
            "edge_max_tz",
        ),
        (
            "e4ffcd00-1d1c-5fa9-cc7e-7cc0ce491b55",
            "2099-12-31",  # maximum DATE
            "23:59:59",
            "23:59:59-12:00",  # minimum negative timezone -12:00
            "127.0.0.1",  # loopback IPv4
            "127.0.0.0/8",  # loopback CIDR
            "de:ad:be:ef:00:01",
            1,
            0.0000,  # zero price
            "edge_min_tz",
        ),
        (
            "f5ffcd00-1d1c-5fa9-cc7e-7cc0ce491b66",
            "2025-03-14",
            "12:30:00",
            "12:30:00+05:30",  # half-hour offset (India)
            "::1",  # IPv6 loopback
            "::1/128",  # IPv6 loopback CIDR /128
            "aa:bb:cc:dd:ee:ff",
            42,
            42.0000,
            "ipv6_loopback_half_tz",
        ),
    ]

    def _ns(h, m, s=0):
        """Convert hours/minutes/seconds to nanoseconds."""
        return (h * 3600 + m * 60 + s) * 1_000_000_000

    # Expected values per row value label
    expected = {
        "positive_tz": {
            "birthday": "2025-03-14T00:00:00.000000000",
            "meeting_time": _ns(12, 30),
            "meeting_timetz": _ns(10, 30),  # 12:30 - 02:00
            "ip_addr": "192.168.1.1",
            "network": "192.168.1.0/24",
            "mac_addr": "08:00:2b:01:02:03",
            "object_id": 12345,
            "price": 123.4567,
        },
        "negative_tz": {
            "birthday": "2025-03-14T00:00:00.000000000",
            "meeting_time": _ns(12, 30),
            "meeting_timetz": _ns(17, 30),  # 12:30 - (-05:00) = 17:30
            "ip_addr": "10.0.0.1",
            "network": "10.0.0.0/8",
            "mac_addr": "ff:ff:ff:ff:ff:ff",
            "object_id": 99999,
            "price": 0.0001,
        },
        "ipv6_midnight_utc": {
            "birthday": "2025-03-14T00:00:00.000000000",
            "meeting_time": _ns(0, 0),
            "meeting_timetz": _ns(0, 0),  # 00:00 UTC+00 → 00:00 UTC
            "ip_addr": "2001:db8::1",
            "network": "2001:db8::/32",
            "mac_addr": "00:00:00:00:00:00",
            "object_id": 0,
            "price": 9999.9999,
        },
        "edge_max_tz": {
            "birthday": "1970-01-01T00:00:00.000000000",
            "meeting_time": _ns(23, 59, 59),
            "meeting_timetz": _ns(9, 59, 59),  # 23:59:59 - 14:00 = 09:59:59
            "ip_addr": "255.255.255.255",
            "network": "0.0.0.0/0",
            "mac_addr": "0a:0b:0c:0d:0e:0f",
            "object_id": 4294967295,
            "price": 1234.5678,
        },
        "edge_min_tz": {
            "birthday": "2099-12-31T00:00:00.000000000",
            "meeting_time": _ns(23, 59, 59),
            "meeting_timetz": _ns(35, 59, 59),  # 23:59:59 - (-12:00) = 35:59:59
            "ip_addr": "127.0.0.1",
            "network": "127.0.0.0/8",
            "mac_addr": "de:ad:be:ef:00:01",
            "object_id": 1,
            "price": 0.0,
        },
        "ipv6_loopback_half_tz": {
            "birthday": "2025-03-14T00:00:00.000000000",
            "meeting_time": _ns(12, 30),
            "meeting_timetz": _ns(7, 0),  # 12:30 - 05:30 = 07:00
            "ip_addr": "::1",
            "network": "::1/128",
            "mac_addr": "aa:bb:cc:dd:ee:ff",
            "object_id": 42,
            "price": 42.0,
        },
    }

    # Insert snapshot rows (all except the last one which will be streamed)
    snapshot_rows = rows_data[:-1]
    streaming_row_data = rows_data[-1]

    for row in snapshot_rows:
        (rid, birthday, mtime, mtimetz, ip, net, mac, oid, price, val) = row
        postgres.execute_sql(
            f"""
            INSERT INTO {table_name}
            (id, birthday, meeting_time, meeting_timetz, ip_addr, network, mac_addr, object_id, price, value)
            VALUES (
                '{rid}', '{birthday}', '{mtime}', '{mtimetz}',
                '{ip}', '{net}', '{mac}', {oid}, {price}, '{val}'
            );
            """
        )

    def assert_row(row, value):
        exp = expected[value]
        assert row["birthday"] == exp["birthday"], f"[{value}] birthday mismatch"
        assert (
            row["meeting_time"] == exp["meeting_time"]
        ), f"[{value}] meeting_time mismatch"
        assert (
            row["meeting_timetz"] == exp["meeting_timetz"]
        ), f"[{value}] meeting_timetz mismatch"
        assert row["ip_addr"] == exp["ip_addr"], f"[{value}] ip_addr mismatch"
        assert row["network"] == exp["network"], f"[{value}] network mismatch"
        assert row["mac_addr"] == exp["mac_addr"], f"[{value}] mac_addr mismatch"
        assert row["object_id"] == exp["object_id"], f"[{value}] object_id mismatch"
        assert row["price"] == pytest.approx(
            exp["price"], rel=1e-4
        ), f"[{value}] price mismatch"
        assert row["value"] == value, f"[{value}] value mismatch"

    n_snapshot = len(snapshot_rows)
    n_total = len(rows_data)

    with postgres.publication(table_name) as publication_name:
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        (rid, birthday, mtime, mtimetz, ip, net, mac, oid, price, val) = (
            streaming_row_data
        )

        def stream_target():
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, n_snapshot), 30, target=None
            )
            postgres.execute_sql(
                f"""
                INSERT INTO {table_name}
                (id, birthday, meeting_time, meeting_timetz, ip_addr, network, mac_addr, object_id, price, value)
                VALUES (
                    '{rid}', '{birthday}', '{mtime}', '{mtimetz}',
                    '{ip}', '{net}', '{mac}', {oid}, {price}, '{val}'
                );
                """
            )

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()
        wait_result_with_checker(FileLinesNumberChecker(output_path, n_total), 60)

    rows_out = []
    with open(output_path) as f:
        for line in f:
            rows_out.append(json.loads(line))

    assert len(rows_out) == n_total

    id_to_row = {r["id"]: r for r in rows_out}

    # Verify all snapshot rows
    for row_data in snapshot_rows:
        rid, *_, val = row_data
        assert rid in id_to_row, f"Missing row {rid} ({val})"
        assert_row(id_to_row[rid], val)

    # Verify streaming row
    rid, *_, val = streaming_row_data
    assert rid in id_to_row, f"Missing streaming row {rid} ({val})"
    assert_row(id_to_row[rid], val)


def test_postgres_date_out_of_range_skipped(tmp_path, postgres):
    """Rows with dates/timestamps outside chrono's supported nanosecond range must be
    skipped with a warning, without crashing the reader. Valid rows must still appear.
    Covers DATE, TIMESTAMP, and TIMESTAMP WITH TIME ZONE column types."""

    class InputSchema(pw.Schema):
        id: str = pw.column_definition(primary_key=True)
        birthday: pw.DateTimeNaive
        created_at: pw.DateTimeNaive
        updated_at: pw.DateTimeUtc
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id UUID PRIMARY KEY,
            birthday DATE NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            value TEXT NOT NULL
        );
        """
    )

    VALID_SNAPSHOT_ID = "a0000000-0000-0000-0000-000000000001"
    OOR_DATE_PAST_ID = "b0000000-0000-0000-0000-000000000002"  # DATE 0001-01-01
    OOR_DATE_FUTURE_ID = "c0000000-0000-0000-0000-000000000003"  # DATE 9999-12-31
    OOR_TS_PAST_ID = "d0000000-0000-0000-0000-000000000004"  # TIMESTAMP 0001-01-01
    OOR_TS_FUTURE_ID = "e0000000-0000-0000-0000-000000000005"  # TIMESTAMP 9999-12-31
    OOR_TSZ_PAST_ID = "f0000000-0000-0000-0000-000000000006"  # TIMESTAMPTZ 0001-01-01
    OOR_TSZ_FUTURE_ID = "a0000000-0000-0000-0000-000000000007"  # TIMESTAMPTZ 9999-12-31
    VALID_STREAMING_ID = "b0000000-0000-0000-0000-000000000008"

    # (id, birthday, created_at, updated_at, value)
    VALID_DATE = "2025-03-14"
    VALID_TS = "2025-03-14 12:00:00"
    VALID_TSZ = "2025-03-14 12:00:00+00"
    OOR_PAST_DATE = "0001-01-01"
    OOR_FUTURE_DATE = "9999-12-31"
    OOR_PAST_TS = "0001-01-01 00:00:00"
    OOR_FUTURE_TS = "9999-12-31 23:59:59"
    OOR_PAST_TSZ = "0001-01-01 00:00:00+00"
    OOR_FUTURE_TSZ = "9999-12-31 23:59:59+00"

    snapshot_inserts = [
        (VALID_SNAPSHOT_ID, VALID_DATE, VALID_TS, VALID_TSZ, "valid_snapshot"),
        (OOR_DATE_PAST_ID, OOR_PAST_DATE, VALID_TS, VALID_TSZ, "oor_date_past"),
        (OOR_DATE_FUTURE_ID, OOR_FUTURE_DATE, VALID_TS, VALID_TSZ, "oor_date_future"),
        (OOR_TS_PAST_ID, VALID_DATE, OOR_PAST_TS, VALID_TSZ, "oor_ts_past"),
        (OOR_TS_FUTURE_ID, VALID_DATE, OOR_FUTURE_TS, VALID_TSZ, "oor_ts_future"),
        (OOR_TSZ_PAST_ID, VALID_DATE, VALID_TS, OOR_PAST_TSZ, "oor_tsz_past"),
        (OOR_TSZ_FUTURE_ID, VALID_DATE, VALID_TS, OOR_FUTURE_TSZ, "oor_tsz_future"),
    ]
    for rid, birthday, created_at, updated_at, val in snapshot_inserts:
        postgres.execute_sql(
            f"""
            INSERT INTO {table_name} (id, birthday, created_at, updated_at, value)
            VALUES ('{rid}', '{birthday}', '{created_at}', '{updated_at}', '{val}');
            """
        )

    OOR_IDS = {
        OOR_DATE_PAST_ID,
        OOR_DATE_FUTURE_ID,
        OOR_TS_PAST_ID,
        OOR_TS_FUTURE_ID,
        OOR_TSZ_PAST_ID,
        OOR_TSZ_FUTURE_ID,
    }
    # 1 valid snapshot row + 1 valid streaming row
    n_expected = 2

    with postgres.publication(table_name) as publication_name:
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 1), 30, target=None
            )
            postgres.execute_sql(
                f"""
                INSERT INTO {table_name} (id, birthday, created_at, updated_at, value)
                VALUES (
                    '{VALID_STREAMING_ID}', '{VALID_DATE}', '{VALID_TS}', '{VALID_TSZ}',
                    'valid_streaming'
                );
                """
            )

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()
        wait_result_with_checker(FileLinesNumberChecker(output_path, n_expected), 60)

    rows_out = []
    with open(output_path) as f:
        for line in f:
            rows_out.append(json.loads(line))

    assert len(rows_out) == n_expected

    ids_out = {r["id"] for r in rows_out}
    assert VALID_SNAPSHOT_ID in ids_out, "Valid snapshot row must be present"
    assert VALID_STREAMING_ID in ids_out, "Valid streaming row must be present"
    for oor_id in OOR_IDS:
        assert oor_id not in ids_out, f"Out-of-range row {oor_id} must be skipped"


@pytest.mark.parametrize("pg_type", ["BIGINT", "INTEGER"])
def test_static_int_read_as_duration(tmp_path, postgres, pg_type):
    """Integral columns declared as pw.Duration must be returned as Duration values
    (nanoseconds in jsonlines output) in static read mode."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        duration_col: pw.Duration

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id {pg_type} PRIMARY KEY,
            duration_col {pg_type} NOT NULL
        );
        """
    )
    # 10 seconds expressed in microseconds, as the output connector writes pw.Duration
    microseconds = 10_000_000
    postgres.execute_sql(
        f"INSERT INTO {table_name} (id, duration_col) VALUES (1, {microseconds});"
    )

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert len(rows) == 1
    # pw.Duration is serialized to jsonlines as nanoseconds
    assert rows[0]["duration_col"] == microseconds * 1_000


def test_static_bigint_array_read_as_duration_list(tmp_path, postgres):
    """BIGINT[] columns declared as list[pw.Duration] must be returned as lists of
    Duration values (each element in nanoseconds in jsonlines) in static read mode."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        durations: list[pw.Duration]

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            durations BIGINT[] NOT NULL
        );
        """
    )
    # Three durations in microseconds: 1s, 2s, 3s
    postgres.execute_sql(
        f"INSERT INTO {table_name} (id, durations)"
        f" VALUES (1, ARRAY[1000000, 2000000, 3000000]::BIGINT[]);"
    )

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert len(rows) == 1
    # Each Duration element serialized as nanoseconds
    assert rows[0]["durations"] == [1_000_000_000, 2_000_000_000, 3_000_000_000]


def test_no_publication(tmp_path, postgres):
    class InputSchema(pw.Schema):
        value: str

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()

    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            value TEXT PRIMARY KEY
        );
        """
    )

    postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('hello');")
    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="streaming",
        publication_name=f"{table_name}_pub",
        autocommit_duration_ms=10,
    )
    pw.io.jsonlines.write(table, output_path)
    with pytest.raises(
        RuntimeError, match=f"Publication '{table_name}_pub' does not exist"
    ):
        pw.run()


def _pg_quote_ident(name: str) -> str:
    """Escape a PostgreSQL identifier with double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _pg_write(table, *, table_name, init_mode):
    """Run ``pw.io.postgres.write`` against the local postgres fixture
    and execute the pipeline, bound over the connection settings so the
    shared helpers stay connection-agnostic."""
    pw.io.postgres.write(
        table,
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        init_mode=init_mode,
    )
    run()


def test_psql_write_table_name_with_special_characters(postgres):
    """``pw.io.postgres.write`` does not quote the ``table_name`` in the
    CREATE TABLE or INSERT statements it generates. A name that PostgreSQL
    accepts when quoted — a hyphen, a mixed-case identifier, a reserved
    word — therefore crashes the writer with an opaque engine-worker
    ``db error`` at pipeline start, even though the same name works fine
    on the *read* side (``PsqlReader::select_prepared_records`` already
    wraps the table and schema name with ``quote_identifier``).

    The fix routes through the shared ``quote_ident`` callback on
    ``SqlQueryTemplate::new`` / ``TableWriterInitMode::initialize``;
    see ``check_write_quotes_table_name_with_special_characters`` in
    ``utils.py``. The same bug affects the MySQL and MSSQL writers and
    is covered by sibling tests there (unverifiable in this harness).
    """
    check_write_quotes_table_name_with_special_characters(
        write=_pg_write, db_context=postgres, quote_ident=_pg_quote_ident
    )


def test_psql_write_column_name_is_reserved_word(postgres):
    """``pw.io.postgres.write`` interpolates user column names straight
    into the generated SQL (``field_list`` in ``SqlQueryTemplate::new``,
    the ``columns`` loop in ``TableWriterInitMode::create_table_if_not_exists``,
    the ``UPDATE SET``/``DELETE WHERE`` tokens, the ``ON CONFLICT`` list).
    A column whose name is a PostgreSQL reserved word (``user``, ``order``,
    ``group``, ``select`` ...) is valid in a Pathway schema and would be
    accepted by PostgreSQL if the writer double-quoted it — but the writer
    does not, so both ``CREATE TABLE`` at init time and every subsequent
    ``INSERT`` panic an engine worker with an opaque ``db error``.

    The shared fix: ``quote_ident`` callback in ``SqlQueryTemplate::new``
    / ``TableWriterInitMode``, plus (transparently) the Postgres-specific
    ``on_insert_conflict_condition`` — which simply interpolates
    already-quoted names. See
    ``check_write_quotes_reserved_word_column_name`` in ``utils.py``.
    """
    check_write_quotes_reserved_word_column_name(
        write=_pg_write, db_context=postgres, quote_ident=_pg_quote_ident
    )


def test_psql_write_custom_schema_name(postgres):
    """``pw.io.postgres.write`` must honor ``schema_name`` so users can
    target a non-``public`` PostgreSQL schema. Before this was added,
    there was no supported way to write outside ``public`` — an
    undocumented workaround of passing ``table_name="myschema.mytable"``
    only survived as long as the writer skipped identifier quoting,
    and broke cleanly the moment quoting was introduced.

    Setup: create a fresh schema, write through the Pathway connector
    with ``schema_name=...``, and read back through psycopg2 directly
    at ``{schema}.{table}`` to verify the rows landed in the right
    namespace.
    """
    import uuid

    schema_name = f"s_{uuid.uuid4().hex[:8]}"
    postgres.execute_sql(f'CREATE SCHEMA "{schema_name}"')
    try:
        table = pw.debug.table_from_markdown(
            """
            k | v
            1 | hello
            2 | world
            """
        )
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name="scoped_table",
            schema_name=schema_name,
            init_mode="create_if_not_exists",
        )
        run()

        # Read the rows back via the raw SQL driver to verify the
        # target namespace was correct. `get_table_contents` uses raw
        # string interpolation, so we pass a fully qualified quoted
        # identifier.
        rows = postgres.get_table_contents(
            f'"{schema_name}"."scoped_table"',
            ["k", "v"],
            sort_by="k",
        )
        assert rows == [{"k": 1, "v": "hello"}, {"k": 2, "v": "world"}]
    finally:
        postgres.execute_sql(f'DROP SCHEMA "{schema_name}" CASCADE')


def test_psql_connection_string_quotes_password_with_space(postgres):
    """``_connection_string_from_settings`` in the Python layer joins
    the ``postgres_settings`` dict into a libpq keyword/value string
    by concatenating ``k=v`` pairs with plain spaces. A value that
    contains a space (a password with a space in it, a ``dbname`` with
    a space, ...) breaks the parser because libpq interprets the
    first embedded space as the boundary between two settings.

    libpq's connection string grammar (see the official docs) supports
    single-quoted values with ``\\'`` and ``\\\\`` escapes for embedded
    single quotes and backslashes. The Python helper must wrap values
    in single quotes when they contain whitespace or a backslash, and
    escape ``\\`` and ``'`` inside the quoted value. Today the writer
    simply errors with ``invalid connection string`` from libpq — the
    account created below authenticates fine via ``psycopg2`` with the
    same password, so the problem is purely in our string formatter.

    The ``_replication_connection_string_from_settings`` sibling has a
    parallel bug (URL form — needs ``urllib.parse.quote``), but this
    test exercises the simpler keyword/value path.
    """
    import uuid

    pwd = "pa ss word"
    user_name = f"u_{uuid.uuid4().hex[:8]}"
    # Randomize the destination table name so concurrent runs of this
    # test (parallel pytest pipelines, --count=N) do not race on the
    # same DROP/CREATE.
    table_name = f"pwd_space_test_{uuid.uuid4().hex[:8]}"
    postgres.execute_sql_with_retry(
        f'CREATE USER "{user_name}" WITH PASSWORD {repr(pwd)}'
    )
    postgres.execute_sql_with_retry(f'GRANT ALL ON SCHEMA public TO "{user_name}"')
    try:
        settings = dict(POSTGRES_SETTINGS)
        settings["user"] = user_name
        settings["password"] = pwd

        table = pw.debug.table_from_markdown(
            """
            k | v
            1 | hello
            """
        )
        pw.io.postgres.write(
            table,
            postgres_settings=settings,
            table_name=table_name,
            init_mode="replace",
        )
        run()

        # If the connector properly quotes the password, the row lands.
        rows = postgres.get_table_contents(table_name, ["k", "v"])
        assert rows == [{"k": 1, "v": "hello"}]
    finally:
        # Cleanup mutates pg_authid / pg_database / pg_class — these
        # catalog tables can't be updated concurrently by two
        # transactions, and PG occasionally surfaces the conflict as
        # ``tuple concurrently updated`` even when the rows being
        # touched are disjoint. Retry on those transient errors.
        postgres.execute_sql_with_retry(f"DROP TABLE IF EXISTS {table_name}")
        postgres.execute_sql_with_retry(f'REASSIGN OWNED BY "{user_name}" TO "user"')
        postgres.execute_sql_with_retry(f'DROP OWNED BY "{user_name}" CASCADE')
        postgres.execute_sql_with_retry(f'DROP USER "{user_name}"')


@pw.udf
def _float_to_sentinel(x: float) -> str:
    """Maps float special values to round-trippable string tokens so
    the result survives ``pw.io.jsonlines.write`` — which otherwise
    serializes NaN/±Infinity as JSON ``null`` and loses the
    information the NUMERIC-special-value tests need to distinguish.
    """
    import math

    if math.isnan(x):
        return "NaN"
    if x == math.inf:
        return "Inf"
    if x == -math.inf:
        return "-Inf"
    return f"{x}"


def test_psql_read_numeric_nan_and_infinity(postgres, tmp_path):
    """``pw.io.postgres.read`` silently corrupted PostgreSQL ``NUMERIC``
    values of ``'NaN'``, ``'Infinity'``, and ``'-Infinity'`` to ``0.0``.

    The binary-format parser for ``NUMERIC`` in
    ``src/connectors/postgres.rs`` (``from_sql`` for ``Type::NUMERIC``)
    reads the header (``ndigits``, ``weight``, ``sign``, ``dscale``)
    and sums the base-10_000 digits into an ``f64``. Special values
    encode ``ndigits == 0`` with distinctive sign bytes:

    * ``0xC000`` → NaN
    * ``0xD000`` → +Infinity (PG14+)
    * ``0xF000`` → -Infinity (PG14+)

    The previous parser only tested ``sign == 0x4000`` (plain negative)
    and ignored the special values, so the initial ``value = 0.0``
    flowed through unchanged, corrupting data without any error.
    """
    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"CREATE TABLE {table_name} (k BIGINT PRIMARY KEY, v NUMERIC NOT NULL)"
    )
    postgres.execute_sql(
        f"INSERT INTO {table_name} VALUES "
        f"(1, 1.5), (2, 'NaN'), (3, 'Infinity'), (4, '-Infinity')"
    )

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: float

    output_path = tmp_path / "out.jsonl"
    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=S,
        mode="static",
    )
    stringified = table.select(pw.this.k, v_str=_float_to_sentinel(pw.this.v))
    pw.io.jsonlines.write(stringified, output_path)
    run()

    with open(output_path) as f:
        rows = sorted((json.loads(line) for line in f), key=lambda r: r["k"])
    assert rows[0]["v_str"] == "1.5"
    assert rows[1]["v_str"] == "NaN", f"NaN was corrupted to {rows[1]['v_str']!r}"
    assert rows[2]["v_str"] == "Inf", f"+Inf was corrupted to {rows[2]['v_str']!r}"
    assert rows[3]["v_str"] == "-Inf", f"-Inf was corrupted to {rows[3]['v_str']!r}"


def test_psql_streaming_read_numeric_nan_and_infinity(postgres, tmp_path):
    """Streaming-mode counterpart to
    ``test_psql_read_numeric_nan_and_infinity``. The static reader goes
    through ``postgres-native`` ``FromSql`` (binary protocol, our own
    ``NUMERIC`` binary parser in ``postgres.rs``); the streaming reader
    goes through ``pg_walstream`` which delivers pgoutput *text* values,
    and those flow into ``WalReader::parse_value_from_str`` — a totally
    different code path.

    The user's Pathway schema declares the column as ``float``, so the
    text parser ends up calling ``s.parse::<f64>()``; Rust's
    ``f64::from_str`` accepts ``"NaN"``, ``"Infinity"``, and
    ``"-Infinity"`` out of the box. This test pins that behavior —
    both for the initial snapshot read (which still runs inside the
    streaming reader once ``wal_reader`` is attached) and for WAL
    events inserted after the reader is running.

    Since ``pw.io.jsonlines.write`` serializes NaN/±Inf as JSON
    ``null`` (erasing the information we care about), we stringify via
    a UDF before writing — that form survives JSON round-tripping.
    """
    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"CREATE TABLE {table_name} (k BIGINT PRIMARY KEY, v NUMERIC NOT NULL)"
    )
    postgres.execute_sql(
        f"INSERT INTO {table_name} VALUES "
        f"(1, 1.5), (2, 'NaN'), (3, 'Infinity'), (4, '-Infinity')"
    )

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: float

    output_path = tmp_path / "out.jsonl"
    with postgres.publication(table_name) as publication_name:
        read_table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=S,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        stringified = read_table.select(pw.this.k, v_str=_float_to_sentinel(pw.this.v))
        pw.io.jsonlines.write(stringified, output_path)

        def stream_target():
            # Wait for the snapshot rows to land, then append one WAL
            # event for each special value so the text-format path for
            # pgoutput-delivered updates is also exercised.
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 4), 30, target=None
            )
            postgres.execute_sql(
                f"INSERT INTO {table_name} VALUES "
                f"(5, 'NaN'), (6, 'Infinity'), (7, '-Infinity')"
            )

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()
        wait_result_with_checker(FileLinesNumberChecker(output_path, 7), 60)

    with open(output_path) as f:
        rows = sorted((json.loads(line) for line in f), key=lambda r: r["k"])

    # Snapshot block (static-like path inside streaming reader)
    assert rows[0]["v_str"] == "1.5"
    assert rows[1]["v_str"] == "NaN"
    assert rows[2]["v_str"] == "Inf"
    assert rows[3]["v_str"] == "-Inf"
    # WAL block (pgoutput text path, different code)
    assert rows[4]["v_str"] == "NaN"
    assert rows[5]["v_str"] == "Inf"
    assert rows[6]["v_str"] == "-Inf"


def test_psql_write_str_to_uuid_column(postgres):
    """Same class as the ``pw.Duration`` → ``INTERVAL`` and
    ``pw.DateTimeNaive`` → ``DATE`` round-trip gaps: the input
    connector already maps ``UUID`` columns to ``str`` (hyphenated
    lowercase form — see the .rst type table), and the write-side
    docstring for ``pw.io.postgres.read`` even shows a first-class
    example with ``id: str = pw.column_definition(primary_key=True)``
    over a ``UUID PRIMARY KEY``. The natural counterpart — writing a
    Pathway ``str`` column back into a PostgreSQL ``UUID`` column —
    panics the engine worker with:

        worker panic: query "INSERT INTO ... VALUES ($1,$2,$3,$4)"
          failed: error serializing parameter 0

    The fix mirrors bugs #10 and #11: in ``impl ToSql for Value`` add
    a ``Type::UUID`` branch under ``Self::String(s)`` that parses
    ``s`` as a UUID and emits the 16-byte binary layout. The default
    writer-emitted type for ``str`` stays ``TEXT``; this branch only
    kicks in for pre-existing ``UUID`` columns.
    """
    ddl = (
        "CREATE TABLE {t} ("
        "  id UUID PRIMARY KEY, v TEXT,"
        "  time BIGINT NOT NULL, diff SMALLINT NOT NULL)"
    )

    class S(pw.Schema):
        id: str
        v: str

    with postgres.temporary_table(ddl) as table_name:
        table = pw.debug.table_from_rows(
            S,
            [
                ("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "first"),
                ("b1ffcd00-1d1c-5fa9-cc7e-7cc0ce491b22", "second"),
            ],
        )
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        run()

        postgres.cursor.execute(f"SELECT id::text, v FROM {table_name} ORDER BY v")
        assert postgres.cursor.fetchall() == [
            ("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "first"),
            ("b1ffcd00-1d1c-5fa9-cc7e-7cc0ce491b22", "second"),
        ]


def test_psql_write_str_to_uuid_column_invalid_value_errors_cleanly(postgres):
    """Counterpart to ``test_psql_write_str_to_uuid_column``: a malformed
    UUID string must surface as a clear Pathway-level error carrying
    the underlying ``uuid::Error`` message (invalid length / character),
    not an opaque engine-worker ``db error`` panic. This pins the
    contract that the UUID-specific branch in ``impl ToSql for Value``
    propagates parse failures through the normal
    ``PostgresError::PsqlQueryFailed`` path.
    """
    ddl = (
        "CREATE TABLE {t} ("
        "  id UUID PRIMARY KEY, v TEXT,"
        "  time BIGINT NOT NULL, diff SMALLINT NOT NULL)"
    )

    class S(pw.Schema):
        id: str
        v: str

    with postgres.temporary_table(ddl) as table_name:
        table = pw.debug.table_from_rows(
            S,
            [("not-a-valid-uuid", "first")],
        )
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        with pytest.raises(
            Exception,
            match=r"invalid (length|character)|error serializing parameter",
        ):
            run()


def test_psql_write_datetime_naive_to_date_column(postgres):
    """Sibling of ``test_psql_write_duration_to_interval_column``:
    ``pw.io.postgres.read`` maps a PostgreSQL ``DATE`` column to
    ``pw.DateTimeNaive`` (midnight of the given date, per the .rst
    type table). Writing a ``pw.DateTimeNaive`` value back into a
    ``DATE`` column today panics the engine worker at first flush
    with:

        worker panic: query "INSERT INTO ... VALUES ($1,$2,$3,$4)"
          failed: error serializing parameter 1

    The advertised round-trip claim — *"All types can be round-tripped
    back via the read connector when the original schema type is
    specified"* — is broken for the ``DATE`` / ``DateTimeNaive`` pair.

    Fix direction: extend ``impl ToSql for Value`` so
    ``Value::DateTimeNaive`` serializes into PostgreSQL's ``DATE``
    binary layout (``i32`` days since 2000-01-01) when the target
    type is ``Type::DATE``. Non-midnight time components are
    truncated silently — this matches PostgreSQL's own implicit
    ``TIMESTAMP``-to-``DATE`` cast.

    The default writer-emitted type for ``DateTimeNaive`` stays
    ``TIMESTAMP`` (see ``postgres_data_type``); this branch only
    kicks in for pre-existing ``DATE`` columns.
    """
    ddl = (
        "CREATE TABLE {t} ("
        "  k BIGINT PRIMARY KEY, "
        "  d DATE, "
        "  time BIGINT NOT NULL, "
        "  diff SMALLINT NOT NULL"
        ")"
    )

    class S(pw.Schema):
        k: int
        d: pw.DateTimeNaive

    with postgres.temporary_table(ddl) as table_name:
        table = pw.debug.table_from_rows(
            S,
            [
                (1, pw.DateTimeNaive(year=2026, month=4, day=23)),
                (2, pw.DateTimeNaive(year=1970, month=1, day=1)),
            ],
        )
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        run()

        rows = postgres.get_table_contents(table_name, ["k", "d"], sort_by="k")
        assert rows == [
            {"k": 1, "d": datetime.date(2026, 4, 23)},
            {"k": 2, "d": datetime.date(1970, 1, 1)},
        ]


def test_psql_write_jagged_array_surfaces_root_cause(postgres):
    """``PsqlQueryFailed`` used to display only ``postgres::Error``'s
    top-level message, which for a parameter-serialization failure
    is always the opaque ``"error serializing parameter N"``. The
    actual root cause (our ``ArraySerializationError::JaggedArray``
    / ``DimensionsOverflow`` / ``UnknownOid`` / ...) was only
    reachable by walking the ``source()`` chain.

    The fix expands the error chain in ``format_error_chain`` so
    the offending cause is visible to the user at pipeline-end,
    not just the generic "error serializing parameter N" wrapper.
    This test pins the happy path: write a jagged
    ``list[list[int]]`` and assert that the root-cause string
    ("jagged") surfaces in the engine-level error.
    """

    class S(pw.Schema):
        k: int
        l: list[list[int]]

    t = pw.debug.table_from_rows(S, [(1, [[1, 2], [3]])])
    with postgres.temporary_table() as table_name:
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="create_if_not_exists",
        )
        with pytest.raises(Exception, match=r"jagged"):
            run()


@pytest.mark.parametrize(
    "pg_col_type, input_values, expected_db_values",
    [
        pytest.param(
            "TIME",
            [
                pd.Timedelta(hours=12, minutes=30),
                pd.Timedelta(hours=23, minutes=59, seconds=59),
            ],
            [datetime.time(12, 30), datetime.time(23, 59, 59)],
            id="time",
        ),
        pytest.param(
            "INTERVAL",
            [pd.Timedelta(seconds=90), pd.Timedelta(days=1, seconds=45)],
            [
                pd.Timedelta(seconds=90).to_pytimedelta(),
                pd.Timedelta(days=1, seconds=45).to_pytimedelta(),
            ],
            id="interval",
        ),
    ],
)
def test_psql_write_duration_to_non_default_column_type(
    postgres, pg_col_type, input_values, expected_db_values
):
    """``pw.io.postgres.read`` maps both PostgreSQL ``TIME`` and
    ``INTERVAL`` columns to ``pw.Duration`` (per ``360.postgres.rst``),
    so writing a ``pw.Duration`` back into either column type must
    round-trip. rust-postgres' ``i64 ToSql`` does not accept either of
    these types on its own, so the writer carries a dedicated binary
    serializer for each — ``i64`` microseconds-since-midnight for
    ``TIME``, ``(i64 usecs, i32 days, i32 months)`` for ``INTERVAL``
    (with days/months zeroed so the inverse read is exact)."""
    ddl = (
        "CREATE TABLE {t} ("
        "  k BIGINT PRIMARY KEY, "
        f"  d {pg_col_type} NOT NULL, "
        "  time BIGINT NOT NULL, "
        "  diff SMALLINT NOT NULL"
        ")"
    )

    class S(pw.Schema):
        k: int
        d: pw.Duration

    with postgres.temporary_table(ddl) as table_name:
        rows = list(zip(range(1, len(input_values) + 1), input_values))
        table = pw.debug.table_from_rows(S, rows)
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        run()

        db_rows = postgres.get_table_contents(table_name, ["k", "d"], sort_by="k")
        assert db_rows == [
            {"k": i + 1, "d": v} for i, v in enumerate(expected_db_values)
        ]


def test_psql_write_str_to_enum_column(postgres):
    """Sibling of ``test_psql_read_enum_column_as_str``. The binary
    ENUM format on the wire is just the raw UTF-8 label bytes;
    rust-postgres' ``&str`` ``ToSql`` rejects ``Kind::Enum``, so
    without the ENUM branch in ``impl ToSql for Value`` a write of
    a Pathway ``str`` column into an ENUM column panicked the
    worker at first flush with an opaque ``error serializing
    parameter N``.
    """
    enum_name = f"mood_{uuid.uuid4().hex[:8]}"
    table_name = postgres.random_table_name()
    postgres.execute_sql(f"CREATE TYPE {enum_name} AS ENUM ('happy', 'sad')")
    postgres.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  k BIGINT PRIMARY KEY, "
        f"  m {enum_name}, "
        f"  time BIGINT NOT NULL, "
        f"  diff SMALLINT NOT NULL"
        ")"
    )

    class S(pw.Schema):
        k: int
        m: str

    try:
        t = pw.debug.table_from_rows(S, [(1, "happy"), (2, "sad")])
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        run()

        rows = postgres.get_table_contents(table_name, ["k", "m"], sort_by="k")
        assert rows == [{"k": 1, "m": "happy"}, {"k": 2, "m": "sad"}]
    finally:
        postgres.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        postgres.execute_sql(f"DROP TYPE IF EXISTS {enum_name}")


def test_psql_read_macaddr8_column(postgres, tmp_path):
    """``MACADDR8`` (EUI-64, 8 bytes) columns used to fall through
    rust-postgres' built-in FromSql's ``MACADDR`` check and land in
    the unsupported-type error path, silently dropping the row.
    After the fix the reader decodes it to an 8-octet
    ``"xx:xx:xx:xx:xx:xx:xx:xx"`` string (documented in
    ``360.postgres.rst``).
    """
    ddl = "CREATE TABLE {t} (k BIGINT PRIMARY KEY, m MACADDR8)"

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        m: str

    output_path = tmp_path / "out.jsonl"
    with postgres.temporary_table(ddl) as table_name:
        postgres.execute_sql(
            f"INSERT INTO {table_name} VALUES (1, '08:00:2b:01:02:03:04:05')"
        )
        t = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=S,
            mode="static",
        )
        pw.io.jsonlines.write(t, output_path)
        run()

        with open(output_path) as f:
            row = json.loads(f.readline())
        assert row["k"] == 1
        assert row["m"] == "08:00:2b:01:02:03:04:05"


def test_psql_read_enum_column_as_str(postgres, tmp_path):
    """Reading a user-defined ``ENUM`` column used to silently drop
    every row: rust-postgres' built-in ``String::accepts`` ignores
    ``Kind::Enum`` so the reader fell through to the unsupported-type
    error path, which bubbles up as a per-row ``ConversionError``
    that jsonlines-style sinks discard without a visible signal.

    After the fix, ENUM columns decode to Pathway ``str`` values
    holding the raw label — matching how
    ``360.postgres.rst`` documents the mapping.
    """
    enum_name = f"mood_{uuid.uuid4().hex[:8]}"
    table_name = postgres.random_table_name()
    postgres.execute_sql(f"CREATE TYPE {enum_name} AS ENUM ('happy', 'sad')")
    postgres.execute_sql(
        f"CREATE TABLE {table_name} (k BIGINT PRIMARY KEY, m {enum_name})"
    )
    postgres.execute_sql(f"INSERT INTO {table_name} VALUES (1, 'happy'), (2, 'sad')")

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        m: str

    output_path = tmp_path / "out.jsonl"
    try:
        t = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=S,
            mode="static",
        )
        pw.io.jsonlines.write(t, output_path)
        run()

        with open(output_path) as f:
            rows = sorted((json.loads(line) for line in f), key=lambda r: r["k"])
        assert [(r["k"], r["m"]) for r in rows] == [(1, "happy"), (2, "sad")]
    finally:
        postgres.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        postgres.execute_sql(f"DROP TYPE IF EXISTS {enum_name}")


@pytest.mark.parametrize(
    "domain_def, sample_value, pw_type, expected",
    [
        pytest.param(
            "TEXT CHECK (VALUE LIKE 'EM-%')",
            "'EM-12345'",
            str,
            "EM-12345",
            id="domain-over-text",
        ),
        pytest.param(
            "INTEGER CHECK (VALUE > 0)",
            "42",
            int,
            42,
            id="domain-over-int",
        ),
        pytest.param(
            "DOUBLE PRECISION CHECK (VALUE >= 0.0)",
            "3.14",
            float,
            pytest.approx(3.14),
            id="domain-over-float",
        ),
    ],
)
def test_psql_read_domain_type_unwraps_to_base(
    postgres, tmp_path, domain_def, sample_value, pw_type, expected
):
    """User-defined ``DOMAIN`` types wrap a base type with a CHECK
    constraint (or other modifier). PostgreSQL sends the underlying
    base type's bytes on the wire, so reading a ``DOMAIN`` column must
    behave identically to reading the base type.

    Historically the connector's ``FromSql for Value`` only inspected
    the outer ``Type``; rust-postgres' built-in ``accepts`` impls reject
    ``Kind::Domain`` because they pattern-match the *exact* ``Type``,
    not the kind. The result was a silent per-row drop with only a
    logged warning (the same class of bug the unsupported-PG-type
    preflight catches for built-in types like MONEY). This test pins
    the round-trip through three common base types."""
    domain_name = f"emp_id_{uuid.uuid4().hex[:8]}"
    postgres.execute_sql(f"CREATE DOMAIN {domain_name} AS {domain_def}")
    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"CREATE TABLE {table_name} (k BIGINT PRIMARY KEY, v {domain_name})"
    )
    postgres.execute_sql(f"INSERT INTO {table_name} VALUES (1, {sample_value})")

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: pw_type  # type: ignore[valid-type]

    output_path = tmp_path / "out.jsonl"
    try:
        t = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=S,
            mode="static",
        )
        pw.io.jsonlines.write(t, output_path)
        run()

        with open(output_path) as f:
            rows = [json.loads(line) for line in f]
        assert len(rows) == 1, f"DOMAIN over {domain_def} silently dropped the row"
        assert rows[0]["v"] == expected
    finally:
        postgres.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        postgres.execute_sql(f"DROP DOMAIN IF EXISTS {domain_name}")


def test_psql_write_to_domain_column(postgres):
    """Companion to ``test_psql_read_domain_type_unwraps_to_base``: a
    user-defined ``DOMAIN`` over a base type that the writer normally
    serializes (e.g. ``INTEGER``) must accept Pathway values of the
    matching base Pathway type. PostgreSQL stores the DOMAIN value
    using the base type's wire format, so as long as the postgres
    client properly forwards the base-type info to ``ToSql`` the round-
    trip works."""
    domain_name = f"emp_id_{uuid.uuid4().hex[:8]}"
    postgres.execute_sql(f"CREATE DOMAIN {domain_name} AS INTEGER CHECK (VALUE > 0)")
    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"CREATE TABLE {table_name} ("
        f"  k BIGINT PRIMARY KEY,"
        f"  v {domain_name},"
        f"  time BIGINT NOT NULL,"
        f"  diff SMALLINT NOT NULL)"
    )

    class S(pw.Schema):
        k: int
        v: int

    try:
        t = pw.debug.table_from_rows(S, [(1, 42), (2, 100)])
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        run()

        rows = postgres.get_table_contents(table_name, ["k", "v"], sort_by="k")
        assert rows == [{"k": 1, "v": 42}, {"k": 2, "v": 100}]
    finally:
        postgres.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        postgres.execute_sql(f"DROP DOMAIN IF EXISTS {domain_name}")


def test_psql_read_rejects_mismatched_schema_type(postgres):
    """``pw.io.postgres.read.validate_table_schema`` used to only check
    column *names*, never types. A ``pw.Schema`` that declares
    ``v: int`` for a PostgreSQL ``TEXT`` column would silently deliver
    ``Value::String`` rows to downstream operators — surfacing only
    later, if some operator actually enforced the declared type.
    Information-destructive sinks like ``pw.io.jsonlines.write`` hid
    the mismatch entirely.

    The fix extends ``validate_table_schema`` to query
    ``information_schema.columns.udt_name`` alongside ``column_name``
    and compare each column's actual PG udt against the declared
    Pathway type (via the mapping table described in
    ``360.postgres.rst``). Container types (ARRAY, DOMAIN, ENUM,
    pgvector) remain permissive — their runtime parsers already
    validate.
    """

    # Lie about the column type: actual is TEXT, declared is int.
    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: int

    import os
    import tempfile

    out = tempfile.mktemp(suffix=".jsonl")
    try:
        with postgres.temporary_table(
            "CREATE TABLE {t} (k BIGINT PRIMARY KEY, v TEXT)"
        ) as table_name:
            postgres.execute_sql(f"INSERT INTO {table_name} VALUES (1, 'oops')")
            table = pw.io.postgres.read(
                postgres_settings=POSTGRES_SETTINGS,
                table_name=table_name,
                schema=S,
                mode="static",
            )
            pw.io.jsonlines.write(table, out)
            with pytest.raises(
                RuntimeError,
                match=(
                    rf"Column public\.{table_name}\.v has PostgreSQL type 'text' "
                    r"which is not compatible with the declared Pathway type"
                ),
            ):
                run()
    finally:
        if os.path.exists(out):
            os.remove(out)


def test_psql_read_accepts_compatible_numeric_types(postgres):
    """Complement of ``test_psql_read_rejects_mismatched_schema_type``:
    the new validator must still accept the documented type
    compatibilities — a PG ``NUMERIC`` / ``REAL`` / ``INT2`` / ``INT4``
    column declared as ``float`` / ``float`` / ``int`` / ``int``. If
    the map in ``is_pathway_type_compatible_with_pg_udt`` is too tight
    every existing downstream test breaks; this test pins the
    intentional compatibilities.
    """
    ddl = (
        "CREATE TABLE {t} ("
        " k BIGINT PRIMARY KEY,"
        " small_i SMALLINT NOT NULL,"
        " reg_i INT NOT NULL,"
        " real_f REAL NOT NULL,"
        " num_f NUMERIC(10, 4) NOT NULL)"
    )

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        small_i: int
        reg_i: int
        real_f: float
        num_f: float

    import os
    import tempfile

    out = tempfile.mktemp(suffix=".jsonl")
    try:
        with postgres.temporary_table(ddl) as table_name:
            postgres.execute_sql(
                f"INSERT INTO {table_name} VALUES (1, 7, 42, 1.5, 3.14)"
            )
            table = pw.io.postgres.read(
                postgres_settings=POSTGRES_SETTINGS,
                table_name=table_name,
                schema=S,
                mode="static",
            )
            pw.io.jsonlines.write(table, out)
            run()

            with open(out) as f:
                row = json.loads(f.readline())
            assert row["small_i"] == 7
            assert row["reg_i"] == 42
            assert row["real_f"] == pytest.approx(1.5)
            assert row["num_f"] == pytest.approx(3.14)
    finally:
        if os.path.exists(out):
            os.remove(out)


def test_psql_write_snapshot_composite_pk_reverse_order_deletes(postgres):
    """Regression for a snapshot-mode retraction bug where the DELETE
    WHERE clause was emitted in user-specified ``primary_key`` order
    but its bound parameters were reordered into sorted-schema-position
    order by ``SqlQueryTemplate::primary_key_fields``. When the two
    orderings disagreed (composite PK specified in reverse schema
    order), the WHERE clause compared each column against the wrong
    value and the DELETE matched zero rows — silently leaving stale
    rows in the destination.

    Setup: schema order is ``(a, b)``, ``primary_key=[t.b, t.a]`` —
    reverse of schema order. Insert one row, retract it in a later
    minibatch; the destination must end up empty.
    """
    from pathway.internals import api

    class Subject(pw.io.python.ConnectorSubject):
        @property
        def _deletions_enabled(self):
            return True

        def run(self):
            row = b'{"a": "A", "b": "B"}'
            self._add(api.ref_scalar(1), row)
            self.commit()
            self._remove(api.ref_scalar(1), row)

    class S(pw.Schema):
        a: str
        b: str

    with postgres.temporary_table() as table_name:
        t = pw.io.python.read(Subject(), schema=S, autocommit_duration_ms=1)
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[t.b, t.a],  # reverse of schema order (a, b)
        )
        run()

        rows = postgres.get_table_contents(table_name, ["a", "b"])
        assert rows == []


def test_psql_write_snapshot_rejects_primary_key_from_other_table(postgres):
    """``primary_key=[other_table.col]`` (or any ColumnReference whose
    ``.name`` isn't a column of the table being written) used to
    slip through the Python layer and emit a malformed CREATE TABLE
    (``PRIMARY KEY ("unknown")``), triggering an opaque engine-worker
    panic. The preflight names the unknown column at call time now.
    """

    class S(pw.Schema):
        k: int
        v: str

    class T(pw.Schema):
        xyz: int

    written = pw.debug.table_from_rows(S, [(1, "a")])
    other = pw.debug.table_from_rows(T, [(99,)])

    with pytest.raises(
        ValueError,
        match=(
            r"primary_key references column\(s\) \['xyz'\] that are not "
            r"present in the written table"
        ),
    ):
        pw.io.postgres.write(
            written,
            postgres_settings=POSTGRES_SETTINGS,
            table_name="pk_cross_table",
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[other.xyz],
        )


def test_psql_write_snapshot_rejects_nullable_primary_key(postgres):
    """``pw.io.postgres.write`` with ``output_table_type="snapshot"`` and
    a ``primary_key`` column whose dtype is nullable (``int | None``)
    silently generates the snapshot UPSERT, then the engine worker
    panics at first flush with a ``db error``. Postgres PKs must be
    ``NOT NULL``; our own ``postgres_data_type`` also emits ``NOT NULL``
    on the PK column in ``create_if_not_exists``, so ``NULL`` rows fail
    the constraint at insert time.

    Even for a pre-existing table this is silent data-loss territory:
    ``DELETE FROM t WHERE pkey=$1`` never matches anything when the
    bound value is NULL (SQL ``= NULL`` is always false), so retracted
    rows are never cleaned up.

    SQLite rejects this at call time with a ``ValueError`` (see the
    pinned ``test_sqlite_snapshot_rejects_nullable_primary_key``). The
    Postgres path should match: at the Python layer, inspect the dtype
    of each ``primary_key`` column and raise ``ValueError`` if any of
    them is nullable, pointing the user at the fix ("make the column
    non-nullable in the schema or filter out nulls upstream").
    """

    @pw.udf
    def maybe_none(x: int) -> int | None:
        return None if x == 2 else x

    class S(pw.Schema):
        x: int
        v: str

    t = pw.debug.table_from_rows(S, [(1, "a"), (2, "b"), (3, "c")])
    t = t.select(pkey=maybe_none(pw.this.x), v=pw.this.v)

    with pytest.raises(
        ValueError,
        match=(
            r"primary_key column 'pkey' is declared nullable.*"
            r"must be non-nullable in snapshot mode"
        ),
    ):
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name="null_pk_rejected",
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[t.pkey],
        )


@pytest.mark.parametrize("reserved", ["time", "diff", "Time", "DIFF"])
def test_psql_write_stream_mode_rejects_reserved_column_name(postgres, reserved):
    """Stream-of-changes mode unconditionally appends ``time BIGINT``
    and ``diff SMALLINT`` metadata columns to the generated
    ``CREATE TABLE``. If the user's ``pw.Schema`` already names one of
    them (case-insensitively — PostgreSQL folds unquoted identifiers
    to lowercase), the emitted CREATE TABLE has a duplicate column and
    the engine worker panics at pipeline start with an opaque
    ``db error``.

    Match SQLite's behavior (the SQLite writer catches this at call
    time with a clear ``ValueError``; see
    ``test_sqlite_write_stream_mode_reserved_column_name_rejected``).
    The check belongs at the Python layer in ``pw.io.postgres.write``:
    scan ``table`` column names for a case-insensitive match against
    ``{"time", "diff"}`` and raise a ``ValueError`` with the
    offending name(s) before any graph is built.

    Snapshot mode does not append these metadata columns, so this
    restriction applies only to ``output_table_type="stream_of_changes"``.
    """

    table = pw.debug.table_from_markdown(
        f"""
        k | {reserved}
        1 | 100
        2 | 200
        """
    )
    with pytest.raises(
        ValueError,
        match=(
            r"collide with the 'time' and 'diff' metadata columns "
            r"appended in stream_of_changes mode"
        ),
    ):
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name="reserved_time_diff",
            init_mode="create_if_not_exists",
        )


@pytest.mark.parametrize("reserved", ["time", "diff", "Time", "DIFF"])
def test_psql_write_snapshot_legacy_rejects_reserved_column_name(postgres, reserved):
    """``pw.io.postgres.write_snapshot`` (the deprecated legacy entry
    point that's still in the public API surface) runs the Rust writer
    in ``legacy_mode=True``, which — like the non-deprecated
    stream-of-changes mode — appends ``time``/``diff`` metadata
    columns to the generated INSERT. If the user's ``pw.Schema``
    already declares a ``time`` or ``diff`` column (case-insensitive,
    because PostgreSQL folds unquoted identifiers to lowercase), the
    emitted SQL becomes ``INSERT INTO foo (\"time\", \"value\",
    \"time\", \"diff\") VALUES (...)`` and PostgreSQL rejects it with
    ``column \"time\" specified more than once`` at flush time — a
    panic that doesn't surface until pipeline runtime.

    The non-deprecated sibling
    ``pw.io.postgres.write(..., output_table_type=\"stream_of_changes\")``
    catches this at construction time (see
    ``test_psql_write_stream_mode_rejects_reserved_column_name``).
    Mirror that here so the deprecated path also fails fast with a
    clear ``ValueError``.
    """
    table = pw.debug.table_from_markdown(
        f"""
        pkey | {reserved}
        1    | 100
        2    | 200
        """
    )
    with pytest.raises(
        ValueError,
        match=(
            r"collide with the 'time' and 'diff' metadata columns "
            r"appended by `pw\.io\.postgres\.write_snapshot`"
        ),
    ):
        pw.io.postgres.write_snapshot(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name="reserved_time_diff_legacy",
            primary_key=["pkey"],
            init_mode="create_if_not_exists",
        )


def test_psql_write_snapshot_allows_reserved_column_name(postgres):
    """Complement to
    ``test_psql_write_stream_mode_rejects_reserved_column_name``:
    snapshot mode does *not* append the ``time`` / ``diff`` metadata
    columns, so a user column named ``time`` or ``diff`` must still
    work there. Pins the expectation so a too-broad reject doesn't
    regress the snapshot path.
    """
    table = pw.debug.table_from_markdown(
        """
        pkey | time
        1    | 100
        2    | 200
        """
    )
    with postgres.temporary_table() as table_name:
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.pkey],
        )
        run()

        rows = postgres.get_table_contents(
            table_name, ["pkey", '"time"'], sort_by="pkey"
        )
        assert rows == [
            {"pkey": 1, '"time"': 100},
            {"pkey": 2, '"time"': 200},
        ]


def test_psql_write_snapshot_rejects_duplicate_primary_key_columns(postgres):
    """``pw.io.postgres.write`` with ``primary_key=[t.k, t.k]`` silently
    accepts the duplicated column reference today. With
    ``init_mode="create_if_not_exists"`` the subsequent SQL emission
    fails at pipeline start with an opaque engine-worker
    ``db error`` (PostgreSQL rejects ``CREATE TABLE ... PRIMARY KEY
    (\"k\", \"k\")`` / the shared ``SqlQueryTemplate`` builds a
    malformed UPSERT). The user never learns what was wrong.

    SQLite has the same bug (see the pinned failing test
    ``test_sqlite_snapshot_rejects_duplicate_primary_key_columns`` in
    ``python/pathway/tests/test_sqlite.py``). The fix is best placed
    at the Python layer (``pw.io.postgres.write`` /
    ``pw.io.sqlite.write``) because both connectors already normalize
    ``primary_key`` into ``key_field_names`` there — a few lines that
    detect duplicates and raise a clear ``ValueError`` at call time,
    before any graph is built:

        primary_key contains duplicate column(s) ['k']

    This matches the SQLite test's expected error message verbatim so
    future refactors can share a single helper.
    """

    class Schema(pw.Schema):
        k: int
        v: str

    table = pw.debug.table_from_rows(Schema, [(1, "a")])
    with pytest.raises(
        ValueError,
        match=r"primary_key contains duplicate column\(s\) \['k'\]",
    ):
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name="dup_pk_rejected",
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.k, table.k],
        )


def test_psql_write_snapshot_default_init_rejects_missing_pk_constraint(postgres):
    """``pw.io.postgres.write`` in ``output_table_type="snapshot"`` mode
    generates ``INSERT ... ON CONFLICT ({primary_key}) DO UPDATE``,
    which PostgreSQL only accepts when there is a ``UNIQUE`` or
    ``PRIMARY KEY`` constraint whose column set matches the
    ``ON CONFLICT`` list exactly. If the user ran with
    ``init_mode="default"`` against a pre-existing table that lacks
    such a constraint, today's writer emits the UPSERT and the engine
    worker panics at flush time with:

        worker panic: query "INSERT INTO ... ON CONFLICT (\\"k\\") ..." failed: db error

    Match the SQLite behavior (``test_sqlite_snapshot_write_default_init
    _rejects_missing_pk_constraint``) by pre-flighting inside
    ``PsqlWriter::new``: query
    ``information_schema.table_constraints`` + ``key_column_usage`` for
    any ``PRIMARY KEY`` or ``UNIQUE`` constraint whose column set
    matches ``primary_key`` exactly. If none matches, raise a clear
    Pathway-level error pointing the user at either adding a proper
    constraint or switching to
    ``init_mode="replace"`` / ``"create_if_not_exists"``.
    """

    class S(pw.Schema):
        k: int
        v: str

    with postgres.temporary_table("CREATE TABLE {t} (k BIGINT, v TEXT)") as table_name:
        t = pw.debug.table_from_rows(S, [(1, "a"), (2, "b")])
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
            output_table_type="snapshot",
            primary_key=[t.k],
        )
        with pytest.raises(
            Exception,
            match=(
                r"do not correspond to any UNIQUE or PRIMARY KEY constraint"
                r"|no UNIQUE or PRIMARY KEY constraint"
            ),
        ):
            run()


def test_psql_read_rejects_non_existent_table(postgres, tmp_path):
    """Reading from a non-existent table (schema exists, table
    doesn't) used to report the misleading "Fields not found in
    table ..." — implying the table has columns just not the ones
    declared. The preflight now distinguishes the two cases and
    reports that the table itself is missing."""

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = tmp_path / "out.jsonl"
    t = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name="nonexistent_table_xyz_9999",
        schema=S,
        mode="static",
    )
    pw.io.jsonlines.write(t, output_path)
    with pytest.raises(
        RuntimeError,
        match=(
            r"Table public\.nonexistent_table_xyz_9999 does not exist in "
            r"the PostgreSQL database"
        ),
    ):
        run()


def test_psql_read_rejects_non_existent_schema(postgres, tmp_path):
    """Reading from a non-existent ``schema_name`` used to surface a
    misleading "Fields not found in table ..." error — the columns
    query returned an empty set regardless of whether the table or
    the schema is the missing piece. The preflight now checks
    ``pg_namespace`` first so the error names the right culprit."""

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = tmp_path / "out.jsonl"
    t = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name="any_table",
        schema=S,
        mode="static",
        schema_name="nonexistent_schema_xyz_9999",
    )
    pw.io.jsonlines.write(t, output_path)
    with pytest.raises(
        RuntimeError,
        match=(
            r"Schema 'nonexistent_schema_xyz_9999' does not exist in "
            r"the PostgreSQL database"
        ),
    ):
        run()


def test_psql_write_rejects_non_existent_schema(postgres):
    """Writing to a non-existent ``schema_name`` used to panic the
    engine worker with an opaque ``db error`` from libpq at init
    time. The preflight now detects the missing schema via
    ``pg_namespace`` and surfaces a clear Pathway-level error."""

    class S(pw.Schema):
        k: int
        v: str

    t = pw.debug.table_from_rows(S, [(1, "a")])
    pw.io.postgres.write(
        t,
        postgres_settings=POSTGRES_SETTINGS,
        table_name="any_table",
        schema_name="nonexistent_schema_xyz_9999",
        init_mode="create_if_not_exists",
    )
    with pytest.raises(
        Exception,
        match=(
            r"schema \"nonexistent_schema_xyz_9999\" does not exist in "
            r"the PostgreSQL database"
        ),
    ):
        run()


@pytest.mark.parametrize(
    "setup_sql,kind_label",
    [
        pytest.param(
            [
                "CREATE TABLE _base_{name} (k BIGINT)",
                "CREATE VIEW {name} AS SELECT * FROM _base_{name}",
            ],
            "view",
            id="view",
        ),
        pytest.param(
            ["CREATE MATERIALIZED VIEW {name} AS SELECT 1 AS k"],
            "materialized view",
            id="materialized_view",
        ),
        pytest.param(
            ["CREATE SEQUENCE {name}"],
            "sequence",
            id="sequence",
        ),
    ],
)
def test_psql_write_rejects_non_table_destination(postgres, setup_sql, kind_label):
    """``pw.io.postgres.write`` must reject destinations whose name
    already exists as something other than a regular table — view,
    materialized view, sequence, etc. Before this preflight the init
    path (``CREATE TABLE [IF NOT EXISTS]`` or the follow-up ``INSERT``
    in ``init_mode="default"``) panicked the engine worker with an
    opaque driver error. The preflight identifies the actual object
    kind via ``pg_class.relkind`` and names it in the error.
    """
    name = f"nontab_{uuid.uuid4().hex[:8]}"
    for stmt in setup_sql:
        postgres.execute_sql(stmt.format(name=name))
    try:

        class S(pw.Schema):
            k: int
            v: str

        t = pw.debug.table_from_rows(S, [(1, "a")])
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=name,
            init_mode="replace",
        )
        with pytest.raises(
            Exception,
            match=(
                rf"destination \"public\"\.\"{name}\" is a {kind_label}, "
                r"not a regular table"
            ),
        ):
            run()
    finally:
        # DROP statements error on "wrong object type" even with IF EXISTS,
        # so try each possible kind independently and swallow failures.
        for drop_stmt in [
            f"DROP VIEW IF EXISTS {name}",
            f"DROP MATERIALIZED VIEW IF EXISTS {name}",
            f"DROP SEQUENCE IF EXISTS {name}",
            f"DROP TABLE IF EXISTS _base_{name} CASCADE",
            f"DROP TABLE IF EXISTS {name}",
        ]:
            try:
                postgres.execute_sql(drop_stmt)
            except Exception:
                pass


@pytest.mark.parametrize(
    "ddl_fmt, init_mode, error_pattern_fmt",
    [
        pytest.param(
            "CREATE TABLE {t} (k BIGINT, v TEXT)",
            "default",
            (
                r"pw\.Schema references column\(s\) \[.*time.*diff.*\]|"
                r"pw\.Schema references column\(s\) \[.*diff.*time.*\]"
            ),
            id="missing-time-diff",
        ),
        pytest.param(
            "CREATE TABLE {t} ("
            " k TEXT PRIMARY KEY, v TEXT,"
            " time BIGINT NOT NULL, diff SMALLINT NOT NULL)",
            "default",
            r"destination column \"public\"\.\"{t}\"\.\"k\" has PostgreSQL type 'text'",
            id="mismatched-column-type-default",
        ),
        pytest.param(
            "CREATE TABLE {t} ("
            " k TEXT PRIMARY KEY, v TEXT,"
            " time BIGINT NOT NULL, diff SMALLINT NOT NULL)",
            "create_if_not_exists",
            r"destination column \"public\"\.\"{t}\"\.\"k\" has PostgreSQL type 'text'",
            id="mismatched-column-type-cine",
        ),
        pytest.param(
            "CREATE TABLE {t} ("
            " k BIGINT, v TEXT,"
            " time SMALLINT NOT NULL, diff SMALLINT NOT NULL)",
            "default",
            (
                r"destination table \"public\"\.\"{t}\" has "
                r"metadata column \"time\" declared as 'int2'"
            ),
            id="narrow-time-column",
        ),
        pytest.param(
            "CREATE TABLE {t} (k BIGINT, time BIGINT NOT NULL, diff SMALLINT NOT NULL)",
            "default",
            (
                r"pw\.Schema references column\(s\) \[.*v.*\] that do not "
                r"exist in destination table \"public\"\.\"{t}\""
            ),
            id="missing-destination-column",
        ),
        pytest.param(
            "CREATE TABLE {t} ("
            " k BIGINT PRIMARY KEY, v TEXT,"
            " audit_user TEXT NOT NULL,"
            " time BIGINT NOT NULL, diff SMALLINT NOT NULL)",
            "default",
            (
                r"destination table \"public\"\.\"{t}\" has "
                r"NOT NULL column\(s\) \[.*audit_user.*\] without a DEFAULT"
            ),
            id="extra-not-null-column",
        ),
    ],
)
def test_psql_write_preflight_rejects_incompatible_destination(
    postgres, ddl_fmt, init_mode, error_pattern_fmt
):
    """Preflight must reject ill-shaped destination tables at pipeline
    start with a specific, actionable error instead of panicking the
    engine worker mid-flush with opaque ``db error`` messages.

    Each parameter pins one class of mismatch:
    * ``missing-time-diff`` — stream-of-changes requires the time/diff
      metadata columns the writer appends to the INSERT;
    * ``mismatched-column-type-*`` — a declared ``int`` column against
      a PostgreSQL ``TEXT`` column, under both ``default`` and
      ``create_if_not_exists`` init modes (the CINE no-ops the CREATE
      for an existing table and falls through to the user's INSERT);
    * ``narrow-time-column`` — Pathway timestamps are 64-bit
      milliseconds since epoch and overflow a SMALLINT / INTEGER
      ``time`` column in production;
    * ``missing-destination-column`` — the user schema names a column
      that does not exist in the destination;
    * ``extra-not-null-column`` — the destination carries a NOT NULL
      column without a DEFAULT that the INSERT does not supply."""

    class S(pw.Schema):
        k: int
        v: str

    with postgres.temporary_table(ddl_fmt) as table_name:
        t = pw.debug.table_from_rows(S, [(1, "a")])
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode=init_mode,
        )
        with pytest.raises(Exception, match=error_pattern_fmt.format(t=table_name)):
            run()


def test_psql_write_default_init_accepts_extra_nullable_or_defaulted_column(postgres):
    """Complement to
    ``test_psql_write_default_init_rejects_extra_not_null_column``:
    the preflight must still allow extra columns that are nullable,
    have a DEFAULT, or are auto-filled identity / generated columns.
    """
    ddl = (
        "CREATE TABLE {t} ("
        "  k BIGINT PRIMARY KEY,"
        "  v TEXT,"
        "  nullable_extra TEXT,"  # nullable
        "  defaulted_extra TEXT NOT NULL DEFAULT 'd',"  # has default
        "  auto_id BIGINT GENERATED BY DEFAULT AS IDENTITY,"  # identity by default
        "  time BIGINT NOT NULL,"
        "  diff SMALLINT NOT NULL)"
    )

    class S(pw.Schema):
        k: int
        v: str

    with postgres.temporary_table(ddl) as table_name:
        t = pw.debug.table_from_rows(S, [(1, "a"), (2, "b")])
        pw.io.postgres.write(
            t,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        run()

        rows = postgres.get_table_contents(table_name, ["k", "v"], sort_by="k")
        assert rows == [{"k": 1, "v": "a"}, {"k": 2, "v": "b"}]


def test_psql_write_default_init_rejects_generated_column_in_schema(postgres):
    """``pw.io.postgres.write`` with ``init_mode="default"`` must pre-flight
    the destination table against the Pathway schema and reject a column
    that PostgreSQL declares as ``GENERATED ALWAYS AS (...) STORED`` —
    inserting a value into such a column is a hard error in PostgreSQL
    (it requires the ``DEFAULT`` keyword or omitting the column) and
    today the writer simply emits the normal ``INSERT`` and panics an
    engine worker mid-flush with a raw driver message:

        worker panic: query "INSERT INTO \\"public\\".\\"gen\\"
        (\\"k\\",\\"v\\",\\"v_upper\\",time,diff) VALUES ($1,...,$5)" failed: db error

    The SQLite writer catches the identical class of bug at pipeline
    start (``test_sqlite_write_rejects_generated_column_in_schema``) —
    the Postgres writer should behave the same way. Detect the set of
    generated columns via ``information_schema.columns`` (the
    ``is_generated`` column, values ``'NEVER'`` / ``'ALWAYS'``), and if
    any generated column name is in ``value_fields`` emit a clear
    Pathway-level error at construction time:

        destination table "public"."gen" has generated column(s) ["v_upper"]
        (`GENERATED ALWAYS AS ...`) that pw.Schema also names;
        drop them from the schema — the reader will still return their
        computed values on SELECT.
    """
    ddl = (
        "CREATE TABLE {t} ("
        "  k BIGINT PRIMARY KEY, "
        "  v TEXT NOT NULL, "
        "  v_upper TEXT GENERATED ALWAYS AS (UPPER(v)) STORED, "
        "  time BIGINT NOT NULL, "
        "  diff SMALLINT NOT NULL"
        ")"
    )
    with postgres.temporary_table(ddl) as table_name:
        table = pw.debug.table_from_markdown(
            """
            k | v     | v_upper
            1 | hello | HELLO
            """
        )
        pw.io.postgres.write(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="default",
        )
        with pytest.raises(
            Exception,
            match=(
                rf"destination table.*{table_name}.*" r"generated column\(s\).*v_upper"
            ),
        ):
            run()


def test_psql_streaming_read_rejects_publication_not_covering_table(postgres, tmp_path):
    """When the user passes a ``publication_name`` that *exists* but
    does NOT cover the target table, today's reader silently proceeds
    past ``PsqlReader::new``, streams the initial snapshot, and then
    hangs forever waiting for WAL events that never come — the
    publication simply doesn't forward anything for this table. No
    error, no warning; the streaming pipeline is effectively dead but
    the user has no signal.

    The existing check only verifies that ``pg_publication`` contains a
    row with ``pubname = $1``; it does not check coverage. A correct
    preflight queries ``pg_publication_tables`` for the ``(pubname,
    schemaname, tablename)`` triple — that view expands ``FOR ALL
    TABLES`` publications correctly as well — and errors out at
    pipeline start with an actionable message:

        "Publication 'foo' exists but does not cover {schema}.{table};
         add the table with ALTER PUBLICATION foo ADD TABLE ..."

    The test below pins the *buggy* behavior first: it proves the
    reader silently starts the pipeline and fails to stream a later
    INSERT.
    """
    from pathlib import Path

    target_table = postgres.random_table_name()
    other_table = postgres.random_table_name()
    publication_name = f"pub_excluding_{target_table}"
    postgres.execute_sql(f"CREATE TABLE {target_table} (k BIGINT PRIMARY KEY, v TEXT)")
    postgres.execute_sql(f"CREATE TABLE {other_table} (k BIGINT PRIMARY KEY, v TEXT)")
    # Publication deliberately covers only `other_table`, not the target.
    postgres.execute_sql(
        f"CREATE PUBLICATION {publication_name} FOR TABLE {other_table}"
    )
    postgres.execute_sql(f"INSERT INTO {target_table} VALUES (1, 'initial')")

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = Path(tmp_path) / "out.jsonl"
    try:
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=target_table,
            schema=S,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)
        with pytest.raises(
            RuntimeError,
            match=(
                rf"Publication '{publication_name}' does not cover "
                rf"public\.{target_table}"
            ),
        ):
            run()
    finally:
        postgres.execute_sql(f"DROP PUBLICATION IF EXISTS {publication_name}")
        postgres.execute_sql(f"DROP TABLE IF EXISTS {target_table}")
        postgres.execute_sql(f"DROP TABLE IF EXISTS {other_table}")


def test_psql_streaming_read_rejects_publication_missing_event_types(
    postgres, tmp_path
):
    """A PostgreSQL publication created with ``WITH (publish = 'insert')``
    (or any subset that omits ``update`` / ``delete`` / ``truncate``)
    forwards only the listed event types. When the Pathway reader
    treats the source as *non* append-only — which is the default —
    it needs INSERT, UPDATE, DELETE, and TRUNCATE in order to
    faithfully mirror the table. If any of those is filtered out at
    the publication level, the user gets silent data drift: a DELETE
    in PostgreSQL leaves a phantom row in Pathway, an UPDATE never
    propagates, etc. No error, no warning.

    The existence+coverage checks already in ``PsqlReader::new``
    (``pg_publication`` / ``pg_publication_tables``) don't consult
    the publish flags. Today's reader happily starts up; this test
    proves the misconfiguration is silent by inserting a row, deleting
    it, and asserting that today's reader never receives the deletion.

    Proposed fix: extend the preflight to consult the ``pubinsert`` /
    ``pubupdate`` / ``pubdelete`` / ``pubtruncate`` columns on
    ``pg_publication``. When ``is_append_only=True`` the reader only
    requires ``pubinsert``; when it's ``False``, all four must be
    enabled. On a mismatch, raise a clear error naming the missing
    event types and the remediation
    (``ALTER PUBLICATION ... SET (publish = 'insert, update, delete, truncate')``).
    """
    from pathlib import Path

    target_table = postgres.random_table_name()
    publication_name = f"pub_insert_only_{uuid.uuid4().hex[:8]}"
    postgres.execute_sql(f"CREATE TABLE {target_table} (k BIGINT PRIMARY KEY, v TEXT)")
    postgres.execute_sql(
        f"CREATE PUBLICATION {publication_name} FOR TABLE {target_table} "
        f"WITH (publish = 'insert')"
    )

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = Path(tmp_path) / "out.jsonl"
    try:
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=target_table,
            schema=S,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)
        with pytest.raises(
            RuntimeError,
            match=(
                rf"Publication '{publication_name}' does not forward .*"
                rf"update.*delete.*truncate"
            ),
        ):
            run()
    finally:
        postgres.execute_sql(f"DROP PUBLICATION IF EXISTS {publication_name}")
        postgres.execute_sql(f"DROP TABLE IF EXISTS {target_table}")


def test_psql_streaming_read_append_only_allows_insert_only_publication(
    postgres, tmp_path
):
    """Complement to
    ``test_psql_streaming_read_rejects_publication_missing_event_types``:
    for an append-only reader the publication only needs to forward
    ``INSERT`` events. ``UPDATE``/``DELETE``/``TRUNCATE`` would violate
    the append-only contract anyway — if they ever happen and the
    publication does forward them, they surface through the existing
    ``AppendOnlyNotRespected`` check — so a publication with
    ``publish = 'insert'`` must be accepted at construction time.
    """
    from pathlib import Path

    target_table = postgres.random_table_name()
    publication_name = f"pub_ao_insert_{uuid.uuid4().hex[:8]}"
    postgres.execute_sql(f"CREATE TABLE {target_table} (k BIGINT PRIMARY KEY, v TEXT)")
    postgres.execute_sql(
        f"CREATE PUBLICATION {publication_name} FOR TABLE {target_table} "
        f"WITH (publish = 'insert')"
    )
    postgres.execute_sql(f"INSERT INTO {target_table} VALUES (1, 'initial')")

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = Path(tmp_path) / "out.jsonl"
    try:
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=target_table,
            schema=S,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
            is_append_only=True,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 1), 30, target=None
            )
            postgres.execute_sql(f"INSERT INTO {target_table} VALUES (2, 'second')")

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()
        wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 60)

        with open(output_path) as f:
            rows = [json.loads(line) for line in f]
        assert sorted(r["k"] for r in rows) == [1, 2]
    finally:
        postgres.execute_sql(f"DROP PUBLICATION IF EXISTS {publication_name}")
        postgres.execute_sql(f"DROP TABLE IF EXISTS {target_table}")


def test_psql_streaming_read_password_with_url_unsafe_chars(postgres, tmp_path):
    """Sibling of ``test_psql_connection_string_quotes_password_with_space``
    for the streaming reader's URL-form connection string.

    ``_replication_connection_string_from_settings`` embeds the password
    directly into a ``postgresql://user:password@host:port/dbname``
    URL. A password containing any URL-reserved character (``@``,
    ``:``, ``/``, ``?``, ``#``, whitespace, non-ASCII, ...) silently
    corrupts the parsed URL — libpq either fails to connect or connects
    to the wrong authority. The fix uses ``urllib.parse.quote`` on each
    user-controlled component.

    This test creates a real user with an ``@``/space password, issues
    a streaming read, inserts one row, and asserts it arrives — so the
    URL form is exercised end-to-end against a real WAL stream.
    """
    import uuid

    # ``@`` and space both need percent-encoding; together they cover
    # both the URL-authority and the path-query parsers.
    pwd = "p@ss wrd"
    user_name = f"u_{uuid.uuid4().hex[:8]}"
    postgres.execute_sql_with_retry(
        f'CREATE USER "{user_name}" WITH REPLICATION PASSWORD {repr(pwd)}'
    )
    postgres.execute_sql_with_retry(f'GRANT ALL ON SCHEMA public TO "{user_name}"')
    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"CREATE TABLE {table_name} (id BIGSERIAL PRIMARY KEY, value TEXT NOT NULL)"
    )
    postgres.execute_sql(f'ALTER TABLE {table_name} OWNER TO "{user_name}"')
    try:
        settings = dict(POSTGRES_SETTINGS)
        settings["user"] = user_name
        settings["password"] = pwd

        output_path = tmp_path / "out.jsonl"

        class S(pw.Schema):
            id: int = pw.column_definition(primary_key=True)
            value: str

        with postgres.publication(table_name) as publication_name:
            read_table = pw.io.postgres.read(
                postgres_settings=settings,
                table_name=table_name,
                schema=S,
                mode="streaming",
                publication_name=publication_name,
                autocommit_duration_ms=10,
            )
            pw.io.jsonlines.write(read_table, output_path)

            def stream_target():
                postgres.execute_sql(
                    f"INSERT INTO {table_name} (value) VALUES ('hello')"
                )

            stream_thread = threading.Thread(target=stream_target, daemon=True)
            stream_thread.start()

            wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 30)

        with open(output_path) as f:
            rows = [json.loads(line) for line in f]
        assert len(rows) == 1
        assert rows[0]["value"] == "hello"
    finally:
        # See ``execute_sql_with_retry`` — these statements modify
        # ``pg_authid`` and friends which can collide with parallel
        # role cleanups in other tests.
        postgres.execute_sql_with_retry(f"DROP TABLE IF EXISTS {table_name}")
        postgres.execute_sql_with_retry(f'REASSIGN OWNED BY "{user_name}" TO "user"')
        postgres.execute_sql_with_retry(f'DROP OWNED BY "{user_name}" CASCADE')
        postgres.execute_sql_with_retry(f'DROP USER "{user_name}"')


def test_psql_read_custom_schema_name(postgres, tmp_path):
    """Counterpart to ``test_psql_write_custom_schema_name``:
    ``pw.io.postgres.read`` must honor ``schema_name`` for both the
    initial snapshot SELECT and the WAL-level event filtering.

    Setup: create a fresh schema with a table, insert a row directly,
    then read it back through the Pathway connector with
    ``schema_name=...``. Run in ``static`` mode so the test doesn't
    depend on WAL streaming.
    """
    import uuid

    schema_name = f"s_{uuid.uuid4().hex[:8]}"
    table_name = "scoped_read"
    postgres.execute_sql(f'CREATE SCHEMA "{schema_name}"')
    try:
        postgres.execute_sql(
            f'CREATE TABLE "{schema_name}"."{table_name}" '
            f"(k BIGINT PRIMARY KEY, v TEXT NOT NULL)"
        )
        postgres.execute_sql(
            f'INSERT INTO "{schema_name}"."{table_name}" VALUES '
            f"(1, 'hello'), (2, 'world')"
        )

        class InputSchema(pw.Schema):
            k: int = pw.column_definition(primary_key=True)
            v: str

        output_path = tmp_path / "out.jsonl"
        read_table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="static",
            schema_name=schema_name,
        )
        pw.io.jsonlines.write(read_table, output_path)
        run()

        with open(output_path) as f:
            rows = sorted((json.loads(line) for line in f), key=lambda r: r["k"])
        assert [(r["k"], r["v"]) for r in rows] == [(1, "hello"), (2, "world")]
    finally:
        postgres.execute_sql(f'DROP SCHEMA "{schema_name}" CASCADE')


@pytest.mark.parametrize(
    "pg_type,declared_pw_type",
    [
        ("MONEY", "float"),
        ("BIT(4)", "str"),
        ("BIT VARYING(8)", "str"),
        ("TSVECTOR", "str"),
        ("TSQUERY", "str"),
        ("XML", "str"),
        ("POINT", "str"),
        ("INT4RANGE", "str"),
        ("JSONPATH", "str"),
        ("PG_LSN", "str"),
        ("XID", "int"),
    ],
)
def test_psql_read_rejects_unsupported_pg_type(
    postgres, tmp_path, pg_type, declared_pw_type
):
    """The reader's ``FromSql for Value`` has no branch for these PG
    types: ``MONEY``, ``BIT``, ``VARBIT``, ``TSVECTOR``, ``TSQUERY``,
    ``XML``, geometric, range, ``JSONPATH``, ``PG_LSN``, ``XID``, and
    the object-identifier aliases. Historically, declaring any Pathway
    type against one of those columns let the preflight pass (the
    compatibility predicate treated the ``udt_name`` as custom/array),
    and the per-row parse then silently dropped the row with only a
    logged warning. That turns a type mismatch into data drift.

    Preflight now rejects the column explicitly so the user hears
    about the problem at pipeline start rather than reading an empty
    output hours later."""
    pytype_map = {"int": int, "float": float, "str": str}
    pytype = pytype_map[declared_pw_type]

    class _Schema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: pytype  # type: ignore[valid-type]

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v {pg_type}
        );
        """
    )
    try:
        output_path = tmp_path / f"out_{pg_type}.jsonl"
        t = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=_Schema,
            mode="static",
        )
        pw.io.jsonlines.write(t, output_path)
        expected_udt_fragments = {
            "MONEY": "money",
            "BIT(4)": "bit",
            "BIT VARYING(8)": "varbit",
            "TSVECTOR": "tsvector",
            "TSQUERY": "tsquery",
            "XML": "xml",
            "POINT": "point",
            "INT4RANGE": "int4range",
            "JSONPATH": "jsonpath",
            "PG_LSN": "pg_lsn",
            "XID": "xid",
        }[pg_type]
        with pytest.raises(RuntimeError) as exc_info:
            run()
        msg = str(exc_info.value).lower()
        assert (
            expected_udt_fragments in msg
        ), f"expected '{expected_udt_fragments}' in error, got: {msg}"
    finally:
        postgres.execute_sql(f"DROP TABLE {table_name}")


@pytest.mark.parametrize(
    "pg_type,sample",
    [
        ("INET", "192.168.1.1/24"),
        ("CIDR", "192.168.0.0/16"),
        ("MACADDR", "08:00:2b:01:02:03"),
        ("MACADDR8", "08:00:2b:01:02:03:04:05"),
    ],
)
def test_psql_write_str_to_inet_cidr_macaddr_column(postgres, pg_type, sample):
    """The reader maps ``INET`` / ``CIDR`` / ``MACADDR`` / ``MACADDR8``
    columns to ``str`` (see ``360.postgres.rst``), and the writer
    mirrors that round-trip: a ``pw.Schema`` declaring ``str`` against
    a pre-existing column of one of these types must produce the
    stored value that the reader would hand back. The regression is
    silent data drift — either the column value in the DB ends up
    matching ``sample`` or the write is rejected cleanly before any
    row is written."""

    class _Schema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v {pg_type} NOT NULL,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );
        """
    )

    t = pw.debug.table_from_markdown(
        f"""
        k | v
        1 | {sample}
        """,
        schema=_Schema,
    )
    try:
        pw.io.postgres.write(t, POSTGRES_SETTINGS, table_name)
        pw.run()
    finally:
        G.clear()

    postgres.cursor.execute(f"SELECT k, v::text FROM {table_name} ORDER BY k")
    rows = postgres.cursor.fetchall()
    assert len(rows) == 1, f"expected 1 row for {pg_type}, got {rows!r}"
    assert rows[0][0] == 1
    assert (
        sample.lower().replace("/32", "") in str(rows[0][1]).lower()
    ), f"{pg_type}: wrote {sample!r}, DB now holds {rows[0][1]!r}"


@pytest.mark.parametrize(
    "pg_type,pw_type,pg_name_fragment",
    [
        ("OID", int, "oid"),
        ("TIMETZ", "duration", "timetz"),
    ],
)
def test_psql_write_preflight_rejects_read_only_asymmetric_types(
    postgres, pg_type, pw_type, pg_name_fragment
):
    """The reader has dedicated branches for ``OID`` (widened to i64)
    and ``TIMETZ`` (microseconds since UTC midnight). Neither has a
    matching writer serializer: rust-postgres routes ``OID`` only
    through ``u32`` (not in our ``try_forward`` chain), and
    ``Value::Duration`` doesn't carry the session-offset parameter
    needed for a meaningful TIMETZ write.

    Before the split between read/write compat, both passed
    preflight and failed at flush with an opaque driver panic.
    The direction-aware predicate now rejects them upfront."""
    pw_type_resolved = pw.Duration if pw_type == "duration" else pw_type

    class _Schema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: pw_type_resolved  # type: ignore[valid-type]

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v {pg_type} NOT NULL,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );
        """
    )
    if pw_type is int:
        t = pw.debug.table_from_markdown(
            "k | v\n1 | 42",
            schema=_Schema,
        )
    else:
        t = pw.debug.table_from_pandas(
            pd.DataFrame({"k": [1], "v": [pd.Timedelta("1 second")]}),
            schema=_Schema,
        )
    try:
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            pw.io.postgres.write(t, POSTGRES_SETTINGS, table_name)
            run()
        assert pg_name_fragment in str(exc_info.value).lower()
    finally:
        G.clear()


def test_psql_streaming_read_rejects_replica_identity_nothing(postgres, tmp_path):
    """A non-append-only streaming reader needs UPDATE / DELETE events
    to carry the primary-key columns of the affected rows so Pathway
    can emit the corresponding retractions. PostgreSQL's replica
    identity controls what pgoutput forwards: ``DEFAULT`` (with a
    PK), ``USING INDEX``, or ``FULL`` all include the key; ``NOTHING``
    does not. A table with ``REPLICA IDENTITY NOTHING`` silently
    drops update/delete events from the WAL, leaving the Pathway
    snapshot permanently out of sync with the database.

    Without a preflight check this is invisible — the pipeline just
    stops producing diffs. Surface it at startup."""

    class _Schema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v TEXT NOT NULL
        );
        ALTER TABLE {table_name} REPLICA IDENTITY NOTHING;
        """
    )
    try:
        with postgres.publication(table_name) as publication_name:
            output_path = tmp_path / "out.jsonl"
            table = pw.io.postgres.read(
                postgres_settings=POSTGRES_SETTINGS,
                table_name=table_name,
                schema=_Schema,
                mode="streaming",
                publication_name=publication_name,
                autocommit_duration_ms=10,
            )
            pw.io.jsonlines.write(table, output_path)
            with pytest.raises(RuntimeError) as exc_info:
                run()
            msg = str(exc_info.value).lower()
            assert (
                "replica identity" in msg or "replica_identity" in msg
            ), f"error should mention replica identity, got: {msg}"
    finally:
        postgres.execute_sql(f"DROP TABLE {table_name}")


def test_psql_write_list_element_type_mismatch(postgres):
    """Writing a ``list[str]`` into a pre-existing ``BIGINT[]`` column
    passes preflight (the ``_int8`` UDT falls through the permissive
    "custom or array" branch), then fails element-by-element at flush
    with an opaque worker panic. A cleaner outcome — preflight
    rejection or a clear per-row error surfaced via the same
    ``format_error_chain`` pretty-printer — is preferable."""

    class _Schema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: list[str]

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v BIGINT[] NOT NULL,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );
        """
    )
    t = pw.debug.table_from_pandas(
        pd.DataFrame({"k": [1], "v": [["foo", "bar"]]}),
        schema=_Schema,
    )
    try:
        # The current behavior is a worker panic at flush — verify
        # the error message names the offending column type so the
        # user can diagnose it, rather than seeing a bare ``error
        # serializing parameter``.
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            pw.io.postgres.write(t, POSTGRES_SETTINGS, table_name)
            run()
        msg = str(exc_info.value).lower()
        assert (
            "int8" in msg
            or "bigint" in msg
            or "array" in msg
            or "element" in msg
            or "list" in msg
        ), f"error must point at the element-type mismatch, got: {msg}"
    finally:
        G.clear()


def test_psql_write_optional_schema_column_into_not_null_destination(postgres):
    """A schema that declares a column ``Optional[T]`` will emit
    ``Value::None`` for rows where that column is null. If the
    destination column is NOT NULL, the ``INSERT`` fails at flush
    with a constraint violation that panics the worker. The user
    only hears about it when a null row first arrives, which can be
    much later than pipeline start.

    This test first verifies the bug reproduces for ``init_mode="default"``
    (pre-existing NOT NULL column vs. nullable schema field). The fix
    surfaces at preflight time."""

    class _Schema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: int | None

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v BIGINT NOT NULL,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );
        """
    )
    t = pw.debug.table_from_pandas(
        pd.DataFrame({"k": [1], "v": [None]}),
        schema=_Schema,
    )
    try:
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            pw.io.postgres.write(t, POSTGRES_SETTINGS, table_name)
            run()
        msg = str(exc_info.value).lower()
        assert (
            "not null" in msg or "optional" in msg or "nullable" in msg
        ), f"preflight should mention nullability mismatch, got: {msg}"
    finally:
        G.clear()


def test_psql_write_float_to_numeric_column_rejected_at_preflight(postgres):
    """Historically ``is_pathway_type_compatible_with_pg_udt`` allowed
    ``Type::Float`` against a ``NUMERIC`` column — that's correct on
    the *reader* side (the reader has a dedicated NUMERIC branch that
    decodes the digit array into f64), but wrong on the *writer*
    side: ``<f64 as ToSql>::accepts(NUMERIC)`` is false, so the
    INSERT panicked the worker at flush time with an opaque
    ``error serializing parameter``. A per-direction predicate now
    rejects the NUMERIC target at preflight with a clear message."""

    class _Schema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: float

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v NUMERIC(10, 3) NOT NULL,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );
        """
    )
    t = pw.debug.table_from_markdown(
        """
        k | v
        1 | 1.5
        """,
        schema=_Schema,
    )
    try:
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            pw.io.postgres.write(t, POSTGRES_SETTINGS, table_name)
            run()
        msg = str(exc_info.value).lower()
        assert "numeric" in msg
    finally:
        G.clear()


@pytest.mark.parametrize(
    "pg_type,sample",
    [
        ("INET", "192.168.1.1"),
        ("INET", "2001:db8::1/64"),
        ("CIDR", "10.0.0.0/8"),
        ("CIDR", "2001:db8::/32"),
        ("MACADDR", "aa:bb:cc:dd:ee:ff"),
        ("MACADDR8", "aa:bb:cc:dd:ee:ff:00:11"),
    ],
)
def test_psql_roundtrip_inet_cidr_macaddr(tmp_path, postgres, pg_type, sample):
    """End-to-end round-trip: write via ``pw.io.postgres.write`` into
    a pre-existing ``INET`` / ``CIDR`` / ``MACADDR`` / ``MACADDR8``
    column and read it back via ``pw.io.postgres.read``. The two sides
    must agree on the string representation so staged pipelines (write
    in job A, read in job B) do not drift."""

    class _WriteSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    class _ReadSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            v {pg_type} NOT NULL,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );
        """
    )

    t = pw.debug.table_from_markdown(
        f"""
        k | v
        1 | {sample}
        """,
        schema=_WriteSchema,
    )
    try:
        pw.io.postgres.write(t, POSTGRES_SETTINGS, table_name)
        pw.run()
    finally:
        G.clear()

    # Drop the metadata columns so the read side sees a plain table.
    read_view = postgres.random_table_name()
    postgres.execute_sql(f"CREATE VIEW {read_view} AS SELECT k, v FROM {table_name};")
    try:
        output_path = tmp_path / "rt.jsonl"
        out = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=read_view,
            schema=_ReadSchema,
            mode="static",
        )
        pw.io.jsonlines.write(out, output_path)
        run()

        with open(output_path) as f:
            rows = [json.loads(line) for line in f]
        assert len(rows) == 1
        read_back = rows[0]["v"]
        # The reader normalizes INET without prefix (when prefix ==
        # max) to the plain host form; compare loosely on the
        # numeric content.
        expected = sample.lower().replace("/32", "").replace("/128", "")
        actual = read_back.lower().replace("/32", "").replace("/128", "")
        assert (
            actual == expected
        ), f"{pg_type} round-trip: wrote {sample!r}, read back {read_back!r}"
    finally:
        postgres.execute_sql(f"DROP VIEW {read_view}")


# Streaming WAL test: the forked ``pw.run`` reader exports the snapshot row,
# then must pick up a row inserted afterwards through ``pgoutput``. Under heavy
# xdist parallelism (``xdist_group("postgres")`` is not honored by the
# ``worksteal`` scheduler, so every postgres test hammers the one server at
# once) the reader very occasionally terminates right after the snapshot, before
# the streamed INSERT's WAL is delivered — the second row never lands and the
# 60 s checker times out. It reproduces about once per ~30 full-suite runs and
# for whichever parametrization happens to lose the race, confirming a transient
# timing/replication-slot contention rather than a parse bug. A rerun starts a
# fresh reader with a fresh slot and passes, matching how the other streaming
# timing-sensitive suites here (mongodb_parsing, questdb, debezium, mssql CDC)
# already guard themselves. FIXME: the reader cleanly exiting mid-stream (exit
# code 0, no error) is worth a deeper look in pg_walstream.
@pytest.mark.flaky(reruns=3)
@pytest.mark.parametrize(
    "pg_interval,expected_td",
    [
        # Inserts that land in the initial snapshot use the binary
        # ``FromSql`` path, so the regression only showed up for
        # ``pg_walstream``'s text output in the streaming loop. Each
        # case below exercises a code branch of
        # ``WalReader::parse_postgres_interval``. The expected value
        # matches the PostgreSQL-computed microseconds in the stored
        # INTERVAL (not a naive reading of the text!) — the regression
        # was that a leading ``-`` on the hours field of a HH:MM:SS
        # component was not propagated to the minutes / seconds /
        # fractional fields, so ``-01:30:00`` was read as -30 minutes
        # instead of -1.5 hours.
        ("-1 second", datetime.timedelta(seconds=-1)),
        ("-1:30:00", datetime.timedelta(hours=-1, minutes=-30)),
        ("-0:00:30", datetime.timedelta(seconds=-30)),
        ("-0:01:30", datetime.timedelta(minutes=-1, seconds=-30)),
        ("-0:00:00.5", datetime.timedelta(seconds=-0.5)),
        ("1 day -1:30:00", datetime.timedelta(days=1, hours=-1, minutes=-30)),
        ("-1 day 1:30:00", datetime.timedelta(days=-1, hours=1, minutes=30)),
        ("00:00:01", datetime.timedelta(seconds=1)),
        (
            "1:30:45.123456",
            datetime.timedelta(hours=1, minutes=30, seconds=45, microseconds=123456),
        ),
    ],
)
def test_psql_streaming_interval_from_wal_text_output(
    tmp_path, postgres, pg_interval, expected_td
):
    """PostgreSQL's pgoutput forwards INTERVAL values as text, so the
    streaming path routes through ``parse_postgres_interval`` in Rust.
    The existing snapshot-based parametrized test does NOT exercise
    this path because it inserts the row *before* the replication slot
    is created — that row ends up in the exported snapshot and is
    parsed via binary ``FromSql``. This test inserts *after* the
    reader has caught up, forcing the value through ``pgoutput``."""

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        duration: pw.Duration

    output_path = tmp_path / "output.jsonl"
    table_name = postgres.random_table_name()
    postgres.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            k BIGINT PRIMARY KEY,
            duration INTERVAL NOT NULL
        );
        INSERT INTO {table_name} (k, duration) VALUES (1, INTERVAL '0 seconds');
        """
    )

    with postgres.publication(table_name) as publication_name:
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=publication_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            # Wait for the warm-up snapshot row to land, then insert
            # through the WAL.
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 1), 30, target=None
            )
            postgres.execute_sql(
                f"INSERT INTO {table_name} (k, duration) VALUES "
                f"(2, INTERVAL '{pg_interval}');"
            )

        t = threading.Thread(target=stream_target, daemon=True)
        t.start()
        wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 60)

    with open(output_path) as f:
        rows = [json.loads(line) for line in f]
    result = next(r for r in rows if r["k"] == 2)
    expected_ns = int(expected_td.total_seconds() * 1_000_000_000)
    assert result["duration"] == expected_ns, (
        f"INTERVAL '{pg_interval}' from WAL: expected {expected_ns} ns, "
        f"got {result['duration']}"
    )


@pytest.mark.parametrize(
    "table_name_suffix",
    [
        pytest.param("MixedCase", id="mixed-case"),
        pytest.param("has-hyphen", id="hyphen"),
        pytest.param("Has-Both", id="mixed-case-and-hyphen"),
    ],
)
def test_postgres_streaming_read_handles_table_name_requiring_quoting(
    tmp_path, postgres, table_name_suffix
):
    """``pw.io.postgres.read`` in streaming mode builds a temporary
    replication slot whose name is derived from ``schema_name`` and
    ``table_name`` (``WalReader::generate_replication_slot_name`` in
    ``src/connectors/postgres.rs``). PostgreSQL rejects any slot name
    that contains characters outside ``[a-z0-9_]`` — the libpq error
    is ``replication slot name "..." contains invalid character`` with
    the hint ``Replication slot names may only contain lower case
    letters, numbers, and the underscore character``.

    A table name that requires SQL quoting (mixed case, hyphens, ...)
    is accepted on every other code path: ``read``'s
    ``select_prepared_records`` wraps the identifier in
    ``quote_identifier`` and the writer does the same via the shared
    ``quote_ident`` callback. But the slot-name builder used to splice
    the raw ``schema_name``/``table_name`` straight into the slot name,
    so a perfectly legal quoted table name crashed slot creation at
    pipeline start with that opaque server-side error.

    The fix sanitizes the slot-name base to ``[a-z0-9_]`` (lowercase
    ASCII letters survive verbatim, everything else becomes ``_``);
    the trailing 8-char UUID tag still guarantees uniqueness even when
    the sanitized base of two different tables collide.
    """

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    table_name = f"{table_name_suffix}_{uuid.uuid4().hex[:8]}"
    pub_name = f"pub_{uuid.uuid4().hex[:8]}"
    output_path = tmp_path / "out.jsonl"

    postgres.execute_sql(
        f'CREATE TABLE "{table_name}" (id BIGINT PRIMARY KEY, value TEXT NOT NULL);'
    )
    postgres.execute_sql(f'CREATE PUBLICATION {pub_name} FOR TABLE "{table_name}";')
    try:
        postgres.execute_sql(
            f"INSERT INTO \"{table_name}\" (id, value) VALUES (1, 'hello');"
        )
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="streaming",
            publication_name=pub_name,
            autocommit_duration_ms=10,
        )
        pw.io.jsonlines.write(table, output_path)

        def stream_target():
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 1), 30, target=None
            )
            postgres.execute_sql(
                f"INSERT INTO \"{table_name}\" (id, value) VALUES (2, 'world');"
            )

        stream_thread = threading.Thread(target=stream_target, daemon=True)
        stream_thread.start()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 60)
    finally:
        try:
            postgres.execute_sql(f"DROP PUBLICATION IF EXISTS {pub_name};")
        except Exception:
            pass
        try:
            postgres.execute_sql(f'DROP TABLE IF EXISTS "{table_name}";')
        except Exception:
            pass


@pytest.mark.parametrize(
    "input_settings, expected_url",
    [
        # Full settings — every component lands in its natural URL
        # position. Preserves the format the connector used before
        # missing components became optional.
        pytest.param(
            {"user": "u", "password": "p", "host": "h", "port": 5432, "dbname": "d"},
            "postgresql://u:p@h:5432/d?replication=database",
            id="full",
        ),
        # No auth keys — libpq applies OS-user + ``.pgpass`` defaults.
        # The minimal-host-only form lets the user connect to a TCP
        # Postgres whose pg_hba.conf is configured for ``trust`` /
        # ``cert`` / ``ident`` auth.
        pytest.param(
            {"host": "h", "dbname": "d"},
            "postgresql://h/d?replication=database",
            id="no-auth-host-and-dbname",
        ),
        # UNIX-socket / peer-auth scenario: no host, no auth keys.
        pytest.param(
            {"dbname": "d"},
            "postgresql:///d?replication=database",
            id="peer-auth-dbname-only",
        ),
        # Empty settings dict — libpq applies all defaults. The URL
        # still has to carry ``replication=database`` (it's how the
        # WAL stream is requested).
        pytest.param(
            {},
            "postgresql:///?replication=database",
            id="all-libpq-defaults",
        ),
        # User without password is valid (``trust`` / ``peer`` / cert
        # auth modes don't send a password).
        pytest.param(
            {"user": "u", "host": "h"},
            "postgresql://u@h?replication=database",
            id="user-without-password",
        ),
        # Password without user — there's no URL userinfo form that
        # encodes a password alone, so the builder hoists it into the
        # query string (libpq treats it the same).
        pytest.param(
            {"password": "p", "host": "h"},
            "postgresql://h?password=p&replication=database",
            id="password-without-user-via-query",
        ),
        # Port without host — same hoisting trick as above; the URL
        # netloc cannot have ``:port`` without a host before it.
        pytest.param(
            {"port": 5432, "dbname": "d"},
            "postgresql:///d?port=5432&replication=database",
            id="port-without-host-via-query",
        ),
        # URL-unsafe characters in password / user / host / dbname
        # must be percent-encoded — already covered end-to-end by
        # ``test_psql_streaming_read_password_with_url_unsafe_chars``;
        # here we just verify the encoding happens in isolation.
        pytest.param(
            {"user": "u@x", "password": "p ss", "host": "h", "dbname": "d/b"},
            "postgresql://u%40x:p%20ss@h/d%2Fb?replication=database",
            id="url-unsafe-characters-encoded",
        ),
    ],
)
def test_psql_replication_connection_string_url_builder(input_settings, expected_url):
    """``_replication_connection_string_from_settings`` accepts any
    subset of ``postgres_settings`` and builds a URL whose missing
    components are filled in by libpq using the same defaults the
    static-mode (keyword/value) path benefits from. Server-side
    ``pg_hba.conf`` decides whether the resulting connection is
    authorized — ``trust`` / ``peer`` / ``cert`` auth modes send no
    password, so the connector must not reject ``postgres_settings``
    that omits one. This is the symmetric counterpart of
    ``_connection_string_from_settings``, which has always forwarded
    a sparse dict to libpq verbatim.

    Pure URL-builder check — no database connection is attempted, so
    we can exhaustively cover the matrix of (un)specified components
    without standing up a separate ``trust``-auth Postgres.
    """
    from pathway.io.postgres import _replication_connection_string_from_settings

    assert _replication_connection_string_from_settings(input_settings) == expected_url


def test_psql_augment_postgres_settings_injects_pathway_defaults():
    """``_augment_postgres_settings`` is the central hook that adds
    Pathway-managed defaults to ``postgres_settings`` — currently
    ``application_name`` (so every Pathway connection is identifiable
    in ``pg_stat_activity``) and the TCP-keepalive trio plus
    ``tcp_user_timeout`` (so a SIGKILL'd Pathway is detected within
    minutes rather than the OS-inherited ~2 hour default).

    The hard constraint is "never override a user-specified value":
    a user who sets ``keepalives_idle=60`` because they're on a flaky
    network must see exactly that value reach libpq. The helper uses
    :py:meth:`dict.setdefault` to enforce this.
    """
    from pathway.io.postgres import _augment_postgres_settings

    # Empty input → every default applied. Names follow libpq's
    # canonical spelling (``keepalives_count``, not the
    # ``keepalives_retries`` rename used by the KV parser) and
    # ``tcp_user_timeout`` is in libpq's milliseconds unit. The two
    # internal connection paths translate this dialect themselves at
    # the serialization boundary so the user sees a single dialect.
    augmented = _augment_postgres_settings({}, unique_name="my-reader")
    assert augmented == {
        "application_name": "pathway:my-reader",
        "keepalives": "1",
        "keepalives_idle": "300",
        "keepalives_interval": "30",
        "keepalives_count": "3",
        "tcp_user_timeout": "300000",
    }

    # `unique_name=None` → application_name has no suffix.
    augmented = _augment_postgres_settings({}, unique_name=None)
    assert augmented["application_name"] == "pathway"

    # User-supplied values for every key Pathway would default — every
    # one of them survives untouched. (This is the regression we care
    # most about: silently overriding a user's keepalive tuning would
    # be a real footgun for ops teams.)
    user_settings = {
        "host": "db.example.com",
        "user": "u",
        "password": "p",
        "application_name": "my-custom-app-name",
        "keepalives": "0",
        "keepalives_idle": "60",
        "keepalives_interval": "5",
        "keepalives_count": "10",
        "tcp_user_timeout": "120000",
    }
    augmented = _augment_postgres_settings(user_settings, unique_name="ignored")
    assert augmented == user_settings
    # The helper must produce a copy — mutating the result must not
    # leak back into the user's dict.
    augmented["host"] = "mutated"
    assert user_settings["host"] == "db.example.com"

    # `unique_name` with non-printable / unicode characters is
    # sanitized so PostgreSQL doesn't silently `?`-mangle it on the
    # server side.
    augmented = _augment_postgres_settings({}, unique_name="prod/cdc 🚀")
    # Each non-printable-ASCII codepoint becomes one `_`; the printable
    # ones (including `/` and the space) pass through, and `🚀` is a
    # single Python codepoint replaced with one `_`.
    assert augmented["application_name"] == "pathway:prod/cdc _"

    # `unique_name` long enough to push the total past 63 chars is
    # truncated — PostgreSQL would otherwise silently truncate the
    # tail server-side.
    very_long = "x" * 200
    augmented = _augment_postgres_settings({}, unique_name=very_long)
    assert len(augmented["application_name"]) == 63
    assert augmented["application_name"].startswith(
        "pathway:" + "x" * (63 - len("pathway:"))
    )


def test_psql_copy_writer_ingests_500k_rows_within_30s(tmp_path, postgres):
    """The Postgres writer must ingest a 500,000-row batch within 30 seconds.

    A row-by-row INSERT path is round-trip-bound (~4k rows/s on a local server)
    and cannot meet this budget; the binary-COPY path the writer now uses by
    default streams the whole batch in bulk and clears it with large headroom.

    The pipeline runs in a background process via ``wait_result_with_checker``,
    which polls the row count and gives up at the 30s deadline -- so a slow or
    broken writer fails the test at ~30s instead of blocking for the whole
    (potentially unbounded) run.
    """
    n_rows = 500_000

    class InputSchema(pw.Schema):
        k: int
        name: str
        value: float
        flag: bool

    # A directory with one CSV file, read in streaming mode so the run process
    # stays alive while we poll (and is terminated by the checker on success or
    # timeout), rather than exiting and racing the count query.
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    with open(input_dir / "data.csv", "w") as f:
        f.write("k,name,value,flag\n")
        for i in range(n_rows):
            f.write(f"{i},item_{i},{i * 0.5},{'True' if i % 2 else 'False'}\n")

    with postgres.temporary_table(ddl_fmt=None) as table_name:
        table = pw.io.csv.read(str(input_dir), schema=InputSchema)
        pw.io.postgres.write(
            table,
            POSTGRES_SETTINGS,
            table_name,
            init_mode="replace",
            max_batch_size=10_000,
        )
        wait_result_with_checker(RowCountChecker(n_rows, postgres, table_name), 30)

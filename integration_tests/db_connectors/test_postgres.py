import copy
import datetime
import json
import os
import threading
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
    SimpleObject,
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
def test_init_default_table_not_exists(write_method):
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

    with pytest.raises(api.EngineError):
        write_method(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name="non_existent_table",
        )
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
            a: str | None
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
            is_primary_key = write_method is not pw.io.postgres.write
            if is_primary_key:
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
            assert row["j"] == pd.Timedelta("4 days 2 seconds 123 us").value // 1_000
            assert row["k"] == ("abc", "def", "ghi")
            assert row["l"] == (
                pd.Timedelta("4 days 2 seconds 123 us").value // 1_000,
                pd.Timedelta("1 days 2 seconds 3 us").value // 1_000,
            )
            assert row["m"] == (("a", "b"), ("c", "d"))

    observer = TestObserver()
    G.clear()
    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
    )
    pw.io.python.write(table, observer)
    run()
    assert observer.n_rows_processed == 1


@pytest.mark.parametrize("write_method", [pw.io.postgres.write, write_snapshot(["i"])])
def test_init_create_if_not_exists_append(write_method, postgres):
    table_name = postgres.random_table_name()

    class InputSchema(pw.Schema):
        i: int
        data: int

    for i in range(3):
        G.clear()
        table = pw.debug.table_from_rows(
            InputSchema,
            [(i, i)],
        )
        write_method(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="create_if_not_exists",
        )
        run()

    result = postgres.get_table_contents(table_name, InputSchema.column_names(), "i")

    assert result == [{"i": 0, "data": 0}, {"i": 1, "data": 1}, {"i": 2, "data": 2}]


@pytest.mark.parametrize("write_method", [pw.io.postgres.write, write_snapshot(["i"])])
def test_init_replace(write_method, postgres):
    table_name = postgres.random_table_name()

    class InputSchema(pw.Schema):
        i: int
        data: int

    for i in range(3):
        G.clear()
        table = pw.debug.table_from_rows(
            InputSchema,
            [(i, i)],
        )
        write_method(
            table,
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            init_mode="replace",
        )
        run()

    result = postgres.get_table_contents(table_name, InputSchema.column_names(), "i")
    assert result == [{"i": 2, "data": 2}]

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
            match="Failed to establish PostgreSQL connection: "
            "failed to perform write in postgres",
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
        match="Failed to establish PostgreSQL connection: "
        "failed to perform write in postgres",
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

        wait_result_with_checker(FileLinesNumberChecker(output_path, 10), 30)

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


def test_postgres_read_missing_value_fields(tmp_path, postgres):
    """Schema fields that don't exist in the table should raise an error."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str
        nonexistent_column: str  # this column is not in the table

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

    with pytest.raises(RuntimeError, match="nonexistent_column"):
        table = pw.io.postgres.read(
            postgres_settings=POSTGRES_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="static",
        )
        pw.io.jsonlines.write(table, output_path)
        pw.run()


def test_postgres_read_missing_multiple_value_fields(tmp_path, postgres):
    """All missing fields should appear in the error message."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str
        missing_a: str
        missing_b: int

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

    with pytest.raises(RuntimeError, match="missing_a|missing_b"):
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


def test_postgres_read_primary_key_mismatch_subset(tmp_path, postgres):
    """Passing only a subset of PK columns should raise a mismatch error."""

    class InputSchema(pw.Schema):
        id_a: int = pw.column_definition(primary_key=True)
        id_b: int
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

    with postgres.publication(table_name) as publication_name:
        with pytest.raises(RuntimeError, match="primary key mismatch|id_a|id_b"):
            table = pw.io.postgres.read(
                postgres_settings=POSTGRES_SETTINGS,
                table_name=table_name,
                schema=InputSchema,
                mode="streaming",
                publication_name=publication_name,
            )
            pw.io.jsonlines.write(table, output_path)
            pw.run()


def test_postgres_read_primary_key_mismatch_superset(tmp_path, postgres):
    """Passing extra non-PK columns as PK should raise a mismatch error."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str = pw.column_definition(primary_key=True)

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

    with postgres.publication(table_name) as publication_name:
        with pytest.raises(RuntimeError, match="primary key mismatch|value"):
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
        wait_result_with_checker(FileLinesNumberChecker(output_path, n_total), 30)

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
        wait_result_with_checker(FileLinesNumberChecker(output_path, n_expected), 30)

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

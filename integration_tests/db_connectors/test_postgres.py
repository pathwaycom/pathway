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
    }
    assert result == [expected_output_row]
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
        def __init(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.last_row: dict[str, Any] | None = None

        def on_change(
            self, key: pw.Pointer, row: dict[str, Any], time: int, is_addition: bool
        ) -> None:
            self.last_row = row

    G.clear()
    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="static",
    )
    observer = TestObserver()
    pw.io.python.write(table, observer)
    run()
    assert observer.last_row is not None
    assert observer.last_row["a"] == "foo"
    assert observer.last_row["b"] == 1.5
    assert not observer.last_row["c"]
    assert observer.last_row["f"] == pw.Json({"foo": "bar", "baz": 123})
    assert observer.last_row["g"] == datetime.datetime(2025, 3, 14, 10, 13)
    assert observer.last_row["h"] == datetime.datetime(
        2025, 4, 23, 10, 13, tzinfo=datetime.timezone.utc
    )
    assert observer.last_row["i"].value == SimpleObject("test")


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
            v_pyobject BYTEA
        );
    """
    postgres.execute_sql(create_table_sql)

    create_publication_sql = f"""
    CREATE PUBLICATION {table_name}_pub FOR TABLE {table_name};
    """
    postgres.execute_sql(create_publication_sql)

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
            v_pyobject
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
            '\\xABCDEF'::bytea
        );
    """
    postgres.execute_sql(insert_row_sql)

    table = pw.io.postgres.read(
        postgres_settings=postgres_settings,
        table_name=table_name,
        schema=InputSchema,
        mode="streaming",
        publication_name=f"{table_name}_pub",
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
    postgres.execute_sql(f"CREATE PUBLICATION {table_name}_pub FOR TABLE {table_name};")

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="streaming",
        publication_name=f"{table_name}_pub",
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
    postgres.execute_sql(f"CREATE PUBLICATION {table_name}_pub FOR TABLE {table_name};")
    postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('value');")

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="streaming",
        publication_name=f"{table_name}_pub",
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
    postgres.execute_sql(f"CREATE PUBLICATION {table_name}_pub FOR TABLE {table_name};")

    for i in range(5):
        postgres.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('row_{i}');")

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="streaming",
        publication_name=f"{table_name}_pub",
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
    postgres.execute_sql(f"CREATE PUBLICATION {table_name}_pub FOR TABLE {table_name};")
    postgres.execute_sql(
        f"INSERT INTO {table_name} (id, duration) VALUES (1, INTERVAL '{pg_interval}');"
    )

    table = pw.io.postgres.read(
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        schema=InputSchema,
        mode="streaming",
        publication_name=f"{table_name}_pub",
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

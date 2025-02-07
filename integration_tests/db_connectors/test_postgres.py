import datetime
import json

import pandas as pd
import pytest
from utils import POSTGRES_SETTINGS

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import run


def test_psql_output_stream(tmp_path, postgres):
    class InputSchema(pw.Schema):
        name: str
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_table = postgres.create_table(InputSchema, used_for_output=True)

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


def test_psql_output_snapshot(tmp_path, postgres):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_table = postgres.create_table(InputSchema, used_for_output=True)

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

    with pytest.raises(ValueError, match="Primary key must be specified"):
        table = pw.debug.table_from_rows(InputSchema, [(1,), (2,), (3,)])
        pw.io.postgres.write_snapshot(
            table,
            table_name=table_name,
            postgres_settings=POSTGRES_SETTINGS,
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
        pw.io.postgres.write_snapshot(
            table,
            table_name=table_name,
            postgres_settings=POSTGRES_SETTINGS,
            primary_key=["a"],
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
        pw.io.postgres.write_snapshot(
            table,
            table_name=table_name,
            postgres_settings=POSTGRES_SETTINGS,
            primary_key=["a", "b"],
            init_mode="create_if_not_exists",
        )
        run()

    result = postgres.get_table_contents(
        table_name, InputSchema.column_names(), ("a", "b")
    )

    assert result == [{"a": 1, "b": 1}, {"a": 2, "b": 2}, {"a": 3, "b": 3}]


def write_snapshot(primary_key: list[str]):
    def _write_snapshot(table: pw.Table, /, **kwargs):
        pw.io.postgres.write_snapshot(table, **kwargs, primary_key=primary_key)

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


class SimpleObject:
    def __init__(self, a):
        self.a = a

    def __eq__(self, other):
        return self.a == other.a


@pytest.mark.parametrize("write_method", [pw.io.postgres.write, write_snapshot(["a"])])
def test_init_create_if_not_exists(write_method, postgres):
    table_name = postgres.random_table_name()

    class InputSchema(pw.Schema):
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

    write_method(
        table,
        postgres_settings=POSTGRES_SETTINGS,
        table_name=table_name,
        init_mode="create_if_not_exists",
    )
    run()

    result = postgres.get_table_contents(table_name, InputSchema.column_names())

    for row in result:
        obj = api.deserialize(bytes(row["i"]))
        assert isinstance(
            obj, pw.PyObjectWrapper
        ), f"expecting PyObjectWrapper, got {type(obj)}"
        row["i"] = obj.value

    assert result == [
        {
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
    ]


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

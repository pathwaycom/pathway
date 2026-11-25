# Copyright © 2026 Pathway

import csv
import json

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import assert_table_equality_wo_index, run_all


class Leaf(pw.Schema):
    three: float
    currency: str = pw.column_definition(default_value="USD")


class Object(pw.Schema):
    two: Leaf


def read_cells(tmp_path, schema, rows, delimiter=","):
    path = tmp_path / "nested.csv"
    with path.open("w", newline="") as file:
        writer = csv.writer(file, delimiter=delimiter)
        writer.writerow(schema.column_names())
        writer.writerows(rows)
    return pw.io.csv.read(
        path,
        schema=schema,
        mode="static",
        csv_settings=pw.io.csv.CsvParserSettings(delimiter=delimiter),
    )


@pytest.mark.parametrize("delimiter", [",", ";"])
def test_nested_csv_deep_access_and_quoted_json(tmp_path, delimiter):
    payload = {
        "two": {"three": 1, "extra": 'a,b; "quoted"\nnext π'},
        "extra": {"keep": True},
    }
    table = read_cells(
        tmp_path,
        pw.schema_from_types(one=Object),
        [[json.dumps(payload, ensure_ascii=False, indent=2)]],
        delimiter,
    )
    projected = table.select(two=table.one.two)
    result = table.select(
        direct=table.one.two.three,
        bracket=table.one["two"]["three"],
        projected=projected.two.three,
        raw=table.one,
        object_=table.one.two,
    )
    expected_schema = pw.schema_from_types(
        direct=float, bracket=float, projected=float, raw=pw.Json, object_=pw.Json
    )
    assert result.schema.typehints() == expected_schema.typehints()
    expected_object = {**payload["two"], "currency": "USD"}
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (
                    1.0,
                    1.0,
                    1.0,
                    pw.Json({**payload, "two": expected_object}),
                    pw.Json(expected_object),
                )
            ],
        ),
    )


def test_nested_csv_optional_cells_and_plain_json_null(tmp_path):
    schema = pw.schema_from_types(row=int, one=Object | None, raw=pw.Json | None)
    tokens = ["", "null", " NULL ", "None", "   "]
    table = read_cells(tmp_path, schema, [[i, s, s] for i, s in enumerate(tokens)])
    result = table.select(
        row=table.row, one=table.one, value=table.one.two.three, raw=table.raw
    )
    expected_schema = pw.schema_from_types(
        row=int, one=pw.Json | None, value=float | None, raw=pw.Json | None
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (i, None, None, pw.Json.NULL if s == "null" else None)
                for i, s in enumerate(tokens)
            ],
        ),
    )


@pytest.mark.parametrize("optional", [False, True])
def test_nested_csv_empty_cell_default_does_not_replace_null(tmp_path, optional):
    schema = pw.schema_builder(
        {
            "row": pw.column_definition(dtype=int),
            "one": pw.column_definition(
                dtype=Object | None if optional else Object,
                default_value={"two": {"three": 2}},
            ),
        }
    )
    table = read_cells(tmp_path, schema, [[1, ""], [2, "null"]])
    result = table.remove_errors().select(row=pw.this.row, value=pw.this.one.two.three)
    expected_schema = pw.schema_from_types(
        row=int, value=float | None if optional else float
    )
    expected_rows = [(1, 2.0), (2, None)] if optional else [(1, 2.0)]
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(expected_schema, expected_rows),
        terminate_on_error=False,
    )


def test_nested_csv_invalid_cells_are_error_values(tmp_path):
    invalid = [
        "",
        "null",
        "{",
        "[]",
        "{}",
        '{"two": {}}',
        '{"two": {"three": "1.5"}}',
        '{"two": {"three": true}}',
        '{"two": {"three": null}}',
        '{"two": {"three": 1, "currency": null}}',
    ]
    schema = pw.schema_from_types(row=int, one=Object)
    table = read_cells(
        tmp_path,
        schema,
        [[i, s] for i, s in enumerate(invalid)] + [[99, '{"two": {"three": 1.5}}']],
    )
    result = table.remove_errors().select(row=pw.this.row, value=pw.this.one.two.three)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(row=int, value=float), [(99, 1.5)]
        ),
        terminate_on_error=False,
    )


def test_nested_csv_lists_and_tuples(tmp_path):
    schema = pw.schema_from_types(items=list[Leaf], pair=tuple[Leaf, str])
    table = read_cells(
        tmp_path,
        schema,
        [[json.dumps([{"three": 1}, {"three": 2}]), json.dumps([{"three": 3}, "x"])]],
    )
    result = table.select(
        first=table.items[0].three,
        missing=table.items[2].three,
        last=table.pair[0].three,
        label=table.pair[1],
    )
    expected_schema = pw.schema_from_types(
        first=float | None, missing=float | None, last=float, label=str
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(expected_schema, [(1.0, None, 3.0, "x")]),
    )


def test_nested_csv_roundtrip(tmp_path):
    schema = pw.schema_from_types(one=Object)
    table = read_cells(tmp_path, schema, [['{"two": {"three": 1.5}}']])
    path = tmp_path / "output.csv"
    pw.io.csv.write(table, path)
    run_all()
    G.clear()
    table = pw.io.csv.read(path, schema=schema, mode="static")
    result = table.select(value=table.one.two.three, currency=table.one.two.currency)
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(value=float, currency=str), [(1.5, "USD")]
        ),
    )


def test_nested_csv_empty_input(tmp_path):
    table = read_cells(tmp_path, pw.schema_from_types(one=Object), [])
    result = table.select(value=table.one.two.three)
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(pw.schema_from_types(value=float), [])
    )

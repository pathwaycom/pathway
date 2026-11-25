# Copyright © 2026 Pathway

import json
from typing import Callable, Optional

import numpy as np
import pytest

import pathway as pw
from pathway.internals import api, dtype as dt
from pathway.internals._io_helpers import _form_value_fields
from pathway.tests.utils import assert_table_equality_wo_index


class Price(pw.Schema):
    value: float
    currency: str = pw.column_definition(default_value="USD")


class Deal(pw.Schema):
    price: Price


class JsonDeal(pw.Schema):
    price: pw.Json


def expected_price(value):
    return pw.debug.table_from_rows(JsonDeal, [(pw.Json(value),)])


@pytest.mark.parametrize("use_path", [False, True])
def test_nested_json_read_and_roundtrip(tmp_path, use_path):
    value = {"value": 59.98, "extra": {"keep": True}}
    record = {"price": value}
    if use_path:
        record = {"payload": record}
    input_path = tmp_path / "input.jsonl"
    input_path.write_text(json.dumps(record) + "\n")
    table = pw.io.jsonlines.read(
        input_path,
        schema=Deal,
        mode="static",
        json_field_paths={"price": "/payload/price"} if use_path else None,
    )
    assert table.schema.typehints() == {"price": pw.Json}
    assert table.select(value=table.price["value"]).schema.typehints() == {
        "value": float
    }
    expected = {**value, "currency": "USD"}
    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    assert json.loads(output_path.read_text())["price"] == expected


@pytest.mark.parametrize("use_path", [False, True])
@pytest.mark.parametrize(
    "invalid",
    [
        None,
        42,
        [],
        {},
        {"value": "59.98"},
        {"value": True},
        {"value": None},
        {"value": 59.98, "currency": 7},
        {"value": 59.98, "currency": None},
    ],
)
def test_nested_json_invalid_object(tmp_path, use_path, invalid):
    input_path = tmp_path / "input.jsonl"
    records = [{"price": invalid}, {"price": {"value": 59.98}}]
    if use_path:
        records = [{"payload": record} for record in records]
    input_path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    table = pw.io.jsonlines.read(
        input_path,
        schema=Deal,
        mode="static",
        json_field_paths={"price": "/payload/price"} if use_path else None,
    )
    assert_table_equality_wo_index(
        table.remove_errors(),
        expected_price({"value": 59.98, "currency": "USD"}),
        terminate_on_error=False,
    )


def test_nested_json_arrays_optional_and_defaults(tmp_path):
    class Basket(pw.Schema):
        prices: list[Price]
        alternative: Optional[Price]
        pair: tuple[Price, int]

    class Input(pw.Schema):
        basket: Basket
        missing: Price = pw.column_definition(default_value={"value": 1})
        optional: Price | None
        default_optional: Price | None = pw.column_definition(
            default_value=pw.Json(None)
        )

    class Expected(pw.Schema):
        basket: pw.Json
        missing: pw.Json
        optional: Optional[pw.Json]
        default_optional: Optional[pw.Json]

    input_path = tmp_path / "input.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "basket": {
                    "prices": [{"value": 2}, {"value": 3}],
                    "alternative": None,
                    "pair": [{"value": 4}, 5],
                },
                "optional": None,
            }
        )
        + "\n"
    )
    table = pw.io.jsonlines.read(input_path, schema=Input, mode="static")
    expected = pw.debug.table_from_rows(
        Expected,
        [
            (
                pw.Json(
                    {
                        "prices": [
                            {"value": 2, "currency": "USD"},
                            {"value": 3, "currency": "USD"},
                        ],
                        "alternative": None,
                        "pair": [{"value": 4, "currency": "USD"}, 5],
                    }
                ),
                pw.Json({"value": 1, "currency": "USD"}),
                None,
                None,
            )
        ],
    )
    assert_table_equality_wo_index(table, expected)


def test_nested_json_schema_metadata_survives_schema_operations():
    class Inherited(Deal):
        other: int

    for schema in [
        Deal,
        Inherited.without("other"),
        Deal | pw.Schema,
        pw.schema_from_dict(**Deal.to_json_serializable_dict()),
        pw.schema_from_dict({"price": Price}),
        pw.schema_builder({"price": pw.column_definition(dtype=Price)}),
    ]:
        assert schema.typehints() == {"price": pw.Json}
        assert schema["price"].json_schema == Deal["price"].json_schema
        assert schema["price"].json_schema is not None
    assert Deal.with_types(price=pw.Json)["price"].json_schema is None

    # typing.Optional hashes its argument, including nested schemas.
    assert pw.schema_from_types(deal=Optional[Deal]).typehints() == {
        "deal": Optional[pw.Json]
    }
    namespace = {"pw": pw, "pathway": pw}
    exec(Deal.generate_class("Generated"), namespace)
    assert namespace["Generated"] == Deal


def test_nested_json_keeps_pointer_and_opaque_annotations():
    types = {
        "pointer": pw.Pointer[Price],
        "pointers": list[pw.Pointer[Price]],
        "optional": Optional[pw.Pointer[Price]],
        "opaque": pw.PyObjectWrapper[Price],
    }
    schema = pw.schema_from_types(**types)
    for name, annotation in types.items():
        assert schema[name].dtype == dt.wrap(annotation)
        assert schema[name].json_schema is None


@pytest.mark.parametrize("format_", ["identity"])
def test_nested_json_rejects_other_formats(format_):
    with pytest.raises(
        ValueError,
        match="only for JSON, CSV, BSON, Avro, transparent or Debezium input",
    ):
        api.DataFormat(format_, None, _form_value_fields(Deal))


@pytest.mark.parametrize("format_", ["bson", "avro", "transparent", "debezium"])
def test_nested_json_accepts_native_format(format_):
    api.DataFormat(format_, None, _form_value_fields(Deal))


@pytest.mark.parametrize("reader", [pw.io.jsonlines.read, pw.io.csv.read])
def test_nested_json_defaults_are_validated_before_reading(tmp_path, reader):
    class BadPrice(pw.Schema):
        value: int = pw.column_definition(default_value="bad")

    class BadDeal(pw.Schema):
        price: BadPrice

    with pytest.raises(ValueError, match="/price/value"):
        reader(tmp_path / "empty", schema=BadDeal, mode="static")

    class BadDefault(pw.Schema):
        price: Price = pw.column_definition(default_value={"value": "bad"})

    with pytest.raises(ValueError, match="/price/value"):
        reader(tmp_path / "empty", schema=BadDefault, mode="static")


@pytest.mark.parametrize("dtype", [Callable[..., int], dt.Future(dt.INT)])
def test_nested_json_rejects_types_without_json_decoding(dtype):
    inner = pw.schema_builder({"value": pw.column_definition(dtype=dtype)})
    with pytest.raises(TypeError, match="not supported inside a nested JSON schema"):
        pw.schema_builder({"price": pw.column_definition(dtype=inner)})


@pytest.mark.parametrize("dtype", [np.ndarray, np.ndarray[tuple[int], np.dtype[str]]])
def test_nested_json_arrays_require_numeric_element_types(dtype):
    inner = pw.schema_from_types(value=dtype)
    with pytest.raises(TypeError, match="require int or float elements"):
        pw.schema_from_types(price=inner)


def test_nested_json_rejects_unsupported_containers():
    with pytest.raises(TypeError, match="Other containers and unions"):

        class Invalid(pw.Schema):
            prices: dict[str, Price]


def test_nested_json_rejects_recursive_schema():
    with pytest.raises(TypeError, match="Recursive nested JSON schemas"):

        class Recursive(pw.Schema):
            child: "Recursive"


@pytest.mark.parametrize("use_path", [False, True])
def test_nested_json_top_level_arrays_and_null_default(tmp_path, use_path):
    class Item(pw.Schema):
        value: int
        label: Optional[str] = pw.column_definition(default_value=None)

    class Input(pw.Schema):
        items: list[Item]
        pair: tuple[Item, bool]

    valid = {"items": [{"value": 1}, {"value": 2}], "pair": [{"value": 3}, True]}
    invalid = [
        {**valid, "items": None},
        {**valid, "items": [{"value": 1}, {"value": "bad"}]},
        {**valid, "items": [{"value": 1, "label": 7}]},
        {**valid, "pair": [{"value": 3}]},
        {**valid, "pair": [{"value": 3}, 1]},
    ]
    records = [valid, *invalid]
    if use_path:
        records = [{"payload": record} for record in records]
    path = tmp_path / "input.jsonl"
    path.write_text("\n".join(map(json.dumps, records)) + "\n")
    table = pw.io.jsonlines.read(
        path,
        schema=Input,
        mode="static",
        json_field_paths=(
            {"items": "/payload/items", "pair": "/payload/pair"} if use_path else None
        ),
    )
    expected = pw.debug.table_from_rows(
        pw.schema_from_types(items=pw.Json, pair=pw.Json),
        [
            (
                pw.Json([{"value": 1, "label": None}, {"value": 2, "label": None}]),
                pw.Json([{"value": 3, "label": None}, True]),
            )
        ],
    )
    assert_table_equality_wo_index(
        table.remove_errors(), expected, terminate_on_error=False
    )


def test_nested_json_optional_field_is_required(tmp_path):
    class Item(pw.Schema):
        value: Optional[int]

    class Input(pw.Schema):
        item: Item

    path = tmp_path / "input.jsonl"
    path.write_text('{"item": {}}\n{"item": {"value": null}}\n')
    table = pw.io.jsonlines.read(path, schema=Input, mode="static")
    expected = pw.debug.table_from_rows(
        pw.schema_from_types(item=pw.Json), [(pw.Json({"value": None}),)]
    )
    assert_table_equality_wo_index(
        table.remove_errors(), expected, terminate_on_error=False
    )


@pytest.mark.parametrize("value_kind", ["float", "object", "json"])
def test_inferred_map_metadata_access_and_roundtrip(tmp_path, value_kind):
    values = {"type": value_kind}
    if value_kind == "object":
        values["fields"] = [
            {"name": "price", "schema": {"type": "float"}},
            {
                "name": "currency",
                "schema": {"type": "string"},
                "default": {"value": "USD"},
            },
        ]
    schema = pw.schema_builder(
        {
            "key": pw.column_definition(dtype=str),
            "prices": pw.column_definition(
                dtype=pw.Json, _json_schema={"type": "map", "values": values}
            ),
        }
    )
    schema = pw.schema_from_dict(
        **json.loads(json.dumps(schema.to_json_serializable_dict()))
    )
    input_path = tmp_path / "maps.jsonl"
    value = {"price": 1.5} if value_kind == "object" else 1.5
    input_path.write_text(json.dumps({"key": "eur", "prices": {"eur": value}}) + "\n")
    table = pw.io.jsonlines.read(input_path, schema=schema, mode="static")
    if value_kind == "object":
        projected = table.select(prices=table.prices)
        result = table.select(
            direct=table.prices.eur.price,
            dynamic=projected.prices[table.key].price,
            missing=table.prices.absent.price,
            default=table.prices.eur.currency,
        )
        expected = pw.schema_from_types(
            direct=float | None,
            dynamic=float | None,
            missing=float | None,
            default=str | None,
        )
        rows = [(1.5, 1.5, None, "USD")]
    else:
        result = table.select(
            direct=table.prices.eur,
            dynamic=table.prices[table.key],
            missing=table.prices.absent,
        )
        dtype = pw.Json if value_kind == "json" else float
        expected = pw.schema_from_types(
            direct=dtype | None, dynamic=dtype | None, missing=dtype | None
        )
        value = pw.Json(1.5) if value_kind == "json" else 1.5
        rows = [(value, value, None)]
    assert result.schema.typehints() == expected.typehints()
    assert_table_equality_wo_index(result, pw.debug.table_from_rows(expected, rows))


def test_inferred_map_validation_reports_escaped_key(tmp_path, caplog):
    schema = pw.schema_builder(
        {
            "prices": pw.column_definition(
                dtype=pw.Json, _json_schema={"type": "map", "values": {"type": "float"}}
            )
        }
    )
    input_path = tmp_path / "maps.jsonl"
    input_path.write_text('{"prices":{"a/b~c":"wrong"}}\n{"prices":{}}\n')
    table = pw.io.jsonlines.read(input_path, schema=schema, mode="static")
    assert_table_equality_wo_index(
        table.remove_errors(),
        pw.debug.table_from_rows(
            pw.schema_from_types(prices=pw.Json), [(pw.Json({}),)]
        ),
        terminate_on_error=False,
    )
    assert "/prices/a~1b~0c" in caplog.text

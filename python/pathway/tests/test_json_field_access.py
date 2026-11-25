# Copyright © 2026 Pathway

import copy
import json
from typing import Optional

import pandas as pd
import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality_wo_index


class Price(pw.Schema):
    value: float
    quantity: int
    currency: str = pw.column_definition(default_value="USD")
    available: bool
    discount: Optional[float]


class Product(pw.Schema):
    price: Price


PRICE = {"value": 12, "quantity": 3, "available": True, "discount": None}


def read_json(tmp_path, schema, rows, filename="input.jsonl"):
    path = tmp_path / filename
    path.write_text("\n".join(map(json.dumps, rows)) + "\n")
    return pw.io.jsonlines.read(path, schema=schema, mode="static")


@pytest.mark.parametrize("use_brackets", [False, True])
def test_typed_json_scalars(tmp_path, use_brackets):
    table = read_json(tmp_path, Product, [{"price": PRICE}])
    value = pw.this.price["value"] if use_brackets else pw.this.price.value
    result = table.select(
        value=value,
        quantity=pw.this.price.quantity,
        currency=pw.this.price.currency,
        available=pw.this.price.available,
        discount=pw.this.price.discount,
        multiplied=value * pw.this.price.quantity,
    )
    expected_schema = pw.schema_from_types(
        value=float,
        quantity=int,
        currency=str,
        available=bool,
        discount=Optional[float],
        multiplied=float,
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(expected_schema, [(12.0, 3, "USD", True, None, 36.0)]),
    )


def test_nested_json_object_and_leaf_types(tmp_path):
    class Inner(pw.Schema):
        three: float

    class Outer(pw.Schema):
        two: Inner

    class Input(pw.Schema):
        one: Outer

    table = read_json(tmp_path, Input, [{"one": {"two": {"three": 7}}}])
    result = table.select(
        one=table.one,
        two=table.one.two,
        three=table.one.two.three,
    )
    expected_schema = pw.schema_from_types(one=pw.Json, two=pw.Json, three=float)
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [(pw.Json({"two": {"three": 7}}), pw.Json({"three": 7}), 7.0)],
        ),
    )

    # Projecting an intermediate JSON object retains its nested field types.
    projected = table.select(two=table.one.two)
    assert projected.schema.typehints() == {"two": pw.Json}
    leaf = projected.select(three=projected.two.three)
    assert leaf.schema.typehints() == {"three": float}
    assert_table_equality_wo_index(
        leaf,
        pw.debug.table_from_rows(pw.schema_from_types(three=float), [(7.0,)]),
    )


def test_untyped_json_dot_access():
    schema = pw.schema_from_types(data=pw.Json)
    table = pw.debug.table_from_rows(
        schema,
        [
            (
                pw.Json(
                    {
                        "address": {"city": "Paris"},
                        "items": [{"value": 3}],
                        "name": "inner",
                        "get": "method",
                        "str": "namespace",
                    }
                ),
            )
        ],
    )
    result = table.select(
        city=pw.this.data.address.city,
        amount=pw.this.data.items[0].value.as_int(unwrap=True),
        name=pw.this.data["name"].as_str(unwrap=True),
        method=pw.this.data["get"],
        namespace=pw.this.data["str"],
        absent=pw.this.data.absent,
    )
    expected = pw.debug.table_from_rows(
        pw.schema_from_types(
            city=pw.Json,
            amount=int,
            name=str,
            method=pw.Json,
            namespace=pw.Json,
            absent=pw.Json,
        ),
        [
            (
                pw.Json("Paris"),
                3,
                "inner",
                pw.Json("method"),
                pw.Json("namespace"),
                pw.Json(None),
            )
        ],
    )
    assert_table_equality_wo_index(result, expected)
    assert table.data.name == "data"
    assert not hasattr(table.data.city, "_dtype")
    assert not hasattr(table.data.city, "__setstate__")
    assert repr(copy.copy(table.data.city)) == repr(table.data.city)


def test_json_schema_survives_projection_rename_and_filter(tmp_path):
    table = read_json(tmp_path, Product, [{"price": PRICE}])
    result = table.select(quote=pw.this.price).rename(renamed=pw.this.quote)
    result = result.filter(result.renamed.available).select(value=pw.this.renamed.value)
    assert result.schema.typehints() == {"value": float}
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(pw.schema_from_types(value=float), [(12.0,)])
    )


def test_nullable_json_objects_and_arrays(tmp_path):
    class Order(pw.Schema):
        current: Optional[Price]
        previous: list[Price]
        pair: tuple[Price, int]

    class Input(pw.Schema):
        order: Optional[Order]

    table = read_json(
        tmp_path,
        Input,
        [
            {"order": {"current": PRICE, "previous": [PRICE], "pair": [PRICE, 5]}},
            {"order": {"current": None, "previous": [], "pair": [PRICE, 6]}},
            {"order": None},
        ],
    )
    # Exercise both a chain in one expression and a materialized nullable object.
    projected = table.select(current=pw.this.order.current)
    assert projected.schema.typehints() == {"current": Optional[pw.Json]}
    result = table.select(
        value=projected.current.value,
        old=pw.this.order.previous[0].value,
        pair_value=pw.this.order.pair[0].value,
        pair_count=pw.this.order.pair[1],
        unknown=pw.this.order.extra,
    )
    expected_schema = pw.schema_from_types(
        value=Optional[float],
        old=Optional[float],
        pair_value=Optional[float],
        pair_count=Optional[int],
        unknown=pw.Json,
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (12.0, 12.0, 12.0, 5, pw.Json(None)),
                (None, None, 12.0, 6, pw.Json(None)),
                (None, None, None, None, pw.Json(None)),
            ],
        ),
    )


def test_json_transformation_drops_structure(tmp_path):
    table = read_json(tmp_path, Product, [{"price": PRICE}])

    @pw.udf
    def replace_price(value: pw.Json) -> pw.Json:
        return pw.Json({"value": "changed"})

    changed = table.select(price=replace_price(pw.this.price))
    assert changed.schema["price"].json_schema is None
    result = changed.select(value=pw.this.price.value)
    assert result.schema.typehints() == {"value": pw.Json}
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            pw.schema_from_types(value=pw.Json), [(pw.Json("changed"),)]
        ),
    )


def test_dot_access_rejects_non_json():
    table = pw.debug.table_from_rows(pw.schema_from_types(value=int), [(1,)])
    with pytest.raises(TypeError, match="JSON or sequence"):
        table.select(value=pw.this.value.missing)


def test_typed_fields_in_left_join(tmp_path):
    class Input(Product):
        key: int

    products = read_json(tmp_path, Input, [{"key": 1, "price": PRICE}])
    keys = pw.debug.table_from_rows(pw.schema_from_types(key=int), [(1,), (2,)])
    joined = keys.join_left(products, keys.key == products.key)
    direct = joined.select(value=products.price.value)
    projected = joined.select(price=products.price).select(value=pw.this.price.value)
    expected = pw.debug.table_from_rows(
        pw.schema_from_types(value=Optional[float]), [(12.0,), (None,)]
    )
    assert direct.schema.typehints() == expected.schema.typehints()
    assert projected.schema.typehints() == expected.schema.typehints()
    assert_table_equality_wo_index(direct, expected)
    assert_table_equality_wo_index(projected, expected)


@pytest.mark.parametrize("operation", ["concat", "update_rows", "update_cells"])
@pytest.mark.parametrize("schema_kind", ["same", "different", "untyped"])
def test_combining_tables_checks_all_input_structures(operation, schema_kind):
    matching = schema_kind == "same"
    other_schema = {
        "same": Product,
        "different": pw.schema_from_types(price=pw.schema_from_types(value=str)),
        "untyped": pw.schema_from_types(price=pw.Json),
    }[schema_kind]
    first_rows = [pw.Json(PRICE)] * (2 if operation == "update_cells" else 1)
    first = pw.debug.table_from_pandas(
        pd.DataFrame({"price": first_rows}, index=[1, 3][: len(first_rows)]),
        schema=Product,
    )
    second = pw.debug.table_from_pandas(
        pd.DataFrame(
            {"price": [pw.Json(PRICE if matching else {"value": "changed"})]},
            index=[1 if operation == "update_cells" else 2],
        ),
        schema=other_schema,
    )
    if operation == "concat":
        pw.universes.promise_are_pairwise_disjoint(first, second)
    elif operation == "update_cells":
        first = first.with_columns(marker=1)
        pw.universes.promise_is_subset_of(second, first)
    combined = getattr(first, operation)(second)
    result = combined.select(value=pw.this.price.value)
    assert result.schema.typehints() == {"value": float if matching else pw.Json}
    values = [12.0, 12.0] if matching else [pw.Json(12), pw.Json("changed")]
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(result.schema, [(value,) for value in values])
    )


def test_conditional_json_preserves_only_common_structure(tmp_path):
    class Input(pw.Schema):
        optional_price: Optional[Price]
        price: Price

    table = read_json(
        tmp_path,
        Input,
        [
            {"optional_price": None, "price": PRICE},
            {"optional_price": PRICE, "price": PRICE},
        ],
    )
    projected = table.select(
        coalesced=pw.coalesce(pw.this.optional_price, pw.this.price),
        conditional=pw.if_else(pw.this.optional_price.is_none(), None, pw.this.price),
        required=pw.require(pw.this.price, pw.this.optional_price),
        different=pw.if_else(
            pw.this.optional_price.is_none(),
            pw.Json({"value": "changed"}),
            pw.this.price,
        ),
    )
    result = projected.select(
        a=pw.this.coalesced.value,
        b=pw.this.conditional.value,
        c=pw.this.required.value,
        d=pw.this.different.value,
    )
    expected_schema = pw.schema_from_types(
        a=float, b=Optional[float], c=Optional[float], d=pw.Json
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (12.0, None, None, pw.Json("changed")),
                (12.0, 12.0, 12.0, pw.Json(12)),
            ],
        ),
    )
    unwrapped = projected.filter(pw.this.conditional.is_not_none()).select(
        value=pw.unwrap(pw.this.conditional).value
    )
    assert unwrapped.schema.typehints() == {"value": float}
    assert_table_equality_wo_index(
        unwrapped,
        pw.debug.table_from_rows(pw.schema_from_types(value=float), [(12.0,)]),
    )


def test_scalar_arrays_and_dynamic_keys(tmp_path):
    class Data(pw.Schema):
        numbers: list[int]
        pair: tuple[str, float]
        empty: None
        name: str
        get: str

    class Input(pw.Schema):
        data: Data
        index: int
        key: str

    table = read_json(
        tmp_path,
        Input,
        [
            {
                "data": {
                    "numbers": [1, 2],
                    "pair": ["a", 3],
                    "empty": None,
                    "name": "field",
                    "get": "reserved",
                    "extra": "kept",
                },
                "index": 1,
                "key": "name",
            }
        ],
    )
    result = table.select(
        number=pw.this.data.numbers[pw.this.index],
        absent=pw.this.data.numbers[99],
        negative=pw.this.data.numbers[-1],
        word=pw.this.data.pair[0],
        value=pw.this.data.pair[1],
        empty=pw.this.data.empty,
        name=pw.this.data["name"],
        method=pw.this.data["get"],
        dynamic=pw.this.data[pw.this.key],
        extra=pw.this.data.extra,
        raw=pw.this.data.get("name", default=pw.Json("fallback")),
    )
    expected_schema = pw.schema_from_types(
        number=Optional[int],
        absent=Optional[int],
        negative=Optional[int],
        word=str,
        value=float,
        empty=type(None),
        name=str,
        method=str,
        dynamic=pw.Json,
        extra=pw.Json,
        raw=pw.Json,
    )
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result,
        pw.debug.table_from_rows(
            expected_schema,
            [
                (
                    2,
                    None,
                    None,
                    "a",
                    3.0,
                    None,
                    "field",
                    "reserved",
                    pw.Json("field"),
                    pw.Json("kept"),
                    pw.Json("field"),
                ),
            ],
        ),
    )
    with pytest.raises(IndexError, match="out of range"):
        table.select(value=pw.this.data.pair[2])


def test_validation_errors_propagate_through_typed_access(tmp_path):
    table = read_json(
        tmp_path,
        Product,
        [
            {"price": PRICE},
            {"price": {**PRICE, "value": "invalid"}},
        ],
    )
    result = table.select(total=pw.this.price.value * pw.this.price.quantity)
    assert result.schema.typehints() == {"total": float}
    assert_table_equality_wo_index(
        result.remove_errors(),
        pw.debug.table_from_rows(pw.schema_from_types(total=float), [(36.0,)]),
        terminate_on_error=False,
    )


def test_deeply_nested_nullable_fields(tmp_path):
    schema = pw.schema_from_types(value=int)
    value = {"value": 7}
    for _ in range(12):
        schema = pw.schema_from_types(child=Optional[schema])
        value = {"child": value}
    table = read_json(
        tmp_path,
        pw.schema_from_types(data=schema),
        [
            {"data": value},
            {"data": {"child": None}},
        ],
    )
    field = table.data
    for _ in range(12):
        field = field.child
    result = table.select(value=field.value)
    expected_schema = pw.schema_from_types(value=Optional[int])
    assert result.schema.typehints() == expected_schema.typehints()
    assert_table_equality_wo_index(
        result, pw.debug.table_from_rows(expected_schema, [(7,), (None,)])
    )

# Copyright © 2023 Pathway

from __future__ import annotations

import re
from typing import Any

import pandas as pd
import pytest

import pathway as pw
from pathway import dt
from pathway.debug import table_from_pandas
from pathway.tests.utils import T, assert_table_equality, run_all


def _json_table_from_list(data):
    class _JsonSubject(pw.io.python.ConnectorSubject):
        def __init__(self, data: list[dict[str, Any]]) -> None:
            super().__init__()
            self.data = data

        def run(self) -> None:
            for key, row in enumerate(self.data):
                self.next_json({"key": key + 1, **row})

    schema = pw.schema_builder(
        columns={
            "key": pw.column_definition(dtype=int, primary_key=True),
            **{name: pw.column_definition(dtype=dt.JSON) for name in data[0]},
        }
    )

    return pw.io.python.read(_JsonSubject(data), schema=schema).without(pw.this.key)


def _json_table(**kwargs) -> pw.Table:
    return _json_table_from_list([dict(zip(kwargs, v)) for v in zip(*kwargs.values())])


def _optional_json_table(**kwargs) -> pw.Table:
    @pw.udf
    def filter_null(col: pw.Json) -> pw.Json | None:
        if col == pw.Json(None):
            return None
        return col

    table = _json_table(**kwargs)

    return table.select(**{name: filter_null(pw.this[name]) for name in kwargs})


def test_json_get_simple():
    input = _json_table(data=[{"value": 1}, {"value": 2}])

    result = input.select(ret=pw.this.data.get("value"))

    assert_table_equality(
        _optional_json_table(ret=[1, 2]),
        result,
    )


def test_json_get_none():
    input = _json_table(data=[{}])

    with pytest.raises(
        TypeError, match=re.escape(f"Cannot get from {dt.Optional(dt.JSON)}.")
    ):
        input.select(result=pw.this.data.get("a").get("b"))


def test_json_get_default():
    input = _json_table(
        data=[
            {"a": {"b": 1}},
            {"a": {"b": None}},
            {"a": {}},
            {"a": [1, 2, 3]},
            {"a": 42},
            {"a": None},
            {},
            [1, 2, 3],
            None,
            1,
            "foo",
        ]
    )

    result = input.select(result=pw.this.data.get("a", default={"b": 42}))

    assert_table_equality(
        _json_table(
            result=[
                {"b": 1},
                {"b": None},
                {},
                [1, 2, 3],
                42,
                None,
                {"b": 42},
                {"b": 42},
                {"b": 42},
                {"b": 42},
                {"b": 42},
            ]
        ),
        result,
    )


def test_json_get_wo_default():
    input = _json_table(
        data=[
            {"a": {"b": 1}},
        ]
    )

    with pytest.raises(
        TypeError, match=re.escape(rf"Cannot get from {dt.Optional(dt.JSON)}.")
    ):
        input.select(result=pw.this.data.get("a").get("b"))


def test_json_dict_get_int_index():
    input = _json_table(
        data=[
            {"a": 1},
        ]
    )

    result = input.select(result=pw.this.data.get(1))

    assert_table_equality(
        T(
            """
                | result
            1   |
            """
        ).update_types(result=dt.Optional(dt.JSON)),
        result,
    )


def test_json_array_get_str_index():
    input = _json_table(
        data=[
            {"a": [1, 2, 3]},
        ]
    )

    result = input.select(result=pw.this.data["a"].get("foo"))

    assert_table_equality(
        T(
            """
                | result
            1   |
            """
        ).update_types(result=dt.Optional(dt.JSON)),
        result,
    )


def test_json_get_wrong_default():
    input = _json_table(
        data=[
            {"a": {"b": 1}},
        ]
    )

    with pytest.raises(
        TypeError,
        match=re.escape(rf"Default must be of type {dt.Optional(dt.JSON)}, found INT."),
    ):
        input.select(result=pw.this.data.get("a", 42).get("b"))


def test_json_get_item():
    input = _json_table(
        data=[
            {"a": {"b": 1}},
            {"a": {"b": None}},
            {},
            {"a": {}},
            {"a": [1, 2, 3]},
            {"a": 42},
            {"a": None},
        ]
    )

    result = input.select(result=pw.this.data["a"]["b"])

    assert_table_equality(
        _json_table(result=[1, None, None, None, None, None, None]),
        result,
    )


def test_json_get_array_index():
    input = _json_table(
        index=[0, 1, 2],
        data=[{"value": [1, 2, 3]}, {"value": [4, 5, 6]}, {"value": [7, 8, 9]}],
    )

    result = input.select(result=pw.this.data["value"][pw.this.index.as_int()])

    assert_table_equality(
        _json_table(result=[1, 5, 9]),
        result,
    )


@pytest.mark.parametrize("index", [-1, -4, 3])
def test_json_get_array_index_out_of_bounds(index):
    input = _json_table(data=[{"value": [0, 1, 2]}])

    result = input.select(result=pw.this.data["value"][index])

    assert_table_equality(
        _json_table(result=[None]),
        result,
    )


def test_json_get_item_optional_json():
    input = _json_table(data=[{}])

    with pytest.raises(
        TypeError,
        match=re.escape(f"Cannot get from {dt.Optional(dt.JSON)}."),
    ):
        input.select(result=pw.this.data.get("a")["b"])


@pytest.mark.parametrize(
    "from_,to_,method",
    [
        (
            [{"value": 42}, {"value": -1}, {"value": None}, {}],
            [42, -1, None, None],
            pw.ColumnExpression.as_int,
        ),
        (
            [
                {"value": 1.5},
                {"value": 10},
                {"value": 0},
                {"value": -1},
                {"value": 2**32 + 1},
                {"value": 2**45 + 1},
                {"value": None},
                {},
            ],
            [1.5, 10.0, 0.0, -1.0, float(2**32 + 1), float(2**45 + 1), None, None],
            pw.ColumnExpression.as_float,
        ),
        (
            [{"value": "foo"}, {"value": "42"}, {"value": "true"}, {"value": None}, {}],
            ["foo", "42", "true", None, None],
            pw.ColumnExpression.as_str,
        ),
        (
            [{"value": True}, {"value": False}, {"value": None}, {}],
            [True, False, None, None],
            pw.ColumnExpression.as_bool,
        ),
    ],
)
def test_json_as_type(from_, to_, method):
    to_dtype = type(to_[0])

    input = _json_table(data=from_)

    result = input.select(result=method(pw.this.data.get("value")))

    expected = table_from_pandas(
        pd.DataFrame({"key": list(range(1, len(to_) + 1)), "result": to_}),
        schema=pw.schema_builder(
            columns={
                "key": pw.column_definition(primary_key=True, dtype=int),
                "result": pw.column_definition(dtype=dt.Optional(to_dtype)),
            }
        ),
    ).without(pw.this.key)

    assert_table_equality(result, expected)


@pytest.mark.parametrize(
    "value",
    ["42", "foo", 1.6, True],
)
def test_json_as_int_wrong_values(value):
    input = _json_table(data=[{"value": value}])

    input.select(result=pw.this.data.get("value").as_int())

    with pytest.raises(ValueError):
        run_all()


@pytest.mark.parametrize(
    "value",
    ["42", "foo", True],
)
def test_json_as_float_wrong_values(value):
    input = _json_table(data=[{"value": value}])

    input.select(result=pw.this.data.get("value").as_float())

    with pytest.raises(ValueError):
        run_all()


@pytest.mark.parametrize(
    "value",
    [1, 1.6, True],
)
def test_json_as_str_wrong_values(value):
    input = _json_table(data=[{"value": value}])

    input.select(result=pw.this.data.get("value").as_str())

    with pytest.raises(ValueError):
        run_all()


@pytest.mark.parametrize(
    "value",
    [1, 0, 1.6, "1", "0", "true", "True"],
)
def test_json_as_bool_wrong_values(value):
    input = _json_table(data=[{"value": value}])

    input.select(result=pw.this.data.get("value").as_bool())

    with pytest.raises(ValueError):
        run_all()


def test_json_input():
    table = _json_table_from_list(
        [
            {
                "a": {"value": 1},
                "b": 2,
                "c": 1.5,
                "d": True,
                "e": "foo",
                "f": [1, 2, 3],
            }
        ]
    )

    result = table.select(
        a=pw.this.a["value"].as_int(),
        b=pw.this.b.as_int(),
        c=pw.this.c.as_float(),
        d=pw.this.d.as_bool(),
        e=pw.this.e.as_str(),
        f=pw.this.f[1].as_int(),
    )

    assert_table_equality(
        T(
            """
                | a | b | c   | d    | e    | f
            1   | 1 | 2 | 1.5 | True | foo  | 2
            """
        ).update_types(
            a=dt.Optional(int),
            b=dt.Optional(int),
            c=dt.Optional(float),
            d=dt.Optional(bool),
            e=dt.Optional(str),
            f=dt.Optional(int),
        ),
        result,
    )


def test_json_apply():
    table = _json_table(a=[1, 2, 3])

    @pw.udf
    def map(a: pw.Json) -> int:
        assert isinstance(a.value, int)
        return a.value + 1

    result = table.select(ret=map(**table))

    assert_table_equality(
        T(
            """
                | ret
            1   | 2
            2   | 3
            3   | 4
            """
        ),
        result,
    )


def test_json_type():
    table = _json_table(
        a=[{"value": 1}], b=[2], c=[1.5], d=[True], e="foo", f=[[1, 2, 3]]
    )

    @pw.udf
    def assert_types(**kwargs) -> bool:
        return all(isinstance(arg, pw.Json) for arg in kwargs.values())

    result = table.select(ret=assert_types(**table))

    assert_table_equality(
        T(
            """
                | ret
            1   | True
            """
        ),
        result,
    )


def test_json_recursive():
    table = T(
        """
            | value
        1   | 1
        2   | 2
        3   | 3
        """
    )

    @pw.udf
    def wrap(value: int) -> pw.Json:
        j = pw.Json(pw.Json(pw.Json(value)))
        assert isinstance(j.value, int)
        return j

    result = table.select(ret=wrap(pw.this.value).as_int())

    assert_table_equality(
        T(
            """
                | ret
            1   | 1
            2   | 2
            3   | 3
            """
        ).update_types(ret=dt.Optional(int)),
        result,
    )


def test_json_nested():
    table = T(
        """
            | value
        1   | 1
        2   | 2
        3   | 3
        """
    )

    @pw.udf
    def wrap(value: int) -> pw.Json:
        j = pw.Json([pw.Json(value)])
        assert isinstance(j.value[0].value, int)  # type:ignore
        return j

    result = table.select(ret=wrap(pw.this.value).get(0).as_int())

    assert_table_equality(
        T(
            """
                | ret
            1   | 1
            2   | 2
            3   | 3
            """
        ).update_types(ret=dt.Optional(int)),
        result,
    )

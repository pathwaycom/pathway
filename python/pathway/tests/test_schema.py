# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Any, Type

import pytest

import pathway as pw
from pathway.internals.schema import Schema
from pathway.io._utils import _compat_schema


def assert_same_schema(left: Type[Schema], right: Type[Schema]):
    assert left == right and left.__name__ == right.__name__


def test_schema_type_inconsistency_error():
    with pytest.raises(TypeError):

        class TestSchema(pw.Schema):
            a: int = pw.column_definition(dtype=str)


def test_schema_column_definition_error():
    with pytest.raises(ValueError):

        class TestSchema(pw.Schema):
            a: int = 1


def test_schema_no_annotation():
    with pytest.raises(
        ValueError, match=r"definitions of columns a, c lack type annotation.*"
    ):

        class TestSchema(pw.Schema):
            a = pw.column_definition()
            b: int = pw.column_definition()
            c = pw.column_definition()


def test_schema_override_column_name():
    class A(pw.Schema):
        a: int = pw.column_definition(name="@timestamp")
        b: int

    assert "@timestamp" in A.as_dict()
    assert "b" in A.as_dict()


def test_schema_builder():
    schema = pw.schema_builder(
        columns={
            "a": pw.column_definition(dtype=int, name="aa"),
            "b": pw.column_definition(dtype=str, default_value="default"),
            "c": pw.column_definition(),
        },
        name="FooSchema",
        properties=pw.SchemaProperties(append_only=True),
    )

    class FooSchema(pw.Schema, append_only=True):
        a: int = pw.column_definition(dtype=int, name="aa")
        b: str = pw.column_definition(dtype=str, default_value="default")
        c: Any

    assert_same_schema(schema, FooSchema)


def test_schema_eq():
    class A(pw.Schema):
        a: int = pw.column_definition(primary_key=True)
        b: str = pw.column_definition(default_value="foo")

    class B(pw.Schema):
        b: str = pw.column_definition(default_value="foo")
        a: int = pw.column_definition(primary_key=True)

    class C(pw.Schema):
        a: int = pw.column_definition(primary_key=True)
        b: str = pw.column_definition(default_value="foo")
        c: int

    class D(pw.Schema):
        a: int = pw.column_definition()
        b: str = pw.column_definition(default_value="foo")

    class E(pw.Schema):
        a: str = pw.column_definition(primary_key=True)
        b: str = pw.column_definition(default_value="foo")

    class F(pw.Schema, append_only=True):
        a: int = pw.column_definition(primary_key=True)
        b: str = pw.column_definition(default_value="foo")

    class Same(pw.Schema):
        a: int = pw.column_definition(primary_key=True)
        b: str = pw.column_definition(default_value="foo")

    schema_from_builder = pw.schema_builder(
        columns={
            "a": pw.column_definition(primary_key=True, dtype=int),
            "b": pw.column_definition(default_value="foo", dtype=str),
        },
        name="Foo",
    )
    compat_schema = _compat_schema(
        value_columns=["a", "b"],
        primary_key=["a"],
        default_values={"b": "foo"},
        types={
            "a": pw.Type.INT,
            "b": pw.Type.STRING,
        },
    )

    assert A != B, "column order should match"
    assert A != C, "column count should match"
    assert A != D, "column properties should match"
    assert A != E, "column types should match"
    assert A != F, "schema properties should match"
    assert A == Same
    assert A == schema_from_builder
    assert A == compat_schema


def test_schema_canonical_json():
    class A(pw.Schema):
        a: dict
        b: pw.Json

    assert A.columns()["a"].dtype == pw.dt.JSON
    assert A.columns()["b"].dtype == pw.dt.JSON

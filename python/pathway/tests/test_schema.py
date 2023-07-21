# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Any, Type

import pytest

import pathway as pw
from pathway.internals.schema import Schema


def assert_same_schema(left: Type[Schema], right: Type[Schema]):
    assert left.__name__ == right.__name__
    assert left.__columns__ == right.__columns__


def test_schema_type_inconsistency_error():
    with pytest.raises(TypeError):

        class TestSchema(pw.Schema):
            a: int = pw.column_definition(dtype=str)


def test_schema_column_definition_error():
    with pytest.raises(ValueError):

        class TestSchema(pw.Schema):
            a: int = 1


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
    )

    class FooSchema(pw.Schema):
        a: int = pw.column_definition(dtype=int, name="aa")
        b: str = pw.column_definition(dtype=str, default_value="default")
        c: Any

    assert_same_schema(schema, FooSchema)

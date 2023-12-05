import pytest

import pathway.internals as pw
from pathway.internals import dtype as dt
from pathway.internals.column_properties import ColumnProperties
from pathway.internals.decorators import empty_from_schema
from pathway.tests.utils import T


def assert_col_props(expr: pw.ColumnReference, properties: ColumnProperties):
    assert expr._column.properties == properties


def test_preserve_dependency_properties():
    input1 = T(
        """
            | a
        1   | 42
        """
    )
    input2 = T(
        """
            | b
        1   | 42
        """,
    )
    input3 = T(
        """
            | c
        1   | 42
        """,
        schema=pw.schema_builder(
            {"c": pw.column_definition(dtype=int)},
            properties=pw.SchemaProperties(append_only=False),
        ),
    )

    result = input1.select(a=input1.a, b=input1.a + input2.b, c=input1.a + input3.c)

    assert_col_props(result.a, ColumnProperties(dtype=dt.INT, append_only=True))
    assert_col_props(result.b, ColumnProperties(dtype=dt.INT, append_only=True))
    assert_col_props(result.c, ColumnProperties(dtype=dt.INT, append_only=False))


def test_preserve_context_dependency_properties():
    input1 = T(
        """
            | a
        1   | 42
        """
    )
    input2 = T(
        """
            | b
        1   | 42
        """,
    )
    input3 = T(
        """
            | c
        1   | 42
        """,
        schema=pw.schema_builder(
            {"c": pw.column_definition(dtype=int)},
            properties=pw.SchemaProperties(append_only=False),
        ),
    )

    res1 = input1.filter(pw.this.a == input2.b)
    res2 = input1.filter(pw.this.a == input3.c)

    assert_col_props(res1.a, ColumnProperties(dtype=dt.INT, append_only=True))
    assert_col_props(res2.a, ColumnProperties(dtype=dt.INT, append_only=False))


@pytest.mark.parametrize("append_only", [True, False])
def test_const_column_properties(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int = pw.column_definition(primary_key=True)

    table = empty_from_schema(Schema)

    result = table.select(ret=42)

    assert table.a._column.properties.append_only == append_only
    assert result.ret._column.properties.append_only == append_only


@pytest.mark.parametrize("append_only", [True, False])
def test_universe_properties(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int = pw.column_definition(primary_key=True)

    table = empty_from_schema(Schema)
    result = table.select()

    assert table._id_column.properties.append_only == append_only
    assert result._id_column.properties.append_only == append_only


def test_universe_properties_with_universe_of():
    class Schema(pw.Schema, append_only=True):
        a: int = pw.column_definition(primary_key=True)

    table = empty_from_schema(Schema)

    reduced = table.groupby(pw.this.a).reduce(pw.this.a)
    reduced_same_universe = (
        table.groupby(pw.this.a).reduce(pw.this.a).with_universe_of(table)
    )

    assert table._id_column.properties.append_only
    assert not reduced._id_column.properties.append_only
    assert reduced_same_universe._id_column.properties.append_only


def test_table_from_markdown_append_only():
    input1 = T(
        """
            | a
        1   | 42
        2   | 13
        """
    )
    assert input1._id_column.properties.append_only

    input2 = T(
        """
            | a  | __diff__
        1   | 42 |     1
        2   | 13 |     1
        """
    )
    assert input2._id_column.properties.append_only

    input3 = T(
        """
            | a  | __diff__
        1   | 42 |     1
        1   | 42 |    -1
        """
    )
    assert not input3._id_column.properties.append_only

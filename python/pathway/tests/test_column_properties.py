import pathway.internals as pw
from pathway.internals import dtype as dt
from pathway.internals.column_properties import ColumnProperties
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
    ).with_universe_of(input1)
    input3 = T(
        """
            | c
        1   | 42
        """,
        schema=pw.schema_builder(
            {"c": pw.column_definition(dtype=int)},
            properties=pw.SchemaProperties(append_only=False),
        ),
    ).with_universe_of(input1)

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
    ).with_universe_of(input1)
    input3 = T(
        """
            | c
        1   | 42
        """,
        schema=pw.schema_builder(
            {"c": pw.column_definition(dtype=int)},
            properties=pw.SchemaProperties(append_only=False),
        ),
    ).with_universe_of(input1)

    res1 = input1.filter(pw.this.a == input2.b)
    res2 = input1.filter(pw.this.a == input3.c)

    assert_col_props(res1.a, ColumnProperties(dtype=dt.INT, append_only=True))
    assert_col_props(res2.a, ColumnProperties(dtype=dt.INT, append_only=False))

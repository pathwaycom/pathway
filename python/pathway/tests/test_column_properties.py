# Copyright © 2024 Pathway

import pytest

import pathway.internals as pw
from pathway import io
from pathway.internals import dtype as dt
from pathway.internals.column_properties import ColumnProperties
from pathway.internals.datasource import DataSource
from pathway.internals.table_io import empty_from_schema, table_from_datasource
from pathway.tests.utils import T, TestDataSource


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
            | c   | __diff__
        1   | 42  |     1
        1   | 42  |    -1
        1   | 43  |     1
        """
    ).with_universe_of(input1)

    result = input1.select(a=input1.a, b=input1.a + input2.b, c=input1.a + input3.c)

    assert_col_props(result.a, ColumnProperties(dtype=dt.INT, append_only=True))
    assert_col_props(result.b, ColumnProperties(dtype=dt.INT, append_only=True))
    assert_col_props(result.c, ColumnProperties(dtype=dt.INT, append_only=False))


def test_preserve_dependency_properties_update_types():
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
            | c   | __diff__
        1   | 42  |     1
        1   | 42  |    -1
        1   | 43  |     1
        """
    ).with_universe_of(input1)

    result = input1.select(a=input1.a, b=input1.a + input2.b, c=input1.a + input3.c)
    result = result.update_types(a=int, b=int, c=int)

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
            | c   | __diff__
        1   | 42  |     1
        1   | 42  |    -1
        1   | 43  |     1
        """
    ).with_universe_of(input1)

    res1 = input1.filter(pw.this.a == input2.b)
    res2 = input1.filter(pw.this.a == input3.c)

    assert_col_props(res1.a, ColumnProperties(dtype=dt.INT, append_only=True))
    assert_col_props(res2.a, ColumnProperties(dtype=dt.INT, append_only=False))


@pytest.mark.parametrize("append_only", [True, False])
def test_const_column_properties(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int = pw.column_definition(primary_key=True)

    table = table_from_datasource(TestDataSource(schema=Schema))

    result = table.select(ret=42)

    assert table.a._column.properties.append_only == append_only
    assert result.ret._column.properties.append_only == append_only


@pytest.mark.parametrize("append_only", [True, False])
def test_universe_properties(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int = pw.column_definition(primary_key=True)

    table = table_from_datasource(TestDataSource(schema=Schema))
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


def test_python_connector_append_only():
    class TestSubject1(io.python.ConnectorSubject):
        def run(self):
            pass

    class TestSubject2(io.python.ConnectorSubject):
        def run(self):
            pass

        @property
        def _deletions_enabled(self) -> bool:
            return False

    class TestSchema(pw.Schema):
        a: int

    input1 = io.python.read(TestSubject1(), schema=TestSchema)
    assert not input1._id_column.properties.append_only

    input2 = io.python.read(TestSubject2(), schema=TestSchema)
    assert input2._id_column.properties.append_only


def test_append_only_no_columns():
    class MockDataSource(DataSource):
        def is_bounded(self) -> bool:
            raise NotImplementedError()

        def is_append_only(self) -> bool:
            return True

    class Schema(pw.Schema, append_only=True):
        pass

    table = table_from_datasource(MockDataSource(schema=Schema))
    assert table._id_column.properties.append_only


@pytest.mark.parametrize("delete_completed_queries", [False, True])
def test_rest_connector(delete_completed_queries: bool):
    class TestSchema(pw.Schema):
        a: int

    table, response_writer = io.http.rest_connector(
        host="127.0.0.1",
        port=30000,  # server is not started, port number does not matter
        schema=TestSchema,
        delete_completed_queries=delete_completed_queries,
    )
    assert table._id_column.properties.append_only != delete_completed_queries


@pytest.mark.parametrize("append_only", [True, False])
def test_buffer(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table._buffer(pw.this.a + 10, pw.this.a)

    assert result._id_column.properties.append_only == append_only
    assert result.a._column.properties.append_only == append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_buffer_2(append_only_1, append_only_2):
    class Schema(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table._buffer(pw.this.a + 10, pw.this.a)

    assert result._id_column.properties.append_only == append_only_1
    assert result.a._column.properties.append_only == append_only_1
    assert result.b._column.properties.append_only == (append_only_1 and append_only_2)
    # not obvious that it requires append_only_1 but if column `a` isn't append only and
    # column `b` is, we can have a situation when an old row leaves the buffer immediately
    # and a new row is kept. Then row is doesn't exist on the output for some time
    # so column `b` can't be append-only.


@pytest.mark.parametrize("append_only", [True, False])
def test_freeze(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table._freeze(pw.this.a + 10, pw.this.a)

    assert result._id_column.properties.append_only == append_only
    assert result.a._column.properties.append_only == append_only


@pytest.mark.parametrize("append_only", [True, False])
def test_reindex(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table.with_id_from(pw.this.a)

    assert result._id_column.properties.append_only == append_only
    assert result.a._column.properties.append_only == append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_reindex_2(append_only_1, append_only_2):
    class Schema(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table.with_id_from(pw.this.a)

    assert result._id_column.properties.append_only == append_only_1
    assert result.a._column.properties.append_only == append_only_1
    assert result.b._column.properties.append_only == (append_only_1 and append_only_2)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("allow_misses", [True, False])
def test_ix(append_only_1, append_only_2, allow_misses):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: pw.Pointer

    class Schema2(pw.Schema, append_only=append_only_2):
        b: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.select(b=table_2.ix(pw.this.a, allow_misses=allow_misses).b)

    assert result._id_column.properties.append_only == append_only_1
    assert result.b._column.properties.append_only == (
        append_only_1 and append_only_2 and (not allow_misses)
    )


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_intersect(append_only_1, append_only_2):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        b: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.intersect(table_2)

    assert result._id_column.properties.append_only == (append_only_1 and append_only_2)
    assert result.a._column.properties.append_only == (append_only_1 and append_only_2)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
def test_intersect_2(append_only_1, append_only_2, append_only_3):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    class Schema2(pw.Schema, append_only=append_only_3):
        c: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.intersect(table_2)

    assert result._id_column.properties.append_only == (
        (append_only_1 or append_only_2) and append_only_3
    )
    assert result.a._column.properties.append_only == (append_only_1 and append_only_3)
    assert result.b._column.properties.append_only == (append_only_2 and append_only_3)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_restrict(append_only_1, append_only_2):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        b: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    pw.universes.promise_is_subset_of(table_2, table_1)
    result = table_1.restrict(table_2)

    assert result._id_column.properties.append_only == (append_only_1 and append_only_2)
    assert result.a._column.properties.append_only == (append_only_1 and append_only_2)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
def test_restrict_2(append_only_1, append_only_2, append_only_3):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    class Schema2(pw.Schema, append_only=append_only_3):
        c: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    pw.universes.promise_is_subset_of(table_2, table_1)
    result = table_1.restrict(table_2)

    assert result._id_column.properties.append_only == (
        (append_only_1 or append_only_2) and append_only_3
    )
    assert result.a._column.properties.append_only == (append_only_1 and append_only_3)
    assert result.b._column.properties.append_only == (append_only_2 and append_only_3)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_difference(append_only_1, append_only_2):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        b: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.difference(table_2)

    assert not result._id_column.properties.append_only
    assert not result.a._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
def test_difference_2(append_only_1, append_only_2, append_only_3):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    class Schema2(pw.Schema, append_only=append_only_3):
        c: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.difference(table_2)

    assert not result._id_column.properties.append_only
    assert not result.a._column.properties.append_only
    assert not result.b._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_having(append_only_1, append_only_2):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        b: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1._having(table_2.id)

    assert result._id_column.properties.append_only == (append_only_1 and append_only_2)
    assert result.a._column.properties.append_only == (append_only_1 and append_only_2)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
def test_having_2(append_only_1, append_only_2, append_only_3):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    class Schema2(pw.Schema, append_only=append_only_3):
        c: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1._having(table_2.id)

    assert result._id_column.properties.append_only == (
        (append_only_1 or append_only_2) and append_only_3
    )
    assert result.a._column.properties.append_only == (append_only_1 and append_only_3)
    assert result.b._column.properties.append_only == (append_only_2 and append_only_3)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_update_rows(append_only_1, append_only_2):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        a: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.update_rows(table_2)

    assert result._id_column.properties.append_only == (append_only_1 and append_only_2)
    assert not result.a._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
@pytest.mark.parametrize("append_only_4", [True, False])
def test_update_rows_2(append_only_1, append_only_2, append_only_3, append_only_4):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    class Schema2(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_3)
        b: int = pw.column_definition(append_only=append_only_4)

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.update_rows(table_2)

    assert result._id_column.properties.append_only == (
        (append_only_1 or append_only_2) and (append_only_3 or append_only_4)
    )
    assert not result.a._column.properties.append_only
    assert not result.b._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_update_cells(append_only_1, append_only_2):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        a: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    table_2.promise_universe_is_subset_of(table_1)
    result = table_1.update_cells(table_2)

    assert result._id_column.properties.append_only == append_only_1
    assert not result.a._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
@pytest.mark.parametrize("append_only_4", [True, False])
def test_update_cells_2(append_only_1, append_only_2, append_only_3, append_only_4):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    class Schema2(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_3)
        b: int = pw.column_definition(append_only=append_only_4)

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    table_2.promise_universe_is_subset_of(table_1)
    result = table_1.update_cells(table_2)

    assert result._id_column.properties.append_only == (append_only_1 or append_only_2)
    assert not result.a._column.properties.append_only
    assert not result.b._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("reindex", [True, False])
def test_concat(append_only_1, append_only_2, reindex):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        a: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    if reindex:
        result = table_1.concat_reindex(table_2)
    else:
        pw.universes.promise_are_pairwise_disjoint(table_1, table_2)
        result = table_1.concat(table_2)

    assert result._id_column.properties.append_only == (append_only_1 and append_only_2)
    assert result.a._column.properties.append_only == (append_only_1 and append_only_2)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
@pytest.mark.parametrize("append_only_4", [True, False])
@pytest.mark.parametrize("reindex", [True, False])
def test_concat_2(append_only_1, append_only_2, append_only_3, append_only_4, reindex):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    class Schema2(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_3)
        b: int = pw.column_definition(append_only=append_only_4)

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    if reindex:
        result = table_1.concat_reindex(table_2)
    else:
        pw.universes.promise_are_pairwise_disjoint(table_1, table_2)
        result = table_1.concat(table_2)

    print(table_1._id_column.properties.append_only)
    print(table_2._id_column.properties.append_only)
    assert result._id_column.properties.append_only == (
        (append_only_1 or append_only_2) and (append_only_3 or append_only_4)
    )
    assert result.a._column.properties.append_only == (append_only_1 and append_only_3)
    assert result.b._column.properties.append_only == (append_only_2 and append_only_4)


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize("append_only_3", [True, False])
def test_with_universe_of(append_only_1, append_only_2, append_only_3):
    class Schema1(pw.Schema):
        k: int = pw.column_definition(primary_key=True, append_only=append_only_3)
        a: int = pw.column_definition(append_only=append_only_1)

    class Schema2(pw.Schema):
        k: int = pw.column_definition(primary_key=True, append_only=append_only_3)
        a: int = pw.column_definition(append_only=append_only_2)

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.with_universe_of(table_2)

    assert result._id_column.properties.append_only == (
        append_only_1 or append_only_2 or append_only_3
    )
    assert result.a._column.properties.append_only == append_only_1


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_join(append_only_1, append_only_2):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        b: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.join(table_2).select(pw.left.a, pw.right.b)

    assert result._id_column.properties.append_only == (append_only_1 and append_only_2)
    assert result.a._column.properties.append_only == (append_only_1 and append_only_2)
    assert result.b._column.properties.append_only == (append_only_1 and append_only_2)


@pytest.mark.parametrize("append_only_a", [True, False])
@pytest.mark.parametrize("append_only_b", [True, False])
@pytest.mark.parametrize("append_only_c", [True, False])
@pytest.mark.parametrize("append_only_d", [True, False])
def test_join_2(append_only_a, append_only_b, append_only_c, append_only_d):
    class Schema1(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_a)
        b: int = pw.column_definition(append_only=append_only_b)

    class Schema2(pw.Schema):
        c: int = pw.column_definition(append_only=append_only_c)
        d: int = pw.column_definition(append_only=append_only_d)

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.join(table_2, table_1.a == table_2.c).select(pw.left.b, pw.right.d)

    assert result._id_column.properties.append_only == (append_only_a and append_only_c)
    assert result.b._column.properties.append_only == (
        append_only_a and append_only_b and append_only_c
    )
    assert result.d._column.properties.append_only == (
        append_only_a and append_only_c and append_only_d
    )


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
@pytest.mark.parametrize(
    "mode", [pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER]
)
def test_outer_join(append_only_1, append_only_2, mode):
    class Schema1(pw.Schema, append_only=append_only_1):
        a: int

    class Schema2(pw.Schema, append_only=append_only_2):
        b: int

    table_1 = table_from_datasource(TestDataSource(schema=Schema1))
    table_2 = table_from_datasource(TestDataSource(schema=Schema2))
    result = table_1.join(table_2, how=mode).select(pw.left.a, pw.right.b)

    assert not result._id_column.properties.append_only
    assert not result.a._column.properties.append_only
    assert not result.b._column.properties.append_only


@pytest.mark.parametrize("append_only", [True, False])
def test_flatten(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: list[int]

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table.flatten(pw.this.a)

    assert result._id_column.properties.append_only == append_only
    assert result.a._column.properties.append_only == append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_flatten_2(append_only_1, append_only_2):
    class Schema(pw.Schema):
        a: list[int] = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table.flatten(pw.this.a)

    assert result._id_column.properties.append_only == append_only_1
    assert result.a._column.properties.append_only == append_only_1
    assert result.b._column.properties.append_only == (append_only_1 and append_only_2)


@pytest.mark.parametrize("append_only", [True, False])
def test_sorting(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table + table.sort(pw.this.a)

    assert result._id_column.properties.append_only == append_only
    assert result.a._column.properties.append_only == append_only
    assert not result.prev._column.properties.append_only
    assert not result.next._column.properties.append_only


@pytest.mark.parametrize("append_only", [True, False])
def test_remove_errors(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table.remove_errors()

    assert result._id_column.properties.append_only == append_only
    assert result.a._column.properties.append_only == append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_remove_errors_2(append_only_1, append_only_2):
    class Schema(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table.remove_errors()

    assert result._id_column.properties.append_only == (append_only_1 or append_only_2)
    assert result.a._column.properties.append_only == append_only_1
    assert result.b._column.properties.append_only == append_only_2


@pytest.mark.parametrize("append_only", [True, False])
def test_remove_retractions(append_only):
    class Schema(pw.Schema, append_only=append_only):
        a: int

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table._remove_retractions()

    assert result._id_column.properties.append_only
    assert result.a._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_remove_retractions_2(append_only_1, append_only_2):
    class Schema(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table._remove_retractions()

    assert result._id_column.properties.append_only
    assert result.a._column.properties.append_only
    assert result.b._column.properties.append_only


@pytest.mark.parametrize("append_only_1", [True, False])
@pytest.mark.parametrize("append_only_2", [True, False])
def test_fully_async_udf(append_only_1, append_only_2):
    class Schema(pw.Schema):
        a: int = pw.column_definition(append_only=append_only_1)
        b: int = pw.column_definition(append_only=append_only_2)

    @pw.udf(executor=pw.udfs.fully_async_executor())
    def foo(a: int, b: int) -> int:
        return a + b

    table = table_from_datasource(TestDataSource(schema=Schema))
    result = table.with_columns(c=foo(pw.this.a, pw.this.b))

    assert result._id_column.properties.append_only == (append_only_1 or append_only_2)
    assert result.a._column.properties.append_only == append_only_1
    assert result.b._column.properties.append_only == append_only_2
    assert result.c._column.properties.append_only is False

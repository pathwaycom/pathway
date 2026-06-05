# Copyright © 2026 Pathway

"""Preflight-validation coverage for the ClickHouse output connector.

Every misconfiguration that the connector can detect up front must surface as an
error at connector construction (when the dataflow starts), not partway through
the run on the first minibatch flush. These tests use an *empty* input table so
that, if a check were deferred to flush time, no flush with data would ever
happen and the misconfiguration would pass silently — which is exactly the
behavior these tests forbid.
"""

import numpy as np
import pytest

import pathway as pw


def _empty_str_table() -> pw.Table:
    class InputSchema(pw.Schema):
        data: str

    return pw.debug.table_from_rows(InputSchema, [])


def test_clickhouse_default_init_mode_requires_existing_table(clickhouse):
    """With ``init_mode="default"`` the docstring states the table must already
    exist. A non-existent table must be reported at construction, even when the
    input is empty, rather than silently writing nothing.
    """
    table_name = clickhouse.random_table_name()  # never created
    pw.io.clickhouse.write(
        _empty_str_table(),
        connection_string=clickhouse.connection_string,
        table_name=table_name,
        init_mode="default",
    )
    with pytest.raises(Exception, match="(?i)does not exist"):
        pw.run()


def test_clickhouse_unsupported_type_rejected_at_construction(clickhouse):
    """An unsupported column type must be rejected at construction regardless of
    ``init_mode`` and regardless of whether any rows are produced. With
    ``init_mode="default"`` the DDL path is skipped, so the type must be
    validated independently of table creation.
    """

    class InputSchema(pw.Schema):
        item: tuple[int, str]  # heterogeneous tuple -> ClickHouse Tuple, unsupported

    table = pw.debug.table_from_rows(InputSchema, [])
    table_name = clickhouse.random_table_name()
    pw.io.clickhouse.write(
        table,
        connection_string=clickhouse.connection_string,
        table_name=table_name,
        init_mode="default",
    )
    with pytest.raises(Exception, match="(?i)unsupported type"):
        pw.run()


@pytest.mark.parametrize(
    "ndarray_type",
    [
        np.ndarray,  # bare ndarray: unknown dimensionality and element type
        np.ndarray[tuple[int, int], np.dtype[np.float64]],  # 2-D
    ],
)
def test_clickhouse_unsupported_ndarray_rejected_at_construction(
    clickhouse, ndarray_type
):
    """Only a one-dimensional, element-typed numeric ndarray is supported, so a
    multi-dimensional or untyped ndarray must be rejected at construction even
    with empty input (the column-type validation runs before any table access).
    """
    schema = type("S", (pw.Schema,), {"__annotations__": {"item": ndarray_type}})
    table = pw.debug.table_from_rows(schema, [])
    table_name = clickhouse.random_table_name()
    pw.io.clickhouse.write(
        table,
        connection_string=clickhouse.connection_string,
        table_name=table_name,
        init_mode="default",
    )
    with pytest.raises(Exception, match="(?i)unsupported type"):
        pw.run()


def test_clickhouse_default_init_mode_requires_metadata_columns(clickhouse):
    """With ``init_mode="default"`` writing to a pre-existing table that lacks
    the auto-appended ``time``/``diff`` metadata columns must be reported at
    construction, naming the missing columns, instead of failing on the first
    insert.
    """
    with clickhouse.temp_table() as table_name:
        clickhouse.execute_sql(
            f"CREATE TABLE {table_name} (data String) ENGINE = MergeTree ORDER BY tuple()"
        )
        pw.io.clickhouse.write(
            _empty_str_table(),
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            init_mode="default",
        )
        with pytest.raises(Exception, match="(?i)(missing|time|diff)"):
            pw.run()


@pytest.mark.parametrize("init_mode", ["default", "create_if_not_exists"])
def test_clickhouse_existing_table_missing_value_column_rejected(clickhouse, init_mode):
    """A pre-existing table missing an output (value) column must be reported at
    construction, naming the missing column, instead of failing on the first
    insert. This holds for every init mode that does not recreate the table:
    ``"default"`` never creates it, and ``"create_if_not_exists"`` is a no-op when
    it already exists, so neither adds the missing column.
    """
    with clickhouse.temp_table() as table_name:
        # The destination has the metadata columns but is missing the ``data`` value
        # column the input table provides.
        clickhouse.execute_sql(
            f"CREATE TABLE {table_name} (other String, time Int64, diff Int8) "
            "ENGINE = MergeTree ORDER BY tuple()"
        )
        pw.io.clickhouse.write(
            _empty_str_table(),
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            init_mode=init_mode,
        )
        with pytest.raises(Exception, match="(?i)(missing|data)"):
            pw.run()


def test_clickhouse_default_init_mode_rejects_incompatible_column_type(clickhouse):
    """With ``init_mode="default"`` a pre-existing column whose type the writer's
    values cannot be inserted into (here a ``Int32`` column for a Pathway ``int``,
    which is written as ``Int64``) must be reported at construction, not on the
    first insert.
    """

    class IntSchema(pw.Schema):
        data: int

    with clickhouse.temp_table() as table_name:
        clickhouse.execute_sql(
            f"CREATE TABLE {table_name} (data Int32, time Int64, diff Int8) "
            "ENGINE = MergeTree ORDER BY tuple()"
        )
        pw.io.clickhouse.write(
            pw.debug.table_from_rows(IntSchema, []),
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            init_mode="default",
        )
        with pytest.raises(Exception, match="(?i)(cast|cannot|type)"):
            pw.run()

# Copyright © 2026 Pathway

"""Snapshot-mode coverage for the ClickHouse output connector.

In snapshot mode the connector maintains the current state of the table in a
``ReplacingMergeTree(version, is_deleted)`` engine. The current state is observed
with ``SELECT ... FINAL``. These tests drive deterministic simulated streams
(``table_from_rows(..., is_stream=True)``) with explicit ``__time__``/``__diff__``
columns so that updates and deletions are exercised without flaky real-time
streaming.
"""

import datetime

import pytest

import pathway as pw
from pathway.tests.utils import wait_result_with_checker

WAIT_TIMEOUT_SECONDS = 60


def _final_equals_checker(clickhouse, table_name, columns, sort_by, expected):
    """A checker that is true only once the FINAL snapshot equals ``expected``.

    Comparing the full content (not just the row count) makes it robust against an
    intermediate state that happens to have the same number of rows.
    """

    def check() -> bool:
        try:
            rows = clickhouse.get_table_contents(
                table_name, columns, sort_by=sort_by, final_=True
            )
        except Exception:
            return False
        return rows == expected

    return check


def _run_snapshot_stream(
    clickhouse,
    schema,
    rows,
    pk_names,
    columns,
    sort_by,
    expected,
    *,
    table_name=None,
    init_mode="create_if_not_exists",
):
    table_name = table_name or clickhouse.random_table_name()
    table = pw.debug.table_from_rows(schema, rows, is_stream=True)
    primary_key = [getattr(table, name) for name in pk_names]
    pw.io.clickhouse.write(
        table,
        connection_string=clickhouse.connection_string,
        table_name=table_name,
        output_table_type="snapshot",
        primary_key=primary_key,
        init_mode=init_mode,
    )
    checker = _final_equals_checker(clickhouse, table_name, columns, sort_by, expected)
    wait_result_with_checker(checker, WAIT_TIMEOUT_SECONDS)
    return table_name


# ---------------------------------------------------------------------------
# Pure-Python validation (no database required)
# ---------------------------------------------------------------------------


def test_clickhouse_snapshot_requires_primary_key():
    """Snapshot mode needs a primary key; omitting it must be rejected eagerly."""
    table = pw.debug.table_from_markdown(
        """
          | data
        1 | Hello
        """
    )
    with pytest.raises(ValueError, match="primary_key"):
        pw.io.clickhouse.write(
            table,
            connection_string="tcp://localhost:9000/default",
            table_name="irrelevant",
            output_table_type="snapshot",
        )


def test_clickhouse_primary_key_rejected_in_stream_mode():
    """``primary_key`` is meaningful only for snapshot mode; passing it in
    stream-of-changes mode must be rejected eagerly.
    """
    table = pw.debug.table_from_markdown(
        """
          | data
        1 | Hello
        """
    )
    with pytest.raises(ValueError, match="primary_key"):
        pw.io.clickhouse.write(
            table,
            connection_string="tcp://localhost:9000/default",
            table_name="irrelevant",
            primary_key=[table.data],
        )


@pytest.mark.parametrize("reserved_column", ["version", "is_deleted"])
def test_clickhouse_snapshot_rejects_reserved_columns(reserved_column):
    """``version`` and ``is_deleted`` are reserved metadata columns in snapshot
    mode; an input column with either name must be rejected eagerly.
    """
    table = pw.debug.table_from_markdown(
        f"""
          | key | {reserved_column}
        1 | 1   | 5
        """
    )
    with pytest.raises(ValueError, match=reserved_column):
        pw.io.clickhouse.write(
            table,
            connection_string="tcp://localhost:9000/default",
            table_name="irrelevant",
            output_table_type="snapshot",
            primary_key=[table.key],
        )


# ---------------------------------------------------------------------------
# End-to-end snapshot behavior
# ---------------------------------------------------------------------------


class _KVSchema(pw.Schema):
    k: int = pw.column_definition(primary_key=True)
    v: str


def test_clickhouse_snapshot_inserts_only(clickhouse):
    """With only insertions the snapshot contains every row."""
    rows = [
        (1, "a", 2, 1),
        (2, "b", 2, 1),
        (3, "c", 4, 1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            rows,
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "a"}, {"k": 2, "v": "b"}, {"k": 3, "v": "c"}],
            table_name=table_name,
        )


def test_clickhouse_snapshot_update_keeps_latest_value(clickhouse):
    """An update (retraction of the old value followed by insertion of the new
    one) across minibatches leaves only the latest value for the key.
    """
    rows = [
        (1, "a", 2, 1),
        (1, "a", 4, -1),
        (1, "b", 4, 1),
        (1, "b", 6, -1),
        (1, "c", 6, 1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            rows,
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "c"}],
            table_name=table_name,
        )


def test_clickhouse_snapshot_intra_minibatch_update_keeps_latest(clickhouse):
    """An update whose retraction and insertion share a minibatch still leaves
    the new value: the engine delivers the deletion before the insertion, so the
    insertion is assigned a higher version and wins.
    """
    rows = [
        (1, "old", 2, 1),
        (1, "old", 4, -1),  # same minibatch as the insertion below
        (1, "new", 4, 1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            rows,
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "new"}],
            table_name=table_name,
        )


def test_clickhouse_snapshot_delete_removes_key(clickhouse):
    """A retracted key disappears from the snapshot."""
    rows = [
        (1, "a", 2, 1),
        (2, "b", 2, 1),
        (3, "c", 2, 1),
        (2, "b", 4, -1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            rows,
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "a"}, {"k": 3, "v": "c"}],
            table_name=table_name,
        )


def test_clickhouse_snapshot_delete_then_reinsert(clickhouse):
    """A key that is deleted and later inserted again is present in the snapshot
    with the new value (the later insertion has the highest version).
    """
    rows = [
        (1, "a", 2, 1),
        (1, "a", 4, -1),
        (1, "revived", 6, 1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            rows,
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "revived"}],
            table_name=table_name,
        )


def test_clickhouse_snapshot_mixed_operations(clickhouse):
    """A mix of inserts, updates, and deletes across minibatches converges to the
    correct current state.
    """
    rows = [
        (1, "a", 2, 1),
        (2, "b", 2, 1),
        (3, "c", 2, 1),
        # minibatch 4: update 1, delete 3, insert 4
        (1, "a", 4, -1),
        (1, "a2", 4, 1),
        (3, "c", 4, -1),
        (4, "d", 4, 1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            rows,
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "a2"}, {"k": 2, "v": "b"}, {"k": 4, "v": "d"}],
            table_name=table_name,
        )


class _CompositeKeySchema(pw.Schema):
    region: str = pw.column_definition(primary_key=True)
    product: str = pw.column_definition(primary_key=True)
    qty: int


def test_clickhouse_snapshot_composite_primary_key(clickhouse):
    """A multi-column primary key identifies rows for updates and deletes."""
    rows = [
        ("eu", "apple", 10, 2, 1),
        ("eu", "pear", 5, 2, 1),
        ("us", "apple", 7, 2, 1),
        # update (eu, apple) -> 20, delete (eu, pear)
        ("eu", "apple", 10, 4, -1),
        ("eu", "apple", 20, 4, 1),
        ("eu", "pear", 5, 4, -1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _CompositeKeySchema,
            rows,
            ["region", "product"],
            ["region", "product", "qty"],
            ("region", "product"),
            [
                {"region": "eu", "product": "apple", "qty": 20},
                {"region": "us", "product": "apple", "qty": 7},
            ],
            table_name=table_name,
        )


class _TypedSchema(pw.Schema):
    k: int = pw.column_definition(primary_key=True)
    flag: bool
    price: float
    note: str | None
    ts: pw.DateTimeUtc


def test_clickhouse_snapshot_value_types_roundtrip(clickhouse):
    """Non-key columns of assorted types survive the snapshot round-trip,
    including an updated value and a nullable column.
    """
    ts0 = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    ts1 = datetime.datetime(2025, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc)
    rows = [
        (1, True, 1.5, "hello", ts0, 2, 1),
        (2, False, 2.5, None, ts0, 2, 1),
        # update row 1: flag/price/note/ts all change
        (1, True, 1.5, "hello", ts0, 4, -1),
        (1, False, 9.5, None, ts1, 4, 1),
    ]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _TypedSchema,
            rows,
            ["k"],
            [
                "k",
                "flag",
                "price",
                "note",
                "toUnixTimestamp64Nano(ts) AS ts_ns",
            ],
            "k",
            [
                {
                    "k": 1,
                    "flag": False,
                    "price": 9.5,
                    "note": None,
                    "ts_ns": int(ts1.timestamp()) * 1_000_000_000,
                },
                {
                    "k": 2,
                    "flag": False,
                    "price": 2.5,
                    "note": None,
                    "ts_ns": int(ts0.timestamp()) * 1_000_000_000,
                },
            ],
            table_name=table_name,
        )


def test_clickhouse_snapshot_creates_replacing_merge_tree(clickhouse):
    """``init_mode`` creates the snapshot table with a ReplacingMergeTree engine
    and the ``version``/``is_deleted`` metadata columns.
    """
    rows = [(1, "a", 2, 1)]
    with clickhouse.temp_table() as table_name:
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            rows,
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "a"}],
            table_name=table_name,
        )
        engine = clickhouse._query(
            "SELECT engine FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{table_name}' "
            "FORMAT TabSeparated"
        ).strip()
        assert engine == "ReplacingMergeTree", engine
        columns = clickhouse._query(
            "SELECT name FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table_name}' "
            "FORMAT TabSeparated"
        ).split()
        assert "version" in columns and "is_deleted" in columns


def test_clickhouse_snapshot_version_continues_across_runs(clickhouse):
    """A second snapshot run against an existing table seeds its version counter
    above the table's current maximum, so the second run's changes win over the
    first run's rows.
    """
    with clickhouse.temp_table() as table_name:
        # First run: insert k=1 -> "first".
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            [(1, "first", 2, 1)],
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "first"}],
            table_name=table_name,
            init_mode="create_if_not_exists",
        )
        pw.internals.parse_graph.G.clear()
        # Second run (default mode, same existing table): overwrite k=1 -> "second".
        _run_snapshot_stream(
            clickhouse,
            _KVSchema,
            [(1, "second", 2, 1)],
            ["k"],
            ["k", "v"],
            "k",
            [{"k": 1, "v": "second"}],
            table_name=table_name,
            init_mode="default",
        )
        # The second run's version must exceed the first run's, so FINAL shows it.
        rows = clickhouse.get_table_contents(table_name, ["k", "v"], final_=True)
        assert rows == [{"k": 1, "v": "second"}]


# ---------------------------------------------------------------------------
# Snapshot preflight against pre-existing tables (init_mode="default")
# ---------------------------------------------------------------------------


def _empty_kv_table() -> pw.Table:
    return pw.debug.table_from_rows(_KVSchema, [])


def test_clickhouse_snapshot_default_mode_requires_replacing_merge_tree(clickhouse):
    """In snapshot mode a pre-existing table that is not a ReplacingMergeTree must
    be rejected at construction, naming the engine requirement.
    """
    with clickhouse.temp_table() as table_name:
        clickhouse.execute_sql(
            f"CREATE TABLE {table_name} (k Int64, v String, version UInt64, is_deleted UInt8) "
            "ENGINE = MergeTree ORDER BY k"
        )
        table = _empty_kv_table()
        pw.io.clickhouse.write(
            table,
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            output_table_type="snapshot",
            primary_key=[table.k],
            init_mode="default",
        )
        with pytest.raises(Exception, match="(?i)ReplacingMergeTree"):
            pw.run()


def test_clickhouse_snapshot_default_mode_requires_metadata_columns(clickhouse):
    """In snapshot mode a pre-existing ReplacingMergeTree table missing the
    ``is_deleted`` metadata column must be rejected at construction.
    """
    with clickhouse.temp_table() as table_name:
        # A ReplacingMergeTree without the is_deleted column.
        clickhouse.execute_sql(
            f"CREATE TABLE {table_name} (k Int64, v String, version UInt64) "
            "ENGINE = ReplacingMergeTree(version) ORDER BY k"
        )
        table = _empty_kv_table()
        pw.io.clickhouse.write(
            table,
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            output_table_type="snapshot",
            primary_key=[table.k],
            init_mode="default",
        )
        with pytest.raises(Exception, match="(?i)(missing|is_deleted)"):
            pw.run()

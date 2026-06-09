"""Integration tests for NeonDB (https://neon.com).

NeonDB is a serverless, PostgreSQL-compatible database. Pathway talks to it
through the very same ``pw.io.postgres`` connector — no NeonDB-specific code
path exists in the framework. These tests exist to *prove* that compatibility
end-to-end against a genuine Neon backend and to raise an alert if a future
change (in Pathway or in Neon) breaks it. See issue #220.

The database is reached through the official ``neondatabase/neon_local`` proxy
container (compose service ``neon``). That proxy provisions a fresh ephemeral
branch of a real Neon cloud project on startup and tears it down on shutdown,
so each CI run gets an isolated database. Because it proxies to the cloud, it
requires ``NEON_API_KEY`` and ``NEON_PROJECT_ID``. These tests do not silently
skip when those are absent: a missing credential is a hard failure, so a
misconfigured CI never passes by accident. The tests pass only when they ran
against a real Neon backend and produced the expected result.

The streaming tests rely on logical replication (``pgoutput`` + a publication),
which Neon supports but only when **logical replication is enabled on the Neon
project** (Project Settings → Logical Replication). The neon_local proxy
forwards the replication protocol to the project's direct compute endpoint.
"""

import json
import threading

import pytest
from utils import (
    NEON_SETTINGS,
    check_write_quotes_reserved_word_column_name,
    check_write_quotes_table_name_with_special_characters,
)

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import FileLinesNumberChecker, run, wait_result_with_checker


def test_neondb_output_stream(tmp_path, neon):
    """``pw.io.postgres.write`` in the default stream-of-changes mode appends
    every (row, +1) / (row, -1) update to the Neon table."""

    class InputSchema(pw.Schema):
        name: str
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_table = neon.create_table(InputSchema, add_special_fields=True)

    def _run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.postgres.write(table, NEON_SETTINGS, output_table)
        run()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    _run(test_items)

    rows = neon.get_table_contents(output_table, InputSchema.column_names())
    rows.sort(key=lambda item: (item["name"], item["available"]))
    assert rows == test_items

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    _run(new_test_items)

    rows = neon.get_table_contents(
        output_table, InputSchema.column_names(), ("name", "available")
    )
    expected_rows = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert rows == expected_rows


def test_neondb_output_snapshot(tmp_path, neon):
    """``output_table_type="snapshot"`` maintains a primary-key-keyed snapshot
    of the latest state (upserts on change, deletes on retraction)."""

    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_table = neon.create_table(InputSchema, add_special_fields=False)

    def _run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.postgres.write(
            table,
            NEON_SETTINGS,
            output_table,
            output_table_type="snapshot",
            primary_key=[table.name],
        )
        run()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    _run(test_items)

    rows = neon.get_table_contents(output_table, InputSchema.column_names())
    rows.sort(key=lambda item: item["name"])
    assert rows == test_items

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    _run(new_test_items)

    rows = neon.get_table_contents(output_table, InputSchema.column_names(), "name")
    expected_rows = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert rows == expected_rows


@pytest.mark.parametrize("init_mode", ["create_if_not_exists", "replace"])
def test_neondb_write_creates_table(neon, init_mode):
    """Pathway owns the ``CREATE TABLE`` DDL on Neon: both ``create_if_not_exists``
    and ``replace`` provision the destination table when it does not yet exist."""

    class InputSchema(pw.Schema):
        i: int
        data: str

    table_name = neon.random_table_name()
    try:
        table = pw.debug.table_from_rows(InputSchema, [(0, "a"), (1, "b"), (2, "c")])
        pw.io.postgres.write(
            table,
            postgres_settings=NEON_SETTINGS,
            table_name=table_name,
            init_mode=init_mode,
        )
        run()

        result = neon.get_table_contents(table_name, InputSchema.column_names(), "i")
        assert result == [
            {"i": 0, "data": "a"},
            {"i": 1, "data": "b"},
            {"i": 2, "data": "c"},
        ]
    finally:
        neon.execute_sql(f"DROP TABLE IF EXISTS {table_name}")


def test_neondb_static_read(tmp_path, neon):
    """``pw.io.postgres.read(mode="static")`` reads a one-shot snapshot of a
    Neon table."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "output.jsonl"
    with neon.temporary_table(
        "CREATE TABLE {t} (id BIGSERIAL PRIMARY KEY, value TEXT NOT NULL)"
    ) as table_name:
        for i in range(5):
            neon.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('row_{i}');")

        table = pw.io.postgres.read(
            postgres_settings=NEON_SETTINGS,
            table_name=table_name,
            schema=InputSchema,
            mode="static",
        )
        pw.io.jsonlines.write(table, output_path)
        run()

    rows = []
    with open(output_path) as f:
        for line in f:
            rows.append(json.loads(line))

    assert len(rows) == 5
    assert {r["value"] for r in rows} == {f"row_{i}" for i in range(5)}
    assert all(r["diff"] == 1 for r in rows)


def test_neondb_streaming_cdc(tmp_path, neon):
    """``pw.io.postgres.read(mode="streaming")`` follows inserts and deletes on
    a Neon table via logical replication (``pgoutput`` + a publication).

    Requires logical replication to be enabled on the Neon project (Project
    Settings -> Logical Replication, which flips ``wal_level`` to ``logical``).
    The CI project this suite runs against must have it enabled; if it is not,
    that is a misconfiguration and the test fails loudly rather than passing by
    skipping.
    """
    neon.execute_sql("SHOW wal_level")
    wal_level = neon.cursor.fetchone()[0]
    assert wal_level == "logical", (
        "NeonDB CDC test needs logical replication: the project's wal_level "
        f"is {wal_level!r}, not 'logical'. Enable logical replication on the "
        "Neon project (Project Settings -> Logical Replication)."
    )

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        value: str

    output_path = tmp_path / "output.jsonl"
    with neon.temporary_table(
        "CREATE TABLE {t} (id BIGSERIAL PRIMARY KEY, value TEXT NOT NULL)"
    ) as table_name:
        with neon.publication(table_name) as publication_name:
            neon.execute_sql(f"INSERT INTO {table_name} (value) VALUES ('seed');")

            table = pw.io.postgres.read(
                postgres_settings=NEON_SETTINGS,
                table_name=table_name,
                schema=InputSchema,
                mode="streaming",
                publication_name=publication_name,
                autocommit_duration_ms=10,
            )
            pw.io.jsonlines.write(table, output_path)

            def stream_target():
                # Wait for the seed row to be replayed, then drive 5 more
                # inserts followed by 5 deletes, waiting for each to surface.
                wait_result_with_checker(
                    FileLinesNumberChecker(output_path, 1), 30, target=None
                )
                for i in range(5):
                    neon.execute_sql(
                        f"INSERT INTO {table_name} (value) VALUES ('row_{i}');"
                    )
                    wait_result_with_checker(
                        FileLinesNumberChecker(output_path, 2 + i), 30, target=None
                    )
                for i in range(5):
                    neon.execute_sql(
                        f"DELETE FROM {table_name} WHERE value = 'row_{i}';"
                    )
                    wait_result_with_checker(
                        FileLinesNumberChecker(output_path, 7 + i), 30, target=None
                    )

            stream_thread = threading.Thread(target=stream_target, daemon=True)
            stream_thread.start()

            # 1 seed insert + 5 inserts + 5 deletes = 11 change records.
            wait_result_with_checker(FileLinesNumberChecker(output_path, 11), 60)

    ids = set()
    with open(output_path) as f:
        for line in f:
            data = json.loads(line)
            diff = data["diff"]
            if diff == 1:
                ids.add(data["id"])
            elif diff == -1:
                ids.discard(data["id"])
            else:
                raise ValueError(f"unexpected diff: {diff}")
    # The seed row is the only one never deleted.
    assert len(ids) == 1


def _neon_quote_ident(name: str) -> str:
    """Escape a PostgreSQL identifier with double quotes (Neon is wire-compatible)."""
    return '"' + name.replace('"', '""') + '"'


def _neon_write(table, *, table_name, init_mode):
    """Bind ``pw.io.postgres.write`` over the Neon connection settings so the
    shared writer regressions stay connection-agnostic."""
    pw.io.postgres.write(
        table,
        postgres_settings=NEON_SETTINGS,
        table_name=table_name,
        init_mode=init_mode,
    )
    run()


def test_neondb_write_table_name_with_special_characters(neon):
    """A ``table_name`` that PostgreSQL/Neon accept only when quoted (a hyphen)
    must be interpolated safely into the generated CREATE TABLE + INSERT."""
    check_write_quotes_table_name_with_special_characters(
        write=_neon_write, db_context=neon, quote_ident=_neon_quote_ident
    )


def test_neondb_write_column_name_is_reserved_word(neon):
    """A column named with a reserved word (``user``) must survive CREATE TABLE
    and every generated INSERT against Neon."""
    check_write_quotes_reserved_word_column_name(
        write=_neon_write, db_context=neon, quote_ident=_neon_quote_ident
    )

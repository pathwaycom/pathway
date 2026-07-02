# Copyright © 2026 Pathway

from __future__ import annotations

import datetime
import json
import multiprocessing
import os
import pathlib
import time

import numpy as np
import pytest

import pathway as pw
from pathway.tests.utils import (
    ExceptionAwareThread,
    needs_multiprocessing_fork,
    only_with_license_key,
    run,
    wait_result_with_checker,
    write_lines,
)

duckdb = pytest.importorskip("duckdb")


def _connect(db_path: str):
    return duckdb.connect(db_path, read_only=True)


# --------------------------------------------------------------------------- #
# Stream of changes
# --------------------------------------------------------------------------- #


@only_with_license_key
def test_stream_of_changes_basic(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    table = pw.debug.table_from_markdown(
        """
          | name  | value
        1 | alice | 10
        2 | bob   | 20
        """
    )
    pw.io.duckdb.write(
        table,
        table_name="people",
        database=db_path,
        init_mode="create_if_not_exists",
    )
    run()

    con = _connect(db_path)
    rows = con.execute("SELECT name, value, diff FROM people ORDER BY name").fetchall()
    assert rows == [("alice", 10, 1), ("bob", 20, 1)]


@only_with_license_key
def test_stream_of_changes_records_deletions(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    table = pw.debug.table_from_markdown(
        """
          | key | value | __time__ | __diff__
        1 | a   | 1     | 2        | 1
        2 | a   | 1     | 4        | -1
        3 | b   | 2     | 4        | 1
        """
    )
    pw.io.duckdb.write(
        table,
        table_name="changelog",
        database=db_path,
        init_mode="create_if_not_exists",
    )
    run()

    con = _connect(db_path)
    rows = con.execute(
        "SELECT key, value, diff FROM changelog ORDER BY time, diff"
    ).fetchall()
    assert rows == [("a", 1, 1), ("a", 1, -1), ("b", 2, 1)]


@only_with_license_key
def test_stream_mode_rejects_reserved_column(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    table = pw.debug.table_from_markdown(
        """
        time | value
        1    | 10
        """
    )
    with pytest.raises(ValueError, match="collide with the 'time' and 'diff'"):
        pw.io.duckdb.write(table, table_name="t", database=db_path)


# --------------------------------------------------------------------------- #
# Snapshot mode
# --------------------------------------------------------------------------- #


@only_with_license_key
def test_snapshot_keeps_current_state(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "snap.duckdb")
    table = pw.debug.table_from_markdown(
        """
          | key | value | __time__ | __diff__
        1 | a   | 1     | 2        | 1
        2 | b   | 2     | 2        | 1
        3 | a   | 1     | 4        | -1
        4 | a   | 9     | 4        | 1
        5 | b   | 2     | 6        | -1
        """
    )
    pw.io.duckdb.write(
        table,
        table_name="state",
        database=db_path,
        output_table_type="snapshot",
        primary_key=[table.key],
        init_mode="create_if_not_exists",
    )
    run()

    con = _connect(db_path)
    # No service columns in snapshot mode.
    columns = [
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'state'"
        ).fetchall()
    ]
    assert set(columns) == {"key", "value"}
    # `a` was upserted to 9, `b` was deleted.
    rows = con.execute("SELECT key, value FROM state ORDER BY key").fetchall()
    assert rows == [("a", 9)]


@only_with_license_key
def test_snapshot_requires_primary_key(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "snap.duckdb")
    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    with pytest.raises(ValueError, match="primary_key must be specified"):
        pw.io.duckdb.write(
            table, table_name="t", database=db_path, output_table_type="snapshot"
        )


@only_with_license_key
def test_primary_key_forbidden_in_stream_mode(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "snap.duckdb")
    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    with pytest.raises(ValueError, match="primary_key can only be specified"):
        pw.io.duckdb.write(
            table, table_name="t", database=db_path, primary_key=[table.value]
        )


@only_with_license_key
def test_snapshot_rejects_nullable_primary_key(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "snap.duckdb")

    class S(pw.Schema):
        key: int | None
        value: int

    table = pw.debug.table_from_rows(S, [(1, 10)])
    with pytest.raises(ValueError, match="must be non-nullable"):
        pw.io.duckdb.write(
            table,
            table_name="t",
            database=db_path,
            output_table_type="snapshot",
            primary_key=[table.key],
        )


@only_with_license_key
def test_snapshot_rejects_duplicate_primary_key(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "snap.duckdb")
    table = pw.debug.table_from_markdown(
        """
        key | value
        1   | 10
        """
    )
    with pytest.raises(ValueError, match="duplicate column"):
        pw.io.duckdb.write(
            table,
            table_name="t",
            database=db_path,
            output_table_type="snapshot",
            primary_key=[table.key, table.key],
        )


@only_with_license_key
def test_snapshot_rejects_sort_by(tmp_path: pathlib.Path):
    """``sort_by`` reorders the changes within a minibatch. In snapshot mode an
    update is a deletion (``-1``) followed by an insertion (``+1``) of the same
    key, and reordering can place the insertion before the deletion — the upsert
    is then immediately wiped by the delete, silently losing rows. ``sort_by`` has
    no meaningful effect on a snapshot's final state anyway, so it must be
    rejected for the snapshot table type with a clear error at call time."""
    db_path = str(tmp_path / "snap.duckdb")
    table = pw.debug.table_from_markdown(
        """
        key | value
        a   | 1
        """
    )
    with pytest.raises(ValueError, match="(?i)sort_by.*snapshot|snapshot.*sort_by"):
        pw.io.duckdb.write(
            table,
            table_name="state",
            database=db_path,
            output_table_type="snapshot",
            primary_key=[table.key],
            sort_by=[table.value],
        )


@only_with_license_key
def test_snapshot_rejects_non_scalar_primary_key(tmp_path: pathlib.Path):
    """DuckDB cannot build a PRIMARY KEY / index on a list, array, tuple or JSON
    column, so using such a column as a snapshot primary key can never work. It
    must be rejected with a clear error at call time instead of crashing the
    engine with an opaque "Invalid type for index key" error mid-run."""
    db_path = str(tmp_path / "snap.duckdb")

    class S(pw.Schema):
        k: list[float]
        v: int

    table = pw.debug.table_from_rows(S, [([1.0, 2.0], 1)])
    with pytest.raises(ValueError, match="(?i)non-scalar|cannot.*index|primary"):
        pw.io.duckdb.write(
            table,
            table_name="t",
            database=db_path,
            output_table_type="snapshot",
            primary_key=[table.k],
            init_mode="create_if_not_exists",
        )


@only_with_license_key
def test_snapshot_composite_primary_key(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "snap.duckdb")
    table = pw.debug.table_from_markdown(
        """
          | owner | pet | age | __time__ | __diff__
        1 | alice | dog | 3   | 2        | 1
        2 | alice | cat | 1   | 2        | 1
        3 | alice | dog | 3   | 4        | -1
        4 | alice | dog | 4   | 4        | 1
        """
    )
    pw.io.duckdb.write(
        table,
        table_name="pets",
        database=db_path,
        output_table_type="snapshot",
        primary_key=[table.owner, table.pet],
        init_mode="create_if_not_exists",
    )
    run()

    con = _connect(db_path)
    rows = con.execute("SELECT owner, pet, age FROM pets ORDER BY pet").fetchall()
    assert rows == [("alice", "cat", 1), ("alice", "dog", 4)]


# --------------------------------------------------------------------------- #
# init_mode
# --------------------------------------------------------------------------- #


@only_with_license_key
def test_init_mode_default_requires_existing_table(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    pw.io.duckdb.write(table, table_name="missing", database=db_path)
    with pytest.raises(Exception, match="does not exist"):
        run()


@only_with_license_key
def test_init_mode_create_creates_table_even_when_output_is_empty(
    tmp_path: pathlib.Path,
):
    """``init_mode="create_if_not_exists"`` must create the destination table even
    when the Pathway table produces no rows. The table initialization is part of
    the connector's contract, not something that should depend on data flowing
    through it."""
    db_path = str(tmp_path / "out.duckdb")

    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    ).filter(
        pw.this.value > 1000
    )  # everything filtered out -> empty output

    pw.io.duckdb.write(
        table, table_name="t", database=db_path, init_mode="create_if_not_exists"
    )
    run()

    con = _connect(db_path)
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 0


@only_with_license_key
def test_init_mode_replace_recreates_table_even_when_output_is_empty(
    tmp_path: pathlib.Path,
):
    """``init_mode="replace"`` must drop and recreate the destination table even
    when the Pathway table produces no rows, so stale data from a previous run
    does not survive."""
    db_path = str(tmp_path / "out.duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE t (value BIGINT, time BIGINT, diff SMALLINT)")
    con.execute("INSERT INTO t VALUES (999, 0, 1)")
    con.close()

    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    ).filter(
        pw.this.value > 1000
    )  # empty output

    pw.io.duckdb.write(table, table_name="t", database=db_path, init_mode="replace")
    run()

    con = _connect(db_path)
    # The stale 999 row must be gone after the replace.
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 0


@only_with_license_key
def test_multiple_tables_in_one_database_file(tmp_path: pathlib.Path):
    """Writing several Pathway tables into different tables of the *same* DuckDB
    database file in one run must keep all of them. Each writer shares the single
    database instance for that file, so no writer's tables are clobbered when
    another writer's connection is closed."""
    db_path = str(tmp_path / "multi.duckdb")

    class A(pw.Schema):
        x: int

    class B(pw.Schema):
        y: str

    alpha = pw.debug.table_from_rows(A, [(1,), (2,), (3,)])
    beta = pw.debug.table_from_rows(B, [("hello",), ("world",)])
    pw.io.duckdb.write(
        alpha, table_name="alpha", database=db_path, init_mode="create_if_not_exists"
    )
    pw.io.duckdb.write(
        beta, table_name="beta", database=db_path, init_mode="create_if_not_exists"
    )
    run()

    con = _connect(db_path)
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    assert {"alpha", "beta"} <= tables
    assert con.execute("SELECT count(*) FROM alpha").fetchone()[0] == 3
    assert con.execute("SELECT count(*) FROM beta").fetchone()[0] == 2


@only_with_license_key
def test_init_mode_replace_recreates_table(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE t (value BIGINT, time BIGINT, diff SMALLINT, junk VARCHAR)"
    )
    con.execute("INSERT INTO t VALUES (999, 0, 1, 'stale')")
    con.close()

    table = pw.debug.table_from_markdown(
        """
        value
        7
        """
    )
    pw.io.duckdb.write(table, table_name="t", database=db_path, init_mode="replace")
    run()

    con = _connect(db_path)
    assert con.execute("SELECT value, diff FROM t").fetchall() == [(7, 1)]
    columns = [
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 't'"
        ).fetchall()
    ]
    assert "junk" not in columns


# --------------------------------------------------------------------------- #
# Destination guardrails (Rust-side preflight)
# --------------------------------------------------------------------------- #


@only_with_license_key
def test_write_rejects_case_insensitive_column_collision(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")

    class S(pw.Schema):
        A: int
        a: int

    table = pw.debug.table_from_rows(S, [(1, 2)])
    with pytest.raises(ValueError, match="differ only in case"):
        pw.io.duckdb.write(
            table, table_name="t", database=db_path, init_mode="create_if_not_exists"
        )


@only_with_license_key
def test_write_rejects_view(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE VIEW t AS SELECT 1 AS value, 2 AS time, 3 AS diff")
    con.close()

    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    pw.io.duckdb.write(table, table_name="t", database=db_path)
    with pytest.raises(Exception, match="view"):
        run()


@only_with_license_key
def test_write_rejects_missing_column(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    con = duckdb.connect(db_path)
    # Missing the `time` / `diff` columns the stream-of-changes writer needs.
    con.execute("CREATE TABLE t (value BIGINT)")
    con.close()

    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    pw.io.duckdb.write(table, table_name="t", database=db_path)
    with pytest.raises(Exception, match="missing required column"):
        run()


@only_with_license_key
def test_write_rejects_optional_on_not_null_column(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE t (value BIGINT NOT NULL, time BIGINT, diff SMALLINT)")
    con.close()

    class S(pw.Schema):
        value: int | None

    table = pw.debug.table_from_rows(S, [(1,)])
    pw.io.duckdb.write(table, table_name="t", database=db_path)
    with pytest.raises(Exception, match="Optional"):
        run()


@only_with_license_key
def test_write_rejects_extra_not_null_column(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "out.duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE t (value BIGINT, extra VARCHAR NOT NULL, time BIGINT, diff SMALLINT)"
    )
    con.close()

    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    pw.io.duckdb.write(table, table_name="t", database=db_path)
    with pytest.raises(Exception, match="NOT NULL"):
        run()


@only_with_license_key
def test_snapshot_rejects_destination_without_unique_constraint(tmp_path: pathlib.Path):
    """Snapshot mode upserts with ``INSERT ... ON CONFLICT (primary_key)``, which
    DuckDB only accepts when the destination table has a matching PRIMARY KEY or
    UNIQUE constraint. If the user points snapshot mode at a pre-existing table
    that has the right columns but no such constraint (``init_mode="default"``),
    that must be reported as a clear error naming the primary key at start-up,
    rather than an opaque binder error on the first upsert."""
    db_path = str(tmp_path / "snap.duckdb")
    con = duckdb.connect(db_path)
    # Right columns, but no PRIMARY KEY / UNIQUE constraint on `key`.
    con.execute("CREATE TABLE state (key VARCHAR, value BIGINT)")
    con.close()

    table = pw.debug.table_from_markdown(
        """
          | key | value | __time__ | __diff__
        1 | a   | 1     | 2        | 1
        """
    )
    pw.io.duckdb.write(
        table,
        table_name="state",
        database=db_path,
        output_table_type="snapshot",
        primary_key=[table.key],
        init_mode="default",
    )
    # The message must name the destination table and the missing constraint —
    # the raw DuckDB binder error mentions neither, so matching the quoted table
    # name distinguishes the clear preflight error from the opaque one.
    with pytest.raises(Exception, match=r'"state".*constraint'):
        run()


# --------------------------------------------------------------------------- #
# Rich types & RAG vectors
# --------------------------------------------------------------------------- #


@only_with_license_key
def test_rich_type_roundtrip(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "rich.duckdb")

    class S(pw.Schema):
        i: int
        f: float
        s: str
        b: bytes
        ts: pw.DateTimeNaive
        flag: bool

    table = pw.debug.table_from_rows(
        S,
        [
            (
                7,
                1.5,
                "hello",
                b"\x00\x01",
                pw.DateTimeNaive(year=2020, month=1, day=1, hour=12),
                True,
            )
        ],
    )
    pw.io.duckdb.write(
        table, table_name="rich", database=db_path, init_mode="create_if_not_exists"
    )
    run()

    con = _connect(db_path)
    row = con.execute("SELECT i, f, s, b, ts, flag FROM rich").fetchone()
    assert row[0] == 7
    assert row[1] == 1.5
    assert row[2] == "hello"
    assert row[3] == b"\x00\x01"
    assert row[4] == datetime.datetime(2020, 1, 1, 12, 0)
    assert row[5] is True


@only_with_license_key
def test_vector_search_for_rag(tmp_path: pathlib.Path):
    """Store embeddings and confirm they are usable for RAG retrieval."""
    db_path = str(tmp_path / "rag.duckdb")

    class DocSchema(pw.Schema):
        text: str
        embedding: np.ndarray

    docs = pw.debug.table_from_rows(
        DocSchema,
        [
            ("a cat sat on a mat", np.array([1.0, 0.0, 0.0])),
            ("a dog in the yard", np.array([0.0, 1.0, 0.0])),
            ("an automobile on the road", np.array([0.0, 0.0, 1.0])),
        ],
    )
    pw.io.duckdb.write(
        docs,
        table_name="documents",
        database=db_path,
        init_mode="create_if_not_exists",
    )
    run()

    con = _connect(db_path)
    # The embedding column must be a list usable by DuckDB's vector functions.
    assert con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'documents' AND column_name = 'embedding'"
    ).fetchone()[0] in ("DOUBLE[]", "LIST")

    query = [0.95, 0.05, 0.0]
    best = con.execute(
        "SELECT text FROM documents WHERE diff = 1 "
        "ORDER BY list_cosine_similarity(embedding, ?::DOUBLE[]) DESC LIMIT 1",
        [query],
    ).fetchone()
    assert best[0] == "a cat sat on a mat"


@only_with_license_key
def test_write_rejects_unsupported_list_element_type(tmp_path: pathlib.Path):
    """A list column whose element type the connector cannot encode (here
    ``list[bytes]``) must be rejected with a clear error that names the offending
    column at start-up, rather than building the table and then failing mid-write
    with an opaque "unsupported type" error once data starts flowing."""
    db_path = str(tmp_path / "out.duckdb")

    class S(pw.Schema):
        payloads: list[bytes]

    table = pw.debug.table_from_rows(S, [([b"\x00\x01", b"\x02"],)])
    pw.io.duckdb.write(
        table, table_name="t", database=db_path, init_mode="create_if_not_exists"
    )
    with pytest.raises(Exception, match="payloads"):
        run()


@only_with_license_key
def test_write_rejects_unsupported_list_element_type_datetime(tmp_path: pathlib.Path):
    """Same guardrail for a ``list`` of datetimes — the element type is not part
    of the JSON encoding the connector binds list columns as, so it must be
    rejected up front with a message naming the column."""
    db_path = str(tmp_path / "out.duckdb")

    class S(pw.Schema):
        stamps: list[pw.DateTimeNaive]

    table = pw.debug.table_from_rows(
        S, [([pw.DateTimeNaive(year=2020, month=1, day=1)],)]
    )
    pw.io.duckdb.write(
        table, table_name="t", database=db_path, init_mode="create_if_not_exists"
    )
    with pytest.raises(Exception, match="stamps"):
        run()


@only_with_license_key
def test_list_float_column_preserves_non_finite(tmp_path: pathlib.Path):
    """Non-finite floats (``NaN`` / ``+Inf`` / ``-Inf``) inside a ``list[float]``
    column must be stored faithfully as DuckDB's native ``nan`` / ``inf`` doubles,
    not silently coerced to ``NULL``. DuckDB's ``DOUBLE`` type represents these
    values natively, so a vector column must round-trip them."""
    db_path = str(tmp_path / "nf.duckdb")

    class S(pw.Schema):
        emb: list[float]

    table = pw.debug.table_from_rows(
        S,
        [([1.0, float("nan"), float("inf"), float("-inf")],)],
    )
    pw.io.duckdb.write(
        table, table_name="t", database=db_path, init_mode="create_if_not_exists"
    )
    run()

    con = _connect(db_path)
    flags = con.execute(
        "SELECT emb[1], isnan(emb[2]), isinf(emb[3]), emb[3] > 0, "
        "isinf(emb[4]), emb[4] < 0 FROM t"
    ).fetchone()
    assert flags == (1.0, True, True, True, True, True)


@only_with_license_key
def test_multidim_array_with_explicit_type_roundtrips(tmp_path: pathlib.Path):
    """A 2-D array column whose dimensionality is declared in the schema is stored
    as a nested ``DOUBLE[][]`` list and round-trips."""
    from pathway.internals import dtype as dt

    db_path = str(tmp_path / "m.duckdb")
    schema = pw.schema_builder({"m": pw.column_definition(dtype=dt.Array(2, dt.FLOAT))})
    table = pw.debug.table_from_rows(schema, [(np.array([[1.0, 2.0], [3.0, 4.0]]),)])
    pw.io.duckdb.write(
        table, table_name="t", database=db_path, init_mode="create_if_not_exists"
    )
    run()

    con = _connect(db_path)
    assert con.execute("SELECT m FROM t").fetchone()[0] == [[1.0, 2.0], [3.0, 4.0]]


@only_with_license_key
def test_array_dimensionality_mismatch_is_clear_error(tmp_path: pathlib.Path):
    """An untyped ``np.ndarray`` column is created as a 1-D ``DOUBLE[]`` list (its
    dimensionality is unknown at table-creation time). A value with more
    dimensions cannot be cast into it; that must surface as a clear error naming
    the dimensionality mismatch, not an opaque DuckDB conversion error."""
    db_path = str(tmp_path / "m.duckdb")

    class S(pw.Schema):
        m: np.ndarray  # dimensionality unspecified -> assumed 1-D

    table = pw.debug.table_from_rows(S, [(np.array([[1.0, 2.0], [3.0, 4.0]]),)])
    pw.io.duckdb.write(
        table, table_name="t", database=db_path, init_mode="create_if_not_exists"
    )
    with pytest.raises(Exception, match="(?i)dimension"):
        run()


@only_with_license_key
def test_numpy_array_preserves_non_finite(tmp_path: pathlib.Path):
    """Same guarantee for embeddings supplied as ``numpy`` arrays, which take the
    ``FloatArray`` encoding path."""
    db_path = str(tmp_path / "nf2.duckdb")

    class DocSchema(pw.Schema):
        embedding: np.ndarray

    docs = pw.debug.table_from_rows(
        DocSchema,
        [(np.array([float("nan"), 2.0, float("inf")]),)],
    )
    pw.io.duckdb.write(
        docs, table_name="t", database=db_path, init_mode="create_if_not_exists"
    )
    run()

    con = _connect(db_path)
    flags = con.execute(
        "SELECT isnan(embedding[1]), embedding[2], isinf(embedding[3]) FROM t"
    ).fetchone()
    assert flags == (True, 2.0, True)


@only_with_license_key
def test_list_float_column(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "vec.duckdb")

    class S(pw.Schema):
        text: str
        embedding: list[float]

    docs = pw.debug.table_from_rows(
        S,
        [("a", [1.0, 2.0, 3.0]), ("b", [4.0, 5.0, 6.0])],
    )
    pw.io.duckdb.write(
        docs, table_name="vecs", database=db_path, init_mode="create_if_not_exists"
    )
    run()

    con = _connect(db_path)
    rows = con.execute("SELECT text, embedding FROM vecs ORDER BY text").fetchall()
    assert rows == [("a", [1.0, 2.0, 3.0]), ("b", [4.0, 5.0, 6.0])]


# --------------------------------------------------------------------------- #
# detach_between_batches
# --------------------------------------------------------------------------- #


def _read_only_row_count(db_path: str, table_name: str) -> int | None:
    """One read-only probe of the database, the way an external serving process
    would query it: a short-lived ``read_only`` connection. Returns the row
    count, or ``None`` when the probe loses the race for the file lock (or the
    file/table does not exist yet) — callers retry."""
    try:
        con = duckdb.connect(db_path, read_only=True)
    except duckdb.Error:
        return None
    try:
        return con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    except duckdb.Error:
        return None
    finally:
        con.close()


@only_with_license_key
def test_detach_rejects_in_memory_database():
    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    with pytest.raises(ValueError, match="in-memory"):
        pw.io.duckdb.write(
            table,
            table_name="t",
            database=":memory:",
            detach_between_batches=True,
        )


@only_with_license_key
def test_detach_with_replace_accumulates_across_batches(tmp_path: pathlib.Path):
    """With ``detach_between_batches=True`` the writer closes and reopens the
    database around every flush. ``init_mode="replace"`` must still run only
    once, on the first open — if it re-ran on each reopen, every batch would drop
    and recreate the table, keeping only the last batch's rows.
    ``max_batch_size=1`` forces a flush (and hence a close/reopen cycle) per
    row."""
    db_path = str(tmp_path / "out.duckdb")
    table = pw.debug.table_from_markdown(
        """
          | key | value | __time__
        1 | a   | 1     | 2
        2 | b   | 2     | 4
        3 | c   | 3     | 6
        4 | d   | 4     | 8
        """
    )
    pw.io.duckdb.write(
        table,
        table_name="t",
        database=db_path,
        init_mode="replace",
        detach_between_batches=True,
        max_batch_size=1,
    )
    run()

    con = _connect(db_path)
    rows = con.execute("SELECT key, value FROM t ORDER BY key").fetchall()
    assert rows == [("a", 1), ("b", 2), ("c", 3), ("d", 4)]


@only_with_license_key
def test_detach_snapshot_across_batches(tmp_path: pathlib.Path):
    """Snapshot mode with detaching: upserts and deletions spread across several
    minibatches (each flushed through its own open/close cycle thanks to
    ``max_batch_size=1``) must produce exactly the same final state as the
    non-detaching mode, and ``init_mode="replace"`` must not wipe earlier
    batches."""
    db_path = str(tmp_path / "snap.duckdb")
    table = pw.debug.table_from_markdown(
        """
          | key | value | __time__ | __diff__
        1 | a   | 1     | 2        | 1
        2 | b   | 2     | 2        | 1
        3 | a   | 1     | 4        | -1
        4 | a   | 9     | 4        | 1
        5 | b   | 2     | 6        | -1
        """
    )
    pw.io.duckdb.write(
        table,
        table_name="state",
        database=db_path,
        output_table_type="snapshot",
        primary_key=[table.key],
        init_mode="replace",
        detach_between_batches=True,
        max_batch_size=1,
    )
    run()

    con = _connect(db_path)
    rows = con.execute("SELECT key, value FROM state ORDER BY key").fetchall()
    assert rows == [("a", 9)]


@only_with_license_key
@needs_multiprocessing_fork
def test_detach_allows_concurrent_readonly_reader(tmp_path: pathlib.Path):
    """The headline scenario: while a streaming pipeline with
    ``detach_between_batches=True`` runs in a separate process, this process
    polls the database with short-lived read-only connections (retrying on lock
    contention) and reads committed rows in the gaps between batches. The input
    stream deliberately continues only after a read-only probe has seen the
    first batch, proving the reads happen mid-run, not after the writer is
    done."""
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)
    db_path = str(tmp_path / "out.duckdb")
    first_batch = list(range(5))
    second_batch = list(range(5, 10))

    class InputSchema(pw.Schema):
        value: int

    table = pw.io.jsonlines.read(
        inputs_path,
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.duckdb.write(
        table,
        table_name="t",
        database=db_path,
        init_mode="create_if_not_exists",
        detach_between_batches=True,
    )

    def stream_inputs():
        write_lines(
            inputs_path / "one.jsonl",
            [json.dumps({"value": value}) for value in first_batch],
        )
        # Hold the second batch back until a read-only reader in this (parent)
        # process has actually seen the first one committed.
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if _read_only_row_count(db_path, "t") == len(first_batch):
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                "no read-only connection managed to read the first batch while "
                "the detaching writer was streaming"
            )
        write_lines(
            inputs_path / "two.jsonl",
            [json.dumps({"value": value}) for value in second_batch],
        )

    stream_thread = ExceptionAwareThread(target=stream_inputs, daemon=True)
    stream_thread.start()
    total = len(first_batch) + len(second_batch)
    wait_result_with_checker(
        lambda: _read_only_row_count(db_path, "t") == total,
        30,
    )
    stream_thread.join()


@only_with_license_key
@needs_multiprocessing_fork
def test_default_mode_keeps_file_locked_while_running(tmp_path: pathlib.Path):
    """Guardrail for the default behavior: with ``detach_between_batches=False``
    the writer holds the database open for the whole run, so every read-only
    connection attempt from another process must keep failing with a lock error
    while the pipeline is alive. A regression that silently released the lock in
    default mode would make these probes succeed."""
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)
    db_path = str(tmp_path / "out.duckdb")
    write_lines(
        inputs_path / "one.jsonl",
        [json.dumps({"value": value}) for value in range(5)],
    )

    class InputSchema(pw.Schema):
        value: int

    table = pw.io.jsonlines.read(
        inputs_path,
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=100,
    )
    pw.io.duckdb.write(
        table,
        table_name="t",
        database=db_path,
        init_mode="create_if_not_exists",
    )

    pathway_process = multiprocessing.Process(target=run)
    pathway_process.start()
    try:
        # The writer creates the database file when it opens it, so the file
        # showing up means the (lazily-opened, held) connection exists.
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline and not os.path.exists(db_path):
            time.sleep(0.05)
        assert os.path.exists(db_path), "the writer never opened the database"

        errors = []
        for _ in range(20):
            try:
                con = duckdb.connect(db_path, read_only=True)
                con.close()
                pytest.fail(
                    "a read-only connection succeeded while the default-mode "
                    "writer was supposed to hold the file lock"
                )
            except duckdb.Error as e:
                errors.append(str(e))
            time.sleep(0.1)
        assert any("lock" in error.lower() for error in errors)
    finally:
        pathway_process.terminate()
        pathway_process.join()


@only_with_license_key
def test_two_detach_writers_same_file(tmp_path: pathlib.Path):
    """Two detaching writers targeting the *same* database file in one run must
    share the process-wide database instance: neither clobbers the other's table
    even though both keep dropping and re-acquiring their connections
    (``max_batch_size=1`` forces a cycle per row)."""
    db_path = str(tmp_path / "multi.duckdb")

    alpha = pw.debug.table_from_markdown(
        """
          | x | __time__
        1 | 1 | 2
        2 | 2 | 4
        3 | 3 | 6
        """
    )
    beta = pw.debug.table_from_markdown(
        """
          | y     | __time__
        1 | hello | 2
        2 | world | 4
        """
    )
    pw.io.duckdb.write(
        alpha,
        table_name="alpha",
        database=db_path,
        init_mode="replace",
        detach_between_batches=True,
        max_batch_size=1,
    )
    pw.io.duckdb.write(
        beta,
        table_name="beta",
        database=db_path,
        init_mode="replace",
        detach_between_batches=True,
        max_batch_size=1,
    )
    run()

    con = _connect(db_path)
    assert con.execute("SELECT x FROM alpha ORDER BY x").fetchall() == [
        (1,),
        (2,),
        (3,),
    ]
    assert con.execute("SELECT y FROM beta ORDER BY y").fetchall() == [
        ("hello",),
        ("world",),
    ]


@only_with_license_key
def test_detach_equivalent_to_default_mode(tmp_path: pathlib.Path):
    """The same input written with detaching on and off must produce identical
    final table contents, in both output table types. The pairs run within one
    ``pw.run()``, so even the ``time`` column of the stream-of-changes tables
    must match exactly."""
    table = pw.debug.table_from_markdown(
        """
          | key | value | __time__ | __diff__
        1 | a   | 1     | 2        | 1
        2 | b   | 2     | 2        | 1
        3 | a   | 1     | 4        | -1
        4 | a   | 9     | 4        | 1
        5 | c   | 3     | 6        | 1
        """
    )
    databases = {
        "stream_detach": str(tmp_path / "stream_detach.duckdb"),
        "stream_hold": str(tmp_path / "stream_hold.duckdb"),
        "snapshot_detach": str(tmp_path / "snapshot_detach.duckdb"),
        "snapshot_hold": str(tmp_path / "snapshot_hold.duckdb"),
    }
    for kind, db_path in databases.items():
        snapshot = kind.startswith("snapshot")
        pw.io.duckdb.write(
            table,
            table_name="t",
            database=db_path,
            output_table_type="snapshot" if snapshot else "stream_of_changes",
            primary_key=[table.key] if snapshot else None,
            init_mode="replace",
            detach_between_batches=kind.endswith("detach"),
            max_batch_size=1,
        )
    run()

    def fetch(db_path: str, query: str):
        return _connect(db_path).execute(query).fetchall()

    stream_query = "SELECT key, value, time, diff FROM t ORDER BY time, diff, key"
    assert fetch(databases["stream_detach"], stream_query) == fetch(
        databases["stream_hold"], stream_query
    )
    snapshot_query = "SELECT key, value FROM t ORDER BY key"
    assert fetch(databases["snapshot_detach"], snapshot_query) == fetch(
        databases["snapshot_hold"], snapshot_query
    )


# --------------------------------------------------------------------------- #
# Validation & large volume
# --------------------------------------------------------------------------- #


@only_with_license_key
def test_write_rejects_directory(tmp_path: pathlib.Path):
    table = pw.debug.table_from_markdown(
        """
        value
        1
        """
    )
    with pytest.raises(ValueError, match="existing directory"):
        pw.io.duckdb.write(table, table_name="t", database=str(tmp_path))


@only_with_license_key
def test_large_volume(tmp_path: pathlib.Path):
    db_path = str(tmp_path / "big.duckdb")
    n = 200_000

    class S(pw.Schema):
        idx: int
        value: float

    rows = [(i, float(i) * 0.5) for i in range(n)]
    table = pw.debug.table_from_rows(S, rows)
    pw.io.duckdb.write(
        table,
        table_name="big",
        database=db_path,
        init_mode="create_if_not_exists",
        max_batch_size=10_000,
    )
    run()

    con = _connect(db_path)
    count, total = con.execute("SELECT count(*), sum(value) FROM big").fetchone()
    assert count == n
    assert total == pytest.approx(sum(r[1] for r in rows))

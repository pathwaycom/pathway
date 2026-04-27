# Copyright © 2026 Pathway

import base64
import json
import pathlib
import sqlite3
import threading
from typing import Optional

import numpy as np
import pytest
from dateutil import tz

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    needs_multiprocessing_fork,
    run_all,
    wait_result_with_checker,
)


@needs_multiprocessing_fork
def test_sqlite(tmp_path: pathlib.Path):
    database_name = tmp_path / "test.db"
    output_path = tmp_path / "output.csv"

    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE users (
            id INTEGER,
            login TEXT,
            name TEXT
        )
        """
    )
    cursor.execute("INSERT INTO users (id, login, name) VALUES (1, 'alice', 'Alice')")
    cursor.execute("INSERT INTO users (id, login, name) VALUES (2, 'bob1999', 'Bob')")
    connection.commit()

    def stream_target():
        wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 5, target=None)
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO users (id, login, name) VALUES (3, 'ch123', 'Charlie')"""
        )
        connection.commit()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 3), 2, target=None)
        cursor = connection.cursor()
        cursor.execute("UPDATE users SET name = 'Bob Smith' WHERE id = 2")
        connection.commit()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 5), 2, target=None)
        cursor = connection.cursor()
        cursor.execute("DELETE FROM users WHERE id = 3")
        connection.commit()

    class InputSchema(pw.Schema):
        id: int
        login: str
        name: str

    table = pw.io.sqlite.read(
        database_name, "users", InputSchema, autocommit_duration_ms=1
    )
    pw.io.jsonlines.write(table, output_path)

    inputs_thread = threading.Thread(target=stream_target, daemon=True)
    inputs_thread.start()

    wait_result_with_checker(FileLinesNumberChecker(output_path, 6), 30)

    events = []
    with open(output_path) as f:
        for row in f:
            events.append(json.loads(row))

    events.sort(key=lambda event: (event["time"], event["diff"], event["name"]))
    events_truncated = []
    for event in events:
        events_truncated.append([event["name"], event["diff"]])

    assert events_truncated == [
        ["Alice", 1],
        ["Bob", 1],
        ["Charlie", 1],
        ["Bob", -1],
        ["Bob Smith", 1],
        ["Charlie", -1],
    ]


@needs_multiprocessing_fork
def test_sqlite_read_float_from_integer_column(tmp_path: pathlib.Path):
    """``SQLite`` has no column-level type enforcement: an ``INTEGER``
    value can sit in a ``REAL`` column (and vice versa). When the
    Pathway schema declares the column as ``float``, the reader widens
    such ``INTEGER`` storage to ``f64`` so integer-valued literals in a
    REAL column still parse.
    """
    database_path = tmp_path / "test.db"
    output_path = tmp_path / "output.json"

    connection = sqlite3.connect(database_path)
    connection.execute("CREATE TABLE prices (id INTEGER, price REAL)")
    # `price` is declared REAL, but the second row stores a plain INTEGER
    # literal — which SQLite stores with INTEGER affinity (not widened).
    connection.execute("INSERT INTO prices VALUES (1, 1.5)")
    connection.execute("INSERT INTO prices VALUES (2, 2)")
    connection.commit()
    connection.close()

    class InputSchema(pw.Schema):
        id: int
        price: float

    table = pw.io.sqlite.read(
        database_path, "prices", InputSchema, autocommit_duration_ms=1
    )
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 30)

    with open(output_path) as f:
        rows = sorted((json.loads(line) for line in f), key=lambda r: r["id"])
    assert [(row["id"], row["price"]) for row in rows] == [(1, 1.5), (2, 2.0)]


@needs_multiprocessing_fork
@pytest.mark.parametrize("output_table_type", ["stream_of_changes", "snapshot"])
def test_sqlite_all_types(tmp_path: pathlib.Path, output_table_type):
    """Round-trip a Pathway table whose columns cover every ``Value``
    variant — primitives, datetimes/duration, pointer, json, tuple, list,
    n-d arrays — through ``pw.io.sqlite.write`` → ``pw.io.sqlite.read``
    and assert each field reappears in its expected JSON-lines form.

    Parametrized by ``output_table_type`` so that both ``stream_of_changes``
    (INSERT-only, with appended ``time``/``diff`` columns) and
    ``snapshot`` (UPSERT/DELETE keyed on ``pkey``) exercise every
    storage-class encoding.
    """
    database_name = tmp_path / "test.db"
    output_path = tmp_path / "output.json"

    ptr = api.ref_scalar(42)
    duration_ns = 86_400_000_000_000  # 1 day
    dt_naive_value = pw.DateTimeNaive(
        year=2026, month=1, day=15, hour=10, minute=30, second=45
    )
    dt_utc_value = pw.DateTimeUtc(
        year=2026, month=1, day=15, hour=10, minute=30, second=45, tz=tz.UTC
    )
    json_obj = {"a": 1, "b": "x"}
    bytes_val = b"\x00\x01\xff"
    int_array = np.array([[1, 2, 3], [4, 5, 6]], dtype=int)
    float_array = np.array([1.5, 2.5, 3.5], dtype=float)

    # Build a single-row Pathway table covering every supported type.
    table = pw.debug.table_from_markdown("seed\n1")
    table = table.select(
        pkey=1,
        int_col=42,
        float_col=3.25,
        str_col="hello",
        bytes_col=bytes_val,
        bool_col=True,
        optional_int_col=None,
        dt_naive_col=dt_naive_value,
        dt_utc_col=dt_utc_value,
        duration_col=pw.Duration(days=1),
        json_col=pw.Json.parse(json.dumps(json_obj)),
        pointer_col=ptr,
        tuple_col=("hello", 7),
        list_col=(1, 2, 3),
        int_array_col=int_array,
        float_array_col=float_array,
    )
    table = table.update_types(
        optional_int_col=Optional[int],
        tuple_col=tuple[str, int],
        list_col=list[int],
        int_array_col=np.ndarray[None, int],  # type: ignore
        float_array_col=np.ndarray[None, float],  # type: ignore
    )
    schema = table.schema

    write_kwargs: dict = {"init_mode": "create_if_not_exists"}
    if output_table_type == "snapshot":
        write_kwargs["output_table_type"] = "snapshot"
        write_kwargs["primary_key"] = [table.pkey]
    pw.io.sqlite.write(table, database_name, "all_types", **write_kwargs)
    run_all()
    G.clear()

    # Read the SQLite table back via the streaming reader and dump to jsonlines.
    read_table = pw.io.sqlite.read(
        database_name, "all_types", schema, autocommit_duration_ms=1
    )
    pw.io.jsonlines.write(read_table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 30)

    with open(output_path) as f:
        row = json.loads(f.readline())

    assert row["pkey"] == 1
    assert row["int_col"] == 42
    assert row["float_col"] == 3.25
    assert row["str_col"] == "hello"
    assert row["bytes_col"] == base64.b64encode(bytes_val).decode()
    assert row["bool_col"] is True
    assert row["optional_int_col"] is None
    assert row["dt_naive_col"] == "2026-01-15T10:30:45.000000000"
    assert row["dt_utc_col"] == "2026-01-15T10:30:45.000000000+0000"
    assert row["duration_col"] == duration_ns
    assert row["json_col"] == json_obj
    assert row["pointer_col"] == str(ptr)
    assert row["tuple_col"] == ["hello", 7]
    assert row["list_col"] == [1, 2, 3]
    assert row["int_array_col"] == {
        "shape": list(int_array.shape),
        "elements": int_array.flatten().tolist(),
    }
    assert row["float_array_col"] == {
        "shape": list(float_array.shape),
        "elements": float_array.flatten().tolist(),
    }


@needs_multiprocessing_fork
@pytest.mark.parametrize("output_table_type", ["stream_of_changes", "snapshot"])
def test_sqlite_write_read_round_trip(
    tmp_path: pathlib.Path, serialization_tester, output_table_type
):
    """End-to-end round-trip: write a table covering every Pathway type to
    SQLite via ``pw.io.sqlite.write``, read it back through
    ``pw.io.sqlite.read``, and verify that both paths produce identical
    jsonlines — i.e. the writer/reader pair round-trips every type
    without loss. Exercised in both ``stream_of_changes`` and
    ``snapshot`` output modes.
    """
    database_path = tmp_path / "variety.db"
    direct_path = tmp_path / "direct.jsonl"
    roundtrip_path = tmp_path / "roundtrip.jsonl"

    # Ground truth: write the original table straight to jsonlines.
    table, known_rows = serialization_tester.create_variety_table(with_optionals=False)
    schema = table.schema
    pw.io.jsonlines.write(table, direct_path)
    run_all()
    G.clear()

    # Writer leg: same table, written through the SQLite output connector.
    table, _ = serialization_tester.create_variety_table(with_optionals=False)
    write_kwargs: dict = {"init_mode": "create_if_not_exists"}
    if output_table_type == "snapshot":
        write_kwargs["output_table_type"] = "snapshot"
        write_kwargs["primary_key"] = [table.pkey]
    pw.io.sqlite.write(table, database_path, "variety", **write_kwargs)
    run_all()
    G.clear()

    # Reader leg: read the SQLite table back and re-emit jsonlines.
    read_table = pw.io.sqlite.read(
        database_path, "variety", schema, autocommit_duration_ms=1
    )
    pw.io.jsonlines.write(read_table, roundtrip_path)
    wait_result_with_checker(
        FileLinesNumberChecker(roundtrip_path, len(known_rows)), 30
    )

    def load(path: pathlib.Path) -> list[dict]:
        with open(path) as f:
            rows = [json.loads(line) for line in f]
        # The time/diff metadata differs between runs; compare only the
        # data columns.
        for row in rows:
            row.pop("time", None)
            row.pop("diff", None)
        rows.sort(key=lambda r: r["pkey"])
        return rows

    assert load(direct_path) == load(roundtrip_path)


def test_sqlite_snapshot_write(tmp_path: pathlib.Path):
    """Snapshot-mode writer maintains the current state of the Pathway
    table: insertions UPSERT on the primary key, deletions remove the
    matching row. After the pipeline finishes, the SQLite table
    contains exactly the logically-live rows — earlier values for an
    updated key do not leak, and retracted-only rows are gone.
    """
    database_path = tmp_path / "birds.db"

    def payload(key: int, genus: str, epithet: str) -> bytes:
        return json.dumps({"key": key, "genus": genus, "epithet": epithet}).encode()

    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            # Initial insertions.
            self._add(api.ref_scalar(1), payload(1, "upupa", "epops"))
            self._add(api.ref_scalar(2), payload(2, "bubo", "scandiacus"))
            self._add(api.ref_scalar(3), payload(3, "corvus", "corax"))
            # Update row 1: retract the old version and insert a new one.
            # Snapshot mode should UPSERT on primary_key=key.
            self._remove(api.ref_scalar(1), payload(1, "upupa", "epops"))
            self._add(api.ref_scalar(1), payload(1, "upupa", "marginata"))
            # Delete row 3 outright.
            self._remove(api.ref_scalar(3), payload(3, "corvus", "corax"))

    class InputSchema(pw.Schema):
        key: int
        genus: str
        epithet: str

    table = pw.io.python.read(TestSubject(), schema=InputSchema)
    pw.io.sqlite.write(
        table,
        database_path,
        "birds",
        output_table_type="snapshot",
        primary_key=[table.key],
        init_mode="create_if_not_exists",
    )
    run_all()

    connection = sqlite3.connect(database_path)
    rows = list(
        connection.execute("SELECT key, genus, epithet FROM birds ORDER BY key")
    )
    assert rows == [
        (1, "upupa", "marginata"),
        (2, "bubo", "scandiacus"),
    ]


@pytest.mark.parametrize("init_mode", ["default", "create_if_not_exists", "replace"])
def test_sqlite_write_init_mode(tmp_path: pathlib.Path, init_mode):
    """Each ``init_mode`` must pre-stage the destination table in the
    right shape so that a subsequent write lands in a table with exactly
    the expected row:

    * ``"default"`` — the table must already exist; we pre-create it with
      a value that would be visible if the writer skipped init_mode
      entirely, and assert it survives (single row from the writer, no
      leftover).
    * ``"create_if_not_exists"`` — starting from a missing table, the
      writer creates it and inserts one row.
    * ``"replace"`` — starting from an *existing* table containing a
      different row, the writer drops it and inserts one fresh row; no
      trace of the old row remains.
    """
    database_path = tmp_path / "init_mode.db"
    connection = sqlite3.connect(database_path)
    if init_mode == "default":
        # Writer expects the table to exist; stream_of_changes also
        # requires the time/diff metadata columns.
        connection.execute(
            "CREATE TABLE notes (k INTEGER, v TEXT, time INTEGER, diff INTEGER)"
        )
    elif init_mode == "replace":
        # Writer should drop this pre-existing table and recreate it
        # with a fresh schema derived from the Pathway table.
        connection.execute("CREATE TABLE notes (stale TEXT)")
        connection.execute("INSERT INTO notes VALUES ('old')")
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | hello
        """
    )
    pw.io.sqlite.write(table, database_path, "notes", init_mode=init_mode)
    run_all()

    connection = sqlite3.connect(database_path)
    rows = list(connection.execute("SELECT k, v FROM notes"))
    assert rows == [(1, "hello")]


def test_sqlite_table_name_with_special_characters(tmp_path: pathlib.Path):
    """SQLite identifiers that need quoting — hyphens, spaces, reserved
    words, etc. — are wrapped in double quotes (``"my-table"``) in the
    generated SQL, with embedded ``"`` doubled per SQLite's identifier-
    escaping rules. The writer can target a hyphenated table name and
    the row lands in ``"my-table"`` as a regular SQLite row.
    """
    database_path = tmp_path / "special.db"
    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | hello
        """
    )
    pw.io.sqlite.write(
        table, database_path, "my-table", init_mode="create_if_not_exists"
    )
    run_all()

    connection = sqlite3.connect(database_path)
    rows = list(connection.execute('SELECT k, v FROM "my-table"'))
    assert rows == [(1, "hello")]


@pytest.mark.parametrize("reserved", ["time", "diff", "Time", "DIFF"])
def test_sqlite_write_stream_mode_reserved_column_name_rejected(
    tmp_path: pathlib.Path, reserved
):
    """Stream-of-changes mode unconditionally appends ``time BIGINT`` and
    ``diff SMALLINT`` metadata columns to the destination table so
    downstream consumers can reconstruct the change log. SQLite
    identifier matching is case-insensitive, so a user column whose
    name matches ``time`` or ``diff`` (in any case) would land twice
    in the generated ``CREATE TABLE``. The Python wrapper catches the
    collision at ``write()`` time and raises a clear ``ValueError``
    with remediation guidance. Snapshot mode does not append those
    metadata columns, so the same column name is allowed there.
    """
    database_path = tmp_path / "events.db"
    table = pw.debug.table_from_markdown(
        f"""
        k | {reserved}
        1 | 100
        2 | 200
        """
    )
    with pytest.raises(
        ValueError,
        match=(
            r"collide with the 'time' and 'diff' metadata columns "
            r"appended in stream_of_changes mode"
        ),
    ):
        pw.io.sqlite.write(
            table, database_path, "events", init_mode="create_if_not_exists"
        )


def test_sqlite_write_snapshot_mode_allows_reserved_column_name(
    tmp_path: pathlib.Path,
):
    """Snapshot mode does not add ``time`` / ``diff`` metadata columns,
    so a user column with one of those names must still be allowed.
    """
    database_path = tmp_path / "events.db"
    table = pw.debug.table_from_markdown(
        """
        pkey | time
        1    | 100
        2    | 200
        """
    )
    pw.io.sqlite.write(
        table,
        database_path,
        "events",
        init_mode="create_if_not_exists",
        output_table_type="snapshot",
        primary_key=[table.pkey],
    )
    run_all()
    connection = sqlite3.connect(database_path)
    rows = list(connection.execute("SELECT pkey, time FROM events ORDER BY pkey"))
    assert rows == [(1, 100), (2, 200)]


def test_sqlite_snapshot_rejects_nullable_primary_key(tmp_path: pathlib.Path):
    """Snapshot mode rejects nullable primary-key columns at
    ``write()`` time. Nullable PKs are unsafe in SQLite for snapshot
    semantics: ``INTEGER PRIMARY KEY`` auto-assigns a rowid for
    ``NULL`` inserts (which a later UPSERT can collide with,
    overwriting unrelated rows), and non-integer primary keys let
    ``NULL`` through but then ``DELETE ... WHERE pk = NULL`` never
    matches on retractions. ``pw.io.sqlite.write`` raises a clear
    ``ValueError`` instead of letting either flavor become silent
    data loss downstream.
    """
    database_path = tmp_path / "snapshot.db"

    @pw.udf
    def nullable_pkey(x: int) -> int | None:
        return None if x == 2 else x

    table = pw.debug.table_from_markdown(
        """
        x
        1
        2
        3
        """
    ).select(pkey=nullable_pkey(pw.this.x), v=pw.this.x)

    with pytest.raises(
        ValueError,
        match=(
            r"primary_key column 'pkey' is declared nullable.*"
            r"must be non-nullable in snapshot mode"
        ),
    ):
        pw.io.sqlite.write(
            table,
            database_path,
            "snap",
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.pkey],
        )


def test_sqlite_write_rejects_case_duplicate_column_names(
    tmp_path: pathlib.Path,
):
    """SQLite identifier matching is case-insensitive, so two
    pw.Schema columns that differ only in case (``A`` vs ``a``)
    collapse to the same destination column and would trip
    ``duplicate column name`` on ``CREATE TABLE``. The Python wrapper
    catches the collision at ``write()`` time with a Pathway-authored
    ``ValueError`` so the user sees the conflict at construction
    instead of as a driver error later.
    """
    db = tmp_path / "case_dup.db"

    @pw.udf
    def mk_a(x: int) -> str:
        return f"row-{x}"

    table = pw.debug.table_from_markdown(
        """
        x
        1
        """
    ).select(
        A=pw.this.x,
        a=mk_a(pw.this.x),
    )
    with pytest.raises(
        ValueError,
        match=r"column names that differ only in case",
    ):
        pw.io.sqlite.write(table, db, "t", init_mode="create_if_not_exists")


def test_sqlite_snapshot_rejects_duplicate_primary_key_columns(
    tmp_path: pathlib.Path,
):
    """A ``primary_key`` list must contain each column at most once.
    SQLite happily accepts ``PRIMARY KEY ("k", "k")`` in a ``CREATE
    TABLE``, but the writer's DELETE template would then bind values
    against the duplicated column index and retractions would match
    zero rows. The writer detects duplicated entries and raises a
    clear ``ValueError`` at ``write()`` time.
    """
    db = tmp_path / "dup.db"
    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | hello
        """
    )
    with pytest.raises(
        ValueError,
        match=r"primary_key contains duplicate column\(s\) \['k'\]",
    ):
        pw.io.sqlite.write(
            table,
            db,
            "t",
            init_mode="create_if_not_exists",
            output_table_type="snapshot",
            primary_key=[table.k, table.k],
        )


def test_sqlite_snapshot_composite_primary_key_reverse_order_deletes(
    tmp_path: pathlib.Path,
):
    """Snapshot-mode retractions DELETE the matching destination row
    even when the user-supplied ``primary_key`` order differs from
    the schema's column order. The DELETE-clause column list and its
    bound parameter order are aligned by the shared
    ``SqlQueryTemplate``, so the WHERE clause compares each column
    against the right value regardless of how the user ordered
    ``primary_key``.

    Setup exercises the alignment by deliberately reversing: schema
    is ``(b, a)`` but ``primary_key=[t.a, t.b]``. After inserting a
    row and then retracting it in a later minibatch, the destination
    is empty.
    """
    db = tmp_path / "t.db"

    class S(pw.Schema):
        b: str
        a: str

    class Subject(pw.io.python.ConnectorSubject):
        @property
        def _deletions_enabled(self) -> bool:
            return True

        def run(self):
            row = b'{"b": "B", "a": "A"}'
            self._add(api.ref_scalar(1), row)
            self.commit()  # force the remove into a separate minibatch
            self._remove(api.ref_scalar(1), row)

    t = pw.io.python.read(Subject(), schema=S, autocommit_duration_ms=1)
    pw.io.sqlite.write(
        t,
        db,
        "t",
        init_mode="create_if_not_exists",
        output_table_type="snapshot",
        primary_key=[t.a, t.b],  # reverse of schema order (b, a)
    )
    run_all()

    rows = list(sqlite3.connect(db).execute("SELECT b, a FROM t"))
    assert rows == []


@needs_multiprocessing_fork
def test_sqlite_read_without_rowid_table_with_schema_primary_key(
    tmp_path: pathlib.Path,
):
    """``pw.io.sqlite.read`` uses ``pw.column_definition(primary_key=True)``
    columns from the schema to identify rows instead of the implicit
    ``_rowid_``, so reading from a ``WITHOUT ROWID`` table works — as
    long as the user points to the key via the schema.
    """
    db = tmp_path / "no_rowid.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
    connection.execute("INSERT INTO t VALUES (1, 'hello')")
    connection.execute("INSERT INTO t VALUES (2, 'world')")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = tmp_path / "out.jsonl"
    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 10)

    with open(output_path) as f:
        rows = sorted((json.loads(line) for line in f), key=lambda r: r["k"])
    assert [(r["k"], r["v"]) for r in rows] == [(1, "hello"), (2, "world")]


def test_sqlite_read_schema_referencing_missing_column_errors(
    tmp_path: pathlib.Path,
):
    """When the user's ``pw.Schema`` names a column that doesn't exist
    in the SQLite table, ``SqliteReader::new`` detects the mismatch
    up-front by comparing the schema against ``PRAGMA table_info`` and
    raises a clear error naming the offending column(s) at pipeline-
    start time. The check spares the user the silent-retry loop that
    a polling SELECT would otherwise emit on every poll.
    """
    db = tmp_path / "t.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (k INTEGER, v TEXT)")
    connection.execute("INSERT INTO t VALUES (1, 'hello')")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int
        v: str
        nonexistent: int  # not in SQLite table

    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    output_path = tmp_path / "out.jsonl"
    pw.io.jsonlines.write(table, output_path)
    with pytest.raises(
        Exception,
        match=(
            r"pw\.Schema references column\(s\) \[.*nonexistent.*\] that do "
            r"not exist in SQLite table"
        ),
    ):
        run_all()


@pytest.mark.parametrize(
    "setup_ddl,expected_match",
    [
        pytest.param(
            None,
            r"destination table \"t\" does not exist",
            id="table_does_not_exist",
        ),
        pytest.param(
            "CREATE TABLE t (k INTEGER, other TEXT)",
            r"destination table \"t\" is missing required column\(s\) \[.*v.*\]",
            id="missing_data_column",
        ),
        pytest.param(
            "CREATE TABLE t (k INTEGER, v TEXT)",
            r"destination table \"t\" is missing required column\(s\) \[.*time.*diff.*\]",
            id="missing_time_diff_metadata_columns",
        ),
    ],
)
def test_sqlite_write_default_init_rejects_incompatible_destination(
    tmp_path: pathlib.Path, setup_ddl, expected_match
):
    """``SqliteWriter::new`` verifies the destination exists and
    carries every column the writer will ``INSERT`` into — the
    schema's columns in every mode, plus the ``time`` / ``diff``
    metadata columns in stream-of-changes mode. Each of the three
    variants below surfaces a clean ``SqliteError`` at pipeline-start
    time so the user never has to reason about a raw SQLite driver
    message at flush.
    """
    db = tmp_path / "incompatible.db"
    connection = sqlite3.connect(db)
    if setup_ddl is not None:
        connection.execute(setup_ddl)
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | hello
        """
    )
    pw.io.sqlite.write(table, db, "t", init_mode="default")
    with pytest.raises(Exception, match=expected_match):
        run_all()


@pytest.mark.parametrize(
    "operation,path_kind,expected_match",
    [
        pytest.param(
            "read",
            "non_sqlite_file",
            r"not a valid SQLite 3 database",
            id="read-non-sqlite-file",
        ),
        pytest.param(
            "write",
            "non_sqlite_file",
            r"not a valid SQLite 3 database",
            id="write-non-sqlite-file",
        ),
        pytest.param(
            "read",
            "directory",
            r"is an existing directory, not a SQLite database file",
            id="read-directory",
        ),
        pytest.param(
            "write",
            "directory",
            r"is an existing directory, not a SQLite database file",
            id="write-directory",
        ),
        pytest.param(
            "read",
            "symlink_to_directory",
            r"is an existing directory, not a SQLite database file",
            id="read-symlink-to-directory",
        ),
        pytest.param(
            "write",
            "symlink_to_directory",
            r"is an existing directory, not a SQLite database file",
            id="write-symlink-to-directory",
        ),
    ],
)
def test_sqlite_connector_rejects_non_sqlite_path(
    tmp_path: pathlib.Path, operation, path_kind, expected_match
):
    """SQLite's ``open`` is lazy: it happily accepts any path string and
    only trips on ``SQLITE_NOTADB`` (or a raw ``disk I/O error`` /
    ``unable to open database file``) the first time you query. Both
    connectors run preflight checks so the user gets a clean Pathway-
    level error at construction instead of an engine-worker panic at
    flush time. Cases:

    * ``non_sqlite_file`` — file with arbitrary non-database bytes;
      caught by the ``PRAGMA schema_version`` preflight that surfaces
      ``SQLITE_NOTADB`` as ``not a valid SQLite 3 database``.
    * ``directory`` — path resolves to an existing directory; SQLite
      cannot open a directory as a database. The Python wrapper's
      ``_reject_directory_path`` preflight catches it before the Rust
      side opens the connection (otherwise the user would see a raw
      ``disk I/O error`` for read or ``unable to open database file``
      for write).
    * ``symlink_to_directory`` — same as above; ``os.path.isdir``
      follows symlinks, so the preflight catches both shapes.
    """
    if path_kind == "non_sqlite_file":
        db = tmp_path / "not_sqlite.db"
        db.write_bytes(b"this is not a sqlite database, just some bytes" * 100)
    elif path_kind == "directory":
        db = tmp_path / "is_a_directory.db"
        db.mkdir()
    elif path_kind == "symlink_to_directory":
        target_dir = tmp_path / "real_dir"
        target_dir.mkdir()
        db = tmp_path / "sym_to_dir.db"
        db.symlink_to(target_dir)
    else:
        raise AssertionError(path_kind)

    class S(pw.Schema):
        k: int
        v: str

    with pytest.raises(Exception, match=expected_match):
        if operation == "read":
            table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
            out = tmp_path / "out.jsonl"
            pw.io.jsonlines.write(table, out)
        else:
            table = pw.debug.table_from_markdown("k | v\n1 | hello")
            pw.io.sqlite.write(table, db, "t", init_mode="default")
        run_all()


def test_sqlite_write_rejects_generated_column_in_schema(
    tmp_path: pathlib.Path,
):
    """Writing into a table whose schema includes a
    ``GENERATED ALWAYS AS (...)`` column that also appears in the
    user's ``pw.Schema`` is rejected at pipeline-start time. SQLite
    does not allow ``INSERT`` / ``UPDATE`` against a generated column
    (virtual or stored), so the writer detects the situation via
    ``PRAGMA table_xinfo`` (which exposes generated columns with the
    ``hidden`` field set, unlike ``PRAGMA table_info`` which hides
    them) and raises a Pathway-level error naming the column and the
    actionable fix: drop it from the schema. The reader still returns
    the computed value on ``SELECT``.
    """
    db = tmp_path / "generated.db"
    connection = sqlite3.connect(db)
    connection.execute(
        "CREATE TABLE t ("
        "  k INTEGER PRIMARY KEY,"
        "  v TEXT,"
        "  v_upper TEXT GENERATED ALWAYS AS (upper(v)) STORED,"
        "  time INTEGER,"
        "  diff INTEGER"
        ")"
    )
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown(
        """
        k | v     | v_upper
        1 | hello | HELLO
        """
    )
    pw.io.sqlite.write(table, db, "t", init_mode="default")
    with pytest.raises(
        Exception,
        match=(
            r"destination table \"t\" has generated column\(s\) \[.*v_upper.*\] "
            r"\(`GENERATED ALWAYS AS \.\.\.`\) that pw\.Schema also names"
        ),
    ):
        run_all()


@pytest.mark.parametrize(
    "setup_ddl, user_markdown, missing_col",
    [
        pytest.param(
            "CREATE TABLE t ("
            "  k INTEGER PRIMARY KEY,"
            "  v TEXT,"
            "  time INTEGER,"
            "  diff INTEGER,"
            "  audit_user TEXT NOT NULL"
            ")",
            "k | v\n1 | hello",
            "audit_user",
            id="extra_notnull_text_column",
        ),
        pytest.param(
            "CREATE TABLE t ("
            "  k INTEGER PRIMARY KEY,"
            "  v TEXT,"
            "  time INTEGER, diff INTEGER"
            ") WITHOUT ROWID",
            "v\nhello",
            "k",
            id="integer_primary_key_in_without_rowid",
        ),
        pytest.param(
            "CREATE TABLE t ("
            "  a INTEGER NOT NULL,"
            "  b INTEGER,"
            "  c TEXT,"
            "  time INTEGER, diff INTEGER,"
            "  PRIMARY KEY (a, b)"
            ")",
            "c\nhello",
            "a",
            id="composite_integer_primary_key_first_column",
        ),
    ],
)
def test_sqlite_write_rejects_destination_missing_required_value(
    tmp_path: pathlib.Path, setup_ddl, user_markdown, missing_col
):
    """Writer preflight's ``DestinationMissingRequiredValue`` path
    fires when the destination table has a column that is ``NOT
    NULL``, has no ``DEFAULT``, is not the ``INTEGER PRIMARY KEY``
    rowid alias, and is not named in ``pw.Schema`` — every INSERT
    would otherwise trip ``NOT NULL constraint failed`` at flush.
    Three shapes collapse to the same clean error at construction:

    * ``extra_notnull_text_column`` — a plain unrelated NOT NULL
      column (the canonical case).
    * ``integer_primary_key_in_without_rowid`` — ``INTEGER PRIMARY
      KEY`` in a ``WITHOUT ROWID`` table is NOT the rowid alias
      (``WITHOUT ROWID`` tables have no rowid), so the column must
      be supplied. The preflight tells these apart by checking for an
      ``origin='pk'`` index in ``PRAGMA index_list`` (present for
      ``WITHOUT ROWID``, absent for the single-column INTEGER PK
      rowid alias).
    * ``composite_integer_primary_key_first_column`` — the rowid
      alias only exists for a *single*-column INTEGER PRIMARY KEY,
      so the first column of a composite INTEGER PK is just a
      regular NOT NULL column. Detected by counting PK columns.
    """
    db = tmp_path / "missing_required.db"
    connection = sqlite3.connect(db)
    connection.execute(setup_ddl)
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown(user_markdown)
    pw.io.sqlite.write(table, db, "t", init_mode="default")
    with pytest.raises(
        Exception,
        match=(
            rf"destination table \"t\" has NOT NULL column\(s\) "
            rf"\[.*{missing_col}.*\] without a DEFAULT value"
        ),
    ):
        run_all()


def test_sqlite_write_rejects_optional_on_not_null_destination(
    tmp_path: pathlib.Path,
):
    """When ``pw.Schema`` declares a column ``Optional[T]`` but the
    destination table has that column as ``NOT NULL`` without an
    auto-assigned rowid alias, the preflight surfaces a clean
    Pathway-level error at construction. The check compares the
    pw.Schema Optional annotation against the destination column's
    nullability — any ``None`` Pathway emits would bind ``NULL`` and
    trip ``NOT NULL constraint failed`` at flush. Unlike
    ``DestinationMissingRequiredValue``, the column IS in the
    writer's INSERT list — it's the nullability that's misaligned,
    not the presence.
    """
    db = tmp_path / "opt_mismatch.db"
    connection = sqlite3.connect(db)
    connection.execute(
        "CREATE TABLE t ("
        "  k INTEGER PRIMARY KEY,"
        "  v TEXT NOT NULL,"
        "  time INTEGER, diff INTEGER"
        ")"
    )
    connection.commit()
    connection.close()

    @pw.udf
    def maybe_null(x: int) -> str | None:
        return None if x == 2 else "hello"

    table = pw.debug.table_from_markdown(
        """
        k
        1
        2
        """
    ).select(k=pw.this.k, v=maybe_null(pw.this.k))

    pw.io.sqlite.write(table, db, "t", init_mode="default")
    with pytest.raises(
        Exception,
        match=(
            r"pw\.Schema declares column\(s\) \[.*v.*\] as Optional but "
            r"the destination table \"t\" marks them NOT NULL"
        ),
    ):
        run_all()


@pytest.mark.parametrize(
    "index_ddl",
    [
        pytest.param(
            "CREATE UNIQUE INDEX t_k ON t(k) WHERE v IS NOT NULL",
            id="partial_unique_index",
        ),
        pytest.param(
            "CREATE UNIQUE INDEX t_lower_v ON t (lower(v))",
            id="functional_unique_index",
        ),
    ],
)
def test_sqlite_snapshot_write_pk_mismatch_on_non_matching_unique_index(
    tmp_path: pathlib.Path, index_ddl
):
    """Snapshot-mode writer's PK-match preflight rejects unique
    indexes that cannot serve as an ``ON CONFLICT`` target, matching
    SQLite's own restrictions:

    * Partial indexes (``CREATE UNIQUE INDEX ... WHERE ...``) only
      enforce uniqueness over a subset of rows, so SQLite refuses
      ``ON CONFLICT`` against them. The preflight filters on the
      ``partial`` flag exposed by ``PRAGMA index_list``.
    * Functional / expression-based indexes (``ON t(expr)``) report
      ``name = NULL`` per expression entry in ``PRAGMA index_info``.
      The preflight skips those NULL entries, so the index collapses
      to an empty column set that never matches a non-empty
      ``primary_key``.

    Either shape produces the same clean Pathway-authored
    ``WriterPrimaryKeyMismatch`` error with actionable guidance to
    add a proper full ``UNIQUE`` / ``PRIMARY KEY`` or switch init
    modes.
    """
    db = tmp_path / "unique_index_variant.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (k INTEGER, v TEXT)")
    connection.execute(index_ddl)
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | hello
        """
    )
    pw.io.sqlite.write(
        table,
        db,
        "t",
        init_mode="default",
        output_table_type="snapshot",
        primary_key=[table.k],
    )
    with pytest.raises(
        Exception,
        match=r"do not correspond to any UNIQUE or PRIMARY KEY constraint",
    ):
        run_all()


@pytest.mark.parametrize(
    "setup_ddl, target_name, error_regex",
    [
        pytest.param(
            [
                "CREATE TABLE base (k INTEGER PRIMARY KEY, v TEXT, "
                "time INTEGER, diff INTEGER)",
                "CREATE VIEW vw AS SELECT * FROM base",
            ],
            "vw",
            r"destination \"vw\" is a SQLite view, which SQLite treats as read-only",
            id="view-same-case",
        ),
        pytest.param(
            [
                "CREATE TABLE base (k INTEGER PRIMARY KEY, v TEXT, "
                "time INTEGER, diff INTEGER)",
                "CREATE VIEW vw AS SELECT * FROM base",
            ],
            "VW",
            r"destination \"VW\" is a SQLite view",
            id="view-mixed-case",
        ),
        pytest.param(
            [
                "CREATE TABLE users (k INTEGER PRIMARY KEY)",
                "CREATE INDEX my_obj ON users(k)",
            ],
            "my_obj",
            r"destination \"my_obj\" is a SQLite index in the database, not a table",
            id="index-same-case",
        ),
        pytest.param(
            [
                "CREATE TABLE users (k INTEGER PRIMARY KEY)",
                "CREATE INDEX my_obj ON users(k)",
            ],
            "MY_OBJ",
            r"destination \"MY_OBJ\" is a SQLite index in the database",
            id="index-mixed-case",
        ),
        pytest.param(
            [
                "CREATE TABLE users (k INTEGER PRIMARY KEY, v TEXT)",
                "CREATE TRIGGER my_obj BEFORE INSERT ON users BEGIN SELECT 1; END",
            ],
            "my_obj",
            r"destination \"my_obj\" is a SQLite trigger in the database, not a table",
            id="trigger-same-case",
        ),
        pytest.param(
            [
                "CREATE TABLE users (k INTEGER PRIMARY KEY, v TEXT)",
                "CREATE TRIGGER my_obj BEFORE INSERT ON users BEGIN SELECT 1; END",
            ],
            "My_Obj",
            r"destination \"My_Obj\" is a SQLite trigger in the database",
            id="trigger-mixed-case",
        ),
    ],
)
def test_sqlite_write_rejects_non_table_destination(
    tmp_path: pathlib.Path, setup_ddl, target_name, error_regex
):
    """Writer's ``sqlite_object_kind`` preflight rejects any
    destination name that points at a non-table object (view, index,
    trigger) in ``sqlite_master``, matching the resolution SQLite
    itself would do at flush time. The lookup uses
    ``COLLATE NOCASE`` so the kind check resolves the same way as
    the eventual PRAGMA queries and SQL — a user passing ``"VW"``
    for a view named ``"vw"`` is rejected just like the same-case
    pointing. The user gets a clean Pathway-authored error naming
    the kind (``DestinationIsView`` / ``DestinationIsNotATable``) at
    construction time, regardless of identifier casing.
    """
    db = tmp_path / "non_table_dest.db"
    connection = sqlite3.connect(db)
    for stmt in setup_ddl:
        connection.execute(stmt)
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown("k | v\n1 | hello")
    pw.io.sqlite.write(table, db, target_name, init_mode="create_if_not_exists")
    with pytest.raises(Exception, match=error_regex):
        run_all()


@needs_multiprocessing_fork
def test_sqlite_read_sql92_datetime_format(tmp_path: pathlib.Path):
    """SQLite's ``CURRENT_TIMESTAMP`` default and most of the
    ``datetime()`` / ``date()`` family produce the SQL-92 form
    ``YYYY-MM-DD HH:MM:SS`` with a *space* separator, while
    ``pw.io.sqlite.write`` emits the ISO-8601 form with a ``T``. The
    reader accepts both: a single character at offset 10 — space or
    ``T`` — is normalized to ``T`` before the value reaches the
    shared datetime parser, so a pre-existing SQLite table whose
    timestamps were populated by ``CURRENT_TIMESTAMP`` round-trips
    without a schema change.
    """
    db = tmp_path / "sql92_dt.db"
    connection = sqlite3.connect(db)
    connection.execute(
        "CREATE TABLE t (k INTEGER PRIMARY KEY, "
        "created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
    )
    # Explicit value in the SQL-92 form to pin the format — matches
    # what `CURRENT_TIMESTAMP` would produce.
    connection.execute("INSERT INTO t VALUES (1, '2026-04-23 15:05:00')")
    connection.execute("INSERT INTO t VALUES (2, '2026-04-23 15:05:01.123')")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        created_at: pw.DateTimeNaive

    output_path = tmp_path / "out.jsonl"
    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 10)

    with open(output_path) as f:
        rows = sorted((json.loads(line) for line in f), key=lambda r: r["k"])
    # The reader emits the canonical ISO-8601 form; the *date* and
    # *time* portions of the input must survive regardless of the
    # separator.
    assert rows[0]["k"] == 1
    assert rows[0]["created_at"].startswith("2026-04-23T15:05:00")
    assert rows[1]["k"] == 2
    assert rows[1]["created_at"].startswith("2026-04-23T15:05:01")


@pytest.mark.parametrize("shadow_col", ["_rowid_", "rowid", "oid", "RowID"])
def test_sqlite_read_user_column_shadows_rowid_alias_errors(
    tmp_path: pathlib.Path, shadow_col
):
    """SQLite resolves the implicit integer rowid via the aliases
    ``rowid`` / ``_rowid_`` / ``oid`` (case-insensitively). A user-
    defined column with one of those names shadows the alias —
    ``SELECT _rowid_ ...`` then returns the user column instead of
    the integer rowid. Without a schema-level primary key, the
    reader's preflight detects the shadow up-front and raises a
    clear error at construction time, with the same remediation as
    ``NoRowIdentity``: declare a primary key in ``pw.Schema`` so the
    reader doesn't need to reach for an unreachable rowid.
    """
    db = tmp_path / "rowid_shadow.db"
    connection = sqlite3.connect(db)
    connection.execute(f'CREATE TABLE t ("{shadow_col}" TEXT, v TEXT)')
    connection.execute(
        f'INSERT INTO t ("{shadow_col}", v) VALUES (?, ?)', ("foo", "hello")
    )
    connection.commit()
    connection.close()

    class S(pw.Schema):
        v: str

    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    output_path = tmp_path / "out.jsonl"
    pw.io.jsonlines.write(table, output_path)
    with pytest.raises(
        Exception,
        match=(
            r"shadows SQLite's implicit rowid alias.*"
            r"Declare primary-key columns in your pw\.Schema"
        ),
    ):
        run_all()


def test_sqlite_read_persistence_config_surfaces_clean_error(
    tmp_path: pathlib.Path,
):
    """SQLite has no change-log history to replay, so persistence-
    based resumption of ``pw.io.sqlite.read`` is unsupported. The
    reader's ``seek`` returns ``PersistenceNotSupported`` (matching
    ``PsqlReader``'s behavior), which surfaces as a clear
    ``persistence is not supported for storage 'Sqlite'`` error at
    pipeline-start time. The engine calls ``seek`` on every start
    (even on a fresh run, with an empty frontier), so the error
    fires immediately when ``pw.persistence.Config`` is attached to
    a SQLite-reader pipeline.
    """
    db = tmp_path / "persist_read.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
    connection.execute("INSERT INTO t VALUES (1, 'first')")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.sqlite.read(
        db, "t", S, autocommit_duration_ms=10, name="sqlite_reader"
    )
    pw.io.jsonlines.write(table, tmp_path / "out.jsonl")

    pstorage = tmp_path / "pstorage"
    backend = pw.persistence.Backend.filesystem(str(pstorage))
    config = pw.persistence.Config(backend)
    with pytest.raises(
        Exception,
        match=r"persistence is not supported for storage 'Sqlite'",
    ):
        pw.run(
            persistence_config=config,
            monitoring_level=pw.MonitoringLevel.NONE,
        )


@needs_multiprocessing_fork
def test_sqlite_read_view_with_case_different_target_name(tmp_path: pathlib.Path):
    """The reader trusts a user-declared primary key on a SQLite
    view (which carries no SQLite-side UNIQUE / PRIMARY KEY
    constraints), via ``check_primary_key_match``'s
    ``ObjectHasNoConstraints`` branch. The view-kind lookup in
    ``sqlite_master`` uses ``COLLATE NOCASE`` so a mixed-case target
    name like ``"VW"`` against an actual view ``"vw"`` resolves
    case-insensitively, just like SQLite resolves identifiers
    elsewhere. The reader emits the view's rows with the user-
    declared PK as the row identity.
    """
    db = tmp_path / "view_case_read.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE base (k INTEGER, v TEXT)")
    connection.execute("INSERT INTO base VALUES (1, 'hello')")
    connection.execute("INSERT INTO base VALUES (2, 'world')")
    connection.execute("CREATE VIEW vw AS SELECT k, v FROM base")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = tmp_path / "out.jsonl"
    # Target name is upper-case, view is lower-case — SQLite resolves
    # this case-insensitively and so must the connector's preflight.
    table = pw.io.sqlite.read(db, "VW", S, autocommit_duration_ms=1)
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 10)
    with open(output_path) as f:
        rows = sorted([json.loads(line) for line in f], key=lambda r: r["k"])
    assert [(r["k"], r["v"]) for r in rows] == [(1, "hello"), (2, "world")]


@needs_multiprocessing_fork
def test_sqlite_read_user_column_shadows_rowid_alias_works_with_schema_pk(
    tmp_path: pathlib.Path,
):
    """A column named ``_rowid_`` is fine when the user declares a
    schema-level primary key — the reader doesn't fall back to the
    Rowid path, so the shadow is never consulted. The user's
    ``_rowid_`` column round-trips like any other TEXT value.
    """
    db = tmp_path / "rowid_shadow_with_pk.db"
    connection = sqlite3.connect(db)
    connection.execute('CREATE TABLE t (k INTEGER PRIMARY KEY, "_rowid_" TEXT)')
    connection.execute('INSERT INTO t (k, "_rowid_") VALUES (1, ?)', ("foo",))
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        _rowid_: str

    output_path = tmp_path / "out.jsonl"
    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 10)
    with open(output_path) as f:
        row = json.loads(f.readline())
    assert row["k"] == 1
    assert row["_rowid_"] == "foo"


def test_sqlite_read_without_rowid_table_without_primary_key_errors(
    tmp_path: pathlib.Path,
):
    """When the SQLite object has no ``_rowid_`` (e.g. a
    ``WITHOUT ROWID`` table) and the user's ``pw.Schema`` does not
    declare a primary key, the reader has no way to identify rows
    across polls. ``SqliteReader`` raises a clear ``NoRowIdentity``
    error at pipeline-start time, with remediation pointing the user
    at adding ``pw.column_definition(primary_key=True)`` to the
    schema.
    """
    db = tmp_path / "no_rowid.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
    connection.execute("INSERT INTO t VALUES (1, 'hello')")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int
        v: str

    output_path = tmp_path / "out.jsonl"
    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    pw.io.jsonlines.write(table, output_path)
    with pytest.raises(
        Exception,
        match=(
            r"does not expose an implicit `_rowid_` column.*"
            r"Declare primary key columns in your pw\.Schema"
        ),
    ):
        run_all()


@pytest.mark.parametrize("init_mode", ["default", "create_if_not_exists"])
def test_sqlite_snapshot_write_pk_match_is_case_insensitive(
    tmp_path: pathlib.Path, init_mode
):
    """SQLite identifier matching is case-insensitive: ``ID`` and
    ``id`` resolve to the same column, and SQLite happily accepts
    ``INSERT ... ON CONFLICT (ID) ...`` against a constraint declared
    on ``id``. The writer's PK-match preflight applies the same rule
    by lowercasing column names on both sides of the set comparison,
    so a user-declared ``primary_key=[t.ID]`` matches a SQLite
    UNIQUE/PRIMARY KEY on ``id``. Both ``init_mode="default"`` and
    ``init_mode="create_if_not_exists"`` exercise the same preflight
    when the table already exists.
    """
    db = tmp_path / "case.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (id INTEGER UNIQUE, v TEXT)")
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown(
        """
        ID | v
        1  | hello
        """
    )
    pw.io.sqlite.write(
        table,
        db,
        "t",
        init_mode=init_mode,
        output_table_type="snapshot",
        primary_key=[table.ID],
    )
    run_all()

    rows = list(sqlite3.connect(db).execute("SELECT id, v FROM t ORDER BY id"))
    assert rows == [(1, "hello")]


@needs_multiprocessing_fork
def test_sqlite_read_pk_match_is_case_insensitive(tmp_path: pathlib.Path):
    """Reader mirror of the writer's PK-match case-insensitivity:
    SQLite identifier matching does not distinguish ``ID`` from
    ``id``, so a ``pw.Schema`` whose
    ``ID: int = pw.column_definition(primary_key=True)`` declaration
    differs only in case from the destination's ``UNIQUE`` /
    ``PRIMARY KEY`` constraint is accepted by the reader's preflight
    and the rows flow through with the user's casing.
    """
    db = tmp_path / "case_read.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (id INTEGER UNIQUE, v TEXT)")
    connection.execute("INSERT INTO t VALUES (1, 'hello')")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        ID: int = pw.column_definition(primary_key=True)
        v: str

    output_path = tmp_path / "out.jsonl"
    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    pw.io.jsonlines.write(table, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 10)
    with open(output_path) as f:
        row = json.loads(f.readline())
    assert row["ID"] == 1
    assert row["v"] == "hello"


@pytest.mark.parametrize("init_mode", ["default", "create_if_not_exists"])
def test_sqlite_snapshot_write_rejects_missing_pk_constraint(
    tmp_path: pathlib.Path, init_mode
):
    """When snapshot mode points at a pre-existing table that has no
    UNIQUE / PRIMARY KEY constraint on the requested ``primary_key``
    columns, the writer surfaces a clear ``WriterPrimaryKeyMismatch``
    error at pipeline-start time. Snapshot writes use
    ``INSERT ... ON CONFLICT (...)`` which SQLite refuses without a
    matching constraint.

    Both ``init_mode="default"`` and ``init_mode="create_if_not_exists"``
    run the check: the latter's ``CREATE TABLE IF NOT EXISTS`` is a
    no-op when the table is already there, so a user-maintained
    PK-less table flows into the SQL template just like under
    ``"default"``. ``"replace"`` is exempt because its DROP+CREATE
    always installs the correct constraint.
    """
    db = tmp_path / "preexisting.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE t (k INTEGER, v TEXT)")  # no constraint
    connection.commit()
    connection.close()

    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | hello
        2 | world
        """
    )
    pw.io.sqlite.write(
        table,
        db,
        "t",
        init_mode=init_mode,
        output_table_type="snapshot",
        primary_key=[table.k],
    )
    with pytest.raises(
        Exception,
        match=(
            r"do not correspond to any UNIQUE or PRIMARY KEY constraint.*"
            r"(Add it in SQLite|init_mode=)"
        ),
    ):
        run_all()


@needs_multiprocessing_fork
def test_sqlite_read_schema_primary_key_duplicate_keys_surface_per_row_error(
    tmp_path: pathlib.Path,
):
    """SQLite's ``UNIQUE`` constraint allows multiple ``NULL`` values
    in a unique column (its longstanding historical behavior), so a
    well-formed SQLite table can produce repeated identities from the
    reader's point of view. The reader detects duplicate identities
    within each poll: the first occurrence of a key is tracked
    normally in ``stored_state``, and every subsequent row sharing
    that key is forwarded downstream as a per-row error whose
    primary-key columns carry a ``ConversionError`` naming the
    duplication. The non-duplicate row still lands in the output
    table.

    Setup: one row with ``k=1`` (valid) and two rows with ``k=NULL``
    (both legal under SQLite's ``UNIQUE`` semantics). Only the valid
    row reaches the output sink; the duplicate NULL rows are
    forwarded as per-row errors visible to operators that surface
    them (e.g. ``dump_errors=True`` sinks).
    """
    db = tmp_path / "null_pk.db"
    connection = sqlite3.connect(db)
    # UNIQUE rather than PRIMARY KEY so NULLs are allowed (SQLite's
    # historical quirk) — this is the only shape where the duplicate
    # check is load-bearing, since the PK preflight already rejects
    # tables that lack any matching constraint.
    connection.execute("CREATE TABLE t (k INTEGER UNIQUE, v TEXT)")
    connection.execute("INSERT INTO t VALUES (1, 'alpha')")
    connection.execute("INSERT INTO t VALUES (NULL, 'beta')")
    connection.execute("INSERT INTO t VALUES (NULL, 'gamma')")
    connection.commit()
    connection.close()

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    output_path = tmp_path / "out.jsonl"
    table = pw.io.sqlite.read(db, "t", S, autocommit_duration_ms=1)
    pw.io.jsonlines.write(table, output_path)
    # Exactly one non-duplicate row — `k=1` — makes it into the table.
    # The two NULL-keyed rows are forwarded downstream as per-row
    # errors (visible in logs and engine error handlers) but don't
    # appear in a standard jsonlines sink.
    wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 10)
    with open(output_path) as f:
        rows = [json.loads(line) for line in f]
    assert [(r["k"], r["v"]) for r in rows] == [(1, "alpha")]

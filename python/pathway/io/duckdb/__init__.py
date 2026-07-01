# Copyright © 2026 Pathway

from __future__ import annotations

import os
from os import PathLike, fspath
from typing import Iterable, Literal

from pathway.internals import api, datasink, dtype as dt
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    SNAPSHOT_OUTPUT_TABLE_TYPE,
    get_column_index,
    init_mode_from_str,
)

IN_MEMORY_DATABASE = ":memory:"


def _reject_directory_path(path_str: str) -> None:
    """Reject a ``database`` path that resolves to an existing directory
    (directly or via a symlink). Without this preflight DuckDB fails later
    with an opaque "unable to open database" error inside an engine worker.
    """
    if path_str == IN_MEMORY_DATABASE:
        return
    if os.path.isdir(path_str):
        raise ValueError(
            f"database {path_str!r} is an existing directory, not a DuckDB "
            "database file. DuckDB cannot open a directory as a database; "
            "pass a file path instead."
        )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    *,
    table_name: str,
    database: PathLike | str,
    max_batch_size: int | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    output_table_type: Literal["stream_of_changes", "snapshot"] = "stream_of_changes",
    primary_key: list[ColumnReference] | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table`` into a table of a `DuckDB <https://duckdb.org/>`_ database
    file. DuckDB is an in-process analytical database, so this connector needs no
    external service: data is written natively into a database file. Two output
    table types are supported, selected with ``output_table_type``.

    With ``"stream_of_changes"`` (the default) the output table contains a log of
    every change seen in the Pathway table. Two extra columns, ``time`` (``BIGINT``)
    and ``diff`` (``SMALLINT``), are appended: ``time`` is the minibatch timestamp
    of the change and ``diff`` is ``1`` for an insertion or ``-1`` for a deletion. A
    row modification is therefore expressed as a deletion (``diff = -1``) followed
    by an insertion (``diff = 1``) within the same minibatch. A schema column named
    ``time`` or ``diff`` collides with these metadata columns and is rejected at
    ``write()`` time.

    With ``"snapshot"`` the output table is kept in sync with the current state of
    the Pathway table: ``+1`` events are applied as
    ``INSERT ... ON CONFLICT (primary_key) DO UPDATE`` and ``-1`` events as
    ``DELETE ... WHERE primary_key = ?``, so no ``time`` / ``diff`` columns are
    written. ``primary_key`` is required in this mode (and forbidden otherwise) and
    must list non-nullable columns of ``table``: a ``NULL`` key makes
    ``DELETE ... WHERE primary_key = NULL`` never match on retractions, leaving
    stale rows behind. The destination table must carry a ``PRIMARY KEY`` /
    ``UNIQUE`` constraint on those columns for the upsert to target;
    ``init_mode="create_if_not_exists"`` / ``"replace"`` create it for you.

    ``init_mode`` decides what state the destination table should be in before the
    first write. ``"default"`` requires the table to already exist with matching
    columns (plus ``time`` / ``diff`` in stream-of-changes mode);
    ``"create_if_not_exists"`` creates it if missing, with columns derived from the
    Pathway table (and a ``PRIMARY KEY`` on ``primary_key`` in snapshot mode);
    ``"replace"`` drops and recreates it. Once the connection is opened the
    destination table is validated and a clear error is raised — instead of an
    opaque mid-write failure — if it is a view, is missing (under ``"default"``),
    lacks a column the writer needs (including ``time`` / ``diff`` in
    stream-of-changes mode), has an extra ``NOT NULL`` column without a default, or
    has a ``NOT NULL`` column that an ``Optional`` Pathway column maps onto. Pathway
    column names that differ only in case are rejected at ``write()`` time, since
    DuckDB compares identifiers case-insensitively.

    Column types are mapped onto their natural DuckDB equivalents as described in
    the type-conversion table above. In particular, columns holding embeddings —
    whether typed as ``numpy`` arrays or as ``list[float]`` — are stored as DuckDB
    ``DOUBLE[]`` lists, which can be queried directly with DuckDB's vector-distance
    functions (``list_cosine_similarity``, ``list_distance``,
    ``list_inner_product``); this makes the resulting table usable as a vector store
    for retrieval in a RAG pipeline.

    DuckDB is an embedded, single-file database — a file can be opened read-write by
    only one process at a time. All writes for a single ``pw.io.duckdb.write`` run
    on one worker even when Pathway runs with several workers; DuckDB serializes
    writes to a file regardless, so this costs no throughput while keeping the
    output complete and deterministic.

    Args:
        table: The table to write.
        table_name: The name of the target table in the DuckDB database.
        database: Path to the DuckDB database file. The file is created if it does
            not exist. ``":memory:"`` opens a private in-process database (only
            useful within a single run). If the path resolves to an existing
            directory, a ``ValueError`` is raised at call time.
        max_batch_size: Optional upper bound on the number of rows buffered between
            flushes. Each batch is committed inside a single DuckDB transaction.
        init_mode: How the destination table is initialized before the first
            write, one of ``"default"``, ``"create_if_not_exists"`` or
            ``"replace"`` (see above).
        output_table_type: How the output table represents the data, either
            ``"stream_of_changes"`` (the default) or ``"snapshot"`` (see above).
        primary_key: One or more columns of ``table`` that form the primary key in
            the destination table. Required for snapshot mode and forbidden
            otherwise.
        name: A unique name for the connector. If provided, this name will be used
            in logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based
            on the values of the given columns within each minibatch. When multiple
            columns are provided, the corresponding value tuples will be compared
            lexicographically.

    Returns:
        None

    Examples:

    Consider a stream of changes appended to a DuckDB file. A schema may mix
    arbitrary column types — here an integer, a float, a string and a boolean —
    and the connector adds the ``time`` / ``diff`` columns automatically:

    >>> import pathway as pw
    >>> class MeasurementSchema(pw.Schema):
    ...     sensor: str
    ...     temperature: float
    ...     readings: int
    ...     active: bool
    >>> measurements = pw.debug.table_from_rows(
    ...     MeasurementSchema,
    ...     [("sensor-a", 21.5, 100, True), ("sensor-b", 19.0, 80, False)],
    ... )
    >>> pw.io.duckdb.write(
    ...     measurements,
    ...     table_name="measurements",
    ...     database="./metrics.duckdb",
    ...     init_mode="create_if_not_exists",
    ... )  # doctest: +SKIP
    >>> pw.run()  # doctest: +SKIP

    Alternatively, the destination table can be kept in sync with the current
    state of the Pathway table by using the snapshot output type. A
    ``primary_key`` is required, and — unlike the stream of changes — no ``time``
    / ``diff`` columns are written:

    >>> class AccountSchema(pw.Schema):
    ...     account_id: int
    ...     balance: float
    >>> accounts = pw.debug.table_from_rows(
    ...     AccountSchema,
    ...     [(1, 100.0), (2, 250.0)],
    ... )
    >>> pw.io.duckdb.write(
    ...     accounts,
    ...     table_name="accounts",
    ...     database="./bank.duckdb",
    ...     output_table_type="snapshot",
    ...     primary_key=[accounts.account_id],
    ...     init_mode="create_if_not_exists",
    ... )  # doctest: +SKIP
    >>> pw.run()  # doctest: +SKIP

    The resulting table holds only the data columns — no ``time`` / ``diff``:

    .. code-block:: text

        SELECT * FROM accounts;
        ┌────────────┬─────────┐
        │ account_id │ balance │
        ├────────────┼─────────┤
        │     1      │  100.0  │
        │     2      │  250.0  │
        └────────────┴─────────┘

    As a final example, a table of document chunks together with their embeddings
    is persisted into a DuckDB file for retrieval (RAG). ``list[float]`` columns
    are stored as native ``DOUBLE[]`` lists:

    >>> class DocSchema(pw.Schema):
    ...     text: str
    ...     embedding: list[float]
    >>> docs = pw.debug.table_from_rows(
    ...     DocSchema,
    ...     [("a cat sat on a mat", [1.0, 0.0, 0.0]),
    ...      ("a dog in the yard", [0.0, 1.0, 0.0])],
    ... )
    >>> pw.io.duckdb.write(
    ...     docs,
    ...     table_name="documents",
    ...     database="./docs.duckdb",
    ...     init_mode="create_if_not_exists",
    ... )  # doctest: +SKIP
    >>> pw.run()  # doctest: +SKIP

    Afterwards the embeddings can be searched with plain DuckDB SQL:

    .. code-block:: sql

        SELECT text, list_cosine_similarity(embedding, [1.0, 0.0, 0.0]) AS score
        FROM documents
        WHERE diff = 1
        ORDER BY score DESC
        LIMIT 5;
    """
    _check_entitlements("duckdb")

    is_snapshot_mode = output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE
    if not is_snapshot_mode and primary_key is not None:
        raise ValueError(
            "primary_key can only be specified for the snapshot table type"
        )
    if is_snapshot_mode and not primary_key:
        raise ValueError("primary_key must be specified for the snapshot table type")
    if is_snapshot_mode and sort_by is not None:
        # In snapshot mode an update is a deletion (-1) followed by an insertion
        # (+1) of the same key. sort_by reorders the changes within a minibatch
        # and can place the insertion before the deletion, so the upsert is
        # immediately wiped by the delete — silently losing rows. sort_by has no
        # effect on a snapshot's final (unordered, keyed) state anyway, so reject
        # the combination instead of corrupting the output.
        raise ValueError(
            "sort_by cannot be used with the snapshot table type: a snapshot "
            "reflects the current state of the table regardless of the order "
            "changes are applied within a minibatch, and reordering would corrupt "
            "the upsert/delete sequence. Remove sort_by, or use "
            'output_table_type="stream_of_changes".'
        )

    database_str = fspath(database)
    _reject_directory_path(database_str)

    value_fields = _format_output_value_fields(table)

    # DuckDB matches identifiers case-insensitively, so two schema columns whose
    # names differ only in case would make CREATE TABLE fail with a raw
    # "Column with name ... already exists" catalog error. Surface it here.
    case_groups: dict[str, list[str]] = {}
    for field in value_fields:
        case_groups.setdefault(field.name.lower(), []).append(field.name)
    case_collisions = [
        sorted(names) for names in case_groups.values() if len(names) > 1
    ]
    if case_collisions:
        raise ValueError(
            f"pw.Schema has column names that differ only in case "
            f"({case_collisions}). DuckDB treats identifiers case-insensitively, "
            "so CREATE TABLE would reject them as duplicates. Rename these columns "
            "in the Pathway table so every column name is unique case-insensitively."
        )

    if not is_snapshot_mode:
        # Stream-of-changes mode appends `time` / `diff` metadata columns to the
        # destination table. If the user's own schema already has a column with
        # one of those names, the generated CREATE TABLE would declare that column
        # twice. Catch it here with a clear message instead of an opaque driver
        # error at start-up.
        reserved_metadata_columns = {"time", "diff"}
        collisions = sorted(
            field.name
            for field in value_fields
            if field.name.lower() in reserved_metadata_columns
        )
        if collisions:
            raise ValueError(
                f"Column(s) {collisions} collide with the 'time' and 'diff' "
                "metadata columns appended in stream_of_changes mode. Rename "
                "these columns in the Pathway table, or use "
                'output_table_type="snapshot".'
            )

    key_field_names: list[str] | None = None
    if primary_key is not None:
        # Duplicate entries in `primary_key` produce a nonsensical SQL template
        # and reorder DELETE bindings using the duplicated index, so retractions
        # would silently match no rows. Reject here with a clear message.
        names_seen: set[str] = set()
        duplicates: list[str] = []
        for pkey in primary_key:
            if pkey.name in names_seen and pkey.name not in duplicates:
                duplicates.append(pkey.name)
            names_seen.add(pkey.name)
        if duplicates:
            raise ValueError(
                f"primary_key contains duplicate column(s) {sorted(duplicates)}. "
                "Each column may appear at most once."
            )
        key_field_names = []
        for pkey in primary_key:
            # Raises ValueError when `pkey` belongs to a different table or does
            # not name a column of `table`, so users get a clear message at
            # write() time instead of an opaque runtime error.
            get_column_index(table, pkey)
            # A nullable primary key makes DELETE ... WHERE pk = NULL never match
            # on retractions, so the destination would keep stale rows.
            if isinstance(pkey._column.dtype, dt.Optional):
                raise ValueError(
                    f"primary_key column {pkey.name!r} is declared nullable "
                    f"({pkey._column.dtype}); primary-key columns must be "
                    "non-nullable in snapshot mode."
                )
            # DuckDB cannot build a PRIMARY KEY / index on a list, array, tuple or
            # JSON column, so such a column can never serve as a snapshot primary
            # key — the CREATE TABLE (or the upsert's ON CONFLICT) would fail with
            # an opaque "Invalid type for index key" error mid-run.
            if isinstance(pkey._column.dtype, (dt.List, dt.Array, dt.Tuple)) or (
                pkey._column.dtype == dt.JSON
            ):
                raise ValueError(
                    f"primary_key column {pkey.name!r} has non-scalar type "
                    f"({pkey._column.dtype}); DuckDB cannot index list, array, "
                    "tuple or JSON columns, so they cannot be used as a snapshot "
                    "primary key."
                )
            key_field_names.append(pkey.name)

    data_storage = api.DataStorage(
        storage_type="duckdb",
        path=database_str,
        table_name=table_name,
        table_writer_init_mode=init_mode_from_str(init_mode),
        max_batch_size=max_batch_size,
        snapshot_maintenance_on_output=is_snapshot_mode,
    )
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=key_field_names,
        value_fields=value_fields,
        table_name=table_name,
    )
    datasink_type = "snapshot" if is_snapshot_mode else "sink"
    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name=f"duckdb.{datasink_type}",
            unique_name=name,
            sort_by=sort_by,
        )
    )


__all__ = ["write"]

# Copyright © 2026 Pathway

from __future__ import annotations

import os
from os import PathLike, fspath
from typing import Any, Literal

from pathway.internals import api, datasink, datasource, dtype as dt
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    SNAPSHOT_OUTPUT_TABLE_TYPE,
    get_column_index,
    init_mode_from_str,
    read_schema,
)


def _reject_directory_path(path_str: str) -> None:
    """Reject ``path`` that resolves to an existing directory (directly
    or via symlink). Without this preflight the underlying ``rusqlite``
    call returns the opaque ``disk I/O error`` (read) or ``unable to
    open database file`` (write) at flush time, panicking an engine
    worker. ``os.path.isdir`` follows symlinks, so this also catches a
    symlink-to-directory.
    """
    if os.path.isdir(path_str):
        raise ValueError(
            f"path {path_str!r} is an existing directory, not a SQLite "
            "database file. SQLite cannot open a directory as a database; "
            "pass a file path instead."
        )


@check_arg_types
@trace_user_frame
def read(
    path: PathLike | str,
    table_name: str,
    schema: type[Schema],
    *,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data: Any = None,
) -> Table:
    """Reads a table or view from a `SQLite <https://www.sqlite.org/>`_ database.
    Both rowid tables and ``WITHOUT ROWID`` tables / views are supported —
    the latter require a primary key declared in the Pathway schema
    (see below).

    The reader polls the database for changes and tracks each row by an
    identity so it can emit insertions, updates, and deletions. That
    identity is chosen from the Pathway schema passed as ``schema``:

    * If the Pathway schema declares one or more columns with
      ``pw.column_definition(primary_key=True)``, those columns form the
      identity. The SQLite table must carry a matching ``PRIMARY KEY``
      or ``UNIQUE`` constraint; otherwise the reader would silently
      conflate rows that share a key value, so ``pw.io.sqlite.read``
      rejects the setup up-front. This mode is required for SQLite
      objects that don't expose an implicit row id — e.g. ``WITHOUT
      ROWID`` tables and views; for views, which can't carry
      constraints, the reader trusts the user's declaration.
    * Otherwise, SQLite's implicit ``_rowid_`` column is used.
      ``pw.io.sqlite.read`` raises ``ValueError`` at call time if the
      target object has neither a primary key in the Pathway schema
      nor ``_rowid_``. The same error fires when the target table has
      a user-defined column named ``rowid``, ``_rowid_``, or ``oid``
      (case-insensitive — these are SQLite's rowid aliases): the user
      column shadows the implicit alias, so the reader cannot fetch
      the integer rowid identity. Declare a primary key in the
      Pathway schema to read such tables.

    The column names of the Pathway schema are verified against the
    target SQLite object's columns (via ``PRAGMA table_xinfo``, which
    exposes generated columns too) at connector construction; if the
    Pathway schema declares a column the SQLite table does not carry,
    the reader refuses to start and names the offending column(s) in
    the error. Identifier comparison is ASCII case-insensitive,
    matching SQLite's own rules — e.g. declaring ``ID`` in the Pathway
    schema matches a table column named ``id``.
    The path must point at a valid SQLite 3 database — the connector
    runs ``PRAGMA schema_version`` up-front and surfaces a clear error
    if the file is non-SQLite or encrypted with a key this connection
    doesn't have. Empty files are NOT rejected here, because SQLite
    treats them as brand-new empty databases; in that case the reader
    raises ``ValueError`` because ``table_name`` does not exist in the
    (empty) database.

    Datetime values in ``TEXT`` columns are accepted in either the
    ISO-8601 form the writer emits (``YYYY-MM-DDTHH:MM:SS``, optionally
    followed by ``.``-prefixed fractional seconds, with a ``T``
    separator) or the SQL-92 form SQLite itself produces via
    ``CURRENT_TIMESTAMP`` / ``datetime()`` (``YYYY-MM-DD HH:MM:SS``,
    optionally followed by ``.``-prefixed fractional seconds, with a
    space separator). The reader normalizes the separator before
    parsing so pre-existing SQLite tables round-trip without a schema
    change.

    **Duplicate primary-key values within a single poll.** A matching
    ``UNIQUE`` constraint does not fully rule these out: SQLite permits
    *multiple* ``NULL`` values in a ``UNIQUE`` column (its longstanding
    historical behavior), and rowid-less primary-key declarations on
    regular tables share the same leniency. When the reader sees two or
    more rows with identical primary-key values in one poll, the first
    row is tracked in the usual way and each subsequent duplicate is
    forwarded downstream as a per-row error: the primary-key columns on
    that event are replaced with an error value (the same marker that
    ``pw.fill_error`` and ``pw.global_error_log`` surface) naming the
    duplication, and the row is not added to the reader's tracked
    snapshot. This way a single ``NULL`` (or any otherwise-legal repeat)
    still reaches the table while genuine collisions surface as visible
    parse errors without aborting the pipeline or dropping the rest of
    the batch.

    **Persistence is not supported.** SQLite has no change-log history
    to replay, so a pipeline that uses ``pw.io.sqlite.read`` cannot be
    resumed from a Pathway snapshot. Enabling ``pw.persistence.Config``
    against such a pipeline raises ``ValueError`` at startup.

    Args:
        path: Path to the database file. If the path resolves to an
            existing directory (directly or via a symlink), a
            ``ValueError`` is raised at call time.
        table_name: Name of the table in the database to be read.
            SQLite resolves identifiers case-insensitively, so a
            mixed-case ``table_name`` (e.g. ``"Users"``) matches a
            table created as ``users``.
        schema: Pathway schema. Optionally annotate one or more
            columns with ``pw.column_definition(primary_key=True)`` to
            drive row-identity tracking (see above).
        autocommit_duration_ms: The maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        max_backlog_size: Limit on the number of entries read from the input source and kept
            in processing at any moment. Reading pauses when the limit is reached and resumes
            as processing of some entries completes. Useful with large sources that
            emit an initial burst of data to avoid memory spikes.

    Returns:
        Table: The table read.
    """
    path_str = fspath(path)
    _reject_directory_path(path_str)
    schema, api_schema = read_schema(schema)

    data_storage = api.DataStorage(
        storage_type="sqlite",
        path=path_str,
        table_name=table_name,
        mode=api.ConnectorMode.STREAMING,
    )
    data_format = api.DataFormat(
        format_type="transparent",
        **api_schema,
    )

    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=name,
        max_backlog_size=max_backlog_size,
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            schema=schema,
            data_source_options=data_source_options,
            datasource_name="sqlite",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    path: PathLike | str,
    table_name: str,
    *,
    max_batch_size: int | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    output_table_type: Literal["stream_of_changes", "snapshot"] = "stream_of_changes",
    primary_key: list[ColumnReference] | None = None,
    name: str | None = None,
) -> None:
    """Writes ``table`` to a table in a `SQLite <https://www.sqlite.org/>`_
    database file. Two types of output tables are supported: **stream of
    changes** and **snapshot**.

    When using **stream of changes**, the output table contains a log of
    every change seen in the Pathway table. Two extra ``INTEGER`` columns,
    ``time`` and ``diff``, are appended: ``time`` is the minibatch
    timestamp and ``diff`` is ``1`` for an insertion or ``-1`` for a
    deletion.

    When using **snapshot**, the output table holds the current state of
    the Pathway table. Insertions are emitted as
    ``INSERT ... ON CONFLICT (primary_key) DO UPDATE SET ...`` and
    deletions as ``DELETE ... WHERE primary_key = ?``, so the destination
    table always mirrors the logical contents of the Pathway table.

    Values are encoded using the same storage-class mapping that
    :py:func:`pathway.io.sqlite.read` expects, so a ``write`` / ``read``
    pair is a lossless round-trip for every supported Pathway type.

    Args:
        table: The table to write. Its column names must be unique
            under SQLite's ASCII-case-insensitive identifier rules —
            a pair like ``A`` / ``a`` is rejected at
            ``write()`` time because ``CREATE TABLE`` would treat
            them as the same destination column.
        path: Path to the SQLite database file. The file is created if
            it does not exist. If the path resolves to an existing
            directory (directly or via a symlink), a ``ValueError`` is
            raised at call time.
        table_name: Name of the destination table. SQLite resolves
            identifiers case-insensitively, so a mixed-case name
            (e.g. ``"Users"``) targets a table created as ``users``.
        max_batch_size: Optional upper bound on the number of rows buffered
            between flushes. Each batch is committed inside a single SQLite
            transaction.
        init_mode: Controls how the destination SQLite table is
            initialized. ``"default"`` requires the SQLite table to
            already exist with columns matching the Pathway table;
            ``"create_if_not_exists"`` creates it if missing;
            ``"replace"`` drops and recreates it. See
            `Output Connector: Initialization and Schema Checks` above
            for the full set of compatibility checks.
        output_table_type: Defines how the output table manages its data.
            ``"stream_of_changes"`` (the default) appends every change with
            its ``time`` and ``diff`` metadata. ``"snapshot"`` maintains
            the current state of the table: ``+1`` events become an
            UPSERT and ``-1`` events become a DELETE.
        primary_key: One or more columns of ``table`` that form the
            primary key in the destination SQLite table. Required for
            snapshot mode and forbidden otherwise. See
            `Output Connector: Snapshot Mode` above for the rules on
            nullability and the matching destination constraint.
        name: A unique name for the connector. If provided, this name will
            be used in logs and monitoring dashboards.

    Examples:

    **Stream of changes.** Every event from the Pathway table is appended
    to the destination table together with its ``time`` and ``diff``
    metadata, so the result is a complete log of insertions and deletions.
    The destination table is created on demand when ``init_mode`` allows
    it:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | cat
    ... 8   | Alice | cat
    ... ''')
    >>> pw.io.sqlite.write(
    ...     t,
    ...     "pets.db",
    ...     "pets",
    ...     init_mode="create_if_not_exists",
    ... )

    The resulting ``pets`` table has the original columns plus the two
    ``INTEGER`` columns ``time`` and ``diff`` automatically added by the
    connector.

    **Snapshot.** The destination table is kept in sync with the current
    state of the Pathway table: every ``+1`` event UPSERTs on the primary
    key and every ``-1`` event issues a DELETE against the matching row.
    A primary key must be supplied via ``primary_key``:

    >>> pw.io.sqlite.write(
    ...     t,
    ...     "pets.db",
    ...     "pets_snapshot",
    ...     output_table_type="snapshot",
    ...     primary_key=[t.owner, t.pet],
    ...     init_mode="replace",
    ... )

    Here ``(owner, pet)`` is the primary key, so at any point in time the
    ``pets_snapshot`` table contains one row per live ``(owner, pet)``
    pair — no history, no ``time`` / ``diff`` columns.
    """
    is_snapshot_mode = output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE
    if not is_snapshot_mode and primary_key is not None:
        raise ValueError(
            "primary_key can only be specified for the snapshot table type"
        )
    if is_snapshot_mode and not primary_key:
        raise ValueError("primary_key must be specified for the snapshot table type")

    path_str = fspath(path)
    _reject_directory_path(path_str)

    value_fields = _format_output_value_fields(table)

    # SQLite identifier matching is case-insensitive (`ID` and `id` are
    # the same column), so any pair of schema columns whose names
    # differ only in case would make ``CREATE TABLE`` fail with a raw
    # ``duplicate column name`` driver error at pipeline-start. Surface
    # the collision here with a clear, Pathway-authored message
    # instead, matching how the ``time`` / ``diff`` reserved-name check
    # below works.
    case_groups: dict[str, list[str]] = {}
    for field in value_fields:
        case_groups.setdefault(field.name.lower(), []).append(field.name)
    case_collisions = [
        sorted(names) for names in case_groups.values() if len(names) > 1
    ]
    if case_collisions:
        raise ValueError(
            f"pw.Schema has column names that differ only in case "
            f"({case_collisions}). SQLite treats identifiers "
            "case-insensitively, so CREATE TABLE would reject them as "
            "duplicates. Rename these columns in the Pathway table so "
            "every column name is unique case-insensitively."
        )

    if not is_snapshot_mode:
        # Stream-of-changes mode appends `time` / `diff` metadata columns
        # to the destination table so the output can be replayed as a
        # change log. If the user's own schema already has a column with
        # one of those names, the generated CREATE TABLE would declare
        # that column twice and SQLite would reject it. Catch this at
        # write() time with a clear message instead of letting the user
        # hit an opaque "duplicate column name" error at start-up.
        # Matching is case-insensitive, consistent with SQLite's identifier
        # comparison rules.
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
        # Duplicate entries in `primary_key` produce a nonsensical SQL
        # template — e.g. `PRIMARY KEY ("k", "k")` — which SQLite tolerates,
        # but the shared `SqlQueryTemplate` then reorders DELETE bindings
        # using the duplicated index and retractions silently match no
        # rows. Reject here with a clear message.
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
            # Raises ValueError when `pkey` belongs to a different table
            # or does not name a column of `table`, so users get a clear
            # message at write() time instead of an opaque runtime error.
            get_column_index(table, pkey)
            # Reject nullable primary-key columns. SQLite's
            # INTEGER PRIMARY KEY auto-assigns a rowid for NULL values,
            # which can silently collide with a later UPSERT and
            # overwrite unrelated rows; other PK types let NULLs through
            # but then DELETE ... WHERE pk = NULL never matches on
            # retractions. Neither case is what the user asked for.
            if isinstance(pkey._column.dtype, dt.Optional):
                raise ValueError(
                    f"primary_key column {pkey.name!r} is declared nullable "
                    f"({pkey._column.dtype}); primary-key columns must be "
                    "non-nullable in snapshot mode."
                )
            key_field_names.append(pkey.name)

    data_storage = api.DataStorage(
        storage_type="sqlite",
        path=path_str,
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
            datasink_name=f"sqlite.{datasink_type}",
            unique_name=name,
        )
    )

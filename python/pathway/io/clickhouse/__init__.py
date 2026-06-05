# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Iterable, Literal

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import SNAPSHOT_OUTPUT_TABLE_TYPE, init_mode_from_str


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    *,
    connection_string: str,
    table_name: str,
    output_table_type: Literal["stream_of_changes", "snapshot"] = "stream_of_changes",
    primary_key: Iterable[ColumnReference] | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    max_batch_size: int | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table`` to a ClickHouse table.

    The output table supports two formats, controlled by ``output_table_type``:
    a ``"stream_of_changes"`` format that appends the full history of updates, and
    a ``"snapshot"`` format that maintains the current state of the table.

    In ``"stream_of_changes"`` mode (the default) the output includes every column
    of the input table plus two additional columns: ``time``, which holds the
    Pathway Live Data Framework minibatch time of the change, and ``diff``, which
    describes the type of change (``1`` for a row insertion and ``-1`` for a row
    deletion). A row update is represented as a deletion of the old value followed
    by an insertion of the new one, both within the same minibatch. Because
    ``time`` and ``diff`` are reserved column names in this format, the input table
    must not contain columns with these names; otherwise a ``ValueError`` is raised
    at construction time. Each ``pw.run()`` invocation appends to the table; use
    ``init_mode="replace"`` to recreate it from scratch instead.

    In ``"snapshot"`` mode the connector maintains the current state of the table
    rather than its history. ClickHouse is an insert-only, append-oriented store
    with no synchronous primary-key upsert or delete, so the current state is
    maintained with a `ReplacingMergeTree(version, is_deleted)
    <https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree>`_
    engine ordered by the ``primary_key`` columns, plus two bookkeeping columns
    the connector appends to every row:

    * ``version`` — a 64-bit counter that increases by one with every change, in
      the order changes are produced. It is *not* the minibatch ``time`` used in
      ``"stream_of_changes"`` mode; see below for why a separate counter is
      needed.
    * ``is_deleted`` — ``1`` if the row is a retraction (deletion), ``0`` if it is
      a live state row. This is exactly the ``diff`` of ``"stream_of_changes"``
      mode re-expressed as a flag (``diff = -1`` becomes ``is_deleted = 1``,
      ``diff = 1`` becomes ``is_deleted = 0``).

    Every Pathway change is just appended as a row (no in-place update). What
    turns that append-only stream into a live snapshot is how ``ReplacingMergeTree``
    behaves at the ClickHouse level: when it merges data parts in the background,
    it collapses all rows that share the same ``ORDER BY`` key (here the
    ``primary_key``) down to the single row with the **largest** ``version``, and
    if that surviving row has ``is_deleted = 1`` it is physically dropped. Both
    columns are therefore required, and each does a specific job:

    * ``version`` makes "the newest write wins" deterministic, and is the reason
      this mode needs a dedicated counter rather than reusing ``time``. An update
      to a row is emitted as a retraction of the old value followed by an insertion
      of the new one — both with the same primary key *and the same* minibatch
      ``time`` — so ``time`` cannot tell the engine which one is newer. ``version``
      can: the connector assigns each change the next counter value in production
      order, emitting retractions before insertions within a minibatch, so the
      insertion always gets the higher ``version`` and the merge keeps the new
      values. (The counter only ever has to break ties between rows of the *same*
      primary key; identical ``version`` values across different keys are
      irrelevant, since ``ReplacingMergeTree`` collapses each ``ORDER BY`` key
      independently.) Reusing ``time`` would give the retraction and the insertion
      an equal ``version``, and ``ReplacingMergeTree`` breaks such ties
      arbitrarily — it could keep the retraction and silently drop the update.
    * ``is_deleted`` lets a key be removed at all. Because only inserts are
      possible, a deletion is represented as an appended row with the highest
      ``version`` and ``is_deleted = 1``; the merge then removes that key. Without
      it a deleted key could never leave the table.

    Because merges run asynchronously, query the table with
    `SELECT ... FINAL <https://clickhouse.com/docs/sql-reference/statements/select/from#final-modifier>`_
    to observe the current, deduplicated state on demand — ``FINAL`` applies the
    same keep-largest-version / drop-``is_deleted`` logic at query time. A plain
    ``SELECT`` (without ``FINAL``) may still return superseded or deleted rows
    until a merge has run.

    ``version`` and ``is_deleted`` are reserved column names in this format, so
    the input table must not contain columns with these names. This mode requires
    the ``primary_key`` parameter and runs on a single worker so that the
    ``version`` counter is globally monotonic; on start-up it is seeded above the
    table's current maximum ``version`` so that a restart against an existing
    table keeps producing winning rows.

    Args:
        table: The table to write to ClickHouse.
        connection_string: The connection string for the ClickHouse server, in the
            native-protocol form
            ``"tcp://user:password@host:9000/database"``. Connection options such
            as ``compression`` may be appended as query parameters, e.g.
            ``"tcp://localhost:9000/default?compression=lz4"``.
        table_name: The name of the target table in ClickHouse.
        output_table_type: Either ``"stream_of_changes"`` (the default), which
            appends the full history of changes with ``time``/``diff`` columns, or
            ``"snapshot"``, which maintains the current state of the table in a
            ``ReplacingMergeTree`` keyed by ``primary_key`` (queried with
            ``SELECT ... FINAL``).
        primary_key: The columns identifying each row. Required when
            ``output_table_type="snapshot"`` (they become the table's ``ORDER BY``);
            must not be set otherwise.
        init_mode: Determines how the table is initialized before the first write.
            ``"default"`` (the default) does nothing and requires the table to
            already exist. ``"create_if_not_exists"`` creates the table if it is
            not present. ``"replace"`` drops the table if it exists and recreates
            it. In the two latter modes the table is created with the engine and
            metadata columns for the chosen ``output_table_type``: a ``MergeTree``
            with ``time``/``diff`` columns for ``"stream_of_changes"``, or a
            ``ReplacingMergeTree(version, is_deleted)`` ordered by ``primary_key``
            with ``version``/``is_deleted`` columns for ``"snapshot"``.
            Regardless of ``init_mode``, the connector validates the destination
            table when the computation starts: it must exist and contain every
            output column (the input columns plus the metadata columns) with a
            compatible type, and in ``"snapshot"`` mode it must use the
            ``ReplacingMergeTree`` engine. A missing table, an absent column, an
            incompatible column type, or a wrong engine is reported immediately
            rather than on the first write. ClickHouse inserts perform almost no
            implicit conversion, so a pre-existing column must have exactly the type
            the connector produces for it (a ``String`` column may also be
            ``FixedString(N)``, and a datetime column may be any
            ``DateTime``/``DateTime64`` variant).
        max_batch_size: The maximum number of changes to accumulate before sending
            an insertion block to the server.
        name: A unique name for the connector. If provided, this name will be used
            in logs and monitoring dashboards.
        sort_by: If specified, the output is sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple
            columns are provided, the corresponding value tuples are compared
            lexicographically.

    Returns:
        None

    Example:

    The easiest way to run ClickHouse locally is with Docker. You can use the
    official image and start it like this:

    .. code-block:: bash

        docker pull clickhouse/clickhouse-server
        docker run -d --name clickhouse \\
            -p 8123:8123 -p 9000:9000 \\
            clickhouse/clickhouse-server

    Port ``9000`` is used for the native protocol, which this connector relies on.
    Port ``8123`` exposes the HTTP interface, which is convenient for inspecting
    the data afterwards.

    You can now write a simple program. In this example a table with one column
    called ``"data"`` is created and sent to the database:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ...      | data
    ...    1 | Hello
    ...    2 | World
    ... ''')

    This table can now be written to ClickHouse. If the output table is called
    ``"test"`` and you want the connector to create it for you, the code looks
    like this:

    >>> pw.io.clickhouse.write(
    ...     table,
    ...     connection_string="tcp://localhost:9000/default",
    ...     table_name="test",
    ...     init_mode="create_if_not_exists",
    ... )

    You can run this pipeline with ``pw.run()``.

    Once the program has finished, you can inspect the data with any ClickHouse
    client, for example over the HTTP interface:

    .. code-block:: bash

        curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20test'

    Note that if you run the program again it will append data to the table. To
    recreate the table from scratch, use ``init_mode="replace"``.

    If instead of the history of changes you want to maintain the current state of
    the table, use ``output_table_type="snapshot"`` and provide the column(s) that
    identify each row via ``primary_key``:

    >>> pets = pw.debug.table_from_markdown('''
    ...      | name  | owner
    ...    1 | Cat   | Alice
    ...    2 | Dog   | Bob
    ... ''')
    >>> pw.io.clickhouse.write(
    ...     pets,
    ...     connection_string="tcp://localhost:9000/default",
    ...     table_name="pets",
    ...     output_table_type="snapshot",
    ...     primary_key=[pets.name],
    ...     init_mode="create_if_not_exists",
    ... )

    Because the deduplication happens during background merges, query the snapshot
    with ``FINAL`` to always observe the current state:

    .. code-block:: bash

        curl 'http://localhost:8123/?query=SELECT%20name,owner%20FROM%20pets%20FINAL'
    """
    _check_entitlements("clickhouse")

    is_snapshot_mode = output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE

    column_names = set(table.schema.column_names())
    metadata_columns = (
        {"version", "is_deleted"} if is_snapshot_mode else {"time", "diff"}
    )
    reserved = metadata_columns & column_names
    if reserved:
        raise ValueError(
            f"Column name(s) {sorted(reserved)!r} collide with the reserved "
            f"{sorted(metadata_columns)!r} metadata columns appended by "
            "pw.io.clickhouse.write. Rename the column(s) in your schema before writing."
        )
    key_field_names: list[str] | None = None
    if is_snapshot_mode:
        if primary_key is None or not list(primary_key):
            raise ValueError(
                "output_table_type='snapshot' requires the primary_key parameter: "
                "pass the column(s) that identify each row so the connector can "
                "maintain the current snapshot keyed by them."
            )
        key_field_names = [column.name for column in primary_key]
    elif primary_key is not None:
        raise ValueError(
            "The primary_key parameter is only used with output_table_type='snapshot'; "
            "remove it or set output_table_type='snapshot'."
        )

    data_storage = api.DataStorage(
        storage_type="clickhouse",
        connection_string=connection_string,
        table_name=table_name,
        max_batch_size=max_batch_size,
        table_writer_init_mode=init_mode_from_str(init_mode),
        snapshot_maintenance_on_output=is_snapshot_mode,
    )
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=key_field_names if key_field_names is not None else [],
        value_fields=_format_output_value_fields(table),
    )

    datasink_type = "snapshot" if is_snapshot_mode else "sink"
    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name=f"clickhouse.{datasink_type}",
            unique_name=name,
            sort_by=sort_by,
        )
    )

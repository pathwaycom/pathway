# Copyright © 2026 Pathway

from __future__ import annotations

import logging
from typing import Any, Iterable, Literal, Optional

from pathway.internals import api, datasink, datasource, dtype as dt
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
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
from pathway.schema import schema_builder


def _validate_identifier(arg_name: str, value: str) -> None:
    """Reject empty / NUL-containing identifiers up-front so users see a
    clear `ValueError` at call time instead of an opaque SQL Server parse
    error (`[]` is not a legal bracket-quoted identifier; embedded NUL
    bytes corrupt the TDS stream).
    """
    if value == "":
        raise ValueError(f"{arg_name} must not be empty")
    if "\0" in value:
        raise ValueError(f"{arg_name} must not contain NUL characters")


@check_arg_types
@trace_user_frame
def read(
    connection_string: str,
    table_name: str,
    schema: type[Schema] | None = None,
    *,
    mode: Literal["static", "streaming"] = "streaming",
    schema_name: str = "dbo",
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data: Any = None,
) -> Table:
    """Reads a table from a Microsoft SQL Server database.

    In ``"static"`` mode, the connector issues a plain ``SELECT`` against the
    table, emits all rows, and terminates. No special database configuration is
    required beyond normal read access to the table. Works on any SQL Server
    edition, including Express.

    In ``"streaming"`` mode (the default), the connector uses MSSQL's Change
    Data Capture (CDC) feature to track changes via the transaction log. This
    requires:

    - SQL Server Developer or Enterprise edition
    - CDC enabled on the database: ``EXEC sys.sp_cdc_enable_db;``
    - CDC enabled on the table:
      ``EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'<table>', @role_name=NULL;``

    **Primary key**: the schema must declare at least one primary key column via
    ``pw.column_definition(primary_key=True)``. The connector uses these columns
    to track which rows have been inserted, updated, or deleted — without them it
    cannot maintain a consistent snapshot of the table.

    **Persistence**: when persistence is enabled, the connector saves the CDC
    Log Sequence Number (LSN) of the last processed change as its offset.  On
    restart it skips the full table snapshot and resumes from that LSN, so
    downstream sees only the rows that changed since the last checkpoint — no
    re-delivery of the original table contents.  Passing an explicit ``name``
    is optional — Pathway Live Data Framework will auto-generate one if omitted — but setting it
    makes the saved state easier to identify in the persistence directory and
    protects against accidental mismatches when the pipeline graph changes
    between runs.

    Persistence applies to both modes.  In ``"streaming"`` mode the connector
    keeps running after the catch-up and continues delivering live CDC
    events.  In ``"static"`` mode it emits the delta accumulated since the
    previous run and terminates.

    Persistence requires CDC on the target table — the LSN comes from CDC.
    If you pass a ``persistence_config`` to ``pw.run`` but CDC has not been
    enabled on the table, the pipeline aborts at startup with an error
    pointing you at ``sp_cdc_enable_table``; it does not silently fall back
    to re-reading the whole table on every restart.

    If the saved LSN predates the capture instance's current retention window
    (SQL Server's CDC cleanup job runs independently of any consumer and
    drops changes older than the configured retention, 4320 minutes by
    default), the connector raises an error on startup asking you to clear
    the persistence directory and re-snapshot.  Pick a retention long enough
    to cover your longest expected downtime.

    The connection uses the TDS protocol via a pure Rust implementation
    (no ODBC drivers required), so it works on any Linux environment without
    additional system dependencies. Compatible with SQL Server 2017, 2019, 2022,
    and Azure SQL Edge.

    Args:
        connection_string: ADO.NET-style connection string for the MSSQL database.
            Example: ``"Server=tcp:localhost,1433;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true"``
        table_name: Name of the table to read from.
        schema: Schema of the resulting table.
        mode: ``"streaming"`` (the default) uses CDC for real-time change
            tracking via the transaction log; requires CDC to be enabled on the
            database and table. ``"static"`` reads the full table once as a
            snapshot, then terminates; no CDC setup is needed.
        schema_name: Name of the database schema containing the table. Defaults to
            ``"dbo"``, which is the default schema in MSSQL.
        autocommit_duration_ms: The maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway Live Data Framework's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        max_backlog_size: Limit on the number of entries read from the input source and kept
            in processing at any moment.
        debug_data: Static data to use instead of the external source (for testing).

    Returns:
        Table: The table read.

    Example:

    To test this connector locally, you can run a MSSQL instance using Docker:

    .. code-block:: bash

        docker run -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=YourStrong!Passw0rd' \\
            -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

    For static snapshot mode (no CDC setup required):

    >>> import pathway as pw
    >>> class MySchema(pw.Schema):
    ...     id: int = pw.column_definition(primary_key=True)
    ...     name: str
    ...     value: float
    >>> table = pw.io.mssql.read(
    ...     connection_string="Server=tcp:localhost,1433;Database=testdb;"
    ...         "User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ...     mode="static",
    ... )

    For streaming mode with CDC, first enable CDC on the database and the table
    (run these once in SQL Server Management Studio or via ``sqlcmd``):

    .. code-block:: sql

        -- Enable CDC on the database
        EXEC sys.sp_cdc_enable_db;

        -- Enable CDC on the table
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name   = N'my_table',
            @role_name     = NULL;

    Then read from it using streaming mode:

    >>> table = pw.io.mssql.read(  # doctest: +SKIP
    ...     connection_string="Server=tcp:localhost,1433;Database=testdb;"
    ...         "User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ... )

    **Persistence.** Pass a ``persistence_config`` to ``pw.run``.  CDC must
    be enabled on the table — without it the pipeline aborts at startup with
    a clear error.  Persistence works the same way in both modes, the only
    difference is what the pipeline does once the delta is consumed:

    >>> persistence_config = pw.persistence.Config(
    ...     backend=pw.persistence.Backend.filesystem("./PStorage")
    ... )

    *Streaming mode* (the default).  The first run delivers the initial
    snapshot and then keeps running to push live CDC events; every
    subsequent run skips the snapshot and starts with the delta since the
    previous checkpoint before continuing to stream:

    >>> table = pw.io.mssql.read(  # doctest: +SKIP
    ...     connection_string="Server=tcp:localhost,1433;Database=testdb;"
    ...         "User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ... )
    >>> pw.io.jsonlines.write(table, "output.jsonl")  # doctest: +SKIP
    >>> pw.run(persistence_config=persistence_config)  # doctest: +SKIP

    *Static mode.* The first run dumps the full table and terminates; every
    subsequent run emits only the CDC delta accumulated since the previous
    run and terminates — handy for scheduled batch pipelines that want
    change-set semantics without a long-lived process:

    >>> table = pw.io.mssql.read(  # doctest: +SKIP
    ...     connection_string="Server=tcp:localhost,1433;Database=testdb;"
    ...         "User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ...     mode="static",
    ... )
    >>> pw.io.jsonlines.write(table, "output.jsonl")  # doctest: +SKIP
    >>> pw.run(persistence_config=persistence_config)  # doctest: +SKIP
    """
    _check_entitlements("mssql")

    _validate_identifier("table_name", table_name)
    _validate_identifier("schema_name", schema_name)

    if schema is None:
        try:
            from pathway.engine import mssql_explore_schema

            full_table_name = f"{schema_name}.{table_name}"
            columns_data, pk_columns = mssql_explore_schema(
                connection_string, full_table_name
            )
            schema_columns = {}
            for col_name, udt_name, is_nullable in columns_data:
                udt_name_lower = udt_name.lower()
                mapping = {
                    "tinyint": int,
                    "smallint": int,
                    "int": int,
                    "bigint": int,
                    "bit": bool,
                    "real": float,
                    "float": float,
                    "decimal": float,
                    "numeric": float,
                    "char": str,
                    "varchar": str,
                    "nchar": str,
                    "nvarchar": str,
                    "text": str,
                    "ntext": str,
                    "uniqueidentifier": str,
                }
                py_type = mapping.get(udt_name_lower, Any)
                if is_nullable and py_type is not Any:
                    py_type = Optional[py_type]

                is_pk = col_name in pk_columns
                from pathway.internals.schema import column_definition

                schema_columns[col_name] = column_definition(
                    dtype=py_type,
                    primary_key=is_pk,
                )

            if not pk_columns:
                logging.getLogger(__name__).warning(
                    f"No primary key found for {schema_name}.{table_name} during schema exploration. "
                    "Falling back to auto-generated row identifiers. "
                    "This may cause issues in streaming mode if the table is not append-only."
                )

            schema_name_class = (
                "".join(c.capitalize() for c in table_name.split("_")) + "Schema"
            )
            schema = schema_builder(schema_columns, name=schema_name_class)
            logging.getLogger(__name__).info(
                f"Derived schema for {schema_name}.{table_name}:\n{schema}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to explore schema automatically: {e}. Please provide an explicit schema."
            ) from e

    schema, api_schema = read_schema(schema)

    primary_key_columns = schema.primary_key_columns()
    if not primary_key_columns:
        raise ValueError(
            "pw.io.mssql.read requires at least one primary key column in the schema. "
            "Mark the column(s) that form the table's primary key with "
            "pw.column_definition(primary_key=True)."
        )

    pk_dtypes = schema._dtypes()
    nullable_pks = [
        name for name in primary_key_columns if isinstance(pk_dtypes[name], dt.Optional)
    ]
    if nullable_pks:
        raise ValueError(
            f"pw.io.mssql.read primary_key column(s) {nullable_pks} are declared "
            "nullable; primary-key columns must be non-nullable so the connector "
            "can derive a unique row identity. NULL values would collide on the "
            "same Pathway key and CDC tracking would silently merge unrelated rows."
        )

    cdc_enabled = mode == "streaming"

    data_storage = api.DataStorage(
        storage_type="mssql",
        connection_string=connection_string,
        table_name=table_name,
        schema_name=schema_name,
        mode=(
            api.ConnectorMode.STATIC if not cdc_enabled else api.ConnectorMode.STREAMING
        ),
    )
    data_format = api.DataFormat(
        format_type="transparent",
        session_type=api.SessionType.UPSERT,
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
            datasource_name="mssql",
            append_only=(mode == "static"),
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    connection_string: str,
    table_name: str,
    *,
    schema_name: str = "dbo",
    max_batch_size: int | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    output_table_type: Literal["stream_of_changes", "snapshot"] = "stream_of_changes",
    primary_key: list[ColumnReference] | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table`` to a Microsoft SQL Server table.

    The connector works in two modes: **snapshot** mode and **stream of changes**.
    In **snapshot** mode, the table maintains the current snapshot of the data
    using MSSQL's ``MERGE`` statement for atomic upserts.
    In **stream of changes** mode, the table contains the log of all data updates
    with ``time`` and ``diff`` columns.

    Compatible with all MSSQL versions on Linux (SQL Server 2017, 2019, 2022,
    and Azure SQL Edge). Uses pure Rust TDS implementation — no ODBC drivers required.

    Writes use SQL Server's native bulk-load protocol (``INSERT BULK``): each
    minibatch is streamed to the server in a single bulk transfer instead of
    row-by-row ``INSERT`` statements. In **stream of changes** mode the rows are
    bulk-loaded straight into the target table when possible; otherwise (and
    always in **snapshot** mode) they are bulk-loaded into a temporary staging
    table and applied to the target with one set-based ``INSERT`` / ``MERGE`` /
    ``DELETE``. Throughput scales with minibatch size, so larger
    ``max_batch_size`` values (or fewer, larger commits) write faster.

    Args:
        table: Table to be written.
        connection_string: ADO.NET-style connection string for the MSSQL database.
            Example: ``"Server=tcp:localhost,1433;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true"``
        table_name: Name of the target table.
        schema_name: Name of the database schema containing the table. Defaults to
            ``"dbo"``, which is the default schema in MSSQL.
        max_batch_size: Maximum number of entries allowed to be committed within a
            single transaction. Larger values mean fewer, larger bulk transfers
            and therefore higher write throughput.
        init_mode: ``"default"``: The default initialization mode;
            ``"create_if_not_exists"``: creates the table if it does not exist;
            ``"replace"``: drops and recreates the table.
        output_table_type: Defines how the output table manages its data. If set to
            ``"stream_of_changes"`` (the default), the system outputs a stream of
            modifications with ``time`` and ``diff`` columns. If set to ``"snapshot"``,
            the table maintains the current state using atomic MERGE upserts.
        primary_key: When using snapshot mode, one or more columns that form the primary
            key in the target MSSQL table.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    To test this connector locally, run a MSSQL instance using Docker:

    .. code-block:: bash

        docker run -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=YourStrong!Passw0rd' \\
            -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

    Then write to it:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ...    key | value
    ...      1 | Hello
    ...      2 | World
    ... ''')

    Stream of changes mode:

    >>> pw.io.mssql.write(
    ...     table,
    ...     "Server=tcp:localhost,1433;Database=testdb;"
    ...         "User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="test",
    ...     init_mode="create_if_not_exists",
    ... )

    Snapshot mode:

    >>> pw.io.mssql.write(
    ...     table,
    ...     "Server=tcp:localhost,1433;Database=testdb;"
    ...         "User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="test_snapshot",
    ...     init_mode="create_if_not_exists",
    ...     output_table_type="snapshot",
    ...     primary_key=[table.key],
    ... )

    You can run this pipeline with ``pw.run()``.
    """

    _validate_identifier("table_name", table_name)
    _validate_identifier("schema_name", schema_name)

    is_snapshot_mode = output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE
    if not is_snapshot_mode and primary_key is not None:
        raise ValueError(
            "primary_key can only be specified for the snapshot table type"
        )

    value_fields = _format_output_value_fields(table)

    # SQL Server's default collation matches identifiers case-insensitively
    # (`id` and `ID` resolve to the same column), so any pair of schema
    # columns that differ only in case would make CREATE TABLE fail with a
    # raw "duplicate column name" driver error at pipeline-startup.  Surface
    # the collision here with a Pathway-authored message instead.
    case_groups: dict[str, list[str]] = {}
    for field in value_fields:
        case_groups.setdefault(field.name.lower(), []).append(field.name)
    case_collisions = [
        sorted(names) for names in case_groups.values() if len(names) > 1
    ]
    if case_collisions:
        raise ValueError(
            f"pw.Schema has column names that differ only in case "
            f"({case_collisions}). SQL Server's default collation is "
            "case-insensitive, so CREATE TABLE would reject them as "
            "duplicates. Rename these columns in the Pathway table so "
            "every column name is unique case-insensitively."
        )

    if not is_snapshot_mode:
        # Stream-of-changes mode appends `[time]` / `[diff]` metadata columns
        # to the destination table.  If the user's own schema already has a
        # column with one of those names, the generated CREATE TABLE would
        # declare it twice and SQL Server would reject it with an opaque
        # "duplicate column name" error at startup.  Comparison is
        # case-insensitive — SQL Server's default collation treats `Time` and
        # `time` as the same identifier.
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

    data_storage = api.DataStorage(
        storage_type="mssql",
        connection_string=connection_string,
        max_batch_size=max_batch_size,
        table_name=table_name,
        schema_name=schema_name,
        table_writer_init_mode=init_mode_from_str(init_mode),
        snapshot_maintenance_on_output=is_snapshot_mode,
    )

    key_field_names = None
    if primary_key is not None:
        # Duplicate entries in `primary_key` produce a nonsensical
        # `PRIMARY KEY ([x], [x])` SQL clause that SQL Server rejects, and
        # the shared `SqlQueryTemplate` reorders DELETE bindings using the
        # duplicated index so retractions silently bind wrong values.
        # Reject with a clear message.
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
            # Raises ValueError when `pkey` belongs to a different table or
            # does not name a column of `table`, so users get a clear
            # message at write() time instead of an opaque runtime error.
            get_column_index(table, pkey)
            # Reject nullable primary-key columns.  SQL Server refuses to
            # build a PRIMARY KEY on a nullable column, and even if the
            # destination table is hand-crafted to allow NULLs, the MERGE
            # statement uses `target.k = source.k` which is UNKNOWN (not
            # TRUE) when both sides are NULL — so retractions never match.
            if isinstance(pkey._column.dtype, dt.Optional):
                raise ValueError(
                    f"primary_key column {pkey.name!r} is declared nullable "
                    f"({pkey._column.dtype}); primary-key columns must be "
                    "non-nullable in snapshot mode."
                )
            key_field_names.append(pkey.name)
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=key_field_names,
        value_fields=value_fields,
    )

    datasink_type = "snapshot" if is_snapshot_mode else "sink"
    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name=f"mssql.{datasink_type}",
            unique_name=name,
            sort_by=sort_by,
        )
    )

# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Any, Iterable, Literal

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import SNAPSHOT_OUTPUT_TABLE_TYPE, init_mode_from_str, read_schema


@check_arg_types
@trace_user_frame
def read(
    connection_string: str,
    table_name: str,
    schema: type[Schema],
    *,
    mode: Literal["static", "streaming"] = "static",
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data: Any = None,
) -> Table:
    """Reads a table from a Microsoft SQL Server database.

    In ``"static"`` mode (default), the connector polls the table periodically
    for changes, comparing against the previous snapshot to detect inserts,
    updates, and deletions.

    In ``"streaming"`` mode, the connector uses MSSQL's Change Data Capture (CDC)
    feature to track changes via the transaction log. This requires:

    - SQL Server Developer or Enterprise edition
    - CDC enabled on the database: ``EXEC sys.sp_cdc_enable_db;``
    - CDC enabled on the table: ``EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'<table>', @role_name=NULL;``

    The connection uses the TDS protocol via a pure Rust implementation
    (no ODBC drivers required), so it works on any Linux environment without
    additional system dependencies. Compatible with SQL Server 2017, 2019, 2022,
    and Azure SQL Edge.

    Args:
        connection_string: ADO.NET-style connection string for the MSSQL database.
            Example: ``"Server=tcp:localhost,1433;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true"``
        table_name: Name of the table to read from.
        schema: Schema of the resulting table.
        mode: ``"static"`` (default) polls the full table periodically and diffs
            against stored state. ``"streaming"`` uses CDC for real-time change
            tracking via the transaction log.
        autocommit_duration_ms: The maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
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

    Then read from it:

    >>> import pathway as pw
    >>> class MySchema(pw.Schema):
    ...     id: int
    ...     name: str
    ...     value: float
    >>> table = pw.io.mssql.read(
    ...     connection_string="Server=tcp:localhost,1433;Database=testdb;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ... )

    For CDC streaming mode (requires CDC to be enabled on the database and table):

    >>> table = pw.io.mssql.read(  # doctest: +SKIP
    ...     connection_string="Server=tcp:localhost,1433;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ...     mode="streaming",
    ... )
    """
    schema, api_schema = read_schema(schema)

    cdc_enabled = mode == "streaming"

    storage_type = "mssql_cdc" if cdc_enabled else "mssql"
    data_storage = api.DataStorage(
        storage_type=storage_type,
        connection_string=connection_string,
        table_name=table_name,
        mode=api.ConnectorMode.STREAMING,
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

    Args:
        table: Table to be written.
        connection_string: ADO.NET-style connection string for the MSSQL database.
            Example: ``"Server=tcp:localhost,1433;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true"``
        table_name: Name of the target table.
        max_batch_size: Maximum number of entries allowed to be committed within a
            single transaction.
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
    ...     "Server=tcp:localhost,1433;Database=testdb;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="test",
    ...     init_mode="create_if_not_exists",
    ... )

    Snapshot mode:

    >>> pw.io.mssql.write(
    ...     table,
    ...     "Server=tcp:localhost,1433;Database=testdb;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true",
    ...     table_name="test_snapshot",
    ...     init_mode="create_if_not_exists",
    ...     output_table_type="snapshot",
    ...     primary_key=[table.key],
    ... )

    You can run this pipeline with ``pw.run()``.
    """

    data_storage = api.DataStorage(
        storage_type="mssql",
        connection_string=connection_string,
        max_batch_size=max_batch_size,
        table_name=table_name,
        table_writer_init_mode=init_mode_from_str(init_mode),
        snapshot_maintenance_on_output=output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE,
    )

    key_field_names = None
    if primary_key is not None:
        key_field_names = []
        for pkey_field in primary_key:
            key_field_names.append(pkey_field.name)
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=key_field_names,
        value_fields=_format_output_value_fields(table),
    )

    datasink_type = (
        "snapshot" if output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE else "sink"
    )
    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name=f"mssql.{datasink_type}",
            unique_name=name,
            sort_by=sort_by,
        )
    )

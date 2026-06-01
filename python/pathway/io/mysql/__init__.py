# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Any, Iterable, Literal

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
    init_mode_from_str,
    read_schema,
)


@check_arg_types
@trace_user_frame
def read(
    connection_string: str,
    table_name: str,
    schema: type[Schema],
    *,
    mode: Literal["static", "streaming"] = "streaming",
    server_id: int | None = None,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data: Any = None,
) -> Table:
    """Reads a table from a MySQL database.

    In ``"static"`` mode, the connector issues a plain ``SELECT`` against the
    table, emits all rows, and terminates. No special server configuration is
    required beyond read access to the table.

    In ``"streaming"`` mode (the default), the connector performs Change Data
    Capture by reading MySQL's **binary log** (binlog). It first reads a snapshot
    of the table, then continuously tails the binary log, delivering every
    insert, update, and delete as it happens. This requires the MySQL server to
    be configured for row-based binary logging:

    - ``log_bin`` enabled (start ``mysqld`` with ``--log-bin``; on by default in
      MySQL 8.0+),
    - ``binlog_format=ROW`` (the default in MySQL 8.0+),
    - ``binlog_row_image=FULL`` (the default),

    and the connecting user to hold the ``REPLICATION SLAVE`` and
    ``REPLICATION CLIENT`` privileges
    (``GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO <user>``).

    **Primary key**: the schema must declare at least one primary-key column via
    ``pw.column_definition(primary_key=True)``. These columns identify each row
    so the connector can correlate snapshot rows with subsequent binlog inserts,
    updates, and deletes.

    **No server-side footprint**: unlike PostgreSQL logical replication, reading
    the MySQL binary log creates **no persistent state on the server** — there is
    no replication slot to leave behind. A disconnected or idle reader therefore
    cannot cause the server's disk to fill up: binary-log retention is governed
    solely by the server's own ``binlog_expire_logs_seconds`` /
    ``max_binlog_size`` settings, independently of any reader.

    **Persistence**: when persistence is enabled (by passing a
    ``persistence_config`` to ``pw.run``), the streaming connector saves the
    binary-log coordinates (file name and position) of the last processed change
    as its offset. On restart it skips the snapshot and resumes tailing from that
    position. If the saved binary-log file has since been purged by the server's
    normal log expiry — which happens only if Pathway was offline longer than the
    server's binary-log retention — the connector raises a clear error at
    startup asking you to clear the persistence directory and re-snapshot, rather
    than silently losing data. Choose a binary-log retention long enough to cover
    your longest expected downtime. Binary-log coordinates are specific to one
    MySQL server: if you restore the server from a backup or fail over to a
    different host, clear the persistence directory. Persistence applies to
    ``"streaming"`` mode; in ``"static"`` mode every run re-reads the full table.

    Args:
        connection_string: \
`Connection string <https://dev.mysql.com/doc/connector-j/en/connector-j-reference-jdbc-url-format.html>`_ \
for the MySQL database. It must include the database name, e.g.
            ``"mysql://user:password@localhost:3306/mydb"``.
        table_name: Name of the table to read from.
        schema: Schema of the resulting table.
        mode: ``"streaming"`` (the default) reads a snapshot and then tails the
            binary log for live changes; requires row-based binary logging.
            ``"static"`` reads the full table once as a snapshot, then terminates;
            no binary-log configuration is needed.
        server_id: The replica server id used when registering for the binary-log
            stream. It must be unique among everything replicating from the source
            server. If omitted, a random value is chosen on each run; set it
            explicitly if you run several readers against the same server and want
            stable identifiers.
        autocommit_duration_ms: The maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the
            connector are committed and pushed into Pathway's computation graph.
        name: A unique name for the connector. If provided, this name will be used
            in logs and monitoring dashboards.
        max_backlog_size: Limit on the number of entries read from the input
            source and kept in processing at any moment.
        debug_data: Static data to use instead of the external source (for testing).

    Returns:
        Table: The table read.

    Example:

    To test this connector locally, run a MySQL instance with binary logging
    enabled (it is on by default in the ``mysql:8.0`` image):

    .. code-block:: bash

        docker run --name mysql-container \\
            -e MYSQL_ROOT_PASSWORD=rootpass \\
            -e MYSQL_DATABASE=testdb \\
            -e MYSQL_USER=testuser \\
            -e MYSQL_PASSWORD=testpass \\
            -p 3306:3306 \\
            mysql:8.0

    Grant the replication privileges the binary-log reader needs (run once):

    .. code-block:: sql

        GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'testuser'@'%';

    For a one-off static read (no binary-log setup required):

    >>> import pathway as pw
    >>> class MySchema(pw.Schema):
    ...     id: int = pw.column_definition(primary_key=True)
    ...     name: str
    ...     value: float
    >>> table = pw.io.mysql.read(  # doctest: +SKIP
    ...     "mysql://testuser:testpass@localhost:3306/testdb",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ...     mode="static",
    ... )

    For streaming change data capture from the binary log, simply use the default
    mode:

    >>> table = pw.io.mysql.read(  # doctest: +SKIP
    ...     "mysql://testuser:testpass@localhost:3306/testdb",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ... )

    The resulting table can be transformed with the usual Pathway operators and
    written to any sink. For example, to mirror the table into another MySQL
    table in real time:

    >>> pw.io.mysql.write(  # doctest: +SKIP
    ...     table,
    ...     "mysql://testuser:testpass@localhost:3306/testdb",
    ...     table_name="my_table_copy",
    ...     init_mode="create_if_not_exists",
    ...     output_table_type="snapshot",
    ...     primary_key=[table.id],
    ... )
    >>> pw.run()  # doctest: +SKIP

    To enable persistence so a restarted pipeline resumes from where it left off,
    pass a ``persistence_config`` to ``pw.run``:

    >>> persistence_config = pw.persistence.Config(  # doctest: +SKIP
    ...     backend=pw.persistence.Backend.filesystem("./PStorage")
    ... )
    >>> table = pw.io.mysql.read(  # doctest: +SKIP
    ...     "mysql://testuser:testpass@localhost:3306/testdb",
    ...     table_name="my_table",
    ...     schema=MySchema,
    ...     name="my_mysql_source",
    ... )
    >>> pw.io.jsonlines.write(table, "output.jsonl")  # doctest: +SKIP
    >>> pw.run(persistence_config=persistence_config)  # doctest: +SKIP
    """
    _check_entitlements("mysql")

    schema, api_schema = read_schema(schema)

    primary_key_columns = schema.primary_key_columns()
    if not primary_key_columns:
        raise ValueError(
            "pw.io.mysql.read requires at least one primary key column in the schema. "
            "Mark the column(s) that form the table's primary key with "
            "pw.column_definition(primary_key=True)."
        )

    pk_dtypes = schema._dtypes()
    nullable_pks = [
        name for name in primary_key_columns if isinstance(pk_dtypes[name], dt.Optional)
    ]
    if nullable_pks:
        raise ValueError(
            f"pw.io.mysql.read primary_key column(s) {nullable_pks} are declared "
            "nullable; primary-key columns must be non-nullable so the connector "
            "can derive a unique row identity. NULL values would collide on the "
            "same Pathway key and change tracking would silently merge unrelated "
            "rows."
        )

    is_streaming = mode == "streaming"

    data_storage = api.DataStorage(
        storage_type="mysql",
        connection_string=connection_string,
        table_name=table_name,
        mode=(
            api.ConnectorMode.STREAMING if is_streaming else api.ConnectorMode.STATIC
        ),
        mysql_server_id=server_id,
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
            datasource_name="mysql",
            append_only=not is_streaming,
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
    """Writes ``table`` to a MySQL table.

    The connector works in two modes: **snapshot** mode and **stream of changes**.
    In **snapshot** mode, the table maintains the current snapshot of the data.
    In **stream of changes** mode, the table contains the log of all data updates. For
    stream of changes you also need to have two columns, ``time`` and ``diff`` of the
    integer type, where ``time`` stores the transactional minibatch time and ``diff``
    is ``1`` for row insertion or ``-1`` for row deletion.

    Args:
        table: Table to be written.
        connection_string: \
`Connection string <https://dev.mysql.com/doc/connector-j/en/connector-j-reference-jdbc-url-format.html>`_ \
for MySQL database.
        table_name: Name of the target table.
        max_batch_size: Maximum number of entries allowed to be committed within a
            single transaction.
        init_mode: ``"default"``: The default initialization mode;
            ``"create_if_not_exists"``: initializes the SQL writer by creating the necessary table
            if they do not already exist;
            ``"replace"``: Initializes the SQL writer by replacing any existing table.
        output_table_type: Defines how the output table manages its data. If set to ``"stream_of_changes"``
            (the default), the system outputs a stream of modifications to the target table.
            This stream includes two additional integer columns: ``time``, representing the computation
            minibatch, and ``diff``, indicating the type of change (``1`` for row addition and
            ``-1`` for row deletion). If set to ``"snapshot"``, the table maintains the current
            state of the data, updated atomically with each minibatch and ensuring that no partial
            minibatch updates are visible.
        primary_key: When using snapshot mode, one or more columns that form the primary
            key in the target MySQL table.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    To test this connector locally, you will need a MySQL instance. The easiest way to do
    this is to run a Docker image. For example, you can use the mysql image. Pull and run
    it as follows:

    .. code-block:: bash

        docker pull mysql
        docker run \
            --name mysql-container \
            -e MYSQL_ROOT_PASSWORD=rootpass \
            -e MYSQL_DATABASE=testdb \
            -e MYSQL_USER=testuser \
            -e MYSQL_PASSWORD=testpass \
            -p 3306:3306 \
            mysql:8.0

    The first command pulls the image from the Docker repository while the second runs it,
    setting the credentials for the user and the database name.

    The database is now created and you can use the connector to write data to a table.
    First, you will need a Pathway table for testing. You can create it as follows:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ...    key | value
    ...      1 | Hello
    ...      2 | World
    ... ''')

    You can write this table using:

    >>> pw.io.mysql.write(
    ...     table,
    ...     "mysql://testuser:testpass@localhost:3306/testdb",
    ...     table_name="test",
    ...     init_mode="create_if_not_exists",
    ... )

    The ``init_mode`` parameter set to ``"create_if_not_exists"`` ensures that the table
    is created by the framework if it does not already exist.

    You can run this pipeline with ``pw.run()``.

    After the pipeline completes, you can connect to the database from the command line:

    .. code-block:: bash

        docker exec -it mysql-container mysql -u testuser -p

    Enter the password set when creating the database, in this example it is ``testpass``.

    You can check the data in the table with a simple command:

    .. code-block:: sql

        select * from test;

    Note that if you run the code again, it will append data to this table. To overwrite
    the entire table, use ``init_mode`` set to ``"replace"``.

    Now suppose that the table is dynamic and you need to maintain a snapshot of the data.
    In this case it makes sense to use snapshot mode. For snapshot mode you need to choose
    which columns will form the primary key. For this example suppose it is the ``key``
    column. If the output table is called ``test_snapshot`` the code will look as follows:

    >>> pw.io.mysql.write(
    ...     table,
    ...     "mysql://testuser:testpass@localhost:3306/testdb",
    ...     table_name="test_snapshot",
    ...     init_mode="create_if_not_exists",
    ...     output_table_type="snapshot",
    ...     primary_key=[table.key],
    ... )

    **Note**: The table can be created in MySQL by Pathway when ``init_mode`` is set to
    ``"replace"`` or when it is set to ``"create_if_not_exists"``. However, when creating
    a table for snapshot mode, Pathway defines the primary key. If you use string or binary
    objects as a primary key, there is a limitation because these fields cannot serve as
    a primary key. Instead, you need to use for example a ``VARCHAR`` with a length limit
    depending on your specific use case. Therefore, if you plan to use strings or
    blobs as primary keys, make sure that the table with the correct schema is created
    manually in advance.
    """

    is_snapshot_mode = output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE

    # Stream-of-changes mode appends ``time BIGINT NOT NULL, diff SMALLINT
    # NOT NULL`` metadata columns to the generated CREATE TABLE / INSERT. A
    # user column with one of those names (case-insensitive -- MySQL column
    # names are not case-sensitive) would otherwise be emitted twice and
    # the engine worker would fail mid-run with an opaque "Duplicate column
    # name" error. Snapshot mode does not append these columns, so the
    # check only applies in stream mode.
    if not is_snapshot_mode:
        offending = sorted(
            {
                column_name
                for column_name in table.schema.column_names()
                if column_name.lower() in ("time", "diff")
            }
        )
        if offending:
            raise ValueError(
                f"Pathway schema column(s) {offending} collide with "
                "the 'time' and 'diff' metadata columns appended in "
                "stream_of_changes mode. Rename the column(s) in your "
                "schema or switch to output_table_type='snapshot' "
                "which does not append these metadata columns."
            )

    data_storage = api.DataStorage(
        storage_type="mysql",
        connection_string=connection_string,
        max_batch_size=max_batch_size,
        table_name=table_name,
        table_writer_init_mode=init_mode_from_str(init_mode),
        snapshot_maintenance_on_output=is_snapshot_mode,
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
            datasink_name=f"mysql.{datasink_type}",
            unique_name=name,
            sort_by=sort_by,
        )
    )

# Copyright © 2025 Pathway

from __future__ import annotations

from typing import Iterable, Literal

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import init_mode_from_str

_SNAPSHOT_OUTPUT_TABLE_TYPE = "snapshot"


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

    data_storage = api.DataStorage(
        storage_type="mysql",
        connection_string=connection_string,
        max_batch_size=max_batch_size,
        table_name=table_name,
        table_writer_init_mode=init_mode_from_str(init_mode),
        snapshot_maintenance_on_output=output_table_type == _SNAPSHOT_OUTPUT_TABLE_TYPE,
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
        "snapshot" if output_table_type == _SNAPSHOT_OUTPUT_TABLE_TYPE else "sink"
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

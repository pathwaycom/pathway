from __future__ import annotations

from typing import Iterable, Literal

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    *,
    connection_string: str,
    table_name: str,
    designated_timestamp_policy: (
        Literal["use_now", "use_pathway_time", "use_column"] | None
    ) = None,
    designated_timestamp: ColumnReference | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """
    Writes updates from ``table`` to a QuestDB table.

    The output includes all columns from the input table, plus two additional columns:
    ``time``, which contains the minibatch time from Pathway, and ``diff``, which indicates
    the type of change (``1`` for row insertion and ``-1`` for row deletion).

    By default, the
    `designated timestamp column <https://questdb.com/docs/concept/designated-timestamp/>`_
    in QuestDB is set to the current machine time when the row is written.

    This behavior can be changed using the ``designated_timestamp`` and ``designated_timestamp_policy``
    parameters. If ``designated_timestamp`` is specified, its values will be used as the timestamp.
    If you set ``designated_timestamp_policy`` to ``use_pathway_time``, the Pathway minibatch time
    will be used as the timestamp. You can also use ``designated_timestamp_policy="use_now"``
    to be more explicit about using the current machine time.

    Note that if you use ``designated_timestamp_policy="use_pathway_time"``, the minibatch time will
    not be added as a separate column; it will only be used as the timestamp. The same
    applies if you set ``designated_timestamp`` - this column is used as the designated timestamp
    and is not duplicated in the output table.

    If the target table does not exist, it will be created when the first write happens.
    If the table already exists, its schema must match the input data structure. An error
    will occur if the column types do not match.

    Args:
        table: The input table to write to QuestDB.
        connection_string: The
            `client configuration string <https://questdb.com/docs/configuration-string/>`_
            used to connect to QuestDB.
        table_name: The name of the target table in QuestDB.
        designated_timestamp_policy: Defines how the designated timestamp column is set. The value
            can be ``"use_now"``, which means the current machine time is used as the timestamp.
            It can also be ``"use_pathway_time"``, in which case the Pathway minibatch time
            is used. Another option is ``"use_column"``, which means a specific column will
            be used as the timestamp; in this case, the ``designated_timestamp`` parameter must be
            provided. If not specified, the default is ``"use_now"``.
        designated_timestamp: The name of the column that will be used as the designated timestamp
            column. This column must have either the ``DateTimeNaive`` or ``DateTimeUtc`` type.
            If this parameter is set, ``designated_timestamp_policy`` can only be set to ``"use_column"``,
            otherwise an error will occur.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    The easiest way to run QuestDB locally is with Docker. You can use the official
    image and start it like this:

    .. code-block:: bash

        docker pull questdb/questdb
        docker run --name questdb -p 8812:8812 -p 9000:9000 questdb/questdb

    The first command pulls the QuestDB image from the official repository. The second
    command starts a container and exposes two ports: ``8812`` and ``9000``. Port ``8812``
    is used for connections over the Postgres wire protocol, which will be demonstrated later.
    Port ``9000`` is used for the HTTP API, which supports both data ingestion and queries.

    You can now write a simple program. In this example, a table with one column called
    ``"data"`` is created and sent to the database:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ...      | data
    ...    1 | Hello
    ...    2 | World
    ... ''')

    This table can now be written to QuestDB. If the output table is called ``"test"``,
    the Pathway code looks like this:

    >>> pw.io.questdb.write(
    ...     table,
    ...     connection_string="http::addr=localhost:9000;",
    ...     table_name="test",
    ... )

    The connection string specifies that the HTTP
    `InfluxDB Line Protocol <https://questdb.com/docs/reference/api/ilp/overview/>`_ is
    used for sending data.

    Once the code has finished, you can connect to QuestDB using any client that supports
    the Postgres wire protocol. For example, with ``psql``:

    .. code-block:: bash

        psql -h localhost -p 8812 -U admin -d qdb

    The command will prompt for a password. Unless you have changed it, the default password is
    ``quest``. Once connected, you can run:

    .. code-block:: sql

        qdb=> select * from test;

    And see the contents of the table.
    """
    _check_entitlements("questdb")

    designated_timestamp_index = None
    if designated_timestamp is not None:
        if (
            designated_timestamp_policy is not None
            and designated_timestamp_policy != "use_column"
        ):
            raise ValueError(
                f"designated_timestamp is passed, but designated_timestamp_policy is {designated_timestamp_policy}"
            )
        designated_timestamp_policy = "use_column"
        if designated_timestamp._table != table:
            raise ValueError(
                f"The column {designated_timestamp} doesn't belong to the target table"
            )
        for index, column in enumerate(table._columns):
            if column == designated_timestamp.name:
                designated_timestamp_index = index
    elif designated_timestamp_policy is None:
        designated_timestamp_policy = "use_now"

    data_storage = api.DataStorage(
        storage_type="questdb",
        path=connection_string,
        table_name=table_name,
        key_field_index=designated_timestamp_index,
    )
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
        designated_timestamp_policy=designated_timestamp_policy,
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="questdb.sink",
            unique_name=name,
            sort_by=sort_by,
        )
    )

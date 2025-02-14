# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Iterable, Literal

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


def _connection_string_from_settings(settings: dict):
    return " ".join(k + "=" + v for (k, v) in settings.items())


def _init_mode_from_str(init_mode: str) -> api.SqlWriterInitMode:
    match init_mode:
        case "default":
            return api.SqlWriterInitMode.DEFAULT
        case "create_if_not_exists":
            return api.SqlWriterInitMode.CREATE_IF_NOT_EXISTS
        case "replace":
            return api.SqlWriterInitMode.REPLACE
        case _:
            raise ValueError(f"Invalid init_mode: {init_mode}")


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    postgres_settings: dict,
    table_name: str,
    *,
    max_batch_size: int | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table``'s stream of updates to a postgres table.

    In order for write to be successful, it is required that the table contains ``time``
    and ``diff`` columns of the integer type.

    Args:
        table: Table to be written.
        postgres_settings: Components for the connection string for Postgres.
        table_name: Name of the target table.
        max_batch_size: Maximum number of entries allowed to be committed within a
            single transaction.
        init_mode: "default": The default initialization mode;
            "create_if_not_exists": initializes the SQL writer by creating the necessary table
            if they do not already exist;
            "replace": Initializes the SQL writer by replacing any existing table.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider there's a need to output a stream of updates from a table in Pathway to
    a table in Postgres. Let's see how this can be done with the connector.

    First of all, one needs to provide the required credentials for Postgres
    `connection string <https://www.postgresql.org/docs/current/libpq-connect.html>`_.
    While the connection string can include a wide variety of settings, such as SSL
    or connection timeouts, in this example we will keep it simple and provide the
    smallest example possible. Suppose that the database is running locally on the standard
    port 5432, that it has the name ``database`` and is accessible under the username
    ``user`` with a password ``pass``.

    It gives us the following content for the connection string:

    >>> connection_string_parts = {
    ...     "host": "localhost",
    ...     "port": "5432",
    ...     "dbname": "database",
    ...     "user": "user",
    ...     "password": "pass",
    ... }

    Now let's load a table, which we will output to the database:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown("age owner pet \\n 1 10 Alice 1 \\n 2 9 Bob 1 \\n 3 8 Alice 2")

    In order to output the table, we will need to create a new table in the database. The table
    would need to have all the columns that the output data has. Moreover it will need
    integer columns ``time`` and ``diff``, because these values are an essential part of the
    output. Finally, it is also a good idea to create the sequential primary key for
    our changes so that we know the updates' order.

    To sum things up, the table creation boils down to the following SQL command:

    .. code-block:: sql

        CREATE TABLE pets (
            id SERIAL PRIMARY KEY,
            time INTEGER NOT NULL,
            diff INTEGER NOT NULL,
            age INTEGER,
            owner TEXT,
            pet TEXT
        );

    Now, having done all the preparation, one can simply call:

    >>> pw.io.postgres.write(
    ...     t,
    ...     connection_string_parts,
    ...     "pets",
    ... )
    """
    data_storage = api.DataStorage(
        storage_type="postgres",
        connection_string=_connection_string_from_settings(postgres_settings),
        max_batch_size=max_batch_size,
        table_name=table_name,
        sql_writer_init_mode=_init_mode_from_str(init_mode),
    )
    data_format = api.DataFormat(
        format_type="sql",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
        table_name=table_name,
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="postgres.sink",
            unique_name=name,
            sort_by=sort_by,
        )
    )


@check_arg_types
def write_snapshot(
    table: Table,
    postgres_settings: dict,
    table_name: str,
    primary_key: list[str],
    *,
    max_batch_size: int | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Maintains a snapshot of a table within a Postgres table.

    In order for write to be successful, it is required that the table contains ``time``
    and ``diff`` columns of the integer type.

    Args:
        postgres_settings: Components of the connection string for Postgres.
        table_name: Name of the target table.
        primary_key: Names of the fields which serve as a primary key in the Postgres table.
        max_batch_size: Maximum number of entries allowed to be committed within a
            single transaction.
        init_mode: "default": The default initialization mode;
            "create_if_not_exists": initializes the SQL writer by creating the necessary table
            if they do not already exist;
            "replace": Initializes the SQL writer by replacing any existing table.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider there is a table ``stats`` in Pathway, containing the average number of requests to some
    service or operation per user, over some period of time. The number of requests
    can be large, so we decide not to store the whole stream of changes, but to only store
    a snapshot of the data, which can be actualized by Pathway.

    The minimum set-up would require us to have a Postgres table with two columns: the ID
    of the user ``user_id`` and the number of requests across some period of time ``number_of_requests``.
    In order to maintain consistency, we also need two extra columns: ``time`` and ``diff``.

    The SQL for the creation of such table would look as follows:

    .. code-block:: sql

        CREATE TABLE user_stats (
            user_id TEXT PRIMARY KEY,
            number_of_requests INTEGER,
            time INTEGER NOT NULL,
            diff INTEGER NOT NULL
        );


    After the table is created, all you need is just to set up the output connector:

    >>> import pathway as pw
    >>> pw.io.postgres.write_snapshot(  # doctest: +SKIP
    ...    stats,
    ...    {
    ...        "host": "localhost",
    ...        "port": "5432",
    ...        "dbname": "database",
    ...        "user": "user",
    ...        "password": "pass",
    ...    },
    ...    "user_stats",
    ...    ["user_id"],
    ... )
    """

    data_storage = api.DataStorage(
        storage_type="postgres",
        connection_string=_connection_string_from_settings(postgres_settings),
        max_batch_size=max_batch_size,
        snapshot_maintenance_on_output=True,
        table_name=table_name,
        sql_writer_init_mode=_init_mode_from_str(init_mode),
    )
    data_format = api.DataFormat(
        format_type="sql_snapshot",
        key_field_names=primary_key,
        value_fields=_format_output_value_fields(table),
        table_name=table_name,
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="postgres.snapshot",
            unique_name=name,
            sort_by=sort_by,
        )
    )

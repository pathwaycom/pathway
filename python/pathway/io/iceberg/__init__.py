from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    catalog_uri: str,
    namespace: list[str],
    table_name: str,
    *,
    warehouse: str | None = None,
    min_commit_frequency: int | None = 60_000,
):
    """
    Writes the stream of changes from ``table`` into `Iceberg <https://iceberg.apache.org/>`_
    data storage. The data storage must be defined with the REST catalog URI, the namespace,
    and the table name.

    If the namespace or the table doesn't exist, they will be created by the connector.
    The schema of the new table is inferred from the ``table``'s schema. The output table
    must include two additional integer columns: ``time``, representing the computation
    minibatch, and ``diff``, indicating the type of change (``1`` for row addition and
    ``-1`` for row deletion).

    Args:
        table: Table to be written.
        catalog_uri: URI of the Iceberg REST catalog.
        namespace: The name of the namespace containing the target table. If the namespace
            doesn't exist, it will be created by the connector.
        table_name: The name of the table to be written. If a table with such a name
            doesn't exist, it will be created by the connector.
        warehouse: Optional, path to the Iceberg storage warehouse.
        min_commit_frequency: Specifies the minimum time interval between two data
            commits in storage, measured in milliseconds. If set to ``None``, finalized
            minibatches will be committed as soon as possible. Keep in mind that each
            commit in Iceberg creates a new Parquet file and writes an entry in the
            transaction log. Therefore, it is advisable to limit the frequency of commits
            to reduce the overhead of processing the resulting table.

    Returns:
        None

    Example:

    Consider a users data table stored locally in a file called ``users.txt`` in CSV format.
    The Iceberg output connector provides the capability to place this table into
    Iceberg storage, defined by the catalog with URI ``http://localhost:8181``. The target
    table is ``users``, located in the ``app`` namespace.

    First, the table must be read. To do this, you need to define the schema. For
    simplicity, consider that it consists of two fields: the user ID and the name.

    The schema definition may look as follows:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...     user_id: int
    ...     name: str

    Using this schema, you can read the table from the input file. You need to use the
    ``pw.io.csv.read`` connector. Here, you can use the static mode since the text file
    with the users doesn't change dynamically.

    >>> users = pw.io.csv.read("./users.txt", schema=InputSchema, mode="static")

    Once the table is read, you can use ``pw.io.iceberg.write`` to save this table into
    Iceberg storage.

    >>> pw.io.iceberg.write(
    ...     users,
    ...     catalog_uri="http://localhost:8181/",
    ...     namespace=["app"],
    ...     table_name="users",
    ... )

    Don't forget to run your program with ``pw.run`` once you define all necessary
    computations. After execution, you will be able to see the users' data in the
    Iceberg storage.
    """
    _check_entitlements("iceberg")
    data_storage = api.DataStorage(
        storage_type="iceberg",
        path=catalog_uri,
        min_commit_frequency=min_commit_frequency,
        database=warehouse,
        table_name=table_name,
        namespace=namespace,
    )

    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=None,
        value_fields=_format_output_value_fields(table),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="iceberg",
        )
    )

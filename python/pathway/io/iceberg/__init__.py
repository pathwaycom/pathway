from __future__ import annotations

from typing import Any, Iterable

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import AwsS3Settings, _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    _get_unique_name,
    _prepare_s3_connection_engine_settings,
    internal_connector_mode,
    read_schema,
)
from pathway.io.minio import MinIOSettings
from pathway.io.s3 import DigitalOceanS3Settings, WasabiS3Settings


@check_arg_types
@trace_user_frame
def read(
    catalog_uri: str,
    namespace: list[str],
    table_name: str,
    schema: type[Schema],
    *,
    mode: str = "streaming",
    s3_connection_settings: (
        AwsS3Settings | MinIOSettings | WasabiS3Settings | DigitalOceanS3Settings | None
    ) = None,
    warehouse: str | None = None,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    debug_data: Any = None,
    **kwargs,
) -> Table:
    """
    Reads a table from Apache Iceberg. If ran in a streaming mode, the connector tracks
    new row additions and old row deletions and reflects them in the table read.

    Note that the connector requires primary key fields to be specified in the schema.
    You can specify the fields to be used in the primary key with ``pw.column_definition``
    function.

    Args:
        catalog_uri: URI of the Iceberg REST catalog.
        namespace: The name of the namespace containing the table read.
        table_name: The name of the table to be read.
        schema: Schema of the resulting table.
        mode: Denotes how the engine polls the new data from the source. Currently
            ``"streaming"`` and ``"static"`` are supported. If set to ``"streaming"``
            the engine will wait for the updates in the specified lake. It will track
            new row additions and reflect these events in the state. On the other hand,
            the ``"static"`` mode will only consider the available data and ingest all
            of it in one commit. The default value is ``"streaming"``.
        s3_connection_settings: S3 credentials when using S3 as a backend.
        warehouse: Optional, path to the Iceberg storage warehouse.
        autocommit_duration_ms: The maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: Table read from the Iceberg source.

    Example:

    Consider a users data table stored in the Iceberg storage. The table is located in the
    ``app`` namespace and is named ``users``. The catalog URI is ``http://localhost:8181``.
    Below is an example of how to read this table into Pathway.

    First, the schema of the table needs to be created. The schema doesn't have to contain
    all the columns of the table, you can only specify the ones that are needed for the
    computation:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...     user_id: int = pw.column_definition(primary_key=True)
    ...     name: str

    Then, this table must be read from the Iceberg storage.

    >>> input_table = pw.io.iceberg.read(
    ...     catalog_uri="http://localhost:8181/",
    ...     namespace=["app"],
    ...     table_name="users",
    ...     schema=InputSchema,
    ...     mode="static",
    ... )

    Don't forget to run your program with ``pw.run`` once you define all necessary
    computations. Note that you can also change the mode to ``"streaming"`` if you want
    the changes in the table to be reflected in your computational pipeline.
    """

    if schema.primary_key_columns() is None:
        raise ValueError(
            "Iceberg reader requires explicit primary key fields specification"
        )

    _check_entitlements("iceberg")
    schema, api_schema = read_schema(schema)
    engine_s3_settings = _prepare_s3_connection_engine_settings(s3_connection_settings)

    data_storage = api.DataStorage(
        storage_type="iceberg",
        path=catalog_uri,
        database=warehouse,
        table_name=table_name,
        namespace=namespace,
        mode=internal_connector_mode(mode),
        aws_s3_settings=engine_s3_settings,
    )
    data_format = api.DataFormat(
        format_type="transparent",
        **api_schema,
    )

    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=_get_unique_name(name, kwargs),
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            schema=schema,
            data_source_options=data_source_options,
            datasource_name="iceberg",
            append_only=True,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    catalog_uri: str,
    namespace: list[str],
    table_name: str,
    *,
    s3_connection_settings: (
        AwsS3Settings | MinIOSettings | WasabiS3Settings | DigitalOceanS3Settings | None
    ) = None,
    warehouse: str | None = None,
    min_commit_frequency: int | None = 60_000,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
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
        s3_connection_settings: S3 credentials when using S3 as a backend.
        warehouse: Optional, path to the Iceberg storage warehouse.
        min_commit_frequency: Specifies the minimum time interval between two data
            commits in storage, measured in milliseconds. If set to ``None``, finalized
            minibatches will be committed as soon as possible. Keep in mind that each
            commit in Iceberg creates a new Parquet file and writes an entry in the
            transaction log. Therefore, it is advisable to limit the frequency of commits
            to reduce the overhead of processing the resulting table.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

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
    ...     user_id: int = pw.column_definition(primary_key=True)
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
    engine_s3_settings = _prepare_s3_connection_engine_settings(s3_connection_settings)
    data_storage = api.DataStorage(
        storage_type="iceberg",
        path=catalog_uri,
        min_commit_frequency=min_commit_frequency,
        database=warehouse,
        table_name=table_name,
        namespace=namespace,
        aws_s3_settings=engine_s3_settings,
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
            unique_name=name,
            sort_by=sort_by,
        )
    )

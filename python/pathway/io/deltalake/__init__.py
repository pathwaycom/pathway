# Copyright © 2024 Pathway

from __future__ import annotations

from os import PathLike, fspath
from typing import Any, Iterable

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import (
    AwsS3Settings,
    _format_output_value_fields,
    is_s3_path,
)
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    _get_unique_name,
    _prepare_s3_connection_settings,
    internal_connector_mode,
    read_schema,
)
from pathway.io.minio import MinIOSettings
from pathway.io.s3 import DigitalOceanS3Settings, WasabiS3Settings


def _engine_s3_connection_settings(
    uri: str, s3_connection_settings: AwsS3Settings | None
) -> api.AwsS3Settings | None:
    if is_s3_path(uri) and s3_connection_settings is None:
        s3_connection_settings = AwsS3Settings.new_from_path(uri)
    if s3_connection_settings is not None:
        s3_connection_settings.authorize()
        return s3_connection_settings.settings
    return None


@check_arg_types
@trace_user_frame
def read(
    uri: str | PathLike,
    schema: type[Schema],
    *,
    mode: str = "streaming",
    s3_connection_settings: (
        AwsS3Settings | MinIOSettings | WasabiS3Settings | DigitalOceanS3Settings | None
    ) = None,
    start_from_timestamp_ms: int | None = None,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    debug_data: Any = None,
    **kwargs,
) -> Table:
    """
    Reads a table from Delta Lake. Currently, local and S3 lakes are supported. The table
    doesn't have to be append only, however, the deletion vectors are not supported yet.

    Note that the connector requires either the table to be append-only or the primary key
    fields to be specified in the schema. You can define the primary key fields using the
    ``pw.column_definition`` function.

    Args:
        uri: URI of the Delta Lake source that must be read.
        schema: Schema of the resulting table.
        mode: Denotes how the engine polls the new data from the source. Currently
            ``"streaming"`` and ``"static"`` are supported. If set to ``"streaming"``
            the engine will wait for the updates in the specified lake. It will track
            new row additions and reflect these events in the state. On the other hand,
            the ``"static"`` mode will only consider the available data and ingest all
            of it in one commit. The default value is ``"streaming"``.
        s3_connection_settings: Configuration for S3 credentials when using S3 storage.
            In addition to the access key and secret access key, you can specify a custom
            endpoint, which is necessary for buckets hosted outside of Amazon AWS. If the
            custom endpoint is left blank, the authorized user's credentials for S3 will
            be used.
        start_from_timestamp_ms: If defined, only changes that occurred after the specified
            timestamp will be read. This parameter can only be used for tables with
            append-only behavior.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        autocommit_duration_ms: The maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        debug_data: Static data replacing original one when debug mode is active.

    Examples:

    Consider an example with a stream of changes on a simple key-value table, streamed by
    another Pathway program with ``pw.io.deltalake.write`` method.

    Let's start writing Pathway code. First, the schema of the table needs to be created:

    >>> import pathway as pw
    >>> class KVSchema(pw.Schema):
    ...     key: str = pw.column_definition(primary_key=True)
    ...     value: str

    Then, this table must be written into a Delta Lake storage. In the example, it can
    be created from the static data with ``pw.debug.table_from_markdown`` method and
    saved into the locally located lake:

    >>> output_table = pw.debug.table_from_markdown("key value \\n one Hello \\n two World")
    >>> lake_path = "./local-lake"
    >>> lake_path = getfixture("tmp_path") / "local-lake"  # NODOCS
    >>> pw.io.deltalake.write(output_table, lake_path)

    Now the producer code can be run with with a simple ``pw.run``:

    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    After that, you can read this table with Pathway as well. It requires the specification
    of the URI and the schema that was created above. In addition, you can use the ``"static"``
    mode, so that the program finishes after the data is read:

    >>> input_table = pw.io.deltalake.read(lake_path, KVSchema, mode="static")

    Please note that the table doesn't necessary have to be created by Pathway: an
    append-only Delta Table created in any other way will also be processed correctly.

    Finally, you can check that the resulting table contains the same set of rows by
    displaying it with ``pw.debug.compute_and_print``:

    >>> pw.debug.compute_and_print(input_table, include_id=False)
    key | value
    one | Hello
    two | World

    Please note that you can use the same communication approach if S3 is used as a
    data storage. To do this, specify an S3 path starting with ``s3://``
    or ``s3a://``, and provide the credentials object as a parameter. If no credentials
    are provided but the path starts with ``s3://`` or ``s3a://``, Pathway will use the
    credentials of the currently authenticated user.
    """
    _check_entitlements("deltalake")
    prepared_connection_settings = _prepare_s3_connection_settings(
        s3_connection_settings
    )

    uri = fspath(uri)
    schema, api_schema = read_schema(schema)

    data_storage = api.DataStorage(
        storage_type="deltalake",
        path=uri,
        mode=internal_connector_mode(mode),
        aws_s3_settings=_engine_s3_connection_settings(
            uri, prepared_connection_settings
        ),
        start_from_timestamp_ms=start_from_timestamp_ms,
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
            datasource_name="deltalake",
            append_only=True,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    uri: str | PathLike,
    *,
    s3_connection_settings: (
        AwsS3Settings | MinIOSettings | WasabiS3Settings | DigitalOceanS3Settings | None
    ) = None,
    partition_columns: Iterable[ColumnReference] | None = None,
    min_commit_frequency: int | None = 60_000,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """
    Writes the stream of changes from ``table`` into `Delta Lake <https://delta.io/>_` data
    storage at the location specified by ``uri``. Supported storage types are S3 and the
    local filesystem.

    The storage type is determined by the URI: paths starting with ``s3://`` or ``s3a://``
    are for S3 storage, while all other paths use the filesystem.

    If the specified storage location doesn't exist, it will be created. The schema of
    the new table is inferred from the ``table``'s schema. The output table must include
    two additional integer columns: ``time``, representing the computation minibatch,
    and ``diff``, indicating the type of change (``1`` for row addition and ``-1`` for row deletion).

    Args:
        table: Table to be written.
        uri: URI of the target Delta Lake.
        s3_connection_settings: Configuration for S3 credentials when using S3 storage.
            In addition to the access key and secret access key, you can specify a custom
            endpoint, which is necessary for buckets hosted outside of Amazon AWS. If the
            custom endpoint is left blank, the authorized user's credentials for S3 will
            be used.
        partition_columns: Partition columns for the table. Used if the table is created by
            Pathway.
        min_commit_frequency: Specifies the minimum time interval between two data commits in
            storage, measured in milliseconds. If set to None, finalized minibatches will
            be committed as soon as possible. Keep in mind that each commit in Delta Lake
            creates a new file and writes an entry in the transaction log. Therefore, it
            is advisable to limit the frequency of commits to reduce the overhead of
            processing the resulting table. Note that to further optimize performance and
            reduce the number of chunks in the table, you can use \
`vacuum <https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table>`_
            or \
`optimize <https://docs.delta.io/2.0.2/optimizations-oss.html#optimize-performance-with-file-management>`_
            operations afterwards.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider a table ``access_log`` that needs to be output to a Delta Lake storage
    located locally at the folder ``./logs/access-log``. It can be done as follows:

    >>> pw.io.deltalake.write(access_log, "./logs/access-log")  # doctest: +SKIP

    Please note that if there is no filesystem object at this path, the corresponding
    folder will be created. However, if you run this code twice, the new data will be
    appended to the storage created during the first run.

    It is also possible to save the table to S3 storage. To save the table to the
    ``access-log`` path within the ``logs`` bucket in the ``eu-west-3`` region,
    modify the code as follows:

    >>> pw.io.deltalake.write(  # doctest: +SKIP
    ...     access_log,
    ...     "s3://logs/access-log/",
    ...     s3_connection_settings=pw.io.s3.AwsS3Settings(
    ...         bucket_name="logs",
    ...         region="eu-west-3",
    ...         access_key=os.environ["S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    ...     )
    ... )

    Note that it is not necessary to specify the credentials explicitly if you are
    logged into S3. Pathway can deduce them for you. For an authorized user, the code
    can be simplified as follows:

    >>> pw.io.deltalake.write(access_log, "s3://logs/access-log/")  # doctest: +SKIP
    """
    _check_entitlements("deltalake")
    prepared_connection_settings = _prepare_s3_connection_settings(
        s3_connection_settings
    )

    prepared_partition_columns = []
    if partition_columns is not None:
        for column in partition_columns:
            if column._table != table:
                raise ValueError(
                    f"The suggested partition column {column} doesn't belong to the table {table}"
                )
            prepared_partition_columns.append(column._name)

    uri = fspath(uri)
    data_storage = api.DataStorage(
        storage_type="deltalake",
        path=uri,
        aws_s3_settings=_engine_s3_connection_settings(
            uri, prepared_connection_settings
        ),
        min_commit_frequency=min_commit_frequency,
        partition_columns=prepared_partition_columns,
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
            datasink_name="deltalake",
            unique_name=name,
            sort_by=sort_by,
        )
    )

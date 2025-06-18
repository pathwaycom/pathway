# Copyright © 2024 Pathway

from __future__ import annotations

import datetime
import json
import logging
import os
import threading
import time
from os import PathLike, fspath
from typing import Any, Iterable, Literal

from deltalake import DeltaTable

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import (
    AwsS3Settings,
    _format_output_value_fields,
    is_s3_path,
)
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema, schema_from_dict
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

_PATHWAY_COLUMN_META_FIELD = "pathway.column.metadata"
_SNAPSHOT_OUTPUT_TABLE_TYPE = "snapshot"
_DELTA_LOG_REL_PATH = "_delta_log"
_LAST_CHECKPOINT_BLOCK_NAME = "_last_checkpoint"
_CHECKPOINT_EXTENSION = ".checkpoint.parquet"


def _engine_s3_connection_settings(
    uri: str, s3_connection_settings: AwsS3Settings | None
) -> api.AwsS3Settings | None:
    if is_s3_path(uri) and s3_connection_settings is None:
        s3_connection_settings = AwsS3Settings.new_from_path(uri)
    if s3_connection_settings is not None:
        s3_connection_settings.authorize()
        return s3_connection_settings.settings
    return None


class TableOptimizer:
    """
    The table optimizer is used to optimize partitioned Delta tables created by the
    output connector. This optimization is limited to tables that are partitioned by a
    string column, where the values represent date and time in a specific format.

    After a ``WRITE`` operation and once the specified interval has passed, the optimizer
    runs `OPTIMIZE <https://delta.io/blog/delta-lake-optimize/>`_ and
    `VACUUM <https://docs.delta.io/latest/delta-utility.html#id1>`_ operations.
    If these operations fail, they will be retried during the next write. Keep in mind
    that running ``OPTIMIZE`` and ``VACUUM`` may cause a delay in the output because they take
    time to complete, but they do not slow down the overall computational pipeline.
    This approach is necessary to prevent conflicts that could occur from simultaneous
    writes to the Delta log.

    When ``optimize_transaction_log`` is enabled, a background process will identify
    and remove parts of the transaction log that no longer reference existing data files.
    This helps reduce the number of Delta log files, which can grow quickly with
    small batches or over long periods.

    If ``remove_old_checkpoints`` is enabled, the background process will also make
    sure that only the most recent checkpoint file is kept.

    Please note that both ``optimize_transaction_log`` and ``remove_old_checkpoints``
    are currently experimental features and work only when the backend uses a filesystem.

    Args:
        tracked_column: The partition column for the observed table.
        time_format: A `strftime-like <https://strftime.org/>`_ format string
            that defines how values in the tracked column are interpreted.
        quick_access_window: All partition values older than this window
            will be compressed using the ``OPTIMIZE`` Delta Lake operation, followed by
            ``VACUUM``.
        compression_frequency: Determines how often the compression process
            is triggered. If a compression attempt fails, it will be retried immediately
            without waiting.
        retention_period: Retention period for the ``VACUUM`` operation.
        optimize_transaction_log: If ``True``, Pathway will clean the Delta transaction
            log by removing entries that reference Parquet files already deleted by
            ``VACUUM``.
        remove_old_checkpoints: If ``True``, Pathway will keep only the most recent
            checkpoint file to reduce storage usage.

    Example:

    Suppose you are writing to a table that is partitioned by the column ``day_utc``,
    where the values follow the ISO-8601 format: ``YYYY-MM-DD``. You want to compress
    data older than 7 days and run this compression once per day.

    In that case, the optimizer settings would be configured as follows:

    >>> import pathway as pw
    >>> optimizer = pw.io.deltalake.TableOptimizer(  # doctest: +SKIP
    ...     tracked_column=table.day_utc,
    ...     time_forma2t="%Y-%m-%d",
    ...     quick_access_window=datetime.timedelta(days=7),
    ...     compression_frequency=datetime.timedelta(days=1),
    ... )

    This optimizer object needs to be passed to the ``pw.io.deltalake.write`` function.

    Note: Background cleanup of old Delta log entries will not run automatically in
    this setup. To enable it, you can turn it on explicitly:

    >>> optimizer = pw.io.deltalake.TableOptimizer(  # doctest: +SKIP
    ...     tracked_column=table.day_utc,
    ...     time_format="%Y-%m-%d",
    ...     quick_access_window=datetime.timedelta(days=7),
    ...     compression_frequency=datetime.timedelta(days=1),
    ...     optimize_transaction_log=True,
    ...     remove_old_checkpoints=True,
    ... )
    """

    def __init__(
        self,
        tracked_column: ColumnReference,
        time_format: str,
        quick_access_window: datetime.timedelta,
        compression_frequency: datetime.timedelta,
        retention_period: datetime.timedelta = datetime.timedelta(0),
        optimize_transaction_log: bool = False,
        remove_old_checkpoints: bool = False,
    ):
        self.compression_frequency = compression_frequency
        self.engine_rule = api.DeltaOptimizerRule(
            field_name=tracked_column.name,
            time_format=time_format,
            quick_access_window=quick_access_window,
            compression_frequency=compression_frequency,
            retention_period=retention_period,
        )
        self.tracked_column = tracked_column
        self.is_active = False
        self.lock = threading.Lock()
        self.optimize_transaction_log = optimize_transaction_log
        self.remove_old_checkpoints = remove_old_checkpoints
        self.optimizer_thread: threading.Thread | None = None
        self.table_path: str | None = None

    def _start_compression(self, table_path: str):
        is_background_thread_needed = (
            self.optimize_transaction_log or self.remove_old_checkpoints
        )
        if not is_background_thread_needed:
            return

        self.is_active = True
        self.table_path = table_path
        cycle_duration = self.compression_frequency.total_seconds()

        def target():
            last_compression_at = None
            while True:
                with self.lock:
                    if not self.is_active:
                        break
                    iteration_stated_at = time.time()
                    is_compression_needed = (
                        last_compression_at is None
                        or iteration_stated_at - last_compression_at >= cycle_duration
                    )
                    if is_compression_needed and self._perform_compression():
                        last_compression_at = iteration_stated_at
                        elapsed = time.time() - iteration_stated_at
                        if elapsed > cycle_duration:
                            logging.warning(
                                "Compression cycle takes more than the specified compression frequency: "
                                f"{elapsed:.3f}s vs {cycle_duration:.3f}s"
                            )
                time.sleep(1.0)

        self.optimizer_thread = threading.Thread(target=target, daemon=True)
        self.optimizer_thread.start()

    def _perform_compression(self) -> bool:
        if self.table_path is not None:
            try:
                _ = DeltaTable(self.table_path)
                tlog_path = os.path.join(self.table_path, _DELTA_LOG_REL_PATH)
                tlog_file_list = os.listdir(tlog_path)
            except Exception:
                # The table is not ready yet
                # There are more specific exceptions, but they are in the private part
                # of the library (deltalake._internal)
                logging.exception(f"The Delta table {self.table_path} is not ready yet")
                return False
            tlog_file_list.sort()
            if self.optimize_transaction_log:
                self._remove_obsolete_versions(tlog_file_list)
            if self.remove_old_checkpoints:
                self._remove_old_checkpoints(tlog_file_list)
        else:
            return False

        return True

    def _stop_compression(self):
        if self.optimizer_thread is not None:
            with self.lock:
                self.is_active = False
            self.optimizer_thread.join()
            self._perform_compression()

    @staticmethod
    def _is_version_obsolete(file_path: str, actual_parquet_files: set[str]) -> bool:
        with open(file_path, "r") as f:
            for row in f:
                row_decoded = json.loads(row)
                path_add = row_decoded.get("add", {}).get("path")
                path_remove = row_decoded.get("remove", {}).get("path")
                path = path_add or path_remove
                if path in actual_parquet_files:
                    return False
        return True

    @staticmethod
    def _version_number(tlog_file_name: str) -> int:
        version = tlog_file_name[: tlog_file_name.find(".")]
        return int(version)

    def _get_actual_parquet_block_paths(self) -> set[str]:
        assert isinstance(self.table_path, str)
        result = set()
        for folder_name in os.listdir(self.table_path):
            nested_root = os.path.join(self.table_path, folder_name)
            if folder_name == _DELTA_LOG_REL_PATH or not os.path.isdir(nested_root):
                continue
            for file_name in os.listdir(nested_root):
                result.add(os.path.join(folder_name, file_name))
        return result

    def _remove_obsolete_versions(self, tlog_file_names: list[str]):
        assert isinstance(self.table_path, str)
        actual_parquet_files = self._get_actual_parquet_block_paths()
        delta_log_path = os.path.join(self.table_path, _DELTA_LOG_REL_PATH)
        for file_name in tlog_file_names:
            file_path = os.path.join(delta_log_path, file_name)
            if (
                file_name.endswith(".json")
                and os.path.isfile(file_path)
                and self._version_number(file_name) > 0
                and self._is_version_obsolete(file_path, actual_parquet_files)
            ):
                logging.info(f"Removing obsolete Delta log entry {file_name}")
                file_path = os.path.join(delta_log_path, file_name)
                os.remove(file_path)

    def _remove_old_checkpoints(self, tlog_file_names: list[str]):
        assert isinstance(self.table_path, str)
        delta_log_path = os.path.join(self.table_path, _DELTA_LOG_REL_PATH)
        last_checkpoint_path = os.path.join(delta_log_path, _LAST_CHECKPOINT_BLOCK_NAME)
        try:
            with open(last_checkpoint_path, "r") as f:
                last_checkpoint_meta = json.load(f)
        except Exception:
            # There is no _last_checkpoint file or it's broken
            return
        last_checkpoint_version = last_checkpoint_meta["version"]
        logging.info(
            f"Last checkpoint version from metadata: {last_checkpoint_version}"
        )
        for file_name in tlog_file_names:
            if not file_name.endswith(_CHECKPOINT_EXTENSION):
                continue
            version = self._version_number(file_name)
            if version != last_checkpoint_version:
                logging.info(f"Removing obsolete Delta checkpoint {file_name}")
                file_path = os.path.join(delta_log_path, file_name)
                os.remove(file_path)


@check_arg_types
@trace_user_frame
def read(
    uri: str | PathLike,
    schema: type[Schema] | None = None,
    *,
    mode: Literal["streaming", "static"] = "streaming",
    s3_connection_settings: (
        AwsS3Settings | MinIOSettings | WasabiS3Settings | DigitalOceanS3Settings | None
    ) = None,
    start_from_timestamp_ms: int | None = None,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    debug_data: Any = None,
    _backfilling_thresholds: list[api.BackfillingThreshold] | None = None,
    **kwargs,
) -> Table:
    """
    Reads a table from Delta Lake. Currently, local and S3 lakes are supported. The table
    doesn't have to be append only, however, the deletion vectors are not supported yet.

    Reads are atomic with respect to the data version. This means that if a data update -
    such as inserting, deleting, or modifying rows - is made within a single Delta transaction,
    all those changes will be applied together, as one atomic operation, in a single minibatch.

    Note that the connector requires either the table to be append-only or the primary key
    fields to be specified in the schema. You can define the primary key fields using the
    ``pw.column_definition`` function.

    Args:
        uri: URI of the Delta Lake source that must be read.
        schema: Defines the schema of the resulting table. You can omit this parameter
            if the table is created using ``pw.io.deltalake.write``, as the schema
            (excluding the special ``time`` and ``diff`` fields) will then be automatically
            stored in the Delta Table's columns metadata.
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
    data_storage = api.DataStorage(
        storage_type="deltalake",
        path=uri,
        mode=internal_connector_mode(mode),
        aws_s3_settings=_engine_s3_connection_settings(
            uri, prepared_connection_settings
        ),
        start_from_timestamp_ms=start_from_timestamp_ms,
        backfilling_thresholds=_backfilling_thresholds,
    )
    if schema is None:
        try:
            storage_options = data_storage.delta_s3_storage_options()
        except ValueError:
            storage_options = {}
        schema = _read_table_schema_from_metadata(uri, storage_options)
    schema, api_schema = read_schema(schema)

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


def _read_table_schema_from_metadata(
    uri: str, storage_options: dict[str, str] | None
) -> type[Schema]:
    table = DeltaTable(uri, storage_options=storage_options)
    schema = table.schema()
    table_schema = {}
    for field in schema.fields:
        name = field.name
        metadata = field.metadata.get(_PATHWAY_COLUMN_META_FIELD)
        if metadata is None:
            continue
        table_schema[name] = json.loads(metadata)
    if len(table_schema) == 0:
        raise ValueError(
            "No Pathway table schema is stored in the given Delta table's metadata"
        )
    return schema_from_dict(table_schema)


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
    output_table_type: Literal["stream_of_changes", "snapshot"] = "stream_of_changes",
    table_optimizer: TableOptimizer | None = None,
) -> None:
    """
    Writes the stream of changes from ``table`` into `Delta Lake <https://delta.io/>_` data
    storage at the location specified by ``uri``. Supported storage types are S3 and the
    local filesystem.

    The storage type is determined by the URI: paths starting with ``s3://`` or ``s3a://``
    are for S3 storage, while all other paths use the filesystem.

    If the specified storage location doesn't exist, it will be created. The schema of
    the new table is inferred from the ``table``'s schema. Additionally, when the connector
    creates a table, its Pathway schema is stored in the column metadata. This allows the
    table to be read using ``pw.io.deltalake.read`` without explicitly specifying a ``schema``.

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
        output_table_type: Defines how the output table manages its data. If set to ``"stream_of_changes"``
            (the default), the system outputs a stream of modifications to the target table.
            This stream includes two additional integer columns: ``time``, representing the computation
            minibatch, and ``diff``, indicating the type of change (``1`` for row addition and
            ``-1`` for row deletion). If set to ``"snapshot"``, the table maintains the current
            state of the data, updated atomically with each minibatch and ensuring that no partial
            minibatch updates are visible. To correctly track the relationship between the Pathway's
            primary key and the output table in this mode, an additional ``_id`` field of the
            ``Pointer`` type is added. **Please note that this mode may be slower when there are many deletions,
            because a deletion in a minibatch causes the entire table to be rewritten once that minibatch reaches
            the output. Please also note that this method is not suitable for the tables that don't
            fit in memory.**
        table_optimizer: The optimization parameters for the output table.

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
        snapshot_maintenance_on_output=output_table_type == _SNAPSHOT_OUTPUT_TABLE_TYPE,
        delta_optimizer_rule=(table_optimizer.engine_rule if table_optimizer else None),
    )
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=None,
        value_fields=_format_output_value_fields(table),
    )

    if table_optimizer is not None:
        if table_optimizer.tracked_column.name not in prepared_partition_columns:
            raise ValueError(
                f"Optimization is based on the column '{table_optimizer.tracked_column}', "
                "which is not a partition column. Please include this column in partition_columns."
            )
        table_optimizer._start_compression(table_path=uri)

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="deltalake",
            unique_name=name,
            sort_by=sort_by,
            on_pipeline_finished=(
                table_optimizer._stop_compression
                if table_optimizer is not None
                else None
            ),
        )
    )


# This is made to force TableOptimizer documentation
__all__ = [
    "TableOptimizer",
    "read",
    "write",
]

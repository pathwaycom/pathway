# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io.s3 import AwsS3Settings

S3_URI_PREFIX = "s3://"


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    uri: str,
    *,
    s3_connection_settings: AwsS3Settings | None = None,
    min_commit_frequency: int | None = 60_000,
) -> None:
    """
    Writes the stream of changes from ``table`` into `DeltaLake <https://delta.io/>_` data
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
        uri: URI of the target DeltaLake.
        s3_connection_settings: Configuration for S3 credentials when using S3 storage.
            In addition to the access key and secret access key, you can specify a custom
            endpoint, which is necessary for buckets hosted outside of Amazon AWS. If the
            custom endpoint is left blank, the authorized user's credentials for S3 will
            be used.
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

    Returns:
        None

    Example:

    Consider a table ``access_log`` that needs to be output to a DeltaLake storage
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
    storage_options = {}
    if uri.startswith(S3_URI_PREFIX):
        if s3_connection_settings is None:
            s3_connection_settings = AwsS3Settings.new_from_path(uri)
        storage_options = s3_connection_settings.as_deltalake_storage_options()

    data_storage = api.DataStorage(
        storage_type="deltalake",
        path=uri,
        storage_options=storage_options,
        min_commit_frequency=min_commit_frequency,
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
        )
    )

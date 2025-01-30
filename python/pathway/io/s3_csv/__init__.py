# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import CsvParserSettings, construct_schema_and_data_format
from pathway.io.s3 import AwsS3Settings, read as s3_read


@check_arg_types
@trace_user_frame
def read(
    path: str,
    *,
    aws_s3_settings: AwsS3Settings | None = None,
    schema: type[Schema] | None = None,
    csv_settings: CsvParserSettings | None = None,
    mode: str = "streaming",
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    debug_data=None,
    **kwargs,
) -> Table:
    """Reads a table from one or several objects in Amazon S3 bucket.

    In case the prefix of S3 path is specified, and there are several objects lying
    under this prefix, their order is determined according to their modification times:
    the smaller the modification time is, the earlier the file will be passed to the
    engine.

    Args:
        path: Path to an object or to a folder of objects in Amazon S3 bucket.
        aws_s3_settings: Connection parameters for the S3 account and the bucket.
        schema: Schema of the resulting table.
        csv_settings: The settings for the CSV parser.
        mode: If set to "streaming", the engine will wait for the new input
            files in the bucket, which fall under the path prefix. Set it to "static", it will only
            consider the available data and ingest all of it in one commit. Default value is
            "streaming".
        autocommit_duration_ms: the maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Let's consider an object store, which is hosted in Amazon S3. The store contains
    datasets in the respective bucket and is located in the region eu-west-3. The goal
    is to read the dataset, located under the path ``animals/`` in this bucket.

    Then, the code may look as follows:

    >>> import os
    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.s3_csv.read(
    ...     "animals/",
    ...     aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
    ...         bucket_name="datasets",
    ...         region="eu-west-3",
    ...         access_key=os.environ["S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    ...     ),
    ...     schema=InputSchema,
    ... )

    Alternatively, there might be a need to read the data from S3 storage, which is
    hosted in a different cloud and, therefore, requires to specify a custom endpoint.

    It can be done with the usage of an extra parameter `endpoint` of `AwsS3Settings`
    object. An example for the OVH-hosted bucket would then look as follows:

    >>> import os
    >>> import pathway as pw
    >>> t = pw.io.s3_csv.read(
    ...     "animals/",
    ...     aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
    ...         bucket_name="datasets",
    ...         region="rbx",
    ...         endpoint="s3.rbx.io.cloud.ovh.net",
    ...         access_key=os.environ["OVH_S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["OVH_S3_SECRET_ACCESS_KEY"],
    ...     ),
    ...     schema=InputSchema,
    ... )

    In case you are dealing with custom S3 buckets, there are
    `two ways <https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html>`_
    to work with paths in requests. The default and the one used by AWS S3 is a
    virtually hosted-style. However, some installations of S3 in, for example, min.io
    do require to use of path-style requests. If this is the case, you can use the
    parameter `with_path_style` of `AwsS3Settings`.

    Then, the code may look as follows:

    >>> import os
    >>> import pathway as pw
    >>> t = pw.io.s3_csv.read(
    ...     "animals/",
    ...     aws_s3_settings=pw.io.s3_csv.AwsS3Settings(
    ...         bucket_name="datasets",
    ...         endpoint="avv749.stackhero-network.com",
    ...         access_key=os.environ["MINIO_S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
    ...         with_path_style=True,
    ...     ),
    ...     schema=InputSchema,
    ... )
    """

    # legacy fields are not supported in pw.io.s3 reader, so the
    # schema should be constructed here
    if not schema:
        schema, _ = construct_schema_and_data_format(
            "csv",
            schema=schema,
            csv_settings=csv_settings,
            _stacklevel=5,
        )

    return s3_read(
        path,
        format="csv",
        aws_s3_settings=aws_s3_settings,
        schema=schema,
        mode=mode,
        csv_settings=csv_settings,
        name=name,
        autocommit_duration_ms=autocommit_duration_ms,
        debug_data=debug_data,
        _stacklevel=5,
        **kwargs,
    )

# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from pathway.internals import api, datasource
from pathway.internals.api import PathwayType
from pathway.internals.decorators import table_from_datasource
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    CsvParserSettings,
    check_deprecated_kwargs,
    construct_connector_properties,
    construct_schema_and_data_format,
    internal_connector_mode,
)
from pathway.io.s3 import AwsS3Settings


@runtime_type_check
@trace_user_frame
def read(
    path: str,
    aws_s3_settings: AwsS3Settings,
    *,
    schema: Optional[Type[Schema]] = None,
    csv_settings: Optional[CsvParserSettings] = None,
    mode: str = "streaming",
    autocommit_duration_ms: Optional[int] = 1500,
    persistent_id: Optional[str] = None,
    debug_data=None,
    value_columns: Optional[List[str]] = None,
    id_columns: Optional[List[str]] = None,
    types: Optional[Dict[str, PathwayType]] = None,
    default_values: Optional[Dict[str, Any]] = None,
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
        persistent_id: (unstable) An identifier, under which the state of the table
            will be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.
        debug_data: Static data replacing original one when debug mode is active.
        value_columns: Names of the columns to be extracted from the files. [will be deprecated soon]
        id_columns: In case the table should have a primary key generated according to
            a subset of its columns, the set of columns should be specified in this field.
            Otherwise, the primary key will be generated randomly. [will be deprecated soon]
        types: Dictionary containing the mapping between the columns and the data
            types (``pw.Type``) of the values of those columns. This parameter is optional, and if not
            provided the default type is ``pw.Type.ANY``. [will be deprecated soon]
        default_values: dictionary containing default values for columns replacing
            blank entries. The default value of the column must be specified explicitly,
            otherwise there will be no default value. [will be deprecated soon]

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

    In case you are dealing with custom S3 buckets, there are \
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
    internal_mode = internal_connector_mode(mode)
    if internal_mode == api.ConnectorMode.STREAMING_WITH_DELETIONS:
        raise NotImplementedError(
            "Snapshot mode is currently unsupported in S3-like connectors"
        )

    check_deprecated_kwargs(kwargs, ["poll_new_objects"])

    data_storage = api.DataStorage(
        storage_type="s3_csv",
        path=path,
        aws_s3_settings=aws_s3_settings.settings,
        csv_parser_settings=csv_settings.api_settings if csv_settings else None,
        mode=internal_mode,
        persistent_id=persistent_id,
    )
    schema, data_format = construct_schema_and_data_format(
        format="csv",
        schema=schema,
        value_columns=value_columns,
        primary_key=id_columns,
        types=types,
        default_values=default_values,
    )
    properties = construct_connector_properties(
        schema_properties=schema.properties(),
        commit_duration_ms=autocommit_duration_ms,
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            connector_properties=properties,
            schema=schema,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )

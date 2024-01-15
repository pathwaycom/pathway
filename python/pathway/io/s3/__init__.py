# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Any

from pathway.internals import api, datasource
from pathway.internals._io_helpers import AwsS3Settings
from pathway.internals.decorators import table_from_datasource
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    CsvParserSettings,
    construct_s3_data_storage,
    construct_schema_and_data_format,
    internal_connector_mode,
)


class DigitalOceanS3Settings:
    """Stores Digital Ocean S3 connection settings.

    Args:
        bucket_name: Name of Digital Ocean S3 bucket.
        access_key: Access key for the bucket.
        secret_access_key: Secret access key for the bucket.
        region: Region of the bucket.
    """

    @trace_user_frame
    def __init__(
        self,
        bucket_name,
        *,
        access_key=None,
        secret_access_key=None,
        region=None,
    ):
        self.settings = api.AwsS3Settings(
            bucket_name,
            access_key,
            secret_access_key,
            False,
            region,
            None,
        )


class WasabiS3Settings:
    """Stores Wasabi S3 connection settings.

    Args:
        bucket_name: Name of Wasabi S3 bucket.
        access_key: Access key for the bucket.
        secret_access_key: Secret access key for the bucket.
        region: Region of the bucket.
    """

    @trace_user_frame
    def __init__(
        self,
        bucket_name,
        *,
        access_key=None,
        secret_access_key=None,
        region=None,
    ):
        self.settings = api.AwsS3Settings(
            bucket_name,
            access_key,
            secret_access_key,
            False,
            f"wa-{region}",
            None,
        )


@check_arg_types
@trace_user_frame
def read(
    path: str,
    format: str,
    *,
    aws_s3_settings: AwsS3Settings | None = None,
    schema: type[Schema] | None = None,
    mode: str = "streaming",
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    persistent_id: str | None = None,
    autocommit_duration_ms: int | None = 1500,
    debug_data: Any = None,
) -> Table:
    """Reads a table from one or several objects in Amazon S3 bucket in the given
    format.

    In case the prefix of S3 path is specified, and there are several objects lying
    under this prefix, their order is determined according to their modification times:
    the smaller the modification time is, the earlier the file will be passed to the
    engine.

    Args:
        path: Path to an object or to a folder of objects in Amazon S3 bucket.
        aws_s3_settings: Connection parameters for the S3 account and the bucket.
        format: Format of data to be read. Currently "csv", "json" and "plaintext"
            formats are supported.
        schema: Schema of the resulting table.
        mode: If set to "streaming", the engine will wait for the new objects under the
            given path prefix. Set it to "static", it will only consider the available
            data and ingest all of it. Default value is "streaming".
        csv_settings: Settings for the CSV parser. This parameter is used only in case
            the specified format is "csv".
        json_field_paths: If the format is "json", this field allows to map field names
            into path in the read json object. For the field which require such mapping,
            it should be given in the format ``<field_name>: <path to be mapped>``,
            where the path to be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        persistent_id: (unstable) An identifier, under which the state of the table
            will be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Let's consider an object store, which is hosted in Amazon S3. The store contains
    datasets in the respective bucket and is located in the region eu-west-3. The goal
    is to read the dataset, located under the path ``animals/`` in this bucket.

    Let's suppose that the format of the dataset rows is jsonlines.

    Then, the code may look as follows:

    >>> import os
    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.s3.read(
    ...     "animals/",
    ...     aws_s3_settings=pw.io.s3.AwsS3Settings(
    ...         bucket_name="datasets",
    ...         region="eu-west-3",
    ...         access_key=os.environ["S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    ...     ),
    ...     format="json",
    ...     schema=InputSchema,
    ... )

    In case you are dealing with a public bucket, the parameters ``access_key`` and
    ``secret_access_key`` can be omitted. In this case, the read part will look as
    follows:

    >>> t = pw.io.s3.read(
    ...     "animals/",
    ...     aws_s3_settings=pw.io.s3.AwsS3Settings(
    ...         bucket_name="datasets",
    ...         region="eu-west-3",
    ...     ),
    ...     format="json",
    ...     schema=InputSchema,
    ... )
    """
    internal_mode = internal_connector_mode(mode)
    if aws_s3_settings:
        prepared_aws_settings = aws_s3_settings
    else:
        prepared_aws_settings = AwsS3Settings.new_from_path(path)

    data_storage = construct_s3_data_storage(
        path=path,
        rust_engine_s3_settings=prepared_aws_settings.settings,
        format=format,
        mode=internal_mode,
        csv_settings=csv_settings,
        persistent_id=persistent_id,
    )

    schema, data_format = construct_schema_and_data_format(
        format,
        schema=schema,
        csv_settings=csv_settings,
        json_field_paths=json_field_paths,
        _stacklevel=5,
    )
    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            schema=schema,
            data_source_options=data_source_options,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def read_from_digital_ocean(
    path: str,
    do_s3_settings: DigitalOceanS3Settings,
    format: str,
    *,
    schema: type[Schema] | None = None,
    mode: str = "streaming",
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    persistent_id: str | None = None,
    autocommit_duration_ms: int | None = 1500,
    debug_data: Any = None,
) -> Table:
    """Reads a table from one or several objects in Digital Ocean S3 bucket.

    In case the prefix of S3 path is specified, and there are several objects lying
    under this prefix, their order is determined according to their modification times:
    the smaller the modification time is, the earlier the file will be passed to the
    engine.

    Args:
        path: Path to an object or to a folder of objects in S3 bucket.
        do_s3_settings: Connection parameters for the account and the bucket.
        format: Format of data to be read. Currently "csv", "json" and "plaintext"
            formats are supported.
        schema: Schema of the resulting table.
        mode: If set to "streaming", the engine will wait for the new objects under the
            given path prefix. Set it to "static", it will only consider the available
            data and ingest all of it. Default value is "streaming".
        csv_settings: Settings for the CSV parser. This parameter is used only in case
            the specified format is "csv".
        json_field_paths: If the format is "json", this field allows to map field names
            into path in the read json object. For the field which require such mapping,
            it should be given in the format ``<field_name>: <path to be mapped>``,
            where the path to be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        persistent_id: (unstable) An identifier, under which the state of the table
            will be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Let's consider an object store, which is hosted in Digital Ocean S3. The store
    contains CSV datasets in the respective bucket and is located in the region ams3.
    The goal is to read the dataset, located under the path ``animals/`` in this bucket.

    Then, the code may look as follows:

    >>> import os
    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.s3.read_from_digital_ocean(
    ...     "animals/",
    ...     do_s3_settings=pw.io.s3.DigitalOceanS3Settings(
    ...         bucket_name="datasets",
    ...         region="ams3",
    ...         access_key=os.environ["DO_S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["DO_S3_SECRET_ACCESS_KEY"],
    ...     ),
    ...     format="csv",
    ...     schema=InputSchema,
    ... )
    """
    internal_mode = internal_connector_mode(mode)
    data_storage = construct_s3_data_storage(
        path=path,
        rust_engine_s3_settings=do_s3_settings.settings,
        format=format,
        mode=internal_mode,
        csv_settings=csv_settings,
        persistent_id=persistent_id,
    )

    schema, data_format = construct_schema_and_data_format(
        format,
        schema=schema,
        csv_settings=csv_settings,
        json_field_paths=json_field_paths,
        _stacklevel=5,
    )
    datasource_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=datasource_options,
            schema=schema,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def read_from_wasabi(
    path: str,
    wasabi_s3_settings: WasabiS3Settings,
    format: str,
    *,
    schema: type[Schema] | None = None,
    mode: str = "streaming",
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    persistent_id: str | None = None,
    autocommit_duration_ms: int | None = 1500,
    debug_data: Any = None,
) -> Table:
    """Reads a table from one or several objects in Wasabi S3 bucket.

    In case the prefix of S3 path is specified, and there are several objects lying under
    this prefix, their order is determined according to their modification times: the
    smaller the modification time is, the earlier the file will be passed to the engine.

    Args:
        path: Path to an object or to a folder of objects in S3 bucket.
        wasabi_s3_settings: Connection parameters for the account and the bucket.
        format: Format of data to be read. Currently "csv", "json" and "plaintext"
            formats are supported.
        schema: Schema of the resulting table.
        mode: If set to "streaming", the engine will wait for the new objects under the
            given path prefix. Set it to "static", it will only consider the available
            data and ingest all of it. Default value is "streaming".
        csv_settings: Settings for the CSV parser. This parameter is used only in case
            the specified format is "csv".
        json_field_paths: If the format is "json", this field allows to map field names
            into path in the read json object. For the field which require such mapping,
            it should be given in the format ``<field_name>: <path to be mapped>``,
            where the path to be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        persistent_id: (unstable) An identifier, under which the state of the table
            will be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Let's consider an object store, which is hosted in Wasabi S3. The store
    contains CSV datasets in the respective bucket and is located in the region us-west-1.
    The goal is to read the dataset, located under the path ``animals/`` in this bucket.

    Then, the code may look as follows:

    >>> import os
    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.s3.read_from_wasabi(
    ...     "animals/",
    ...     wasabi_s3_settings=pw.io.s3.WasabiS3Settings(
    ...         bucket_name="datasets",
    ...         region="us-west-1",
    ...         access_key=os.environ["WASABI_S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["WASABI_S3_SECRET_ACCESS_KEY"],
    ...     ),
    ...     format="csv",
    ...     schema=InputSchema,
    ... )
    """
    internal_mode = internal_connector_mode(mode)
    data_storage = construct_s3_data_storage(
        path=path,
        rust_engine_s3_settings=wasabi_s3_settings.settings,
        format=format,
        mode=internal_mode,
        csv_settings=csv_settings,
        persistent_id=persistent_id,
    )
    schema, data_format = construct_schema_and_data_format(
        format,
        schema=schema,
        csv_settings=csv_settings,
        json_field_paths=json_field_paths,
        _stacklevel=5,
    )
    datasource_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=datasource_options,
            schema=schema,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


# This is made to force AwsS3Settings documentation
__all__ = [
    "AwsS3Settings",
    "DigitalOceanS3Settings",
    "WasabiS3Settings",
    "read",
    "read_from_digital_ocean",
    "read_from_wasabi",
]

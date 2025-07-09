# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Any, Literal

from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import CsvParserSettings
from pathway.io.s3 import AwsS3Settings, read as s3_read


class MinIOSettings:
    """Stores MinIO bucket connection settings.

    Args:
        endpoint: Endpoint for the bucket.
        bucket_name: Name of a bucket.
        access_key: Access key for the bucket.
        secret_access_key: Secret access key for the bucket.
        region: Region of the bucket.
        with_path_style: Whether to use path-style addresses for bucket access. It defaults
            to True as this is the most widespread way to access MinIO, but can be overridden
            in case of a custom configuration.
    """

    def __init__(
        self,
        endpoint,
        bucket_name,
        access_key,
        secret_access_key,
        *,
        with_path_style=True,
        region=None,
    ):
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.with_path_style = with_path_style
        self.region = region

    def create_aws_settings(self):
        return AwsS3Settings(
            endpoint=self.endpoint,
            bucket_name=self.bucket_name,
            access_key=self.access_key,
            secret_access_key=self.secret_access_key,
            with_path_style=self.with_path_style,
            region=self.region,
        )


@check_arg_types
@trace_user_frame
def read(
    path: str,
    minio_settings: MinIOSettings,
    format: Literal["csv", "json", "plaintext", "plaintext_by_object", "binary"],
    *,
    schema: type[Schema] | None = None,
    mode: Literal["streaming", "static"] = "streaming",
    with_metadata: bool = False,
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    downloader_threads_count: int | None = None,
    name: str | None = None,
    autocommit_duration_ms: int | None = 1500,
    debug_data: Any = None,
    **kwargs,
) -> Table:
    """Reads a table from one or several objects from S3 bucket in MinIO.

    In case the prefix is specified, and there are several objects lying under this
    prefix, their order is determined according to their modification times: the smaller
    the modification time is, the earlier the file will be passed to the engine.

    Args:
        path: Path to an object or to a folder of objects in MinIO S3 bucket.
        minio_settings: Connection parameters for the MinIO account and the bucket.
        format: Format of data to be read. Currently ``csv``, ``json``, ``plaintext``,
            ``plaintext_by_object`` and ``binary`` formats are supported. The difference
            between ``plaintext`` and ``plaintext_by_object`` is how the input is
            tokenized: if the ``plaintext`` option is chosen, it's split by the newlines.
            Otherwise, the files are split in full and one row will correspond to one
            file. In case the ``binary`` format is specified, the data is read as raw
            bytes without UTF-8 parsing.
        schema: Schema of the resulting table. Not required for ``plaintext_by_object``
            and ``binary`` formats: if they are chosen, the contents of the read objects
            are stored in the column ``data``.
        mode: If set to ``streaming``, the engine waits for the new objects under the
            given path prefix. Set it to ``static``, it only considers the available
            data and ingest all of it. Default value is ``streaming``.
        with_metadata: When set to true, the connector will add an additional column
            named ``_metadata`` to the table. This column will be a JSON field that will
            contain an optional field ``modified_at``. Additionally, the column will also
            have an optional field named ``owner`` containing an ID of the object owner.
            Finally, the column will also contain a field named ``path`` that will show
            the full path to the object within a bucket from where a row was filled.
        csv_settings: Settings for the CSV parser. This parameter is used only in case
            the specified format is "csv".
        json_field_paths: If the format is "json", this field allows to map field names
            into path in the read json object. For the field which require such mapping,
            it should be given in the format ``<field_name>: <path to be mapped>``,
            where the path to be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        downloader_threads_count: The number of threads created to download the contents
            of the bucket under the given path. It defaults to the number of cores
            available on the machine. It is recommended to increase the number of
            threads if your bucket contains many small files.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Consider that there is a table, which is stored in CSV format in the min.io S3
    bucket. Then, you can use this method in order to connect and acquire its contents.

    It may look as follows:

    >>> import os
    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...     owner: str
    ...     pet: str
    >>> t = pw.io.minio.read(
    ...     "animals/",
    ...     minio_settings=pw.io.minio.MinIOSettings(
    ...         bucket_name="datasets",
    ...         endpoint="avv749.stackhero-network.com",
    ...         access_key=os.environ["MINIO_S3_ACCESS_KEY"],
    ...         secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
    ...     ),
    ...     format="csv",
    ...     schema=InputSchema,
    ... )

    Please note that this connector is **interoperable** with the **AWS S3** connector,
    therefore all examples concerning different data formats in ``pw.io.s3.read`` also
    work with min.io input.
    """

    return s3_read(
        path=path,
        aws_s3_settings=minio_settings.create_aws_settings(),
        format=format,
        schema=schema,
        csv_settings=csv_settings,
        mode=mode,
        with_metadata=with_metadata,
        autocommit_duration_ms=autocommit_duration_ms,
        name=name,
        json_field_paths=json_field_paths,
        downloader_threads_count=downloader_threads_count,
        debug_data=debug_data,
        _stacklevel=5,
        **kwargs,
    )

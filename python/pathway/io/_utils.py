# Copyright © 2024 Pathway

from __future__ import annotations

import functools
import warnings
from dataclasses import KW_ONLY, dataclass
from typing import TYPE_CHECKING, Any, Iterable

import pathway.internals as pw
import pathway.internals.dtype as dt
from pathway.internals import api
from pathway.internals._io_helpers import (
    AwsS3Settings,
    SchemaRegistrySettings,
    _form_value_fields,
    _format_output_value_fields,
)
from pathway.internals.api import ConnectorMode, PathwayType, ReadMethod
from pathway.internals.expression import ColumnReference
from pathway.internals.schema import Schema
from pathway.internals.table import Table

if TYPE_CHECKING:
    from pathway.io.minio import MinIOSettings
    from pathway.io.s3 import DigitalOceanS3Settings, WasabiS3Settings

STATIC_MODE_NAME = "static"
STREAMING_MODE_NAME = "streaming"
SNAPSHOT_MODE_NAME = "streaming_with_deletions"  # deprecated

METADATA_COLUMN_NAME = "_metadata"

STATUS_SIZE_LIMIT_EXCEEDED = "size_limit_exceeded"
STATUS_DOWNLOADED = "downloaded"
STATUS_SYMLINKS_NOT_SUPPORTED = "skipped_symlinks_not_supported"

_INPUT_MODES_MAPPING = {
    STATIC_MODE_NAME: ConnectorMode.STATIC,
    STREAMING_MODE_NAME: ConnectorMode.STREAMING,
    SNAPSHOT_MODE_NAME: ConnectorMode.STREAMING,
}

_DATA_FORMAT_MAPPING = {
    "csv": "dsv",
    "plaintext": "identity",
    "json": "jsonlines",
    "raw": "identity",
    "binary": "identity",
    "plaintext_by_file": "identity",
    "plaintext_by_object": "identity",
    "only_metadata": "identity",
}

_PATHWAY_TYPE_MAPPING: dict[PathwayType, dt.DType] = {
    PathwayType.INT: dt.INT,
    PathwayType.BOOL: dt.BOOL,
    PathwayType.FLOAT: dt.FLOAT,
    PathwayType.STRING: dt.STR,
    PathwayType.ANY: dt.ANY,
    PathwayType.POINTER: dt.ANY_POINTER,
    PathwayType.DATE_TIME_NAIVE: dt.DATE_TIME_NAIVE,
    PathwayType.DATE_TIME_UTC: dt.DATE_TIME_UTC,
    PathwayType.DURATION: dt.DURATION,
    PathwayType.JSON: dt.JSON,
    PathwayType.BYTES: dt.BYTES,
    PathwayType.PY_OBJECT_WRAPPER: dt.ANY_PY_OBJECT_WRAPPER,
}

SUPPORTED_INPUT_FORMATS: set[str] = {
    "csv",
    "json",
    "plaintext",
    "raw",
    "binary",
    "plaintext_by_file",
    "plaintext_by_object",
    "only_metadata",
}


class RawDataSchema(pw.Schema):
    data: bytes


class PlaintextDataSchema(pw.Schema):
    data: str


class MetadataSchema(Schema):
    _metadata: dict


def get_data_format_type(format: str, supported_formats: set[str]):
    if format not in _DATA_FORMAT_MAPPING or format not in supported_formats:
        raise ValueError(f"data format `{format}` not supported")
    return _DATA_FORMAT_MAPPING[format]


def check_deprecated_kwargs(
    kwargs: dict[str, Any], deprecated_kwarg_names: list[str], stacklevel: int = 2
):
    for kwarg_name in deprecated_kwarg_names:
        if kwarg_name in kwargs:
            warnings.warn(
                f"'{kwarg_name}' is deprecated and will be ignored",
                DeprecationWarning,
                stacklevel=stacklevel + 1,
            )
            kwargs.pop(kwarg_name)
    deprecated_kwargs_with_custom_report = ["persistent_id"]
    unexpected_args_list = []
    for kwarg in kwargs.keys():
        if kwarg not in deprecated_kwargs_with_custom_report:
            unexpected_args_list.append(kwarg)
    if unexpected_args_list:
        unexpected_arg_names = ", ".join(repr(arg) for arg in unexpected_args_list)
        raise TypeError(f"Got unexpected keyword arguments: {unexpected_arg_names}")


def internal_connector_mode(mode: str | api.ConnectorMode) -> api.ConnectorMode:
    if isinstance(mode, api.ConnectorMode):
        return mode
    internal_mode = _INPUT_MODES_MAPPING.get(mode)
    if not internal_mode:
        raise ValueError(
            "Unknown mode: {}. Only {} are supported".format(
                mode, ", ".join(_INPUT_MODES_MAPPING.keys())
            )
        )

    return internal_mode


def internal_read_method(format: str) -> ReadMethod:
    if format in (
        "binary",
        "plaintext_by_file",
        "plaintext_by_object",
        "only_metadata",
    ):
        return ReadMethod.FULL
    return ReadMethod.BY_LINE


class CsvParserSettings:
    """
    Class representing settings for the CSV parser.

    Args:
        delimiter: Field delimiter to use when parsing CSV.
        quote: Quote character to use when parsing CSV.
        escape: What character to use for escaping fields in CSV.
        enable_double_quote_escapes: Enable escapes of double quotes.
        enable_quoting: Enable quoting for the fields.
        comment_character: If specified, the lines starting with the comment
            character will be treated as comments and therefore, will be ignored by
            parser
    """

    def __init__(
        self,
        delimiter=",",
        quote='"',
        escape=None,
        enable_double_quote_escapes=True,
        enable_quoting=True,
        comment_character=None,
    ):
        self.api_settings = api.CsvParserSettings(
            delimiter,
            quote,
            escape,
            enable_double_quote_escapes,
            enable_quoting,
            comment_character,
        )


def read_schema(
    schema: type[Schema],
) -> tuple[type[Schema], dict[str, Any]]:
    value_fields = _form_value_fields(schema)
    return schema, dict(
        # There is a distinction between an empty set of columns denoting
        # the primary key and None. If any (including empty) set of keys if provided,
        # then it will be used to compute the primary key.
        key_field_names=schema.primary_key_columns(),
        value_fields=value_fields,
    )


def assert_schema_not_none(
    schema: type[Schema] | None,
    data_format_type: str | None = None,
) -> type[Schema]:
    if schema is None:
        if data_format_type == "dsv":
            raise ValueError(
                "Schema must be specified. "
                "Consider using `pw.schema_from_csv` for generating schema from a CSV file"
            )
        else:
            raise ValueError("Schema must be specified.")
    else:
        return schema


def construct_schema_and_data_format(
    format: str,
    *,
    schema: type[Schema] | None = None,
    with_metadata: bool = False,
    autogenerate_key: bool = False,
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    schema_registry_settings: SchemaRegistrySettings | None = None,
    _stacklevel: int = 1,
) -> tuple[type[Schema], api.DataFormat]:
    data_format_type = get_data_format_type(format, SUPPORTED_INPUT_FORMATS)

    if data_format_type == "identity":
        kwargs = locals()
        unexpected_params = [
            "schema",
            "csv_settings",
            "json_field_paths",
        ]
        for param in unexpected_params:
            if param in kwargs and kwargs[param] is not None:
                raise ValueError(f"Unexpected argument for plaintext format: {param}")

        parse_utf8 = format not in ("binary", "only_metadata")
        if parse_utf8:
            schema = PlaintextDataSchema
        else:
            schema = RawDataSchema

        if with_metadata:
            schema |= MetadataSchema
        schema, api_schema = read_schema(schema)

        return schema, api.DataFormat(
            format_type=data_format_type,
            **api_schema,
            parse_utf8=parse_utf8,
            key_generation_policy=(
                api.KeyGenerationPolicy.ALWAYS_AUTOGENERATE
                if autogenerate_key
                else api.KeyGenerationPolicy.PREFER_MESSAGE_KEY
            ),
            schema_registry_settings=maybe_schema_registry_settings(
                schema_registry_settings
            ),
        )

    schema = assert_schema_not_none(schema, data_format_type)
    if with_metadata:
        schema |= MetadataSchema

    schema, api_schema = read_schema(schema)
    if data_format_type == "dsv":
        if json_field_paths is not None:
            raise ValueError("Unexpected argument for csv format: json_field_paths")
        return schema, api.DataFormat(
            **api_schema,
            format_type=data_format_type,
            delimiter=",",
            schema_registry_settings=maybe_schema_registry_settings(
                schema_registry_settings
            ),
        )
    elif data_format_type == "jsonlines":
        if csv_settings is not None:
            raise ValueError("Unexpected argument for json format: csv_settings")
        return schema, api.DataFormat(
            **api_schema,
            format_type=data_format_type,
            column_paths=json_field_paths,
            schema_registry_settings=maybe_schema_registry_settings(
                schema_registry_settings
            ),
        )
    else:
        raise ValueError(f"data format `{format}` not supported")


def check_raw_and_plaintext_only_kwargs_for_message_queues(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        data_format = kwargs.get("format")
        if data_format not in ("raw", "plaintext"):
            if "value" in kwargs and kwargs["value"] is not None:
                raise ValueError(
                    f"Unsupported argument for {data_format} format: 'value'"
                )

        return f(*args, **kwargs)

    return wrapper


@dataclass(frozen=True)
class MessageQueueOutputFormat:
    _: KW_ONLY
    table: Table
    key_field_index: int | None
    header_fields: dict[str, int]
    data_format: api.DataFormat
    topic_name_index: int | None

    @classmethod
    def construct(
        cls,
        table: Table,
        *,
        format: str = "json",
        delimiter: str = ",",
        key: ColumnReference | None = None,
        value: ColumnReference | None = None,
        headers: Iterable[ColumnReference] | None = None,
        topic_name: ColumnReference | None = None,
        schema_registry_settings: SchemaRegistrySettings | None = None,
        subject: str | None = None,
    ) -> MessageQueueOutputFormat:
        key_field_index = None
        header_fields: dict[str, int] = {}
        extracted_field_indices: dict[str, int] = {}
        columns_to_extract: list[ColumnReference] = []
        allowed_column_types = (dt.BYTES, dt.STR, dt.ANY)

        if topic_name is not None:
            topic_name_index = cls.add_column_reference_to_extract(
                topic_name, columns_to_extract, extracted_field_indices
            )
            if topic_name._column.dtype not in (dt.STR, dt.ANY):
                raise ValueError(
                    "The topic name column must have a string type, however "
                    f"{topic_name._column.dtype.typehint} is used"
                )
        else:
            topic_name_index = None

        # Common part for all formats: obtain key field index and prepare header fields
        if key is not None:
            if table[key._name]._column.dtype not in allowed_column_types:
                raise ValueError(
                    f"The key column should be of the type '{allowed_column_types[0]}'"
                )
            key_field_index = cls.add_column_reference_to_extract(
                key, columns_to_extract, extracted_field_indices
            )
        if headers is not None:
            for header in headers:
                header_fields[header.name] = cls.add_column_reference_to_extract(
                    header, columns_to_extract, extracted_field_indices
                )

        # Format-dependent parts: handle json and dsv separately
        if format == "json" or format == "dsv":
            for column_name in table._columns:
                cls.add_column_reference_to_extract(
                    table[column_name], columns_to_extract, extracted_field_indices
                )
            table = table.select(*columns_to_extract)
            data_format = api.DataFormat(
                format_type="jsonlines" if format == "json" else "dsv",
                key_field_names=[],
                value_fields=_format_output_value_fields(table),
                delimiter=delimiter,
                schema_registry_settings=maybe_schema_registry_settings(
                    schema_registry_settings
                ),
                subject=subject,
            )
        elif format == "raw" or format == "plaintext":
            value_field_index = None
            if key is not None and value is None:
                raise ValueError("'value' must be specified if 'key' is not None")
            if value is not None:
                value_field_index = cls.add_column_reference_to_extract(
                    value, columns_to_extract, extracted_field_indices
                )
            else:
                column_names = list(table._columns.keys())
                if len(column_names) != 1:
                    raise ValueError(
                        f"'{format}' format without explicit 'value' specification "
                        "can only be used with single-column tables"
                    )
                value = table[column_names[0]]
                value_field_index = cls.add_column_reference_to_extract(
                    value, columns_to_extract, extracted_field_indices
                )

            table = table.select(*columns_to_extract)
            if table[value._name]._column.dtype not in allowed_column_types:
                raise ValueError(
                    f"The value column should be of the type '{allowed_column_types[0]}'"
                )

            data_format = api.DataFormat(
                format_type="single_column",
                key_field_names=[],
                value_fields=_format_output_value_fields(table),
                value_field_index=value_field_index,
                schema_registry_settings=maybe_schema_registry_settings(
                    schema_registry_settings
                ),
                subject=subject,
            )
        else:
            raise ValueError(f"Unsupported format: {format}")

        return cls(
            table=table,
            key_field_index=key_field_index,
            header_fields=header_fields,
            data_format=data_format,
            topic_name_index=topic_name_index,
        )

    @staticmethod
    def add_column_reference_to_extract(
        column_reference: ColumnReference,
        selection_list: list[ColumnReference],
        field_indices: dict[str, int],
    ) -> int:
        column_name = column_reference.name

        index_in_new_table = field_indices.get(column_name)
        if index_in_new_table is not None:
            # This column will already be selected, no need to do anything
            return index_in_new_table

        index_in_new_table = len(selection_list)
        field_indices[column_name] = index_in_new_table
        selection_list.append(column_reference)
        return index_in_new_table


def maybe_schema_registry_settings(
    schema_registry_settings: SchemaRegistrySettings | None,
) -> api.SchemaRegistrySettings | None:
    if schema_registry_settings is not None:
        return schema_registry_settings.to_engine
    return None


def _get_unique_name(
    name: str | None, kwargs: dict[str, Any], stacklevel: int = 6
) -> str:
    deprecated_name = kwargs.get("persistent_id")
    if name is not None:
        if deprecated_name is not None:
            raise ValueError(
                "'persistent_id' and 'name' should not be used together. Please use 'name' only."
            )
        return name
    if deprecated_name is not None:
        warnings.warn(
            "'persistent_id' is deprecated. Please use 'name' instead.",
            DeprecationWarning,
            stacklevel=stacklevel,
        )
    return deprecated_name


def _prepare_s3_connection_settings(
    s3_connection_settings: (
        AwsS3Settings | MinIOSettings | WasabiS3Settings | DigitalOceanS3Settings | None
    ),
) -> AwsS3Settings | None:
    if isinstance(s3_connection_settings, AwsS3Settings):
        return s3_connection_settings
    elif s3_connection_settings is None:
        return None
    else:
        return s3_connection_settings.create_aws_settings()


def _prepare_s3_connection_engine_settings(
    s3_connection_settings: (
        AwsS3Settings | MinIOSettings | WasabiS3Settings | DigitalOceanS3Settings | None
    ),
) -> api.AwsS3Settings | None:
    aws_s3_settings = _prepare_s3_connection_settings(s3_connection_settings)
    if aws_s3_settings is None:
        return None
    return aws_s3_settings.settings

# Copyright © 2024 Pathway

import dataclasses
import warnings
from typing import Any

import pathway.internals as pw
from pathway.internals import api, dtype as dt
from pathway.internals._io_helpers import _form_value_fields
from pathway.internals.api import ConnectorMode, PathwayType, ReadMethod
from pathway.internals.schema import ColumnDefinition, Schema

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
}


class RawDataSchema(pw.Schema):
    data: Any


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
    if kwargs:
        unexpected_arg_names = ", ".join(repr(arg) for arg in kwargs.keys())
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
    if format in ("binary", "plaintext_by_file", "plaintext_by_object"):
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
        comment_character: If specified, the lines starting with the comment \
character will be treated as comments and therefore, will be ignored by \
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


def _compat_schema(
    value_columns: list[str] | None,
    primary_key: list[str] | None,
    types: dict[str, api.PathwayType] | None,
    default_values: dict[str, Any] | None,
):
    columns: dict[str, ColumnDefinition] = {
        name: pw.column_definition(primary_key=True) for name in primary_key or {}
    }
    columns.update(
        {
            name: pw.column_definition()
            for name in (value_columns or {})
            if name not in columns
        }
    )
    if types is not None:
        for name, dtype in types.items():
            columns[name] = dataclasses.replace(
                columns[name],
                dtype=_PATHWAY_TYPE_MAPPING.get(dtype, dt.ANY),
            )
    if default_values is not None:
        for name, default_value in default_values.items():
            columns[name] = dataclasses.replace(
                columns[name], default_value=default_value
            )
    return pw.schema_builder(columns=columns)


def _read_schema(
    *,
    schema: type[Schema] | None,
    value_columns: list[str] | None,
    primary_key: list[str] | None,
    types: dict[str, api.PathwayType] | None,
    default_values: dict[str, Any] | None,
    _stacklevel: int = 1,
) -> type[Schema]:
    kwargs = locals()
    deprecated_kwargs = ["value_columns", "primary_key", "types", "default_values"]

    if schema is None:
        for name in deprecated_kwargs:
            if name in kwargs and kwargs[name] is not None:
                warnings.warn(
                    "'{}' is deprecated and will be removed soon. Please use `schema` instead".format(
                        name
                    ),
                    DeprecationWarning,
                    stacklevel=_stacklevel + 1,
                )
        schema = _compat_schema(
            primary_key=primary_key,
            value_columns=value_columns,
            types=types,
            default_values=default_values,
        )
    else:
        for name in deprecated_kwargs:
            if kwargs[name] is not None:
                raise ValueError(f"cannot use `schema` and `{name}`")

    return schema


def read_schema(
    *,
    schema: type[Schema] | None,
    value_columns: list[str] | None = None,
    primary_key: list[str] | None = None,
    types: dict[str, api.PathwayType] | None = None,
    default_values: dict[str, Any] | None = None,
    _stacklevel: int = 1,
) -> tuple[type[Schema], dict[str, Any]]:
    schema = _read_schema(
        schema=schema,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
        _stacklevel=_stacklevel + 1,
    )
    value_fields = _form_value_fields(schema)
    return schema, dict(
        # There is a distinction between an empty set of columns denoting
        # the primary key and None. If any (including empty) set of keys if provided,
        # then it will be used to compute the primary key.
        key_field_names=schema.primary_key_columns(),
        value_fields=value_fields,
    )


def assert_schema_or_value_columns_not_none(
    schema: type[Schema] | None,
    value_columns: list[str] | None,
    data_format_type: str | None = None,
):
    if schema is None and value_columns is None:
        if data_format_type == "dsv":
            raise ValueError(
                "Neither schema nor value_columns were specified. "
                "Consider using `pw.schema_from_csv` for generating schema from a CSV file"
            )
        else:
            raise ValueError("Neither schema nor value_columns were specified")


def construct_schema_and_data_format(
    format: str,
    *,
    schema: type[Schema] | None = None,
    with_metadata: bool = False,
    autogenerate_key: bool = False,
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    value_columns: list[str] | None = None,
    primary_key: list[str] | None = None,
    types: dict[str, PathwayType] | None = None,
    default_values: dict[str, Any] | None = None,
    _stacklevel: int = 1,
) -> tuple[type[Schema], api.DataFormat]:
    data_format_type = get_data_format_type(format, SUPPORTED_INPUT_FORMATS)

    if data_format_type == "identity":
        kwargs = locals()
        unexpected_params = [
            "schema",
            "value_columns",
            "primary_key",
            "csv_settings",
            "json_field_paths",
            "types",
        ]
        for param in unexpected_params:
            if param in kwargs and kwargs[param] is not None:
                raise ValueError(f"Unexpected argument for plaintext format: {param}")

        schema = RawDataSchema
        if with_metadata:
            schema |= MetadataSchema
        schema, api_schema = read_schema(
            schema=schema,
            value_columns=None,
            primary_key=None,
            types=None,
            default_values=None,
        )

        return schema, api.DataFormat(
            format_type=data_format_type,
            **api_schema,
            parse_utf8=(format != "binary"),
            key_generation_policy=(
                api.KeyGenerationPolicy.ALWAYS_AUTOGENERATE
                if autogenerate_key
                else api.KeyGenerationPolicy.PREFER_MESSAGE_KEY
            ),
        )

    assert_schema_or_value_columns_not_none(schema, value_columns, data_format_type)

    if with_metadata:
        if schema is not None:
            schema |= MetadataSchema
        elif value_columns is not None:
            value_columns.append(METADATA_COLUMN_NAME)
        else:
            raise ValueError("Neither schema nor value_columns were specified")

    schema, api_schema = read_schema(
        schema=schema,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
        _stacklevel=_stacklevel + 1,
    )
    if data_format_type == "dsv":
        if json_field_paths is not None:
            raise ValueError("Unexpected argument for csv format: json_field_paths")
        return schema, api.DataFormat(
            **api_schema,
            format_type=data_format_type,
            delimiter=",",
        )
    elif data_format_type == "jsonlines":
        if csv_settings is not None:
            raise ValueError("Unexpected argument for json format: csv_settings")
        return schema, api.DataFormat(
            **api_schema,
            format_type=data_format_type,
            column_paths=json_field_paths,
        )
    else:
        raise ValueError(f"data format `{format}` not supported")


def construct_s3_data_storage(
    path: str,
    rust_engine_s3_settings: api.AwsS3Settings,
    format: str,
    mode: str | api.ConnectorMode,
    *,
    downloader_threads_count: int | None = None,
    csv_settings: CsvParserSettings | None = None,
    persistent_id: str | None = None,
):
    if format == "csv":
        return api.DataStorage(
            storage_type="s3_csv",
            path=path,
            aws_s3_settings=rust_engine_s3_settings,
            csv_parser_settings=csv_settings.api_settings if csv_settings else None,
            downloader_threads_count=downloader_threads_count,
            mode=internal_connector_mode(mode),
            persistent_id=persistent_id,
        )
    else:
        return api.DataStorage(
            storage_type="s3",
            path=path,
            aws_s3_settings=rust_engine_s3_settings,
            mode=internal_connector_mode(mode),
            read_method=internal_read_method(format),
            downloader_threads_count=downloader_threads_count,
            persistent_id=persistent_id,
        )

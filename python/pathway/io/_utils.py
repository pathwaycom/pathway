# Copyright © 2023 Pathway

import dataclasses
import warnings
from typing import Any, Dict, List, Optional, Set, Tuple, Type

import numpy as np

import pathway.internals as pw
from pathway.internals import api
from pathway.internals import dtype as dt
from pathway.internals._io_helpers import _form_value_fields
from pathway.internals.api import ConnectorMode, PathwayType, ReadMethod
from pathway.internals.schema import ColumnDefinition, Schema, SchemaProperties

STATIC_MODE_NAME = "static"
STREAMING_MODE_NAME = "streaming"
SNAPSHOT_MODE_NAME = "streaming_with_deletions"

_INPUT_MODES_MAPPING = {
    STATIC_MODE_NAME: ConnectorMode.STATIC,
    STREAMING_MODE_NAME: ConnectorMode.SIMPLE_STREAMING,
    SNAPSHOT_MODE_NAME: ConnectorMode.STREAMING_WITH_DELETIONS,
}

_DATA_FORMAT_MAPPING = {
    "csv": "dsv",
    "plaintext": "identity",
    "json": "jsonlines",
    "raw": "identity",
    "binary": "identity",
}

_PATHWAY_TYPE_MAPPING: Dict[PathwayType, Any] = {
    PathwayType.INT: int,
    PathwayType.BOOL: bool,
    PathwayType.FLOAT: float,
    PathwayType.STRING: str,
    PathwayType.ANY: Any,
    PathwayType.POINTER: api.BasePointer,
    PathwayType.DATE_TIME_NAIVE: dt.DATE_TIME_NAIVE,
    PathwayType.DATE_TIME_UTC: dt.DATE_TIME_UTC,
    PathwayType.DURATION: dt.DURATION,
    PathwayType.ARRAY: np.ndarray,
    PathwayType.JSON: dict,
}

SUPPORTED_INPUT_FORMATS: Set[str] = set(
    [
        "csv",
        "json",
        "plaintext",
        "raw",
        "binary",
    ]
)


class RawDataSchema(pw.Schema):
    data: Any


def get_data_format_type(format: str, supported_formats: Set[str]):
    if format not in _DATA_FORMAT_MAPPING or format not in supported_formats:
        raise ValueError(f"data format `{format}` not supported")
    return _DATA_FORMAT_MAPPING[format]


def check_deprecated_kwargs(kwargs: Dict[str, Any], deprecated_kwarg_names: List[str]):
    for kwarg_name in deprecated_kwarg_names:
        if kwarg_name in kwargs:
            warnings.warn(
                "'{}' is deprecated and will be ignored".format(kwarg_name),
                DeprecationWarning,
                stacklevel=2,
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
    if format == "binary":
        return ReadMethod.FULL
    return ReadMethod.BY_LINE


class CsvParserSettings:
    """Class representing settings for the CSV parser."""

    def __init__(
        self,
        delimiter=",",
        quote='"',
        escape=None,
        enable_double_quote_escapes=True,
        enable_quoting=True,
        comment_character=None,
    ):
        """Constructs the CSV parser settings.

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
        self.api_settings = api.CsvParserSettings(
            delimiter,
            quote,
            escape,
            enable_double_quote_escapes,
            enable_quoting,
            comment_character,
        )


def _compat_schema(
    value_columns: Optional[List[str]],
    primary_key: Optional[List[str]],
    types: Optional[Dict[str, api.PathwayType]],
    default_values: Optional[Dict[str, Any]],
):
    columns: Dict[str, ColumnDefinition] = {
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
                dtype=dt.wrap(_PATHWAY_TYPE_MAPPING.get(dtype, Any)),
            )
    if default_values is not None:
        for name, default_value in default_values.items():
            columns[name] = dataclasses.replace(
                columns[name], default_value=default_value
            )
    return pw.schema_builder(columns=columns)


def _read_schema(
    *,
    schema: Optional[Type[Schema]],
    value_columns: Optional[List[str]],
    primary_key: Optional[List[str]],
    types: Optional[Dict[str, api.PathwayType]],
    default_values: Optional[Dict[str, Any]],
) -> Type[Schema]:
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
                    stacklevel=2,
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
    schema: Optional[Type[Schema]],
    value_columns: Optional[List[str]],
    primary_key: Optional[List[str]],
    types: Optional[Dict[str, api.PathwayType]],
    default_values: Optional[Dict[str, Any]],
) -> Tuple[Type[Schema], Dict[str, Any]]:
    schema = _read_schema(
        schema=schema,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
    )
    value_fields = _form_value_fields(schema)
    return schema, dict(
        # There is a distinction between an empty set of columns denoting
        # the primary key and None. If any (including empty) set of keys if provided,
        # then it will be used to compute the primary key.
        key_field_names=schema.primary_key_columns(),
        value_fields=value_fields,
    )


def construct_schema_and_data_format(
    format: str,
    *,
    schema: Optional[Type[Schema]] = None,
    csv_settings: Optional[CsvParserSettings] = None,
    json_field_paths: Optional[Dict[str, str]] = None,
    value_columns: Optional[List[str]] = None,
    primary_key: Optional[List[str]] = None,
    types: Optional[Dict[str, PathwayType]] = None,
    default_values: Optional[Dict[str, Any]] = None,
) -> Tuple[Type[Schema], api.DataFormat]:
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
                raise ValueError(
                    "Unexpected argument for plaintext format: {}".format(param)
                )

        return RawDataSchema, api.DataFormat(
            format_type=data_format_type,
            key_field_names=None,
            value_fields=[api.ValueField("data", PathwayType.ANY)],
            parse_utf8=(format != "binary"),
        )
    schema, api_schema = read_schema(
        schema=schema,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
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
    csv_settings: Optional[CsvParserSettings] = None,
    persistent_id: Optional[str] = None,
):
    if format == "csv":
        return api.DataStorage(
            storage_type="s3_csv",
            path=path,
            aws_s3_settings=rust_engine_s3_settings,
            csv_parser_settings=csv_settings.api_settings if csv_settings else None,
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
            persistent_id=persistent_id,
        )


def construct_connector_properties(
    schema_properties: SchemaProperties = SchemaProperties(),
    commit_duration_ms: Optional[int] = None,
    unsafe_trusted_ids: bool = False,
):
    return api.ConnectorProperties(
        commit_duration_ms=commit_duration_ms,
        unsafe_trusted_ids=unsafe_trusted_ids,
        append_only=schema_properties.append_only,
    )

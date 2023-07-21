# Copyright © 2023 Pathway

import warnings
from typing import Any, Dict, List, Optional, Set, Type

from pathway.internals import api
from pathway.internals._io_helpers import _form_value_fields
from pathway.internals.api import PathwayType
from pathway.internals.dtype import _is_optional, _strip_optional  # type:ignore
from pathway.internals.schema import Schema

SUPPORTED_INPUT_MODES: Set[str] = set(
    [
        "static",
        "streaming",
    ]
)

STREAMING_MODE_NAME = "streaming"


_DATA_FORMAT_MAPPING = {
    "csv": "dsv",
    "plaintext": "identity",
    "json": "jsonlines",
    "raw": "identity",
}


SUPPORTED_INPUT_FORMATS: Set[str] = set(
    [
        "csv",
        "json",
        "plaintext",
        "raw",
    ]
)


_TYPE_MAPPING = {
    Any: api.PathwayType.ANY,
    int: api.PathwayType.INT,
    str: api.PathwayType.STRING,
    float: api.PathwayType.FLOAT,
    bool: api.PathwayType.BOOL,
}


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


def need_poll_new_objects(mode: str) -> bool:
    if mode not in SUPPORTED_INPUT_MODES:
        raise ValueError(
            "Unknown mode: {}. Only {} are supported".format(
                format, ", ".join(SUPPORTED_INPUT_MODES)
            )
        )

    return mode == STREAMING_MODE_NAME


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


def _read_schema(
    *,
    schema: Optional[Type[Schema]],
    value_columns: Optional[List[str]],
    primary_key: Optional[List[str]],
    types: Optional[Dict[str, api.PathwayType]],
    default_values: Optional[Dict[str, Any]],
):
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
        return (value_columns, primary_key, types, default_values)
    else:
        for name in deprecated_kwargs:
            if kwargs[name] is not None:
                raise ValueError(f"cannot use `schema` and `{name}`")

    # XXX fix mapping schema types to PathwayType
    def _unoptionalize(dtype):
        if _is_optional(dtype):
            return _strip_optional(dtype)
        else:
            return dtype

    types = {
        name: _TYPE_MAPPING[_unoptionalize(dtype)]
        for name, dtype in schema.as_dict().items()
    }
    return (
        schema.column_names(),
        schema.primary_key_columns(),
        types,
        schema.default_values(),
    )


def read_schema(
    *,
    schema: Optional[Type[Schema]],
    value_columns: Optional[List[str]],
    primary_key: Optional[List[str]],
    types: Optional[Dict[str, api.PathwayType]],
    default_values: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    (value_columns, primary_key, types, default_values) = _read_schema(
        schema=schema,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
    )
    if value_columns is None:
        raise ValueError("cannot determine input schema columns")
    value_fields = _form_value_fields(
        id_fields=primary_key,
        value_fields=value_columns,
        schema_types=types,
        default_values=default_values,
    )
    return dict(
        # There is a distinction between an empty set of columns denoting
        # the primary key and None. If any (including empty) set of keys if provided,
        # then it will be used to compute the primary key.
        key_field_names=primary_key or None,
        value_fields=value_fields,
    )


def construct_input_data_format(
    format: str,
    *,
    schema: Optional[Type[Schema]] = None,
    csv_settings: Optional[CsvParserSettings] = None,
    json_field_paths: Optional[Dict[str, str]] = None,
    value_columns: Optional[List[str]] = None,
    primary_key: Optional[List[str]] = None,
    types: Optional[Dict[str, PathwayType]] = None,
    default_values: Optional[Dict[str, Any]] = None,
):
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

        return api.DataFormat(
            format_type=data_format_type,
            key_field_names=["key"],
            value_fields=[api.ValueField("data", PathwayType.ANY)],
        )
    else:
        schema_definition = read_schema(
            schema=schema,
            value_columns=value_columns,
            primary_key=primary_key,
            types=types,
            default_values=default_values,
        )
        if data_format_type == "dsv":
            if json_field_paths is not None:
                raise ValueError("Unexpected argument for csv format: json_field_paths")
            return api.DataFormat(
                **schema_definition,
                format_type=data_format_type,
                delimiter=",",
            )
        elif data_format_type == "jsonlines":
            if csv_settings is not None:
                raise ValueError("Unexpected argument for json format: csv_settings")
            return api.DataFormat(
                **schema_definition,
                format_type=data_format_type,
                column_paths=json_field_paths,
            )


def construct_s3_data_storage(
    path: str,
    rust_engine_s3_settings: api.AwsS3Settings,
    format: str,
    poll_new_objects: bool,
    *,
    csv_settings: Optional[CsvParserSettings] = None,
    persistent_id: Optional[int] = None,
):
    if format == "csv":
        return api.DataStorage(
            storage_type="s3_csv",
            path=path,
            aws_s3_settings=rust_engine_s3_settings,
            csv_parser_settings=csv_settings.api_settings if csv_settings else None,
            poll_new_objects=poll_new_objects,
            persistent_id=persistent_id,
        )
    else:
        return api.DataStorage(
            storage_type="s3",
            path=path,
            aws_s3_settings=rust_engine_s3_settings,
            poll_new_objects=poll_new_objects,
            persistent_id=persistent_id,
        )

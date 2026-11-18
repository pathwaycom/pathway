# Copyright © 2026 Pathway

from __future__ import annotations

import datetime
import functools
import inspect
import json
import keyword
import logging
import math
import os
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
from pathway.internals.schema import Schema, schema_from_dict
from pathway.internals.table import Table

if TYPE_CHECKING:
    from pathway.io.minio import MinIOSettings
    from pathway.io.s3 import DigitalOceanS3Settings, WasabiS3Settings

STATIC_MODE_NAME = "static"
STREAMING_MODE_NAME = "streaming"
SNAPSHOT_MODE_NAME = "streaming_with_deletions"  # deprecated
SNAPSHOT_OUTPUT_TABLE_TYPE = "snapshot"

METADATA_COLUMN_NAME = "_metadata"
MESSAGE_QUEUE_KEY_COLUMN_NAME = "key"


class EngineTimeMarker:
    """The type of the ``pw.io.ENGINE_TIME`` marker. Do not instantiate it:
    pass the ``pw.io.ENGINE_TIME`` singleton where a connector accepts it."""

    def __repr__(self) -> str:
        return "pathway.io.ENGINE_TIME"


#: A marker selecting the engine (minibatch) time of an update — the UNIX
#: timestamp in milliseconds that the message-queue connectors also attach as
#: the ``pathway_time`` property — instead of a column of the table.
ENGINE_TIME = EngineTimeMarker()

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
    "avro": "avro",
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
    "avro",
}


DurationLike = int | float | datetime.timedelta
"""Type of duration parameters accepted by connectors: a number of seconds
(``int`` or ``float``) or a ``datetime.timedelta`` (including its subclasses
``pd.Timedelta`` and ``pw.Duration``)."""


def as_duration_seconds(
    value: DurationLike,
    param_name: str,
    *,
    allow_zero: bool = True,
) -> float:
    """Coerces a duration-like parameter value to a float number of seconds.

    ``int`` and ``float`` values are interpreted as seconds. ``datetime.timedelta``
    values (including ``pd.Timedelta`` and ``pw.Duration``, which subclass it) are
    converted with ``total_seconds()``.

    Args:
        value: The value of the parameter to be coerced.
        param_name: The name of the parameter, used in error messages.
        allow_zero: If ``True`` (the default), a zero duration is accepted: for
            polling-type intervals it is a legitimate way to ask for updates as
            often as possible, at the price of a busy-wait loop. Set to ``False``
            for parameters where zero can never be meaningful (e.g. timeouts).

    Returns:
        The duration expressed as a float number of seconds.
    """
    if isinstance(value, datetime.timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value)
    else:
        raise TypeError(
            f"'{param_name}' must be a number of seconds (int or float) or a "
            f"datetime.timedelta, got {type(value).__name__}"
        )
    if not math.isfinite(seconds):
        raise ValueError(f"'{param_name}' must be finite, got {value!r}")
    if seconds < 0 or (seconds == 0 and not allow_zero):
        constraint = "non-negative" if allow_zero else "positive"
        raise ValueError(f"'{param_name}' must be {constraint}, got {value!r}")
    return seconds


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


class PlaintextKeySchema(pw.Schema):
    key: str


class RawKeySchema(pw.Schema):
    key: bytes


def construct_raw_data_schema_by_flags(
    *, with_native_record_key: bool, parse_utf8: bool, with_metadata: bool
) -> type[pw.Schema]:
    Schema: Any
    if parse_utf8:
        Schema = PlaintextDataSchema
        if with_native_record_key:
            Schema = Schema | PlaintextKeySchema
    else:
        Schema = RawDataSchema
        if with_native_record_key:
            Schema = Schema | RawKeySchema
    if with_metadata:
        Schema = Schema | MetadataSchema
    return Schema


def construct_schema_and_data_format(
    format: str,
    *,
    schema: type[Schema] | None = None,
    with_metadata: bool = False,
    autogenerate_key: bool = False,
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    schema_registry_settings: SchemaRegistrySettings | None = None,
    with_native_record_key: bool = False,
    _stacklevel: int = 1,
) -> tuple[type[Schema], api.DataFormat]:
    data_format_type = get_data_format_type(format, SUPPORTED_INPUT_FORMATS)
    # The key generation only has a meaning for the formats that produce a
    # single payload column; the record formats derive the columns from the
    # schema and would silently ignore the flag.
    if autogenerate_key and data_format_type != "identity":
        raise ValueError(
            f"'autogenerate_key' is only meaningful for 'raw' or "
            f"'plaintext' formats and would have no effect with "
            f"{format!r}. Drop it or pick a compatible format."
        )

    if data_format_type == "identity":
        kwargs = locals()
        unexpected_params = [
            "schema",
            "csv_settings",
            "json_field_paths",
            "schema_registry_settings",
        ]
        for param in unexpected_params:
            if param in kwargs and kwargs[param] is not None:
                raise ValueError(f"Unexpected argument for {format!r} format: {param}")

        parse_utf8 = format not in ("binary", "only_metadata", "raw")
        schema = construct_raw_data_schema_by_flags(
            with_native_record_key=with_native_record_key,
            parse_utf8=parse_utf8,
            with_metadata=with_metadata,
        )
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
            message_queue_key_field=(
                MESSAGE_QUEUE_KEY_COLUMN_NAME if with_native_record_key else None
            ),
        )

    schema = assert_schema_not_none(schema, data_format_type)
    if METADATA_COLUMN_NAME in schema.column_names():
        if with_metadata:
            raise ValueError(
                f"The schema already declares a {METADATA_COLUMN_NAME!r} column, "
                f"which conflicts with 'with_metadata=True'. Either remove "
                f"{METADATA_COLUMN_NAME!r} from the schema or set "
                "'with_metadata=False'."
            )
        raise ValueError(
            f"{METADATA_COLUMN_NAME!r} is a reserved column name used by "
            "Pathway's connector metadata. Declaring it in the schema would "
            "be silently shadowed by the connector. Rename your column to "
            "something else, or set 'with_metadata=True' to receive the "
            "auto-generated metadata in this column."
        )
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
        if json_field_paths is not None:
            schema_columns = set(schema.column_names())
            for field_name, path in json_field_paths.items():
                if field_name == METADATA_COLUMN_NAME:
                    raise ValueError(
                        f"'json_field_paths' cannot be used for "
                        f"{METADATA_COLUMN_NAME!r}: the connector populates "
                        f"this column itself when 'with_metadata=True', so "
                        f"any JSON path would be silently ignored."
                    )
                if field_name not in schema_columns:
                    raise ValueError(
                        f"'json_field_paths' references field {field_name!r} "
                        f"which is not in the schema. Known fields: "
                        f"{sorted(schema_columns)}."
                    )
                if path != "" and not path.startswith("/"):
                    raise ValueError(
                        f"Invalid JSON Pointer for field {field_name!r}: "
                        f"{path!r}. JSON Pointers (RFC 6901) must be empty "
                        f"or start with '/' (e.g. '/foo/bar')."
                    )
        return schema, api.DataFormat(
            **api_schema,
            format_type=data_format_type,
            column_paths=json_field_paths,
            schema_registry_settings=maybe_schema_registry_settings(
                schema_registry_settings
            ),
        )
    elif data_format_type == "avro":
        if csv_settings is not None:
            raise ValueError("Unexpected argument for avro format: csv_settings")
        if json_field_paths is not None:
            raise ValueError("Unexpected argument for avro format: json_field_paths")
        if schema_registry_settings is not None:
            raise ValueError(
                "'schema_registry_settings' configures the Confluent schema "
                "registry and has no effect with the 'avro' format, which "
                "reads the schemas from the message broker's own registry. "
                "Drop the argument."
            )
        return schema, api.DataFormat(
            **api_schema,
            format_type=data_format_type,
        )
    else:
        raise ValueError(f"data format `{format}` not supported")


def check_raw_and_plaintext_only_kwargs_for_message_queues(f):
    default_format = inspect.signature(f).parameters["format"].default

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        data_format = kwargs.get("format", default_format)
        if data_format not in ("raw", "plaintext"):
            if "value" in kwargs and kwargs["value"] is not None:
                raise ValueError(
                    f"Unsupported argument for {data_format!r} format: 'value'"
                )

        return f(*args, **kwargs)

    return wrapper


@dataclass(frozen=True)
class MessageQueueOutputFormat:
    _: KW_ONLY
    table: Table
    key_field_index: int | None
    ordering_key_field_index: int | None
    event_time_field_index: int | None
    deliver_at_field_index: int | None
    deliver_after_field_index: int | None
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
        ordering_key: ColumnReference | None = None,
        event_time: ColumnReference | None = None,
        deliver_at: ColumnReference | None = None,
        deliver_after: ColumnReference | None = None,
        value: ColumnReference | None = None,
        headers: Iterable[ColumnReference] | None = None,
        topic_name: ColumnReference | None = None,
        schema_registry_settings: SchemaRegistrySettings | None = None,
        subject: str | None = None,
        allowed_key_types: tuple[dt.DType, ...] | None = (dt.BYTES, dt.STR, dt.ANY),
        allowed_value_types: tuple[dt.DType, ...] | None = (dt.BYTES, dt.STR, dt.ANY),
    ) -> MessageQueueOutputFormat:
        if delimiter != "," and format != "dsv":
            raise ValueError(
                f"'delimiter' is only meaningful for the 'dsv' format, but "
                f"{format!r} was specified. Drop the 'delimiter' argument "
                f"or use format='dsv'."
            )
        if subject is not None and schema_registry_settings is None:
            raise ValueError(
                "'subject' was provided without 'schema_registry_settings'. "
                "The 'subject' parameter only has an effect when a schema "
                "registry is configured; either pass 'schema_registry_settings' "
                "or remove 'subject'."
            )
        if schema_registry_settings is not None and subject is None:
            raise ValueError(
                "'schema_registry_settings' was provided without 'subject'. "
                "When a schema registry is configured, 'subject' must also be "
                "set so the formatter knows which subject to encode under."
            )
        if subject is not None and not subject:
            raise ValueError(
                "'subject' must be a non-empty string; got an empty string. "
                "Schema Registry subjects identify a named schema version, "
                "and an empty subject is never a valid registry entry."
            )
        if schema_registry_settings is not None and format != "json":
            raise ValueError(
                f"'schema_registry_settings' is only meaningful for the 'json' "
                f"format, but {format!r} was specified. The Confluent Schema "
                "Registry currently encodes JSON payloads only; remove "
                "'schema_registry_settings' or use format='json'."
            )

        key_field_index = None
        header_fields: dict[str, int] = {}
        extracted_field_indices: dict[str, int] = {}
        columns_to_extract: list[ColumnReference] = []

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
            if (
                allowed_key_types is not None
                and table[key._name]._column.dtype not in allowed_key_types
            ):
                raise ValueError(
                    f"The key column must have one of the following types: {allowed_key_types}"
                )
            key_field_index = cls.add_column_reference_to_extract(
                key, columns_to_extract, extracted_field_indices
            )
        ordering_key_field_index = None
        if ordering_key is not None:
            if (
                allowed_key_types is not None
                and table[ordering_key._name]._column.dtype not in allowed_key_types
            ):
                raise ValueError(
                    f"The ordering key column must have one of the following "
                    f"types: {allowed_key_types}"
                )
            ordering_key_field_index = cls.add_column_reference_to_extract(
                ordering_key, columns_to_extract, extracted_field_indices
            )
        event_time_field_index = None
        if event_time is not None:
            cls.check_timestamp_column(table, event_time, "event time")
            event_time_field_index = cls.add_column_reference_to_extract(
                event_time, columns_to_extract, extracted_field_indices
            )
        if deliver_at is not None and deliver_after is not None:
            raise ValueError(
                "'deliver_at' and 'deliver_after' cannot be set at the same "
                "time: a message has a single delivery time, given either as "
                "an absolute timestamp or as a delay from the publishing."
            )
        deliver_at_field_index = None
        if deliver_at is not None:
            cls.check_timestamp_column(table, deliver_at, "delivery time")
            deliver_at_field_index = cls.add_column_reference_to_extract(
                deliver_at, columns_to_extract, extracted_field_indices
            )
        deliver_after_field_index = None
        if deliver_after is not None:
            deliver_after_dtype = table[deliver_after._name]._column.dtype
            if deliver_after_dtype not in (dt.INT, dt.DURATION, dt.ANY):
                raise ValueError(
                    "The delivery delay column must be an integer (milliseconds) "
                    "or a duration, however "
                    f"{deliver_after_dtype.typehint} is used"
                )
            deliver_after_field_index = cls.add_column_reference_to_extract(
                deliver_after, columns_to_extract, extracted_field_indices
            )
        if headers is not None:
            reserved_header_names = {"pathway_time", "pathway_diff"}
            for header in headers:
                if header.name in reserved_header_names:
                    raise ValueError(
                        f"{header.name!r} is reserved for the Pathway-injected "
                        "headers (pathway_time / pathway_diff) and cannot be "
                        "used as a user header name. Alias the column to "
                        "another name with `table.select(<new_name>=...)`."
                    )
                if header.name in header_fields:
                    raise ValueError(
                        f"Duplicate header name {header.name!r}: two columns "
                        "produce a header with the same name. Alias one of "
                        "them to a different name (e.g. via `table.select(...)`) "
                        "to keep both as separate Kafka headers."
                    )
                header_fields[header.name] = cls.add_column_reference_to_extract(
                    header, columns_to_extract, extracted_field_indices
                )

        # Format-dependent parts: the record formats (json, dsv, avro) share
        # the whole-table extraction; the avro specifics are reduced to the
        # payload subset.
        if format in ("json", "dsv", "avro"):
            if value is not None:
                raise ValueError(
                    f"'value' and format='{format}' cannot be set at the same time"
                )
            if format == "json":
                reserved = {"time", "diff"}
                conflicting = reserved.intersection(table._columns.keys())
                if conflicting:
                    raise ValueError(
                        f"The table has columns {sorted(conflicting)} which "
                        f"clash with the reserved JSON fields written by the "
                        f"connector ('time', 'diff'). Rename or drop the "
                        f"conflicting column(s) before writing in 'json' "
                        f"format, otherwise the output JSON would contain "
                        f"duplicate keys."
                    )
            for column_name in table._columns:
                cls.add_column_reference_to_extract(
                    table[column_name], columns_to_extract, extracted_field_indices
                )
            table = table.select(*columns_to_extract)
            if format == "avro":
                # The registered Avro schema is a public, versioned contract
                # of the topic, so the service columns — the dynamic topic,
                # the keys, the event time, the delivery schedule and the
                # headers — stay out of the
                # payload. A column needed both as a service input and in the
                # payload can be duplicated under another name with
                # `table.select(...)`.
                service_field_indices = {
                    topic_name_index,
                    key_field_index,
                    ordering_key_field_index,
                    event_time_field_index,
                    deliver_at_field_index,
                    deliver_after_field_index,
                    *header_fields.values(),
                }
                payload_field_indices = [
                    index
                    for index in range(len(table._columns))
                    if index not in service_field_indices
                ]
                if not payload_field_indices:
                    raise ValueError(
                        "format='avro' needs at least one payload column, but "
                        "every column of the table is used as a service column "
                        "(topic/key/ordering_key/event_time/deliver_at/"
                        "deliver_after/headers). Duplicate "
                        "a column under another name with `table.select(...)` if "
                        "it must serve both purposes."
                    )
                data_format = api.DataFormat(
                    format_type="avro",
                    key_field_names=[],
                    value_fields=_format_output_value_fields(table),
                    payload_field_indices=payload_field_indices,
                )
            else:
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
            if (
                allowed_value_types is not None
                and table[value._name]._column.dtype not in allowed_value_types
            ):
                raise ValueError(
                    f"The value column must have one of the following types: {allowed_value_types}"
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
            ordering_key_field_index=ordering_key_field_index,
            event_time_field_index=event_time_field_index,
            deliver_at_field_index=deliver_at_field_index,
            deliver_after_field_index=deliver_after_field_index,
            header_fields=header_fields,
            data_format=data_format,
            topic_name_index=topic_name_index,
        )

    @staticmethod
    def check_timestamp_column(
        table: Table, column: ColumnReference, purpose: str
    ) -> None:
        """Checks that the column can carry a message timestamp: an integer
        (milliseconds since the UNIX epoch) or a UTC datetime."""
        dtype = table[column._name]._column.dtype
        if dtype == dt.DATE_TIME_NAIVE:
            raise ValueError(
                f"The {purpose} column must not be a timezone-naive "
                "datetime: its UTC interpretation would be a silent "
                "assumption. Convert it explicitly, e.g. with "
                "`.dt.to_utc(...)`."
            )
        if dtype not in (dt.INT, dt.DATE_TIME_UTC, dt.ANY):
            raise ValueError(
                f"The {purpose} column must be an integer (milliseconds "
                "since the UNIX epoch) or a UTC datetime, however "
                f"{dtype.typehint} is used"
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
            existing = selection_list[index_in_new_table]
            # If a *different* column reference shares the same output name we
            # would silently drop the new value because `table.select(...)`
            # collapses entries by name. Detect this and fail loudly so the
            # user can alias one of them to a unique name.
            #
            # Note: a `pw.this.X` reference has ``_column is None`` because it
            # is resolved later at expression time. We treat such references
            # as compatible with any earlier same-named column — they refer to
            # the same column in the target table by definition.
            if (
                existing._column is not None
                and column_reference._column is not None
                and existing._column is not column_reference._column
            ):
                raise ValueError(
                    f"Two different columns share the output name "
                    f"{column_name!r}. This typically happens when, e.g., a "
                    f"header is aliased to the same name as the 'topic_name' "
                    f"or 'key' column, or when two separate selects produce "
                    f"the same output name. Alias one of them to a different "
                    f"name (e.g. via `table.select(<new_name>=...)`)."
                )
            # Same column referenced more than once is fine — reuse the slot.
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


def explore_schema(
    data_storage: api.DataStorage,
    *,
    source_description: str,
    format: str | None = None,
    schema_name: str | None = None,
) -> type[Schema]:
    """Deduces a ``pw.Schema`` from the schema the data source itself
    declares (e.g. the topic's schema in the Pulsar registry).

    The deduction happens at pipeline construction time, so the resulting
    table is statically typed as usual. The returned value is an ordinary
    schema class: a caller unhappy with any deduced column can adjust it with
    the standard schema utilities, or write a schema by hand instead — an
    explicit schema always wins over the deduction.

    Internal for now; connectors call it when their ``schema`` parameter is
    omitted for a format that supports the deduction.
    """
    # In a multi-process run every process constructs its own graph, so each
    # would query the source independently — and a schema change between
    # their starts would make the processes build diverging graphs, failing
    # later with an obscure cross-worker mismatch instead of a schema error.
    if int(os.environ.get("PATHWAY_PROCESSES", "1")) > 1:
        raise ValueError(
            f"The schema of {source_description} cannot be deduced in a "
            "multi-process run: every process would query the source "
            "independently, and a schema change between their starts would "
            "make the processes construct diverging computation graphs. Pass "
            "an explicit schema instead."
        )
    fields = api.explore_schema(data_storage, format)
    # `id` resolves to the row key and would silently shadow the column;
    # `_metadata` is claimed by the metadata machinery. Neither can be
    # renamed by the deduction, so such fields must fail loudly.
    reserved_column_names = {"id", METADATA_COLUMN_NAME}
    columns: dict[str, Any] = {}
    for name, type_descriptor_json, has_default, default, doc in fields:
        if name in reserved_column_names:
            raise ValueError(
                f"The deduced schema of {source_description} contains the field "
                f"{name!r}, which is a reserved Pathway column name. Pass an "
                f"explicit schema that omits this field (for the 'json' format, "
                f"json_field_paths can map it onto a different column name)."
            )
        if not name.isidentifier() or keyword.iskeyword(name):
            raise ValueError(
                f"The deduced schema of {source_description} contains the field "
                f"{name!r}, which is not usable as a Pathway column name. Pass "
                f"an explicit schema instead."
            )
        entry: dict[str, Any] = {"dtype": json.loads(type_descriptor_json)}
        # The presence flag distinguishes "no default" from a default that
        # is legitimately None (e.g. an optional Avro field with
        # "default": null).
        if has_default:
            entry["default_value"] = default
        if doc:
            entry["description"] = doc
        columns[name] = entry
    if not columns:
        raise ValueError(
            f"The deduced schema of {source_description} has no columns. Pass "
            f"an explicit schema instead."
        )
    schema = schema_from_dict(columns, name=schema_name)
    # The deduced contract is implicit, so make it visible: with an explicit
    # schema a typo fails loudly, while here the pipeline just runs with
    # whatever the source declared.
    logging.info("Deduced the schema of %s: %s", source_description, schema)
    return schema


def remap_sort_by(
    sort_by: Iterable[ColumnReference] | None,
    original_table: Table,
    output_table: Table,
    connector_name: str,
) -> list[ColumnReference] | None:
    """Rewrites the ``sort_by`` columns onto the table the sink is attached to.

    The message-queue writers rebuild their input with ``table.select(...)`` to
    lay the service columns out for the engine, so the references the caller
    passed — taken against the table they called ``write`` on — belong to a
    different table and the sink would reject them.
    """
    if sort_by is None:
        return None
    remapped: list[ColumnReference] = []
    for column in sort_by:
        if column._table is output_table:
            remapped.append(column)
            continue
        if column._table is not original_table:
            raise ValueError(
                f"The sort_by column {column} doesn't belong to the table "
                f"passed to {connector_name}."
            )
        if column.name not in output_table._columns:
            raise ValueError(
                f"The sort_by column {column.name!r} is not part of the "
                "data being written. For 'raw' or 'plaintext' format, only "
                "the columns forwarded to the message (the payload, the keys, "
                "the topic name and the headers) are."
            )
        remapped.append(output_table[column.name])
    return remapped


def resolve_start_from_timestamp_ms(
    start_from: str,
    start_from_timestamp_ms: int | None,
) -> int | None:
    """Validates a (``start_from``, ``start_from_timestamp_ms``) pair and encodes
    it into the single ``start_from_timestamp_ms`` field understood by the engine.

    The engine reserves ``-1`` in this field as an internal sentinel for
    ``start_from="end"``, so an explicit timestamp must be non-negative —
    otherwise a user-provided ``-1`` would silently turn into "start from
    the end of the stream" instead of raising an error.
    """
    if start_from == "timestamp":
        if start_from_timestamp_ms is None:
            raise ValueError(
                "start_from_timestamp_ms is required when start_from='timestamp'"
            )
        if start_from_timestamp_ms < 0:
            raise ValueError(
                "start_from_timestamp_ms must be a non-negative number of"
                f" milliseconds since the UNIX epoch; got {start_from_timestamp_ms}"
            )
        return start_from_timestamp_ms
    if start_from_timestamp_ms is not None:
        raise ValueError(
            f"start_from_timestamp_ms must not be set when start_from='{start_from}'"
        )
    return -1 if start_from == "end" else None


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


def get_column_index(table: Table, column: ColumnReference | None) -> int | None:
    if column is None:
        return None
    if column._table != table:
        raise ValueError(f"The column {column} doesn't belong to the target table")
    for index, table_column in enumerate(table._columns):
        if table_column == column.name:
            return index
    raise RuntimeError(f"The column {column} is not found in the table {table}")


def init_mode_from_str(init_mode: str) -> api.TableWriterInitMode:
    match init_mode:
        case "default":
            return api.TableWriterInitMode.DEFAULT
        case "create_if_not_exists":
            return api.TableWriterInitMode.CREATE_IF_NOT_EXISTS
        case "replace":
            return api.TableWriterInitMode.REPLACE
        case _:
            raise ValueError(f"Invalid init_mode: {init_mode}")

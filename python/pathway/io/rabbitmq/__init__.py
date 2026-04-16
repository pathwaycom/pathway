# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Iterable, Literal

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import TLSSettings
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    MessageQueueOutputFormat,
    _get_unique_name,
    check_raw_and_plaintext_only_kwargs_for_message_queues,
    construct_schema_and_data_format,
    internal_connector_mode,
)


@check_arg_types
@trace_user_frame
def read(
    uri: str,
    stream_name: str,
    *,
    schema: type[Schema] | None = None,
    format: Literal["plaintext", "raw", "json"] = "raw",
    mode: Literal["streaming", "static"] = "streaming",
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    with_metadata: bool = False,
    start_from: Literal["beginning", "end", "timestamp"] = "beginning",
    start_from_timestamp_ms: int | None = None,
    name: str | None = None,
    max_backlog_size: int | None = None,
    tls_settings: TLSSettings | None = None,
    debug_data=None,
    **kwargs,
) -> Table:
    """Reads data from a `RabbitMQ <https://www.rabbitmq.com/docs/streams>`_ stream.

    This connector supports plain RabbitMQ Streams. Super Streams (partitioned
    streams) are not supported in the current version.

    There are three formats supported: ``"plaintext"``, ``"raw"``, and ``"json"``.

    For the ``"raw"`` format, the payload is read as raw bytes and added directly to the
    table. In the ``"plaintext"`` format, the payload is decoded from UTF-8 and stored as
    plain text. In both cases, the table will have a ``"data"`` column representing
    the payload.

    If ``"json"`` is chosen, the connector parses the message payload as JSON
    and creates table columns based on the schema provided in the ``schema`` parameter.

    **Application properties (headers).** When ``with_metadata=True``, the
    ``_metadata`` column includes ``application_properties`` — a dict of all
    AMQP 1.0 application properties set by the writer. This is consistent with
    how :py:func:`pw.io.kafka.read` exposes Kafka headers in ``_metadata``.

    **Persistence.** When persistence is enabled, the connector saves the current
    stream offset (a single integer) to the snapshot. On restart, it resumes from
    the saved offset, so already-processed messages are not re-read.

    Args:
        uri: The URI of the RabbitMQ server with Streams enabled, e.g.
            ``"rabbitmq-stream://guest:guest@localhost:5552"``.
        stream_name: The name of the RabbitMQ stream to read data from. The stream
            must already exist on the server.
        schema: The table schema, used only when the format is set to ``"json"``.
        format: The input data format, which can be ``"raw"``, ``"plaintext"``, or
            ``"json"``.
        mode: The reading mode, which can be ``"streaming"`` or ``"static"``. In
            ``"streaming"`` mode, the connector reads messages continuously. In
            ``"static"`` mode, it reads all existing messages and then stops.
        autocommit_duration_ms: The time interval (in milliseconds) between commits.
            After this time, the updates received by the connector are committed and
            added to Pathway's computation graph.
        json_field_paths: For the ``"json"`` format, this allows mapping field names to
            paths within the JSON structure. Use the format ``<field_name>: <path>``
            where the path follows the
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        with_metadata: If ``True``, adds a ``_metadata`` column containing a JSON dict
            with ``offset``, ``stream_name``, AMQP 1.0 message properties when
            available (``message_id``, ``correlation_id``, ``content_type``,
            ``content_encoding``, ``subject``, ``reply_to``, ``priority``, ``durable``),
            and ``application_properties`` — a dict of string key-value pairs
            containing the AMQP application properties set by the writer. Values
            produced by :py:func:`write` are JSON-encoded strings (see ``headers``
            parameter of :py:func:`write`), so they can be parsed back with
            ``json.loads``.
        start_from: Where to start reading from. ``"beginning"`` starts from the
            first message in the stream. ``"end"`` skips all existing messages and only
            reads new ones arriving after the reader starts. ``"timestamp"`` starts
            from messages at or after the time given in ``start_from_timestamp_ms``.
        start_from_timestamp_ms: Timestamp in milliseconds since epoch. Required when
            ``start_from="timestamp"``, must not be set otherwise.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        max_backlog_size: Limit on the number of entries read from the input source and
            kept in processing at any moment.
        tls_settings: TLS connection settings. Use ``TLSSettings`` to configure
            root certificates, client certificates, and verification mode.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Read messages in raw format (the default). The table will have ``key`` and ``data``
    columns:

    >>> import pathway as pw
    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ... )

    Read messages as UTF-8 plaintext:

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="plaintext",
    ... )

    Read and parse JSON messages with a schema:

    >>> class InputSchema(pw.Schema):
    ...     owner: str
    ...     pet: str

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="json",
    ...     schema=InputSchema,
    ... )

    Extract nested JSON fields using JSON Pointer paths:

    >>> class InputSchema(pw.Schema):
    ...     name: str
    ...     age: int

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="json",
    ...     schema=InputSchema,
    ...     json_field_paths={"name": "/user/name", "age": "/user/age"},
    ... )

    Read in static mode (bounded snapshot):

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     mode="static",
    ... )

    Read only new messages, ignoring all existing data in the stream:

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     start_from="end",
    ... )

    Read with persistence enabled, so progress is saved across restarts:

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="json",
    ...     schema=InputSchema,
    ...     name="my-rabbitmq-source",
    ... )
    >>> # Then run with persistence:
    >>> # pw.run(persistence_config=pw.persistence.Config(
    >>> #     pw.persistence.Backend.filesystem("./PStorage")
    >>> # ))

    Read with metadata column:

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     with_metadata=True,
    ... )
    """
    _check_entitlements("rabbitmq")
    if start_from == "timestamp":
        if start_from_timestamp_ms is None:
            raise ValueError(
                "start_from_timestamp_ms is required when start_from='timestamp'"
            )
        effective_timestamp = start_from_timestamp_ms
    else:
        if start_from_timestamp_ms is not None:
            raise ValueError(
                "start_from_timestamp_ms must not be set when"
                f" start_from='{start_from}'"
            )
        # Encode start_from as a sentinel in start_from_timestamp_ms:
        # None → beginning (OffsetSpecification::First)
        # -1   → end (OffsetSpecification::Next)
        effective_timestamp = -1 if start_from == "end" else None

    data_storage = api.DataStorage(
        storage_type="rabbitmq",
        path=uri,
        topic=stream_name,
        mode=internal_connector_mode(mode),
        start_from_timestamp_ms=effective_timestamp,
        tls_settings=tls_settings.settings if tls_settings is not None else None,
        with_metadata=with_metadata,
    )
    schema, data_format = construct_schema_and_data_format(
        "binary" if format == "raw" else format,
        schema=schema,
        json_field_paths=json_field_paths,
        with_metadata=with_metadata,
    )
    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=_get_unique_name(name, kwargs),
        max_backlog_size=max_backlog_size,
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=data_source_options,
            schema=schema,
            datasource_name="rabbitmq",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_raw_and_plaintext_only_kwargs_for_message_queues
@check_arg_types
@trace_user_frame
def write(
    table: Table,
    uri: str,
    stream_name: str | ColumnReference,
    *,
    format: Literal["json", "plaintext", "raw"] = "json",
    value: ColumnReference | None = None,
    headers: Iterable[ColumnReference] | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
    tls_settings: TLSSettings | None = None,
) -> None:
    """Writes data into the specified RabbitMQ stream.

    The produced messages consist of the payload, corresponding to the values of the
    table that are serialized according to the chosen format. Two AMQP 1.0 application
    properties are always added: ``pathway_time`` (processing time) and ``pathway_diff``
    (either ``1`` or ``-1``). If ``headers`` parameter is used, additional properties
    can be added to the message.

    There are several serialization formats supported: ``"json"``,
    ``"plaintext"`` and ``"raw"``.

    If the selected format is either ``"plaintext"`` or ``"raw"``, you also need to
    specify which column of the table corresponds to the payload of the produced message.
    It can be done by providing the ``value`` parameter.

    Args:
        table: The table for output.
        uri: The URI of the RabbitMQ server with Streams enabled, e.g.
            ``"rabbitmq-stream://guest:guest@localhost:5552"``.
        stream_name: The RabbitMQ stream where data will be written. The stream must
            already exist on the server. Can be a column reference for dynamic
            routing — each row will be written to the stream named by that column's
            value. All target streams must be pre-created.
        format: Format in which the data is put into RabbitMQ. Currently ``"json"``,
            ``"plaintext"`` and ``"raw"`` are supported.
        value: Reference to the column that should be used as a payload in the produced
            message in ``"plaintext"`` or ``"raw"`` format.
        headers: References to the table fields that must be provided as AMQP 1.0
            application properties (analogous to Kafka headers). Values are
            serialized as AMQP strings using their JSON representation, following
            the same encoding as :py:func:`pw.io.jsonlines.write` (e.g. ``42``
            for an int, ``"\"hello\""`` for a string, ``null`` for None,
            base64-encoded string for bytes). RabbitMQ Streams does not reliably
            confirm messages with non-string application property values, so all
            types are JSON-encoded. On the reader side, header values are
            available in ``_metadata.application_properties`` (when
            ``with_metadata=True``).
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch.
        tls_settings: TLS connection settings. Use ``TLSSettings`` to configure
            root certificates, client certificates, and verification mode.

    Examples:

    Consider a RabbitMQ server with Streams enabled running locally on port ``5552``.
    First, create a Pathway table:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | cat
    ... 8   | Alice | cat
    ... ''')

    Write the table in JSON format. Each row is serialized as a JSON object:

    >>> pw.io.rabbitmq.write(
    ...     table,
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="json",
    ... )

    Use the ``"plaintext"`` format to send a single column as the message payload.
    When the table has more than one column, you must specify which column to use
    via the ``value`` parameter. Additional columns can be forwarded as AMQP
    application properties using the ``headers`` parameter:

    >>> pw.io.rabbitmq.write(
    ...     table,
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="plaintext",
    ...     value=table.owner,
    ...     headers=[table.age, table.pet],
    ... )

    Write each row to a different stream based on a column value (dynamic topics).
    All target streams must already exist on the server:

    >>> table_with_targets = pw.debug.table_from_markdown('''
    ... value | target_stream
    ... hello | stream-a
    ... world | stream-b
    ... ''')
    >>> pw.io.rabbitmq.write(
    ...     table_with_targets,
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     table_with_targets.target_stream,
    ...     format="json",
    ... )
    """
    _check_entitlements("rabbitmq")
    output_format = MessageQueueOutputFormat.construct(
        table,
        format=format,
        delimiter=",",
        value=value,
        headers=headers,
        topic_name=stream_name if isinstance(stream_name, ColumnReference) else None,
    )
    table = output_format.table

    data_storage = api.DataStorage(
        storage_type="rabbitmq",
        path=uri,
        topic=stream_name if isinstance(stream_name, str) else None,
        topic_name_index=output_format.topic_name_index,
        header_fields=list(output_format.header_fields.items()),
        tls_settings=tls_settings.settings if tls_settings is not None else None,
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            output_format.data_format,
            datasink_name="rabbitmq",
            unique_name=name,
            sort_by=sort_by,
        )
    )

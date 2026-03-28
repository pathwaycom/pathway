# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Iterable, Literal

from pathway.internals import api, datasink, datasource
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import (
    KEY_SOURCE_COMPONENT,
    PAYLOAD_SOURCE_COMPONENT,
    Schema,
)
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
    autogenerate_key: bool = False,
    with_metadata: bool = False,
    start_from_timestamp_ms: int | None = None,
    parallel_readers: int | None = None,
    name: str | None = None,
    max_backlog_size: int | None = None,
    tls_root_certificates: str | None = None,
    tls_client_cert: str | None = None,
    tls_client_key: str | None = None,
    tls_trust_certificates: bool = False,
    debug_data=None,
    _stacklevel: int = 5,
    **kwargs,
) -> Table:
    """Reads data from a specified RabbitMQ stream.

    It supports three formats: ``"plaintext"``, ``"raw"``, and ``"json"``.

    For the ``"raw"`` format, the payload is read as raw bytes and added directly to the
    table. In the ``"plaintext"`` format, the payload is decoded from UTF-8 and stored as
    plain text. In both cases, the table will have a ``"key"`` column and a ``"data"``
    column representing the payload.

    If you select the ``"json"`` format, the connector parses the message payload as JSON
    and creates table columns based on the schema provided in the ``schema`` parameter.

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
        autogenerate_key: If True, always auto-generate the primary key instead of
            using the message key from ``application_properties``.
        with_metadata: If True, adds a ``_metadata`` column containing a dict with
            ``offset``, ``stream_name``, and ``timestamp_millis`` fields. Note:
            ``timestamp_millis`` is currently always null due to protocol library
            limitations.
        start_from_timestamp_ms: If specified, start reading from messages at or after
            this timestamp (in milliseconds since epoch).
        parallel_readers: The number of reader instances running in parallel.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        max_backlog_size: Limit on the number of entries read from the input source and
            kept in processing at any moment.
        tls_root_certificates: Path to the root CA certificate file for TLS connections.
        tls_client_cert: Path to the client certificate file for mutual TLS.
        tls_client_key: Path to the client private key file for mutual TLS.
        tls_trust_certificates: If True, trust server certificates without verification.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    If your RabbitMQ server with Streams enabled is running on ``localhost`` using the
    default Streams port ``5552``, you can stream the ``"events"`` stream to a Pathway
    table like this:

    >>> import pathway as pw
    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ... )

    You can also parse messages as UTF-8 during reading:

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="plaintext",
    ... )

    Read and parse a JSON table with schema:

    >>> class InputSchema(pw.Schema):
    ...     user_id: int = pw.column_definition(primary_key=True)
    ...     username: str
    ...     phone: str

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="json",
    ...     schema=InputSchema,
    ... )

    Read with metadata column:

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     with_metadata=True,
    ... )

    Read in static mode (bounded snapshot):

    >>> table = pw.io.rabbitmq.read(
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     mode="static",
    ... )
    """
    tls_kwargs: dict = {}
    if tls_root_certificates is not None:
        tls_kwargs["rabbitmq_tls_root_certificates"] = tls_root_certificates
    if tls_client_cert is not None:
        tls_kwargs["rabbitmq_tls_client_cert"] = tls_client_cert
    if tls_client_key is not None:
        tls_kwargs["rabbitmq_tls_client_key"] = tls_client_key
    if tls_trust_certificates:
        tls_kwargs["rabbitmq_tls_trust_certificates"] = tls_trust_certificates
    data_storage = api.DataStorage(
        storage_type="rabbitmq",
        path=uri,
        topic=stream_name,
        parallel_readers=parallel_readers,
        mode=internal_connector_mode(mode),
        start_from_timestamp_ms=start_from_timestamp_ms,
        **tls_kwargs,
    )
    schema, data_format = construct_schema_and_data_format(
        "binary" if format == "raw" else format,
        schema=schema,
        json_field_paths=json_field_paths,
        with_native_record_key=True,
        autogenerate_key=autogenerate_key,
        with_metadata=with_metadata,
        _stacklevel=_stacklevel,
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
        supported_components=(
            KEY_SOURCE_COMPONENT,
            PAYLOAD_SOURCE_COMPONENT,
        ),
    )


@check_arg_types
@trace_user_frame
def simple_read(
    host: str,
    stream_name: str,
    *,
    port: int = 5552,
    schema: type[Schema] | None = None,
    format: Literal["plaintext", "raw", "json"] = "raw",
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    parallel_readers: int | None = None,
    name: str | None = None,
    debug_data=None,
    **kwargs,
) -> Table:
    """Simplified method to read data from a RabbitMQ stream. Only requires the
    server host and the stream name. Uses default credentials (``guest:guest``).
    If you need TLS, authentication, or fine-tuning, use :py:func:`read` instead.

    Read starts from the beginning of the stream, unless the ``read_only_new``
    parameter is set to True.

    There are three formats currently supported: ``"plaintext"``, ``"raw"``, and
    ``"json"``. If ``"raw"`` is chosen, the key and the payload are read as raw bytes.
    If ``"plaintext"`` is chosen, they are parsed from UTF-8. In both cases the table
    has a ``"data"`` column. If ``"json"`` is chosen, the payload is parsed according
    to the ``schema`` parameter.

    Args:
        host: Hostname of the RabbitMQ server with Streams enabled.
        stream_name: Name of the RabbitMQ stream to read from.
        port: Port of the RabbitMQ Streams endpoint (default ``5552``).
        schema: Schema of the resulting table (used with ``"json"`` format).
        format: Input data format: ``"raw"``, ``"plaintext"``, or ``"json"``.
        autocommit_duration_ms: The maximum time between two commits in milliseconds.
        json_field_paths: For ``"json"`` format, mapping of field names to JSON
            Pointer paths.
        parallel_readers: Number of reader instances running in parallel.
        name: A unique name for the connector.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    >>> import pathway as pw
    >>> t = pw.io.rabbitmq.simple_read("localhost", "events")
    """
    uri = f"rabbitmq-stream://guest:guest@{host}:{port}"
    return read(
        uri=uri,
        stream_name=stream_name,
        schema=schema,
        format=format,
        mode="streaming",
        autocommit_duration_ms=autocommit_duration_ms,
        json_field_paths=json_field_paths,
        parallel_readers=parallel_readers,
        name=name,
        debug_data=debug_data,
        _stacklevel=7,
        **kwargs,
    )


@check_raw_and_plaintext_only_kwargs_for_message_queues
@check_arg_types
@trace_user_frame
def write(
    table: Table,
    uri: str,
    stream_name: str | ColumnReference,
    *,
    format: Literal["json", "dsv", "plaintext", "raw"] = "json",
    delimiter: str = ",",
    key: ColumnReference | None = None,
    value: ColumnReference | None = None,
    headers: Iterable[ColumnReference] | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
    tls_root_certificates: str | None = None,
    tls_client_cert: str | None = None,
    tls_client_key: str | None = None,
    tls_trust_certificates: bool = False,
) -> None:
    """Writes data into the specified RabbitMQ stream.

    The produced messages consist of the payload, corresponding to the values of the
    table that are serialized according to the chosen format. Two AMQP 1.0 application
    properties are always added: ``pathway_time`` (processing time) and ``pathway_diff``
    (either ``1`` or ``-1``). If ``headers`` parameter is used, additional properties
    can be added to the message.

    There are several serialization formats supported: ``"json"``, ``"dsv"``,
    ``"plaintext"`` and ``"raw"``.

    If the selected format is either ``"plaintext"`` or ``"raw"``, you also need to
    specify which column of the table corresponds to the payload of the produced message.
    It can be done by providing the ``value`` parameter.

    Args:
        table: The table for output.
        uri: The URI of the RabbitMQ server with Streams enabled, e.g.
            ``"rabbitmq-stream://guest:guest@localhost:5552"``.
        stream_name: The RabbitMQ stream where data will be written. The stream must
            already exist on the server.
        format: Format in which the data is put into RabbitMQ. Currently ``"json"``,
            ``"plaintext"``, ``"raw"`` and ``"dsv"`` are supported.
        delimiter: Field delimiter to be used in case of delimiter-separated values
            format.
        key: Reference to a column that should be used as the message key. The key is
            stored as an AMQP 1.0 application property named ``"key"``. On the read
            side, this property is extracted when ``autogenerate_key`` is False.
        value: Reference to the column that should be used as a payload in the produced
            message in ``"plaintext"`` or ``"raw"`` format.
        headers: References to the table fields that must be provided as message
            application properties. These properties are named in the same way as fields
            that are forwarded and correspond to the string representations of the
            respective values encoded in UTF-8.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch.
        tls_root_certificates: Path to the root CA certificate file for TLS connections.
        tls_client_cert: Path to the client certificate file for mutual TLS.
        tls_client_key: Path to the client private key file for mutual TLS.
        tls_trust_certificates: If True, trust server certificates without verification.

    Example:

    Assume you have the RabbitMQ server with Streams enabled running locally on the
    default Streams port, ``5552``.

    First, you'll need to create a Pathway table:

    >>> import pathway as pw
    ...
    >>> table = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | cat
    ... 8   | Alice | cat
    ... ''')

    To output the table's contents in JSON format:

    >>> pw.io.rabbitmq.write(
    ...     table,
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="json",
    ... )

    You can also use a single column from the table as the payload with headers:

    >>> pw.io.rabbitmq.write(
    ...     table,
    ...     "rabbitmq-stream://guest:guest@localhost:5552",
    ...     "events",
    ...     format="plaintext",
    ...     value=table.owner,
    ...     headers=[table.age, table.pet],
    ... )
    """
    output_format = MessageQueueOutputFormat.construct(
        table,
        format=format,
        delimiter=delimiter,
        key=key,
        value=value,
        headers=headers,
        topic_name=stream_name if isinstance(stream_name, ColumnReference) else None,
    )
    table = output_format.table

    tls_kwargs: dict = {}
    if tls_root_certificates is not None:
        tls_kwargs["rabbitmq_tls_root_certificates"] = tls_root_certificates
    if tls_client_cert is not None:
        tls_kwargs["rabbitmq_tls_client_cert"] = tls_client_cert
    if tls_client_key is not None:
        tls_kwargs["rabbitmq_tls_client_key"] = tls_client_key
    if tls_trust_certificates:
        tls_kwargs["rabbitmq_tls_trust_certificates"] = tls_trust_certificates
    data_storage = api.DataStorage(
        storage_type="rabbitmq",
        path=uri,
        topic=stream_name if isinstance(stream_name, str) else None,
        topic_name_index=output_format.topic_name_index,
        key_field_index=output_format.key_field_index,
        header_fields=list(output_format.header_fields.items()),
        **tls_kwargs,
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

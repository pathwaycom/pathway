# Copyright © 2026 Pathway

from __future__ import annotations

import datetime
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
    EngineTimeMarker,
    MessageQueueOutputFormat,
    _get_unique_name,
    check_raw_and_plaintext_only_kwargs_for_message_queues,
    construct_schema_and_data_format,
    explore_schema,
    internal_connector_mode,
    remap_sort_by,
    resolve_start_from_timestamp_ms,
)


class TokenAuthentication:
    """Token-based authentication for the Pulsar connectors.

    The token (for example, a JWT) is presented to the broker when the
    connection is established. This is the most common authentication
    mechanism, supported by most managed Pulsar installations.

    Args:
        token: The authentication token.

    Example:

    >>> import pathway as pw
    >>> auth = pw.io.pulsar.TokenAuthentication("my-jwt-token")
    """

    @trace_user_frame
    def __init__(self, token: str):
        self._token = token

    def _settings_kwargs(self) -> dict[str, str | None]:
        return {"auth_token": self._token}


class OAuth2Authentication:
    """OAuth2 client-credentials authentication for the Pulsar connectors.

    An access token is obtained from the OAuth2 authentication provider and
    refreshed automatically. This mechanism is used by StreamNative Cloud and
    similar managed installations.

    Args:
        issuer_url: The URL of the OAuth2 authentication provider.
        credentials_url: The URL of the OAuth2 credentials file. Both
            ``file://`` and ``data://`` URLs are supported.
        audience: The audience identifier of the Pulsar cluster, if required
            by the provider.
        scope: The OAuth2 scope to request, if required by the provider.

    Example:

    >>> import pathway as pw
    >>> auth = pw.io.pulsar.OAuth2Authentication(
    ...     issuer_url="https://auth.streamnative.cloud/",
    ...     credentials_url="file:///path/to/credentials.json",
    ...     audience="urn:sn:pulsar:my-org:my-instance",
    ... )
    """

    @trace_user_frame
    def __init__(
        self,
        issuer_url: str,
        credentials_url: str,
        audience: str | None = None,
        scope: str | None = None,
    ):
        self._issuer_url = issuer_url
        self._credentials_url = credentials_url
        self._audience = audience
        self._scope = scope

    def _settings_kwargs(self) -> dict[str, str | None]:
        return {
            "oauth2_issuer_url": self._issuer_url,
            "oauth2_credentials_url": self._credentials_url,
            "oauth2_audience": self._audience,
            "oauth2_scope": self._scope,
        }


PulsarAuthentication = TokenAuthentication | OAuth2Authentication


def _construct_pulsar_settings(
    auth: PulsarAuthentication | None,
    subscription_type: str | None = None,
    compression: str | None = None,
    read_compacted: bool = False,
    producer_name: str | None = None,
    event_time_from_engine: bool = False,
    deliver_after_millis: int | None = None,
) -> api.PulsarSettings | None:
    if (
        auth is None
        and subscription_type is None
        and compression is None
        and not read_compacted
        and producer_name is None
        and not event_time_from_engine
        and deliver_after_millis is None
    ):
        return None
    auth_kwargs = auth._settings_kwargs() if auth is not None else {}
    return api.PulsarSettings(
        **auth_kwargs,
        subscription_type=subscription_type,
        compression=compression,
        read_compacted=read_compacted,
        producer_name=producer_name,
        event_time_from_engine=event_time_from_engine,
        deliver_after_millis=deliver_after_millis,
    )


def _check_tls_settings(tls_settings: TLSSettings | None) -> None:
    if tls_settings is None:
        return
    if (
        tls_settings._client_cert_path is not None
        or tls_settings._client_key_path is not None
    ):
        raise ValueError(
            "Mutual TLS (client certificate) authentication is not supported by "
            "the Pulsar connector. Use TokenAuthentication or "
            "OAuth2Authentication instead."
        )


@check_arg_types
@trace_user_frame
def read(
    uri: str,
    topic: str,
    *,
    schema: type[Schema] | None = None,
    format: Literal["plaintext", "raw", "json", "avro"] = "raw",
    mode: Literal["streaming", "static"] = "streaming",
    subscription_name: str | None = None,
    subscription_type: (
        Literal["reader", "shared", "key_shared", "exclusive", "failover"] | None
    ) = None,
    read_compacted: bool = False,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    autogenerate_key: bool = False,
    with_metadata: bool = False,
    start_from: Literal["beginning", "end", "timestamp"] = "beginning",
    start_from_timestamp_ms: int | None = None,
    auth: PulsarAuthentication | None = None,
    tls_settings: TLSSettings | None = None,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data=None,
    **kwargs,
) -> Table:
    """Reads data from an `Apache Pulsar <https://pulsar.apache.org/>`_ topic.

    The connector has two reading mechanisms, chosen by ``subscription_type``:
    the Kafka-like partition-reader mode (the only one allowed with
    persistence, where it recovers without losing or duplicating messages)
    and the broker-side subscription modes. Their semantics and trade-offs
    are described in the *Delivery semantics* section above.

    In the static mode the connector reads the messages that exist in the topic at
    the start of the computation and finishes afterwards, always through the
    partition-reader mechanism; no cursor is left behind on the broker.

    There are four formats supported: ``"plaintext"``, ``"raw"``, ``"json"``
    and ``"avro"``.

    For the ``"raw"`` format, the partition key and the payload are read as raw
    bytes and added to the table as they are. In the ``"plaintext"`` format they
    are decoded from UTF-8 and stored as plain text. In both cases the table has
    two columns: ``"key"`` with the partition key of the message (or ``None`` if
    the message was published without one) and ``"data"`` with the payload. The
    connector first tries to use the partition key of the message as the primary
    key of the row, and autogenerates one when the message has no partition key.
    Note that this makes the row identity follow the partition key rather than
    the message: several messages sharing a partition key produce several rows
    with the same primary key, which the operations relying on key uniqueness
    reject. Set ``autogenerate_key=True`` to give every message its own row
    instead.

    If ``"json"`` is chosen, the connector parses the message payload as JSON and
    creates table columns based on the schema provided in the ``schema`` parameter.

    Compressed messages (``lz4``, ``zlib``, ``zstd``) are decompressed
    automatically: the codec travels in the message metadata, so no configuration
    is needed on the reading side. Snappy-compressed topics are not supported:
    reading such messages fails with a decompression error.

    Args:
        uri: The Pulsar service URI, e.g. ``pulsar://localhost:6650`` or
            ``pulsar+ssl://my-cluster:6651`` for a TLS-encrypted connection.
        topic: The name of the topic to read from. A ``non-persistent://``
            topic is accepted only in the streaming mode with a subscription
            type (see the topic-persistence notes in the overview above).
        schema: Schema of the resulting table. For the ``"json"`` and ``"avro"``
            formats it may be omitted: the columns are then deduced from the
            topic's registry schema (see the *Schema registry* section above
            for the mechanics and the restrictions). An explicit schema always
            overrides the deduction; a topic with no registered schema, a
            multi-process run and a persistent pipeline require one.
        format: Format of the incoming messages: ``"plaintext"``, ``"raw"``,
            ``"json"`` or ``"avro"``. With ``"avro"``, the payloads are bare
            Avro binary datums decoded through the broker's schema registry —
            the *Schema registry* section above describes the mechanics, the
            schema-evolution behavior and the type conversions. A message
            produced under a non-AVRO registry version is reported as a
            per-row error without stopping the pipeline; so is a message
            produced without a schema — unless the table schema is explicit,
            in which case it doubles as the decoding schema for such
            messages.
        mode: Denotes how the engine polls the topic for the new data. If set to
            ``"streaming"``, it waits for new messages indefinitely. Otherwise, in
            the ``"static"`` mode, it reads the messages that are present in the
            topic at the start of the computation and finishes.
        subscription_name: The name of the Pulsar subscription to attach to, for
            the subscription-based reading mechanisms. Providing an explicit
            name creates a durable subscription: its cursor survives pipeline
            redeployments and can be inspected by external tools, but it also
            pins the topic backlog on the broker until the subscription is
            removed, so its lifecycle is the user's responsibility. If not
            set, the connector generates a per-run name and subscribes
            non-durably, leaving no state behind on the broker — but a
            non-durable cursor only lives as long as the connection, so
            whenever the broker drops it (a topic unload, a rebalancing, a
            broker restart) the reading resumes from ``start_from`` and the
            messages processed so far are delivered again (the overview above
            compares the recovery guarantees of the mechanisms). Multi-process
            runs of the ``"shared"`` and ``"key_shared"`` types require an
            explicit name as well, because every process must attach to one
            shared subscription. The partition-reader mode does not create
            broker-side subscriptions and ignores this parameter.
        subscription_type: The reading mechanism. ``"reader"`` is the
            Kafka-like partition-reader mode — required for (and implied by)
            persistence. The subscription modes are ``"shared"`` (messages
            distributed between the workers without ordering guarantees),
            ``"key_shared"`` (distributed by the hash of the message key, so
            the messages with equal keys are processed in order by one
            worker), ``"exclusive"`` and ``"failover"`` (a single active
            consumer, full order, one reading worker). ``None`` (the
            default) selects ``"reader"`` when persistence is enabled and
            ``"shared"`` otherwise. The static mode always uses the
            partition-reader mechanism and ignores this parameter.
        read_compacted: If ``True``, the subscription reads the compacted view
            of the topic: for the part of the topic that has been compacted,
            only the latest message of every partition key is delivered,
            followed by the messages published after the compaction horizon as
            usual. Pulsar restricts compacted reads to the single-consumer
            subscriptions, so this requires the ``"exclusive"`` or
            ``"failover"`` subscription type and the ``"streaming"`` mode; the
            partition-reader mechanism (the static mode, persistence,
            ``subscription_type="reader"``) does not support it.
        autocommit_duration_ms: The maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the
            connector are committed and pushed into Pathway's computation graph.
            Required for streaming reads: a Pulsar topic never signals completion, so
            the timer is the only thing that advances time; ``None`` is rejected at
            construction.
        json_field_paths: If the format is ``"json"``, this field allows to map field
            names into path in the read json object. For the field which require such
            mapping, it should be given in the format ``<field_name>: <path to be
            mapped>``, where the path to be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        autogenerate_key: If ``True``, Pathway autogenerates a unique primary key
            for every message. Otherwise it first tries to use the partition key
            of the message, and autogenerates the key only for the messages
            published without one — so the messages sharing a partition key
            produce rows with the same primary key. Use ``True`` when the
            partition keys repeat and every message must stay a separate row.
            This parameter is only used with the ``"raw"`` and ``"plaintext"``
            formats.
        with_metadata: When set to ``True``, the connector adds an additional
            column named ``_metadata`` to the table, a JSON field describing
            the Pulsar message the row came from: ``topic`` (the physical
            topic, including the ``-partition-N`` suffix for partitioned
            topics), ``partition`` (``-1`` for a non-partitioned topic),
            ``ledger_id``, ``entry_id`` and ``batch_index`` (the components of
            the message id; the batch index is ``-1`` for non-batched
            messages), ``publish_time_millis`` (the broker-assigned publish
            timestamp, in milliseconds since the UNIX epoch),
            ``event_time_millis`` (the producer-assigned event timestamp, or
            ``null`` if the producer didn't set one), ``producer_name``,
            ``ordering_key`` (base64-encoded, or ``null``), ``schema_version``
            (the registry version the message was produced under, or ``null``
            for the messages produced without a schema) and ``properties``
            (the user-defined message properties, a string-to-string map).
        start_from: The position to start reading from, if the subscription does not
            exist yet: ``"beginning"`` reads the topic from the earliest available
            message, ``"end"`` reads only the messages published after the
            computation start, and ``"timestamp"`` delivers only the messages whose
            publish timestamp is at least ``start_from_timestamp_ms`` (the earlier
            messages are consumed and skipped). If the subscription already exists,
            the reading continues from its cursor and the earlier messages are not
            re-read. ``"end"`` cannot be combined with persistence — its position
            would be re-resolved at every restart, losing the downtime window; use
            ``"timestamp"`` with an explicit timestamp instead.
        start_from_timestamp_ms: The publish timestamp, in milliseconds since the
            UNIX epoch, to start reading from. Requires
            ``start_from="timestamp"``.
        auth: The authentication mechanism: ``TokenAuthentication``,
            ``OAuth2Authentication``, or ``None`` (the default) for clusters
            without authentication — see the *Authentication* section above.
        tls_settings: TLS connection settings, described in the
            *Authentication* section above. ``None`` (the default) leaves the
            client with its default certificate verification.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled,
            it will be used as the name for the snapshot that stores the connector's
            progress.
        max_backlog_size: Limit on the number of entries read from the input source
            and kept in processing at any moment. Reading pauses when the limit is
            reached and resumes as processing of some entries completes. Useful with
            large sources that emit an initial burst of data to avoid memory
            spikes.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Consider a topic ``"measurements"`` on a broker running locally, with JSON
    messages of the form ``{"sensor_id": "front-door", "temperature": 21.5}``.
    To parse such messages into a two-column table, describe their fields with
    a schema and pass it together with the ``"json"`` format — the connector
    then creates one column per schema field:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...     sensor_id: str
    ...     temperature: float
    >>> table = pw.io.pulsar.read(
    ...     "pulsar://localhost:6650",
    ...     "measurements",
    ...     format="json",
    ...     schema=InputSchema,
    ... )

    If the payloads are not JSON, the ``"plaintext"`` format reads each message
    into the ``data`` column as a UTF-8 string, with the partition key of the
    message in the ``key`` column (``"raw"`` does the same without decoding,
    producing bytes). No schema is needed:

    >>> table = pw.io.pulsar.read(
    ...     "pulsar://localhost:6650",
    ...     "measurements",
    ...     format="plaintext",
    ... )

    By default the reading starts from the earliest available message and
    continues indefinitely. For a bounded, batch-style computation, the
    ``"static"`` mode reads only the messages present in the topic at the
    start and then finishes the pipeline:

    >>> table = pw.io.pulsar.read(
    ...     "pulsar://localhost:6650",
    ...     "measurements",
    ...     format="json",
    ...     schema=InputSchema,
    ...     mode="static",
    ... )

    A token-authenticated read from a TLS-protected cluster, interested only
    in the messages published after the computation start:

    >>> table = pw.io.pulsar.read(
    ...     "pulsar+ssl://my-cluster.example.com:6651",
    ...     "measurements",
    ...     format="json",
    ...     schema=InputSchema,
    ...     auth=pw.io.pulsar.TokenAuthentication("my-jwt-token"),
    ...     start_from="end",
    ... )

    To replay the history from a specific moment instead — for example, the
    last hour — pass the publish timestamp to start from. The messages
    published earlier are skipped:

    >>> import time
    >>> table = pw.io.pulsar.read(
    ...     "pulsar://localhost:6650",
    ...     "measurements",
    ...     format="json",
    ...     schema=InputSchema,
    ...     start_from="timestamp",
    ...     start_from_timestamp_ms=int(time.time() * 1000) - 3600 * 1000,
    ... )

    The reading position lives in the broker-side subscription. By default the
    connector generates a subscription name; providing an explicit one makes
    the position survive pipeline redeployments (the next run continues from
    where the previous one stopped) and lets external tools inspect the
    subscription's backlog:

    >>> table = pw.io.pulsar.read(
    ...     "pulsar://localhost:6650",
    ...     "measurements",
    ...     format="json",
    ...     schema=InputSchema,
    ...     subscription_name="my-pipeline",
    ... )

    Finally, deduplication. The partition-reader recovery itself introduces no
    duplicates, and neither does a named (durable) subscription. An
    auto-generated subscription does: it is non-durable, so a broker-side
    reconnection restarts it from ``start_from``. The topic may also contain
    duplicates for reasons of its own — a producer that retried a send
    (including ``pw.io.pulsar.write``, whose delivery is at-least-once), or an
    upstream system that emits the same event twice. If
    this matters for the downstream logic — for example, the pipeline counts
    events — and the events carry a unique identifier, the duplicates can be
    removed by grouping on that identifier: every copy of an event has the
    same ``event_id``, so the group produces exactly one row regardless of how
    many copies arrive:

    >>> class EventSchema(pw.Schema):
    ...     event_id: str
    ...     temperature: float
    >>> events = pw.io.pulsar.read(
    ...     "pulsar://localhost:6650",
    ...     "measurements",
    ...     format="json",
    ...     schema=EventSchema,
    ... )
    >>> deduplicated = events.groupby(events.event_id).reduce(
    ...     events.event_id,
    ...     temperature=pw.reducers.earliest(events.temperature),
    ... )
    """
    _check_entitlements("pulsar")
    _check_tls_settings(tls_settings)
    if not topic:
        raise ValueError("Topic name must not be empty")
    if subscription_name is not None and not subscription_name:
        raise ValueError(
            "subscription_name must not be empty: Pulsar cannot attach a "
            "subscription under an empty name, and the connector would deliver "
            "nothing. Drop the parameter to let the connector generate a name."
        )
    if topic.startswith("non-persistent://") and (
        mode == "static" or subscription_type == "reader"
    ):
        raise ValueError(
            "a non-persistent Pulsar topic cannot be read through the "
            "partition-reader mechanism: it stores no messages, so there are "
            "no positions to read from — the static mode has nothing to "
            'snapshot and subscription_type="reader" nothing to replay. '
            "Use a persistent topic, or the streaming mode with a "
            "subscription type"
        )
    if read_compacted and (
        mode != "streaming" or subscription_type not in ("exclusive", "failover")
    ):
        raise ValueError(
            'read_compacted requires the streaming mode and an "exclusive" or '
            '"failover" subscription_type: Pulsar serves the compacted view of '
            "a topic only to single-consumer subscriptions, and the "
            "partition-reader mechanism does not support it"
        )

    effective_timestamp = resolve_start_from_timestamp_ms(
        start_from, start_from_timestamp_ms
    )

    deduced_schema = schema is None and format in ("avro", "json")
    data_storage = api.DataStorage(
        storage_type="pulsar",
        path=uri,
        topic=topic,
        mode=internal_connector_mode(mode),
        durable_consumer_name=subscription_name,
        start_from_timestamp_ms=effective_timestamp,
        with_metadata=with_metadata,
        tls_settings=tls_settings.settings if tls_settings is not None else None,
        pulsar_settings=_construct_pulsar_settings(
            auth, subscription_type, read_compacted=read_compacted
        ),
        deduced_schema=deduced_schema,
    )
    if deduced_schema:
        # The topic's own schema in the broker's registry describes the
        # columns; an explicit schema always overrides the deduction.
        schema = explore_schema(
            data_storage,
            source_description=f"the Pulsar topic {topic!r}",
            format=format,
        )
    schema, data_format = construct_schema_and_data_format(
        "binary" if format == "raw" else format,
        schema=schema,
        json_field_paths=json_field_paths,
        autogenerate_key=autogenerate_key,
        with_metadata=with_metadata,
        with_native_record_key=True,
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
            datasource_name="pulsar",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_raw_and_plaintext_only_kwargs_for_message_queues
@check_arg_types
@trace_user_frame
def write(
    table: Table,
    uri: str,
    topic: str | ColumnReference,
    *,
    format: Literal["json", "dsv", "raw", "plaintext", "avro"] = "json",
    delimiter: str = ",",
    key: ColumnReference | None = None,
    ordering_key: ColumnReference | None = None,
    event_time: ColumnReference | EngineTimeMarker | None = None,
    deliver_at: ColumnReference | None = None,
    deliver_after: ColumnReference | datetime.timedelta | None = None,
    value: ColumnReference | None = None,
    headers: Iterable[ColumnReference] | None = None,
    compression: Literal["lz4", "zlib", "zstd"] | None = None,
    producer_name: str | None = None,
    auth: PulsarAuthentication | None = None,
    tls_settings: TLSSettings | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes data into an `Apache Pulsar <https://pulsar.apache.org/>`_ topic.

    Every update of the table becomes a Pulsar message: an insertion carries the
    ``pathway_diff`` property equal to ``1``, and a deletion of a previously sent
    row carries ``pathway_diff`` equal to ``-1`` (the attached properties and the
    partition key are described in the *Delivery semantics* section above).

    Args:
        table: The table to write.
        uri: The Pulsar service URI, e.g. ``pulsar://localhost:6650`` or
            ``pulsar+ssl://my-cluster:6651`` for a TLS-encrypted connection.
        topic: The name of the topic to write to. It can also be a reference to
            a string column of the table: then each row is produced into the topic
            given by the value of this column. In the ``"avro"`` format the topic
            column — like the other service columns — is not a part of the
            payload; the other formats include every column of the table in the
            payload.
        format: Format in which the message payload is produced. Can be
            ``"json"``, ``"dsv"``, ``"plaintext"``, ``"raw"`` or ``"avro"``. For
            ``"plaintext"`` and ``"raw"``, the table must consist of a single
            column of the string or binary type respectively, unless the ``value``
            parameter points at the payload column explicitly. For ``"avro"``,
            the payload is the Avro binary encoding of the row, and the derived
            schema is declared to the broker's schema registry — see the
            *Schema registry* section above for the mechanics and the type
            conversions. The columns designated as the service inputs —
            ``topic``, ``key``, ``ordering_key``, ``event_time``, ``deliver_at``,
            ``deliver_after``, ``headers`` — stay out of the record; duplicate a column under another name with
            ``table.select(...)`` if it must also be a part of the payload.
        delimiter: The delimiter separating the fields, if the ``"dsv"`` format is
            used.
        key: The column carrying the partition key of the messages. The column must
            be of the string or binary type; a binary key must be valid UTF-8,
            because Pulsar stores partition keys as strings. If not specified, the
            key is derived from the row's primary key.
        ordering_key: The column carrying the ordering key of the messages, of the
            string or binary type. The ordering key is what a ``key_shared``
            subscription hashes when distributing the messages between its
            consumers: setting it keeps the messages of one entity in order on one
            consumer while the partition routing still follows ``key``. If not
            specified, the ordering key is left unset and ``key_shared``
            subscriptions fall back to the partition key.
        event_time: Where the ``event_time`` of the messages comes from. A
            column reference takes the value from that column of the table:
            an integer (milliseconds since the UNIX epoch) or a UTC datetime.
            The ``pw.io.ENGINE_TIME`` marker uses the engine (minibatch) time
            of the update — the same UNIX timestamp the messages carry in the
            ``pathway_time`` property, but placed into the native
            ``event_time`` field, so the consumers read it without parsing
            the properties. If not set (the default), the ``event_time``
            field is left unset and the messages only carry the
            broker-assigned publish time. Note that for the static tables
            created with ``pw.debug`` utilities the engine time is a small
            logical counter rather than a wall-clock timestamp; data read
            through ``pw.io.*`` connectors always carries wall-clock engine
            times, in both the streaming and the static modes.
        deliver_at: The column carrying the delivery time of the messages,
            for the *delayed delivery* — an integer (milliseconds since the
            UNIX epoch) or a UTC datetime. The broker accepts and stores such
            a message right away, but hands it to the consumers only once the
            delivery time comes; a delivery time in the past delivers the
            message immediately. Mutually exclusive with ``deliver_after``.
            See the *Delayed delivery* section below for what the consumers
            must look like to honor the schedule, and for the configurations
            that are rejected.
        deliver_after: The delay of the delivery, counted from the moment
            the message is published: either a constant ``datetime.timedelta``
            applied to every message, or a column reference holding the delay
            of each row — a duration or an integer number of milliseconds.
            Mutually exclusive with ``deliver_at``; the same caveats apply.
        value: The column carrying the payload of the messages in the
            ``"plaintext"`` or ``"raw"`` formats. Can be omitted if the table has
            exactly one column, which then becomes the payload.
        headers: Columns to attach to every message as its properties. The values
            are serialized to JSON strings, because Pulsar message properties are
            string-to-string pairs.
        compression: The codec the message payloads are compressed with:
            ``"lz4"``, ``"zlib"``, ``"zstd"``, or ``None`` (the default) to
            send them uncompressed. Compression is transparent to the
            consumers: the codec travels in the message metadata, so any
            Pulsar client — including ``pw.io.pulsar.read`` — decompresses the
            messages automatically, with no matching setting on the reading
            side. Snappy is not supported: the underlying client library frames
            it differently from the other Pulsar clients, so such messages would
            be unreadable outside Pathway.
        producer_name: The name the producers register themselves under on the
            broker, visible in the topic stats and logs — useful to identify
            the pipeline among the writers of a topic. Pulsar requires
            producer names to be unique within a topic and every Pathway
            worker runs its own producer, so the worker index is appended to
            the name (e.g. ``my-pipeline-0``). If not set, the broker assigns
            a generated name.
        auth: The authentication mechanism: ``TokenAuthentication``,
            ``OAuth2Authentication``, or ``None`` (the default) for clusters
            without authentication — see the *Authentication* section above.
        tls_settings: TLS connection settings, described in the
            *Authentication* section above. ``None`` (the default) leaves the
            client with its default certificate verification.
        name: A unique name for the connector. If provided, this name will be used
            in logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on
            the values of the given columns within each minibatch. When multiple
            columns are provided, the corresponding value tuples will be compared
            lexicographically.

    Returns:
        None

    Delayed delivery:

    ``deliver_at`` and ``deliver_after`` schedule the messages for a later
    delivery. The schedule is honored only by the consumers of the
    ``"shared"`` and ``"key_shared"`` subscriptions — the ``"exclusive"`` and
    ``"failover"`` subscriptions and the partition readers receive the
    messages as soon as they are published, as the Pulsar protocol defines.
    ``pw.io.pulsar.read`` therefore observes the schedule only with
    ``subscription_type="shared"`` or ``"key_shared"`` in the streaming
    mode; its default partition-reader mechanism, the persistence-enabled
    pipelines and the static mode read the scheduled messages immediately.
    The broker must run with the delayed delivery enabled
    (``delayedDeliveryEnabled``, the default), otherwise it ignores the
    schedule; it releases the messages with the precision of its
    delayed-delivery tick (``delayedDeliveryTickTimeMillis``, a second by
    default). Every update of the table follows the schedule of its row,
    the deletions included.

    The configurations that cannot work are rejected instead of publishing
    messages that would be delivered immediately: ``deliver_at`` and
    ``deliver_after`` together, a timezone-naive datetime column, a column of
    another type, a negative delay or delivery time (at the construction or
    when the row is written), and a ``non-persistent://`` topic, which stores
    no messages and dispatches everything at once. The scheduled messages are
    published one by one rather than in the producer batches, so the write
    throughput with a schedule is lower than without one.

    Example:

    Suppose you want to send a stream of updates of the table ``t`` to a locally
    running Pulsar instance. First, create a sample table:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ...     '''
    ...     age | owner | pet
    ...     10  | Alice | dog
    ...     9   | Bob   | cat
    ...     8   | Alice | cat
    ...     '''
    ... )

    The simplest write sends every update of the table into the topic
    ``"clients"`` as a JSON message. Each message contains all the columns of
    the table, plus the ``pathway_time`` and ``pathway_diff`` properties
    describing the update:

    >>> pw.io.pulsar.write(t, "pulsar://localhost:6650", "clients")

    If the receiving side expects a plain string instead of JSON, use the
    ``"plaintext"`` format and point ``value`` at the column that carries the
    payload:

    >>> pw.io.pulsar.write(
    ...     t,
    ...     "pulsar://localhost:6650",
    ...     "clients",
    ...     format="plaintext",
    ...     value=t.owner,
    ... )

    The topic doesn't have to be fixed: if it is given as a column reference,
    each row is produced into the topic named by that column's value (see the
    ``topic`` parameter for whether the column is part of the payload).
    Combined with ``key``, which
    pins the partition of a partitioned topic (rows with equal keys keep their
    order), one ``write`` call can route a single table into many topics:

    >>> pw.io.pulsar.write(
    ...     t,
    ...     "pulsar://localhost:6650",
    ...     topic=t.owner,
    ...     key=t.pet,
    ... )

    Additional per-message metadata can travel in the message properties. The
    ``headers`` columns are attached to every message as JSON-serialized
    string properties, so the consumer can inspect them without parsing the
    payload:

    >>> pw.io.pulsar.write(
    ...     t,
    ...     "pulsar://localhost:6650",
    ...     "clients",
    ...     headers=[t.age, t.pet],
    ... )

    To reduce the network traffic and the storage used by the topic, the
    payloads can be compressed. The consumers decompress transparently, so no
    setting is needed on the reading side:

    >>> pw.io.pulsar.write(
    ...     t,
    ...     "pulsar://localhost:6650",
    ...     "clients",
    ...     compression="zstd",
    ... )

    A message can be scheduled for a later delivery. With a constant delay
    every message is handed to the ``"shared"`` and ``"key_shared"``
    consumers a minute after it is published:

    >>> import datetime
    >>> pw.io.pulsar.write(
    ...     t,
    ...     "pulsar://localhost:6650",
    ...     "clients",
    ...     deliver_after=datetime.timedelta(minutes=1),
    ... )

    The schedule can also come from the data — for example, a reminder is
    delivered at the time stored in its row, given either as an integer
    number of milliseconds since the UNIX epoch or as a UTC datetime:

    >>> reminders = pw.debug.table_from_markdown(
    ...     '''
    ...     text        | remind_at_ms
    ...     water-plants | 1893456000000
    ...     call-alice   | 1893542400000
    ...     '''
    ... )
    >>> pw.io.pulsar.write(
    ...     reminders,
    ...     "pulsar://localhost:6650",
    ...     "reminders",
    ...     deliver_at=reminders.remind_at_ms,
    ... )

    Finally, for a token-protected cluster pass the authentication object —
    the same way as in ``read``:

    >>> pw.io.pulsar.write(
    ...     t,
    ...     "pulsar+ssl://my-cluster.example.com:6651",
    ...     "clients",
    ...     auth=pw.io.pulsar.TokenAuthentication("my-jwt-token"),
    ... )
    """
    _check_entitlements("pulsar")
    _check_tls_settings(tls_settings)
    if isinstance(topic, str) and not topic:
        raise ValueError("Topic name must not be empty")
    if producer_name is not None and not producer_name:
        raise ValueError(
            "producer_name must not be empty. Drop the parameter to let the "
            "broker assign a generated name."
        )

    if deliver_at is not None and deliver_after is not None:
        raise ValueError(
            "'deliver_at' and 'deliver_after' cannot be set at the same time: "
            "a message has a single delivery time, given either as an absolute "
            "timestamp or as a delay from the publishing."
        )
    deliver_after_column: ColumnReference | None = None
    deliver_after_millis: int | None = None
    if isinstance(deliver_after, datetime.timedelta):
        if deliver_after < datetime.timedelta(0):
            raise ValueError(
                f"'deliver_after' must not be negative, got {deliver_after!r}. "
                "Drop the parameter to deliver the messages immediately."
            )
        deliver_after_millis = deliver_after // datetime.timedelta(milliseconds=1)
    else:
        deliver_after_column = deliver_after
    if (
        isinstance(topic, str)
        and topic.startswith("non-persistent://")
        and (deliver_at is not None or deliver_after is not None)
    ):
        raise ValueError(
            f"the non-persistent Pulsar topic '{topic}' ignores the delivery "
            "schedule (deliver_at / deliver_after): such topics store no "
            "messages and the broker dispatches everything immediately. "
            "Publish the delayed messages into a persistent topic."
        )

    event_time_column: ColumnReference | None
    if isinstance(event_time, EngineTimeMarker):
        event_time_from_engine = True
        event_time_column = None
    else:
        event_time_from_engine = False
        event_time_column = event_time
    output_format = MessageQueueOutputFormat.construct(
        table,
        format=format,
        delimiter=delimiter,
        key=key,
        ordering_key=ordering_key,
        event_time=event_time_column,
        deliver_at=deliver_at,
        deliver_after=deliver_after_column,
        value=value,
        headers=headers,
        topic_name=topic if isinstance(topic, ColumnReference) else None,
    )
    # The sink is attached to the table `construct` rebuilt, so the
    # sort_by references taken against the original one are remapped.
    sort_by = remap_sort_by(sort_by, table, output_format.table, "pw.io.pulsar.write")
    table = output_format.table

    data_storage = api.DataStorage(
        storage_type="pulsar",
        path=uri,
        topic=topic if isinstance(topic, str) else None,
        topic_name_index=output_format.topic_name_index,
        key_field_index=output_format.key_field_index,
        ordering_key_field_index=output_format.ordering_key_field_index,
        event_time_field_index=output_format.event_time_field_index,
        deliver_at_field_index=output_format.deliver_at_field_index,
        deliver_after_field_index=output_format.deliver_after_field_index,
        header_fields=list(output_format.header_fields.items()),
        tls_settings=tls_settings.settings if tls_settings is not None else None,
        pulsar_settings=_construct_pulsar_settings(
            auth,
            compression=compression,
            producer_name=producer_name,
            event_time_from_engine=event_time_from_engine,
            deliver_after_millis=deliver_after_millis,
        ),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            output_format.data_format,
            datasink_name="pulsar",
            unique_name=name,
            sort_by=sort_by,
        )
    )

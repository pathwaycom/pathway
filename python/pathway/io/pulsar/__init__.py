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
    auth: PulsarAuthentication | None, subscription_type: str | None = None
) -> api.PulsarSettings | None:
    kwargs = auth._settings_kwargs() if auth is not None else {}
    if subscription_type is not None:
        kwargs["subscription_type"] = subscription_type
    if not kwargs:
        return None
    return api.PulsarSettings(**kwargs)


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
    format: Literal["plaintext", "raw", "json"] = "raw",
    mode: Literal["streaming", "static"] = "streaming",
    subscription_name: str | None = None,
    subscription_type: (
        Literal["reader", "shared", "key_shared", "exclusive", "failover"] | None
    ) = None,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
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

    There are three formats supported: ``"plaintext"``, ``"raw"``, and ``"json"``.

    For the ``"raw"`` format, the payload is read as raw bytes and added directly to
    the table. In the ``"plaintext"`` format, the payload is decoded from UTF-8 and
    stored as plain text. In both cases, the table will have a ``"data"`` column
    representing the payload.

    If ``"json"`` is chosen, the connector parses the message payload as JSON and
    creates table columns based on the schema provided in the ``schema`` parameter.

    Args:
        uri: The Pulsar service URI, e.g. ``pulsar://localhost:6650`` or
            ``pulsar+ssl://my-cluster:6651`` for a TLS-encrypted connection.
        topic: The name of the topic to read from.
        schema: Schema of the resulting table. Required for the ``"json"`` format.
        format: Format of the incoming messages: ``"plaintext"``, ``"raw"``, or
            ``"json"``.
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
            non-durably, leaving no state behind on the broker; multi-process
            runs of the ``"shared"`` and ``"key_shared"`` types require an
            explicit name, because every process must attach to one shared
            subscription. The partition-reader mode does not create
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
        autocommit_duration_ms: The maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the
            connector are committed and pushed into Pathway's computation graph.
        json_field_paths: If the format is ``"json"``, this field allows to map field
            names into path in the read json object. For the field which require such
            mapping, it should be given in the format ``<field_name>: <path to be
            mapped>``, where the path to be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
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
            ``OAuth2Authentication``, or ``None`` for clusters without
            authentication.
        tls_settings: TLS connection settings. Use ``TLSSettings`` to provide the CA
            certificate used to verify the broker (``root_cert_path``) or, for
            development setups, to accept any broker certificate
            (``trust_certificates=True``). The connection is encrypted when the
            ``uri`` uses the ``pulsar+ssl://`` scheme. Mutual TLS (client
            certificate) authentication is not supported.
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
    into a single ``data`` column as a UTF-8 string (and ``"raw"`` does the
    same without decoding, producing bytes). No schema is needed:

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
    duplicates, but the topic may contain them for other reasons — a producer
    that retried a send (including ``pw.io.pulsar.write``, whose delivery is
    at-least-once), or an upstream system that emits the same event twice. If
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

    effective_timestamp = resolve_start_from_timestamp_ms(
        start_from, start_from_timestamp_ms
    )

    data_storage = api.DataStorage(
        storage_type="pulsar",
        path=uri,
        topic=topic,
        mode=internal_connector_mode(mode),
        durable_consumer_name=subscription_name,
        start_from_timestamp_ms=effective_timestamp,
        tls_settings=tls_settings.settings if tls_settings is not None else None,
        pulsar_settings=_construct_pulsar_settings(auth, subscription_type),
    )
    schema, data_format = construct_schema_and_data_format(
        "binary" if format == "raw" else format,
        schema=schema,
        json_field_paths=json_field_paths,
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
    format: Literal["json", "dsv", "raw", "plaintext"] = "json",
    delimiter: str = ",",
    key: ColumnReference | None = None,
    value: ColumnReference | None = None,
    headers: Iterable[ColumnReference] | None = None,
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
            given by the value of this column, and the column itself is excluded
            from the message payload.
        format: Format in which the message payload is produced. Can be
            ``"json"``, ``"dsv"``, ``"plaintext"`` or ``"raw"``. For
            ``"plaintext"`` and ``"raw"``, the table must consist of a single
            column of the string or binary type respectively, unless the ``value``
            parameter points at the payload column explicitly.
        delimiter: The delimiter separating the fields, if the ``"dsv"`` format is
            used.
        key: The column carrying the partition key of the messages. The column must
            be of the string or binary type. If not specified, the key is derived
            from the row's primary key.
        value: The column carrying the payload of the messages in the
            ``"plaintext"`` or ``"raw"`` formats. Can be omitted if the table has
            exactly one column, which then becomes the payload.
        headers: Columns to attach to every message as its properties. The values
            are serialized to JSON strings, because Pulsar message properties are
            string-to-string pairs.
        auth: The authentication mechanism: ``TokenAuthentication``,
            ``OAuth2Authentication``, or ``None`` for clusters without
            authentication.
        tls_settings: TLS connection settings. Use ``TLSSettings`` to provide the CA
            certificate used to verify the broker (``root_cert_path``) or, for
            development setups, to accept any broker certificate
            (``trust_certificates=True``). The connection is encrypted when the
            ``uri`` uses the ``pulsar+ssl://`` scheme. Mutual TLS (client
            certificate) authentication is not supported.
        name: A unique name for the connector. If provided, this name will be used
            in logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on
            the values of the given columns within each minibatch. When multiple
            columns are provided, the corresponding value tuples will be compared
            lexicographically.

    Returns:
        None

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
    each row is produced into the topic named by that column's value, and the
    column itself is excluded from the payload. Combined with ``key``, which
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

    output_format = MessageQueueOutputFormat.construct(
        table,
        format=format,
        delimiter=delimiter,
        key=key,
        value=value,
        headers=headers,
        topic_name=topic if isinstance(topic, ColumnReference) else None,
    )
    table = output_format.table

    data_storage = api.DataStorage(
        storage_type="pulsar",
        path=uri,
        topic=topic if isinstance(topic, str) else None,
        topic_name_index=output_format.topic_name_index,
        key_field_index=output_format.key_field_index,
        header_fields=list(output_format.header_fields.items()),
        tls_settings=tls_settings.settings if tls_settings is not None else None,
        pulsar_settings=_construct_pulsar_settings(auth),
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

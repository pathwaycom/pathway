# Copyright © 2026 Pathway

from __future__ import annotations

import copy
import logging
import urllib.parse
import warnings
from typing import Any, Iterable, Literal, Optional

from pathway.internals import api, datasink, datasource, dtype
from pathway.internals._io_helpers import TLSSettings, _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Schema, Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    SNAPSHOT_OUTPUT_TABLE_TYPE,
    get_column_index,
    init_mode_from_str,
    read_schema,
)
from pathway.schema import schema_builder


def _quote_libpq_value(value) -> str:
    """Serialize a libpq connection-string value safely.

    libpq parses keyword/value connection strings by splitting on
    whitespace, so any value that contains a space, a backslash, or a
    single quote must be wrapped in single quotes with ``\\`` and ``'``
    escaped. Quoting unconditionally is equally valid and keeps the
    code simple. Non-string values fall through ``str()`` first.
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _build_application_name(unique_name: str | None) -> str:
    """Build the ``application_name`` libpq parameter so that every
    The Pathway Live Data Framework connection is identifiable in ``pg_stat_activity`` /
    ``pg_stat_replication`` and in ``log_line_prefix`` lines that
    include ``%a``.

    PostgreSQL stores up to ``NAMEDATALEN - 1`` = 63 bytes and silently
    truncates the rest; non-printable bytes (anything outside the
    printable ASCII range ``0x20``–``0x7E``) get replaced with ``?``
    on the server side, which is useless for log filtering. We
    pre-sanitize and pre-truncate so operators see the value the
    Python side intended rather than PG's ``?``-mangling.

    The value is ``pathway`` when no ``unique_name`` is given, and
    ``pathway:<unique_name>`` otherwise. Uniqueness is *not* a
    PostgreSQL requirement — pid + start time uniquely identify a
    session in ``pg_stat_activity`` regardless — so duplicate names
    are allowed; they just lose the operator-readable
    name-to-connector mapping for the duplicates.
    """
    prefix = "pathway"
    if not unique_name:
        return prefix
    suffix = ":" + unique_name
    sanitized = "".join(c if 0x20 <= ord(c) <= 0x7E else "_" for c in suffix)
    return (prefix + sanitized)[:63]


# Defaults injected into ``postgres_settings`` for any key the user
# has not set. ``application_name`` is filled in by
# :func:`_augment_postgres_settings` per-call because it depends on
# the connector's ``name`` parameter.
#
# These keys follow libpq's canonical spelling (and libpq's unit for
# ``tcp_user_timeout``: milliseconds). The streaming reader goes
# through libpq directly, so the URL form accepts them verbatim.
# The static reader/writer KV form is consumed by a parser whose
# name set and ``tcp_user_timeout`` unit differ from libpq — those
# differences are absorbed inside
# :func:`_connection_string_from_settings`, so the same libpq-style
# dict produces equivalent TCP-keepalive behavior on both paths.
#
# Detection-time math: with ``keepalives_idle=300`` and
# ``keepalives_interval=30 × keepalives_count=3``, an idle but
# half-broken connection is declared dead at 300 + 3 × 30 = 390 s
# (~6.5 min). ``tcp_user_timeout=300000`` ms (= 5 min) covers the
# active-connection case where the wal_sender is shipping bytes but
# the client isn't ACKing (keepalives don't fire on active
# connections).
#
# These are tuned to be conservative on the "minimize false
# positives" axis: routine network blips (NAT rebinding, brief
# routing changes, single-AZ failover) typically resolve in under a
# minute, and Pathway's temporary replication slots auto-clean on
# the next disconnect, so the cost of a slow detection is just a
# bit more WAL pinned briefly — not the days-long retention you'd
# get from a leaked persistent slot.
_LIBPQ_TIMEOUT_DEFAULTS = {
    "keepalives": "1",
    "keepalives_idle": "300",
    "keepalives_interval": "30",
    "keepalives_count": "3",
    "tcp_user_timeout": "300000",
}


# Translation applied when serializing ``postgres_settings`` to the
# KV form consumed by the static reader/writer. The streaming reader
# goes through libpq (URL form) and accepts the libpq spellings
# verbatim; the KV parser does not — it spells ``keepalives_count``
# as ``keepalives_retries`` and interprets ``tcp_user_timeout`` as
# seconds, not the milliseconds documented in libpq. Translating
# here lets the user (and the defaults above) speak a single
# libpq-canonical dialect.
_KV_KEY_RENAME = {
    "keepalives_count": "keepalives_retries",
}


def _kv_translate_value(key: str, value):
    """Apply libpq → KV-parser unit conversions for select keys.
    Returns the value unchanged when no translation applies.
    """
    if key == "tcp_user_timeout":
        try:
            ms = int(str(value))
        except (TypeError, ValueError):
            return value
        return str(ms // 1000)
    return value


def _augment_postgres_settings(settings: dict, unique_name: str | None) -> dict:
    """Return a copy of ``settings`` with the Pathway Live Data Framework-managed defaults
    (``application_name`` and TCP-keepalive tuning) injected for any
    key the user did not provide. The user's explicit values always
    win — :py:meth:`dict.setdefault` is the explicit Python idiom for
    "set if absent" and is what we rely on here.
    """
    augmented = dict(settings)
    augmented.setdefault("application_name", _build_application_name(unique_name))
    for key, value in _LIBPQ_TIMEOUT_DEFAULTS.items():
        augmented.setdefault(key, value)
    return augmented


def _connection_string_from_settings(settings: dict):
    out = []
    for k, v in settings.items():
        kv_key = _KV_KEY_RENAME.get(k, k)
        kv_value = _kv_translate_value(k, v)
        out.append(f"{kv_key}={_quote_libpq_value(kv_value)}")
    return " ".join(out)


def _replication_connection_string_from_settings(settings: dict):
    owned_settings = copy.copy(settings)

    # URL-form connection strings require percent-encoding of
    # user-supplied components — otherwise characters like ``@``, ``:``,
    # ``/``, ``?``, ``#``, or whitespace inside a password silently
    # corrupt the parsed URL.
    def enc(v) -> str:
        return urllib.parse.quote(str(v), safe="")

    # Every URL component is optional. We mirror the static path
    # (which builds a libpq keyword/value string and lets libpq apply
    # its own defaults for missing keys) by omitting absent components
    # from the URL — libpq then resolves them through the normal
    # mechanisms (OS user for ``user``, ``~/.pgpass`` for
    # ``password``, UNIX socket for ``host``, ``5432`` for ``port``,
    # ``user`` for ``dbname``). The server-side ``pg_hba.conf``
    # decides whether the resulting connection is authorized; for
    # ``trust`` / ``peer`` / ``cert`` auth modes the absence of a
    # password is the normal case, and rejecting at the Python
    # boundary would lock those configurations out.
    user = owned_settings.pop("user", None)
    password = owned_settings.pop("password", None)
    host = owned_settings.pop("host", None)
    port = owned_settings.pop("port", None)
    dbname = owned_settings.pop("dbname", None)

    userinfo = ""
    if user is not None:
        userinfo = enc(user)
        if password is not None:
            userinfo += ":" + enc(password)
        userinfo += "@"
    elif password is not None:
        # The URL userinfo slot is ``user[:password]@`` — there is no
        # legal form for a password without a user. libpq's KV format
        # accepts ``password=...`` standalone (letting it pair with
        # libpq's default user), so push the password into the URL
        # query string, which libpq treats identically.
        owned_settings["password"] = password

    hostport = ""
    if host is not None:
        hostport = enc(host)
        if port is not None:
            hostport += f":{port}"
    elif port is not None:
        # ``postgresql://:5432`` would be parsed as
        # ``host="", port=5432``, which is ambiguous. Push the port
        # into the query string where its meaning is unambiguous.
        owned_settings["port"] = port

    if dbname is not None:
        path = "/" + enc(dbname)
    elif not hostport and not userinfo:
        # Without any authority component the URL would otherwise
        # render as ``postgresql://?<query>``. libpq's URI grammar
        # accepts the empty-authority form, but every documented
        # example uses ``postgresql:///<query>`` instead — emit the
        # explicit path separator so the URL stays in the documented
        # shape.
        path = "/"
    else:
        path = ""

    owned_settings["replication"] = "database"
    query = "&".join(f"{enc(k)}={enc(v)}" for (k, v) in owned_settings.items())

    return f"postgresql://{userinfo}{hostport}{path}?{query}"


def _build_tls_settings(owned_postgres_settings: dict) -> TLSSettings:
    sslmode = owned_postgres_settings.pop("sslmode", "prefer")

    sslrootcert = owned_postgres_settings.pop("sslrootcert", None)
    if sslrootcert is not None:
        try:
            open(sslrootcert).close()
        except IsADirectoryError as e:
            raise ValueError("sslrootcert doesn't point to a file") from e
        except FileNotFoundError as e:
            raise ValueError("sslrootcert points to a non-existent path") from e
        except OSError as e:
            raise ValueError(f"sslrootcert is not readable: {e}") from e

    return TLSSettings(mode=sslmode, root_cert_path=sslrootcert)


def _construct_replication_settings(
    *,
    mode: Literal["streaming", "static"],
    postgres_settings: dict,
    publication_name: str | None,
    replication_slot_name: str | None,
    snapshot_name: str | None,
):
    # static mode doesn't require replication slots
    if mode == "static":
        if publication_name is not None:
            raise ValueError("'publication_name' is not needed for the static mode")
        if replication_slot_name is not None:
            raise ValueError(
                "'replication_slot_name' is not needed for the static mode"
            )
        if snapshot_name is not None:
            raise ValueError("'snapshot_name' is not needed for the static mode")
        return None
    if publication_name is None:
        raise ValueError("'publication_name' is required for the streaming mode")

    # streaming mode: user provides publication, we create the slot
    replication_slot_defined = replication_slot_name is None
    snapshot_name_defined = snapshot_name is None
    if replication_slot_defined != snapshot_name_defined:
        raise ValueError(
            "Either none or both of 'replication_slot_name', 'snapshot_name' must be specified"
        )

    return api.PsqlReplicationSettings(
        connection_string=(
            _replication_connection_string_from_settings(postgres_settings)
        ),
        publication_name=publication_name,
        snapshot_name=snapshot_name,
        replication_slot_name=replication_slot_name,
    )


@check_arg_types
@trace_user_frame
def read(
    postgres_settings: dict,
    table_name: str,
    schema: type[Schema] | None = None,
    *,
    mode: Literal["streaming", "static"] = "streaming",
    is_append_only: bool = False,
    publication_name: str | None = None,
    schema_name: str | None = "public",
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data: Any = None,
) -> Table:
    """
    **This module is available when using one of the following licenses only:**
    `Pathway Live Data Framework Scale, Pathway Live Data Framework Enterprise </pricing>`_.

    Reads a table from a PostgreSQL database.

    This connector provides a lightweight alternative to ``pw.io.debezium.read``.
    It supports two modes: ``"static"`` and ``"streaming"``.

    In ``"static"`` mode, the table is read once and the connector stops afterward.

    In ``"streaming"`` mode, a *temporary* replication slot is created using the
    ``pgoutput`` logical decoding plugin, which is bundled with PostgreSQL and requires
    no additional installation. The slot is created with the ``export snapshot`` option,
    ensuring a consistent initial read. On startup, the connector first performs a
    snapshot of the table as it existed at the moment the replication slot was created,
    and then begins consuming the PostgreSQL write-ahead log (WAL), applying incremental
    changes on top of that snapshot. Because the replication slot is temporary,
    PostgreSQL will automatically drop it once the connection is closed (i.e., when the
    program terminates).

    To enable replication, a publication must be created in the database beforehand:

    .. code-block:: sql

        CREATE PUBLICATION {publication_name} FOR TABLE {table_name};

    Args:
        postgres_settings: Connection parameters for PostgreSQL, provided as a
            dictionary of key-value pairs. The connection string is assembled by joining
            all pairs with spaces, each formatted as ``key=value``. Keys must be strings;
            values of other types are converted via Python's ``str()``.
            The Pathway Live Data Framework injects conservative TCP-keepalive defaults (``keepalives``,
            ``keepalives_idle=300``, ``keepalives_interval=30``, ``keepalives_count=3``,
            and ``tcp_user_timeout=300000``) so that an unreachable
            the Pathway Live Data Framework process is detected by PostgreSQL within minutes rather than
            the OS-inherited ~2-hour default; any of these can be overridden by
            passing the same key in ``postgres_settings``.
        table_name: Name of the PostgreSQL table to read from. Any PostgreSQL
            identifier is accepted — the connector quotes the name before
            interpolating it into generated SQL, so hyphens, mixed case, and
            reserved words round-trip as-is.
        schema: Pathway Live Data Framework schema describing the table's columns and their types.
            Column names may be any PostgreSQL identifier for the same reason
            as ``table_name``.
        mode: Polling mode for the connector. Accepted values are ``"streaming"``
            (default) and ``"static"``. In ``"streaming"`` mode, the connector tracks
            changes in the table via the WAL, reflecting insertions, updates, deletions,
            and truncations in real time; requires ``publication_name`` to be specified.
            In ``"static"`` mode, the connector reads all currently available rows in a
            single commit and then stops.
        is_append_only: Used in streaming mode. Specifies whether the input table is append-only.
            If the table is not append-only, it must have a primary key, and all primary key
            columns must be declared as such in the schema. This is required because when reading
            diffs from the log, update and delete records only expose the primary key columns
            of the affected rows. If the table is declared as append-only but a deletion,
            truncation or modification is encountered, an error is raised.
        publication_name: Name of the PostgreSQL publication that covers the target
            table. Required when ``mode="streaming"``.
        schema_name: Name of the PostgreSQL schema in which the table resides.
            Defaults to ``"public"``; only needs to be changed when using a non-default
            schema.
        autocommit_duration_ms: the maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the connector
            are committed and pushed into Pathway Live Data Framework's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
            It is also surfaced to PostgreSQL as part of the connection's ``application_name``
            (``pathway:<name>``), so operators can filter ``pg_stat_activity`` and server logs
            by connector.
        max_backlog_size: Limit on the number of entries read from the input source and kept
            in processing at any moment. Reading pauses when the limit is reached and resumes
            as processing of some entries completes. Useful with large sources that
            emit an initial burst of data to avoid memory spikes.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Suppose you have a ``users`` table with the following columns: ``id`` (an auto-incremented
    integer serving as the primary key), ``login`` (a string), and ``last_seen_at`` (a unix
    timestamp). To read this table with the Pathway Live Data Framework, start by declaring the corresponding schema:

    >>> import pathway as pw
    >>> class UsersSchema(pw.Schema):
    ...     id: int = pw.column_definition(primary_key=True)
    ...     login: str
    ...     last_seen_at: int

    To perform a one-time read of the table, no additional database configuration is required.
    Simply provide the connection parameters and use ``"static"`` mode:

    >>> connection_string_parts = {
    ...     "host": "localhost",
    ...     "port": "5432",
    ...     "dbname": "database",
    ...     "user": "user",
    ...     "password": "pass",
    ... }

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="users",
    ...     schema=UsersSchema,
    ...     mode="static",
    ... )

    The resulting ``table`` object supports all Pathway Live Data Framework transformations and can be passed
    to any output connector for further processing or storage.

    To go beyond a one-time snapshot and perform Change Data Capture (CDC), continuously
    tracking insertions, updates, deletions, and truncations as they happen, you need to
    switch to ``"streaming"`` mode. This requires the PostgreSQL server to have logical replication
    enabled (``wal_level = logical``) and a publication to be created for the target table:

    .. code-block:: sql

        CREATE PUBLICATION users_pub FOR TABLE users;

    With the publication in place, the streaming connector can be configured as follows:

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="users",
    ...     schema=UsersSchema,
    ...     mode="streaming",
    ...     publication_name="users_pub",
    ... )

    There is no need to create a replication slot manually, and doing so is strongly
    discouraged. A replication slot causes PostgreSQL to retain WAL segments until all
    changes have been acknowledged by the consumer. If a slot is created but its LSN
    position is not advanced regularly, unacknowledged WAL can accumulate and eventually
    exhaust disk space on the database server. To prevent this, the Pathway Live Data Framework manages the
    replication slot internally: it uses a temporary slot that is automatically dropped
    when the session ends, and continuously acknowledges processed LSN positions while
    the program is running.

    The examples above use an integer primary key, but other primary key types are
    supported as well.

    Suppose you have a ``products`` table where each row is identified by a string
    product code such as ``"SKU-001"``, alongside a ``name`` column and a ``price``
    column. The schema in this case is:

    >>> class ProductsSchema(pw.Schema):
    ...     sku: str = pw.column_definition(primary_key=True)
    ...     name: str
    ...     price: float

    Both ``"static"`` and ``"streaming"`` modes are supported, set up in exactly
    the same way as for an integer primary key. For a one-time snapshot:

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="products",
    ...     schema=ProductsSchema,
    ...     mode="static",
    ... )

    For continuous CDC, create a publication first:

    .. code-block:: sql

        CREATE PUBLICATION products_pub FOR TABLE products;

    Then configure the streaming connector:

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="products",
    ...     schema=ProductsSchema,
    ...     mode="streaming",
    ...     publication_name="products_pub",
    ... )

    PostgreSQL's ``UUID`` type is also supported. Because Pathway Live Data Framework represents UUID
    values as strings, the corresponding schema field must be declared as ``str``.
    Suppose you have a ``messages`` table whose primary key is a UUID column ``id``,
    alongside a string ``body`` column:

    >>> class MessagesSchema(pw.Schema):
    ...     id: str = pw.column_definition(primary_key=True)
    ...     body: str

    The Pathway Live Data Framework will read the UUID values as standard hyphenated strings, for example
    ``"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"``. Both modes are supported. For a
    one-time snapshot:

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="messages",
    ...     schema=MessagesSchema,
    ...     mode="static",
    ... )

    For continuous CDC, create a publication first:

    .. code-block:: sql

        CREATE PUBLICATION messages_pub FOR TABLE messages;

    Then configure the streaming connector:

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="messages",
    ...     schema=MessagesSchema,
    ...     mode="streaming",
    ...     publication_name="messages_pub",
    ... )

    Tables with composite primary keys — where the primary key spans multiple columns
    — are supported as well. To declare a composite primary key in the Pathway Live Data Framework, mark every
    participating column with ``pw.column_definition(primary_key=True)``. Suppose you
    have an ``order_items`` table where each row is uniquely identified by the
    combination of ``order_id`` and ``product_id``, both integers, alongside a
    ``quantity`` column:

    >>> class OrderItemsSchema(pw.Schema):
    ...     order_id: int = pw.column_definition(primary_key=True)
    ...     product_id: int = pw.column_definition(primary_key=True)
    ...     quantity: int

    Both ``order_id`` and ``product_id`` are marked as primary key columns, matching
    the ``PRIMARY KEY (order_id, product_id)`` constraint on the PostgreSQL side. In
    streaming mode, this is especially important: when an update or delete event arrives
    in the WAL, PostgreSQL only exposes the primary key columns of the affected row, so
    all primary key columns must be declared as such in the schema.

    Both modes are supported. For a one-time snapshot:

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="order_items",
    ...     schema=OrderItemsSchema,
    ...     mode="static",
    ... )

    For continuous CDC, create a publication first:

    .. code-block:: sql

        CREATE PUBLICATION order_items_pub FOR TABLE order_items;

    Then configure the streaming connector:

    >>> table = pw.io.postgres.read(
    ...     postgres_settings=connection_string_parts,
    ...     table_name="order_items",
    ...     schema=OrderItemsSchema,
    ...     mode="streaming",
    ...     publication_name="order_items_pub",
    ... )
    """
    _check_entitlements("postgres-wal-reader")

    postgres_settings = _augment_postgres_settings(postgres_settings, name)
    owned_postgres_settings = copy.copy(postgres_settings)
    tls = _build_tls_settings(owned_postgres_settings)
    replication_settings = _construct_replication_settings(
        mode=mode,
        postgres_settings=postgres_settings,  # use original settings for libpq
        publication_name=publication_name,
        # Note: an externally-provided replication slot can be added here
        replication_slot_name=None,
        snapshot_name=None,
    )
    data_storage = api.DataStorage(
        storage_type="postgres",
        connection_string=_connection_string_from_settings(owned_postgres_settings),
        psql_replication=replication_settings,
        table_name=table_name,
        schema_name=schema_name,
        tls_settings=tls.settings,
    )

    if schema is None:
        try:
            from pathway.engine import postgres_explore_schema

            ssl_mode = owned_postgres_settings.get("sslmode", "prefer")
            ssl_cert_path = owned_postgres_settings.get("sslrootcert", None)

            columns_data, pk_columns = postgres_explore_schema(
                _connection_string_from_settings(owned_postgres_settings),
                schema_name,
                table_name,
                ssl_mode,
                ssl_cert_path,
            )

            schema_columns = {}
            for col_name, udt_name, is_nullable in columns_data:
                # Map to pw types
                mapping = {
                    "int2": int,
                    "int4": int,
                    "int8": int,
                    "float4": float,
                    "float8": float,
                    "numeric": float,
                    "bool": bool,
                    "text": str,
                    "varchar": str,
                    "bpchar": str,
                    "char": str,
                    "uuid": str,
                    "json": str,
                    "jsonb": str,
                }
                py_type = mapping.get(udt_name, Any)
                if is_nullable and py_type is not Any:
                    py_type = Optional[py_type]

                is_pk = col_name in pk_columns
                from pathway.internals.schema import column_definition

                schema_columns[col_name] = column_definition(
                    dtype=py_type,
                    primary_key=is_pk,
                )

            if not pk_columns:
                logging.getLogger(__name__).warning(
                    f"No primary key found for {schema_name}.{table_name} during schema exploration. "
                    "Falling back to auto-generated row identifiers. "
                    "This may cause issues in streaming mode if the table is not append-only."
                )

            schema_name_class = (
                "".join(c.capitalize() for c in table_name.split("_")) + "Schema"
            )
            schema = schema_builder(schema_columns, name=schema_name_class)
            logging.getLogger(__name__).info(
                f"Derived schema for {schema_name}.{table_name}:\n{schema}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to explore schema automatically: {e}. Please provide an explicit schema."
            ) from e

    schema, api_schema = read_schema(schema)
    data_format = api.DataFormat(
        format_type="transparent",
        session_type=api.SessionType.UPSERT,
        **api_schema,
    )

    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=name,
        max_backlog_size=max_backlog_size,
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            schema=schema,
            data_source_options=data_source_options,
            datasource_name="postgres",
            append_only=(mode == "static") or is_append_only,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    postgres_settings: dict,
    table_name: str,
    *,
    schema_name: str | None = "public",
    max_batch_size: int | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    output_table_type: Literal["stream_of_changes", "snapshot"] = "stream_of_changes",
    primary_key: list[ColumnReference] | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
    _external_diff_column: ColumnReference | None = None,
) -> None:
    """Writes ``table`` to a Postgres table. Two types of output tables are supported:
    **stream of changes** and **snapshot**.

    When using **stream of changes**, the output table contains a log of all changes that
    occurred in the Pathway Live Data Framework table. In this case, it is expected to have two additional columns,
    ``time`` and ``diff``, both of integer type. ``time`` indicates the transactional
    minibatch time in which the row change occurred. ``diff`` can be either ``1`` for
    row insertion or ``-1`` for row deletion.

    When using **snapshot**, the set of columns in the output table matches the set of
    columns in the table you are writing. No additional columns are created.

    Args:
        table: Table to be written.
        postgres_settings: Components for the connection string for Postgres. The string is
            formed by joining key-value pairs from the given dictionary with spaces,
            with each pair formatted as `key=value`. Keys must be strings. Values can be
            of any type; if a value is not a string, it will be converted using Python's
            `str()` function.
            The Pathway Live Data Framework injects conservative TCP-keepalive defaults (``keepalives``,
            ``keepalives_idle=300``, ``keepalives_interval=30``, ``keepalives_count=3``,
            and ``tcp_user_timeout=300000``) so that an unreachable
            the Pathway Live Data Framework process is detected by PostgreSQL within minutes rather than
            the OS-inherited ~2-hour default; any of these can be overridden by
            passing the same key in ``postgres_settings``.
        table_name: Name of the target table. Any PostgreSQL identifier is
            accepted — the connector quotes the name before interpolating it
            into generated SQL, so hyphens, mixed case, and reserved words
            round-trip as-is. Column names in ``table`` and in ``primary_key``
            are quoted the same way.
        schema_name: Name of the PostgreSQL schema that owns the target table.
            Defaults to ``"public"``. Set this when writing to a non-default
            schema; the name is quoted identically to ``table_name``.
        max_batch_size: Maximum number of entries allowed to be committed within a
            single transaction.
        init_mode: "default": The default initialization mode;
            "create_if_not_exists": initializes the SQL writer by creating the necessary table
            if they do not already exist;
            "replace": Initializes the SQL writer by replacing any existing table.
        output_table_type: Defines how the output table manages its data. If set to ``"stream_of_changes"``
            (the default), the system outputs a stream of modifications to the target table.
            This stream includes two additional integer columns: ``time``, representing the computation
            minibatch, and ``diff``, indicating the type of change (``1`` for row addition and
            ``-1`` for row deletion). If set to ``"snapshot"``, the table maintains the current
            state of the data, updated atomically with each minibatch and ensuring that no partial
            minibatch updates are visible.
        primary_key: When using snapshot mode, one or more columns that form the primary
            key in the target Postgres table.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. It is also surfaced to PostgreSQL as part of
            the connection's ``application_name`` (``pathway:<name>``), so operators can
            filter ``pg_stat_activity`` and server logs by connector.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider there's a need to output a stream of updates from a table in the Pathway Live Data Framework to
    a table in Postgres. Let's see how this can be done with the connector.

    First of all, one needs to provide the required credentials for Postgres
    `connection string <https://www.postgresql.org/docs/current/libpq-connect.html>`_.
    While the connection string can include a wide variety of settings, such as SSL
    or connection timeouts, in this example we will keep it simple and provide the
    smallest example possible. Suppose that the database is running locally on the standard
    port 5432, that it has the name ``database`` and is accessible under the username
    ``user`` with a password ``pass``.

    It gives us the following content for the connection string:

    >>> connection_string_parts = {
    ...     "host": "localhost",
    ...     "port": "5432",
    ...     "dbname": "database",
    ...     "user": "user",
    ...     "password": "pass",
    ... }

    Now let's load a table, which we will output to the database:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown("age owner pet \\n 1 10 Alice 1 \\n 2 9 Bob 1 \\n 3 8 Alice 2")

    In order to output the table, we will need to create a new table in the database. The table
    would need to have all the columns that the output data has. Moreover it will need
    a ``time`` column of type ``BIGINT`` (Pathway Live Data Framework timestamps are milliseconds since epoch and
    routinely exceed the 32-bit range) and a ``diff`` column of type ``SMALLINT``. Finally,
    it is also a good idea to create the sequential primary key for our changes so that we
    know the updates' order.

    To sum things up, the table creation boils down to the following SQL command:

    .. code-block:: sql

        CREATE TABLE pets (
            id SERIAL PRIMARY KEY,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL,
            age BIGINT,
            owner TEXT,
            pet TEXT
        );

    Now, having done all the preparation, one can simply call:

    >>> pw.io.postgres.write(
    ...     t,
    ...     connection_string_parts,
    ...     "pets",
    ... )

    Consider another scenario: the ``pets`` table is updated and you need to keep only
    the latest record for each pet, identified by the ``pet`` field in this table.
    In this case, you need the output table type to be ``"snapshot"``. The table can be
    created automatically in the database if you set ``init_mode`` to ``"replace"`` or
    ``"create_if_not_exists"``. If you create it manually, the command can look like this:

    .. code-block:: sql

        CREATE TABLE pets (
            pet TEXT PRIMARY KEY,
            age INTEGER,
            owner TEXT
        );

    The primary key in the target table is the ``pet`` field. Therefore, the ``primary_key``
    parameter for the command should be ``[t.pet]``. You can write this table as follows:

    >>> pw.io.postgres.write(
    ...     t,
    ...     connection_string_parts,
    ...     "pets",
    ...     output_table_type="snapshot",
    ...     primary_key=[t.pet],
    ... )

    **Indexing vectors with pgvector.** Columns holding one-dimensional float
    arrays (``np.ndarray``) are written natively into `pgvector
    <https://github.com/pgvector/pgvector>`_ columns: declare the destination
    column as ``vector(n)`` (single precision) or ``halfvec(n)`` (half
    precision), sized to the embedding dimension ``n``, and the array is
    serialized straight into it over the binary ``COPY`` protocol. The ``vector``
    extension must be enabled (``CREATE EXTENSION vector``) and the column must
    already have the pgvector type — ``init_mode`` auto-creation maps an
    ``np.ndarray`` to a plain PostgreSQL ``ARRAY``, not to a pgvector type, so
    create the column yourself.

    Suppose you embed incoming documents and store the embeddings in a
    pgvector-backed table to serve similarity search. The embeddings are
    produced as:

    >>> import numpy as np
    >>> documents = pw.debug.table_from_markdown(
    ...     '''
    ...     doc_id
    ...     1
    ...     2
    ...     3
    ...     '''
    ... )
    >>> @pw.udf
    ... def embed(doc_id: int) -> np.ndarray:
    ...     return np.ones(3) * doc_id
    >>> embeddings = documents.select(pw.this.doc_id, vector=embed(pw.this.doc_id))

    When the collection is **append-only** — embeddings are only ever added —
    use ``"stream_of_changes"`` so every new vector is appended to the index.
    The destination table carries the embedding column plus the ``time`` /
    ``diff`` bookkeeping columns:

    .. code-block:: sql

        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE TABLE document_embeddings (
            doc_id BIGINT,
            vector VECTOR(3),
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );

    >>> pw.io.postgres.write(
    ...     embeddings,
    ...     connection_string_parts,
    ...     "document_embeddings",
    ... )

    When documents come and go — a document leaving the collection must drop its
    vector from the index, and a re-embedded document must replace it — use
    ``"snapshot"`` keyed by the document id, so the table always mirrors the
    current set of embeddings (no ``time`` / ``diff`` columns are added):

    .. code-block:: sql

        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE TABLE document_index (
            doc_id BIGINT PRIMARY KEY,
            vector VECTOR(3)
        );

    >>> pw.io.postgres.write(
    ...     embeddings,
    ...     connection_string_parts,
    ...     "document_index",
    ...     output_table_type="snapshot",
    ...     primary_key=[embeddings.doc_id],
    ... )
    """

    postgres_settings = _augment_postgres_settings(postgres_settings, name)
    tls = _build_tls_settings(postgres_settings)
    is_snapshot_mode = output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE

    # Stream-of-changes mode appends `time BIGINT NOT NULL, diff SMALLINT
    # NOT NULL` metadata columns to the generated CREATE TABLE. A user
    # column with one of those names (case-insensitive — PostgreSQL
    # folds unquoted identifiers to lowercase) would otherwise land
    # twice in the DDL and the engine worker would panic at pipeline
    # start with an opaque `db error`. Snapshot mode does not append
    # these columns, so the check only applies in stream mode.
    if not is_snapshot_mode:
        offending = sorted(
            {
                name
                for name in table.schema.column_names()
                if name.lower() in ("time", "diff")
            }
        )
        if offending:
            raise ValueError(
                f"Pathway schema column(s) {offending} collide with "
                "the 'time' and 'diff' metadata columns appended in "
                "stream_of_changes mode. Rename the column(s) in your "
                "schema or switch to output_table_type='snapshot' "
                "which does not append these metadata columns."
            )

    data_storage = api.DataStorage(
        storage_type="postgres",
        connection_string=_connection_string_from_settings(postgres_settings),
        max_batch_size=max_batch_size,
        table_name=table_name,
        schema_name=schema_name,
        table_writer_init_mode=init_mode_from_str(init_mode),
        snapshot_maintenance_on_output=is_snapshot_mode,
        tls_settings=tls.settings,
    )

    if not is_snapshot_mode:
        if _external_diff_column is not None:
            raise ValueError(
                "_external_diff_column is only supported for the snapshot table type"
            )
        if primary_key is not None:
            raise ValueError(
                "primary_key can only be specified for the snapshot table type"
            )
    else:
        # Snapshot mode requires at least one primary-key column —
        # the writer's INSERT ... ON CONFLICT (...) DO UPDATE
        # statement is malformed without one. If we let an empty list
        # reach the engine it would error out only AFTER ``init_mode``
        # has already mutated the destination (CREATE TABLE for
        # ``"replace"`` / ``"create_if_not_exists"``), and under
        # multi-worker (PATHWAY_THREADS > 1) the worker that loses the
        # CREATE race observes the partially-created table and
        # surfaces a less specific error instead — making any
        # message-based test flaky. Reject at call time so no DB side
        # effect happens.
        if primary_key is None or len(primary_key) == 0:
            raise ValueError(
                "primary key field names must be specified for a snapshot mode"
            )
    if (
        _external_diff_column is not None
        and _external_diff_column._column.dtype != dtype.INT
    ):
        raise ValueError("_external_diff_column can only have an integer type")

    external_diff_column_index = get_column_index(table, _external_diff_column)
    key_field_names = None
    if primary_key is not None:
        key_field_names = [pkey_field.name for pkey_field in primary_key]
        # `primary_key=[other_table.col]` (or a reference whose name
        # simply isn't in `table`) is accepted today but then either
        # generates a malformed CREATE TABLE (``PRIMARY KEY
        # ("unknown")``) on init or produces an UPSERT that panics at
        # flush. Reject up-front with a clear message.
        table_columns = set(table.schema.column_names())
        foreign = sorted(n for n in key_field_names if n not in table_columns)
        if foreign:
            raise ValueError(
                f"primary_key references column(s) {foreign} that are "
                "not present in the written table; pass ColumnReferences "
                "from the table being written, e.g. primary_key=[table.k]."
            )
        # A duplicate reference like `primary_key=[t.k, t.k]` would slip
        # through to `SqlQueryTemplate` and either yield malformed SQL
        # (``PRIMARY KEY ("k", "k")``) on CREATE TABLE or corrupt the
        # ``primary_key_fields`` index-swap used on DELETE. Reject it
        # here with a clear message instead.
        duplicates = sorted(
            {name for name in key_field_names if key_field_names.count(name) > 1}
        )
        if duplicates:
            raise ValueError(f"primary_key contains duplicate column(s) {duplicates}")
        # A nullable primary-key column in snapshot mode is silently
        # broken: either the `NOT NULL` PRIMARY KEY we emit on
        # create_if_not_exists / replace rejects the NULL row at
        # insert time, or (against a pre-existing table that allows
        # NULL in the PK) the retraction ``DELETE ... WHERE pkey=$1``
        # never matches anything because SQL ``= NULL`` is always
        # false. Both are data-loss footguns, so we refuse the setup
        # here with an actionable message.
        if is_snapshot_mode:
            for pkey_field in primary_key:
                if isinstance(pkey_field._column.dtype, dtype.Optional):
                    raise ValueError(
                        f"primary_key column '{pkey_field.name}' is "
                        "declared nullable; primary_key columns must be "
                        "non-nullable in snapshot mode. Either remove "
                        "the Optional wrapper in the schema or filter "
                        "out nulls upstream via "
                        ".filter(t.pkey.is_not_none())."
                    )
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=key_field_names,
        value_fields=_format_output_value_fields(table),
        table_name=table_name,
        external_diff_column_index=external_diff_column_index,
    )

    datasink_type = "snapshot" if is_snapshot_mode else "sink"
    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name=f"postgres.{datasink_type}",
            unique_name=name,
            sort_by=sort_by,
        )
    )


@check_arg_types
def write_snapshot(
    table: Table,
    postgres_settings: dict,
    table_name: str,
    primary_key: list[str],
    *,
    max_batch_size: int | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
    _external_diff_column: ColumnReference | None = None,
) -> None:
    """**WARNING**: This method is deprecated. Please use ``pw.io.postgres.write`` with
    the parameter ``output_table_type="snapshot"`` instead. Note that the new version
    does not create the ``time`` and ``diff`` columns and maintains a current snapshot
    of the table you are writing.

    Maintains a snapshot of a table within a Postgres table.

    In order for write to be successful, it is required that the table contains ``time``
    and ``diff`` columns of the integer type.

    Args:
        postgres_settings: Components of the connection string for Postgres.
        table_name: Name of the target table.
        primary_key: Names of the fields which serve as a primary key in the Postgres table.
        max_batch_size: Maximum number of entries allowed to be committed within a
            single transaction.
        init_mode: "default": The default initialization mode;
            "create_if_not_exists": initializes the SQL writer by creating the necessary table
            if they do not already exist;
            "replace": Initializes the SQL writer by replacing any existing table.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider there is a table ``stats`` in the Pathway Live Data Framework, containing the average
    number of requests to some
    service or operation per user, over some period of time. The number of requests
    can be large, so we decide not to store the whole stream of changes, but to only store
    a snapshot of the data, which can be actualized by the Pathway Live Data Framework.

    The minimum set-up would require us to have a Postgres table with two columns: the ID
    of the user ``user_id`` and the number of requests across some period of time ``number_of_requests``.
    In order to maintain consistency, we also need two extra columns: ``time`` and ``diff``.

    The SQL for the creation of such table would look as follows:

    .. code-block:: sql

        CREATE TABLE user_stats (
            user_id TEXT PRIMARY KEY,
            number_of_requests BIGINT,
            time BIGINT NOT NULL,
            diff SMALLINT NOT NULL
        );


    After the table is created, all you need is just to set up the output connector:

    >>> import pathway as pw
    >>> pw.io.postgres.write_snapshot(  # doctest: +SKIP
    ...    stats,
    ...    {
    ...        "host": "localhost",
    ...        "port": "5432",
    ...        "dbname": "database",
    ...        "user": "user",
    ...        "password": "pass",
    ...    },
    ...    "user_stats",
    ...    ["user_id"],
    ... )
    """

    warnings.warn(
        "`pw.io.postgres.write_snapshot` is deprecated and will be removed soon. "
        'Please use `pw.io.postgres.write` with output_table_type="snapshot" instead.',
        DeprecationWarning,
        stacklevel=5,
    )

    duplicates = sorted({name for name in primary_key if primary_key.count(name) > 1})
    if duplicates:
        raise ValueError(f"primary_key contains duplicate column(s) {duplicates}")

    # ``write_snapshot`` runs in ``legacy_mode=True`` which keeps the
    # ``time``/``diff`` metadata columns on the writer's INSERT — and
    # the generated SQL becomes ``INSERT INTO foo ("time", "value",
    # "time", "diff") VALUES (...)`` if the user schema also carries a
    # ``time`` or ``diff`` column, which PostgreSQL rejects with
    # ``column "time" specified more than once``. The non-legacy
    # sibling ``write`` runs the same check (and is already pinned by
    # ``test_psql_write_stream_mode_rejects_reserved_column_name``);
    # mirror it here so the deprecated path doesn't surface an opaque
    # engine-worker ``db error``.
    offending = sorted(
        {
            cname
            for cname in table.schema.column_names()
            if cname.lower() in ("time", "diff")
        }
    )
    if offending:
        raise ValueError(
            f"Pathway schema column(s) {offending} collide with "
            "the 'time' and 'diff' metadata columns appended by "
            "`pw.io.postgres.write_snapshot`. Rename the column(s) "
            "in your schema or migrate to `pw.io.postgres.write` "
            "with output_table_type='snapshot' (no metadata "
            "columns appended)."
        )

    postgres_settings = _augment_postgres_settings(postgres_settings, name)
    data_storage = api.DataStorage(
        storage_type="postgres",
        connection_string=_connection_string_from_settings(postgres_settings),
        max_batch_size=max_batch_size,
        snapshot_maintenance_on_output=True,
        table_name=table_name,
        schema_name="public",
        table_writer_init_mode=init_mode_from_str(init_mode),
        legacy_mode=True,
    )

    if (
        _external_diff_column is not None
        and _external_diff_column._column.dtype != dtype.INT
    ):
        raise ValueError("_external_diff_column can only have an integer type")
    external_diff_column_index = get_column_index(table, _external_diff_column)
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=primary_key,
        value_fields=_format_output_value_fields(table),
        table_name=table_name,
        external_diff_column_index=external_diff_column_index,
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="postgres.snapshot",
            unique_name=name,
            sort_by=sort_by,
        )
    )

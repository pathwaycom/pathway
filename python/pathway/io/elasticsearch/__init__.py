# Copyright © 2026 Pathway

from __future__ import annotations

import datetime
from typing import Any, Iterable, Literal

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Schema, Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    DurationLike,
    as_duration_seconds,
    internal_connector_mode,
    read_schema,
)


class ElasticSearchAuth:
    """
    Elasticsearch authentication object to be used in the ``write`` method.
    """

    def __init__(self, engine_es_auth: api.ElasticSearchAuth) -> None:
        self._engine_es_auth = engine_es_auth

    @classmethod
    def apikey(cls, apikey_id, apikey):
        """
        Constructs API key-based Elasticsearch authorization.

        Args:
            apikey_id: The ID of the API key.
            apikey: The API key.

        Returns:
            An authentication object to use for Elasticsearch authorization.
        """
        return cls(
            api.ElasticSearchAuth(
                "apikey",
                apikey_id=apikey_id,
                apikey=apikey,
            )
        )

    @classmethod
    def basic(cls, username, password):
        """
        Constructs basic Elasticsearch authorization using a username and password.

        Args:
            username: The username to use for authentication.
            password: The password for the specified user.

        Returns:
            An authentication object to use for Elasticsearch authorization.
        """
        return cls(
            api.ElasticSearchAuth(
                "basic",
                username=username,
                password=password,
            )
        )

    @classmethod
    def bearer(cls, bearer):
        """
        Constructs Elasticsearch authorization using the specified bearer token.

        Args:
            bearer: The bearer token.

        Returns:
            An authentication object to use for Elasticsearch authorization.
        """
        return cls(
            api.ElasticSearchAuth(
                "bearer",
                bearer=bearer,
            )
        )

    @property
    def engine_es_auth(self) -> api.ElasticSearchAuth:
        return self._engine_es_auth


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    host: str,
    auth: ElasticSearchAuth,
    index_name: str,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Write a table to a given index in ElasticSearch.

    The rows of the table are serialized into JSON. Type conversions are the same as in
    the `JSON output connector </developers/api-docs/pathway-io/jsonlines>`_.

    Note that two additional fields are included in the generated JSON: ``time``, which
    indicates the time of the Pathway Live Data Framework minibatch, and ``diff``, which can be either
    ``1`` (row addition) or ``-1`` (row deletion).

    Args:
        table: the table to output.
        host: the host and port, on which Elasticsearch server works.
        auth: credentials for Elasticsearch authorization.
        index_name: name of the index, which gets the docs.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider there is an instance of Elasticsearch, running locally on a port ``9200``.
    There we have an index ``"animals"``, containing an information about pets and their
    owners.

    For the sake of simplicity we will also consider that the cluster has a simple
    username-password authentication having both username and password equal to ``"admin"``.

    Now suppose we want to send a Pathway Live Data Framework table pets to this local instance of
    Elasticsearch.

    >>> import pathway as pw
    >>> pets = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | cat
    ... 8   | Alice | cat
    ... ''')

    It can be done as follows:

    >>> pw.io.elasticsearch.write(
    ...     table=pets,
    ...     host="http://localhost:9200",
    ...     auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
    ...     index_name="animals",
    ... )

    All the updates of table ``"pets"`` will be indexed to ``"animals"`` as well.
    """

    _check_entitlements("elasticsearch")
    data_storage = api.DataStorage(
        storage_type="elasticsearch",
        elasticsearch_params=api.ElasticSearchParams(
            host=host,
            index_name=index_name,
            auth=auth.engine_es_auth,
        ),
    )

    data_format = api.DataFormat(
        format_type="jsonlines",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="elasticsearch",
            unique_name=name,
            sort_by=sort_by,
        )
    )


@check_arg_types
@trace_user_frame
def read(
    host: str,
    auth: ElasticSearchAuth,
    index_name: str,
    schema: type[Schema],
    *,
    timestamp_column: str,
    id_column: str,
    max_transaction_duration: DurationLike,
    mode: Literal["static", "streaming"] = "streaming",
    poll_interval: DurationLike = datetime.timedelta(seconds=1),
    read_batch_size: int = 10_000,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data: Any = None,
) -> Table:
    """Reads an index from Elasticsearch into a Pathway Live Data Framework table.

    Elasticsearch exposes no change-data-capture (CDC) API, so this connector ingests
    incrementally by *polling*: it repeatedly queries the index for rows at or after a
    watermark and reconciles the overlap between consecutive queries so that no row is
    missed and no row is delivered twice. The reconciliation is driven by two columns
    that the index must contain:

    * ``timestamp_column`` — a numeric field (e.g. epoch milliseconds) recording when a
      row was indexed. The connector orders and watermarks rows by this value, so it
      must be a sortable integer field, not a date string.
    * ``id_column`` — a unique, sortable identifier (a ``keyword`` or numeric field). It
      is used both to deduplicate rows seen in the overlap window and as the Pathway
      Live Data Framework row key.

    This is an **append-only** mechanism: it ingests new documents, but does not observe
    in-place updates or deletes (a document re-indexed under the same ``id_column`` is
    treated as already-seen and is not re-delivered). It also assumes ``timestamp_column``
    is trustworthy and non-decreasing — a single document timestamped far in the future
    pushes the watermark past all normal documents and silently drops everything indexed
    afterwards. See the connector documentation for the full list of cases this mechanism
    fits, the cases it does not, and how to tune the parameters below.

    The third polling parameter, ``max_transaction_duration``, is the largest amount of
    time within which a concurrent writer may still commit a row whose
    ``timestamp_column`` is older than the current watermark (i.e. the maximum clock
    skew / transaction duration to tolerate). The connector keeps re-reading rows newer
    than ``now - max_transaction_duration`` until they settle, guaranteeing that a
    slow-to-commit row is never skipped. Set it conservatively: too small a value can
    miss rows committed late, while too large a value only widens the re-read window and
    costs a little extra work.

    When persistence is enabled, the connector saves the watermark together with the set
    of ids still inside the overlap window as its offset. On restart it resumes from that
    state and delivers only the rows that arrived since the last checkpoint, deduplicating
    anything that was already emitted. The ``name`` parameter is required when using
    persistence so the engine can match the connector to its saved state.

    Args:
        host: The host and port on which the Elasticsearch server works, e.g.
            ``"http://localhost:9200"``.
        auth: Credentials for Elasticsearch authorization.
        index_name: The name of the index to read from.
        schema: Schema of the resulting table. Column names must match the field names in
            the Elasticsearch documents. Both ``timestamp_column`` and ``id_column`` must
            be present. Do not declare a primary key in the schema — the connector keys
            the table by ``id_column``.
        timestamp_column: Name of the numeric column recording when each row was written
            or last updated. Used to order and watermark the polled rows.
        id_column: Name of the unique, sortable identifier column. Used to deduplicate the
            overlap window and as the Pathway Live Data Framework row key.
        max_transaction_duration: Maximum time within which a concurrent writer may still
            commit a row with a timestamp older than the current watermark. Given as a
            number of seconds or a ``datetime.timedelta`` / ``pw.Duration``.
        mode: If set to ``"streaming"`` (the default), the connector keeps polling for new
            rows. If set to ``"static"``, it reads the index once and terminates.
        poll_interval: How long to wait between two consecutive polls in streaming mode.
            Given as a number of seconds or a ``datetime.timedelta`` / ``pw.Duration``.
        read_batch_size: Maximum number of documents fetched per query. The connector
            pages through the index in blocks of this size — each block becomes one
            minibatch — instead of pulling the whole index at once, which bounds memory
            on a cold start over a large index. If a single ``timestamp_column`` value
            holds more rows than this, the connector transparently enlarges the query
            until that timestamp is fully read, so the value is a throughput/latency
            knob, not a correctness one; it must be positive.
        autocommit_duration_ms: The maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the connector
            are committed and pushed into Pathway Live Data Framework's computation graph.
        name: A unique name for the connector. If provided, this name will be used in logs
            and monitoring dashboards, and as the name for the persisted snapshot.
        max_backlog_size: Limit on the number of entries read from the input source and
            kept in processing at any moment. Reading pauses when the limit is reached and
            resumes as processing of some entries completes.
        debug_data: Static data replacing the original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Consider an Elasticsearch instance running locally on port ``9200`` with an index
    ``"logs"`` whose documents carry an integer ``ts`` field (epoch milliseconds) and a
    unique ``doc_id`` field:

    >>> import datetime
    >>> import pathway as pw
    >>> class LogSchema(pw.Schema):
    ...     doc_id: str
    ...     ts: int
    ...     message: str
    >>> table = pw.io.elasticsearch.read(
    ...     host="http://localhost:9200",
    ...     auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
    ...     index_name="logs",
    ...     schema=LogSchema,
    ...     timestamp_column="ts",
    ...     id_column="doc_id",
    ...     max_transaction_duration=datetime.timedelta(minutes=5),
    ... )
    """

    _check_entitlements("elasticsearch")

    column_names = schema.column_names()
    if timestamp_column not in column_names:
        raise ValueError(
            f"timestamp_column {timestamp_column!r} is not present in the schema"
        )
    if id_column not in column_names:
        raise ValueError(f"id_column {id_column!r} is not present in the schema")
    if schema.primary_key_columns():
        raise ValueError(
            "Defining a primary key in the schema is not supported for "
            "pw.io.elasticsearch.read. The connector keys the resulting table by "
            "id_column. If you need to reindex the table by a different column, use "
            "pw.Table.with_id_from() after reading."
        )

    max_transaction_duration_ms = round(
        as_duration_seconds(max_transaction_duration, "max_transaction_duration") * 1000
    )
    poll_interval_ms = round(as_duration_seconds(poll_interval, "poll_interval") * 1000)
    if read_batch_size <= 0:
        raise ValueError("read_batch_size must be positive")

    data_storage = api.DataStorage(
        storage_type="elasticsearch",
        mode=internal_connector_mode(mode),
        elasticsearch_params=api.ElasticSearchParams(
            host=host,
            index_name=index_name,
            auth=auth.engine_es_auth,
        ),
        elasticsearch_reader_params=api.ElasticSearchReaderParams(
            timestamp_field=timestamp_column,
            id_field=id_column,
            max_transaction_duration_ms=max_transaction_duration_ms,
            read_batch_size=read_batch_size,
            poll_interval_ms=poll_interval_ms,
        ),
    )

    schema, api_schema = read_schema(schema)
    # The connector keys the table by id_column regardless of the schema's primary
    # key (which is disallowed above), so the same document is upserted rather than
    # duplicated if it is ever re-read.
    api_schema["key_field_names"] = [id_column]
    data_format = api.DataFormat(
        format_type="jsonlines",
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
            datasource_name="elasticsearch",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


# This is made to force ElasticSearchAuth documentation
__all__ = [
    "ElasticSearchAuth",
    "read",
    "write",
]

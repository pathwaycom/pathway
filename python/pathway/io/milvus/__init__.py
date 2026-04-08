# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Iterable

import numpy as np

from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.json import Json as _PwJson
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.table_subscription import subscribe
from pathway.internals.trace import trace_user_frame
from pathway.optional_import import optional_imports

# Python types accepted by pymilvus for the Milvus field types that Pathway supports.
# bool must appear before int because bool is a subclass of int.
# tuple is included because Pathway represents list[float] columns as tuples internally.
_SUPPORTED_TYPES = (bool, int, float, str, dict, list, tuple, bytes, np.ndarray)


def _prepare_row(row: dict) -> dict:
    """Convert Pathway-internal types to plain Python values for pymilvus.

    Unwraps ``pw.Json`` wrapper objects, converts 1-D ``numpy.ndarray`` values
    to lists, and validates that every value belongs to a type the Milvus
    connector supports.  Raises ``TypeError`` with a descriptive message for
    unsupported types, and ``ValueError`` for multi-dimensional arrays.
    """
    result = {}
    for k, v in row.items():
        if isinstance(v, _PwJson):
            v = v.value
        if isinstance(v, np.ndarray):
            if v.ndim != 1:
                raise ValueError(
                    f"Column {k!r} contains a {v.ndim}-dimensional numpy array. "
                    f"pw.io.milvus.write only supports 1-D arrays (for "
                    f"FLOAT_VECTOR / BINARY_VECTOR fields)."
                )
            v = v.tolist()
        elif not isinstance(v, _SUPPORTED_TYPES):
            raise TypeError(
                f"Column {k!r} contains a value of unsupported type "
                f"{type(v).__name__!r}. pw.io.milvus.write supports the "
                f"following Pathway types: int, float, str, bool, pw.Json, "
                f"list[float], bytes, and numpy.ndarray (1-D only)."
            )
        result[k] = v
    return result


def _make_client(MilvusClient, uri: str):
    """Create a MilvusClient, working around a pymilvus 2.6.x bug.

    When the URI is a local ``.db`` file, pymilvus starts a milvus-lite
    server and rewrites the URI to a Unix-domain-socket path
    (``unix:/path/to/sock``).  In 2.6.x the socket address is stored in
    ``ConnectionConfig.address`` but is never forwarded to ``GrpcHandler``,
    so ``GrpcHandler._address`` ends up as an empty string and the
    connection hangs.  Passing ``address=`` as an explicit keyword argument
    causes it to flow through ``handler_kwargs`` directly into
    ``GrpcHandler.__init__``, where ``kwargs.get("address")`` picks it up.
    """
    if uri.endswith(".db"):
        with optional_imports("milvus"):
            from milvus_lite.server_manager import server_manager_instance

            uds_uri = server_manager_instance.start_and_get_uri(uri)
            if uds_uri is None:
                raise RuntimeError(
                    f"milvus-lite failed to start a local server for: {uri}"
                )
            return MilvusClient(uds_uri)

    return MilvusClient(uri)


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    uri: str,
    collection_name: str,
    *,
    primary_key: ColumnReference,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """**This connector is available when using one of the following licenses only:**
    `Pathway Scale, Pathway Enterprise </pricing>`_.

    Writes a Pathway table to a Milvus collection.

    Each row addition (``diff = 1``) is sent to Milvus as an upsert and each row
    deletion (``diff = -1``) is sent as a delete. The value of the ``primary_key``
    column is used as the Milvus primary key. The column must belong to ``table``;
    passing a column from a different table raises a ``ValueError``.

    The target collection must already exist before the pipeline starts and its
    schema must be compatible with the table's columns. Pathway cannot create
    it automatically because the vector field dimension is not part of the
    Python type (a ``list`` of floats carries no size) and is only known once
    the first row arrives. Use ``pymilvus.MilvusClient.create_collection`` to
    create the collection upfront.

    Within every mini-batch, deletes are applied before upserts so that update
    pairs (retraction followed by insertion of the same key) are handled correctly.

    **Supported type mappings** (Pathway → Milvus):

    .. list-table::
       :header-rows: 1

       * - Pathway type
         - Milvus field type
         - Notes
       * - ``int``
         - ``INT64``
         -
       * - ``float``
         - ``DOUBLE``
         -
       * - ``str``
         - ``VARCHAR``
         - Field must declare ``max_length``
       * - ``bool``
         - ``BOOL``
         -
       * - ``pw.Json``
         - ``JSON``
         - Wrapper unwrapped automatically
       * - ``list`` of ``float``
         - ``FLOAT_VECTOR``
         - Dimension set in the collection schema
       * - ``bytes``
         - ``BINARY_VECTOR``
         - Dimension set in the collection schema
       * - ``numpy.ndarray``
         - ``FLOAT_VECTOR`` or ``BINARY_VECTOR``
         - Must be 1-D; converted to list

    Any other Pathway type raises a ``TypeError`` before reaching pymilvus,
    with a message that names the offending column and lists the supported types.
    A multi-dimensional ``numpy.ndarray`` raises a ``ValueError``.

    The ``uri`` parameter is passed directly to ``pymilvus.MilvusClient``.
    Use a local ``.db`` file path (e.g. ``"./milvus.db"``) to use
    `Milvus Lite <https://milvus.io/docs/milvus_lite.md>`_, an embedded
    single-file database that requires no server, which is convenient for
    development and testing. Use a server address (e.g.
    ``"http://localhost:19530"``) to connect to a running Milvus instance
    for production workloads.

    Args:
        table: The table to write.
        uri: URI passed to ``pymilvus.MilvusClient``. Use a local ``.db`` file
            path for Milvus Lite (e.g. ``"./milvus.db"``) or a server address
            for a running Milvus instance (e.g. ``"http://localhost:19530"``).
        collection_name: Name of the Milvus collection to write to.
        primary_key: A column reference (e.g. ``table.doc_id``) whose values are
            used as the Milvus primary key. The column must belong to ``table``.
        name: A unique name for the connector. If provided, this name will be
            used in logs and monitoring dashboards.
        sort_by: If specified, the output within each mini-batch will be sorted
            in ascending order by the given columns. When multiple columns are
            provided, the corresponding value tuples are compared
            lexicographically.

    Returns:
        None

    Example:

    Suppose you are building a document search pipeline and want to store
    embeddings in Milvus. The example below uses Milvus Lite — a local
    single-file database that requires no running server, which is convenient
    for development. For production, replace ``"./milvus.db"`` with your
    server URI, e.g. ``"http://localhost:19530"``.

    Create the collection before starting the pipeline. The schema
    must define an integer primary key and a ``FLOAT_VECTOR`` field whose
    dimension matches the embeddings your pipeline will produce:

    >>> import pathway as pw
    >>> from pymilvus import DataType, MilvusClient  # doctest: +SKIP
    ...
    >>> client = MilvusClient("./milvus.db")  # doctest: +SKIP
    >>> schema = client.create_schema(auto_id=False)  # doctest: +SKIP
    >>> schema.add_field("doc_id", DataType.INT64, is_primary=True)  # doctest: +SKIP
    >>> schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=4)  # doctest: +SKIP
    >>> index_params = client.prepare_index_params()  # doctest: +SKIP
    >>> index_params.add_index("embedding", metric_type="COSINE", index_type="FLAT")  # doctest: +SKIP
    >>> client.create_collection("docs", schema=schema, index_params=index_params)  # doctest: +SKIP
    >>> client.close()   # doctest: +SKIP

    Define your Pathway schema and build the table:

    >>> class DocSchema(pw.Schema):
    ...     doc_id: int = pw.column_definition(primary_key=True)
    ...     embedding: list[float]
    ...
    >>> table = pw.debug.table_from_rows(
    ...     DocSchema,
    ...     [(1, [0.1, 0.2, 0.3, 0.4]), (2, [0.5, 0.6, 0.7, 0.8])],
    ... )

    Attach the Milvus output connector and specify which column maps to the
    Milvus primary key field:

    >>> pw.io.milvus.write(   # doctest: +SKIP
    ...     table,
    ...     uri="./milvus.db",
    ...     collection_name="docs",
    ...     primary_key=table.doc_id,
    ... )
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)  # doctest: +SKIP
    """
    _check_entitlements("milvusdb")
    with optional_imports("milvus"):
        from pymilvus import MilvusClient

    if primary_key._table is not table:
        raise ValueError(
            f"primary_key column {primary_key._name!r} does not belong to the "
            f"provided table. Pass a column reference from the same table, "
            f"e.g. primary_key=table.{primary_key._name}."
        )

    client = _make_client(MilvusClient, uri)

    pk = primary_key._name
    # Accumulates (is_addition, row) in arrival order for the current batch.
    _buffer: list[tuple] = []

    def on_change(key, row, time, is_addition):
        _buffer.append((is_addition, _prepare_row(row)))

    def on_time_end(time):
        to_delete = []
        to_upsert = []

        for is_add, row in _buffer:
            if is_add:
                to_upsert.append(row)
            else:
                to_delete.append(row[pk])
        _buffer.clear()

        # Deletes before upserts: handles update pairs (delete old + insert new)
        # with the same primary key within one batch.
        if to_delete:
            client.delete(collection_name=collection_name, ids=to_delete)
        if to_upsert:
            client.upsert(collection_name=collection_name, data=to_upsert)

    def on_end():
        client.close()

    subscribe(
        table,
        skip_persisted_batch=True,
        on_change=on_change,
        on_time_end=on_time_end,
        on_end=on_end,
        name=name,
        sort_by=sort_by,
    )

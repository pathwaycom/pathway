# Copyright © 2026 Pathway

from __future__ import annotations

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import get_column_index


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    url: str,
    collection_name: str,
    *,
    vector: ColumnReference,
    api_key: str | None = None,
    batch_size: int = 256,
    name: str | None = None,
) -> None:
    """Writes a Pathway Live Data Framework table to a `Qdrant <https://qdrant.tech/>`_ collection.

    Each row addition (``diff = 1``) is sent to Qdrant as a point upsert and each
    row deletion (``diff = -1``) removes the corresponding point. The ``vector``
    column provides the point's vector and every other column of ``table`` is
    stored in the point's payload. The point id is assigned internally rather
    than taken from a column, so keep any identifier you need as an ordinary
    column (it is stored in the payload).

    Within every minibatch an insertion always wins over a deletion of the same
    key, so an update (a retraction of the old row followed by an insertion of the
    new one) replaces the point rather than removing it.

    If the target collection does not exist yet, it is created on the first write
    with a single ``Cosine``-distance vector whose size matches the written
    vectors. Create the collection yourself beforehand if you need a different
    distance metric or vector configuration.

    The ``vector`` column must hold a ``list[float]`` or a 1-D ``numpy.ndarray``.
    Payload columns may be of any type the Pathway Live Data Framework JSON serializer supports
    (``int``, ``float``, ``str``, ``bool``, ``pw.Json``, lists, tuples, ``bytes``,
    and 1-D ``numpy.ndarray``).

    Args:
        table: The table to write.
        url: URL of the Qdrant instance's gRPC endpoint, e.g.
            ``"http://localhost:6334"`` (Qdrant's gRPC port is ``6334`` by default).
        collection_name: Name of the Qdrant collection to write to.
        vector: A column reference (e.g. ``table.embedding``) holding the point
            vector as a ``list[float]`` or 1-D ``numpy.ndarray``. The column must
            belong to ``table``.
        api_key: Optional API key used to authenticate with Qdrant Cloud or a
            secured instance.
        batch_size: Maximum number of points sent to Qdrant in a single upsert
            or delete request. A minibatch larger than this is split into several
            requests, keeping each request bounded for high-dimensional vectors.
        name: A unique name for the connector. If provided, this name will be
            used in logs and monitoring dashboards.

    Returns:
        None

    Example:

    Suppose you are building a document search pipeline and want to store
    embeddings in Qdrant.

    >>> import pathway as pw
    >>> class DocSchema(pw.Schema):
    ...     doc_id: int = pw.column_definition(primary_key=True)
    ...     embedding: list[float]
    ...     title: str
    ...
    >>> table = pw.debug.table_from_rows(
    ...     DocSchema,
    ...     [(1, [0.1, 0.2, 0.3, 0.4], "a"), (2, [0.5, 0.6, 0.7, 0.8], "b")],
    ... )

    Attach the Qdrant output connector, pointing at the vector column. The
    remaining columns (here ``doc_id`` and ``title``) are stored in the point
    payload:

    >>> pw.io.qdrant.write(   # doctest: +SKIP
    ...     table,
    ...     url="http://localhost:6334",
    ...     collection_name="docs",
    ...     vector=table.embedding,
    ... )
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)  # doctest: +SKIP
    """
    _check_entitlements("qdrant")

    if batch_size < 1:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size}.")

    vector_field_index = get_column_index(table, vector)
    # ``vector`` is a required argument, so the column is always present.
    assert vector_field_index is not None

    data_storage = api.DataStorage(
        storage_type="qdrant",
        qdrant_params=api.QdrantParams(
            url=url,
            collection_name=collection_name,
            vector_field_index=vector_field_index,
            api_key=api_key,
            batch_size=batch_size,
        ),
    )
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="qdrant",
            unique_name=name,
        )
    )

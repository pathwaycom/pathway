# Copyright © 2026 Pathway

from __future__ import annotations

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    url: str,
    collection_name: str,
    *,
    api_key: str | None = None,
    batch_size: int = 256,
    name: str | None = None,
) -> None:
    """Writes a Pathway Live Data Framework table to a `Qdrant <https://qdrant.tech/>`_ collection.

    The collection schema is the single source of truth for what is written as
    a vector. At startup the connector introspects the collection and binds
    every declared named vector slot to the table column **with the same name**:

    - a *dense* slot is bound to a ``list[float]`` (or 1-D numeric
      ``numpy.ndarray``) column;
    - a *sparse* slot is bound to a ``list[tuple[int, float]]`` column holding
      ``(index, weight)`` pairs;
    - *multivector* slots (``list[list[float]]`` columns) are not supported yet
      and raise ``NotImplementedError``.

    Columns whose names match no vector slot are stored in the point payload.
    All vectors of a point are written atomically in the same upsert, which
    enables native hybrid (dense + sparse, e.g. BM25) search. Sparse weights
    are sent raw (e.g. term frequencies); when the sparse slot is configured
    with the IDF modifier, Qdrant applies IDF server-side.

    The collection is **never created automatically** — create it beforehand
    with the desired named vector configuration (dimensions, distance metrics,
    IDF modifier, etc.). The connector fails fast at startup if the collection
    does not exist, declares no vector slots, uses a single unnamed vector,
    declares a slot with no same-named column, or a slot's kind does not match
    the column's type. A dense vector whose dimension does not match the slot's
    configured dimension is rejected at write time with an error naming the
    slot and both dimensions.

    Each row addition (``diff = 1``) is sent to Qdrant as a point upsert and
    each row deletion (``diff = -1``) removes the corresponding point. The
    point id is assigned internally rather than taken from a column, so keep
    any identifier you need as an ordinary column (it is stored in the
    payload). Within every minibatch an insertion always wins over a deletion
    of the same key, so an update (a retraction of the old row followed by an
    insertion of the new one) replaces the point rather than removing it.

    Payload columns may be of any type the Pathway Live Data Framework JSON serializer supports
    (``int``, ``float``, ``str``, ``bool``, ``pw.Json``, lists, tuples, ``bytes``,
    and 1-D ``numpy.ndarray``).

    Args:
        table: The table to write.
        url: URL of the Qdrant instance's gRPC endpoint, e.g.
            ``"http://localhost:6334"`` (Qdrant's gRPC port is ``6334`` by default).
        collection_name: Name of the pre-created Qdrant collection to write to.
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

    Suppose you are building a hybrid document search pipeline: dense
    embeddings for semantic similarity plus sparse term weights for BM25-style
    scoring. First create the collection with a named dense slot and a named
    sparse slot (using the `qdrant-client
    <https://python-client.qdrant.tech/>`_ package):

    >>> from qdrant_client import QdrantClient, models  # doctest: +SKIP
    >>> client = QdrantClient(url="http://localhost:6333")  # doctest: +SKIP
    >>> client.create_collection(  # doctest: +SKIP
    ...     collection_name="docs",
    ...     vectors_config={
    ...         "embedding": models.VectorParams(
    ...             size=4, distance=models.Distance.COSINE
    ...         ),
    ...     },
    ...     sparse_vectors_config={
    ...         "bm25": models.SparseVectorParams(
    ...             modifier=models.Modifier.IDF
    ...         ),
    ...     },
    ... )

    Then write a table whose column names match the slot names. The
    ``embedding`` column feeds the dense slot, ``bm25`` feeds the sparse slot
    (raw term frequencies as ``(token_id, count)`` pairs — IDF is applied by
    Qdrant), and the remaining columns (``doc_id``, ``title``) are stored in
    the point payload:

    >>> import pathway as pw
    >>> class DocSchema(pw.Schema):
    ...     doc_id: int = pw.column_definition(primary_key=True)
    ...     embedding: list[float]
    ...     bm25: list[tuple[int, float]]
    ...     title: str
    ...
    >>> table = pw.debug.table_from_rows(
    ...     DocSchema,
    ...     [
    ...         (1, [0.1, 0.2, 0.3, 0.4], [(7, 2.0), (21, 1.0)], "a"),
    ...         (2, [0.5, 0.6, 0.7, 0.8], [(3, 1.0)], "b"),
    ...     ],
    ... )
    >>> pw.io.qdrant.write(   # doctest: +SKIP
    ...     table,
    ...     url="http://localhost:6334",
    ...     collection_name="docs",
    ... )
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)  # doctest: +SKIP

    If the collection declares only the dense ``embedding`` slot, drop the
    ``bm25`` column from the schema above — this is the common non-hybrid
    setup.
    """
    _check_entitlements("qdrant")

    if batch_size < 1:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size}.")

    data_storage = api.DataStorage(
        storage_type="qdrant",
        qdrant_params=api.QdrantParams(
            url=url,
            collection_name=collection_name,
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

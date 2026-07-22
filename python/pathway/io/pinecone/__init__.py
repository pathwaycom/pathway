# Copyright © 2026 Pathway

from __future__ import annotations

import os
from typing import Iterable

from pathway.internals import api, datasink, dtype as dt
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import get_column_index


def _is_statically_unknown(dtype: dt.DType) -> bool:
    """Whether ``dtype`` carries no usable static type information.

    A plain ``ANY`` column (e.g. the result of an unannotated UDF) could hold
    anything at runtime, so the up-front guards below skip it and defer to the
    engine's per-row type check instead of rejecting a possibly-valid pipeline.
    """
    return dtype == dt.ANY


def _is_numeric(dtype: dt.DType) -> bool:
    return dtype in (dt.INT, dt.FLOAT) or _is_statically_unknown(dtype)


def _check_primary_key_dtype(name: str, dtype: dt.DType) -> None:
    """Reject a ``primary_key`` whose type can never be a Pinecone record id.

    The id must always be present and must be an ``int`` or ``str`` (a pointer
    is accepted too, since the engine stringifies it). The matching runtime
    guard (``PineconeError::InvalidId``) only fires once an offending row reaches
    the sink, so catch the statically-known cases at ``write()`` time.
    """
    if _is_statically_unknown(dtype):
        return
    if isinstance(dtype, dt.Optional):
        raise ValueError(
            f"primary_key column {name!r} is nullable (type {dtype}); a Pinecone "
            "record id must always be present, so the column cannot be optional."
        )
    if dtype in (dt.INT, dt.STR) or isinstance(dtype, dt.Pointer):
        return
    raise ValueError(
        f"primary_key column {name!r} has unsupported type {dtype}; a Pinecone "
        "record id must be int or str."
    )


def _is_sparse_pair(dtype: dt.DType) -> bool:
    """Whether ``dtype`` is the ``tuple[int, float]`` of a sparse (index, weight) pair."""
    return (
        isinstance(dtype, dt.Tuple)
        and len(dtype.args) == 2
        and dtype.args[0] == dt.INT
        and dtype.args[1] == dt.FLOAT
    )


def _check_vector_dtype(name: str, dtype: dt.DType) -> None:
    """Reject a ``vector`` that is neither a dense nor a sparse vector column.

    A dense vector is a numeric list / array, a sparse one a
    ``list[tuple[int, float]]`` of ``(index, weight)`` pairs. Mirrors the runtime
    ``PineconeError::InvalidVector`` / ``InvalidSparseVector`` guards so a wrong
    column type fails at ``write()`` time rather than once data flows.
    """
    if _is_statically_unknown(dtype):
        return
    if isinstance(dtype, dt.Optional):
        raise ValueError(
            f"vector column {name!r} is nullable (type {dtype}); every row must "
            "carry a vector, so the column cannot be optional."
        )
    if isinstance(dtype, dt.List):
        inner = dtype.wrapped
        if _is_numeric(inner) or _is_sparse_pair(inner):
            return
        if isinstance(inner, (dt.List, dt.Array)):
            raise NotImplementedError(
                f"vector column {name!r} has type {dtype}, which is a multivector; "
                "a Pinecone record carries a single dense or sparse vector, so "
                "multivectors are not supported."
            )
    if isinstance(dtype, dt.Array) and _is_numeric(dtype.wrapped):
        return
    if isinstance(dtype, dt.Tuple) and all(_is_numeric(arg) for arg in dtype.args):
        return
    raise ValueError(
        f"vector column {name!r} has unsupported type {dtype}; a Pinecone vector "
        "must be a list[float] or a 1-D float array (dense), or a "
        "list[tuple[int, float]] of (index, weight) pairs (sparse)."
    )


def _check_metadata_dtype(name: str, dtype: dt.DType) -> None:
    """Reject a metadata column whose type Pinecone cannot store.

    Pinecone metadata supports ``int``, ``float``, ``bool``, ``str``, and
    ``list[str]``; ``None`` is allowed (it is dropped). Mirrors the runtime
    ``PineconeError::UnsupportedMetadataType`` guard.
    """
    inner = dtype.wrapped if isinstance(dtype, dt.Optional) else dtype
    if _is_statically_unknown(inner):
        return
    if inner in (dt.INT, dt.FLOAT, dt.BOOL, dt.STR):
        return
    if isinstance(inner, dt.List) and (
        inner.wrapped == dt.STR or _is_statically_unknown(inner.wrapped)
    ):
        return
    if isinstance(inner, dt.Tuple) and all(
        arg == dt.STR or _is_statically_unknown(arg) for arg in inner.args
    ):
        return
    raise ValueError(
        f"metadata column {name!r} has unsupported type {dtype}; Pinecone "
        "metadata supports int, float, bool, str, and list[str]."
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    index_name: str,
    *,
    primary_key: ColumnReference | None = None,
    vector: ColumnReference,
    api_key: str | None = None,
    host: str | None = None,
    namespace: str = "",
    metadata_columns: Iterable[ColumnReference] | None = None,
    batch_size: int = 100,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes a Pathway Live Data Framework table to a `Pinecone <https://www.pinecone.io/>`_ index.

    The Pinecone index is kept in sync with the current state of the table. When a
    row is added to the table it is upserted into the index, and when a row is
    removed from the table the corresponding record is deleted; an update replaces
    the previous record for that id. Only the current state is reflected — the
    connector does not append a change log, so no ``time`` or ``diff`` columns are
    written.

    The Pinecone record id is taken from the ``primary_key`` column when given. By
    default (``primary_key=None``) the table's internal row key is used as the id:
    it is unique by construction and matches the way the dataflow shards rows, so
    writing runs in parallel across workers. A ``primary_key`` column instead lets
    you choose a meaningful id, but its values must uniquely identify rows; to
    guarantee that, writing then runs on a single worker and a collision (two rows
    with the same id) raises an error rather than silently overwriting.

    The ``vector`` column provides the vector written to the index, and its type
    decides which kind of record is written:

    - a ``list[float]`` (or 1-D ``numpy.ndarray``) column is written as the
      record's dense ``values`` and requires a *dense* index;
    - a ``list[tuple[int, float]]`` column of ``(index, weight)`` pairs is written
      as the record's ``sparse_values`` and requires a *sparse* index. Weights are
      sent raw (e.g. term frequencies): a sparse index scores by the dot product
      of the stored weights, so IDF weighting, if wanted, is applied upstream;
    - a ``list[list[float]]`` (multivector) column raises ``NotImplementedError``,
      since a Pinecone record carries a single vector.

    A Pinecone index holds vectors of one kind only, so a hybrid (dense + sparse)
    setup is two indexes: create both, then attach two ``write()`` calls to the
    same table — one naming the dense column and one naming the sparse column.
    The same record ids then land in both indexes, so the two result sets can be
    fused client-side. See the example below.

    Every remaining column — or just the columns listed in ``metadata_columns`` —
    is stored as record metadata. ``None`` metadata values are dropped; a metadata
    value of a type Pinecone cannot store raises an error naming the offending
    column. See the connector documentation for the full type-conversion table.
    For high-dimensional vectors, ``batch_size`` may be reduced automatically so a
    single request stays within Pinecone's size limit.

    The target index must already exist before the pipeline starts; the connector
    never creates it. For a dense index its dimension must match the length of the
    produced vectors. If the index's kind does not match the ``vector`` column's
    type, the connector fails at start-up with an error naming the index, the
    column, and both kinds.

    Args:
        table: The table to write.
        index_name: Name of the Pinecone index to write to.
        primary_key: A column reference (e.g. ``table.doc_id``) whose values are
            used as the Pinecone record id. The column must belong to ``table`` and
            its values must uniquely identify rows (the id selects the record to
            upsert or delete); a collision raises an error at runtime and writing
            runs on a single worker. When ``None`` (the default), the table's
            internal row key is used as the id — always unique and written in
            parallel across workers — but the ids are then Pathway's internal
            keys rather than your own values.
        vector: A column reference holding the vector to write. Either a dense
            embedding (``list[float]`` or a 1-D ``numpy.ndarray``), written to a
            dense index, or a sparse vector (``list[tuple[int, float]]`` of
            ``(index, weight)`` pairs), written to a sparse index. The column must
            belong to ``table``.
        api_key: Pinecone API key. When ``None``, the ``PINECONE_API_KEY``
            environment variable is used.
        host: Control-plane host. Leave as ``None`` for Pinecone cloud
            (``https://api.pinecone.io``); set it to a local URL such as
            ``"http://localhost:5080"`` to target Pinecone Local.
        namespace: Pinecone namespace to write to. Defaults to the index's default
            namespace.
        metadata_columns: Column references for the columns to store as record
            metadata. When ``None``, every column except ``primary_key`` and
            ``vector`` is stored. All columns must belong to ``table``.
        batch_size: Maximum number of records sent to Pinecone per ``upsert`` call
            (capped at Pinecone's per-request limit of 1000).
        name: A unique name for the connector. If provided, this name will be used
            in logs and monitoring dashboards.
        sort_by: If specified, the output within each mini-batch will be sorted in
            ascending order by the given columns.

    Returns:
        None

    Example:

    Suppose you have a couple of documents, each already turned into a
    4-dimensional embedding, and you want them to be searchable in Pinecone — and
    to stay in sync as the documents change.

    The first thing to take care of is the index itself: the connector never
    creates it, so it has to exist before the pipeline starts, and its
    ``dimension`` must match the length of your vectors (``4`` here).

    >>> from pinecone import Pinecone, ServerlessSpec  # doctest: +SKIP
    >>> pc = Pinecone(api_key="YOUR_API_KEY")  # doctest: +SKIP
    >>> pc.create_index(  # doctest: +SKIP
    ...     name="docs",
    ...     dimension=4,
    ...     metric="cosine",
    ...     spec=ServerlessSpec(cloud="aws", region="us-east-1"),
    ... )

    With the index in place, you describe the data. Each document has an id that
    will become the Pinecone record id, the embedding itself, and a ``title`` you
    would like to keep around — here as a small static table, but in a real
    pipeline this would come from a connector:

    >>> import pathway as pw
    >>> class DocSchema(pw.Schema):
    ...     doc_id: str = pw.column_definition(primary_key=True)
    ...     embedding: list[float]
    ...     title: str
    ...
    >>> table = pw.debug.table_from_rows(
    ...     DocSchema,
    ...     [("a", [0.1, 0.2, 0.3, 0.4], "First"), ("b", [0.5, 0.6, 0.7, 0.8], "Second")],
    ... )

    Now you point the connector at the index, spelling out which column is the
    record id and which one holds the vector. Everything left over — in this case
    ``title`` — is stored as record metadata automatically, so there is nothing
    else to configure:

    >>> pw.io.pinecone.write(   # doctest: +SKIP
    ...     table,
    ...     index_name="docs",
    ...     primary_key=table.doc_id,
    ...     vector=table.embedding,
    ...     api_key="YOUR_API_KEY",
    ... )

    Notice that nothing has reached Pinecone yet: like every Pathway pipeline, the
    write is lazy. It is the final ``pw.run()`` that actually starts the dataflow,
    upserts the two records, and from then on keeps the index in sync with any
    later change to ``table``:

    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)  # doctest: +SKIP

    For hybrid retrieval you need two indexes, since a Pinecone index stores
    vectors of one kind: the dense one you already have, plus a sparse one
    (``vector_type="sparse"``, which is always scored by ``dotproduct`` and takes
    no dimension):

    >>> pc.create_index(  # doctest: +SKIP
    ...     name="docs-sparse",
    ...     metric="dotproduct",
    ...     vector_type="sparse",
    ...     spec=ServerlessSpec(cloud="aws", region="us-east-1"),
    ... )

    The table now carries both vectors — the dense embedding and, say, BM25 term
    weights as ``(token_id, weight)`` pairs:

    >>> class HybridSchema(pw.Schema):
    ...     doc_id: str = pw.column_definition(primary_key=True)
    ...     embedding: list[float]
    ...     bm25: list[tuple[int, float]]
    ...     title: str
    ...
    >>> table = pw.debug.table_from_rows(
    ...     HybridSchema,
    ...     [
    ...         ("a", [0.1, 0.2, 0.3, 0.4], [(7, 2.0), (21, 1.0)], "First"),
    ...         ("b", [0.5, 0.6, 0.7, 0.8], [(3, 1.0)], "Second"),
    ...     ],
    ... )

    Attach one ``write()`` per index off that single table, each naming the column
    it writes. Both calls use the same ``primary_key``, so a document lands in the
    two indexes under the same record id — that is what lets you fuse the dense
    and the sparse result lists at query time. ``metadata_columns`` is spelled out
    here because the default would try to store the *other* vector column as
    metadata, which Pinecone cannot hold. Additions, updates, and deletions are
    propagated to both indexes:

    >>> pw.io.pinecone.write(   # doctest: +SKIP
    ...     table,
    ...     index_name="docs",
    ...     primary_key=table.doc_id,
    ...     vector=table.embedding,
    ...     metadata_columns=[table.title],
    ...     api_key="YOUR_API_KEY",
    ... )
    >>> pw.io.pinecone.write(   # doctest: +SKIP
    ...     table,
    ...     index_name="docs-sparse",
    ...     primary_key=table.doc_id,
    ...     vector=table.bm25,
    ...     metadata_columns=[table.title],
    ...     api_key="YOUR_API_KEY",
    ... )
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)  # doctest: +SKIP
    """
    _check_entitlements("pinecone")

    if primary_key is not None and primary_key._table is not table:
        raise ValueError(
            f"primary_key column {primary_key._name!r} does not belong to the "
            f"provided table. Pass a column reference from the same table, "
            f"e.g. primary_key=table.{primary_key._name}."
        )
    if vector._table is not table:
        raise ValueError(
            f"vector column {vector._name!r} does not belong to the provided "
            f"table. Pass a column reference from the same table, "
            f"e.g. vector=table.{vector._name}."
        )

    pk_name = primary_key._name if primary_key is not None else None
    vector_name = vector._name

    if pk_name == vector_name:
        raise ValueError(
            f"primary_key and vector both reference column {pk_name!r}; they must "
            "be different columns."
        )

    if metadata_columns is None:
        metadata_names = [
            col_name
            for col_name in table.column_names()
            if col_name not in (pk_name, vector_name)
        ]
    else:
        metadata_names = []
        for col in metadata_columns:
            if col._table is not table:
                raise ValueError(
                    f"metadata column {col._name!r} does not belong to the "
                    f"provided table. Pass column references from the same table, "
                    f"e.g. table.{col._name}."
                )
            if col._name in (pk_name, vector_name):
                role = "primary_key" if col._name == pk_name else "vector"
                raise ValueError(
                    f"column {col._name!r} is used as the {role} and cannot also "
                    "be a metadata column. Remove it from metadata_columns."
                )
            metadata_names.append(col._name)

    if batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size}.")

    if pk_name is not None:
        _check_primary_key_dtype(pk_name, table._get_column(pk_name).dtype)
    _check_vector_dtype(vector_name, table._get_column(vector_name).dtype)
    for col_name in metadata_names:
        _check_metadata_dtype(col_name, table._get_column(col_name).dtype)

    resolved_api_key = (
        api_key if api_key is not None else os.environ.get("PINECONE_API_KEY")
    )
    if not resolved_api_key:
        raise ValueError(
            "A Pinecone API key is required. Pass api_key=... or set the "
            "PINECONE_API_KEY environment variable."
        )

    column_index = {name_: index for index, name_ in enumerate(table.column_names())}
    vector_index = column_index[vector_name]
    metadata_indices = [column_index[name_] for name_ in metadata_names]

    data_storage = api.DataStorage(
        storage_type="pinecone",
        key_field_index=get_column_index(table, primary_key),
        max_batch_size=batch_size,
        pinecone_params=api.PineconeParams(
            api_key=resolved_api_key,
            index_name=index_name,
            vector_index=vector_index,
            metadata_indices=metadata_indices,
            namespace=namespace,
            control_host=host,
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
            datasink_name="pinecone",
            unique_name=name,
            sort_by=sort_by,
        )
    )


__all__ = ["write"]

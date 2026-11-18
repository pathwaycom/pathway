# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Iterable

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    collection_name: str,
    *,
    primary_key: ColumnReference | None = None,
    vector: ColumnReference | None = None,
    http_host: str = "localhost",
    http_port: int = 8080,
    http_secure: bool = False,
    api_key: str | None = None,
    headers: dict[str, str] | None = None,
    batch_size: int = 100,
    concurrency: int = 8,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes a Pathway Live Data Framework table to a `Weaviate <https://weaviate.io/>`_
    collection.

    Each row addition (``diff = 1``) upserts an object and each row deletion
    (``diff = -1``) removes it, keeping the collection in sync with the table. The
    target collection must already exist. See the connector documentation for how
    objects are identified, how each Pathway type is stored, and how to authenticate.

    Args:
        table: The table to write.
        collection_name: Name of the Weaviate collection to write to. It must
            already exist.
        primary_key: An optional column reference (e.g. ``table.doc_id``) whose
            values are used to derive each object's UUID; the column is not stored
            as a property. When omitted, the UUID is derived from the row's internal
            Pathway key. The column must belong to ``table``.
        vector: An optional column reference (e.g. ``table.embedding``) holding the
            vector embedding for each object. When given, that column is written as
            the object's vector and not as a property. The column must belong to
            ``table``.
        http_host: Host of the Weaviate server.
        http_port: Port of the Weaviate server.
        http_secure: Whether the connection uses TLS (``https``).
        api_key: An optional API key used to authenticate with Weaviate.
        headers: Optional additional headers sent with every request, e.g.
            ``{"X-OpenAI-Api-Key": "..."}`` to authorize a server-side vectorizer.
        batch_size: Number of objects grouped together per write.
        concurrency: Maximum number of writes performed in parallel per worker.
        name: A unique name for the connector. If provided, this name will be used
            in logs and monitoring dashboards.
        sort_by: If specified, the output within each mini-batch will be sorted in
            ascending order by the given columns. When multiple columns are
            provided, the corresponding value tuples are compared lexicographically.

    Returns:
        None

    Example:

    Suppose you are building a document search pipeline and want to store embeddings
    in Weaviate running locally (e.g. via the official Docker image, which serves
    its API on port ``8080``).

    Create the collection before starting the pipeline. The ``none`` vectorizer
    keeps the embeddings the pipeline sends instead of recomputing them
    server-side:

    >>> import weaviate  # doctest: +SKIP
    >>> from weaviate.classes.config import Configure  # doctest: +SKIP
    >>> client = weaviate.connect_to_local()  # doctest: +SKIP
    >>> client.collections.create(  # doctest: +SKIP
    ...     "Docs",
    ...     vectorizer_config=Configure.Vectorizer.none(),
    ... )
    >>> client.close()  # doctest: +SKIP

    Define your Pathway Live Data Framework schema and build the table:

    >>> import pathway as pw
    >>> class DocSchema(pw.Schema):
    ...     doc_id: int = pw.column_definition(primary_key=True)
    ...     embedding: list[float]
    ...
    >>> table = pw.debug.table_from_rows(
    ...     DocSchema,
    ...     [(1, [0.1, 0.2, 0.3, 0.4]), (2, [0.5, 0.6, 0.7, 0.8])],
    ... )

    Attach the Weaviate output connector, mapping the primary key and the vector
    column:

    >>> pw.io.weaviate.write(  # doctest: +SKIP
    ...     table,
    ...     collection_name="Docs",
    ...     primary_key=table.doc_id,
    ...     vector=table.embedding,
    ... )
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)  # doctest: +SKIP
    """
    _check_entitlements("weaviate")

    if primary_key is not None and primary_key._table is not table:
        raise ValueError(
            f"primary_key column {primary_key._name!r} does not belong to the "
            f"provided table. Pass a column reference from the same table, "
            f"e.g. primary_key=table.{primary_key._name}."
        )
    if vector is not None and vector._table is not table:
        raise ValueError(
            f"vector column {vector._name!r} does not belong to the provided "
            f"table. Pass a column reference from the same table, "
            f"e.g. vector=table.{vector._name}."
        )

    pk = primary_key._name if primary_key is not None else None
    vector_field = vector._name if vector is not None else None

    # Weaviate reserves "id" and "vector" as object-level keys and rejects them as
    # property names. The primary key (encoded in the UUID) and the vector column
    # are never sent as properties, so they may freely use these names; any other
    # column that does collides and is reported up front.
    reserved_property_columns = [
        column_name
        for column_name in table.schema.column_names()
        if column_name in ("id", "vector")
        and column_name != pk
        and column_name != vector_field
    ]
    if reserved_property_columns:
        raise ValueError(
            f"Column(s) {reserved_property_columns!r} use a name reserved by "
            f"Weaviate ('id' and 'vector' cannot be object properties). Rename "
            f"the column(s), or pass one as the primary_key / vector argument."
        )

    scheme = "https" if http_secure else "http"
    url = f"{scheme}://{http_host}:{http_port}"

    data_storage = api.DataStorage(
        storage_type="weaviate",
        weaviate_params=api.WeaviateParams(
            url=url,
            collection_name=collection_name,
            pk_field=pk,
            vector_field=vector_field,
            api_key=api_key,
            headers=headers,
            batch_size=batch_size,
            concurrency=concurrency,
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
            datasink_name="weaviate",
            unique_name=name,
            sort_by=sort_by,
        )
    )


__all__ = ["write"]

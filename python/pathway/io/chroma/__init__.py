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


def _check_belongs(column: ColumnReference, table: Table, role: str) -> None:
    if column._table is not table:
        raise ValueError(
            f"{role} column {column._name!r} does not belong to the provided "
            f"table. Pass a column reference from the same table, "
            f"e.g. {role}=table.{column._name}."
        )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    collection_name: str,
    *,
    primary_key: ColumnReference | None = None,
    embedding: ColumnReference,
    document: ColumnReference | None = None,
    metadata_columns: Iterable[ColumnReference] | None = None,
    host: str = "localhost",
    port: int = 8000,
    ssl: bool = False,
    headers: dict[str, str] | None = None,
    tenant: str = "default_tenant",
    database: str = "default_database",
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes a Pathway Live Data Framework table to a `Chroma <https://www.trychroma.com/>`_
    collection over the server's HTTP API.

    The collection always mirrors the current state of the table: a row added to
    the table is stored in the collection, a row removed from the table is
    deleted from the collection, and a changed row replaces the previous value.
    The ``primary_key`` column identifies each record; since Chroma record ids
    are strings, its value is stored as its string form. When ``primary_key`` is
    omitted, the row's internal Pathway key is used as the record ``id`` instead.

    Chroma records have a fixed shape, so the columns of ``table`` are mapped
    onto Chroma's record fields explicitly:

    * ``primary_key`` → the record ``id``,
    * ``embedding`` → the record ``embedding`` vector,
    * ``document`` (optional) → the record ``document`` text,
    * ``metadata_columns`` (optional) → the record ``metadata``.

    The target collection must already exist before the pipeline starts; the
    connector does not create it. Create it upfront, e.g. with
    ``chromadb.HttpClient(...).create_collection(...)``.

    Args:
        table: The table to write.
        collection_name: Name of the Chroma collection to write to. It must
            already exist on the server.
        primary_key: An optional column reference (e.g. ``table.doc_id``) whose
            values are used as the Chroma record id. The column must belong to
            ``table``; values are converted to ``str``. If omitted (the default),
            the row's internal Pathway key is used as the record id instead.
        embedding: A column reference (e.g. ``table.vector``) holding the
            embedding vector for each row. Must be of Pathway type
            ``list[float]`` or a 1-D ``numpy.ndarray``.
        document: An optional column reference (e.g. ``table.text``) holding the
            document text stored alongside each vector. Must be of type ``str``.
        metadata_columns: Optional column references for scalar metadata stored
            with each vector (e.g. ``table.title, table.category``). Each value
            must be ``str``, ``int``, ``float``, or ``bool``. A ``None`` value
            drops that key for the affected record.
        host: Host of the Chroma server. Defaults to ``"localhost"``.
        port: Port of the Chroma server. Defaults to ``8000``.
        ssl: Whether to connect over HTTPS. Defaults to ``False``.
        headers: Optional HTTP headers forwarded with every request, e.g. an
            authorization token for Chroma Cloud.
        tenant: Chroma tenant to use. Defaults to ``"default_tenant"``.
        database: Chroma database to use. Defaults to ``"default_database"``.
        name: A unique name for the connector. If provided, this name will be
            used in logs and monitoring dashboards.
        sort_by: If specified, the output within each minibatch will be sorted
            in ascending order by the given columns. When multiple columns are
            provided, the corresponding value tuples are compared
            lexicographically.

    Returns:
        None

    Example:

    Suppose you are building a document search pipeline and want to store
    embeddings in Chroma running in a container
    (``docker run -p 8000:8000 chromadb/chroma``).

    Create the collection before starting the pipeline:

    >>> import chromadb  # doctest: +SKIP
    >>> client = chromadb.HttpClient(host="localhost", port=8000)  # doctest: +SKIP
    >>> client.create_collection("docs")  # doctest: +SKIP

    Define your Pathway Live Data Framework schema and build the table:

    >>> import pathway as pw
    >>> class DocSchema(pw.Schema):
    ...     doc_id: int = pw.column_definition(primary_key=True)
    ...     text: str
    ...     embedding: list[float]
    ...
    >>> table = pw.debug.table_from_rows(
    ...     DocSchema,
    ...     [(1, "hello", [0.1, 0.2, 0.3, 0.4]), (2, "world", [0.5, 0.6, 0.7, 0.8])],
    ... )

    Attach the Chroma output connector, mapping each column to a Chroma record
    field:

    >>> pw.io.chroma.write(  # doctest: +SKIP
    ...     table,
    ...     collection_name="docs",
    ...     primary_key=table.doc_id,
    ...     embedding=table.embedding,
    ...     document=table.text,
    ... )
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)  # doctest: +SKIP
    """
    _check_entitlements("chromadb")

    if primary_key is not None:
        _check_belongs(primary_key, table, "primary_key")
    _check_belongs(embedding, table, "embedding")
    if document is not None:
        _check_belongs(document, table, "document")

    metadata_field_names: list[str] = []
    if metadata_columns is not None:
        for column in metadata_columns:
            _check_belongs(column, table, "metadata")
            metadata_field_names.append(column._name)

    chroma_params = api.ChromaParams(
        host=host,
        port=port,
        ssl=ssl,
        headers=headers,
        tenant=tenant,
        database=database,
        collection_name=collection_name,
        primary_key_field=primary_key._name if primary_key is not None else None,
        embedding_field=embedding._name,
        document_field=document._name if document is not None else None,
        metadata_fields=metadata_field_names,
    )
    data_storage = api.DataStorage(
        storage_type="chroma",
        chroma_params=chroma_params,
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
            datasink_name="chroma.sink",
            unique_name=name,
            sort_by=sort_by,
        )
    )


__all__ = ["write"]

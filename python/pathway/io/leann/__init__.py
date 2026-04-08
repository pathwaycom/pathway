# Copyright © 2026 Pathway

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import pathway.internals.dtype as dt
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io.python import ConnectorObserver

if TYPE_CHECKING:
    from pathway.internals.api import Pointer

logger = logging.getLogger(__name__)

_LEANN_INSTALL_ERROR_MESSAGE = (
    "LEANN connector requires the `leann` package. "
    "Please visit https://github.com/yichuan-w/LEANN and follow the "
    "installation instructions there."
)


class _LeannObserver(ConnectorObserver):
    """Observer that accumulates documents and builds LEANN index."""

    def __init__(
        self,
        index_path: str | os.PathLike,
        text_column: str,
        metadata_columns: list[str] | None,
        backend_name: str,
        embedding_mode: str | None,
        embedding_model: str | None,
        embedding_options: dict | None,
    ):
        self.index_path = Path(index_path)
        self.text_column = text_column
        self.metadata_columns = metadata_columns or []
        self.backend_name = backend_name
        self.embedding_mode = embedding_mode
        self.embedding_model = embedding_model
        self.embedding_options = embedding_options or {}
        # Store documents as {Pointer: {"text": str, "metadata": dict}}
        self.documents: dict[Pointer, dict[str, Any]] = {}
        self._dirty = False
        self._skipped_count = 0

    def on_change(
        self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
    ) -> None:
        if is_addition:
            text = row.get(self.text_column)
            # Skip empty/null text with warning
            if not text or (isinstance(text, str) and not text.strip()):
                self._skipped_count += 1
                logger.warning(
                    "Skipping row with empty text (key=%s, time=%s). Total skipped: %s",
                    key,
                    time,
                    self._skipped_count,
                )
                return
            # Extract metadata from specified columns
            metadata = {col: row.get(col) for col in self.metadata_columns}
            self.documents[key] = {"text": text, "metadata": metadata}
            self._dirty = True
        else:
            if self.documents.pop(key, None) is not None:
                self._dirty = True

    def on_time_end(self, time: int) -> None:
        if self._dirty:
            self._build_index()
            self._dirty = False

    def on_end(self) -> None:
        if self._dirty or not self.index_path.exists():
            self._build_index()

    def _build_index(self) -> None:
        try:
            from leann import LeannBuilder
        except ImportError as e:
            raise ImportError(_LEANN_INSTALL_ERROR_MESSAGE) from e

        if not self.documents:
            logger.warning("No documents to index - skipping index build")
            return

        builder_kwargs: dict[str, Any] = {"backend_name": self.backend_name}
        if self.embedding_mode:
            builder_kwargs["embedding_mode"] = self.embedding_mode
        if self.embedding_model:
            builder_kwargs["embedding_model"] = self.embedding_model
        if self.embedding_options:
            builder_kwargs["embedding_options"] = self.embedding_options

        builder = LeannBuilder(**builder_kwargs)
        for doc in self.documents.values():
            builder.add_text(**doc)

        # Ensure parent directory exists
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        builder.build_index(str(self.index_path))
        logger.info(
            f"LEANN index built at {self.index_path} with {len(self.documents)} documents"
        )


def _check_leann_available() -> None:
    """Check if leann package is available and raise helpful error if not."""
    try:
        import leann  # noqa: F401
    except ImportError as e:
        raise ImportError(_LEANN_INSTALL_ERROR_MESSAGE) from e


def _check_str_column(col: ColumnReference, role: str) -> None:
    """Raise ValueError if *col* is not of type str."""
    if col._column.dtype != dt.STR:
        raise ValueError(
            f"{role} {col._name!r} must be of type str, " f"got {col._column.dtype!r}."
        )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    index_path: str | os.PathLike,
    text_column: ColumnReference,
    *,
    metadata_columns: list[ColumnReference] | None = None,
    backend_name: Literal["hnsw", "diskann"] = "hnsw",
    embedding_mode: (
        Literal["sentence-transformers", "openai", "mlx", "ollama"] | None
    ) = None,
    embedding_model: str | None = None,
    embedding_options: dict | None = None,
    name: str | None = None,
) -> None:
    """
    Write table data to a `LEANN <https://github.com/yichuan-w/LEANN>`_ vector index.

    LEANN is a storage-efficient vector database that uses graph-based selective
    recomputation to achieve up to 97% storage reduction compared to traditional
    vector databases while maintaining high recall.

    The connector observes every Pathway minibatch. Whenever rows are added or
    removed, it rebuilds the full LEANN index from the current snapshot of the
    table. The result is written as a set of files that share ``index_path`` as
    their prefix (e.g. ``./articles.leann.hnsw``, ``./articles.leann.meta.json``).
    This keeps the index always consistent with the latest committed state of
    the table.

    **Performance considerations.** LEANN currently builds the index from
    scratch on every update — there is no incremental add or delete operation.
    If the document set is large and changes arrive frequently, rebuilding the
    full index after every minibatch will be slow. Use this connector with
    caution in streaming pipelines:

    - **Static mode** is the ideal fit. When you run Pathway once to convert
      a collection from one format into a LEANN index, the index is built
      exactly once and the cost is fully amortized.
    - **Infrequent commits** also work well. If your streaming pipeline
      commits rarely (large ``autocommit_duration_ms``, or an external commit
      trigger), rebuilds happen seldom and the overhead stays manageable.
    - **High-frequency streaming** over a large corpus is not a good fit.
      Every commit triggers a full rebuild; with many small commits and
      thousands of documents this can become a bottleneck. In that scenario,
      consider a vector store that supports incremental updates.

    **Limitations.** Only ``str`` columns are accepted for ``text_column`` and
    ``metadata_columns`` — passing a column of any other type raises a
    ``ValueError`` at pipeline construction time. Rows whose text column is
    empty or ``None`` are silently skipped and a warning is logged.

    Args:
        table: The Pathway table to index.
        index_path: Prefix for the LEANN index files. LEANN writes several
            files with this value as the common prefix (e.g. providing
            ``"./articles.leann"`` produces ``"./articles.leann.hnsw"``,
            ``"./articles.leann.meta.json"``, and so on).
        text_column: Column reference for the column containing text to embed
            (e.g. ``table.body``). The column must belong to ``table`` and be
            of type ``str``.
        metadata_columns: Column references for additional ``str`` columns to
            store alongside each vector (e.g. ``table.title, table.category``).
            All columns must belong to ``table``.
        backend_name: LEANN graph backend — ``"hnsw"`` (default) or
            ``"diskann"``.
        embedding_mode: Embedding provider — ``"sentence-transformers"``,
            ``"openai"``, ``"mlx"``, or ``"ollama"``. When ``None``, LEANN's
            own default is used.
        embedding_model: Specific model name, e.g. ``"facebook/contriever"``.
            When ``None``, the provider's default model is used.
        embedding_options: Additional options forwarded to the embedding
            provider, e.g. ``{"api_key": "...", "base_url": "..."}``.
        name: Unique name for this connector instance, used in logs and
            persistence snapshots.

    Returns:
        None

    Note:

    - The index is fully rebuilt after every minibatch that contains changes.
      Existing index files are overwritten on each build.
    - Requires the ``leann`` package. See https://github.com/yichuan-w/LEANN for
      installation instructions.

    Example:

    Suppose you have a CSV file ``articles.csv`` with columns ``title``,
    ``body``, and ``category``, and you want to build a LEANN vector index
    over the article bodies so that you can run semantic search against it.

    Start by defining the schema that matches your CSV:

    >>> import pathway as pw
    >>> class ArticleSchema(pw.Schema):
    ...     title: str
    ...     body: str
    ...     category: str
    ...

    Read the source file and register the LEANN sink. Pass the ``body``
    column as the text to embed; ``title`` and ``category`` are stored as
    metadata that travels with each vector and can be returned alongside
    search results:

    >>> table = pw.io.csv.read("articles.csv", schema=ArticleSchema)  # doctest: +SKIP
    >>> pw.io.leann.write(  # doctest: +SKIP
    ...     table,
    ...     index_path="./articles.leann",
    ...     text_column=table.body,
    ...     metadata_columns=[table.title, table.category],
    ...     backend_name="hnsw",
    ...     embedding_model="facebook/contriever",
    ... )

    Run the pipeline. In static mode Pathway processes the file once and
    writes the index; in streaming mode it keeps the index up to date as
    new articles arrive:

    >>> pw.run()  # doctest: +SKIP
    """
    _check_entitlements("leann")
    _check_leann_available()

    if text_column._table is not table:
        raise ValueError(
            f"text_column {text_column._name!r} does not belong to the provided "
            f"table. Pass a column reference from the same table, "
            f"e.g. text_column=table.{text_column._name}."
        )
    _check_str_column(text_column, "text_column")

    metadata_col_names: list[str] | None = None
    if metadata_columns is not None:
        for col in metadata_columns:
            if col._table is not table:
                raise ValueError(
                    f"metadata column {col._name!r} does not belong to the provided "
                    f"table. Pass column references from the same table, "
                    f"e.g. table.{col._name}."
                )
            _check_str_column(col, "metadata column")
        metadata_col_names = [col._name for col in metadata_columns]

    # Import here to avoid circular import
    from pathway.io.python import write as python_write

    observer = _LeannObserver(
        index_path=index_path,
        text_column=text_column._name,
        metadata_columns=metadata_col_names,
        backend_name=backend_name,
        embedding_mode=embedding_mode,
        embedding_model=embedding_model,
        embedding_options=embedding_options,
    )
    python_write(table, observer, name=name)


__all__ = ["write"]

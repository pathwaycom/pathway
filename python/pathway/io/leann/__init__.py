# Copyright © 2026 Pathway

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pathway.internals.trace import trace_user_frame
from pathway.io.python import ConnectorObserver

if TYPE_CHECKING:
    from pathway.internals.api import Pointer
    from pathway.internals.table import Table

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
            # TODO: Pass metadata to LEANN when the API supports it.
            # Currently metadata is collected but not indexed.
            builder.add_text(doc["text"])

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


@trace_user_frame
def write(
    table: Table,
    index_path: str | os.PathLike,
    text_column: str = "text",
    *,
    metadata_columns: list[str] | None = None,
    backend_name: Literal["hnsw", "diskann"] = "hnsw",
    embedding_mode: Literal["sentence-transformers", "openai", "mlx", "ollama"] | None = None,
    embedding_model: str | None = None,
    embedding_options: dict | None = None,
    name: str | None = None,
) -> None:
    """
    Write table data to a `LEANN <https://github.com/yichuan-w/LEANN>`_ vector index.

    LEANN is a storage-efficient vector database that uses graph-based selective
    recomputation to achieve 97% storage reduction compared to traditional solutions.
    The index is rebuilt after each minibatch when there are pending changes.

    Args:
        table: The Pathway table to index.
        index_path: Path where the LEANN index will be saved (.leann file).
        text_column: Name of the column containing text to index.
        metadata_columns: List of additional columns to include as metadata.
        backend_name: LEANN backend - "hnsw" or "diskann".
        embedding_mode: Embedding provider - "sentence-transformers", "openai",
            "mlx", or "ollama".
        embedding_model: Specific embedding model name (e.g., "facebook/contriever").
        embedding_options: Additional embedding config (api_key, base_url, etc.).
        name: Unique name for this connector instance.

    Returns:
        None

    Note:
        - Rows with empty or null text are skipped with a warning logged.
        - Existing index files are overwritten on each build.
        - LEANN requires the ``leann`` package. Please visit
          https://github.com/yichuan-w/LEANN for installation instructions.
        - metadata_columns are collected but not yet indexed (pending LEANN API support).

    Example:

    >>> import pathway as pw
    >>>
    >>> class DocumentSchema(pw.Schema):
    ...     content: str
    ...     title: str
    ...     author: str
    >>>
    >>> table = pw.io.csv.read("./documents.csv", schema=DocumentSchema)  # doctest: +SKIP
    >>> pw.io.leann.write(  # doctest: +SKIP
    ...     table,
    ...     index_path="./my_index.leann",
    ...     text_column="content",
    ...     metadata_columns=["title", "author"],
    ...     backend_name="hnsw",
    ...     embedding_model="facebook/contriever",
    ... )
    >>> pw.run()  # doctest: +SKIP
    """
    _check_leann_available()

    # Import here to avoid circular import
    from pathway.io.python import write as python_write

    observer = _LeannObserver(
        index_path=index_path,
        text_column=text_column,
        metadata_columns=metadata_columns,
        backend_name=backend_name,
        embedding_mode=embedding_mode,
        embedding_model=embedding_model,
        embedding_options=embedding_options,
    )
    python_write(table, observer, name=name)


__all__ = ["write"]

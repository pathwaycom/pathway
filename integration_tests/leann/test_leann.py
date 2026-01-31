# Copyright © 2026 Pathway

from __future__ import annotations

from pathlib import Path

import pytest

import pathway as pw

# Skip all tests if leann is not installed
leann = pytest.importorskip("leann")


class DocumentSchema(pw.Schema):
    text: str


class DocumentWithMetadataSchema(pw.Schema):
    text: str
    title: str
    category: str


class TestLeannIntegration:
    """Integration tests for the LEANN output connector."""

    def test_basic_write(self, tmp_path: Path):
        """Test basic writing to a LEANN index."""
        index_path = tmp_path / "test_basic.leann"

        table = pw.debug.table_from_rows(
            schema=DocumentSchema,
            rows=[
                ("LEANN is a vector database",),
                ("Pathway enables streaming",),
                ("Vector search is powerful",),
            ],
        )

        pw.io.leann.write(
            table,
            index_path=str(index_path),
            text_column="text",
            backend_name="hnsw",
        )

        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        # Verify the index was created (LEANN creates multiple files with base name)
        # Check for the metadata file which is always created
        meta_file = Path(str(index_path) + ".meta.json")
        assert meta_file.exists(), f"Expected {meta_file} to exist"

        # Verify we can search the index
        from leann import LeannSearcher

        searcher = LeannSearcher(str(index_path))
        results = searcher.search("vector database", top_k=1)
        assert len(results) > 0

    def test_write_with_metadata_columns(self, tmp_path: Path):
        """Test writing with metadata columns."""
        index_path = tmp_path / "test_metadata.leann"

        table = pw.debug.table_from_rows(
            schema=DocumentWithMetadataSchema,
            rows=[
                ("Introduction to LEANN", "LEANN Guide", "tutorial"),
                ("Advanced vector search", "Search Tips", "advanced"),
            ],
        )

        pw.io.leann.write(
            table,
            index_path=str(index_path),
            text_column="text",
            metadata_columns=["title", "category"],
            backend_name="hnsw",
        )

        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        assert Path(str(index_path) + ".meta.json").exists()

    def test_write_with_diskann_backend(self, tmp_path: Path):
        """Test writing with DiskANN backend."""
        index_path = tmp_path / "test_diskann.leann"

        table = pw.debug.table_from_rows(
            schema=DocumentSchema,
            rows=[
                ("Document one for DiskANN test",),
                ("Document two for DiskANN test",),
            ],
        )

        pw.io.leann.write(
            table,
            index_path=str(index_path),
            text_column="text",
            backend_name="diskann",
        )

        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        assert Path(str(index_path) + ".meta.json").exists()

    def test_write_with_custom_text_column(self, tmp_path: Path):
        """Test writing with a custom text column name."""
        index_path = tmp_path / "test_custom_column.leann"

        class ContentSchema(pw.Schema):
            content: str
            id: int

        table = pw.debug.table_from_rows(
            schema=ContentSchema,
            rows=[
                ("First document content", 1),
                ("Second document content", 2),
            ],
        )

        pw.io.leann.write(
            table,
            index_path=str(index_path),
            text_column="content",
            backend_name="hnsw",
        )

        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        assert Path(str(index_path) + ".meta.json").exists()

    def test_write_creates_parent_directories(self, tmp_path: Path):
        """Test that write() creates parent directories if they don't exist."""
        index_path = tmp_path / "nested" / "dir" / "test.leann"

        table = pw.debug.table_from_rows(
            schema=DocumentSchema,
            rows=[("Test document",)],
        )

        pw.io.leann.write(
            table,
            index_path=str(index_path),
            text_column="text",
            backend_name="hnsw",
        )

        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        assert Path(str(index_path) + ".meta.json").exists()

    def test_write_with_named_connector(self, tmp_path: Path):
        """Test writing with a named connector for monitoring."""
        index_path = tmp_path / "test_named.leann"

        table = pw.debug.table_from_rows(
            schema=DocumentSchema,
            rows=[("Named connector test",)],
        )

        pw.io.leann.write(
            table,
            index_path=str(index_path),
            text_column="text",
            backend_name="hnsw",
            name="my_leann_index",
        )

        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        assert Path(str(index_path) + ".meta.json").exists()


class TestLeannEdgeCases:
    """Edge case tests for the LEANN connector."""

    def test_empty_table(self, tmp_path: Path):
        """Test behavior with an empty table."""
        index_path = tmp_path / "test_empty.leann"

        # Create an empty table
        table = pw.debug.table_from_rows(
            schema=DocumentSchema,
            rows=[],
        )

        pw.io.leann.write(
            table,
            index_path=str(index_path),
            text_column="text",
            backend_name="hnsw",
        )

        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        # Index should not be created for empty table
        assert not Path(str(index_path) + ".meta.json").exists()

# Copyright © 2026 Pathway

from __future__ import annotations

import pathlib
from unittest import mock

import pytest

from pathway.io.leann import _LeannObserver


class TestLeannObserver:
    """Unit tests for the _LeannObserver class."""

    def test_on_change_addition(self, tmp_path: pathlib.Path):
        """Test that additions are properly tracked."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        # Create a mock Pointer
        key = mock.MagicMock()
        row = {"text": "Hello world"}

        observer.on_change(key, row, time=0, is_addition=True)

        assert key in observer.documents
        assert observer.documents[key]["text"] == "Hello world"
        assert observer._dirty is True

    def test_on_change_deletion(self, tmp_path: pathlib.Path):
        """Test that deletions are properly tracked."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        key = mock.MagicMock()
        row = {"text": "Hello world"}

        # First add the document
        observer.on_change(key, row, time=0, is_addition=True)
        assert key in observer.documents

        # Reset dirty flag
        observer._dirty = False

        # Now delete it
        observer.on_change(key, row, time=1, is_addition=False)

        assert key not in observer.documents
        assert observer._dirty is True

    def test_on_change_deletion_nonexistent_key(self, tmp_path: pathlib.Path):
        """Test that deleting a nonexistent key doesn't set dirty flag."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        key = mock.MagicMock()

        # Try to delete a key that was never added
        observer.on_change(key, {"text": "Hello"}, time=0, is_addition=False)

        assert key not in observer.documents
        assert observer._dirty is False  # No actual change occurred

    def test_on_change_skips_empty_text(self, tmp_path: pathlib.Path):
        """Test that rows with empty text are skipped."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        key = mock.MagicMock()

        # Test with empty string
        observer.on_change(key, {"text": ""}, time=0, is_addition=True)
        assert key not in observer.documents
        assert observer._skipped_count == 1
        assert observer._dirty is False  # Skipped rows don't set dirty

        # Test with whitespace-only string
        observer.on_change(key, {"text": "   "}, time=0, is_addition=True)
        assert key not in observer.documents
        assert observer._skipped_count == 2
        assert observer._dirty is False

        # Test with None
        observer.on_change(key, {"text": None}, time=0, is_addition=True)
        assert key not in observer.documents
        assert observer._skipped_count == 3
        assert observer._dirty is False

        # Test with missing text column
        observer.on_change(key, {"other": "value"}, time=0, is_addition=True)
        assert key not in observer.documents
        assert observer._skipped_count == 4
        assert observer._dirty is False

    def test_on_change_with_metadata(self, tmp_path: pathlib.Path):
        """Test that metadata columns are extracted."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="content",
            metadata_columns=["title", "author"],
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        key = mock.MagicMock()
        row = {"content": "Document text", "title": "My Title", "author": "Author Name"}

        observer.on_change(key, row, time=0, is_addition=True)

        assert observer.documents[key]["text"] == "Document text"
        assert observer.documents[key]["metadata"]["title"] == "My Title"
        assert observer.documents[key]["metadata"]["author"] == "Author Name"

    def test_on_time_end_rebuilds_when_dirty(self, tmp_path: pathlib.Path):
        """Test that index is rebuilt on time_end when there are pending changes."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        key = mock.MagicMock()
        observer.on_change(key, {"text": "Hello"}, time=0, is_addition=True)

        with mock.patch.object(observer, "_build_index") as mock_build:
            observer.on_time_end(time=0)
            mock_build.assert_called_once()
            assert observer._dirty is False

    def test_on_end_builds_index_when_dirty(self, tmp_path: pathlib.Path):
        """Test that on_end builds the index when there are pending changes."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        key = mock.MagicMock()
        observer.on_change(key, {"text": "Hello"}, time=0, is_addition=True)

        with mock.patch.object(observer, "_build_index") as mock_build:
            observer.on_end()
            mock_build.assert_called_once()

    def test_on_end_skips_build_when_not_dirty_and_index_exists(self, tmp_path: pathlib.Path):
        """Test that on_end skips build when not dirty and index already exists."""
        with mock.patch("pathlib.Path.exists", return_value=True):
            observer = _LeannObserver(
                index_path=tmp_path / "test.leann",
                text_column="text",
                metadata_columns=None,
                backend_name="hnsw",
                embedding_mode=None,
                embedding_model=None,
                embedding_options=None,
            )
            # Not dirty (no changes)
            observer._dirty = False

            with mock.patch.object(observer, "_build_index") as mock_build:
                observer.on_end()
                mock_build.assert_not_called()

    def test_build_index_skips_when_no_documents(self, tmp_path: pathlib.Path):
        """Test that _build_index skips when there are no documents."""
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode=None,
            embedding_model=None,
            embedding_options=None,
        )

        # No documents added - mock the leann import
        mock_leann_module = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"leann": mock_leann_module}):
            observer._build_index()
            mock_leann_module.LeannBuilder.assert_not_called()

    def test_build_index_with_documents(self, tmp_path: pathlib.Path):
        """Test that _build_index properly creates the LEANN index."""
        index_path = tmp_path / "test_build.leann"
        observer = _LeannObserver(
            index_path=index_path,
            text_column="text",
            metadata_columns=None,
            backend_name="hnsw",
            embedding_mode="sentence-transformers",
            embedding_model="facebook/contriever",
            embedding_options={"device": "cpu"},
        )

        # Add some documents
        key1, key2 = mock.MagicMock(), mock.MagicMock()
        observer.on_change(key1, {"text": "First document"}, time=0, is_addition=True)
        observer.on_change(key2, {"text": "Second document"}, time=0, is_addition=True)

        mock_builder = mock.MagicMock()
        mock_leann_module = mock.MagicMock()
        mock_leann_module.LeannBuilder.return_value = mock_builder

        with (
            mock.patch.dict("sys.modules", {"leann": mock_leann_module}),
            mock.patch("pathlib.Path.mkdir"),
        ):
            observer._build_index()

            # Check LeannBuilder was initialized with correct kwargs
            mock_leann_module.LeannBuilder.assert_called_once_with(
                backend_name="hnsw",
                embedding_mode="sentence-transformers",
                embedding_model="facebook/contriever",
                embedding_options={"device": "cpu"},
            )

            # Check add_text was called for each document
            assert mock_builder.add_text.call_count == 2

            # Check build_index was called
            mock_builder.build_index.assert_called_once_with(str(index_path))


class TestLeannWrite:
    """Test the pw.io.leann.write() function."""

    def test_write_function_exists(self):
        """Test that write function is exported and callable."""
        from pathway.io.leann import write

        assert callable(write)
        assert write.__name__ == "write"

    def test_observer_initialization_via_write_params(self, tmp_path: pathlib.Path):
        """Test that _LeannObserver is initialized with correct parameters."""
        # Instead of calling write() directly (which has beartype issues in tests),
        # we test that _LeannObserver can be created with the expected parameters
        observer = _LeannObserver(
            index_path=tmp_path / "test.leann",
            text_column="content",
            metadata_columns=["title"],
            backend_name="diskann",
            embedding_mode="openai",
            embedding_model="text-embedding-3-small",
            embedding_options={"api_key": "test-key"},
        )

        assert observer.text_column == "content"
        assert observer.metadata_columns == ["title"]
        assert observer.backend_name == "diskann"
        assert observer.embedding_mode == "openai"
        assert observer.embedding_model == "text-embedding-3-small"
        assert observer.embedding_options == {"api_key": "test-key"}

    def test_check_leann_available_raises_on_missing_package(self):
        """Test that _check_leann_available raises ImportError when leann is missing."""
        from pathway.io.leann import _check_leann_available

        with mock.patch.dict("sys.modules", {"leann": None}):
            with pytest.raises(ImportError, match="leann"):
                _check_leann_available()

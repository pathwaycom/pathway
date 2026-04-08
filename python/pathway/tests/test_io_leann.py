# Copyright © 2026 Pathway

from __future__ import annotations

import pathlib
from unittest import mock

import pytest

import pathway as pw
from pathway.io.leann import _check_leann_available, _check_str_column, _LeannObserver


def _make_observer(tmp_path, text_column="text", metadata_columns=None):
    return _LeannObserver(
        index_path=tmp_path / "test.leann",
        text_column=text_column,
        metadata_columns=metadata_columns,
        backend_name="hnsw",
        embedding_mode=None,
        embedding_model=None,
        embedding_options=None,
    )


def test_on_change_addition(tmp_path: pathlib.Path):
    observer = _make_observer(tmp_path)
    key = mock.MagicMock()

    observer.on_change(key, {"text": "Hello world"}, time=0, is_addition=True)

    assert key in observer.documents
    assert observer.documents[key]["text"] == "Hello world"
    assert observer._dirty is True


def test_on_change_deletion(tmp_path: pathlib.Path):
    observer = _make_observer(tmp_path)
    key = mock.MagicMock()

    observer.on_change(key, {"text": "Hello world"}, time=0, is_addition=True)
    assert key in observer.documents

    observer._dirty = False
    observer.on_change(key, {"text": "Hello world"}, time=1, is_addition=False)

    assert key not in observer.documents
    assert observer._dirty is True


def test_on_change_deletion_nonexistent_key(tmp_path: pathlib.Path):
    observer = _make_observer(tmp_path)
    key = mock.MagicMock()

    observer.on_change(key, {"text": "Hello"}, time=0, is_addition=False)

    assert key not in observer.documents
    assert observer._dirty is False


def test_on_change_skips_empty_text(tmp_path: pathlib.Path):
    observer = _make_observer(tmp_path)
    key = mock.MagicMock()

    observer.on_change(key, {"text": ""}, time=0, is_addition=True)
    assert key not in observer.documents
    assert observer._skipped_count == 1
    assert observer._dirty is False

    observer.on_change(key, {"text": "   "}, time=0, is_addition=True)
    assert key not in observer.documents
    assert observer._skipped_count == 2
    assert observer._dirty is False

    observer.on_change(key, {"text": None}, time=0, is_addition=True)
    assert key not in observer.documents
    assert observer._skipped_count == 3
    assert observer._dirty is False

    observer.on_change(key, {"other": "value"}, time=0, is_addition=True)
    assert key not in observer.documents
    assert observer._skipped_count == 4
    assert observer._dirty is False


def test_on_change_with_metadata(tmp_path: pathlib.Path):
    observer = _make_observer(
        tmp_path, text_column="content", metadata_columns=["title", "author"]
    )
    key = mock.MagicMock()

    observer.on_change(
        key,
        {"content": "Document text", "title": "My Title", "author": "Author Name"},
        time=0,
        is_addition=True,
    )

    assert observer.documents[key]["text"] == "Document text"
    assert observer.documents[key]["metadata"]["title"] == "My Title"
    assert observer.documents[key]["metadata"]["author"] == "Author Name"


def test_on_time_end_rebuilds_when_dirty(tmp_path: pathlib.Path):
    observer = _make_observer(tmp_path)
    key = mock.MagicMock()
    observer.on_change(key, {"text": "Hello"}, time=0, is_addition=True)

    with mock.patch.object(observer, "_build_index") as mock_build:
        observer.on_time_end(time=0)
        mock_build.assert_called_once()
        assert observer._dirty is False


def test_on_end_builds_index_when_dirty(tmp_path: pathlib.Path):
    observer = _make_observer(tmp_path)
    key = mock.MagicMock()
    observer.on_change(key, {"text": "Hello"}, time=0, is_addition=True)

    with mock.patch.object(observer, "_build_index") as mock_build:
        observer.on_end()
        mock_build.assert_called_once()


def test_on_end_skips_build_when_not_dirty_and_index_exists(tmp_path: pathlib.Path):
    with mock.patch("pathlib.Path.exists", return_value=True):
        observer = _make_observer(tmp_path)
        observer._dirty = False

        with mock.patch.object(observer, "_build_index") as mock_build:
            observer.on_end()
            mock_build.assert_not_called()


def test_build_index_skips_when_no_documents(tmp_path: pathlib.Path):
    observer = _make_observer(tmp_path)
    mock_leann_module = mock.MagicMock()

    with mock.patch.dict("sys.modules", {"leann": mock_leann_module}):
        observer._build_index()
        mock_leann_module.LeannBuilder.assert_not_called()


def test_build_index_with_documents(tmp_path: pathlib.Path):
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

        mock_leann_module.LeannBuilder.assert_called_once_with(
            backend_name="hnsw",
            embedding_mode="sentence-transformers",
            embedding_model="facebook/contriever",
            embedding_options={"device": "cpu"},
        )
        assert mock_builder.add_text.call_count == 2
        mock_builder.add_text.assert_any_call(text="First document", metadata={})
        mock_builder.add_text.assert_any_call(text="Second document", metadata={})
        mock_builder.build_index.assert_called_once_with(str(index_path))


def test_observer_initialization_via_write_params(tmp_path: pathlib.Path):
    # Instead of calling write() directly (which has beartype issues in tests),
    # we test that _LeannObserver can be created with the expected parameters.
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


def test_check_leann_available_raises_on_missing_package():
    with mock.patch.dict("sys.modules", {"leann": None}):
        with pytest.raises(ImportError, match="leann"):
            _check_leann_available()


def test_write_raises_on_non_string_text_column():
    class DocSchema(pw.Schema):
        count: int

    table = pw.debug.table_from_rows(DocSchema, [(1,)])

    with pytest.raises(ValueError, match="must be of type str"):
        _check_str_column(table.count, "text_column")


def test_write_raises_on_non_string_metadata_column():
    class DocSchema(pw.Schema):
        score: float

    table = pw.debug.table_from_rows(DocSchema, [(1.0,)])

    with pytest.raises(ValueError, match="must be of type str"):
        _check_str_column(table.score, "metadata column")

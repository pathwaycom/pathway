# Copyright © 2026 Pathway

import json
from unittest.mock import Mock

import pytest
from googleapiclient.discovery import build
from httplib2 import Http, Response

from pathway.io.gdrive import DEFAULT_MIME_TYPE_MAPPING, GDriveFile, _GDriveClient


@pytest.fixture
def client(monkeypatch):
    http = Mock(spec=Http)
    client = _GDriveClient.__new__(_GDriveClient)
    client.drive = build("drive", "v3", http=http, static_discovery=True)
    client.object_size_limit = None
    client.export_type_mapping = DEFAULT_MIME_TYPE_MAPPING
    monkeypatch.setattr("googleapiclient.http.time.sleep", lambda delay: None)
    return client, http


@pytest.fixture(
    params=[
        "timeout",
        "connection_reset",
        "rate_limit",
        "user_rate_limit",
        "server_error",
    ]
)
def transient_failure(request):
    if request.param == "timeout":
        return TimeoutError("Connection timed out")
    if request.param == "connection_reset":
        return ConnectionResetError("Connection reset")
    if request.param == "user_rate_limit":
        return (
            Response({"status": "403"}),
            b'{"error": {"errors": [{"reason": "userRateLimitExceeded"}]}}',
        )
    status = "429" if request.param == "rate_limit" else "503"
    return Response({"status": status}), b"{}"


def test_metadata_read_recovers_after_transient_failure(client, transient_failure):
    client, http = client
    metadata = {"id": "file", "name": "first.txt"}
    http.request.side_effect = [
        transient_failure,
        (Response({"status": "200"}), json.dumps(metadata).encode()),
    ]

    assert client._get("file") == metadata
    assert http.request.call_count == 2


def test_listing_retries_current_page_without_duplicate_files(
    client, transient_failure
):
    client, http = client
    http.request.side_effect = [
        (
            Response({"status": "200"}),
            b'{"files": [{"id": "first"}], "nextPageToken": "next"}',
        ),
        transient_failure,
        (Response({"status": "200"}), b'{"files": [{"id": "second"}]}'),
    ]

    assert client._query() == [{"id": "first"}, {"id": "second"}]
    assert http.request.call_count == 3
    calls = http.request.call_args_list
    assert calls[1].args == calls[2].args
    assert "pageToken=next" in calls[1].args[0]


@pytest.mark.parametrize(
    "mime_type", ["text/plain", "application/vnd.google-apps.spreadsheet"]
)
def test_download_retries_current_chunk_without_duplicate_bytes(
    client, transient_failure, mime_type
):
    client, http = client
    http.request.side_effect = [
        (Response({"status": "206", "content-range": "bytes 0-2/6"}), b"abc"),
        transient_failure,
        (Response({"status": "206", "content-range": "bytes 3-5/6"}), b"def"),
    ]
    file = GDriveFile({"id": "file", "mimeType": mime_type, "size": "6"})

    assert client.download(file) == b"abcdef"
    assert http.request.call_count == 3
    calls = http.request.call_args_list
    assert calls[1].kwargs["headers"]["range"].startswith("bytes=3-")
    assert calls[1] == calls[2]


def test_metadata_read_stops_after_retry_budget_is_exhausted(client):
    client, http = client
    http.request.side_effect = TimeoutError("Connection timed out")

    with pytest.raises(TimeoutError):
        client._get("file")
    assert http.request.call_count == 4


def test_metadata_read_does_not_retry_permission_denied(client):
    client, http = client
    http.request.return_value = (
        Response({"status": "403"}),
        b'{"error": {"message": "Forbidden", "errors": [{"reason": "insufficientFilePermissions"}]}}',
    )

    with pytest.warns(UserWarning, match="cannot fetch metadata"):
        assert client._get("file") is None
    assert http.request.call_count == 1

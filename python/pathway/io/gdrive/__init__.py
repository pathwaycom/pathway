# Copyright © 2024 Pathway

from __future__ import annotations

import io
import json
import logging
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NewType

from google.oauth2.service_account import Credentials as ServiceCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

import pathway as pw
from pathway.internals import api
from pathway.internals.api import SessionType
from pathway.internals.runtime_type_check import check_arg_types
from pathway.io.python import ConnectorSubject

SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
MIME_TYPE_FOLDER = "application/vnd.google-apps.folder"
FILE_FIELDS = "id, name, mimeType, parents, modifiedTime, thumbnailLink, lastModifyingUser, trashed, size"

DEFAULT_MIME_TYPE_MAPPING: dict[str, str] = {
    "application/vnd.google-apps.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",  # noqa: E501
}

GDriveFile = NewType("GDriveFile", dict)


class _GDriveClient:
    def __init__(
        self, credentials: ServiceCredentials, object_size_limit: int | None = None
    ) -> None:
        self.drive = build("drive", "v3", credentials=credentials, num_retries=3)
        self.export_type_mapping = DEFAULT_MIME_TYPE_MAPPING
        self.object_size_limit = object_size_limit

    def _query(self, q: str = "") -> list:
        items = []
        page_token = None
        while True:
            response = (
                self.drive.files()
                .list(
                    q=q,
                    pageSize=10,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    fields=f"nextPageToken, files({FILE_FIELDS})",
                    pageToken=page_token,
                )
                .execute()
            )
            items.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

        return items

    def _ls(self, id: str) -> list[GDriveFile]:
        root = self._get(id)
        files: list[GDriveFile] = []
        if root is None:
            return []
        elif root["mimeType"] != MIME_TYPE_FOLDER:
            return [root]
        else:
            subitems = self._query(f"'{id}' in parents and trashed=false")
            files = [i for i in subitems if i["mimeType"] != MIME_TYPE_FOLDER]
            files = self._filter_by_size(files)
            subdirs = [i for i in subitems if i["mimeType"] == MIME_TYPE_FOLDER]
            for subdir in subdirs:
                files.extend(self._ls(subdir["id"]))
            return files

    def _filter_by_size(self, files: list[GDriveFile]) -> list[GDriveFile]:
        if self.object_size_limit is None:
            return files
        result = []
        for file in files:
            size = int(file["size"])
            if size > self.object_size_limit:
                name = file["name"]
                logging.info(
                    f"Skipping object {name} because its size "
                    f"{size} exceeds the limit {self.object_size_limit}"
                )
                continue
            result.append(file)
        return result

    def _get(self, file_id: str) -> GDriveFile | None:
        try:
            file = (
                self.drive.files()
                .get(
                    fileId=file_id,
                    fields=FILE_FIELDS,
                    supportsAllDrives=True,
                )
                .execute()
            )
            if file.get("trashed") is True:
                warnings.warn(
                    f"cannot fetch metadata of file with id {file_id}, reason: moved to trash"
                )
                return None
            else:
                return file
        except HttpError as e:
            reason: str = e.reason
            warnings.warn(
                f"cannot fetch metadata of file with id {file_id}, reason: {reason}"
            )
            return None

    def _prepare_download_request(self, file: GDriveFile) -> Any:
        file_id = file["id"]
        mime_type = file["mimeType"]
        export_type = self.export_type_mapping.get(mime_type, None)
        if export_type is not None:
            return self.drive.files().export_media(fileId=file_id, mimeType=export_type)
        else:
            return self.drive.files().get_media(fileId=file_id)

    def download(self, file: GDriveFile) -> bytes | None:
        try:
            response = io.BytesIO()
            request = self._prepare_download_request(file)
            downloader = MediaIoBaseDownload(response, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
            return response.getvalue()
        except HttpError as e:
            file_id = file["id"]
            reason: str = e.reason
            warnings.warn(f"cannot fetch file with id {file_id}, reason: {reason}")
            return None

    def tree(self, root_id: str) -> _GDriveTree:
        return _GDriveTree({file["id"]: file for file in self._ls(root_id)})


@dataclass(frozen=True)
class _GDriveTree:
    files: dict[str, GDriveFile]

    def _diff(self, other: _GDriveTree) -> list[GDriveFile]:
        return [file for file in self.files.values() if file["id"] not in other.files]

    def _modified_files(self, previous: _GDriveTree) -> list[GDriveFile]:
        result = []
        for file in self.files.values():
            previous_file = previous.files.get(file["id"], None)
            if (
                previous_file is not None
                and file["modifiedTime"] > previous_file["modifiedTime"]
            ):
                result.append(file)
        return result

    def removed_files(self, previous: _GDriveTree) -> list[GDriveFile]:
        return previous._diff(self)

    def new_and_changed_files(self, previous: _GDriveTree) -> list[GDriveFile]:
        return self._diff(previous) + self._modified_files(previous)


class _GDriveSubject(ConnectorSubject):
    _credentials_factory: Callable[[], ServiceCredentials]
    _root: str
    _refresh_interval: int
    _mode: str
    _append_metadata: bool
    _object_size_limit: int | None

    def __init__(
        self,
        *,
        credentials_factory: Callable[[], ServiceCredentials],
        root: str,
        refresh_interval: int,
        mode: str,
        with_metadata: bool,
        object_size_limit: int | None,
    ) -> None:
        super().__init__()
        self._credentials_factory = credentials_factory
        self._refresh_interval = refresh_interval
        self._root = root
        self._mode = mode
        self._append_metadata = with_metadata
        self._object_size_limit = object_size_limit
        assert mode in ["streaming", "static"]

    @property
    def _session_type(self) -> SessionType:
        return SessionType.UPSERT if self._mode == "streaming" else SessionType.NATIVE

    @property
    def _with_metadata(self) -> bool:
        return self._append_metadata

    def run(self) -> None:
        client = _GDriveClient(self._credentials_factory(), self._object_size_limit)
        prev = _GDriveTree({})

        while True:
            tree = client.tree(self._root)
            for file in tree.removed_files(prev):
                self.remove(file)
            for file in tree.new_and_changed_files(prev):
                payload = client.download(file)
                if payload is not None:
                    self.upsert(file, payload)

            if self._mode == "static":
                break
            prev = tree
            time.sleep(self._refresh_interval)

    def upsert(self, file: GDriveFile, payload: bytes):
        metadata = json.dumps(file).encode(encoding="utf-8")
        self._add(api.ref_scalar(file["id"]), payload, metadata)

    def remove(self, file: GDriveFile):
        self._remove(api.ref_scalar(file["id"]), b"")


@check_arg_types
def read(
    object_id: str,
    *,
    mode: str = "streaming",
    object_size_limit: int | None = None,
    refresh_interval: int = 30,
    service_user_credentials_file: str,
    with_metadata: bool = False,
) -> pw.Table:
    """Reads a table from a Google Drive directory or file.

    It will return a table with single column `data` containing each file in a binary format.

    Args:
        object_id: id of a directory or file. Directories will be scanned recursively.
        mode: denotes how the engine polls the new data from the source. Currently "streaming"
            and "static" are supported. If set to "streaming", it will check for updates, deletions
            and new files every `refresh_interval` seconds. "static" mode will only consider
            the available data and ingest all of it in one commit.
            The default value is "streaming".
        object_size_limit: Maximum size (in bytes) of a file that will be processed by \
this connector or `None` if no filtering by size should be made;
        refresh_interval: time in seconds between scans. Applicable if mode is set to 'streaming'.
        service_user_credentials_file: Google API service user json file.
        with_metadata: when set to True, the connector will add an additional column named
            `_metadata` to the table. This column will contain file metadata,
            such as: `id`, `name`, `mimeType`, `parents`, `modifiedTime`, `thumbnailLink`, `lastModifyingUser`.
    Returns:
        The table read.

    Example:

    >>> import pathway as pw
    ...
    >>> table = pw.io.gdrive.read(
    ...     object_id="0BzDTMZY18pgfcGg4ZXFRTDFBX0j",
    ...     service_user_credentials_file="credentials.json"
    ... )
    """

    if mode not in ["streaming", "static"]:
        raise ValueError(f"Unrecognized connector mode: {mode}")

    def credentials_factory() -> ServiceCredentials:
        return ServiceCredentials.from_service_account_file(
            service_user_credentials_file
        )

    subject = _GDriveSubject(
        credentials_factory=credentials_factory,
        root=object_id,
        refresh_interval=refresh_interval,
        mode=mode,
        with_metadata=with_metadata,
        object_size_limit=object_size_limit,
    )

    return pw.io.python.read(
        subject,
        format="binary",
    )

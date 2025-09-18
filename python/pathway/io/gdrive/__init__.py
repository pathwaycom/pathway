# Copyright © 2024 Pathway

from __future__ import annotations

import fnmatch
import io
import json
import logging
import time
import uuid
import warnings
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from os import PathLike, fspath
from queue import Queue
from typing import Any, Literal, NewType

from google.oauth2.service_account import Credentials as ServiceCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

import pathway as pw
from pathway.internals import api
from pathway.internals.api import SessionType
from pathway.internals.runtime_type_check import check_arg_types
from pathway.io._utils import (
    STATUS_DOWNLOADED,
    STATUS_SIZE_LIMIT_EXCEEDED,
    STATUS_SYMLINKS_NOT_SUPPORTED,
)
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

# To optimize usage of the Google Drive API, we combine listing requests for all files
# with a specific parent so that a single request retrieves listings for multiple
# directories at once.
#
# I haven't found reliable documentation on the query length limit or on how many
# conditions can be included in a single request. However, numerous unofficial sources
# mention that people successfully combine up to several hundred parents, so the chosen
# value seems reasonable and can be slightly increased if necessary.
_MAX_ITEMS_PER_LIST_REQUEST = 32

# The Google Drive API doesn't provide functionality to list a directory while including
# all nested objects, so we have to handle this ourselves. There are two approaches:
#
# 1. Traverse the directory using any graph-traversal algorithm, basically making one
#    API request per small group of nodes. If there are many such groups, this becomes
#    costly.
# 2. List all files on the entire drive and then filter out only the needed ones.
#
# A reasonable compromise is required. For example, in tests you might select a small
# directory on a large drive, or conversely process the entire drive. Therefore, we
# first attempt to traverse the structure without spending many requests, and if that
# fails, we fall back to listing the entire drive. Note that if the root of the drive
# is passed, we can skip heuristics and safely list all files directly.
_MAX_DIRECTORIES_FOR_TRAVERSAL = 32


# Determines how objects are scanned. When a file ID is provided, the algorithm is
# straightforward; otherwise, on the first run, the heuristic described above is used.
# It is possible to skip running the heuristic in the first step and explicitly specify
# the method using the `_list_objects_strategy` parameter in `pw.io.gdrive.read`.
class _ListObjectsStrategy(Enum):
    FullScan = 0
    TreeTraversal = 1
    SingleObjectRequest = 2


class _ListObjectsLimitExceeded(Exception):
    pass


def extend_metadata(metadata: GDriveFile) -> GDriveFile:
    metadata = add_url(metadata)
    metadata = add_path(metadata)
    metadata = add_seen_at(metadata)
    metadata = add_status(metadata)
    return metadata


def add_seen_at(metadata: GDriveFile) -> GDriveFile:
    metadata["seen_at"] = int(time.time())
    return metadata


def add_url(metadata: GDriveFile) -> GDriveFile:
    id = metadata["id"]
    metadata["url"] = f"https://drive.google.com/file/d/{id}/"
    return metadata


def add_path(metadata: GDriveFile) -> GDriveFile:
    metadata["path"] = metadata["name"]
    return metadata


def add_status(metadata: GDriveFile) -> GDriveFile:
    metadata["status"] = STATUS_DOWNLOADED
    return metadata


class _GDriveClient:
    def __init__(
        self,
        root: str,
        credentials: ServiceCredentials,
        object_size_limit: int | None = None,
        file_name_pattern: list | str | None = None,
        list_objects_strategy: _ListObjectsStrategy | None = None,
    ) -> None:
        self.root = root
        self.drive = build("drive", "v3", credentials=credentials, num_retries=3)
        self.export_type_mapping = DEFAULT_MIME_TYPE_MAPPING
        self.object_size_limit = object_size_limit
        self.file_name_pattern = file_name_pattern
        if list_objects_strategy is None:
            list_objects_strategy = self._deduce_list_objects_strategy()
        self.list_objects_strategy = list_objects_strategy

    def _deduce_list_objects_strategy(self) -> _ListObjectsStrategy:
        root_object = self._get(self.root)
        if root_object is None or root_object["mimeType"] != MIME_TYPE_FOLDER:
            return _ListObjectsStrategy.SingleObjectRequest
        elif "parents" not in root_object:
            return _ListObjectsStrategy.FullScan
        else:
            return _ListObjectsStrategy.TreeTraversal

    def _query(self, q: str = "") -> list:
        items = []
        page_token = None
        while True:
            response = (
                self.drive.files()
                .list(
                    q=q,
                    pageSize=1000,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    fields=f"nextPageToken, files({FILE_FIELDS})",
                    pageToken=page_token,
                    corpora="allDrives",
                )
                .execute()
            )
            items.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

        return items

    def _traverse_objects_with_limit(self) -> list[GDriveFile]:
        files = []
        n_requests_done = 0
        queue: Queue[str] = Queue()
        reachable_folder_ids = set([self.root])

        queue.put(self.root)
        while not queue.empty():
            items: list[str] = []
            while len(items) < _MAX_ITEMS_PER_LIST_REQUEST and not queue.empty():
                items.append(queue.get())

            # We create and execute an API request to retrieve the contents of multiple
            # directories. Note that if there is a continuation token and we actually made
            # multiple requests, this will not be counted; we still count it as a single
            # request.
            query_parts = [f"'{item}' in parents" for item in items]
            parent_condition = " or ".join(query_parts)
            subitems = self._query(f"({parent_condition}) and trashed=false")
            n_requests_done += 1

            # Add files to the search result.
            files_found = [i for i in subitems if i["mimeType"] != MIME_TYPE_FOLDER]
            files.extend(files_found)

            # Put the subdirs to the queue.
            subdirs = [i["id"] for i in subitems if i["mimeType"] == MIME_TYPE_FOLDER]
            for subdir in subdirs:
                if subdir not in reachable_folder_ids:
                    reachable_folder_ids.add(subdir)
                    queue.put(subdir)

            # If the limit will be exceeded, terminate.
            n_requests_needed = queue.qsize() / _MAX_DIRECTORIES_FOR_TRAVERSAL
            if queue.qsize() % _MAX_DIRECTORIES_FOR_TRAVERSAL > 0:
                n_requests_needed += 1
            if n_requests_done + n_requests_needed > _MAX_DIRECTORIES_FOR_TRAVERSAL:
                raise _ListObjectsLimitExceeded()

        return files

    def _detect_objects_with_full_scan(self) -> list[GDriveFile]:
        all_items = self._query("trashed=false")
        connections = defaultdict(list)
        for item in all_items:
            if item["mimeType"] != MIME_TYPE_FOLDER:
                # Don't build useless connections, we're only
                # interested in folder hierarchy
                continue

            parents = item.get("parents")
            if parents is not None:
                assert (
                    len(parents) == 1
                ), "GDrive API returned unexpected number of parents"
                parent_id = parents[0]
                connections[parent_id].append(item["id"])

        reachable_folder_ids = set([self.root])
        queue: Queue[str] = Queue()
        queue.put(self.root)
        while not queue.empty():
            item = queue.get()
            for nested_item in connections[item]:
                if nested_item not in reachable_folder_ids:
                    reachable_folder_ids.add(nested_item)
                    queue.put(nested_item)

        result_items = []
        for item in all_items:
            if item["mimeType"] == MIME_TYPE_FOLDER:
                continue

            parents = item.get("parents")
            if parents is not None:
                item_is_reachable = (
                    parents[0] in reachable_folder_ids or item["id"] == self.root
                )
            else:
                item_is_reachable = item["id"] == self.root

            if item_is_reachable:
                result_items.append(item)

        return result_items

    def _apply_filters(self, files: list[GDriveFile]) -> list[GDriveFile]:
        files = self._filter_by_size(files)
        files = self._filter_by_pattern(files)
        return files

    def _filter_by_pattern(self, files: list[GDriveFile]) -> list[GDriveFile]:
        if self.file_name_pattern is None:
            return files
        elif isinstance(self.file_name_pattern, str):
            return [
                i for i in files if fnmatch.fnmatch(i["name"], self.file_name_pattern)
            ]
        else:
            return [
                i
                for i in files
                if any(
                    fnmatch.fnmatch(i["name"], pattern)
                    for pattern in self.file_name_pattern
                )
            ]

    def _filter_by_size(self, files: list[GDriveFile]) -> list[GDriveFile]:
        if self.object_size_limit is None:
            return files
        result = []
        for file in files:
            name = file["name"]
            is_symlink = "size" not in file
            if is_symlink:
                logging.info(f"Skipping object {name} because it doesn't have size")
                file["status"] = STATUS_SYMLINKS_NOT_SUPPORTED
            else:
                size = int(file["size"])
                if size > self.object_size_limit:
                    logging.info(
                        f"Skipping object {name} because its size "
                        f"{size} exceeds the limit {self.object_size_limit}"
                    )
                    file["status"] = STATUS_SIZE_LIMIT_EXCEEDED
                else:
                    file["status"] = STATUS_DOWNLOADED
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
        is_symlink = file.get("size") is None
        is_too_large = (
            self.object_size_limit is not None
            and int(file.get("size", "0")) > self.object_size_limit
        )
        if is_symlink:
            file["status"] = STATUS_SYMLINKS_NOT_SUPPORTED
            return b""
        if is_too_large:
            file["status"] = STATUS_SIZE_LIMIT_EXCEEDED
            return b""
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
            file["status"] = "download_error"
            return None

    def tree(self) -> _GDriveTree:
        match self.list_objects_strategy:
            case _ListObjectsStrategy.TreeTraversal:
                try:
                    items = self._traverse_objects_with_limit()
                except _ListObjectsLimitExceeded:
                    self.list_objects_strategy = _ListObjectsStrategy.FullScan
                    return self.tree()

            case _ListObjectsStrategy.FullScan:
                items = self._detect_objects_with_full_scan()

            case _ListObjectsStrategy.SingleObjectRequest:
                item = self._get(self.root)
                if item is None:
                    items = []
                elif item["mimeType"] != MIME_TYPE_FOLDER:
                    items = [item]
                else:
                    logging.error(
                        f"The object {self.root} is not expected to be a folder"
                    )
                    self.list_objects_strategy = _ListObjectsStrategy.TreeTraversal
                    return self.tree()

            case _:
                raise ValueError(
                    f"Unknown list objects strategy: {self.list_objects_strategy}"
                )

        items = self._apply_filters(items)
        items = [extend_metadata(file) for file in items]
        return _GDriveTree({file["id"]: file for file in items})


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
    _only_metadata: bool
    _append_metadata: bool
    _object_size_limit: int | None
    _file_name_pattern: list | str | None

    def __init__(
        self,
        *,
        credentials_factory: Callable[[], ServiceCredentials],
        root: str,
        refresh_interval: int,
        mode: str,
        only_metadata: bool,
        with_metadata: bool,
        object_size_limit: int | None,
        file_name_pattern: list | str | None,
        list_objects_strategy: _ListObjectsStrategy | None,
    ) -> None:
        super().__init__(datasource_name="gdrive")
        self._credentials_factory = credentials_factory
        self._refresh_interval = refresh_interval
        self._root = root
        self._mode = mode
        self._only_metadata = only_metadata
        self._append_metadata = with_metadata
        self._object_size_limit = object_size_limit
        self._file_name_pattern = file_name_pattern
        self._list_objects_strategy = list_objects_strategy
        assert mode in ["streaming", "static"]

    @property
    def _session_type(self) -> SessionType:
        return SessionType.UPSERT if self._mode == "streaming" else SessionType.NATIVE

    @property
    def _with_metadata(self) -> bool:
        return self._append_metadata

    @property
    def _deletions_enabled(self) -> bool:
        return self._mode == "streaming"

    def run(self) -> None:
        client = _GDriveClient(
            self._root,
            self._credentials_factory(),
            self._object_size_limit,
            self._file_name_pattern,
            self._list_objects_strategy,
        )
        prev = _GDriveTree({})

        while True:
            try:
                tree = client.tree()
            except HttpError as e:
                logging.error(
                    f"Failed to query GDrive: {e}. Retrying in {self._refresh_interval} seconds...",
                )
            else:
                for file in tree.removed_files(prev):
                    self.remove(file)
                for file in tree.new_and_changed_files(prev):
                    payload = (
                        uuid.uuid4().bytes  # Trigger a change inside UpsertSession
                        if self._only_metadata
                        else client.download(file)
                    )
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
    mode: Literal["streaming", "static"] = "streaming",
    format: Literal["binary", "only_metadata"] = "binary",
    object_size_limit: int | None = None,
    refresh_interval: int = 30,
    service_user_credentials_file: str | PathLike,
    with_metadata: bool = False,
    file_name_pattern: list | str | None = None,
    name: str | None = None,
    max_backlog_size: int | None = None,
    _list_objects_strategy: _ListObjectsStrategy | None = None,
    **kwargs,
) -> pw.Table:
    """Reads a table from a Google Drive directory or file.

    Returns a table containing a binary column ``data`` with the binary contents
    of objects in the specified directory, as well as a dict ``_metadata`` that
    contains metadata corresponding to each object. Metadata is reported only if
    the ``with_metadata`` flag is set, or if the ``"only_metadata"`` format is chosen.

    Note that if you only need to monitor changes in the given directory, you can
    use the ``"only_metadata"`` format, in which case the table will contain only
    metadata, and no time or resources will be spent downloading the objects.

    Args:
        object_id: ``id`` of a directory or file. Directories will be scanned recursively.
        mode: denotes how the engine polls the new data from the source. Currently ``"streaming"``
            and ``"static"`` are supported. If set to ``"streaming"``, it will check for updates, deletions
            and new files every ``refresh_interval`` seconds. ``"static"`` mode will only consider
            the available data and ingest all of it in one commit.
            The default value is ``"streaming"``.
        format: the format of the resulting table. Can be either ``"binary"``, which
            corresponds to a table with a ``data`` column containing the object's contents,
            or ``"only_metadata"``, which corresponds to a table that has only the ``_metadata``
            column with the objects' metadata, without downloading the objects themselves.
        object_size_limit: Maximum size (in bytes) of a file that will be processed by
            this connector or ``None`` if no filtering by size should be made;
        refresh_interval: time in seconds between scans. Applicable if mode is set to ``"streaming"``.
        service_user_credentials_file: Google API service user json file. Please follow the instructions
            provided in the `developer's user guide
            <https://pathway.com/developers/user-guide/connect/connectors/gdrive-connector/#setting-up-google-drive>`_
            to obtain them.
        with_metadata: when set to ``True``, the connector will add an additional column named
            ``_metadata`` to the table. This column will contain file metadata,
            such as: ``id``, ``name``, ``mimeType``, ``parents``, ``modifiedTime``,
            ``thumbnailLink``, ``lastModifyingUser``.
        file_name_pattern: glob pattern (or list of patterns) to be used to filter files based on their names.
            Defaults to ``None`` which doesn't filter anything. Doesn't apply to folder names.
            For example, ``*.pdf`` will only return files that has ``.pdf`` extension.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        max_backlog_size: Limit on the number of entries read from the input source and kept
            in processing at any moment. Reading pauses when the limit is reached and resumes
            as processing of some entries completes. Useful with large sources that
            emit an initial burst of data to avoid memory spikes.

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
            fspath(service_user_credentials_file)
        )

    only_provide_metadata = format == "only_metadata"
    subject = _GDriveSubject(
        credentials_factory=credentials_factory,
        root=object_id,
        refresh_interval=refresh_interval,
        mode=mode,
        only_metadata=only_provide_metadata,
        with_metadata=with_metadata or only_provide_metadata,
        object_size_limit=object_size_limit,
        file_name_pattern=file_name_pattern,
        list_objects_strategy=_list_objects_strategy,
    )

    table = pw.io.python.read(
        subject,
        format="binary",
        name=name,
        max_backlog_size=max_backlog_size,
        _stacklevel=4,
        **kwargs,
    )
    if only_provide_metadata:
        table = table.select(_metadata=table._metadata)

    return table

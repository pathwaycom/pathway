# Copyright © 2024 Pathway

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Literal

from fs.base import FS
from fs.errors import ResourceNotFound as FSResourceNotFound
from fs.walk import Walker

from pathway.internals import api
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import STATIC_MODE_NAME
from pathway.io.python import ConnectorSubject, read as python_connector_read


@dataclass(frozen=True, kw_only=True)
class _PyFilesystemUpdate:
    changed_paths: list[str]
    deleted_paths: list[str]


class _PyFilesystemSubject(ConnectorSubject):

    def __init__(
        self, source, *, path, mode, refresh_interval, with_metadata, **kwargs
    ):
        super().__init__(datasource_name="pyfilesystem", **kwargs)
        self.source = source
        self.path = path
        self.mode = mode
        self.refresh_interval = refresh_interval
        self.with_metadata = with_metadata
        self.stored_modify_times = {}

    def run(self):
        while True:
            start_time = time.time()

            update = self._get_snapshot_update()
            for changed_path in update.changed_paths:
                try:
                    with self.source.open(changed_path) as file:
                        data = file.read()
                except FileNotFoundError:
                    logging.exception(
                        f"Failed to read file from {changed_path}. "
                        "Most likely it was deleted between the change "
                        "tracking and file read"
                    )
                    update.deleted_paths.append(changed_path)
                    continue

                data = data.encode("utf-8")

                provided_metadata = None
                if self.with_metadata:
                    try:
                        metadata = self.source.getinfo(
                            changed_path,
                            namespaces=[
                                "basic",
                                "details",
                                "access",
                            ],
                        )
                        metadata_dict = self._metadata_to_dict(changed_path, metadata)
                        provided_metadata = json.dumps(metadata_dict).encode("utf-8")
                    except FSResourceNotFound:
                        logging.exception(
                            f"Failed to acquire metadata for the object: {changed_path}"
                        )

                self._add(api.ref_scalar(changed_path), data, provided_metadata)
            for deleted_path in update.deleted_paths:
                self._remove(api.ref_scalar(deleted_path), b"")
                self.stored_modify_times.pop(deleted_path)
            self.commit()
            if self.mode == STATIC_MODE_NAME:
                break

            elapsed_time = time.time() - start_time
            if elapsed_time < self.refresh_interval:
                time.sleep(self.refresh_interval - elapsed_time)

    @property
    def _with_metadata(self):
        return self.with_metadata

    @property
    def _session_type(self) -> api.SessionType:
        return api.SessionType.UPSERT

    def _metadata_to_dict(self, path, metadata):
        return {
            "created_at": self._maybe_unix_timestamp(metadata.created),
            "modified_at": self._maybe_unix_timestamp(metadata.modified),
            "accessed_at": self._maybe_unix_timestamp(metadata.accessed),
            "seen_at": int(time.time()),
            "size": metadata.size,
            "owner": metadata.user,
            "name": metadata.name,
            "path": path,
        }

    def _maybe_unix_timestamp(self, dt):
        if dt is None:
            return None
        return int(dt.timestamp())

    def _get_snapshot_update(self):
        changed_paths = []
        deleted_paths = []
        existing_paths = set()

        walker = Walker()
        for path in walker.files(self.source, path=self.path):
            existing_paths.add(path)
            modified_at = self.source.getmodified(path)
            stored_modified_at = self.stored_modify_times.get(path)
            if modified_at != stored_modified_at:
                self.stored_modify_times[path] = modified_at
                changed_paths.append(path)

        for path in self.stored_modify_times:
            if path not in existing_paths:
                deleted_paths.append(path)

        return _PyFilesystemUpdate(
            changed_paths=changed_paths,
            deleted_paths=deleted_paths,
        )


@check_arg_types
@trace_user_frame
def read(
    source: FS,
    *,
    path: str = "",
    refresh_interval: float = 30,
    mode: Literal["streaming", "static"] = "streaming",
    with_metadata: bool = False,
    name: str | None = None,
) -> Table:
    """Reads a table from
    `PyFilesystem <https://docs.pyfilesystem.org/en/latest/introduction.html>_` source.

    It returns a table with a single column ``data`` containing each file in a binary
    format. If the ``with_metadata`` option is specified, it also attaches a column
    ``_metadata`` containing the metadata of the objects read.

    Args:
        source: PyFilesystem source.
        path: Path inside the PyFilesystem source to process. All files within this
            path will be processed recursively. If unspecified, the root of the source is taken.
        mode: denotes how the engine polls the new data from the source. Currently
            "streaming" and "static" are supported. If set to "streaming", it will check for
            updates, deletions, and new files every ``refresh_interval`` seconds. "static" mode will
            only consider the available data and ingest all of it in one commit.
            The default value is "streaming".
        refresh_interval: time in seconds between scans. Applicable if the mode is
            set to "streaming".
        with_metadata: when set to True, the connector will add column
            named ``_metadata`` to the table. This column will contain file metadata, such as:
            ``path``, ``name``, ``owner``, ``created_at``, ``modified_at``, ``accessed_at``,
            ``size``.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.

    Returns:
        The table read.

    Example:

    Suppose that you want to read a file from a ZIP archive ``projects.zip`` with the
    usage of PyFilesystem. To do that, you first need to import the `fs` library or just
    the ``open_fs`` method and to create the data source. It can be done as follows:

    >>> from fs import open_fs
    >>> source = open_fs("zip://projects.zip")  # doctest: +SKIP

    Then you can use the connector as follows:

    >>> import pathway as pw
    ...
    >>> table = pw.io.pyfilesystem.read(source)  # doctest: +SKIP

    This command reads all files in the archive in full. If the data is not supposed to
    be changed, it makes sense to run this read in the static mode. It can be done by
    specifying the ``mode`` parameter:

    >>> table = pw.io.pyfilesystem.read(source, mode="static")  # doctest: +SKIP

    Please note that PyFilesystem offers a great variety of sources that can be
    read. You can refer to the
    `"Index of Filesystems" <https://www.pyfilesystem.org/page/index-of-filesystems/>_`
    web page for the list and the respective documentation.

    For instance, you can also read a dataset from the remote FTP source with this
    connector. It can be done with the usage of ``FTP`` file source with the code as follows:

    >>> source = fs.open_fs('ftp://login:password@ftp.example.com/datasets')  # doctest: +SKIP
    >>> table = pw.io.pyfilesystem.read(source)  # doctest: +SKIP
    """

    subject = _PyFilesystemSubject(
        source=source,
        path=path,
        mode=mode,
        refresh_interval=refresh_interval,
        with_metadata=with_metadata,
    )

    return python_connector_read(
        subject,
        format="binary",
        autocommit_duration_ms=None,
        name=name,
        _stacklevel=5,
    )

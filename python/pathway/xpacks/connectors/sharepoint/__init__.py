# Copyright © 2024 Pathway

from __future__ import annotations

import json
import logging
import time

import pathway as pw
from pathway.internals import api
from pathway.internals.config import _check_entitlements
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    STATIC_MODE_NAME,
    STATUS_DOWNLOADED,
    STATUS_SIZE_LIMIT_EXCEEDED,
)
from pathway.io.python import ConnectorSubject
from pathway.optional_import import optional_imports

with optional_imports("xpack-sharepoint"):
    from office365.sharepoint.client_context import ClientContext

from typing import Any
from urllib.parse import quote, urlparse


class _SharePointEntryMeta:
    def __init__(self, entry):
        self.created_at = int(entry.time_created.timestamp())
        self.modified_at = int(entry.time_last_modified.timestamp())
        self.path = entry.properties["ServerRelativeUrl"]
        self.size = entry.length
        self.seen_at = int(time.time())
        self.status = STATUS_DOWNLOADED
        self.base_url: str | None = None

    def __eq__(self, other):
        if not isinstance(other, _SharePointEntryMeta):
            return False
        return (
            self.created_at == other.created_at
            and self.modified_at == other.modified_at
            and self.path == other.path
            and self.size == other.size
        )

    def as_dict(self):
        return {
            "created_at": self.created_at,
            "modified_at": self.modified_at,
            "path": self.path,
            "size": self.size,
            "seen_at": self.seen_at,
            "status": self.status,
            "url": self.url or "",
        }

    def as_json(self):
        return json.dumps(self.as_dict())

    def update(self, data):
        for key, value in data.items():
            setattr(self, key, value)

    @property
    def url(self) -> str | None:
        base_url = self.base_url
        if base_url:
            encoded_path = quote(self.path)
            url = f"{base_url}{encoded_path}"
            return url
        else:
            return None


class _SharePointUpdate:
    def __init__(self, updated_entries, deleted_entries):
        self.updated_entries = updated_entries
        self.deleted_entries = deleted_entries


class _SharePointScanner:
    def __init__(
        self,
        context: ClientContext,
        root_path: str,
        recursive: bool,
        stored_metadata: dict[str, _SharePointEntryMeta],
        object_size_limit: int | None = None,
        common_metadata: dict[str, Any] = {},
    ):
        self._context = context
        self._root_path = root_path
        self._recursive = recursive
        self._stored_metadata = stored_metadata
        self._object_size_limit = object_size_limit
        self.common_metadata = common_metadata

    def _is_changed(self, metadata):
        return self._stored_metadata.get(metadata.path) != metadata

    def get_snapshot_diff(self):
        root_folder = self._context.web.get_folder_by_server_relative_path(
            self._root_path
        )
        files = root_folder.get_files(self._recursive).execute_query()

        updated_entries = []
        deleted_entries = []
        seen_objects = set()

        for file in files:
            metadata = _SharePointEntryMeta(file)
            metadata.update(self.common_metadata)
            size_limit_exceeded = (
                self._object_size_limit is not None
                and metadata.size > self._object_size_limit
            )
            if size_limit_exceeded:
                metadata.status = STATUS_SIZE_LIMIT_EXCEEDED
                logging.info(
                    f"Skipping object {metadata.as_dict()} because its size "
                    f"{metadata.size} exceeds the limit {self._object_size_limit}"
                )
            if self._is_changed(metadata):
                if size_limit_exceeded:
                    content = b""
                else:
                    content = file.get_content().execute_query().value
                updated_entries.append((content, metadata))
                self._stored_metadata[metadata.path] = metadata
            seen_objects.add(metadata.path)

        for path in self._stored_metadata:
            if path not in seen_objects:
                deleted_entries.append(path)

        for path in deleted_entries:
            self._stored_metadata.pop(path)

        return _SharePointUpdate(updated_entries, deleted_entries)


class _SharePointSubject(ConnectorSubject):
    def __init__(
        self,
        context_wrapper,
        root_path,
        refresh_interval,
        mode,
        with_metadata,
        recursive,
        object_size_limit,
        max_failed_attempts_in_row,
    ):
        _check_entitlements("xpack-sharepoint")
        super().__init__(datasource_name="sharepoint")
        self._context_wrapper = context_wrapper
        self._root_path = root_path
        self._refresh_interval = refresh_interval
        self._mode = mode
        self._append_metadata = with_metadata
        self._recursive = recursive
        self._object_size_limit = object_size_limit
        self._stored_metadata = {}
        self._max_failed_attempts_in_row = max_failed_attempts_in_row

    @property
    def _session_type(self) -> api.SessionType:
        return (
            api.SessionType.NATIVE
            if self._mode == STATIC_MODE_NAME
            else api.SessionType.UPSERT
        )

    @property
    def _with_metadata(self) -> bool:
        return self._append_metadata

    @property
    def _context(self):
        return self._context_wrapper.context

    def run(self) -> None:
        n_failed_attempts_in_row = 0
        while True:
            try:
                _url = urlparse(self._context_wrapper._url)
                scanner = _SharePointScanner(
                    self._context,
                    self._root_path,
                    self._recursive,
                    self._stored_metadata,
                    self._object_size_limit,
                    common_metadata={"base_url": f"{_url.scheme}://{_url.netloc}"},
                )
                diff = scanner.get_snapshot_diff()
                n_failed_attempts_in_row = 0
            except Exception as e:
                n_failed_attempts_in_row += 1
                if n_failed_attempts_in_row == self._max_failed_attempts_in_row:
                    raise
                logging.error(
                    f"Failed to get snapshot diff: {e}. Retrying in {self._refresh_interval} seconds..."
                )
                time.sleep(self._refresh_interval)
                continue

            for deleted_path in diff.deleted_entries:
                self.remove(deleted_path)
            for payload, metadata in diff.updated_entries:
                self.upsert(metadata, payload)

            if self._mode == STATIC_MODE_NAME:
                break

            time.sleep(self._refresh_interval)

    def upsert(self, metadata: _SharePointEntryMeta, payload: bytes):
        self._add(
            api.ref_scalar(metadata.path),
            payload,
            metadata.as_json().encode(encoding="utf-8"),
        )

    def remove(self, path: str):
        self._remove(api.ref_scalar(path), b"")


class _ContextWrapper:
    def __init__(self, url, tenant, client_id, thumbprint, cert_path):
        self._url = url
        self._tenant = tenant
        self._client_id = client_id
        self._thumbprint = thumbprint
        self._cert_path = cert_path

    @property
    def context(self):
        context = ClientContext(self._url).with_client_certificate(
            tenant=self._tenant,
            client_id=self._client_id,
            thumbprint=self._thumbprint,
            cert_path=self._cert_path,
        )
        current_web = context.web
        context.load(current_web)
        context.execute_query()
        return context


@trace_user_frame
def read(
    url: str,
    *,
    tenant: str,
    client_id: str,
    cert_path: str,
    thumbprint: str,
    root_path: str,
    mode: str = "streaming",
    recursive: bool = True,
    object_size_limit: int | None = None,
    with_metadata: bool = False,
    refresh_interval: int = 30,
    max_failed_attempts_in_row: int | None = 8,
) -> Table:
    """Reads a table from a directory or a file in Microsoft SharePoint site.
    Requires a valid Pathway Scale license key.

    It will return a table with single column `data` containing each file in a binary format.

    Args:
        url: URL of the SharePoint site including the path to the site. For example: \
``https://company.sharepoint.com/sites/MySite``;
        tenant: ID of SharePoint tenant. It is normally a GUID;
        client_id: ClientID of the SharePoint application that has the required grants \
and will be used to access the data;
        cert_path: Path to the certificate, normally .pem-file, added to the application\
specified above and used to authenticate;
        thumbprint: Thumbprint for the specified certificate;
        root_path: The path for a directory or a file within the SharePoint space to be\
read;
        mode: Denotes how the engine polls the new data from the source. Currently \
"streaming" and "static" are supported. If set to "streaming", it will check for \
updates, deletions and new files every `refresh_interval` seconds. "static" mode will \
only consider the available data and ingest all of it in one commit. The default value \
is "streaming";
        recursive: If set to True, the connector will scan the nested directories. \
Otherwise it will only process files that are placed in the specified directory;
        object_size_limit: Maximum size (in bytes) of a file that will be processed by \
this connector or `None` if no filtering by size should be made;
        with_metadata: when set to True, the connector will add an additional column \
named `_metadata` to the table. This column will contain file metadata, such as:  \
`path`, `modified_at`, `created_at`. The creation and modification times will be given \
as UNIX timestamps;
        refresh_interval: Time in seconds between scans. Applicable if mode is set to\
'streaming'.
        max_failed_attempts_in_row: The maximum number of consecutive read errors before\
the connector terminates with an error. If set to ``None``, the connector tries to read\
data indefinitely, regardless of possible errors in the provided credentials.

    Returns:
        The table read.

    Example:

    Let's consider that there is a dataset stored in SharePoint site Datasets. Below we \
give an example for reading this dataset in the steaming mode. Please note that you can\
use this example for the reference of how the parameters should look:


    >>> t = pw.xpacks.connectors.sharepoint.read(  # doctest: +SKIP
    ...     url="https://company.sharepoint.com/sites/Datasets",
    ...     tenant="c2efaf1f-8add-4334-b1ca-32776acb61ea",
    ...     client_id="f521a53a-0b36-4f47-8ef7-60dc07587eb2",
    ...     cert_path="certificate.pem",
    ...     thumbprint="33C1B9D17115E848B1E956E54EECAF6E77AB1B35",
    ...     root_path="Shared Documents/Data",
    ... )

    In the example above we also consider that this dataset is located by the path \
`Shared Documents/Data`. This code will also recursively scan the subdirectories of the\
given directory.

    We can change it a little. Let's suppose that we need to take the dataset from the \
directory `Datasets/Animals/2023` and not take the nested subdirectories into consideration. \
That leads us to the following snippet:

    >>> t = pw.xpacks.connectors.sharepoint.read(  # doctest: +SKIP
    ...     url="https://company.sharepoint.com/sites/Datasets",
    ...     tenant="c2efaf1f-8add-4334-b1ca-32776acb61ea",
    ...     client_id="f521a53a-0b36-4f47-8ef7-60dc07587eb2",
    ...     cert_path="certificate.pem",
    ...     thumbprint="33C1B9D17115E848B1E956E54EECAF6E77AB1B35",
    ...     root_path="Datasets/Animals/2023",
    ...     recursive=False,
    ... )

    SharePoint sites are often used with the subsites. Pathway supports the data reads \
from the subsites as well. To read the data from the subsite, you need to specify its' \
URL in the `url` parameter. For example, if you read the dataset from `vendor` subspace, \
you can configure the connector this way:

    >>> t = pw.xpacks.connectors.sharepoint.read(  # doctest: +SKIP
    ...     url="https://company.sharepoint.com/sites/Datasets/vendor",
    ...     tenant="c2efaf1f-8add-4334-b1ca-32776acb61ea",
    ...     client_id="f521a53a-0b36-4f47-8ef7-60dc07587eb2",
    ...     cert_path="certificate.pem",
    ...     thumbprint="33C1B9D17115E848B1E956E54EECAF6E77AB1B35",
    ...     root_path="Datasets/Animals/2023",
    ...     recursive=False,
    ... )
    """
    context_wrapper = _ContextWrapper(
        url=url,
        tenant=tenant,
        client_id=client_id,
        thumbprint=thumbprint,
        cert_path=cert_path,
    )

    subject = _SharePointSubject(
        context_wrapper=context_wrapper,
        root_path=root_path,
        refresh_interval=refresh_interval,
        mode=mode,
        with_metadata=with_metadata,
        recursive=recursive,
        object_size_limit=object_size_limit,
        max_failed_attempts_in_row=max_failed_attempts_in_row,
    )

    return pw.io.python.read(subject, format="binary")

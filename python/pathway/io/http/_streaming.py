# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pathway.io.python import ConnectorSubject

from ._common import Sender


class HttpStreamingSubject(ConnectorSubject):
    _url: str
    _headers: dict[str, str] | None
    _sender: Sender
    _delimiter: str | bytes | None
    _response_mapper: Callable[[str | bytes], bytes] | None

    def __init__(
        self,
        url: str,
        *,
        sender: Sender,
        payload: Any | None = None,
        headers: dict[str, str] | None = None,
        delimiter: str | bytes | None = None,
        response_mapper: Callable[[str | bytes], bytes] | None = None,
    ) -> None:
        super().__init__(datasource_name="http")
        self._url = url
        self._headers = headers
        self._sender = sender
        self._payload = payload
        self._delimiter = delimiter
        self._response_mapper = response_mapper

    def run(self):
        response = self._sender.send(
            url=self._url, headers=self._headers, data=self._payload, stream=True
        )
        for line in response.iter_lines(delimiter=self._delimiter):
            if self._response_mapper:
                line = self._response_mapper(line)
            self.next_bytes(line)

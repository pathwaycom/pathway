# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from pathway.io.python import ConnectorSubject

from ._common import Sender


class HttpStreamingSubject(ConnectorSubject):
    _url: str
    _headers: Optional[Dict[str, str]]
    _sender: Sender
    _delimiter: Optional[str | bytes]
    _response_mapper: Optional[Callable[[str | bytes], bytes]]

    def __init__(
        self,
        url: str,
        *,
        sender: Sender,
        payload: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        delimiter: Optional[str | bytes] = None,
        response_mapper: Optional[Callable[[str | bytes], bytes]] = None,
    ) -> None:
        super().__init__()
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

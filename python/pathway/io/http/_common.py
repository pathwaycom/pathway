# Copyright © 2024 Pathway

import json
import random
import time
from typing import Any

import requests

import pathway as pw


class RetryPolicy:
    """Class representing policy of delays or backoffs for the retries."""

    def __init__(self, first_delay_ms: int, backoff_factor: float, jitter_ms: int):
        self._next_retry_duration = first_delay_ms * 1e-3
        self._backoff_factor = backoff_factor
        self._jitter = jitter_ms * 1e-3

    @classmethod
    def default(cls):
        return cls(
            first_delay_ms=1000,
            backoff_factor=1.5,
            jitter_ms=300,
        )

    def wait_duration_before_retry(self):
        result = self._next_retry_duration

        self._next_retry_duration *= self._backoff_factor
        self._next_retry_duration += random.random() * self._jitter

        return result


class Sender:
    def __init__(
        self,
        request_method: str,
        n_retries: int,
        retry_policy: RetryPolicy,
        connect_timeout_ms: int | None,
        request_timeout_ms: int | None,
        allow_redirects: bool,
        retry_codes: tuple | None,
    ) -> None:
        self._request_method = request_method
        self._n_retries = n_retries
        self._retry_policy = retry_policy
        self._timeout = self.format_timeouts_tuple(
            connect_timeout_ms, request_timeout_ms
        )
        self._allow_redirects = allow_redirects
        self._retry_codes = retry_codes or ()

    def send(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        data: Any | None = None,
        stream: bool = False,
    ) -> requests.Response:
        headers = headers or {}
        if "User-Agent" not in headers:
            headers["User-Agent"] = f"pathway/{pw.__version__}"
        retry_policy = self._retry_policy
        for n_attempt in range(0, self._n_retries + 1):
            try:
                response = requests.request(
                    self._request_method,
                    url,
                    timeout=self._timeout,
                    headers=headers,
                    data=data,
                    allow_redirects=self._allow_redirects,
                    stream=stream,
                )
                if response.ok or response.status_code not in self._retry_codes:
                    break
            except requests.exceptions.ConnectTimeout:
                if n_attempt == self._n_retries:
                    raise

            sleep_duration = retry_policy.wait_duration_before_retry()
            time.sleep(sleep_duration)

        return response

    @staticmethod
    def format_timeouts_tuple(
        connect_timeout_ms: int | None, request_timeout_ms: int | None
    ):
        connect_timeout = connect_timeout_ms * 1e-3 if connect_timeout_ms else None
        request_timeout = request_timeout_ms * 1e-3 if request_timeout_ms else None
        return (connect_timeout, request_timeout)


def unescape(message: str, row: dict[str, Any], time: int, is_addition: bool):
    message = message.replace("{table.time}", str(time))
    message = message.replace("{table.diff}", "1" if is_addition else "-1")
    for k, v in row.items():
        wildcard_to_replace = "{table." + k + "}"
        message = message.replace(wildcard_to_replace, str(v))
    return message


def prepare_request_payload(
    row: dict[str, Any],
    time: int,
    is_addition: bool,
    req_format: str,
    text: str | None,
):
    if req_format == "json":
        row["time"] = time
        row["diff"] = 1 if is_addition else -1
        return json.dumps(row)
    elif req_format == "custom":
        return unescape(text or "", row, time, is_addition)
    else:
        raise ValueError(f"Unknown payload format: {req_format}")

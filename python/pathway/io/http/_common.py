# Copyright © 2026 Pathway

from __future__ import annotations

import copy
import json
import random
import threading
import time
import weakref
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import requests

import pathway as pw


class RetryPolicy:
    """
    Represents a retry policy defining delays or backoff strategies for retries.

    Args:
        first_delay_ms: Duration of the initial retry delay, in milliseconds.
        backoff_factor: Factor by which the retry delay increases after each attempt.
        jitter_ms: Maximum random jitter (in milliseconds) to add to the scaled delay.

    Returns:
        A retry policy object.
    """

    def __init__(self, first_delay_ms: int, backoff_factor: float, jitter_ms: int):
        self._next_retry_duration = first_delay_ms * 1e-3
        self._backoff_factor = backoff_factor
        self._jitter = jitter_ms * 1e-3

    @classmethod
    def default(cls):
        """
        Constructs the default retry settings:

        - The initial delay between calls is 1 second.
        - The backoff factor is 1.5, meaning each subsequent retry will wait at
          least 1.5 times longer than the previous one.
        - The maximum jitter added to the delay is 300 milliseconds.
        """
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
        # Keep requests.Session's mutable state local to the sending thread.
        self._local = threading.local()
        self._sessions: set[requests.Session] = set()
        self._session_lock = threading.Lock()
        weakref.finalize(self, self._close_sessions, self._sessions)

    def _get_session(self) -> requests.Session:
        import requests

        with self._session_lock:
            session = getattr(self._local, "session", None)
            if session not in self._sessions:
                session = requests.Session()
                self._local.session = session
                self._sessions.add(session)
            return session

    @staticmethod
    def _close_sessions(sessions: set[requests.Session]) -> None:
        while sessions:
            sessions.pop().close()

    def close(self) -> None:
        # on_end runs once, after all workers have finished their callbacks.
        with self._session_lock:
            self._close_sessions(self._sessions)

    def send(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        data: Any | None = None,
        stream: bool = False,
    ) -> requests.Response:
        import requests

        headers = headers or {}
        if "User-Agent" not in headers:
            headers["User-Agent"] = f"pathway/{pw.__version__}"
        retry_policy = copy.copy(self._retry_policy)
        session = self._get_session()
        try:
            for n_attempt in range(0, self._n_retries + 1):
                try:
                    response = session.request(
                        self._request_method,
                        url,
                        timeout=self._timeout,
                        headers=headers,
                        data=data,
                        allow_redirects=self._allow_redirects,
                        stream=stream,
                    )
                except requests.exceptions.ConnectTimeout:
                    if n_attempt == self._n_retries:
                        raise
                else:
                    if (
                        response.ok
                        or response.status_code not in self._retry_codes
                        or n_attempt == self._n_retries
                    ):
                        break
                    # A retried streaming response may have an unread body.
                    response.close()
                finally:
                    # Preserve requests.request's cookie isolation between
                    # attempts, including retries. Redirects within an
                    # attempt still use their response cookies normally.
                    session.cookies.clear()

                sleep_duration = retry_policy.wait_duration_before_retry()
                time.sleep(sleep_duration)

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                # A failed streaming response can still have an unread body.
                response.close()
                raise
        except BaseException:
            with self._session_lock:
                self._sessions.discard(session)
            session.close()
            raise

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

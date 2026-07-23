# Copyright © 2026 Pathway

from unittest.mock import MagicMock

import pytest

pytest.importorskip("office365")

import requests  # noqa: E402
from office365.runtime.client_request_exception import (  # noqa: E402
    ClientRequestException,
)
from office365.runtime.client_runtime_context import ClientRuntimeContext  # noqa: E402

from pathway.xpacks.connectors.sharepoint import (  # noqa: E402
    QUERY_MAX_ATTEMPTS,
    _execute_with_retries,
)


def connection_error():
    return requests.exceptions.ConnectionError("Network is unreachable")


def http_error():
    response = requests.Response()
    response.status_code = 503
    response._content = b"Service Unavailable"
    response.headers["Content-Type"] = "text/plain"
    return ClientRequestException("Service Unavailable", response=response)


TRANSIENT_ERRORS = [
    pytest.param(connection_error, id="connection_error"),
    pytest.param(http_error, id="http_error"),
]


class FakeContext(ClientRuntimeContext):
    """A context whose requests fail the first ``n_failures`` times."""

    def __init__(self, n_failures, error):
        super().__init__()
        self._n_failures = n_failures
        self._error = error
        self.attempts = 0

    def pending_request(self):
        def execute_query(query):
            self.attempts += 1
            if self.attempts <= self._n_failures:
                raise self._error

        request = MagicMock()
        request.execute_query.side_effect = execute_query
        return request


@pytest.fixture
def no_retry_delay(monkeypatch):
    monkeypatch.setattr(
        "office365.runtime.client_runtime_context.sleep", lambda _: None
    )


def query_context(n_failures, error):
    context = FakeContext(n_failures, error)
    context.add_query(MagicMock())
    return context


@pytest.mark.parametrize("make_error", TRANSIENT_ERRORS)
def test_transient_failures_are_retried(make_error, no_retry_delay):
    # A connection-level failure is as transient as an HTTP one: the site is reachable
    # again a few seconds later, so the query has to be retried rather than propagated
    # from the very first attempt.
    context = query_context(n_failures=QUERY_MAX_ATTEMPTS - 1, error=make_error())

    assert _execute_with_retries(context) is context
    assert context.attempts == QUERY_MAX_ATTEMPTS


@pytest.mark.parametrize("make_error", TRANSIENT_ERRORS)
def test_exhausted_retries_report_the_failure(make_error, no_retry_delay):
    # ``execute_query_retry`` returns normally once it runs out of attempts, which
    # would leave the connector with a silently empty listing. The failure has to
    # reach the caller instead.
    error = make_error()
    context = query_context(n_failures=QUERY_MAX_ATTEMPTS, error=error)

    with pytest.raises(type(error)):
        _execute_with_retries(context)
    assert context.attempts == QUERY_MAX_ATTEMPTS

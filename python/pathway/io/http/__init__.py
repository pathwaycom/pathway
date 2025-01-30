# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pathway.internals.api import Pointer
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io import python

from .._subscribe import subscribe
from ._common import RetryPolicy, Sender, prepare_request_payload, unescape
from ._server import (
    EndpointDocumentation,
    EndpointExamples,
    PathwayWebserver,
    rest_connector,
)
from ._streaming import HttpStreamingSubject


@check_arg_types
@trace_user_frame
def read(
    url: str,
    *,
    schema: type[Schema] | None = None,
    method: str = "GET",
    payload: Any | None = None,
    headers: dict[str, str] | None = None,
    response_mapper: Callable[[str | bytes], bytes] | None = None,
    format: str = "json",
    delimiter: str | bytes | None = None,
    n_retries: int = 0,
    retry_policy: RetryPolicy = RetryPolicy.default(),
    connect_timeout_ms: int | None = None,
    request_timeout_ms: int | None = None,
    allow_redirects: bool = True,
    retry_codes: tuple | None = (429, 500, 502, 503, 504),
    autocommit_duration_ms: int = 10000,
    debug_data=None,
    name: str | None = None,
):
    """Reads a table from an HTTP stream.

    Args:
        url: the full URL of streaming endpoint to fetch data from.
        schema: Schema of the resulting table.
        method: request method for streaming. It should be one of
          `HTTP request methods <https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods>`_.
        payload: data to be send in the body of the request.
        headers: request headers in the form of dict. Wildcards are allowed both, in
          keys and in values.
        response_mapper: in case a response needs to be processed, this method can be
          provided. It will be applied to each slice of a stream.
        format: format of the data, "json" or "raw". In case of a "raw" format,
          table with single "data" column will be produced. For "json" format, bytes
          encoded json is expected.
        delimiter: delimiter used to split stream into messages.
        n_retries: how many times to retry the failed request.
        retry_policy: policy of delays or backoffs for the retries.
        connect_timeout_ms: connection timeout, specified in milliseconds. In case
          it's None, no restrictions on connection duration will be applied.
        request_timeout_ms: request timeout, specified in milliseconds. In case
          it's None, no restrictions on request duration will be applied.
        allow_redirects: whether to allow redirects.
        retry_codes: HTTP status codes that trigger retries.
        content_type: content type of the data to send. In case the chosen format is
          JSON, it will be defaulted to "application/json".
        autocommit_duration_ms: the maximum time between two commits. Every
          autocommit_duration_ms milliseconds, the updates received by the connector are
          committed and pushed into Pathway's computation graph.
        debug_data: static data replacing original one when debug mode is active.
        name: A unique name for the connector. If provided, this name will be used in
          logs and monitoring dashboards. Additionally, if persistence is enabled, it
          will be used as the name for the snapshot that stores the connector's progress.

    Examples:

    Raw format:

    >>> import os
    >>> import pathway as pw
    >>> table = pw.io.http.read(
    ...   "https://localhost:8000/stream",
    ...   method="GET",
    ...   headers={"Authorization": f"Bearer {os.environ['BEARER_TOKEN']}"},
    ...   format="raw",
    ... )

    JSON with response mapper:

    Input can be adjusted using a mapping function that will be applied to each
    slice of a stream. The mapping function should return bytes.

    >>> def mapper(msg: bytes) -> bytes:
    ...   result = json.loads(msg.decode())
    ...   return json.dumps({"key": result["id"], "text": result["data"]}).encode()
    >>> class InputSchema(pw.Schema):
    ...  key: int
    ...  text: str
    >>> t = pw.io.http.read(
    ...   "https://localhost:8000/stream",
    ...   method="GET",
    ...   headers={"Authorization": f"Bearer {os.environ['BEARER_TOKEN']}"},
    ...   schema=InputSchema,
    ...   response_mapper=mapper
    ... )
    """

    sender = Sender(
        request_method=method,
        n_retries=n_retries,
        retry_policy=retry_policy,
        connect_timeout_ms=connect_timeout_ms,
        request_timeout_ms=request_timeout_ms,
        allow_redirects=allow_redirects,
        retry_codes=retry_codes,
    )

    return python.read(
        HttpStreamingSubject(
            url=url,
            sender=sender,
            payload=payload,
            headers=headers,
            response_mapper=response_mapper,
            delimiter=delimiter,
        ),
        schema=schema,
        format=format,
        autocommit_duration_ms=autocommit_duration_ms,
        debug_data=debug_data,
        name=name,
        _stacklevel=5,
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    url: str,
    *,
    method: str = "POST",
    format: str = "json",
    request_payload_template: str | None = None,
    n_retries: int = 0,
    retry_policy: RetryPolicy = RetryPolicy.default(),
    connect_timeout_ms: int | None = None,
    request_timeout_ms: int | None = None,
    content_type: str | None = None,
    headers: dict[str, str] | None = None,
    allow_redirects: bool = True,
    retry_codes: tuple | None = (429, 500, 502, 503, 504),
    name: str | None = None,
) -> None:
    """Sends the stream of updates from the table to the specified HTTP API.

    Args:
        table: table to be tracked.
        method: request method for streaming. It should be one of
          `HTTP request methods <https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods>`_.
        url: the full URL of the endpoint to push data into. Can contain wildcards.
        format: the payload format, one of {"json", "custom"}. If "json" is
          specified, the plain JSON will be formed and sent. Otherwise, the contents of the
          field request_payload_template will be used.
        request_payload_template: the template to format and send in case "custom" was
          specified in the format field. Can include wildcards.
        n_retries: how many times to retry the failed request.
        retry_policy: policy of delays or backoffs for the retries.
        connect_timeout_ms: connection timeout, specified in milliseconds. In case
          it's None, no restrictions on connection duration will be applied.
        request_timeout_ms: request timeout, specified in milliseconds. In case it's
          None, no restrictions on request duration will be applied.
        allow_redirects: Whether to allow redirects.
        retry_codes: HTTP status codes that trigger retries.
        content_type: content type of the data to send. In case the chosen format is
          JSON, it will be defaulted to "application/json".
        headers: request headers in the form of dict. Wildcards are allowed both, in
          keys and in values.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.

    Wildcards:

    Wildcards are the proposed way to customize the HTTP requests composed. The
    engine will replace all entries of ``{table.<column_name>}`` with a value from the
    column ``<column_name>`` in the row sent. This wildcard resolving will happen in url,
    request payload template and headers.

    Examples:

    For the sake of demonstration, let's try different ways to send the stream of changes
    on a table ``pets``, containing data about pets and their owners. The table contains
    just two columns: the pet and the owner's name.

    >>> import pathway as pw
    >>> pets = pw.debug.table_from_markdown("owner pet \\n Alice dog \\n Bob cat \\n Alice cat")

    Consider that there is a need to send the stream of changes on such table to the
    external API endpoint (let's pick some exemplary URL for the sake of demonstration).

    To keep things simple, we can suppose that this API accepts flat JSON objects, which
    are sent in POST requests. Then, the communication can be done with a simple code
    snippet:

    >>> pw.io.http.write(pets, "http://www.example.com/api/event")

    Now let's do something more custom. Suppose that the API endpoint requires us to
    communicate via PUT method and to pass the values as CGI-parameters. In this case,
    wildcards are the way to go:

    >>> pw.io.http.write(
    ...     pets,
    ...     "http://www.example.com/api/event?owner={table.owner}&pet={table.pet}",
    ...     method="PUT"
    ... )

    A custom payload can also be formed from the outside. What if the endpoint requires
    the data in tskv format in request body?

    First of all, let's form a template for the message body:

    >>> message_template_tokens = [
    ...     "owner={table.owner}",
    ...     "pet={table.pet}",
    ...     "time={table.time}",
    ...     "diff={table.diff}",
    ... ]
    >>> message_template = "\\t".join(message_template_tokens)

    Now, we can use this template and the custom format, this way:

    >>> pw.io.http.write(
    ...     pets,
    ...     "http://www.example.com/api/event",
    ...     method="POST",
    ...     format="custom",
    ...     request_payload_template=message_template
    ... )
    """

    sender = Sender(
        request_method=method,
        n_retries=n_retries,
        retry_policy=retry_policy,
        connect_timeout_ms=connect_timeout_ms,
        request_timeout_ms=request_timeout_ms,
        allow_redirects=allow_redirects,
        retry_codes=retry_codes,
    )

    def callback(key: Pointer, row: dict[str, Any], time: int, is_addition: bool):
        payload = prepare_request_payload(
            row, time, is_addition, format, request_payload_template
        )

        patched_headers = {}
        if headers:
            for k, v in headers.items():
                unescaped_key = unescape(k, row, time, is_addition)
                unescaped_value = unescape(v, row, time, is_addition)
                patched_headers[unescaped_key] = unescaped_value

        if content_type:
            patched_headers["Content-Type"] = content_type
        elif format == "json":
            patched_headers["Content-Type"] = "application/json"

        sender.send(
            url=unescape(url, row, time, is_addition),
            headers=patched_headers,
            data=payload,
        )

    subscribe(table, callback, name=name)


__all__ = [
    "read",
    "write",
    "RetryPolicy",
    "rest_connector",
    "PathwayWebserver",
    "EndpointDocumentation",
    "EndpointExamples",
]

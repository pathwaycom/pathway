# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Iterable

from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame

from ..http import RetryPolicy, write as http_write


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    endpoint: str,
    n_retries: int = 0,
    retry_policy: RetryPolicy = RetryPolicy.default(),
    connect_timeout_ms: int | None = None,
    request_timeout_ms: int | None = None,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Sends the stream of updates from the table to
    `HTTP input <https://www.elastic.co/guide/en/logstash/current/plugins-inputs-http.html>`
    of Logstash. The data is sent in the format of flat JSON objects, with two extra
    fields for time and diff.

    Args:
        table: table to be tracked;
        endpoint: Logstash endpoint, accepting entries;
        n_retries: number of retries in case of failure;
        retry_policy: policy of delays or backoffs for the retries;
        connect_timeout_ms: connection timeout, specified in milliseconds. In cas it's None,
            no restrictions on connection duration will be applied;
        request_timeout_ms: request timeout, specified in milliseconds. In case it's None,
            no restrictions on request duration will be applied.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Example:

    Suppose that we need to send the stream of updates to locally installed Logstash.
    For example, you can use `docker-elk <https://github.com/deviantony/docker-elk>`
    repository in order to get the ELK stack up and running at your local machine in a
    few minutes.

    If Logstash stack is installed, you need to configure the input pipeline. The
    simplest possible way to do this, is to add the following lines in the input plugins
    list:

    .. code-block:: text

        http {
            port => 8012
        }

    The port is specified for the sake of example and can be changed. Further, we will
    use 8012 for clarity.

    Now, with the pipeline configured, you can stream the changed into Logstash as
    simple as:

        >>> pw.io.logstash.write(table, "http://localhost:8012")  # doctest: +SKIP
    """

    http_write(
        table,
        url=endpoint,
        n_retries=n_retries,
        retry_policy=retry_policy,
        connect_timeout_ms=connect_timeout_ms,
        request_timeout_ms=request_timeout_ms,
        name=name,
        sort_by=sort_by,
    )

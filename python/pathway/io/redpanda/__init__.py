# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Iterable

from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io import kafka
from pathway.io._utils import check_deprecated_kwargs


@check_arg_types
@trace_user_frame
def read(
    rdkafka_settings: dict,
    topic: str | list[str] | None = None,
    *,
    schema: type[Schema] | None = None,
    format: str = "raw",
    debug_data=None,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    parallel_readers: int | None = None,
    name: str | None = None,
    **kwargs,
) -> Table:
    """Reads table from a set of topics in Redpanda.
    There are three formats currently supported: "raw", "csv", and "json".

    Args:
        rdkafka_settings: Connection settings in the format of
            `librdkafka <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_.
        topic: Name of topic in Redpanda from which the data should be read.
        schema: Schema of the resulting table.
        format: format of the input data, "raw", "csv", or "json"
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms:the maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        json_field_paths: If the format is JSON, this field allows to map field names
            into path in the field. For the field which require such mapping, it should be
            given in the format `<field_name>: <path to be mapped>`, where the path to
            be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        parallel_readers: number of copies of the reader to work in parallel. In case
            the number is not specified, min{pathway_threads, total number of partitions}
            will be taken. This number also can't be greater than the number of Pathway
            engine threads, and will be reduced to the number of engine threads, if it
            exceeds.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.

    Returns:
        Table: The table read.

    When using the format "raw", the connector will produce a single-column table:
    all the data is saved into a column named `data`.
    For other formats, the argument value_column is required and defines the columns.

    Example:

    Consider a simple instance of Redpanda without authentication. Settings for rdkafka
    will look as follows:

    >>> import os
    >>> rdkafka_settings = {
    ...    "bootstrap.servers": "localhost:9092",
    ...    "security.protocol": "plaintext",
    ...    "group.id": "$GROUP_NAME",
    ...    "session.timeout.ms": "60000"
    ... }

    To connect to the topic "animals" and accept messages, the connector must be used
    as follows, depending on the format:

    Raw version:

    >>> import pathway as pw
    >>> t = pw.io.redpanda.read(
    ...    rdkafka_settings,
    ...    topic="animals",
    ...    format="raw",
    ... )

    All the data will be accessible in the column data.

    CSV version:

    >>> import pathway as pw
    >>>
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>>
    >>> t = pw.io.redpanda.read(
    ...     rdkafka_settings,
    ...     topic="animals",
    ...     format="csv",
    ...     schema=InputSchema,
    ... )

    In case of CSV format, the first message must be the header:

    .. code-block:: csv

        owner,pet

    Then, simple data rows are expected. For example:

    .. code-block:: csv

        Alice,cat
        Bob,dog

    This way, you get a table which looks as follows:

    >>> pw.debug.compute_and_print(t, include_id=False)  # doctest: +SKIP
    owner pet
    Alice cat
      Bob dog


    JSON version:

    >>> import pathway as pw
    >>> t = pw.io.redpanda.read(
    ...     rdkafka_settings,
    ...     topic="animals",
    ...     format="json",
    ...     schema=InputSchema,
    ... )

    For the JSON connector, you can send these two messages:

    .. code-block:: json

        {"owner": "Alice", "pet": "cat"}
        {"owner": "Bob", "pet": "dog"}

    This way, you get a table which looks as follows:

    >>> pw.debug.compute_and_print(t, include_id=False)  # doctest: +SKIP
    owner pet
    Alice cat
      Bob dog

    Now consider that the data about pets come in a more sophisticated way. For instance
    you have an owner, kind and name of an animal, along with some physical measurements.

    The JSON payload in this case may look as follows:

    .. code-block:: json

        {
            "name": "Jack",
            "pet": {
                "animal": "cat",
                "name": "Bob",
                "measurements": [100, 200, 300]
            }
        }

    Suppose you need to extract a name of the pet and the height, which is the 2nd
    (1-based) or the 1st (0-based) element in the array of measurements. Then, you
    use JSON Pointer and do a connector, which gets the data as follows:


    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...    pet_name: str
    ...    pet_height: int
    >>> t = pw.io.redpanda.read(
    ...    rdkafka_settings,
    ...    topic="animals",
    ...    format="json",
    ...    schema=InputSchema,
    ...    json_field_paths={
    ...        "pet_name": "/pet/name",
    ...        "pet_height": "/pet/measurements/1"
    ...    },
    ... )
    """

    check_deprecated_kwargs(kwargs, ["topic_names"], stacklevel=5)

    return kafka.read(
        rdkafka_settings=rdkafka_settings,
        topic=topic,
        schema=schema,
        format=format,
        debug_data=debug_data,
        autocommit_duration_ms=autocommit_duration_ms,
        json_field_paths=json_field_paths,
        parallel_readers=parallel_readers,
        name=name,
        _stacklevel=5,
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    rdkafka_settings: dict,
    topic_name: str,
    *,
    format: str = "json",
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
    **kwargs,
) -> None:
    """Write a table to a given topic on a Redpanda instance.

    Args:
        table: the table to output.
        rdkafka_settings: Connection settings in the format of \
`librdkafka <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_.
        topic_name: name of topic in Redpanda to which the data should be sent.
        format: format of the input data, only "json" is currently supported.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Limitations:

    For future proofing, the format is configurable, but (for now) only JSON is available.

    Example:

    Consider there is a queue in Redpanda, running locally on port 9092. Our queue can
    use SASL-SSL authentication over a SCRAM-SHA-256 mechanism. You can set up a queue
    with similar parameters in `Upstash <https://upstash.com/>`_. Settings for rdkafka
    will look as follows:

    >>> import os
    >>> rdkafka_settings = {
    ...    "bootstrap.servers": "localhost:9092",
    ...    "security.protocol": "sasl_ssl",
    ...    "sasl.mechanism": "SCRAM-SHA-256",
    ...    "sasl.username": os.environ["KAFKA_USERNAME"],
    ...    "sasl.password": os.environ["KAFKA_PASSWORD"]
    ... }

    You want to send a Pathway table t to the Redpanda instance.

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown("age owner pet \\n 1 10 Alice dog \\n 2 9 Bob cat \\n 3 8 Alice cat")

    To connect to the topic "animals" and send messages, the connector must be used
    as follows, depending on the format:

    JSON version:

    >>> pw.io.redpanda.write(
    ...    t,
    ...    rdkafka_settings,
    ...    "animals",
    ...    format="json",
    ... )

    All the updates of table t will be sent to the Redpanda instance.
    """

    kafka.write(
        table=table,
        rdkafka_settings=rdkafka_settings,
        topic_name=topic_name,
        format=format,
        name=name,
        sort_by=sort_by,
        **kwargs,
    )

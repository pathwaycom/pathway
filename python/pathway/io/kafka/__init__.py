# Copyright © 2024 Pathway

from __future__ import annotations

import uuid
import warnings
from typing import Any

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.api import PathwayType
from pathway.internals.decorators import table_from_datasource
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import check_deprecated_kwargs, construct_schema_and_data_format

SUPPORTED_INPUT_FORMATS: set[str] = {
    "csv",
    "json",
    "raw",
}


@check_arg_types
@trace_user_frame
def read(
    rdkafka_settings: dict,
    topic: str | list[str] | None = None,
    *,
    schema: type[Schema] | None = None,
    format="raw",
    debug_data=None,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    parallel_readers: int | None = None,
    persistent_id: str | None = None,
    value_columns: list[str] | None = None,
    primary_key: list[str] | None = None,
    types: dict[str, PathwayType] | None = None,
    default_values: dict[str, Any] | None = None,
    _stacklevel: int = 1,
    **kwargs,
) -> Table:
    """Generalized method to read the data from the given topic in Kafka.

    There are three formats currently supported: "raw", "csv", and "json".

    Args:
        rdkafka_settings: Connection settings in the format of `librdkafka
            <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_.
        topic: Name of topic in Kafka from which the data should be read.
        schema: Schema of the resulting table.
        format: format of the input data, "raw", "csv", or "json".
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms:the maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        json_field_paths: If the format is JSON, this field allows to map field names
            into path in the field. For the field which require such mapping, it should be
            given in the format ``<field_name>: <path to be mapped>``, where the path to
            be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        parallel_readers: number of copies of the reader to work in parallel. In case
            the number is not specified, min{pathway_threads, total number of partitions}
            will be taken. This number also can't be greater than the number of Pathway
            engine threads, and will be reduced to the number of engine threads, if it
            exceeds.
        persistent_id: (unstable) An identifier, under which the state of the table will
            be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.
        value_columns: Columns to extract for a table, required for format other than
            "raw". [will be deprecated soon]
        primary_key: In case the table should have a primary key generated according to
            a subset of its columns, the set of columns should be specified in this field.
            Otherwise, the primary key will be generated randomly. [will be deprecated soon]
        types: Dictionary containing the mapping between the columns and the data
            types (``pw.Type``) of the values of those columns. This parameter is optional, and if not
            Otherwise, the primary key will be generated randomly.
            provided the default type is ``pw.Type.ANY``. [will be deprecated soon]
        default_values: dictionary containing default values for columns replacing
            blank entries. The default value of the column must be specified explicitly,
            Otherwise, the primary key will be generated randomly.
            otherwise there will be no default value. [will be deprecated soon]

    Returns:
        Table: The table read.

    When using the format "raw", the connector will produce a single-column table:
    all the data is saved into a column named ``data``.
    For other formats, the argument value_column is required and defines the columns.

    Example:

    Consider there is a queue in Kafka, running locally on port 9092. Our queue can
    use SASL-SSL authentication over a SCRAM-SHA-256 mechanism. You can set up a queue
    with similar parameters in `Upstash <https://upstash.com/>`_. Settings for rdkafka
    will look as follows:

    >>> import os
    >>> rdkafka_settings = {
    ...    "bootstrap.servers": "localhost:9092",
    ...    "security.protocol": "sasl_ssl",
    ...    "sasl.mechanism": "SCRAM-SHA-256",
    ...    "group.id": "$GROUP_NAME",
    ...    "session.timeout.ms": "60000",
    ...    "sasl.username": os.environ["KAFKA_USERNAME"],
    ...    "sasl.password": os.environ["KAFKA_PASSWORD"]
    ... }

    To connect to the topic "animals" and accept messages, the connector must be used \
        as follows, depending on the format:

    Raw version:

    >>> import pathway as pw
    >>> t = pw.io.kafka.read(
    ...   rdkafka_settings,
    ...    topic="animals",
    ...    format="raw",
    ... )

    All the data will be accessible in the column data.

    CSV version:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.kafka.read(
    ...    rdkafka_settings,
    ...    topic="animals",
    ...    format="csv",
    ...    schema=InputSchema
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
    >>> t = pw.io.kafka.read(
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
    ...   pet_name: str
    ...   pet_height: int
    >>> t = pw.io.kafka.read(
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
    # The data_storage is common to all kafka connectors

    if not topic:
        topic_names = kwargs.get("topic_names")
        if not topic_names:
            raise ValueError("Missing topic name specification")
        topic = topic_names[0]
    if isinstance(topic, list):
        warnings.warn(
            "'topic' should be a str, not list. First element will be used.",
            SyntaxWarning,
            stacklevel=_stacklevel + 4,
        )
        topic = topic[0]

    check_deprecated_kwargs(kwargs, ["topic_names"], stacklevel=_stacklevel + 4)

    data_storage = api.DataStorage(
        storage_type="kafka",
        rdkafka_settings=rdkafka_settings,
        topic=topic,
        parallel_readers=parallel_readers,
        persistent_id=persistent_id,
        mode=api.ConnectorMode.STREAMING,
    )
    schema, data_format = construct_schema_and_data_format(
        format,
        schema=schema,
        csv_settings=None,
        json_field_paths=json_field_paths,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
        _stacklevel=5,
    )
    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=data_source_options,
            schema=schema,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def simple_read(
    server: str,
    topic: str,
    *,
    read_only_new: bool = False,
    schema: type[Schema] | None = None,
    format="raw",
    debug_data=None,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    parallel_readers: int | None = None,
    persistent_id: str | None = None,
) -> Table:
    """Simplified method to read data from Kafka. Only requires the server address and
    the topic name. If you have any kind of authentication or require fine-tuning of the
    parameters, please use `read` method.

    Read starts from the beginning of the topic, unless the `read_only_new` parameter is
    set to True.

    There are three formats currently supported: "raw", "csv", and "json".

    Args:
        server: Address of the server.
        topic: Name of topic in Kafka from which the data should be read.
        read_only_new: If set to `True` only the entries which appear after the start \
of the program will be read. Otherwise, the read will be done from the beginning of the\
topic.
        schema: Schema of the resulting table.
        format: format of the input data, "raw", "csv", or "json".
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms: The maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        json_field_paths: If the format is JSON, this field allows to map field names
            into path in the field. For the fields which require such mapping, it should be
            given in the format ``<field_name>: <path to be mapped>``, where the path to
            be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        parallel_readers: number of copies of the reader to work in parallel. In case
            the number is not specified, min{pathway_threads, total number of partitions}
            will be taken. This number also can't be greater than the number of Pathway
            engine threads, and will be reduced to the number of engine threads, if it
            exceeds.
        persistent_id: (unstable) An identifier, under which the state of the table will
            be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.

    Returns:
        Table: The table read.

    When using the format "raw", the connector will produce a single-column table:
    all the data is saved into a column named ``data``.

    For other formats, the argument value_column is required and defines the columns.

    Example:

    Consider that there's a Kafka queue running locally on the port 9092 and we need
    to read raw messages from the topic "test-topic". Then, it can be done in the
    following way:

    >>> import pathway as pw
    >>> t = pw.io.kafka.simple_read("localhost:9092", "test-topic")
    """

    rdkafka_settings = {
        "bootstrap.servers": server,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "end" if read_only_new else "beginning",
    }
    return read(
        rdkafka_settings=rdkafka_settings,
        topic=topic,
        schema=schema,
        format=format,
        debug_data=debug_data,
        autocommit_duration_ms=autocommit_duration_ms,
        json_field_paths=json_field_paths,
        parallel_readers=parallel_readers,
        persistent_id=persistent_id,
    )


@check_arg_types
@trace_user_frame
def read_from_upstash(
    endpoint: str,
    username: str,
    password: str,
    topic: str,
    *,
    read_only_new: bool = False,
    schema: type[Schema] | None = None,
    format="raw",
    debug_data=None,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    parallel_readers: int | None = None,
    persistent_id: str | None = None,
) -> Table:
    """Simplified method to read data from Kafka instance hosted in Upstash. It requires
    endpoint address and topic along with credentials.

    Read starts from the beginning of the topic, unless the `read_only_new` parameter is
    set to True.

    There are three formats currently supported: "raw", "csv", and "json".

    Args:
        endpoint: Upstash endpoint for the sought queue, which can be found on \
"Details" page.
        username: Username generated for this queue.
        password: Password generated for this queue. These credentials are also \
available on "Details" page.
        topic: Name of topic in Kafka from which the data should be read.
        read_only_new: If set to `True` only the entries which appear after the start \
of the program will be read. Otherwise, the read will be done from the beginning of the\
topic.
        schema: Schema of the resulting table.
        format: format of the input data, "raw", "csv", or "json".
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms: The maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        json_field_paths: If the format is JSON, this field allows to map field names
            into path in the field. For the fields which require such mapping, it should be
            given in the format ``<field_name>: <path to be mapped>``, where the path to
            be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        parallel_readers: number of copies of the reader to work in parallel. In case
            the number is not specified, min{pathway_threads, total number of partitions}
            will be taken. This number also can't be greater than the number of Pathway
            engine threads, and will be reduced to the number of engine threads, if it
            exceeds.
        persistent_id: (unstable) An identifier, under which the state of the table will
            be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.

    Returns:
        Table: The table read.

    When using the format "raw", the connector will produce a single-column table:
    all the data is saved into a column named ``data``.

    Example:

    Consider that there is a queue running in Upstash. Let's say the endpoint name is
    "https://example-endpoint.com:19092", topic is "test-topic" and the credentials are
    stored in environment variables.

    Suppose that we need just to read the raw messages for the further processing. Then
    it can be done in the following way:

    >>> import os
    >>> import pathway as pw
    >>> t = pw.io.kafka.read_from_upstash(
    ...     endpoint="https://example-endpoint.com:19092",
    ...     topic="test-topic",
    ...     username=os.environ["KAFKA_USERNAME"],
    ...     password=os.environ["KAFKA_PASSWORD"],
    ... )
    """

    rdkafka_settings = {
        "bootstrap.servers": endpoint,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "end" if read_only_new else "beginning",
        "security.protocol": "sasl_ssl",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": username,
        "sasl.password": password,
    }
    return read(
        rdkafka_settings=rdkafka_settings,
        topic=topic,
        schema=schema,
        format=format,
        debug_data=debug_data,
        autocommit_duration_ms=autocommit_duration_ms,
        json_field_paths=json_field_paths,
        parallel_readers=parallel_readers,
        persistent_id=persistent_id,
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    rdkafka_settings: dict,
    topic_name: str,
    *,
    format: str = "json",
    delimiter: str = ",",
    **kwargs,
) -> None:
    """Write a table to a given topic on a Kafka instance.

    Args:
        table: the table to output.
        rdkafka_settings: Connection settings in the format of
            `librdkafka <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_.
        topic_name: name of topic in Kafka to which the data should be sent.
        format: format of the input data, currently "json" and "dsv" are supported.
        delimiter: field delimiter to be used in case of delimiter-separated values
            format.

    Returns:
        None

    Limitations:

    For future proofing, the format is configurable, but (for now) only JSON is available.

    Example:

    Consider there is a queue in Kafka, running locally on port 9092. Our queue can
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

    You want to send a Pathway table t to the Kafka instance.

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown("age owner pet \\n 1 10 Alice dog \\n 2 9 Bob cat \\n 3 8 Alice cat")

    To connect to the topic "animals" and send messages, the connector must be used \
        as follows, depending on the format:

    JSON version:

    >>> pw.io.kafka.write(
    ...    t,
    ...    rdkafka_settings,
    ...    "animals",
    ...    format="json",
    ... )

    All the updates of table t will be sent to the Kafka instance.
    """

    check_deprecated_kwargs(
        kwargs, ["commit_frequency_ms", "commit_frequency_in_messages"]
    )

    data_storage = api.DataStorage(
        storage_type="kafka",
        rdkafka_settings=rdkafka_settings,
        topic=topic_name,
    )

    if format == "json":
        data_format = api.DataFormat(
            format_type="jsonlines",
            key_field_names=[],
            value_fields=_format_output_value_fields(table),
        )
    elif format == "dsv":
        data_format = api.DataFormat(
            format_type="dsv",
            key_field_names=[],
            value_fields=_format_output_value_fields(table),
            delimiter=delimiter,
        )
    else:
        raise ValueError(f"Unsupported format: {format}")

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
        )
    )

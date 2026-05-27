# Copyright © 2026 Pathway

from __future__ import annotations

import uuid
import warnings
from typing import Iterable, Literal

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import SchemaRegistryHeader, SchemaRegistrySettings
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import (
    KEY_SOURCE_COMPONENT,
    PAYLOAD_SOURCE_COMPONENT,
    Schema,
)
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    MessageQueueOutputFormat,
    _get_unique_name,
    check_deprecated_kwargs,
    check_raw_and_plaintext_only_kwargs_for_message_queues,
    construct_schema_and_data_format,
    internal_connector_mode,
)


@check_arg_types
@trace_user_frame
def read(
    rdkafka_settings: dict,
    topic: str | list[str] | None = None,
    *,
    schema: type[Schema] | None = None,
    mode: Literal["streaming", "static"] = "streaming",
    format: Literal["plaintext", "raw", "json"] = "raw",
    schema_registry_settings: SchemaRegistrySettings | None = None,
    debug_data=None,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    autogenerate_key: bool = False,
    with_metadata: bool = False,
    start_from_timestamp_ms: int | None = None,
    parallel_readers: int | None = None,
    name: str | None = None,
    max_backlog_size: int | None = None,
    _stacklevel: int = 1,
    **kwargs,
) -> Table:
    """Generalized method to read the data from the given topic in Kafka.

    There are three formats currently supported: ``"plaintext"``, ``"raw"``, and ``"json"``.
    If the ``"raw"`` format is chosen, the key and the payload are read from the topic as raw
    bytes and used in the table "as is". If you choose the ``"plaintext"`` option, however,
    they are parsed from the UTF-8 into the plaintext entries. In both cases, the
    table consists of a primary key and two columns ``"key"`` and ``"data"``,
    denoting the key and the payload read.

    If ``"json"`` is chosen, the connector first parses the payload of the message
    according to the JSON format and then creates the columns corresponding to the
    schema defined by the ``schema`` parameter. The values of these columns are
    taken from the respective parsed JSON fields.

    Args:
        rdkafka_settings: Connection settings in the format of `librdkafka
            <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_.
        topic: Name of topic in Kafka from which the data should be read.
        schema: Schema of the resulting table.
        mode: Specifies how the engine retrieves data from the topic. The default value is
            ``"streaming"``, which means the engine will constantly wait for new messages,
            process them as they arrive, and send them into the engine. Alternatively,
            if set to ``"static"``, the engine will only read and process the data that
            is already available at the time of execution.
        format: format of the input data, ``"raw"``, ``"plaintext"``, or ``"json"``.
        schema_registry_settings: settings for connecting to the Confluent Schema Registry,
            if this type of registry is used.
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms: the maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        json_field_paths: If the format is JSON, this field allows to map field names
            into path in the field. For the field which require such mapping, it should be
            given in the format ``<field_name>: <path to be mapped>``, where the path to
            be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        autogenerate_key: If ``True``, Pathway automatically generates unique primary key
            for the entries read. Otherwise it first tries to use the key from the message.
            This parameter is used only if the ``format`` is "raw" or "plaintext".
        with_metadata: When set to ``True``, the connector will add an additional column
            named ``_metadata`` to the table. This column will be a JSON field. It'll contain
            an optional field ``timestamp_millis`` denoting the UNIX timestamp of a record
            in milliseconds, if available. It will also contain fields ``topic``, ``partition``
            and ``offset`` denoting the topic, partition and offset respectively, that
            correspond to the Kafka message that produced this row. Finally, the top level of
            this column's JSON will contain a ``headers`` array. Each element will be a pair
            consisting of a string (the header name) and an optional base64-encoded string
            (the header value). The header value is ``null`` only when the header body is
            absent (the Kafka protocol distinguishes a missing body from an empty one); a
            present-but-empty body is encoded as an empty string.
        start_from_timestamp_ms: If defined, the read starts from entries with the given
            timestamp in the past, specified in milliseconds.
        parallel_readers: number of copies of the reader to work in parallel. In case
            the number is not specified, min{pathway_threads, total number of partitions}
            will be taken. This number also can't be greater than the number of Pathway
            engine threads, and will be reduced to the number of engine threads, if it
            exceeds.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        max_backlog_size: Limit on the number of entries read from the input source and kept
            in processing at any moment. Reading pauses when the limit is reached and resumes
            as processing of some entries completes. Useful with large sources that
            emit an initial burst of data to avoid memory spikes.

    Returns:
        Table: The table read.

    When using the format ``"raw"`` or ``"plaintext"``, the connector will produce a
    two-column table: all the payloads are saved into a column named ``data``, while the
    keys are saved into a column ``key``.

    For other formats, the schema is required and defines the columns.

    Example:

    Consider a Kafka queue running locally on port 9092. For demonstration purposes, our
    queue uses simple SASL/PLAIN authentication. You can set up a Kafka cluster with
    similar parameters in `Confluent Cloud <https://confluent.cloud/>`_ or run it locally
    using Docker or Docker Compose. The rdkafka settings in our example will look as follows:

    >>> import os
    >>> rdkafka_settings = {
    ...     "bootstrap.servers": "localhost:9092",
    ...     "group.id": "kafka-tests",
    ...     "security.protocol": "sasl_ssl",
    ...     "sasl.mechanism": "PLAIN",
    ...     "sasl.username": os.environ["KAFKA_USERNAME"],
    ...     "sasl.password": os.environ["KAFKA_PASSWORD"]
    ... }

    To connect to the topic "animals" and accept messages, the connector must be used
    as follows, depending on the format:

    Raw version:

    >>> import pathway as pw
    >>> t = pw.io.kafka.read(
    ...     rdkafka_settings,
    ...     topic="animals",
    ...     format="raw",
    ... )

    All the payload data will be accessible in the column ``data``, the keys of the messages
    will be stored in the column ``key``.

    JSON version:

    >>> class InputSchema(pw.Schema):
    ...     owner: str
    ...     pet: str
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

    >>> class InputSchema(pw.Schema):
    ...     pet_name: str
    ...     pet_height: int
    >>> t = pw.io.kafka.read(
    ...     rdkafka_settings,
    ...     topic="animals",
    ...     format="json",
    ...     schema=InputSchema,
    ...     json_field_paths={
    ...         "pet_name": "/pet/name",
    ...         "pet_height": "/pet/measurements/1"
    ...     },
    ... )

    Note that a Kafka message contains a key and a payload. By default, the schema fields
    are parsed from the payload, but this behavior can be changed. To do that, you need
    to specify the ``source_component`` parameter for the target fields. For example,
    if the schema is similar to the example above, but there is also a unique pet ID
    stored in the key JSON at the path ``/pet/identification/id``, you can read it by
    first modifying the schema:

    >>> class InputSchema(pw.Schema):
    ...     pet_id: int = pw.column_definition(primary_key=True, source_component="key")
    ...     pet_name: str
    ...     pet_height: int

    And then by providing a JSONPath to this field as well in the ``read`` method:

    >>> t = pw.io.kafka.read(
    ...     rdkafka_settings,
    ...     topic="animals",
    ...     format="json",
    ...     schema=InputSchema,
    ...     json_field_paths={
    ...         "pet_id": "/pet/identification/id",
    ...         "pet_name": "/pet/name",
    ...         "pet_height": "/pet/measurements/1"
    ...     },
    ... )

    Note that you would not need to provide the JSONPath for ``pet_id`` if it is
    at the top level of the key JSON.
    """
    # The data_storage is common to all kafka connectors

    if not rdkafka_settings.get("bootstrap.servers"):
        raise ValueError(
            "rdkafka_settings must contain a non-empty 'bootstrap.servers' "
            "entry so the consumer can locate a broker; got "
            f"{rdkafka_settings.get('bootstrap.servers')!r}."
        )
    if not rdkafka_settings.get("group.id"):
        raise ValueError(
            "rdkafka_settings must contain a non-empty 'group.id' entry: "
            "Pathway's Kafka reader uses 'subscribe' (not 'assign'), which "
            "librdkafka refuses to perform without a configured consumer "
            f"group id; got {rdkafka_settings.get('group.id')!r}."
        )

    if max_backlog_size is not None and max_backlog_size <= 0:
        raise ValueError(
            f"'max_backlog_size' must be positive; got {max_backlog_size}. "
            f"A non-positive value would prevent any entry from being "
            f"processed and the reader would never make progress."
        )
    if parallel_readers is not None and parallel_readers <= 0:
        raise ValueError(
            f"'parallel_readers' must be positive; got {parallel_readers}."
        )
    if start_from_timestamp_ms is not None and start_from_timestamp_ms < 0:
        raise ValueError(
            f"'start_from_timestamp_ms' must be non-negative; got "
            f"{start_from_timestamp_ms}. The value is a Unix timestamp in "
            f"milliseconds — negative values are pre-epoch and not "
            f"meaningful for Kafka."
        )
    if autocommit_duration_ms is not None and autocommit_duration_ms <= 0:
        raise ValueError(
            f"'autocommit_duration_ms' must be positive; got "
            f"{autocommit_duration_ms}. It is the maximum time between "
            f"two commits and zero/negative values would prevent commits "
            f"from happening."
        )

    # When 'start_from_timestamp_ms' is set, the engine seeks lazily after
    # the consumer is positioned at the partition's earliest offset, so any
    # user-supplied 'auto.offset.reset' value that doesn't already mean
    # "start at the beginning" is silently rewritten on the Rust side.
    # Surface that rewrite explicitly so somebody who picked 'latest' on
    # purpose doesn't see Pathway read from the beginning instead. The
    # librdkafka aliases 'earliest', 'beginning' and 'smallest' all mean
    # "start at the beginning" and therefore don't trigger the override.
    _START_FROM_BEGINNING_ALIASES = {"earliest", "beginning", "smallest"}
    user_offset_reset = rdkafka_settings.get("auto.offset.reset")
    if (
        start_from_timestamp_ms is not None
        and user_offset_reset is not None
        and user_offset_reset not in _START_FROM_BEGINNING_ALIASES
    ):
        warnings.warn(
            "'auto.offset.reset' is overridden to 'earliest' whenever "
            "'start_from_timestamp_ms' is set, so the seek can fall back "
            f"to the start of the partition. Your value "
            f"{user_offset_reset!r} is being ignored.",
            stacklevel=_stacklevel + 4,
        )

    # Distinguish "missing topic" from "explicitly empty topic" — the former
    # is a user typo (rename to 'topic='), the latter is an invalid value
    # that Kafka itself would reject with a less actionable error.
    if topic is None:
        if "topic_name" in kwargs:
            raise TypeError(
                "Got unexpected keyword argument 'topic_name'. "
                "pw.io.kafka.read uses 'topic' (the corresponding parameter "
                "in pw.io.kafka.write is 'topic_name'). Please rename "
                "'topic_name=' to 'topic='."
            )
        topic_names = kwargs.pop("topic_names", None)
        if not topic_names:
            raise ValueError("Missing topic name specification")
        if isinstance(topic_names, str):
            warnings.warn(
                "'topic_names' is deprecated; please use 'topic' instead.",
                DeprecationWarning,
                stacklevel=_stacklevel + 4,
            )
            topic = topic_names
        elif isinstance(topic_names, (list, tuple)):
            warnings.warn(
                "'topic_names' is deprecated; please use 'topic' instead. "
                "Only the first element of the provided list is used as "
                "the topic name.",
                DeprecationWarning,
                stacklevel=_stacklevel + 4,
            )
            topic = topic_names[0]
        else:
            raise TypeError(
                f"'topic_names' must be a str or a list of str; got "
                f"{type(topic_names).__name__}"
            )
    if isinstance(topic, list):
        if not topic:
            raise ValueError(
                "'topic' must be a non-empty string; got an empty list. "
                "Kafka does not allow empty topic names."
            )
        warnings.warn(
            "'topic' should be a str, not list. First element will be used.",
            DeprecationWarning,
            stacklevel=_stacklevel + 4,
        )
        topic = topic[0]

    if not isinstance(topic, str) or not topic:
        raise ValueError(
            f"'topic' must be a non-empty string; got {topic!r}. "
            "Kafka does not allow empty topic names."
        )

    check_deprecated_kwargs(kwargs, ["topic_names"], stacklevel=_stacklevel + 4)

    data_storage = api.DataStorage(
        storage_type="kafka",
        rdkafka_settings=rdkafka_settings,
        topic=topic,
        parallel_readers=parallel_readers,
        start_from_timestamp_ms=start_from_timestamp_ms,
        mode=internal_connector_mode(mode),
    )

    # TODO: support case when the key is scalar and the value is json
    schema, data_format = construct_schema_and_data_format(
        format,
        with_metadata=with_metadata,
        autogenerate_key=autogenerate_key,
        schema=schema,
        json_field_paths=json_field_paths,
        schema_registry_settings=schema_registry_settings,
        with_native_record_key=True,
        _stacklevel=5,
    )
    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=_get_unique_name(name, kwargs, stacklevel=_stacklevel + 5),
        max_backlog_size=max_backlog_size,
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=data_source_options,
            schema=schema,
            datasource_name="kafka",
            append_only=data_format.is_native_session_used(),  # simple comparison wouldn't work
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
        supported_components=(
            KEY_SOURCE_COMPONENT,
            PAYLOAD_SOURCE_COMPONENT,
        ),
    )


@check_arg_types
@trace_user_frame
def simple_read(
    server: str,
    topic: str,
    *,
    read_only_new: bool = False,
    schema: type[Schema] | None = None,
    format: Literal["plaintext", "raw", "json"] = "raw",
    debug_data=None,
    autocommit_duration_ms: int | None = 1500,
    json_field_paths: dict[str, str] | None = None,
    parallel_readers: int | None = None,
    name: str | None = None,
    **kwargs,
) -> Table:
    """Simplified method to read data from Kafka. Only requires the server address and
    the topic name. If you have any kind of authentication or require fine-tuning of the
    parameters, please use `read` method.

    Read starts from the beginning of the topic, unless the `read_only_new` parameter is
    set to True.

    There are three formats currently supported: "plaintext", "raw", and "json".
    If the "raw" format is chosen, the key and the payload are read from the topic as raw
    bytes and used in the table "as is". If you choose the "plaintext" option, however,
    they are parsed from the UTF-8 into the plaintext entries. In both cases, the
    resulting table has two columns: ``key`` (the Kafka message key) and ``data``
    (the Kafka message payload).

    If "json" is chosen, the connector first parses the payload of the message
    according to the JSON format and then creates the columns corresponding to the
    schema defined by the ``schema`` parameter. The values of these columns are
    taken from the respective parsed JSON fields.

    Args:
        server: Address of the server.
        topic: Name of topic in Kafka from which the data should be read.
        read_only_new: If set to `True` only the entries which appear after the start
            of the program will be read. Otherwise, the read will be done from the
            beginning of the topic.
        schema: Schema of the resulting table.
        format: format of the input data, "raw", "plaintext", or "json".
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
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.

    Returns:
        Table: The table read.

    When using the format "raw" or "plaintext", the connector will produce a two-column
    table: the message key is saved in a column named ``key`` and the message payload
    in a column named ``data``.

    For the "json" format, a ``schema`` is required and its columns define the table.

    Example:

    Consider that there's a Kafka queue running locally on the port 9092 and we need
    to read raw messages from the topic "test-topic". Then, it can be done in the
    following way:

    >>> import pathway as pw
    >>> t = pw.io.kafka.simple_read("localhost:9092", "test-topic")
    """

    forbidden_kwargs = ("rdkafka_settings", "_stacklevel")
    for forbidden in forbidden_kwargs:
        if forbidden in kwargs:
            raise TypeError(
                f"'simple_read' does not accept {forbidden!r}. "
                "If you need to customize the rdkafka settings (auth, TLS, "
                "consumer group, etc.) use 'pw.io.kafka.read' directly; "
                "'simple_read' is a thin wrapper that builds these settings "
                "itself."
            )

    if read_only_new and kwargs.get("mode") == "static":
        raise ValueError(
            "'read_only_new=True' together with mode='static' is "
            "self-contradictory: 'read_only_new' starts the consumer at the "
            "end of the partition, so the static reader has nothing to "
            "consume. Pick one — drop 'read_only_new=True' or use "
            "'mode=\"streaming\"' (the default)."
        )
    if read_only_new and kwargs.get("start_from_timestamp_ms") is not None:
        raise ValueError(
            "'read_only_new=True' and 'start_from_timestamp_ms' both control "
            "the starting position and conflict. Pick one — drop "
            "'read_only_new=True' if you want to start from the timestamp, "
            "or drop 'start_from_timestamp_ms' if you want to start from "
            "the end."
        )

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
        name=name,
        _stacklevel=5,
        **kwargs,
    )


@check_raw_and_plaintext_only_kwargs_for_message_queues
@check_arg_types
@trace_user_frame
def write(
    table: Table,
    rdkafka_settings: dict,
    topic_name: str | ColumnReference,
    *,
    format: Literal["raw", "plaintext", "json", "dsv"] = "json",
    schema_registry_settings: SchemaRegistrySettings | None = None,
    subject: str | None = None,
    delimiter: str = ",",
    key: ColumnReference | None = None,
    value: ColumnReference | None = None,
    headers: Iterable[ColumnReference] | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Write a table to a given topic on a Kafka instance.

    The produced messages consist of the key, corresponding to row's key, the value,
    corresponding to the values of the table that are serialized according to the chosen
    format and two headers: ``pathway_time``, corresponding to the logical time of the entry
    and ``pathway_diff`` that is either 1 or -1. Both header values are provided as UTF-8
    encoded strings.

    There are several serialization formats supported: 'json', 'dsv', 'plaintext' and 'raw'.
    The format defines how the message is formed. In case of JSON and DSV (delimiter
    separated values), the message is formed in accordance with the respective data format.

    If the selected format is either 'plaintext' or 'raw', you also need to specify, which
    columns of the table correspond to the key and the value of the produced Kafka
    message. It can be done by providing ``key`` and ``value`` parameters. In order to
    output extra values from the table in these formats, Kafka headers can be used. You
    can specify the column references in the ``headers`` parameter, which leads to
    serializing the extracted fields into UTF-8 strings and passing them as additional
    Kafka headers.

    Args:
        table: the table to output.
        rdkafka_settings: Connection settings in the format of
            `librdkafka <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_.
        topic_name: The Kafka topic where data will be written. This can be a specific topic name
            or a reference to a column whose values will be used as the topic for each message.
            If using a column reference, the column must contain string values.
        format: format in which the data is put into Kafka. Currently "json",
            "plaintext", "raw" and "dsv" are supported. If the "raw" format is selected,
            ``table`` must either contain exactly one binary column that will be dumped as it is into the
            Kafka message, or the reference to the target binary column must be specified explicitly
            in the ``value`` parameter. Similarly, if "plaintext" is chosen, the table should consist
            of a single column of the string type, or the reference to the target string column
            must be specified explicitly in the ``value`` parameter.
        schema_registry_settings: settings for connecting to the Confluent Schema Registry,
            if this type of registry is used.
        subject: the subject name for the schema in the Confluent Schema Registry, if the
            registry is used.
        delimiter: field delimiter to be used in case of delimiter-separated values
            format 'dsv'.
        key: reference to the column that should be used as a key in the produced message.
            If left empty, an internal primary key will be used.
        value: reference to the column that should be used as a value in
            the produced message in 'plaintext' or 'raw' format. It can be deduced automatically if the
            table has exactly one column. Otherwise it must be specified directly. It also has to be
            explicitly specified, if ``key`` is set. The type of the column must correspond to the
            format used: ``str`` for the 'plaintext' format and ``binary`` for the 'raw' format.
        headers: references to the table fields that must be provided as message
            headers. These headers are named in the same way as fields that are forwarded and correspond
            to the string representations of the respective values encoded in UTF-8. If a binary
            column is requested, it will be produced "as is" in the respective header.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Examples:

    Consider a Kafka queue running locally on port 9092. For demonstration purposes, our
    queue uses simple SASL/PLAIN authentication. You can set up a Kafka cluster with
    similar parameters in `Confluent Cloud <https://confluent.cloud/>`_ or run it locally
    using Docker or Docker Compose. The rdkafka settings in our example will look as follows:

    >>> import os
    >>> rdkafka_settings = {
    ...     "bootstrap.servers": "localhost:9092",
    ...     "security.protocol": "sasl_ssl",
    ...     "sasl.mechanism": "PLAIN",
    ...     "sasl.username": os.environ["KAFKA_USERNAME"],
    ...     "sasl.password": os.environ["KAFKA_PASSWORD"]
    ... }

    You want to send a Pathway table ``t`` to the Kafka instance.

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown("age owner pet \\n 1 10 Alice dog \\n 2 9 Bob cat \\n 3 8 Alice cat")

    To connect to the topic "animals" and send messages, the connector must be used as
    follows, depending on the format:

    JSON version:

    >>> pw.io.kafka.write(
    ...     t,
    ...     rdkafka_settings,
    ...     "animals",
    ...     format="json",
    ... )

    All the updates of table ``t`` will be sent to the Kafka instance.

    Another thing to be demonstrated is the usage of 'raw' format in the output. Please
    note that the same rules will be applicable for the 'plaintext' with the only difference
    being the requirement for the columns to have the ``string`` type.

    Now consider that a table ``t2`` contains two binary columns ``foo`` and ``bar``, and
    a numerical column ``baz``. That is, the schema of this table looks as follows:

    >>> class T2Schema(pw.Schema):
    ...     foo: bytes
    ...     bar: bytes
    ...     baz: int

    This table can be generated with a Python input connector as follows:

    >>> class T2GenerationSubject(pw.io.python.ConnectorSubject):
    ...     def run(self) -> None:
    ...         # TODO: define generation logic
    ...         pass
    >>> t2 = pw.io.python.read(T2GenerationSubject(), schema=T2Schema)

    Since there is more than one column, you need to specify which one you want to use in the
    output, when using the 'raw' format. If this is the column ``foo``, you may output this
    table as follows:

    >>> pw.io.kafka.write(
    ...     t2,
    ...     rdkafka_settings,
    ...     "test",
    ...     format="raw",
    ...     value=t2.foo,
    ... )

    If at the same time you would prefer to have the key of the produced messages to be
    defined by the value of another binary column ``bar``, you can use the ``key`` parameter as
    follows:

    >>> pw.io.kafka.write(
    ...     t2,
    ...     rdkafka_settings,
    ...     "test",
    ...     format="raw",
    ...     key=t2.bar,
    ...     value=t2.foo,
    ... )

    Still, the table has three fields and the field ``baz`` is not produced. You can do it
    with the usage of headers. To pass it to the header with the same name ``baz``, you need to
    specify it:

    >>> pw.io.kafka.write(
    ...     t2,
    ...     rdkafka_settings,
    ...     "test",
    ...     format="raw",
    ...     key=t2.bar,
    ...     value=t2.foo,
    ...     headers=[t2.baz],
    ... )
    """
    if not rdkafka_settings.get("bootstrap.servers"):
        raise ValueError(
            "rdkafka_settings must contain a non-empty 'bootstrap.servers' "
            "entry so the producer can locate a broker; got "
            f"{rdkafka_settings.get('bootstrap.servers')!r}."
        )
    if isinstance(topic_name, str) and not topic_name:
        raise ValueError(
            "'topic_name' must be a non-empty string; got an empty string. "
            "Kafka does not allow empty topic names."
        )

    output_format = MessageQueueOutputFormat.construct(
        table,
        format=format,
        delimiter=delimiter,
        key=key,
        value=value,
        headers=headers,
        topic_name=topic_name if isinstance(topic_name, ColumnReference) else None,
        schema_registry_settings=schema_registry_settings,
        subject=subject,
    )
    output_table = output_format.table
    remapped_sort_by = _remap_sort_by(sort_by, table, output_table)

    data_storage = api.DataStorage(
        storage_type="kafka",
        rdkafka_settings=rdkafka_settings,
        topic=topic_name if isinstance(topic_name, str) else None,
        topic_name_index=output_format.topic_name_index,
        key_field_index=output_format.key_field_index,
        header_fields=[item for item in output_format.header_fields.items()],
    )

    output_table.to(
        datasink.GenericDataSink(
            data_storage,
            output_format.data_format,
            datasink_name="kafka",
            unique_name=name,
            sort_by=remapped_sort_by,
        )
    )


def _remap_sort_by(
    sort_by: Iterable[ColumnReference] | None,
    original_table: Table,
    output_table: Table,
) -> list[ColumnReference] | None:
    if sort_by is None:
        return None
    remapped: list[ColumnReference] = []
    for column in sort_by:
        if column._table is output_table:
            remapped.append(column)
            continue
        if column._table is not original_table:
            raise ValueError(
                f"The sort_by column {column} doesn't belong to the table "
                "passed to pw.io.kafka.write."
            )
        if column.name not in output_table._columns:
            raise ValueError(
                f"The sort_by column {column.name!r} is not part of the "
                "data being written. For 'raw' or 'plaintext' format, only "
                "the 'value', 'key', 'topic_name' and 'headers' columns "
                "are forwarded."
            )
        remapped.append(output_table[column.name])
    return remapped


__all__ = [
    "SchemaRegistryHeader",
    "SchemaRegistrySettings",
    "read",
    "write",
]

# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.engine import DebeziumDBType
from pathway.internals import api, datasource
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import _get_unique_name, read_schema


@check_arg_types
@trace_user_frame
def read(
    rdkafka_settings: dict,
    topic_name: str,
    *,
    db_type: DebeziumDBType = DebeziumDBType.POSTGRES,
    schema: type[Schema],
    debug_data=None,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    **kwargs,
) -> Table:
    """
    Connector, which takes a topic in the format of Debezium
    and maintains a corresponding table in Pathway, on which you can do all the
    table operations provided. In order to do that, you will need a Debezium connector.

    Args:
        rdkafka_settings: Connection settings in the format of
            `librdkafka <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_.
        topic_name: Name of topic in Kafka to which the updates are streamed.
        db_type: Type of the database from which events are streamed;
        schema: Schema of the resulting table.
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms:the maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.

    Returns:
        Table: The table read.

    Example:

    Consider there is a need to stream a database table along with its changes directly into
    the Pathway engine. One of the standard well-known solutions for table streaming is
    `Debezium <https://debezium.io/>`_:
    it supports streaming data from MySQL, Postgres, MongoDB and a few more databases directly to a
    topic in Kafka. The streaming first sends a snapshot of the data and then streams
    changes for the specific change (namely: inserted, updated or removed) rows.

    Consider there is a table in Postgres, which is
    created according to the following schema:

    .. code-block:: sql

        CREATE TABLE pets (
            id SERIAL PRIMARY KEY,
            age INTEGER,
            owner TEXT,
            pet TEXT
        );

    This table, by default, will be streamed to the topic with the same name. In order to
    read it,you need to set the settings for ``rdkafka``. For the sake of demonstration,
    let's take those from the example of the Kafka connector:

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

    Now, using the settings you can set up a connector. It is as simple as:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   id: str = pw.column_definition(primary_key=True)
    ...   age: int
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.debezium.read(
    ...    rdkafka_settings,
    ...    topic_name="pets",
    ...    schema=InputSchema
    ... )

    As a result, upon its start, the connector would provide the full snapshot of the
    table ``pets`` into the table ``t`` in Pathway. The table ``t`` can then be operated as
    usual. Throughout the run time, the rows in the table ``pets`` can change. In this
    case, the changes in the result will be provided in the output connectors by the
    Stream of Updates mechanism.
    """

    data_storage = api.DataStorage(
        storage_type="kafka",
        rdkafka_settings=rdkafka_settings,
        topic=topic_name,
    )
    schema, data_format_definition = read_schema(schema)
    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=_get_unique_name(name, kwargs),
    )
    data_format = api.DataFormat(
        format_type="debezium", debezium_db_type=db_type, **data_format_definition
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=data_source_options,
            schema=schema,
            datasource_name="debezium",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )

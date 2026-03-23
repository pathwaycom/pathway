# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Any, Iterable, Literal

from pathway.internals import api, datasink, datasource
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Schema, Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    SNAPSHOT_OUTPUT_TABLE_TYPE,
    internal_connector_mode,
    read_schema,
)


@check_arg_types
@trace_user_frame
def read(
    connection_string: str,
    database: str,
    collection: str,
    schema: type[Schema],
    *,
    mode: Literal["static", "streaming"] = "streaming",
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    max_backlog_size: int | None = None,
    debug_data: Any = None,
) -> Table:
    """
    **This module is available when using one of the following licenses only:**
    `Pathway Scale, Pathway Enterprise </pricing>`_.

    .. warning::

        This is an early version of the connector. The API and behavior may change
        in the next release as the connector is stabilized.

    Reads a collection from MongoDB into a Pathway table.

    The connector fetches all documents from the specified collection and maps each
    document's fields to table columns according to the ``schema`` parameter. Field
    names in the documents must match the column names in the schema exactly.

    **Note:** Specifying a primary key in the schema is not supported. The connector
    uses MongoDB's ``_id`` field as the Pathway row key, ensuring that document
    identity is preserved consistently across the initial snapshot and subsequent
    incremental updates. Using a different primary key could cause mismatches between
    Pathway's internal state and the actual collection contents. To reindex the
    resulting table by a different column, use ``pw.Table.with_id_from()`` after
    reading.

    In ``"streaming"`` mode (the default), the connector first emits the full
    collection as an initial snapshot, then subscribes to MongoDB's change stream to
    receive incremental inserts, replacements, updates, and deletions in real time.
    The change stream is backed by the oplog, so the collection must be part of a
    `replica set <https://www.mongodb.com/docs/manual/replication/>`_ or a sharded
    cluster — a standalone MongoDB instance without replica set configuration does
    not support change streams. In ``"static"`` mode, the connector reads the
    collection once and terminates without subscribing to the change stream.

    When persistence is enabled, the connector saves the oplog position — specifically,
    the change stream resume token of the last processed event — as its offset. On
    restart, it resumes from that token and delivers only the changes that occurred
    since the last checkpoint, so the downstream computation sees only the new delta
    rather than the full collection again. The ``name`` parameter is required when
    using persistence, so that the engine can match the connector to its saved state
    across restarts.

    Args:
        connection_string: The connection string for the MongoDB deployment. See the
            `MongoDB documentation <https://www.mongodb.com/docs/manual/reference/connection-string/>`_
            for the details.
        database: The name of the database to read from.
        collection: The name of the collection to read.
        schema: Schema of the resulting table. Column names must match the field names
            in the MongoDB documents. Specifying a primary key in the schema is not
            supported; see above for details.
        mode: If set to ``"streaming"`` (the default), the connector first delivers
            the initial collection snapshot and then continuously watches for new
            changes via the change stream. If set to ``"static"``, it reads the
            collection once and terminates without opening a change stream.
        autocommit_duration_ms: The maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector
            are committed and pushed into Pathway's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's
            progress.
        max_backlog_size: Limit on the number of entries read from the input source
            and kept in processing at any moment. Reading pauses when the limit is
            reached and resumes as processing of some entries completes. Useful with
            large sources that emit an initial burst of data to avoid memory spikes.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    To get started, you need to run MongoDB locally. The connector uses MongoDB's
    change stream, which requires a replica set. The easiest way to spin up a
    single-node replica set with Docker is:

    .. code-block:: bash

        docker pull mongo
        docker run -d --name mongo -p 27017:27017 mongo --replSet rs0

    The ``--replSet rs0`` flag enables replica set mode. After the container starts,
    initialize the replica set with:

    .. code-block:: bash

        docker exec -it mongo mongosh --eval "rs.initiate()"

    You only need to do this once. Once the replica set is up, connect to the shell
    to insert some sample data:

    .. code-block:: bash

        docker exec -it mongo mongosh

    Inside the shell, create a collection and populate it:

    .. code-block:: rst

        use shop
        db.orders.insertMany([
            { product: "apple",  qty: 10 },
            { product: "banana", qty: 5  },
            { product: "cherry", qty: 20 },
        ])

    With data in place, define a matching schema in Pathway. Note that no primary key
    is declared — the connector derives the row key from each document's ``_id``.

    >>> import pathway as pw
    >>> class OrderSchema(pw.Schema):
    ...     product: str
    ...     qty: int

    **Static mode.** To read the collection once and stop, use ``mode="static"``.
    This is suitable for batch pipelines that process all available documents and
    then terminate.

    >>> table = pw.io.mongodb.read(
    ...     "mongodb://127.0.0.1:27017/?replicaSet=rs0",
    ...     database="shop",
    ...     collection="orders",
    ...     schema=OrderSchema,
    ...     mode="static",
    ... )
    >>> pw.debug.compute_and_print(table, include_id=False)  # doctest: +SKIP
    product  qty
      apple   10
     banana    5
     cherry   20

    **Streaming mode.** When ``mode="streaming"`` (the default), the connector first
    delivers the full collection as an initial snapshot and then continues to watch
    for changes. Every insert, replacement, update, or deletion in MongoDB is
    forwarded to Pathway in real time.

    >>> table = pw.io.mongodb.read(
    ...     "mongodb://127.0.0.1:27017/?replicaSet=rs0",
    ...     database="shop",
    ...     collection="orders",
    ...     schema=OrderSchema,
    ... )

    After the snapshot is delivered, any change made in the MongoDB shell will be
    reflected in Pathway immediately. For example, running the following in
    ``mongosh``:

    .. code-block:: rst

        db.orders.insertOne({ product: "durian", qty: 2 })

    will cause Pathway to receive a new row ``{ product: "durian", qty: 2 }`` with
    ``diff = 1``. Running:

    .. code-block:: rst

        db.orders.deleteOne({ product: "banana" })

    will cause Pathway to retract the ``banana`` row with ``diff = -1``.

    **Persistence in static mode.** With persistence enabled, the connector records
    the oplog position after each run. On the next run it resumes from that position
    and delivers only the documents that changed since the last checkpoint, so the
    output contains the delta rather than the full collection.

    >>> persistence_config = pw.persistence.Config(
    ...     backend=pw.persistence.Backend.filesystem("./PStorage")
    ... )
    >>> table = pw.io.mongodb.read(
    ...     "mongodb://127.0.0.1:27017/?replicaSet=rs0",
    ...     database="shop",
    ...     collection="orders",
    ...     schema=OrderSchema,
    ...     mode="static",
    ...     name="orders_source",
    ... )
    >>> pw.io.jsonlines.write(table, "output.jsonl")
    >>> pw.run(persistence_config=persistence_config)  # doctest: +SKIP

    On the first run, ``output.jsonl`` will contain all three documents with
    ``diff = 1``. If you then insert a new document into the collection and run
    the program again with the same ``persistence_config``, only the newly inserted
    document will appear in the output.

    **Persistence in streaming mode.** Persistence works the same way in streaming
    mode. Pass the same ``persistence_config`` to ``pw.run()`` and provide the same
    ``name`` to ``pw.io.mongodb.read()`` so the engine can find the saved offset:

    >>> table = pw.io.mongodb.read(
    ...     "mongodb://127.0.0.1:27017/?replicaSet=rs0",
    ...     database="shop",
    ...     collection="orders",
    ...     schema=OrderSchema,
    ...     name="orders_source",
    ... )
    >>> pw.run(persistence_config=persistence_config)  # doctest: +SKIP

    If the program is restarted, it will resume from the saved oplog position and
    emit only the changes that arrived after the previous run terminated, without
    replaying the initial snapshot.
    """
    _check_entitlements("mongodb-oplog-reader")

    if schema.primary_key_columns():
        raise ValueError(
            "Defining a primary key in the schema is not supported for pw.io.mongodb.read. "
            "The connector maintains a snapshot of the MongoDB collection keyed by the "
            "document's _id field. Using a different primary key could cause mismatches "
            "between Pathway's internal state and the actual collection contents. "
            "If you need to reindex the resulting table by a different column, use "
            "pw.Table.with_id_from() after reading."
        )

    data_storage = api.DataStorage(
        storage_type="mongodb",
        connection_string=connection_string,
        database=database,
        table_name=collection,
        mode=internal_connector_mode(mode),
    )

    schema, api_schema = read_schema(schema)
    data_format = api.DataFormat(
        format_type="bson",
        session_type=api.SessionType.UPSERT,
        **api_schema,
    )

    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=name,
        max_backlog_size=max_backlog_size,
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            schema=schema,
            data_source_options=data_source_options,
            datasource_name="mongodb",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    *,
    connection_string: str,
    database: str,
    collection: str,
    output_table_type: Literal["stream_of_changes", "snapshot"] = "stream_of_changes",
    max_batch_size: int | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table`` to a MongoDB table.

    The output table supports two formats, controlled by the ``output_table_type``
    parameter.

    The ``stream_of_changes`` format provides a complete history of all modifications
    applied to the table. Each entry contains the full data row along with two
    additional fields: ``time`` and ``diff``. The ``time`` field identifies the
    transactional minibatch in which the change occurred, while ``diff`` describes
    the nature of the change: ``diff = 1`` indicates that the row was inserted into
    the Pathway table, and ``diff = -1`` indicates that the row was removed. Row
    updates are represented as two events within the same transactional minibatch:
    first the old version of the row with ``diff = -1``, followed by the new version
    with ``diff = 1``. This format is used by default.

    The ``snapshot`` format maintains the current state of the Pathway table in the
    output. The table's primary key is stored in the ``_id`` field. When a change
    occurs, no additional metadata fields are added; instead, the engine locates the
    corresponding row by ``_id`` and applies the update directly. As a result, the
    output table always reflects the latest state of the Pathway table.

    If the specified database or table doesn't exist, it will be created during the
    first write.

    **Note:** Since MongoDB
    `stores DateTime in milliseconds <https://www.mongodb.com/docs/manual/reference/bson-types/#date>`_,
    the `Duration </developers/api-docs/pathway/#pathway.Duration>`_ type is also
    serialized as an integer number of milliseconds for consistency.

    Args:
        table: The table to output.
        connection_string: The connection string for the MongoDB database. See the \
`MongoDB documentation <https://www.mongodb.com/docs/manual/reference/connection-string/>`_ \
for the details.
        database: The name of the database to update.
        collection: The name of the collection to write to.
        output_table_type: The type of the output table, defining whether a current snapshot
            or a history of modifications must be maintained.
        max_batch_size: The maximum number of entries to insert in one batch.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    To get started, you need to run MongoDB locally. The easiest way to do this, if it
    isn't already running, is by using Docker. You can set up MongoDB in Docker with
    the following commands.

    .. code-block:: bash

        docker pull mongo
        docker run -d --name mongo -p 27017:27017 mongo

    The first command pulls the latest MongoDB image from Docker. The second command
    runs MongoDB in the background, naming the container ``mongo`` and exposing port
    ``27017`` for external connections, such as from your Pathway program.

    If the container doesn't start, check if port ``27017`` is already in use. If so, you
    can map it to a different port.

    Once MongoDB is running, you can access its shell with:

    .. code-block:: bash

        docker exec -it mongo mongosh

    There's no need to create anything in the new instance at this point.

    With MongoDB running, you can proceed with a Pathway program to write data to the
    database. Start by importing Pathway and creating a test table.

    >>> import pathway as pw
    ...
    >>> pet_owners = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | cat
    ... 8   | Alice | cat
    ... ''')

    Next, write this data to your MongoDB instance with the Pathway connector.

    >>> pw.io.mongodb.write(
    ...     pet_owners,
    ...     connection_string="mongodb://127.0.0.1:27017/",
    ...     database="pathway-test",
    ...     collection="pet-owners",
    ... )

    If you've changed the port, make sure to update the connection string with the
    correct one.

    You can modify the code to change the data source or add more processing steps.
    Remember to run the program with ``pw.run()`` to execute it.

    After the program runs, you can check that the database and collection were created.
    Access the MongoDB shell again and run:

    .. code-block:: rst

        show dbs

    You should see the ``pathway-test`` database listed, along with some pre-existing
    databases:

    .. code-block:: rst

        admin         40.00 KiB
        config        60.00 KiB
        local         40.00 KiB
        pathway-test  40.00 KiB

    Switch to the ``pathway-test`` database and list its collections:

    .. code-block:: rst

        use pathway-test
        show collections

    You should see:

    .. code-block:: rst

        pet-owners

    Finally, check the data in the ``pet-owners`` collection with:

    .. code-block:: rst

        db["pet-owners"].find().pretty()

    This should return the following entries, along with additional ``diff`` and ``time``
    fields:

    .. code-block:: rst

        [
            {
                _id: ObjectId('67180150d94db90697c07853'),
                age: Long('9'),
                owner: 'Bob',
                pet: 'cat',
                diff: Long('1'),
                time: Long('0')
            },
            {
                _id: ObjectId('67180150d94db90697c07854'),
                age: Long('8'),
                owner: 'Alice',
                pet: 'cat',
                diff: Long('1'),
                time: Long('0')
            },
            {
                _id: ObjectId('67180150d94db90697c07855'),
                age: Long('10'),
                owner: 'Alice',
                pet: 'dog',
                diff: Long('1'),
                time: Long('0')
            }
        ]

    For more advanced setups, such as replica sets, authentication, or custom
    read/write concerns, refer to the official MongoDB documentation on
    `connection strings <https://www.mongodb.com/docs/manual/reference/connection-string/>`_

    Note that if you do not need the full history of modifications, you can use the
    ``snapshot`` output table type. In this case, the connector configuration would
    look as follows:

    >>> pw.io.mongodb.write(
    ...     pet_owners,
    ...     connection_string="mongodb://127.0.0.1:27017/",
    ...     database="pathway-test",
    ...     collection="pet-owners",
    ...     output_table_type="snapshot",
    ... )

    The resulting output will be as follows:

    .. code-block:: rst

        [
            {
                _id: ObjectId('67180150d94db90697c07853'),
                age: Long('9'),
                owner: 'Bob',
                pet: 'cat',
            },
            {
                _id: ObjectId('67180150d94db90697c07854'),
                age: Long('8'),
                owner: 'Alice',
                pet: 'cat',
            },
            {
                _id: ObjectId('67180150d94db90697c07855'),
                age: Long('10'),
                owner: 'Alice',
                pet: 'dog',
            }
        ]
    """
    is_snapshot_mode = output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE
    data_storage = api.DataStorage(
        storage_type="mongodb",
        connection_string=connection_string,
        database=database,
        table_name=collection,
        max_batch_size=max_batch_size,
        snapshot_maintenance_on_output=is_snapshot_mode,
    )
    data_format = api.DataFormat(
        format_type="bson",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
        with_special_fields=not is_snapshot_mode,
    )

    datasink_type = (
        "snapshot" if output_table_type == SNAPSHOT_OUTPUT_TABLE_TYPE else "sink"
    )
    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name=f"mongodb.{datasink_type}",
            unique_name=name,
            sort_by=sort_by,
        )
    )

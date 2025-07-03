# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Iterable

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    *,
    connection_string: str,
    database: str,
    collection: str,
    max_batch_size: int | None = None,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table``'s stream of updates to a MongoDB table.

    If the specified database or table doesn't exist, it will be created during the
    first write.

    The entries in the resulting table will have two additional fields: ``time``
    and ``diff``. In particular, ``time`` is a processing time of a row
    and ``diff`` shows the nature of the change: ``1`` means a row was added and ``-1``
    means a row was deleted.

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
    """
    data_storage = api.DataStorage(
        storage_type="mongodb",
        connection_string=connection_string,
        database=database,
        table_name=collection,
        max_batch_size=max_batch_size,
    )
    data_format = api.DataFormat(
        format_type="bson",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="mongodb.sink",
            unique_name=name,
            sort_by=sort_by,
        )
    )

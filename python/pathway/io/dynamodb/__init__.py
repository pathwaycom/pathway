# Copyright © 2025 Pathway

from __future__ import annotations

from typing import Literal

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import get_column_index, init_mode_from_str


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    table_name: str,
    partition_key: ColumnReference,
    *,
    sort_key: ColumnReference | None = None,
    init_mode: Literal["default", "create_if_not_exists", "replace"] = "default",
    name: str | None = None,
) -> None:
    """
    Writes ``table`` into a DynamoDB table. The connection settings are retrieved from
    the environment.

    This connector supports three modes: ``default`` mode, which performs no preparation
    on the target table; ``create_if_not_exists`` mode, which creates the table if it does
    not already exist; and ``replace`` mode, which replaces the table and clears any
    previously existing data. The table is created with an
    `on-demand <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/capacity-mode.html>`_
    billing mode. Be aware that this mode may not be optimal for your use case, and the
    provisioned mode with capacity planning might offer better performance or cost
    efficiency. In such cases, we recommend creating the table yourself in AWS with the
    desired provisioned throughput settings.

    Note that if the table already exists and you use either ``default`` or
    ``create_if_not_exists`` mode, the schema of the table, including the primary key and
    optional sort key, must match the schema of the table you are writing.

    The connector performs writes using the primary key, defined as a combination of the
    partition key and an optional sort key. Note that, due to how DynamoDB operates,
    entries may overwrite existing ones if their keys coincide. When an entry is deleted
    from the Pathway table, the corresponding entry is also removed from the DynamoDB
    table maintained by the connector. In this sense, the connector behaves similarly to
    the snapshot mode in the
    `Delta Lake </developers/api-docs/pathway-io/deltalake/#pathway.io.deltalake.write>`_
    output connector or the
    `Postgres </developers/api-docs/pathway-io/postgres#pathway.io.postgres.write_snapshot>`_
    output connector.

    Args:
        table: The table to write.
        table_name: The name of the destination table in DynamoDB.
        partition_key: The column to use as the
            `partition key <https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/>`_
            in the destination table. Note that only scalar types, specifically ``Boolean``,
            ``String`` and ``Number``, can be used as index fields in DynamoDB. Therefore,
            the field you select in the Pathway table must serialize to one of these types.
            You can verify this using the conversion table provided in the connector documentation.
        sort_key: An optional sort key for the destination table. Note that only scalar types can be used as the
            index fields in DynamoDB. Similarly to the partition key, you can only use
            fields that serialize into a scalar DynamoDB type.
        init_mode: The table initialization mode, one of the three described above.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.

    Returns:
        None

    Example:

    AWS provides an official DynamoDB Docker image that allows you to test locally.
    The image is available as ``amazon/dynamodb-local`` and can be run as follows:

    .. code-block:: bash

        docker pull amazon/dynamodb-local:latest
        docker run -p 8000:8000 --name dynamodb-local amazon/dynamodb-local:latest

    The first command pulls the DynamoDB image from the official repository. The second
    command starts a container and exposes port ``8000``, which will be used for the
    connection.

    Since the database runs locally and the settings are retrieved from the environment,
    you will need to configure them accordingly. The easiest way to do this is by setting
    a few environment variables to point to the running Docker image:

    .. code-block:: bash

        export AWS_ENDPOINT_URL=http://localhost:8000
        export AWS_REGION=us-west-2

    Please note that specifying the AWS region is required; however, the exact region does
    not matter for the run to succeed, it simply needs to be set. The endpoint, in turn,
    should point to the database running in the Docker container, accessible through the
    exposed port.

    At this point, the database is ready, and you can start writing a program. For example,
    you can implement a program that stores data in a table in the locally running database.

    First, create a table:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ...    key | value
    ...    1   | Hello
    ...    2   | World
    ... ''')

    Next, save it as follows:

    >>> pw.io.dynamodb.write(
    ...     table,
    ...     table_name="test",
    ...     partition_key=table.key,
    ...     init_mode="create_if_not_exists",
    ... )

    Remember to run your program by calling ``pw.run()``. Note that if the table does not
    already exist, using ``init_mode="default"`` will result in a failure, as Pathway will
    not create the table and the write will fail due to its absence.

    When finished, you can query the local DynamoDB for the table contents using the AWS
    command-line tool:

    .. code-block:: bash

        aws dynamodb scan --table-name test

    This will display the contents of the freshly created table:

    .. code-block:: rst

        {
            "Items": [
                {
                    "value": {
                        "S": "World"
                    },
                    "key": {
                        "N": "2"
                    }
                },
                {
                    "value": {
                        "S": "Hello"
                    },
                    "key": {
                        "N": "1"
                    }
                }
            ],
            "Count": 2,
            "ScannedCount": 2,
            "ConsumedCapacity": null
        }

    Note that since the ``table.key`` field is the partition key, writing an entry with
    the same partition key will overwrite the existing data. For example, you can create
    a smaller table with a repeated key:

    >>> table = pw.debug.table_from_markdown('''
    ...    key | value
    ...    1   | Bonjour
    ... ''')

    Then write it again in ``"default"`` mode:

    >>> pw.io.dynamodb.write(
    ...     table,
    ...     table_name="test",
    ...     partition_key=table.key,
    ... )

    Then, the contents of the target table will be updated with this new entry where
    ``key`` equals to ``1``:

    .. code-block:: rst

        {
            "Items": [
                {
                    "value": {
                        "S": "World"
                    },
                    "key": {
                        "N": "2"
                    }
                },
                {
                    "value": {
                        "S": "Bonjour"
                    },
                    "key": {
                        "N": "1"
                    }
                }
            ],
            "Count": 2,
            "ScannedCount": 2,
            "ConsumedCapacity": null
        }

    Finally, you can run a program in ``"replace"`` table initialization mode, which
    will overwrite the existing data:

    >>> table = pw.debug.table_from_markdown('''
    ...    key | value
    ...    3   | Hi
    ... ''')
    >>> pw.io.dynamodb.write(
    ...     table,
    ...     table_name="test",
    ...     partition_key=table.key,
    ...     init_mode="replace",
    ... )

    The next run of ``aws dynamodb scan --table-name test`` will then return a single-row
    table:

    .. code-block:: rst

        {
            "Items": [
                {
                    "value": {
                        "S": "Hi"
                    },
                    "key": {
                        "N": "3"
                    }
                }
            ],
            "Count": 1,
            "ScannedCount": 1,
            "ConsumedCapacity": null
        }
    """
    _check_entitlements("dynamodb")
    data_storage = api.DataStorage(
        storage_type="dynamodb",
        table_name=table_name,
        table_writer_init_mode=init_mode_from_str(init_mode),
        key_field_index=get_column_index(table, partition_key),
        sort_key_index=get_column_index(table, sort_key),
    )

    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="dynamodb",
            unique_name=name,
        )
    )

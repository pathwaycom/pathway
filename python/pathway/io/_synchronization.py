import datetime

from pathway.internals import api, dtype
from pathway.internals.datasource import GenericDataSource
from pathway.internals.expression import ColumnReference
from pathway.internals.operator import InputOperator
from pathway.internals.parse_graph import G

_SUPPORTED_COLUMN_DTYPES = [
    dtype.INT,
    dtype.DATE_TIME_NAIVE,
    dtype.DATE_TIME_UTC,
    dtype.DURATION,
]


def register_input_synchronization_group(
    *columns: ColumnReference,
    max_difference: api.Value,
    name: str = "default",
):
    """
    Creates a synchronization group for a specified set of columns.
    The set must consist of at least two columns, each belonging to a different table.
    These tables must be read using one of the input connectors (they have to be input tables).
    Transformed tables cannot be used.

    The synchronization group ensures that the engine reads data into the specified tables
    in such a way that the difference between the maximum read values from each column
    does not exceed ``max_difference``.

    All columns must have the same data type to allow for proper comparison,
    and ``max_difference`` must be the result of subtracting values from two columns.

    The logic of synchronization group is the following:
    - If a data source lags behind, the engine will read more data from it to align \
      its values with the others and will continue reading from the other sources \
      only after the lagging one has caught up.
    - If a data source is too fast compared to others, the engine will delay its reading \
      until the slower sources (i.e., those with lower values in their specified columns) \
      catch up.

    Limitations:
    - This mechanism currently works only in runs that use a single Pathway process. The \
      multi-processing support will be added soon.
    - Currently, ``int``, ``DateTimeNaive``, ``DateTimeUtc`` and ``Duration`` field types \
      are supported.

    Please note that all columns within the synchronization group must have the same type.

    Args:
        columns: A list of columns that will be monitored and synchronized.
            Each column must belong to a different table read from an input connector.
        max_difference: The maximum allowed difference between the highest values
            in the tracked columns at any given time. Must be derived from subtracting values
            of two columns specified before.
        name: The name of the synchronization group, used for logging and debugging purposes.

    Returns:
        None

    Example:

    Suppose you have two data sources:
    - ``login_events``, a table read from the Kafka topic ``"logins"``.
    - ``transactions``, a table read from the Kafka topic ``"transactions"``.

    Each table contains a ``timestamp`` field that represents the number of seconds
    since the UNIX Epoch. You want to ensure that these tables are read
    simultaneously, with no more than a 10-minute (600-second) difference
    between their maximum ``timestamp`` values.

    First, you need define the table schema:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...     event_id: str
    ...     unix_timestamp: int
    ...     data: pw.Json
    ...     # Other relevant fields can be added here

    Next, you read both tables from Kafka. Assuming the Kafka server runs on host ``"kafka"``
    and port ``8082``:

    >>> login_events = pw.io.kafka.simple_read("kafka:8082", "logins", format="json", schema=InputSchema)
    >>> transactions = pw.io.kafka.simple_read("kafka:8082", "transactions", format="json", schema=InputSchema)

    Finally, you can synchronize these two tables by creating a synchronization group:

    >>> pw.io.register_input_synchronization_group(
    ...     login_events.unix_timestamp,
    ...     transactions.unix_timestamp,
    ...     max_difference=600,
    ... )

    This ensures that both topics are read in such a way that the difference between the
    maximum ``timestamp`` values at any moment does not exceed 600 seconds (10 minutes).

    Note:

    If all data sources have a gap larger than ``max_difference``, the synchronization group
    will wait until data from all sources arrives. Once all sources move past the gap,
    the synchronization group will allow reading to proceed further.

    **Example scenario:**
    Consider a synchronization group with two data sources, both tracking a ``timestamp``
    column, and ``max_difference`` set to 600 seconds (10 minutes).

    - Initially, both sources send a record with timestamp ``T``.
    - Later, the first source sends a record with ``T + 1h``. \
      This record is not yet forwarded for processing because it exceeds ``max_difference``.
    - If the second source then sends a record with ``T + 1h``, the system detects a 1-hour gap. \
      Since both sources have moved beyond ``T``, the synchronization group accepts ``T + 1h`` \
      as the new baseline and continues processing from there.
    - However, if the second source instead sends a record with ``T + 5m``, this record \
      is processed normally. The system will continue waiting for the first source to \
      catch up before advancing further.

    This behavior ensures that data gaps do not cause deadlocks but are properly detected and handled.
    """

    if len(columns) < 2:
        raise ValueError("At least two columns must participate in a connector group")

    if not isinstance(max_difference, int) and not isinstance(
        max_difference, datetime.timedelta
    ):
        raise ValueError(
            "The 'max_difference' must either be an integer or a datetime.timedelta"
        )

    if isinstance(max_difference, int) and max_difference < 0:
        raise ValueError("The 'max_difference' can't be negative")
    if isinstance(
        max_difference, datetime.timedelta
    ) and max_difference < datetime.timedelta(0):
        raise ValueError("The 'max_difference' can't be negative")

    column_types = set()
    for column in columns:
        column_types.add(column._column.dtype)
        _check_column_type(column, max_difference)

        column_idx = None
        for index, field in enumerate(column._table._schema.column_names()):
            if field == column._name:
                column_idx = index
                break
        if column_idx is None:
            raise ValueError(
                f"Failed to find the column '{column._name}' in table {column._table}"
            )

        is_table_found = False
        for node in G._current_scope.nodes:
            if (
                not isinstance(node, InputOperator)
                or not isinstance(node.datasource, GenericDataSource)
                or node.outputs[0].value != column._table
            ):
                continue
            is_table_found = True
            group = api.ConnectorGroupDescriptor(name, column_idx, max_difference)
            if node.datasource.data_source_options.synchronization_group is not None:
                raise ValueError(
                    "Only one column from a table can be used in a synchronization group"
                )
            node.datasource.data_source_options.set_synchronization_group(group)
            break

        if not is_table_found:
            raise ValueError(
                "Only unchanged columns of an input tables can be used in input synchronization groups"
            )

    if len(column_types) > 1:
        raise ValueError(
            "All synchronization group column types must coincide. "
            "However several types have been detected: {}".format(
                ", ".join(sorted([f"'{t}'" for t in column_types]))
            )
        )


def _check_column_type(column, max_difference):
    column_type = column._column.dtype
    if column_type not in _SUPPORTED_COLUMN_DTYPES:
        raise ValueError(
            f"Fields of type {column_type.typehint} are not supported in connector groups"
        )
    if isinstance(max_difference, int) and column_type != dtype.INT:
        raise ValueError(
            "If max_difference is an integer value, the values of a column must have a type of "
            f"int. However, the column '{column}' has type '{column_type.typehint}'"
        )
    if isinstance(max_difference, datetime.timedelta) and column_type not in (
        dtype.DATE_TIME_NAIVE,
        dtype.DATE_TIME_UTC,
        dtype.DURATION,
    ):
        raise ValueError(
            "If max_difference is a Duration, the values of a column must either have a type of "
            f"DateTimeUtc, DateTimeNaive or Duration. However, the column '{column}' "
            f"has type '{column_type.typehint}'"
        )

# Copyright © 2024 Pathway

import pathway as pw
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def diff(
    self: pw.Table,
    timestamp: pw.ColumnReference,
    *values: pw.ColumnReference,
    instance: pw.ColumnReference | None = None,
) -> pw.Table:
    """
    Compute the difference between the values in the ``values`` columns and the previous values
    according to the order defined by the column ``timestamp``.

    Args:

        timestamp (pw.ColumnReference[int | float | datetime | str | bytes]):
            The column reference to the ``timestamp`` column on which the order is computed.
        *values (pw.ColumnReference[int | float | datetime]):
            Variable-length argument representing the column references to the ``values`` columns.
        instance (pw.ColumnReference):
            Can be used to group the values. The difference is only computed between rows with
            the same ``instance`` value.

    Returns:
        ``Table``: A new table where each column is replaced with a new column containing
        the difference and whose name is the concatenation of `diff_` and the former name.

    Raises:
        ValueError: If the columns are not ColumnReference.

    Note:
        - The value of the "first" value (the row with the lowest value \
        in the ``timestamp`` column) is ``None``.

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ... timestamp | values
    ... 1         | 1
    ... 2         | 2
    ... 3         | 4
    ... 4         | 7
    ... 5         | 11
    ... 6         | 16
    ... ''')
    >>> table += table.diff(pw.this.timestamp, pw.this.values)
    >>> pw.debug.compute_and_print(table, include_id=False)
    timestamp | values | diff_values
    1         | 1      |
    2         | 2      | 1
    3         | 4      | 2
    4         | 7      | 3
    5         | 11     | 4
    6         | 16     | 5

    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ... timestamp | instance | values
    ... 1         | 0        | 1
    ... 2         | 1        | 2
    ... 3         | 1        | 4
    ... 3         | 0        | 7
    ... 6         | 1        | 11
    ... 6         | 0        | 16
    ... '''
    ... )
    >>> table += table.diff(pw.this.timestamp, pw.this.values, instance=pw.this.instance)
    >>> pw.debug.compute_and_print(table, include_id=False)
    timestamp | instance | values | diff_values
    1         | 0        | 1      |
    2         | 1        | 2      |
    3         | 0        | 7      | 6
    3         | 1        | 4      | 2
    6         | 0        | 16     | 9
    6         | 1        | 11     | 7
    """

    if isinstance(timestamp, pw.ColumnReference):
        timestamp = self[timestamp]
    else:
        if isinstance(timestamp, str):
            raise ValueError(
                "statistical.diff(): Invalid column reference for the parameter timestamp,"
                + f" found a string. Did you mean this.{timestamp} instead of {repr(timestamp)}?"
            )
        raise ValueError(
            "statistical.diff(): Invalid column reference for the parameter timestamp."
        )

    ordered_table = self.sort(key=timestamp, instance=instance)

    for value in values:
        if isinstance(value, pw.ColumnReference):
            value = self[value]
        else:
            if isinstance(value, str):
                raise ValueError(
                    "statistical.diff(): Invalid column reference for the parameter value,"
                    + f" found a string. Did you mean this.{value} instead of {repr(value)}?"
                )
            raise ValueError(
                "statistical.diff(): Invalid column reference for the parameter value."
            )

        ordered_table += ordered_table.select(
            diff=pw.require(
                value
                - pw.unwrap(self.ix(ordered_table.prev, optional=True)[value._name]),
                value,
                ordered_table.prev,
            )
        )

        ordered_table = ordered_table.rename({"diff": "diff_" + value.name})

    return ordered_table.without(ordered_table.prev, ordered_table.next)

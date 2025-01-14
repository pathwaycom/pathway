# Copyright © 2024 Pathway

from __future__ import annotations

from enum import Enum

import pathway.internals as pw
from pathway.internals.trace import trace_user_frame


@pw.udf
def _linear_interpolate(
    t: float,
    t_prev: float | None,
    v_prev: float | None,
    t_next: float | None,
    v_next: float | None,
) -> float | None:
    if v_prev is None:
        return v_next
    assert t_prev is not None
    if v_next is None:
        return v_prev
    assert t_next is not None
    return v_prev + (t - t_prev) * (v_next - v_prev) / (t_next - t_prev)


class InterpolateMode(Enum):
    LINEAR = 0


def _retrieving_prev_next_value(tab: pw.Table) -> pw.Table:
    return tab.with_columns(
        prev_value=pw.coalesce(
            pw.this.prev_value, tab.ix(pw.this.prev, optional=True).prev_value
        ),
        next_value=pw.coalesce(
            pw.this.next_value, tab.ix(pw.this.next, optional=True).next_value
        ),
    )


def _retrieve_prev_next_values(ordered_table: pw.Table) -> pw.Table:

    ordered_table = ordered_table[["prev", "next", "value"]]
    ordered_table = ordered_table.with_columns(
        prev_value=pw.require(pw.this.id, pw.this.value),
        next_value=pw.require(pw.this.id, pw.this.value),
    )
    return pw.iterate(_retrieving_prev_next_value, tab=ordered_table)[
        ["prev_value", "next_value"]
    ]


@trace_user_frame
def interpolate(
    self: pw.Table,
    timestamp: pw.ColumnReference,
    *values: pw.ColumnReference,
    mode: InterpolateMode = InterpolateMode.LINEAR,
):
    """
    Interpolates missing values in a column using the previous and next values based on a timestamps column.

    Args:
        timestamp (ColumnReference): Reference to the column containing timestamps.
        *values (ColumnReference): References to the columns containing values to be interpolated.
        mode (InterpolateMode, optional): The interpolation mode. Currently,\
            only InterpolateMode.LINEAR is supported. Default is InterpolateMode.LINEAR.

    Returns:
        Table: A new table with the interpolated values.

    Raises:
        ValueError: If the columns are not ColumnReference or if the interpolation mode is not supported.

    Note:
        - The interpolation is performed based on linear interpolation between the previous and next values.
        - If a value is missing at the beginning or end of the column, no interpolation is performed.

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ... timestamp | values_a | values_b
    ... 1         | 1        | 10
    ... 2         |          |
    ... 3         | 3        |
    ... 4         |          |
    ... 5         |          |
    ... 6         | 6        | 60
    ... ''')
    >>> table = table.interpolate(pw.this.timestamp, pw.this.values_a, pw.this.values_b)
    >>> pw.debug.compute_and_print(table, include_id=False)
    timestamp | values_a | values_b
    1         | 1.0      | 10.0
    2         | 2.0      | 20.0
    3         | 3.0      | 30.0
    4         | 4.0      | 40.0
    5         | 5.0      | 50.0
    6         | 6.0      | 60.0
    """

    if mode != InterpolateMode.LINEAR:
        raise ValueError(
            """interpolate: Invalid mode. Only Interpolate.LINEAR is currently available."""
        )

    if isinstance(timestamp, pw.ColumnReference):
        timestamp = self[timestamp]
    else:
        if isinstance(timestamp, str):
            raise ValueError(
                "Table.interpolate(): Invalid column reference for the parameter timestamp,"
                + f" found a string. Did you mean this.{timestamp} instead of {repr(timestamp)}?"
            )
        raise ValueError(
            "Table.interpolate(): Invalid column reference for the parameter timestamp."
        )

    ordered_table = self.sort(key=timestamp)

    table = self

    for value in values:
        if isinstance(value, pw.ColumnReference):
            value = self[value]
        else:
            if isinstance(value, str):
                raise ValueError(
                    "Table.interpolate(): Invalid column reference for the parameter value,"
                    + f" found a string. Did you mean this.{value} instead of {repr(value)}?"
                )
            raise ValueError(
                "Table.interpolate(): Invalid column reference for the parameter value."
            )
        assert timestamp.name != value.name

        sorted_timestamp_value = ordered_table + self.select(
            timestamp=timestamp, value=value
        )

        table_with_prev_next = _retrieve_prev_next_values(sorted_timestamp_value)

        interpolated_table = table_with_prev_next + sorted_timestamp_value

        prev_tab = interpolated_table.ix(pw.this.prev_value, optional=True)
        next_tab = interpolated_table.ix(pw.this.next_value, optional=True)

        interpolated_table = interpolated_table.with_columns(
            interpolated_value=pw.coalesce(
                pw.this.value,
                _linear_interpolate(
                    pw.this.timestamp,
                    prev_tab.timestamp,
                    prev_tab.value,
                    next_tab.timestamp,
                    next_tab.value,
                ),
            )
        )

        table = table.with_columns(
            **{value.name: interpolated_table.interpolated_value}
        )

    return table

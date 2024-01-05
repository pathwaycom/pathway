# Copyright © 2024 Pathway

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from pathway.internals.expression import ColumnReference
from pathway.internals.trace import trace_user_frame

if TYPE_CHECKING:
    from pathway.internals.table import Table


def _linear_interpolate(t, t_prev, v_prev, t_next, v_next) -> float:
    return v_prev + (t - t_prev) * (v_next - v_prev) / (t_next - t_prev)


def _compute_interpolate(table_with_prev_next: Table) -> Table:
    import pathway.internals as pw

    @pw.transformer
    class computing_interpolate:
        class ordered_ts(pw.ClassArg):
            timestamp = pw.input_attribute()
            value = pw.input_attribute()
            prev_value = pw.input_attribute()
            next_value = pw.input_attribute()

            @pw.output_attribute
            def interpolated_value(self) -> float | None:
                if self.value is not None:
                    return self.value
                t = self.timestamp

                if self.prev_value is None and self.next_value is None:
                    return None
                if self.prev_value is None:
                    return self.transformer.ordered_ts[self.next_value].value
                if self.next_value is None:
                    return self.transformer.ordered_ts[self.prev_value].value

                t_prev = self.transformer.ordered_ts[self.prev_value].timestamp
                value_prev = self.transformer.ordered_ts[self.prev_value].value
                t_next = self.transformer.ordered_ts[self.next_value].timestamp
                value_next = self.transformer.ordered_ts[self.next_value].value
                return _linear_interpolate(t, t_prev, value_prev, t_next, value_next)

    return computing_interpolate(
        ordered_ts=table_with_prev_next
    ).ordered_ts  # type: ignore


class InterpolateMode(Enum):
    LINEAR = 0


@trace_user_frame
def interpolate(
    self: Table,
    timestamp: ColumnReference,
    *values: ColumnReference,
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
    1         | 1        | 10
    2         | 2.0      | 20.0
    3         | 3        | 30.0
    4         | 4.0      | 40.0
    5         | 5.0      | 50.0
    6         | 6        | 60
    """

    from pathway.stdlib.indexing.sorting import retrieve_prev_next_values

    if mode != InterpolateMode.LINEAR:
        raise ValueError(
            """interpolate: Invalid mode. Only Interpolate.LINEAR is currently available."""
        )

    if isinstance(timestamp, ColumnReference):
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
        if isinstance(value, ColumnReference):
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

        table_with_prev_next = retrieve_prev_next_values(sorted_timestamp_value)

        interpolated_table = _compute_interpolate(
            table_with_prev_next + sorted_timestamp_value
        )

        table = table.with_columns(
            **{value.name: interpolated_table.interpolated_value}
        )

    return table

# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Iterable

from pathway.internals.expression import ColumnReference
from pathway.internals.table_subscription import (
    OnChangeCallback,
    OnFinishCallback,
    OnTimeEndCallback,
    subscribe as internal_subscribe,
)


def subscribe(
    table,
    on_change: OnChangeCallback,
    on_end: OnFinishCallback = lambda: None,
    on_time_end: OnTimeEndCallback = lambda time: None,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
):
    """
    Calls a callback function on_change on every change happening in table.

    Args:
        table: the table to subscribe.
        on_change: the callback to be called on every change in the table. The
          function is required to accept four parameters: the key, the row changed, the time
          of the change in microseconds and the flag stating if the change had been an
          addition of the row. These parameters of the callback are expected to have
          names key, row, time and is_addition respectively.
        on_end: the callback to be called when the stream of changes ends.
        on_time_end: the callback function to be called on each closed time of computation.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.
    Returns:
        None

    Example:

    >>> from pathway.tests import utils  # NODOCS
    >>> utils.skip_on_multiple_workers()  # NODOCS
    >>> import pathway as pw
    ...
    >>> table = pw.debug.table_from_markdown('''
    ...      | pet  | owner   | age | __time__ | __diff__
    ...    1 | dog  | Alice   | 10  | 0        | 1
    ...    2 | cat  | Alice   | 8   | 2        | 1
    ...    3 | dog  | Bob     | 7   | 4        | 1
    ...    2 | cat  | Alice   | 8   | 6        | -1
    ... ''')
    ...
    >>> def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
    ...     print(f"{row}, {time}, {is_addition}")
    ...
    >>> def on_end():
    ...     print("End of stream.")
    ...
    >>> pw.io.subscribe(table, on_change, on_end)
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    {'pet': 'dog', 'owner': 'Alice', 'age': 10}, 0, True
    {'pet': 'cat', 'owner': 'Alice', 'age': 8}, 2, True
    {'pet': 'dog', 'owner': 'Bob', 'age': 7}, 4, True
    {'pet': 'cat', 'owner': 'Alice', 'age': 8}, 6, False
    End of stream.
    """

    internal_subscribe(
        table,
        skip_persisted_batch=True,
        on_change=on_change,
        on_time_end=on_time_end,
        on_end=on_end,
        name=name,
        sort_by=sort_by,
    )

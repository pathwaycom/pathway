from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from pathway.internals import datasink
from pathway.internals.api import Pointer


class OnChangeCallback(Protocol):
    def __call__(
        self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
    ) -> Any:
        ...


def subscribe(
    table,
    *,
    skip_persisted_batch: bool,
    on_change: OnChangeCallback,
    on_end: Callable[[], Any] = lambda: None,
):
    """
    Calls a callback function on_change on every change happening in table. This method
    is similar to the one we expose to the user but provides more parameters for
    internal usage.

    Args:
        table: the table to subscribe.
        skip_persisted_batch: whether the output for fully-persisted data should be
          ignored in case the program re-runs. The default usage is True (as not
          outputting things twice is required from persistence). However, it can be
          overridden, which is required by some parts of internal functionality.
        on_change: the callback function to be called on every change in the table. The
          function is required to accept three parameters: the row changed, the time
          of the change in microseconds and the flag stating if the change had been an
          addition of the row. These parameters of the callback are expected to have
          names row, time and is_addition respectively.
        on_end: the callback function to be called when the stream of changes ends.
          It will be called on each engine worker separately.
    Returns:
        None
    """

    def wrapper(key, values, time, diff):
        """
        Wraps a change event from Pathway in a more human-friendly format.

        What we get:
          key: key in Pathway format, e.g. a hash
          values: an array of values of the columns. The order is guaranteed to be the
            same as in the table's schema
          time: time of the change
          diff: diff in the format of +1/-1

          What format do we provide for the user:
            values: a dict from the column name to the column value
            time: time of the change
            is_addition: is this an addition of a row to the collection. In case the field
              if False, that means that this row has been extracted from collection
        """

        row = {}
        for field_name, field_value in zip(table._columns.keys(), values):
            row[field_name] = field_value

        return on_change(key=key, row=row, time=time, is_addition=(diff == 1))

    return table.to(datasink.CallbackDataSink(wrapper, on_end, skip_persisted_batch))

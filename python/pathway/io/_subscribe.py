# Copyright © 2023 Pathway

from __future__ import annotations

from pathway.internals.table_subscription import (
    OnChangeCallback,
    OnFinishCallback,
    subscribe as internal_subscribe,
)


def subscribe(
    table, on_change: OnChangeCallback, on_end: OnFinishCallback = lambda: None
):
    """
    Calls a callback function on_change on every change happening in table.

    Args:
        table: the table to subscribe.
        on_change: the callback to be called on every change in the table. The
          function is required to accept three parameters: the row changed, the time
          of the change in microseconds and the flag stating if the change had been an
          addition of the row. These parameters of the callback are expected to have
          names row, time and is_addition respectively.
        on_end: the callback to be called when the stream of changes ends.
          It will be called on each engine worker separately.
    Returns:
        None
    """

    internal_subscribe(
        table,
        skip_persisted_batch=True,
        on_change=on_change,
        on_end=on_end,
    )

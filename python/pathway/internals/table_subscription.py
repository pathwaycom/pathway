# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import functools
from typing import Any, Awaitable, Callable, Iterable, Protocol, overload

from pathway.internals import Table, datasink
from pathway.internals.api import Pointer
from pathway.internals.expression import ColumnReference
from pathway.internals.table_io import table_to_datasink


class OnFinishCallback(Protocol):
    """
    The callback function to be called when the stream of changes ends. It will be called \
    on each engine worker separately.
    """

    def __call__(self) -> None:
        """
        The callable part of the callback. It will be called without arguments and its
        return result won't be used by the engine.
        """
        ...


class OnChangeCallback(Protocol):
    """
    The callback to be called on every change in the table. It is required to be
    callable and to accept four parameters: the key, the row changed, the time of the
    change in milliseconds and the flag stating if the change was an addition
    of the row.
    """

    def __call__(
        self,
        key: Pointer,
        row: dict[str, Any],
        time: int,
        is_addition: bool,
    ) -> None:
        """
        The callable part of the callback.

        Args:
            key: the key of the changed row;
            row: the changed row as a dict mapping from the field name to the value;
            time: the processing time of the modification, also can be referred as
                minibatch ID of the change;
            is_addition: boolean value, equals to true if the row is inserted into the
                table, false otherwise. Please note that update is basically two operations: the
                deletion of the old value and the insertion of a new value, which happen within a single
                batch;

        Returns:
            None
        """
        ...


class OnChangeCallbackAsync(Protocol):
    """
    The async callback to be called on every change in the table. It is required to be
    callable and to accept four parameters: the key, the row changed, the time of the
    change in milliseconds and the flag stating if the change was an addition
    of the row.
    """

    async def __call__(
        self,
        key: Pointer,
        row: dict[str, Any],
        time: int,
        is_addition: bool,
    ) -> None:
        """
        The callable part of the callback.

        Args:
            key: the key of the changed row;
            row: the changed row as a dict mapping from the field name to the value;
            time: the processing time of the modification, also can be referred as
                minibatch ID of the change;
            is_addition: boolean value, equals to true if the row is inserted into the
                table, false otherwise. Please note that update is basically two operations: the
                deletion of the old value and the insertion of a new value, which happen within a single
                batch;

        Returns:
            None
        """
        ...


class OnTimeEndCallback(Protocol):
    """
    The callback to be called on every time finished. It is required
    to accept one parameter: time.
    """

    def __call__(self, time: int) -> None:
        """
        The callable part of the callback.

        Args:
            time: the time finished
        Returns:
            None
        """
        ...


@overload
def _on_change_wrapper(
    on_change: OnChangeCallbackAsync, table: Table
) -> Callable[..., Awaitable[None]]: ...


@overload
def _on_change_wrapper(
    on_change: OnChangeCallback, table: Table
) -> Callable[..., None]: ...


def _on_change_wrapper(
    on_change: OnChangeCallback | OnChangeCallbackAsync, table: Table
) -> Callable[..., None | Awaitable[None]]:
    """
    Wraps a change event from Pathway in a more human-friendly format.

    Handles synchronous and asynchronous callbacks.

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

    def prepare_row(values: list[Any], diff: int) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for field_name, field_value in zip(table._columns.keys(), values):
            row[field_name] = field_value
        return row

    if asyncio.iscoroutinefunction(on_change):

        @functools.wraps(on_change)
        async def _wrapper(
            key: Pointer, values: list[Any], time: int, diff: int
        ) -> None:
            row = prepare_row(values, diff)
            await on_change(key=key, row=row, time=time, is_addition=(diff >= 1))

        return _wrapper

    else:

        @functools.wraps(on_change)
        def _async_wrapper(
            key: Pointer, values: list[Any], time: int, diff: int
        ) -> None:
            row = prepare_row(values, diff)
            on_change(key=key, row=row, time=time, is_addition=(diff >= 1))

        return _async_wrapper


def subscribe(
    table: Table,
    *,
    skip_persisted_batch: bool,
    on_change: OnChangeCallback | OnChangeCallbackAsync,
    on_time_end: OnTimeEndCallback = lambda time: None,
    on_end: OnFinishCallback = lambda: None,
    skip_errors: bool = True,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
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
          function is required to accept four parameters: the key, the row changed, the time
          of the change in milliseconds and the flag stating if the change had been an
          addition of the row. These parameters of the callback are expected to have
          names row, time and is_addition respectively.
        on_time_end: the callback function to be called on each closed time of computation.
        on_end: the callback function to be called when the stream of changes ends.
        skip_errors: whether to skip rows containing errors
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.
            Incompatible with async callbacks.
    Returns:
        None
    """

    table_to_datasink(
        table,
        datasink.CallbackDataSink(
            on_change=_on_change_wrapper(on_change, table),
            on_time_end=on_time_end,
            on_end=on_end,
            skip_persisted_batch=skip_persisted_batch,
            skip_errors=skip_errors,
            unique_name=name,
            sort_by=sort_by,
        ),
    )

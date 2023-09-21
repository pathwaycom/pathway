# Copyright © 2023 Pathway

from __future__ import annotations

import dataclasses
import datetime
from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Sequence, Tuple, Union

import pathway.internals as pw
from pathway.internals import dtype as dt
from pathway.internals.arg_handlers import arg_handler, windowby_handler
from pathway.internals.desugaring import desugar
from pathway.internals.join import validate_join_condition
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame
from pathway.stdlib import temporal
from pathway.stdlib.temporal.utils import (
    IntervalType,
    TimeEventType,
    check_joint_types,
    get_default_shift,
)


class Window(ABC):
    @abstractmethod
    def _apply(
        self,
        table: pw.Table,
        key: pw.ColumnExpression,
        shard: Optional[pw.ColumnExpression],
    ) -> pw.GroupedTable:
        ...

    @abstractmethod
    def _join(
        self,
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        *on: pw.ColumnExpression,
        mode: pw.JoinMode,
    ) -> temporal.WindowJoinResult:
        ...


_SessionPredicateType = Callable[[Any, Any], bool]


@dataclasses.dataclass
class _SessionWindow(Window):
    predicate: Optional[_SessionPredicateType]
    max_gap: Optional[IntervalType]

    def _merge(
        self, cur: pw.ColumnExpression, next: pw.ColumnExpression
    ) -> pw.ColumnExpression:
        if self.predicate is not None:
            return pw.apply_with_type(self.predicate, bool, cur, next)
        else:
            return next - cur < self.max_gap

    def _compute_group_repr(
        self,
        table: pw.Table,
        key: pw.ColumnExpression,
        shard: Optional[pw.ColumnExpression],
    ) -> pw.Table:
        target = table.select(key=key, instance=shard)
        import pathway.stdlib.indexing

        target = target + pathway.stdlib.indexing.sort_from_index(
            **pathway.stdlib.indexing.build_sorted_index(target)
        )

        sel_key = target.select(next_key=target.ix(target.next, optional=True).key)
        target += target.select(
            _pw_window=pw.if_else(
                sel_key.next_key.is_not_none(),
                pw.if_else(
                    self._merge(target.key, pw.unwrap(sel_key.next_key)),
                    target.next,
                    target.id,
                ),
                target.id,
            ),
        ).update_types(_pw_window=pw.Pointer)

        def merge_ccs(data):
            data <<= data.select(_pw_window=data.ix(data._pw_window)._pw_window)
            return dict(data=data)

        return pw.iterate(merge_ccs, data=target).data

    @runtime_type_check
    def _apply(
        self,
        table: pw.Table,
        key: pw.ColumnExpression,
        shard: Optional[pw.ColumnExpression],
    ) -> pw.GroupedTable:
        if self.max_gap is not None:
            check_joint_types(
                {
                    "time_expr": (key, TimeEventType),
                    "window.max_gap": (self.max_gap, IntervalType),
                }
            )

        target = self._compute_group_repr(table, key, shard)
        tmp = target.groupby(target._pw_window).reduce(
            _pw_window_start=pw.reducers.min(key),
            _pw_window_end=pw.reducers.max(key),
        )

        gb = table.with_columns(
            target._pw_window,
            tmp.ix_ref(target._pw_window)._pw_window_start,
            tmp.ix_ref(target._pw_window)._pw_window_end,
            _pw_shard=shard,
        ).groupby(
            pw.this._pw_window,
            pw.this._pw_window_start,
            pw.this._pw_window_end,
            pw.this._pw_shard,
        )

        return gb

    @runtime_type_check
    def _join(
        self,
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        *on: pw.ColumnExpression,
        mode: pw.JoinMode,
    ) -> temporal.WindowJoinResult:
        def maybe_make_tuple(
            conditions: Sequence[pw.ColumnExpression],
        ) -> pw.ColumnExpression:
            if len(conditions) > 1:
                return pw.make_tuple(*conditions)
            elif len(conditions) == 1:
                return conditions[0]
            else:
                return None  # type: ignore

        check_joint_types(
            {
                "left_time_expression": (left_time_expression, TimeEventType),
                "right_time_expression": (right_time_expression, TimeEventType),
                "window.max_gap": (self.max_gap, IntervalType),
            }
        )

        left_on: List[pw.ColumnReference] = []
        right_on: List[pw.ColumnReference] = []
        for cond in on:
            cond_left, cond_right, _ = validate_join_condition(cond, left, right)
            left_on.append(cond_left)
            right_on.append(cond_right)

        concatenated_events = pw.Table.concat_reindex(
            left.select(
                key=left_time_expression,
                shard=maybe_make_tuple(left_on),
                is_left=True,
                original_id=left.id,
            ),
            right.select(
                key=right_time_expression,
                shard=maybe_make_tuple(right_on),
                is_left=False,
                original_id=right.id,
            ),
        )
        group_repr = self._compute_group_repr(
            concatenated_events, concatenated_events.key, concatenated_events.shard
        )
        tmp = group_repr.groupby(group_repr._pw_window).reduce(
            _pw_window_start=pw.reducers.min(concatenated_events.key),
            _pw_window_end=pw.reducers.max(concatenated_events.key),
        )
        session_ids = concatenated_events.with_columns(
            group_repr._pw_window,
            tmp.ix_ref(group_repr._pw_window)._pw_window_start,
            tmp.ix_ref(group_repr._pw_window)._pw_window_end,
        )

        left_session_ids = (
            session_ids.filter(session_ids.is_left)
            .with_id(session_ids.original_id)
            .with_universe_of(left)
        )
        right_session_ids = (
            session_ids.filter(~session_ids.is_left)
            .with_id(session_ids.original_id)
            .with_universe_of(right)
        )

        left_with_session_id = left.with_columns(
            left_session_ids._pw_window,
            left_session_ids._pw_window_start,
            left_session_ids._pw_window_end,
        )
        right_with_session_id = right.with_columns(
            right_session_ids._pw_window,
            right_session_ids._pw_window_start,
            right_session_ids._pw_window_end,
        )

        join_result = pw.JoinResult._table_join(
            left_with_session_id,
            right_with_session_id,
            left_with_session_id._pw_window_start
            == right_with_session_id._pw_window_start,
            left_with_session_id._pw_window_end == right_with_session_id._pw_window_end,
            left_with_session_id._pw_window == right_with_session_id._pw_window,
            *[
                left_with_session_id[left_cond.name]
                == right_with_session_id[right_cond.name]
                for left_cond, right_cond in zip(left_on, right_on)
            ],
            mode=mode,
        )

        return temporal.WindowJoinResult(
            join_result, left, right, left_with_session_id, right_with_session_id
        )


@dataclasses.dataclass
class _SlidingWindow(Window):
    hop: IntervalType
    duration: Optional[IntervalType]
    ratio: Optional[int]
    offset: Optional[TimeEventType]
    shift: TimeEventType

    def __init__(
        self,
        hop: IntervalType,
        duration: Optional[IntervalType],
        offset: Optional[TimeEventType],
        ratio: Optional[int],
    ) -> None:
        if offset is None:
            self.shift = get_default_shift(hop)
        else:
            self.shift = offset

        self.hop = hop
        self.duration = duration
        self.ratio = ratio
        self.offset = offset

    def _kth_stable_window(self, k):
        """Numerically stable k-th window."""
        start = k * self.hop + self.shift

        if self.ratio is not None:
            end = (k + self.ratio) * self.hop + self.shift
        else:
            end = k * self.hop + self.shift + self.duration

        return (start, end)

    def _assign_windows(
        self, shard: Any, key: TimeEventType
    ) -> List[Tuple[Any, TimeEventType, TimeEventType]]:
        """Returns the list of all the windows the given key belongs to.

        Each window is a tuple (window_start, window_end) describing the range
        of the window (window_start inclusive, window_end exclusive).
        """
        # compute lower and upper bound for multipliers (first_k and last_k) of hop
        # for which corresponding windows could contain key.
        last_k = int((key - self.shift) // self.hop) + 1  # type: ignore[operator, arg-type]
        if self.ratio is not None:
            first_k = last_k - self.ratio - 1
        else:
            assert self.duration is not None
            first_k = last_k - int(self.duration // self.hop) - 1  # type: ignore[operator, arg-type]
        first_k -= 1  # safety to avoid off-by one

        candidate_windows = [
            self._kth_stable_window(k) for k in range(first_k, last_k + 1)
        ]

        # filtering below is needed to handle case when hop > duration
        return [
            (shard, start, end)
            for (start, end) in candidate_windows
            if start <= key
            and key < end
            and (self.offset is None or start >= self.offset)
        ]

    @runtime_type_check
    def _apply(
        self,
        table: pw.Table,
        key: pw.ColumnExpression,
        shard: Optional[pw.ColumnExpression],
    ) -> pw.GroupedTable:
        check_joint_types(
            {
                "time_expr": (key, TimeEventType),
                "window.hop": (self.hop, IntervalType),
                "window.duration": (self.duration, IntervalType),
                "window.offset": (self.offset, TimeEventType),
            }
        )

        from pathway.internals.type_interpreter import eval_type

        target = table.select(
            _pw_window=pw.apply_with_type(
                self._assign_windows,
                dt.List(
                    dt.Tuple(
                        eval_type(shard),  # type:ignore
                        eval_type(key),
                        eval_type(key),
                    )
                ),
                shard,
                key,
            )
        )
        target = target.flatten(target._pw_window, *table)
        target = target.with_columns(
            _pw_shard=pw.this._pw_window.get(0),
            _pw_window_start=pw.this._pw_window.get(1),
            _pw_window_end=pw.this._pw_window.get(2),
        )
        return target.groupby(
            target._pw_window,
            target._pw_shard,
            target._pw_window_start,
            target._pw_window_end,
        )

    @runtime_type_check
    def _join(
        self,
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        *on: pw.ColumnExpression,
        mode: pw.JoinMode,
    ) -> temporal.WindowJoinResult:
        from pathway.internals.type_interpreter import eval_type

        check_joint_types(
            {
                "left_time_expression": (left_time_expression, TimeEventType),
                "right_time_expression": (right_time_expression, TimeEventType),
                "window.hop": (self.hop, IntervalType),
                "window.duration": (self.duration, IntervalType),
                "window.offset": (self.offset, TimeEventType),
            }
        )

        left_window = left.select(
            _pw_window=pw.apply_with_type(
                self._assign_windows,
                dt.List(
                    dt.Tuple(
                        dt.NONE,
                        eval_type(left_time_expression),
                        eval_type(left_time_expression),
                    )
                ),
                None,
                left_time_expression,
            )
        )
        left_window = left_window.flatten(left_window._pw_window, *left)

        left_window = left_window.with_columns(
            _pw_window_start=pw.this._pw_window.get(1),
            _pw_window_end=pw.this._pw_window.get(2),
        )

        right_window = right.select(
            _pw_window=pw.apply_with_type(
                self._assign_windows,
                dt.List(
                    dt.Tuple(
                        dt.NONE,
                        eval_type(right_time_expression),
                        eval_type(right_time_expression),
                    )
                ),
                None,
                right_time_expression,
            )
        )
        right_window = right_window.flatten(right_window._pw_window, *right)

        right_window = right_window.with_columns(
            _pw_window_start=pw.this._pw_window.get(1),
            _pw_window_end=pw.this._pw_window.get(2),
        )

        for cond in on:
            cond_left, cond_right, cond = validate_join_condition(cond, left, right)
            cond._left = left_window[cond_left._name]
            cond._right = right_window[cond_right._name]

        join_result = pw.JoinResult._table_join(
            left_window,
            right_window,
            left_window._pw_window_start == right_window._pw_window_start,
            left_window._pw_window_end == right_window._pw_window_end,
            left_window._pw_window == right_window._pw_window,
            *on,
            mode=mode,
        )

        return temporal.WindowJoinResult(
            join_result, left, right, left_window, right_window
        )


@dataclasses.dataclass
class _IntervalsOverWindow(Window):
    at: pw.ColumnReference
    lower_bound: Union[int, float, datetime.timedelta]
    upper_bound: Union[int, float, datetime.timedelta]

    @runtime_type_check
    def _apply(
        self,
        table: pw.Table,
        key: pw.ColumnExpression,
        shard: Optional[pw.ColumnExpression],
    ) -> pw.GroupedTable:
        check_joint_types(
            {
                "time_expr": (key, TimeEventType),
                "window.lower_bound": (self.lower_bound, IntervalType),
                "window.upper_bound": (self.upper_bound, IntervalType),
                "window.at": (self.at, TimeEventType),
            }
        )

        if self.at.table == table:
            at_table = self.at.table.copy()
            at = at_table[self.at.name]
        else:
            at_table = self.at.table
            at = self.at
        return (
            temporal.interval_join(
                at_table,
                table,
                at,
                key,
                temporal.interval(self.lower_bound, self.upper_bound),
            )
            .select(
                _pw_window_location=pw.left[self.at.name],
                _pw_window_start=pw.left[self.at.name] + self.lower_bound,
                _pw_window_end=pw.left[self.at.name] + self.upper_bound,
                _pw_shard=shard,
                *pw.right,
            )
            .groupby(
                pw.this._pw_window_location,
                pw.this._pw_window_start,
                pw.this._pw_window_end,
                pw.this._pw_shard,
            )
        )

    @runtime_type_check
    def _join(
        self,
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        *on: pw.ColumnExpression,
        mode: pw.JoinMode,
    ) -> temporal.WindowJoinResult:
        raise NotImplementedError(
            "window_join doesn't support windows of type intervals_over"
        )


@runtime_type_check
@trace_user_frame
def session(
    *,
    predicate: Optional[_SessionPredicateType] = None,
    max_gap: Optional[Union[int, float, datetime.timedelta]] = None,
) -> Window:
    """Allows grouping together elements within a window across ordered time-like
    data column by locally grouping adjacent elements either based on a maximum time
    difference or using a custom predicate.

    Note:
        Usually used as an argument of `.windowby()`.
        Exactly one of the arguments `predicate` or `max_gap` should be provided.

    Args:
        predicate: function taking two adjacent entries that returns a boolean saying
            whether the two entries should be grouped
        max_gap: Two adjacent entries will be grouped if `b - a < max_gap`

    Returns:
        Window: object to pass as an argument to `.windowby()`

    Examples:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ... '''
    ...     | shard |  t |  v
    ... 1   | 0     |  1 |  10
    ... 2   | 0     |  2 |  1
    ... 3   | 0     |  4 |  3
    ... 4   | 0     |  8 |  2
    ... 5   | 0     |  9 |  4
    ... 6   | 0     |  10|  8
    ... 7   | 1     |  1 |  9
    ... 8   | 1     |  2 |  16
    ... ''')
    >>> result = t.windowby(
    ...     t.t, window=pw.temporal.session(predicate=lambda a, b: abs(a-b) <= 1), shard=t.shard
    ... ).reduce(
    ... pw.this._pw_shard,
    ... pw.this._pw_window_start,
    ... pw.this._pw_window_end,
    ... min_t=pw.reducers.min(pw.this.t),
    ... max_v=pw.reducers.max(pw.this.v),
    ... count=pw.reducers.count(),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    _pw_shard | _pw_window_start | _pw_window_end | min_t | max_v | count
    0         | 1                | 2              | 1     | 10    | 2
    0         | 4                | 4              | 4     | 3     | 1
    0         | 8                | 10             | 8     | 8     | 3
    1         | 1                | 2              | 1     | 16    | 2
    """
    if predicate is None and max_gap is None:
        raise ValueError(
            "At least one of the parameters [predicate, max_gap] should be provided."
        )
    elif predicate is not None and max_gap is not None:
        raise ValueError("Cannot provide both [predicate, max_gap] at the same time.")
    return _SessionWindow(predicate=predicate, max_gap=max_gap)


@runtime_type_check
@trace_user_frame
def sliding(
    hop: Union[int, float, datetime.timedelta],
    duration: Optional[Union[int, float, datetime.timedelta]] = None,
    ratio: Optional[int] = None,
    offset: Optional[Union[int, float, datetime.datetime]] = None,
) -> Window:
    """Allows grouping together elements within a window of a given length sliding
    across ordered time-like data column according to a specified interval (hop)
    starting from a given offset.

    Note:
        Usually used as an argument of `.windowby()`.
        Exactly one of the arguments `hop` or `ratio` should be provided.

    Args:
        hop: frequency of a window
        duration: length of the window
        ratio: used as an alternative way to specify duration as hop * ratio
        offset: beginning of the first window

    Returns:
        Window: object to pass as an argument to `.windowby()`

    Examples:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ... '''
    ...        | shard | t
    ...    1   | 0     |  12
    ...    2   | 0     |  13
    ...    3   | 0     |  14
    ...    4   | 0     |  15
    ...    5   | 0     |  16
    ...    6   | 0     |  17
    ...    7   | 1     |  10
    ...    8   | 1     |  11
    ... ''')
    >>> result = t.windowby(
    ...     t.t, window=pw.temporal.sliding(duration=10, hop=3), shard=t.shard
    ... ).reduce(
    ...   pw.this._pw_shard,
    ...   pw.this._pw_window_start,
    ...   pw.this._pw_window_end,
    ...   min_t=pw.reducers.min(pw.this.t),
    ...   max_t=pw.reducers.max(pw.this.t),
    ...   count=pw.reducers.count(),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    _pw_shard | _pw_window_start | _pw_window_end | min_t | max_t | count
    0         | 3                | 13             | 12    | 12    | 1
    0         | 6                | 16             | 12    | 15    | 4
    0         | 9                | 19             | 12    | 17    | 6
    0         | 12               | 22             | 12    | 17    | 6
    0         | 15               | 25             | 15    | 17    | 3
    1         | 3                | 13             | 10    | 11    | 2
    1         | 6                | 16             | 10    | 11    | 2
    1         | 9                | 19             | 10    | 11    | 2
    """
    if duration is None and ratio is None:
        raise ValueError(
            "At least one of the parameters [duration, ratio] should be provided."
        )
    elif duration is not None and ratio is not None:
        raise ValueError("Cannot provide both [duration, ratio] at the same time.")

    return _SlidingWindow(
        duration=duration,
        hop=hop,
        ratio=ratio,
        offset=offset,
    )


@runtime_type_check
@trace_user_frame
def tumbling(
    duration: Union[int, float, datetime.timedelta],
    offset: Optional[Union[int, float, datetime.datetime]] = None,
) -> Window:
    """Allows grouping together elements within a window of a given length tumbling
    across ordered time-like data column starting from a given offset.

    Note:
        Usually used as an argument of `.windowby()`.

    Args:
        duration: length of the window
        offset: beginning of the first window

    Returns:
        Window: object to pass as an argument to `.windowby()`

    Examples:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ... '''
    ...        | shard | t
    ...    1   | 0     |  12
    ...    2   | 0     |  13
    ...    3   | 0     |  14
    ...    4   | 0     |  15
    ...    5   | 0     |  16
    ...    6   | 0     |  17
    ...    7   | 1     |  12
    ...    8   | 1     |  13
    ... ''')
    >>> result = t.windowby(
    ...     t.t, window=pw.temporal.tumbling(duration=5), shard=t.shard
    ... ).reduce(
    ...   pw.this._pw_shard,
    ...   pw.this._pw_window_start,
    ...   pw.this._pw_window_end,
    ...   min_t=pw.reducers.min(pw.this.t),
    ...   max_t=pw.reducers.max(pw.this.t),
    ...   count=pw.reducers.count(),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    _pw_shard | _pw_window_start | _pw_window_end | min_t | max_t | count
    0         | 10               | 15             | 12    | 14    | 3
    0         | 15               | 20             | 15    | 17    | 3
    1         | 10               | 15             | 12    | 13    | 2
    """
    return _SlidingWindow(
        duration=None,
        hop=duration,
        ratio=1,
        offset=offset,
    )


@runtime_type_check
@trace_user_frame
def intervals_over(
    *,
    at: pw.ColumnReference,
    lower_bound: Union[int, float, datetime.timedelta],
    upper_bound: Union[int, float, datetime.timedelta],
) -> Window:
    """Allows grouping together elements within a window.

    Windows are created for each time t in at, by taking values with times
    within [t+lower_bound, t+upper_bound].

    Args:
        lower_bound: lower bound for interval
        upper_bound: upper bound for interval
        at: column of times for which windows are to be created

    Returns:
        Window: object to pass as an argument to `.windowby()`

    Examples:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ... '''
    ...     | t |  v
    ... 1   | 1 |  10
    ... 2   | 2 |  1
    ... 3   | 4 |  3
    ... 4   | 8 |  2
    ... 5   | 9 |  4
    ... 6   | 10|  8
    ... 7   | 1 |  9
    ... 8   | 2 |  16
    ... ''')
    >>> probes = pw.debug.table_from_markdown(
    ... '''
    ... t
    ... 2
    ... 4
    ... 6
    ... 8
    ... 10
    ... ''')
    >>> result = (
    ...     pw.temporal.windowby(t, t.t, window=pw.temporal.intervals_over(
    ...         at=probes.t, lower_bound=-2, upper_bound=1
    ...      ))
    ...     .reduce(pw.this._pw_window_location, v=pw.reducers.tuple(pw.this.v))
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    _pw_window_location | v
    2                   | (16, 1, 10, 9)
    4                   | (3, 16, 1)
    6                   | (3,)
    8                   | (4, 2)
    10                  | (4, 8, 2)
    """
    return _IntervalsOverWindow(at, lower_bound, upper_bound)


@trace_user_frame
@desugar
@arg_handler(handler=windowby_handler)
@runtime_type_check
def windowby(
    self: pw.Table,
    time_expr: pw.ColumnExpression,
    *,
    window: Window,
    shard: Optional[pw.ColumnExpression] = None,
) -> pw.GroupedTable:
    """
    Create a GroupedTable by windowing the table (based on `expr` and `window`),
    optionally sharded with `shard`

    Args:
        time_expr: Column expression used for windowing
        window: type window to use
        shard: optional column expression to act as a shard key

    Examples:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ... '''
    ...     | shard |  t |  v
    ... 1   | 0     |  1 |  10
    ... 2   | 0     |  2 |  1
    ... 3   | 0     |  4 |  3
    ... 4   | 0     |  8 |  2
    ... 5   | 0     |  9 |  4
    ... 6   | 0     |  10|  8
    ... 7   | 1     |  1 |  9
    ... 8   | 1     |  2 |  16
    ... ''')
    >>> result = t.windowby(
    ...     t.t, window=pw.temporal.session(predicate=lambda a, b: abs(a-b) <= 1), shard=t.shard
    ... ).reduce(
    ... pw.this.shard,
    ... min_t=pw.reducers.min(pw.this.t),
    ... max_v=pw.reducers.max(pw.this.v),
    ... count=pw.reducers.count(),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    shard | min_t | max_v | count
    0     | 1     | 10    | 2
    0     | 4     | 3     | 1
    0     | 8     | 8     | 3
    1     | 1     | 16    | 2
    """

    return window._apply(self, time_expr, shard)

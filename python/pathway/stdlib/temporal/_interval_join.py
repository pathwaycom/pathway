# Copyright © 2024 Pathway


from __future__ import annotations

import datetime
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, overload

import pathway.internals as pw
from pathway.internals.arg_handlers import (
    arg_handler,
    join_kwargs_handler,
    select_args_handler,
)
from pathway.internals.desugaring import (
    DesugaringContext,
    TableReplacementWithNoneDesugaring,
    TableSubstitutionDesugaring,
    combine_args_kwargs,
    desugar,
)
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame
from pathway.internals.type_interpreter import eval_type

from .temporal_behavior import CommonBehavior, apply_temporal_behavior
from .utils import IntervalType, TimeEventType, check_joint_types, get_default_origin

T = TypeVar("T")


@dataclass
class Interval(Generic[T]):
    lower_bound: T
    upper_bound: T


@overload
def interval(
    lower_bound: int,
    upper_bound: int,
) -> Interval[int]: ...


@overload
def interval(
    lower_bound: float,
    upper_bound: float,
) -> Interval[float]: ...


@overload
def interval(
    lower_bound: datetime.timedelta,
    upper_bound: datetime.timedelta,
) -> Interval[datetime.timedelta]: ...


def interval(
    lower_bound: int | float | datetime.timedelta,
    upper_bound: int | float | datetime.timedelta,
) -> Interval:
    """Allows testing whether two times are within a certain distance.

    Note:
        Usually used as an argument of `.interval_join()`.

    Args:
        lower_bound: a lower bound on `other_time - self_time`.
        upper_bound: an upper bound on `other_time - self_time`.

    Returns:
        Window: object to pass as an argument to `.interval_join()`

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 3
    ...   2 | 4
    ...   3 | 5
    ...   4 | 11
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 0
    ...   2 | 1
    ...   3 | 4
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.interval_join(t2, t1.t, t2.t, pw.temporal.interval(-2, 1)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    3      | 1
    3      | 4
    4      | 4
    5      | 4
    """
    return Interval(lower_bound=lower_bound, upper_bound=upper_bound)


class IntervalJoinResult(DesugaringContext):
    """
    Result of an interval join between tables.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 3
    ...   2 | 4
    ...   3 | 5
    ...   4 | 11
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 0
    ...   2 | 1
    ...   3 | 4
    ...   4 | 7
    ... '''
    ... )
    >>> join_result = t1.interval_join_inner(t2, t1.t, t2.t, pw.temporal.interval(-2, 1))
    >>> isinstance(join_result, pw.temporal.IntervalJoinResult)
    True
    >>> pw.debug.compute_and_print(
    ...     join_result.select(left_t=t1.t, right_t=t2.t), include_id=False
    ... )
    left_t | right_t
    3      | 1
    3      | 4
    4      | 4
    5      | 4
    """

    _table_substitution: dict[pw.TableLike, pw.Table]
    _filter_out_results_of_forgetting: bool

    def __init__(
        self,
        left: pw.Table,
        right: pw.Table,
        table_substitution: dict[pw.TableLike, pw.Table],
        _filter_out_results_of_forgetting: bool,
    ):
        self._substitution = {
            pw.left: left,
            pw.right: right,
            pw.this: pw.this,  # type: ignore[dict-item]
        }
        self._table_substitution = table_substitution
        self._filter_out_results_of_forgetting = _filter_out_results_of_forgetting

    @staticmethod
    @staticmethod
    def _should_filter_out_results_of_forgetting(
        behavior: CommonBehavior | None,
    ) -> bool:
        return (
            behavior is not None
            and behavior.cutoff is not None
            and behavior.keep_results
        )

    @staticmethod
    def _interval_join(
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
        *on: pw.ColumnExpression,
        behavior: CommonBehavior | None = None,
        mode: pw.JoinMode,
        left_instance: pw.ColumnReference | None = None,
        right_instance: pw.ColumnReference | None = None,
    ) -> IntervalJoinResult:
        """Creates an IntervalJoinResult. To perform an interval join uses it uses two
        tumbling windows of size `lower_bound` + `upper_bound` and then filters the result.
        """
        check_joint_types(
            {
                "self_time_expression": (left_time_expression, TimeEventType),
                "other_time_expression": (right_time_expression, TimeEventType),
                "lower_bound": (interval.lower_bound, IntervalType),
                "upper_bound": (interval.upper_bound, IntervalType),
            }
        )
        if left == right:
            raise ValueError(
                "Cannot join table with itself. Use <table>.copy() as one of the arguments of the join."
            )
        if interval.lower_bound > interval.upper_bound:  # type: ignore[operator]
            raise ValueError(
                "lower_bound has to be less than or equal to the upper_bound in the Table.interval_join()."
            )

        if interval.lower_bound == interval.upper_bound:
            cls: type[IntervalJoinResult] = _ZeroDifferenceIntervalJoinResult
        else:
            cls = _NonZeroDifferenceIntervalJoinResult
        return cls._interval_join(
            left,
            right,
            left_time_expression,
            right_time_expression,
            interval,
            *on,
            behavior=behavior,
            mode=mode,
            left_instance=left_instance,
            right_instance=right_instance,
        )

    @property
    def _desugaring(self) -> TableSubstitutionDesugaring:
        return TableSubstitutionDesugaring(self._table_substitution)

    @abstractmethod
    def select(self, *args: pw.ColumnReference, **kwargs: Any) -> pw.Table:
        """
        Computes a result of an interval join.

        Args:
            args: Column references.
            kwargs: Column expressions with their new assigned names.

        Returns:
            Table: Created table.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown(
        ...     '''
        ...     | a | t
        ...   1 | 1 | 3
        ...   2 | 1 | 4
        ...   3 | 1 | 5
        ...   4 | 1 | 11
        ...   5 | 2 | 2
        ...   6 | 2 | 3
        ...   7 | 3 | 4
        ... '''
        ... )
        >>> t2 = pw.debug.table_from_markdown(
        ...     '''
        ...     | b | t
        ...   1 | 1 | 0
        ...   2 | 1 | 1
        ...   3 | 1 | 4
        ...   4 | 1 | 7
        ...   5 | 2 | 0
        ...   6 | 2 | 2
        ...   7 | 4 | 2
        ... '''
        ... )
        >>> t3 = t1.interval_join_inner(
        ...     t2, t1.t, t2.t, pw.temporal.interval(-2, 1), t1.a == t2.b
        ... ).select(t1.a, left_t=t1.t, right_t=t2.t)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        a | left_t | right_t
        1 | 3      | 1
        1 | 3      | 4
        1 | 4      | 4
        1 | 5      | 4
        2 | 2      | 0
        2 | 2      | 2
        2 | 3      | 2
        """
        ...


class _NonZeroDifferenceIntervalJoinResult(IntervalJoinResult):
    _left_bucketed: pw.Table
    _right_bucketed: pw.Table
    _earlier_part_filtered: pw.JoinResult
    _later_part_filtered: pw.JoinResult
    _mode: pw.JoinMode

    def __init__(
        self,
        left_bucketed: pw.Table,
        right_bucketed: pw.Table,
        earlier_part_filtered: pw.JoinResult,
        later_part_filtered: pw.JoinResult,
        table_substitution: dict[pw.TableLike, pw.Table],
        mode: pw.JoinMode,
        _filter_out_results_of_forgetting: bool,
    ):
        super().__init__(
            left_bucketed,
            right_bucketed,
            table_substitution=table_substitution,
            _filter_out_results_of_forgetting=_filter_out_results_of_forgetting,
        )
        self._left_bucketed = left_bucketed
        self._right_bucketed = right_bucketed
        self._earlier_part_filtered = earlier_part_filtered
        self._later_part_filtered = later_part_filtered
        self._mode = mode

    @staticmethod
    def _interval_join(
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
        *on: pw.ColumnExpression,
        behavior: CommonBehavior | None = None,
        mode: pw.JoinMode,
        left_instance: pw.ColumnReference | None = None,
        right_instance: pw.ColumnReference | None = None,
    ) -> IntervalJoinResult:
        if left_instance is not None and right_instance is not None:
            on = (*on, left_instance == right_instance)
        else:
            assert left_instance is None and right_instance is None
        assert left != right
        assert interval.lower_bound < interval.upper_bound  # type: ignore[operator]

        shift = get_default_origin(eval_type(left_time_expression))
        left_with_time = left.with_columns(_pw_time=left_time_expression)
        right_with_time = right.with_columns(_pw_time=right_time_expression)
        left_with_time = apply_temporal_behavior(left_with_time, behavior)
        right_with_time = apply_temporal_behavior(right_with_time, behavior)
        bounds_difference = interval.upper_bound - interval.lower_bound  # type: ignore[operator]

        left_bucketed = left_with_time.with_columns(
            _pw_bucket=pw.cast(int, (pw.this._pw_time - shift) // bounds_difference)
        )
        right_bucketed = right_with_time.with_columns(
            _pw_bucket=pw.cast(
                int,
                (pw.this._pw_time - shift - interval.upper_bound) // bounds_difference,
            ),
        )

        right_bucketed = right_bucketed.with_columns(
            _pw_bucket_plus_one=right_bucketed._pw_bucket + 1
        )

        from pathway.internals.joins import validate_join_condition

        for cond in on:
            cond_left, cond_right, cond = validate_join_condition(cond, left, right)
            cond._left = left_bucketed[cond_left._name]
            cond._right = right_bucketed[cond_right._name]

        earlier_part = left_bucketed.join(
            right_bucketed,
            left_bucketed._pw_bucket == right_bucketed._pw_bucket,
            *on,
        )
        later_part = left_bucketed.join(
            right_bucketed,
            left_bucketed._pw_bucket == right_bucketed._pw_bucket_plus_one,
            *on,
        )
        pw.universes.promise_are_pairwise_disjoint(earlier_part, later_part)

        earlier_part_filtered = earlier_part.filter(
            pw.right._pw_time <= pw.left._pw_time + interval.upper_bound
        )
        later_part_filtered = later_part.filter(
            pw.left._pw_time + interval.lower_bound <= pw.right._pw_time
        )

        table_substitution: dict[pw.TableLike, pw.Table] = {
            left: left_bucketed,
            right: right_bucketed,
        }

        filter_out_results_of_forgetting = (
            IntervalJoinResult._should_filter_out_results_of_forgetting(behavior)
        )

        return _NonZeroDifferenceIntervalJoinResult(
            left_bucketed,
            right_bucketed,
            earlier_part_filtered,
            later_part_filtered,
            table_substitution,
            mode,
            _filter_out_results_of_forgetting=filter_out_results_of_forgetting,
        )

    @desugar
    @arg_handler(handler=select_args_handler)
    @trace_user_frame
    def select(self, *args: pw.ColumnReference, **kwargs: Any) -> pw.Table:
        exclude_columns = {"_pw_time", "_pw_bucket", "_pw_bucket_plus_one"}
        # remove internal columns that can appear if using *pw.left, *pw.right
        all_args = combine_args_kwargs(args, kwargs, exclude_columns=exclude_columns)

        earlier_part_selected = self._earlier_part_filtered.select(
            _pw_left_id=pw.left.id,
            _pw_right_id=pw.right.id,
            **all_args,
        )

        later_part_selected = self._later_part_filtered.select(
            _pw_left_id=pw.left.id,
            _pw_right_id=pw.right.id,
            **all_args,
        )

        joined = earlier_part_selected.concat(later_part_selected)

        to_concat = [joined.without(joined._pw_left_id, joined._pw_right_id)]
        if self._mode in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
            unmatched_left = self._get_unmatched_rows(
                joined,
                self._left_bucketed,
                self._right_bucketed,
                all_args,
                True,
            )
            to_concat.append(unmatched_left)
        if self._mode in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
            unmatched_right = self._get_unmatched_rows(
                joined,
                self._right_bucketed,
                self._left_bucketed,
                all_args,
                False,
            )
            to_concat.append(unmatched_right)

        if len(to_concat) == 1:
            result = to_concat[0]
        else:
            from pathway.internals.table import Table

            result = Table.concat_reindex(*to_concat)

        if self._filter_out_results_of_forgetting:
            result = result._filter_out_results_of_forgetting()
        return result

    @staticmethod
    def _get_unmatched_rows(
        joined: pw.Table,
        side: pw.Table,
        other: pw.Table,
        cols: dict[str, pw.ColumnExpression],
        is_side_left: bool,
    ) -> pw.Table:
        id_column = joined["_pw_left_id" if is_side_left else "_pw_right_id"]
        matched = joined.groupby(id_column).reduce(old_id=id_column)
        unmatched = side.difference(matched.with_id(matched.old_id))
        cols_new = {}
        expression_replacer_1 = TableSubstitutionDesugaring({side: unmatched})
        expression_replacer_2 = TableReplacementWithNoneDesugaring(other)
        for column_name, expression in cols.items():
            if column_name not in ("_pw_left_id", "_pw_right_id"):
                expression = expression_replacer_1.eval_expression(expression)
                expression = expression_replacer_2.eval_expression(expression)
                cols_new[column_name] = expression
        return unmatched.select(**cols_new)


class _ZeroDifferenceIntervalJoinResult(IntervalJoinResult):
    _join_result: pw.JoinResult

    def __init__(
        self,
        left: pw.Table,
        right: pw.Table,
        join_result: pw.JoinResult,
        table_substitution: dict[pw.TableLike, pw.Table],
        _filter_out_results_of_forgetting: bool,
    ) -> None:
        super().__init__(
            left,
            right,
            table_substitution=table_substitution,
            _filter_out_results_of_forgetting=_filter_out_results_of_forgetting,
        )
        self._join_result = join_result

    @staticmethod
    def _interval_join(
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
        *on: pw.ColumnExpression,
        behavior: CommonBehavior | None = None,
        mode: pw.JoinMode,
        left_instance: pw.ColumnReference | None = None,
        right_instance: pw.ColumnReference | None = None,
    ) -> IntervalJoinResult:
        assert left != right
        assert interval.lower_bound == interval.upper_bound

        left_with_time = left.with_columns(_pw_time=left_time_expression)
        right_with_time = right.with_columns(_pw_time=right_time_expression)
        left_with_time = apply_temporal_behavior(left_with_time, behavior)
        left_with_time = left_with_time.with_columns(
            _pw_time=pw.this._pw_time + interval.lower_bound
        )
        right_with_time = apply_temporal_behavior(right_with_time, behavior)

        from pathway.internals.joins import validate_join_condition

        for cond in on:
            cond_left, cond_right, cond = validate_join_condition(cond, left, right)
            cond._left = left_with_time[cond_left._name]
            cond._right = right_with_time[cond_right._name]

        if left_instance is not None and right_instance is not None:
            left_instance = left_with_time[left_instance._name]
            right_instance = right_with_time[right_instance._name]
        else:
            assert left_instance is None and right_instance is None

        join_result = left_with_time.join(
            right_with_time,
            left_with_time._pw_time == right_with_time._pw_time,
            *on,
            how=mode,
            left_instance=left_instance,
            right_instance=right_instance,
        )

        filter_out_results_of_forgetting = (
            IntervalJoinResult._should_filter_out_results_of_forgetting(behavior)
        )

        table_substitution: dict[pw.TableLike, pw.Table] = {
            left: left_with_time,
            right: right_with_time,
        }

        return _ZeroDifferenceIntervalJoinResult(
            left_with_time,
            right_with_time,
            join_result,
            table_substitution=table_substitution,
            _filter_out_results_of_forgetting=filter_out_results_of_forgetting,
        )

    @desugar
    @arg_handler(handler=select_args_handler)
    @trace_user_frame
    def select(self, *args: pw.ColumnReference, **kwargs: Any) -> pw.Table:
        exclude_columns = {"_pw_time"}
        # remove internal columns that can appear if using *pw.left, *pw.right
        all_args = combine_args_kwargs(args, kwargs, exclude_columns=exclude_columns)
        result = self._join_result.select(**all_args)

        if self._filter_out_results_of_forgetting:
            result = result._filter_out_results_of_forgetting()
        return result


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=True, allow_id=False))
@check_arg_types
@trace_user_frame
def interval_join(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
    how: pw.JoinMode = pw.JoinMode.INNER,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> IntervalJoinResult:
    """Performs an interval join of self with other using a time difference
    and join expressions. If `self_time + lower_bound <=
    other_time <= self_time + upper_bound`
    and conditions in `on` are satisfied, the rows are joined.

    Args:
        other:  the right side of a join.
        self_time (pw.ColumnExpression[int | float | datetime]):
            time expression in self.
        other_time (pw.ColumnExpression[int | float | datetime]):
            time expression in other.
        lower_bound: a lower bound on time difference between other_time
            and self_time.
        upper_bound: an upper bound on time difference between other_time
            and self_time.
        on:  a list of column expressions. Each must have == as the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        behavior: defines a temporal behavior of a join - features like delaying entries
            or ignoring late entries. You can see examples below or read more in the
            `temporal behavior of interval join tutorial </developers/user-guide/temporal-data/temporal_behavior>`_ .
        how: decides whether to run `interval_join_inner`, `interval_join_left`, `interval_join_right`
            or `interval_join_outer`. Default is INNER.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        IntervalJoinResult: a result of the interval join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 3
    ...   2 | 4
    ...   3 | 5
    ...   4 | 11
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 0
    ...   2 | 1
    ...   3 | 4
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.interval_join(t2, t1.t, t2.t, pw.temporal.interval(-2, 1)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    3      | 1
    3      | 4
    4      | 4
    5      | 4
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 3
    ...   2 | 1 | 4
    ...   3 | 1 | 5
    ...   4 | 1 | 11
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 0
    ...   2 | 1 | 1
    ...   3 | 1 | 4
    ...   4 | 1 | 7
    ...   5 | 2 | 0
    ...   6 | 2 | 2
    ...   7 | 4 | 2
    ... '''
    ... )
    >>> t3 = t1.interval_join(
    ...     t2, t1.t, t2.t, pw.temporal.interval(-2, 1), t1.a == t2.b, how=pw.JoinMode.INNER
    ... ).select(t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    a | left_t | right_t
    1 | 3      | 1
    1 | 3      | 4
    1 | 4      | 4
    1 | 5      | 4
    2 | 2      | 0
    2 | 2      | 2
    2 | 3      | 2

    Setting `behavior` allows to control temporal behavior of an interval join. Then, each side of
    the interval join keeps track of the maximal already seen time (`self_time` and `other_time`).
    The arguments of `behavior` mean in the context of an interval join what follows:
    - **delay** - buffers results until the maximal already seen time is greater than \
        or equal to their time plus `delay`.
    - **cutoff** - ignores records with times less or equal to the maximal already seen time minus `cutoff`; \
        it is also used to garbage collect records that have times lower or equal to the above threshold. \
        When `cutoff` is not set, interval join will remember all records from both sides.
    - **keep_results** - if set to `True`, keeps all results of the operator. If set to `False`, \
        keeps only results that are newer than the maximal seen time minus `cutoff`.

    Example without and with forgetting:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       1   |     1    |      0     |     2
    ...       2   |     2    |      2     |     4
    ...       3   |     1    |      4     |     4
    ...       4   |     2    |      8     |     8
    ...       5   |     1    |      0     |    10
    ...       6   |     1    |      4     |    10
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       42  |     1    |      2     |     2
    ...        8  |     2    |     10     |    14
    ...       10  |     2    |      4     |    30
    ... '''
    ... )
    >>> result_without_cutoff = t1.interval_join(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_without_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    5          | 42          | 1        | 0         | 2          | 10       | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          | 8           | 2        | 8         | 10         | 14       | 1
    2          | 10          | 2        | 2         | 4          | 30       | 1
    >>> result_with_cutoff = t1.interval_join(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ...     behavior=pw.temporal.common_behavior(cutoff=6),
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_with_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          | 8           | 2        | 8         | 10         | 14       | 1

    The record with ``value=5`` from table ``t1`` was not joined because its ``event_time``
    was less than the maximal already seen time minus ``cutoff`` (``0 <= 8-6``).
    The record with ``value=10`` from table ``t2`` was not joined because its ``event_time``
    was equal to the maximal already seen time minus ``cutoff`` (``4 <= 10-6``).
    """
    return IntervalJoinResult._interval_join(
        self,
        other,
        self_time,
        other_time,
        interval,
        *on,
        behavior=behavior,
        mode=how,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def interval_join_inner(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> IntervalJoinResult:
    """Performs an interval join of self with other using a time difference
    and join expressions. If `self_time + lower_bound <=
    other_time <= self_time + upper_bound`
    and conditions in `on` are satisfied, the rows are joined.

    Args:
        other:  the right side of a join.
        self_time: time expression in self.
        other_time: time expression in other.
        lower_bound: a lower bound on time difference between other_time
            and self_time.
        upper_bound: an upper bound on time difference between other_time
            and self_time.
        on:  a list of column expressions. Each must have == as the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        behavior: defines temporal behavior of a join - features like delaying entries
            or ignoring late entries.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        IntervalJoinResult: a result of the interval join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 3
    ...   2 | 4
    ...   3 | 5
    ...   4 | 11
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 0
    ...   2 | 1
    ...   3 | 4
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.interval_join_inner(t2, t1.t, t2.t, pw.temporal.interval(-2, 1)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    3      | 1
    3      | 4
    4      | 4
    5      | 4
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 3
    ...   2 | 1 | 4
    ...   3 | 1 | 5
    ...   4 | 1 | 11
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 0
    ...   2 | 1 | 1
    ...   3 | 1 | 4
    ...   4 | 1 | 7
    ...   5 | 2 | 0
    ...   6 | 2 | 2
    ...   7 | 4 | 2
    ... '''
    ... )
    >>> t3 = t1.interval_join_inner(
    ...     t2, t1.t, t2.t, pw.temporal.interval(-2, 1), t1.a == t2.b
    ... ).select(t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    a | left_t | right_t
    1 | 3      | 1
    1 | 3      | 4
    1 | 4      | 4
    1 | 5      | 4
    2 | 2      | 0
    2 | 2      | 2
    2 | 3      | 2

    Setting `behavior` allows to control temporal behavior of an interval join. Then, each side of
    the interval join keeps track of the maximal already seen time (`self_time` and `other_time`).
    The arguments of `behavior` mean in the context of an interval join what follows:
    - **delay** - buffers results until the maximal already seen time is greater than \
        or equal to their time plus `delay`.
    - **cutoff** - ignores records with times less or equal to the maximal already seen time minus `cutoff`; \
        it is also used to garbage collect records that have times lower or equal to the above threshold. \
        When `cutoff` is not set, interval join will remember all records from both sides.
    - **keep_results** - if set to `True`, keeps all results of the operator. If set to `False`, \
        keeps only results that are newer than the maximal seen time minus `cutoff`.

    Example without and with forgetting:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       1   |     1    |      0     |     2
    ...       2   |     2    |      2     |     4
    ...       3   |     1    |      4     |     4
    ...       4   |     2    |      8     |     8
    ...       5   |     1    |      0     |    10
    ...       6   |     1    |      4     |    10
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       42  |     1    |      2     |     2
    ...        8  |     2    |     10     |    14
    ...       10  |     2    |      4     |    30
    ... '''
    ... )
    >>> result_without_cutoff = t1.interval_join_inner(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_without_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    5          | 42          | 1        | 0         | 2          | 10       | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          | 8           | 2        | 8         | 10         | 14       | 1
    2          | 10          | 2        | 2         | 4          | 30       | 1
    >>> result_with_cutoff = t1.interval_join_inner(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ...     behavior=pw.temporal.common_behavior(cutoff=6),
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_with_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          | 8           | 2        | 8         | 10         | 14       | 1

    The record with ``value=5`` from table ``t1`` was not joined because its ``event_time``
    was less than the maximal already seen time minus ``cutoff`` (``0 <= 8-6``).
    The record with ``value=10`` from table ``t2`` was not joined because its ``event_time``
    was equal to the maximal already seen time minus ``cutoff`` (``4 <= 10-6``).
    """
    return IntervalJoinResult._interval_join(
        self,
        other,
        self_time,
        other_time,
        interval,
        *on,
        behavior=behavior,
        mode=pw.JoinMode.INNER,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def interval_join_left(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> IntervalJoinResult:
    """Performs an interval left join of self with other using a time difference
    and join expressions. If `self_time + lower_bound <=
    other_time <= self_time + upper_bound`
    and conditions in `on` are satisfied, the rows are joined. Rows from the left
    side that haven't been matched with the right side are returned with missing
    values on the right side replaced with `None`.

    Args:
        other:  the right side of the join.
        self_time: time expression in self.
        other_time: time expression in other.
        lower_bound: a lower bound on time difference between other_time
            and self_time.
        upper_bound: an upper bound on time difference between other_time
            and self_time.
        on:  a list of column expressions. Each must have == as the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        behavior: defines temporal behavior of a join - features like delaying entries
            or ignoring late entries.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        IntervalJoinResult: a result of the interval join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 3
    ...   2 | 4
    ...   3 | 5
    ...   4 | 11
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 0
    ...   2 | 1
    ...   3 | 4
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.interval_join_left(t2, t1.t, t2.t, pw.temporal.interval(-2, 1)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    3      | 1
    3      | 4
    4      | 4
    5      | 4
    11     |
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 3
    ...   2 | 1 | 4
    ...   3 | 1 | 5
    ...   4 | 1 | 11
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 0
    ...   2 | 1 | 1
    ...   3 | 1 | 4
    ...   4 | 1 | 7
    ...   5 | 2 | 0
    ...   6 | 2 | 2
    ...   7 | 4 | 2
    ... '''
    ... )
    >>> t3 = t1.interval_join_left(
    ...     t2, t1.t, t2.t, pw.temporal.interval(-2, 1), t1.a == t2.b
    ... ).select(t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    a | left_t | right_t
    1 | 3      | 1
    1 | 3      | 4
    1 | 4      | 4
    1 | 5      | 4
    1 | 11     |
    2 | 2      | 0
    2 | 2      | 2
    2 | 3      | 2
    3 | 4      |

    Setting `behavior` allows to control temporal behavior of an interval join. Then, each side of
    the interval join keeps track of the maximal already seen time (`self_time` and `other_time`).
    The arguments of `behavior` mean in the context of an interval join what follows:
    - **delay** - buffers results until the maximal already seen time is greater than \
        or equal to their time plus `delay`.
    - **cutoff** - ignores records with times less or equal to the maximal already seen time minus `cutoff`; \
        it is also used to garbage collect records that have times lower or equal to the above threshold. \
        When `cutoff` is not set, interval join will remember all records from both sides.
    - **keep_results** - if set to `True`, keeps all results of the operator. If set to `False`, \
        keeps only results that are newer than the maximal seen time minus `cutoff`.

    Example without and with forgetting:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       1   |     1    |      0     |     2
    ...       2   |     2    |      2     |     4
    ...       3   |     1    |      4     |     4
    ...       4   |     2    |      8     |     8
    ...       5   |     1    |      0     |    10
    ...       6   |     1    |      4     |    10
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       42  |     1    |      2     |     2
    ...        8  |     2    |     10     |    14
    ...       10  |     2    |      4     |    30
    ... '''
    ... )
    >>> result_without_cutoff = t1.interval_join_left(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_without_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    2          |             | 2        | 2         |            | 4        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    4          |             | 2        | 8         |            | 8        | 1
    5          | 42          | 1        | 0         | 2          | 10       | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          |             | 2        | 8         |            | 14       | -1
    4          | 8           | 2        | 8         | 10         | 14       | 1
    2          |             | 2        | 2         |            | 30       | -1
    2          | 10          | 2        | 2         | 4          | 30       | 1
    >>> result_with_cutoff = t1.interval_join_left(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ...     behavior=pw.temporal.common_behavior(cutoff=6),
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )

    >>> pw.debug.compute_and_print_update_stream(result_with_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    2          |             | 2        | 2         |            | 4        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    4          |             | 2        | 8         |            | 8        | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          |             | 2        | 8         |            | 14       | -1
    4          | 8           | 2        | 8         | 10         | 14       | 1

    The record with ``value=5`` from table ``t1`` was not joined because its ``event_time``
    was less than the maximal already seen time minus ``cutoff`` (``0 <= 8-6``).
    The record with ``value=10`` from table ``t2`` was not joined because its ``event_time``
    was equal to the maximal already seen time minus ``cutoff`` (``4 <= 10-6``).

    Notice also the entries with ``__diff__=-1``. They're deletion entries caused by the arrival
    of matching entries on the right side of the join. The matches caused the removal of entries
    without values in the fields from the right side and insertion of entries with values
    in these fields.
    """
    return IntervalJoinResult._interval_join(
        self,
        other,
        self_time,
        other_time,
        interval,
        *on,
        behavior=behavior,
        mode=pw.JoinMode.LEFT,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def interval_join_right(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> IntervalJoinResult:
    """Performs an interval right join of self with other using a time difference
    and join expressions. If `self_time + lower_bound <=
    other_time <= self_time + upper_bound`
    and conditions in `on` are satisfied, the rows are joined. Rows from the right
    side that haven't been matched with the left side are returned with missing
    values on the left side replaced with `None`.

    Args:
        other:  the right side of the join.
        self_time: time expression in self.
        other_time: time expression in other.
        lower_bound: a lower bound on time difference between other_time
            and self_time.
        upper_bound: an upper bound on time difference between other_time
            and self_time.
        on:  a list of column expressions. Each must have == as the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        behavior: defines temporal behavior of a join - features like delaying entries
            or ignoring late entries.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        IntervalJoinResult: a result of the interval join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 3
    ...   2 | 4
    ...   3 | 5
    ...   4 | 11
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 0
    ...   2 | 1
    ...   3 | 4
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.interval_join_right(t2, t1.t, t2.t, pw.temporal.interval(-2, 1)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
           | 0
           | 7
    3      | 1
    3      | 4
    4      | 4
    5      | 4
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 3
    ...   2 | 1 | 4
    ...   3 | 1 | 5
    ...   4 | 1 | 11
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 0
    ...   2 | 1 | 1
    ...   3 | 1 | 4
    ...   4 | 1 | 7
    ...   5 | 2 | 0
    ...   6 | 2 | 2
    ...   7 | 4 | 2
    ... '''
    ... )
    >>> t3 = t1.interval_join_right(
    ...     t2, t1.t, t2.t, pw.temporal.interval(-2, 1), t1.a == t2.b
    ... ).select(t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    a | left_t | right_t
      |        | 0
      |        | 2
      |        | 7
    1 | 3      | 1
    1 | 3      | 4
    1 | 4      | 4
    1 | 5      | 4
    2 | 2      | 0
    2 | 2      | 2
    2 | 3      | 2

    Setting `behavior` allows to control temporal behavior of an interval join. Then, each side of
    the interval join keeps track of the maximal already seen time (`self_time` and `other_time`).
    The arguments of `behavior` mean in the context of an interval join what follows:
    - **delay** - buffers results until the maximal already seen time is greater than \
        or equal to their time plus `delay`.
    - **cutoff** - ignores records with times less or equal to the maximal already seen time minus `cutoff`; \
        it is also used to garbage collect records that have times lower or equal to the above threshold. \
        When `cutoff` is not set, interval join will remember all records from both sides.
    - **keep_results** - if set to `True`, keeps all results of the operator. If set to `False`, \
        keeps only results that are newer than the maximal seen time minus `cutoff`.

    Example without and with forgetting:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       1   |     1    |      0     |     2
    ...       2   |     2    |      2     |     4
    ...       3   |     1    |      4     |     4
    ...       4   |     2    |      8     |     8
    ...       5   |     1    |      0     |    10
    ...       6   |     1    |      4     |    10
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       42  |     1    |      2     |     2
    ...        8  |     2    |     10     |    14
    ...       10  |     2    |      4     |    30
    ... '''
    ... )
    >>> result_without_cutoff = t1.interval_join_right(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_without_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    5          | 42          | 1        | 0         | 2          | 10       | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          | 8           | 2        | 8         | 10         | 14       | 1
    2          | 10          | 2        | 2         | 4          | 30       | 1
    >>> result_with_cutoff = t1.interval_join_right(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ...     behavior=pw.temporal.common_behavior(cutoff=6),
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_with_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          | 8           | 2        | 8         | 10         | 14       | 1

    The record with ``value=5`` from table ``t1`` was not joined because its ``event_time``
    was less than the maximal already seen time minus ``cutoff`` (``0 <= 8-6``).
    The record with ``value=10`` from table ``t2`` was not joined because its ``event_time``
    was equal to the maximal already seen time minus ``cutoff`` (``4 <= 10-6``).
    """
    return IntervalJoinResult._interval_join(
        self,
        other,
        self_time,
        other_time,
        interval,
        *on,
        behavior=behavior,
        mode=pw.JoinMode.RIGHT,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def interval_join_outer(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval[int] | Interval[float] | Interval[datetime.timedelta],
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> IntervalJoinResult:
    """Performs an interval outer join of self with other using a time difference
    and join expressions. If `self_time + lower_bound <=
    other_time <= self_time + upper_bound`
    and conditions in `on` are satisfied, the rows are joined. Rows that haven't
    been matched with the other side are returned with missing values on the other
    side replaced with `None`.

    Args:
        other:  the right side of the join.
        self_time: time expression in self.
        other_time: time expression in other.
        lower_bound: a lower bound on time difference between other_time
            and self_time.
        upper_bound: an upper bound on time difference between other_time
            and self_time.
        on:  a list of column expressions. Each must have == as the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        behavior: defines temporal behavior of a join - features like delaying entries
            or ignoring late entries.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        IntervalJoinResult: a result of the interval join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 3
    ...   2 | 4
    ...   3 | 5
    ...   4 | 11
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 0
    ...   2 | 1
    ...   3 | 4
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.interval_join_outer(t2, t1.t, t2.t, pw.temporal.interval(-2, 1)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
           | 0
           | 7
    3      | 1
    3      | 4
    4      | 4
    5      | 4
    11     |
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 3
    ...   2 | 1 | 4
    ...   3 | 1 | 5
    ...   4 | 1 | 11
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 0
    ...   2 | 1 | 1
    ...   3 | 1 | 4
    ...   4 | 1 | 7
    ...   5 | 2 | 0
    ...   6 | 2 | 2
    ...   7 | 4 | 2
    ... '''
    ... )
    >>> t3 = t1.interval_join_outer(
    ...     t2, t1.t, t2.t, pw.temporal.interval(-2, 1), t1.a == t2.b
    ... ).select(t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    a | left_t | right_t
      |        | 0
      |        | 2
      |        | 7
    1 | 3      | 1
    1 | 3      | 4
    1 | 4      | 4
    1 | 5      | 4
    1 | 11     |
    2 | 2      | 0
    2 | 2      | 2
    2 | 3      | 2
    3 | 4      |

    Setting `behavior` allows to control temporal behavior of an interval join. Then, each side of
    the interval join keeps track of the maximal already seen time (`self_time` and `other_time`).
    The arguments of `behavior` mean in the context of an interval join what follows:
    - **delay** - buffers results until the maximal already seen time is greater than \
        or equal to their time plus `delay`.
    - **cutoff** - ignores records with times less or equal to the maximal already seen time minus `cutoff`; \
        it is also used to garbage collect records that have times lower or equal to the above threshold. \
        When `cutoff` is not set, interval join will remember all records from both sides.
    - **keep_results** - if set to `True`, keeps all results of the operator. If set to `False`, \
        keeps only results that are newer than the maximal seen time minus `cutoff`.

    Example without and with forgetting:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       1   |     1    |      0     |     2
    ...       2   |     2    |      2     |     4
    ...       3   |     1    |      4     |     4
    ...       4   |     2    |      8     |     8
    ...       5   |     1    |      0     |    10
    ...       6   |     1    |      4     |    10
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     value | instance | event_time | __time__
    ...       42  |     1    |      2     |     2
    ...        8  |     2    |     10     |    14
    ...       10  |     2    |      4     |    30
    ... '''
    ... )
    >>> result_without_cutoff = t1.interval_join_outer(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )
    >>> pw.debug.compute_and_print_update_stream(result_without_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    2          |             | 2        | 2         |            | 4        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    4          |             | 2        | 8         |            | 8        | 1
    5          | 42          | 1        | 0         | 2          | 10       | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          |             | 2        | 8         |            | 14       | -1
    4          | 8           | 2        | 8         | 10         | 14       | 1
    2          |             | 2        | 2         |            | 30       | -1
    2          | 10          | 2        | 2         | 4          | 30       | 1
    >>> result_with_cutoff = t1.interval_join_outer(
    ...     t2,
    ...     t1.event_time,
    ...     t2.event_time,
    ...     pw.temporal.interval(-2, 2),
    ...     t1.instance == t2.instance,
    ...     behavior=pw.temporal.common_behavior(cutoff=6),
    ... ).select(
    ...     left_value=t1.value,
    ...     right_value=t2.value,
    ...     instance=t1.instance,
    ...     left_time=t1.event_time,
    ...     right_time=t2.event_time,
    ... )

    >>> pw.debug.compute_and_print_update_stream(result_with_cutoff, include_id=False)
    left_value | right_value | instance | left_time | right_time | __time__ | __diff__
    1          | 42          | 1        | 0         | 2          | 2        | 1
    2          |             | 2        | 2         |            | 4        | 1
    3          | 42          | 1        | 4         | 2          | 4        | 1
    4          |             | 2        | 8         |            | 8        | 1
    6          | 42          | 1        | 4         | 2          | 10       | 1
    4          |             | 2        | 8         |            | 14       | -1
    4          | 8           | 2        | 8         | 10         | 14       | 1

    The record with ``value=5`` from table ``t1`` was not joined because its ``event_time``
    was less than the maximal already seen time minus ``cutoff`` (``0 <= 8-6``).
    The record with ``value=10`` from table ``t2`` was not joined because its ``event_time``
    was equal to the maximal already seen time minus ``cutoff`` (``4 <= 10-6``).

    Notice also the entries with ``__diff__=-1``. They're deletion entries caused by the arrival
    of matching entries on the right side of the join. The matches caused the removal of entries
    without values in the fields from the right side and insertion of entries with values
    in these fields.
    """
    return IntervalJoinResult._interval_join(
        self,
        other,
        self_time,
        other_time,
        interval,
        *on,
        behavior=behavior,
        mode=pw.JoinMode.OUTER,
        left_instance=left_instance,
        right_instance=right_instance,
    )

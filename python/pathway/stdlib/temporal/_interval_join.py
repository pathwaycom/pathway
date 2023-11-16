# Copyright © 2023 Pathway


from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any

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
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.thisclass import ThisMetaclass
from pathway.internals.trace import trace_user_frame

from .temporal_behavior import CommonBehavior
from .utils import IntervalType, TimeEventType, check_joint_types, get_default_shift


@dataclass
class Interval:
    lower_bound: int | float | datetime.timedelta
    upper_bound: int | float | datetime.timedelta


def interval(
    lower_bound: int | float | datetime.timedelta,
    upper_bound: int | float | datetime.timedelta,
):
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

    _left_bucketed: pw.Table
    _right_bucketed: pw.Table
    _earlier_part_filtered: pw.JoinResult
    _later_part_filtered: pw.JoinResult
    _table_substitution: dict[pw.TableLike, pw.Table]
    _mode: pw.JoinMode
    _substitution: dict[ThisMetaclass, pw.Joinable]
    _filter_out_results_of_forgetting: bool

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
        self._left_bucketed = left_bucketed
        self._right_bucketed = right_bucketed
        self._earlier_part_filtered = earlier_part_filtered
        self._later_part_filtered = later_part_filtered
        self._table_substitution = table_substitution
        self._mode = mode
        self._substitution = {
            pw.left: self._left_bucketed,
            pw.right: self._right_bucketed,
            pw.this: pw.this,  # type: ignore[dict-item]
        }
        self._filter_out_results_of_forgetting = _filter_out_results_of_forgetting

    @staticmethod
    def _apply_temporal_behavior(
        table: pw.Table, behavior: CommonBehavior | None
    ) -> pw.Table:
        if behavior is not None:
            if behavior.delay is not None:
                table = table._buffer(
                    pw.this._pw_time + behavior.delay, pw.this._pw_time
                )
            if behavior.cutoff is not None:
                cutoff_threshold = pw.this._pw_time + behavior.cutoff
                table = table._freeze(cutoff_threshold, pw.this._pw_time)
                table = table._forget(
                    cutoff_threshold, pw.this._pw_time, behavior.keep_results
                )
        return table

    @staticmethod
    def _interval_join(
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        interval: Interval,
        *on: pw.ColumnExpression,
        behavior: CommonBehavior | None = None,
        mode: pw.JoinMode,
    ):
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
        if interval.lower_bound == interval.upper_bound:
            raise ValueError(
                "The difference between lower_bound and upper_bound has to be positive in the Table.interval_join().\n"
                + " For lower_bound == upper_bound, use a regular join with"
                + " left_time_column+lower_bound == right_time_column."
            )
        if interval.lower_bound > interval.upper_bound:  # type: ignore[operator]
            raise ValueError(
                "lower_bound has to be less than upper_bound in the Table.interval_join()."
            )

        shift = get_default_shift(interval.lower_bound)
        left_with_time = left.with_columns(_pw_time=left_time_expression)
        right_with_time = right.with_columns(_pw_time=right_time_expression)
        left_with_time = IntervalJoinResult._apply_temporal_behavior(
            left_with_time, behavior
        )
        right_with_time = IntervalJoinResult._apply_temporal_behavior(
            right_with_time, behavior
        )
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

        from pathway.internals.join import validate_join_condition

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
            behavior is not None
            and behavior.cutoff is not None
            and behavior.keep_results
        )

        return IntervalJoinResult(
            left_bucketed,
            right_bucketed,
            earlier_part_filtered,
            later_part_filtered,
            table_substitution,
            mode,
            _filter_out_results_of_forgetting=filter_out_results_of_forgetting,
        )

    @property
    def _desugaring(self) -> TableSubstitutionDesugaring:
        return TableSubstitutionDesugaring(self._table_substitution)

    @desugar
    @arg_handler(handler=select_args_handler)
    @trace_user_frame
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
        for arg in args:
            if not isinstance(arg, pw.ColumnReference):
                if isinstance(arg, str):
                    raise ValueError(
                        f"Expected a ColumnReference, found a string. Did you mean this.{arg} instead of {repr(arg)}?"
                    )
                else:
                    raise ValueError(
                        "In IntervalJoinResult.select() all positional arguments have to be a ColumnReference."
                    )
        all_args = combine_args_kwargs(args, kwargs)

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


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=True, allow_id=False))
@runtime_type_check
@trace_user_frame
def interval_join(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval,
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
    how: pw.JoinMode = pw.JoinMode.INNER,
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
        how: decides whether to run `interval_join_inner`, `interval_join_left`, `interval_join_right`
            or `interval_join_outer`. Default is INNER.

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
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@runtime_type_check
@trace_user_frame
def interval_join_inner(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval,
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
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
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@runtime_type_check
@trace_user_frame
def interval_join_left(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval,
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
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
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@runtime_type_check
@trace_user_frame
def interval_join_right(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval,
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
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
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@runtime_type_check
@trace_user_frame
def interval_join_outer(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    interval: Interval,
    *on: pw.ColumnExpression,
    behavior: CommonBehavior | None = None,
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
    )

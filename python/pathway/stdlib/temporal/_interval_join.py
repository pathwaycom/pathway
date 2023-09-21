# Copyright © 2023 Pathway


from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any, Dict, Union

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

from .utils import IntervalType, TimeEventType, check_joint_types, get_default_shift


@dataclass
class Interval:
    lower_bound: Union[int, float, datetime.timedelta]
    upper_bound: Union[int, float, datetime.timedelta]


def interval(
    lower_bound: Union[int, float, datetime.timedelta],
    upper_bound: Union[int, float, datetime.timedelta],
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
    _earlier_part_filtered: pw.FilteredJoinResult
    _later_part_filtered: pw.FilteredJoinResult
    _table_replacer: TableSubstitutionDesugaring
    _table_substitution: Dict[pw.TableLike, pw.Table]
    _mode: pw.JoinMode
    _substitution: Dict[ThisMetaclass, pw.Joinable]

    def __init__(
        self,
        left_bucketed: pw.Table,
        right_bucketed: pw.Table,
        earlier_part_filtered: pw.FilteredJoinResult,
        later_part_filtered: pw.FilteredJoinResult,
        table_substitution: Dict[pw.TableLike, pw.Table],
        mode: pw.JoinMode,
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

    @staticmethod
    def _interval_join(
        left: pw.Table,
        right: pw.Table,
        left_time_expression: pw.ColumnExpression,
        right_time_expression: pw.ColumnExpression,
        interval: Interval,
        *on: pw.ColumnExpression,
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
        left_time_expression = left_time_expression - shift
        right_time_expression = right_time_expression - shift
        bounds_difference = interval.upper_bound - interval.lower_bound  # type: ignore[operator]

        left_bucketed = left.with_columns(
            _pathway_bucket=pw.cast(int, left_time_expression // bounds_difference)
        )
        right_bucketed = right.with_columns(
            _pathway_bucket=pw.cast(
                int,
                (right_time_expression - interval.upper_bound) // bounds_difference,
            )
        )

        right_bucketed = right_bucketed.with_columns(
            _pathway_bucket_plus_one=right_bucketed._pathway_bucket + 1
        )
        table_replacer = TableSubstitutionDesugaring(
            {left: left_bucketed, right: right_bucketed}
        )
        left_time_expression = table_replacer.eval_expression(left_time_expression)
        right_time_expression = table_replacer.eval_expression(right_time_expression)

        from pathway.internals.join import validate_join_condition

        for cond in on:
            cond_left, cond_right, cond = validate_join_condition(cond, left, right)
            cond._left = left_bucketed[cond_left._name]
            cond._right = right_bucketed[cond_right._name]

        earlier_part = left_bucketed.join(
            right_bucketed,
            left_bucketed._pathway_bucket == right_bucketed._pathway_bucket,
            *on,
        )
        later_part = left_bucketed.join(
            right_bucketed,
            left_bucketed._pathway_bucket == right_bucketed._pathway_bucket_plus_one,
            *on,
        )
        pw.universes.promise_are_pairwise_disjoint(earlier_part, later_part)

        earlier_part_filtered = earlier_part.filter(
            right_time_expression <= left_time_expression + interval.upper_bound
        )
        later_part_filtered = later_part.filter(
            left_time_expression + interval.lower_bound <= right_time_expression
        )

        table_substitution: Dict[pw.TableLike, pw.Table] = {
            left: left_bucketed,
            right: right_bucketed,
        }

        return IntervalJoinResult(
            left_bucketed,
            right_bucketed,
            earlier_part_filtered,
            later_part_filtered,
            table_substitution,
            mode,
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

        if self._mode != pw.JoinMode.INNER:
            all_args["_pathway_left_id"] = self._left_bucketed.id
            all_args["_pathway_right_id"] = self._right_bucketed.id

        earlier_part_selected = self._earlier_part_filtered.select(**all_args)

        later_part_selected = self._later_part_filtered.select(**all_args)

        joined = earlier_part_selected.concat(later_part_selected)
        if self._mode == pw.JoinMode.INNER:
            return joined

        result = joined.without(joined._pathway_left_id, joined._pathway_right_id)
        to_concat = [result]
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

        from pathway.internals.table import Table

        return Table.concat_reindex(*to_concat)

    @staticmethod
    def _get_unmatched_rows(
        joined: pw.Table,
        side: pw.Table,
        other: pw.Table,
        cols: Dict[str, pw.ColumnExpression],
        is_side_left: bool,
    ) -> pw.Table:
        id_column = joined["_pathway_left_id" if is_side_left else "_pathway_right_id"]
        matched = joined.groupby(id_column).reduce(old_id=id_column)
        unmatched = side.difference(matched.with_id(matched.old_id))
        cols_new = {}
        expression_replacer = TableReplacementWithNoneDesugaring(other)
        for column_name, expression in cols.items():
            if column_name not in ("_pathway_left_id", "_pathway_right_id"):
                cols_new[column_name] = expression_replacer.eval_expression(expression)
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
        mode=pw.JoinMode.OUTER,
    )

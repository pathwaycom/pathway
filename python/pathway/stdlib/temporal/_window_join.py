# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Any

import pathway.internals as pw
from pathway.internals.arg_handlers import (
    arg_handler,
    join_kwargs_handler,
    select_args_handler,
)
from pathway.internals.desugaring import (
    DesugaringContext,
    TableSubstitutionDesugaring,
    desugar,
)
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.thisclass import ThisMetaclass
from pathway.internals.trace import trace_user_frame
from pathway.stdlib import temporal


class WindowJoinResult(DesugaringContext):
    """
    Result of a window join between tables.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 1
    ...   2 | 2
    ...   3 | 3
    ...   4 | 7
    ...   5 | 13
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 2
    ...   2 | 5
    ...   3 | 6
    ...   4 | 7
    ... '''
    ... )
    >>> join_result = t1.window_join_outer(t2, t1.t, t2.t, pw.temporal.tumbling(2))
    >>> isinstance(join_result, pw.temporal.WindowJoinResult)
    True
    >>> pw.debug.compute_and_print(
    ...     join_result.select(left_t=t1.t, right_t=t2.t), include_id=False
    ... )
    left_t | right_t
           | 5
    1      |
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    13     |
    """

    _join_result: pw.JoinResult
    _table_substitution: dict[pw.TableLike, pw.Table]
    _substitution: dict[ThisMetaclass, pw.Joinable]

    def __init__(
        self,
        join_result: pw.JoinResult,
        left_original: pw.Table,
        right_original: pw.Table,
        left_new: pw.Table,
        right_new: pw.Table,
    ):
        self._join_result = join_result
        self._universe = join_result._universe
        self._table_substitution = {
            left_original: left_new,
            right_original: right_new,
        }
        self._substitution = {
            pw.left: left_new,
            pw.right: right_new,
            pw.this: join_result,
        }

    @property
    def _desugaring(self) -> TableSubstitutionDesugaring:
        return TableSubstitutionDesugaring(self._table_substitution)

    @desugar
    @arg_handler(handler=select_args_handler)
    @trace_user_frame
    def select(self, *args: pw.ColumnReference, **kwargs: Any) -> pw.Table:
        """Computes a result of a window join.
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
        ...   1 | 1 | 1
        ...   2 | 1 | 2
        ...   3 | 1 | 3
        ...   4 | 1 | 7
        ...   5 | 1 | 13
        ...   6 | 2 | 1
        ...   7 | 2 | 2
        ...   8 | 3 | 4
        ... '''
        ... )
        >>> t2 = pw.debug.table_from_markdown(
        ...     '''
        ...     | b | t
        ...   1 | 1 | 2
        ...   2 | 1 | 5
        ...   3 | 1 | 6
        ...   4 | 1 | 7
        ...   5 | 2 | 2
        ...   6 | 2 | 3
        ...   7 | 4 | 3
        ... '''
        ... )
        >>> t3 = t1.window_join_outer(t2, t1.t, t2.t, pw.temporal.tumbling(2), t1.a == t2.b).select(
        ...     key=pw.coalesce(t1.a, t2.b), left_t=t1.t, right_t=t2.t
        ... )
        >>> pw.debug.compute_and_print(t3, include_id=False)
        key | left_t | right_t
        1   |        | 5
        1   | 1      |
        1   | 2      | 2
        1   | 3      | 2
        1   | 7      | 6
        1   | 7      | 7
        1   | 13     |
        2   | 1      |
        2   | 2      | 2
        2   | 2      | 3
        3   | 4      |
        4   |        | 3
        """
        return self._join_result.select(*args, **kwargs)


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=True, allow_id=False))
@check_arg_types
@trace_user_frame
def window_join(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    window: temporal.Window,
    *on: pw.ColumnExpression,
    how: pw.JoinMode = pw.JoinMode.INNER,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> WindowJoinResult:
    """Performs a window join of self with other using a window and join expressions.
    If two records belong to the same window and meet the conditions specified in
    the `on` clause, they will be joined. Note that if a sliding window is used and
    there are pairs of matching records that appear in more than one window,
    they will be included in the result multiple times (equal to the number of
    windows they appear in).

    When using a session window, the function creates sessions by concatenating
    records from both sides of a join. Only pairs of records that meet
    the conditions specified in the `on` clause can be part of the same session.
    The result of a given session will include all records from the left side of
    a join that belong to this session, joined with all records from the right
    side of a join that belong to this session.

    Args:
        other:  the right side of a join.
        self_time: time expression in self.
        other_time: time expression in other.
        window: a window to use.
        on:  a list of column expressions. Each must have == on the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        how: decides whether to run `window_join_inner`, `window_join_left`, `window_join_right`
            or `window_join_outer`. Default is INNER.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        WindowJoinResult: a result of the window join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 1
    ...   2 | 2
    ...   3 | 3
    ...   4 | 7
    ...   5 | 13
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 2
    ...   2 | 5
    ...   3 | 6
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.window_join(t2, t1.t, t2.t, pw.temporal.tumbling(2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    >>> t4 = t1.window_join(t2, t1.t, t2.t, pw.temporal.sliding(1, 2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t4, include_id=False)
    left_t | right_t
    1      | 2
    2      | 2
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    7      | 7
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 2
    ...   3 | 1 | 3
    ...   4 | 1 | 7
    ...   5 | 1 | 13
    ...   6 | 2 | 1
    ...   7 | 2 | 2
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 2
    ...   2 | 1 | 5
    ...   3 | 1 | 6
    ...   4 | 1 | 7
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join(t2, t1.t, t2.t, pw.temporal.tumbling(2), t1.a == t2.b).select(
    ...     key=t1.a, left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 2      | 2
    1   | 3      | 2
    1   | 7      | 6
    1   | 7      | 7
    2   | 2      | 2
    2   | 2      | 3
    >>>
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | 0
    ...     1 | 5
    ...     2 | 10
    ...     3 | 15
    ...     4 | 17
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | -3
    ...     1 | 2
    ...     2 | 3
    ...     3 | 6
    ...     4 | 16
    ... '''
    ... )
    >>> t3 = t1.window_join(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2)
    ... ).select(left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    0      | 2
    0      | 3
    0      | 6
    5      | 2
    5      | 3
    5      | 6
    15     | 16
    17     | 16
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 4
    ...   3 | 1 | 7
    ...   4 | 2 | 0
    ...   5 | 2 | 3
    ...   6 | 2 | 4
    ...   7 | 2 | 7
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | -1
    ...   2 | 1 | 6
    ...   3 | 2 | 2
    ...   4 | 2 | 10
    ...   5 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2), t1.a == t2.b
    ... ).select(key=t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 1      | -1
    1   | 4      | 6
    1   | 7      | 6
    2   | 0      | 2
    2   | 3      | 2
    2   | 4      | 2
    """
    return window._join(
        self,
        other,
        self_time,
        other_time,
        *on,
        mode=how,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def window_join_inner(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    window: temporal.Window,
    *on: pw.ColumnExpression,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> WindowJoinResult:
    """Performs a window join of self with other using a window and join expressions.
    If two records belong to the same window and meet the conditions specified in
    the `on` clause, they will be joined. Note that if a sliding window is used and
    there are pairs of matching records that appear in more than one window,
    they will be included in the result multiple times (equal to the number of
    windows they appear in).

    When using a session window, the function creates sessions by concatenating
    records from both sides of a join. Only pairs of records that meet
    the conditions specified in the `on` clause can be part of the same session.
    The result of a given session will include all records from the left side of
    a join that belong to this session, joined with all records from the right
    side of a join that belong to this session.

    Args:
        other:  the right side of a join.
        self_time: time expression in self.
        other_time: time expression in other.
        window: a window to use.
        on:  a list of column expressions. Each must have == on the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        WindowJoinResult: a result of the window join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 1
    ...   2 | 2
    ...   3 | 3
    ...   4 | 7
    ...   5 | 13
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 2
    ...   2 | 5
    ...   3 | 6
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.window_join_inner(t2, t1.t, t2.t, pw.temporal.tumbling(2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    >>> t4 = t1.window_join_inner(t2, t1.t, t2.t, pw.temporal.sliding(1, 2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t4, include_id=False)
    left_t | right_t
    1      | 2
    2      | 2
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    7      | 7
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 2
    ...   3 | 1 | 3
    ...   4 | 1 | 7
    ...   5 | 1 | 13
    ...   6 | 2 | 1
    ...   7 | 2 | 2
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 2
    ...   2 | 1 | 5
    ...   3 | 1 | 6
    ...   4 | 1 | 7
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_inner(t2, t1.t, t2.t, pw.temporal.tumbling(2), t1.a == t2.b).select(
    ...     key=t1.a, left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 2      | 2
    1   | 3      | 2
    1   | 7      | 6
    1   | 7      | 7
    2   | 2      | 2
    2   | 2      | 3
    >>>
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | 0
    ...     1 | 5
    ...     2 | 10
    ...     3 | 15
    ...     4 | 17
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | -3
    ...     1 | 2
    ...     2 | 3
    ...     3 | 6
    ...     4 | 16
    ... '''
    ... )
    >>> t3 = t1.window_join_inner(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2)
    ... ).select(left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    0      | 2
    0      | 3
    0      | 6
    5      | 2
    5      | 3
    5      | 6
    15     | 16
    17     | 16
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 4
    ...   3 | 1 | 7
    ...   4 | 2 | 0
    ...   5 | 2 | 3
    ...   6 | 2 | 4
    ...   7 | 2 | 7
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | -1
    ...   2 | 1 | 6
    ...   3 | 2 | 2
    ...   4 | 2 | 10
    ...   5 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_inner(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2), t1.a == t2.b
    ... ).select(key=t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 1      | -1
    1   | 4      | 6
    1   | 7      | 6
    2   | 0      | 2
    2   | 3      | 2
    2   | 4      | 2
    """
    return window._join(
        self,
        other,
        self_time,
        other_time,
        *on,
        mode=pw.JoinMode.INNER,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def window_join_left(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    window: temporal.Window,
    *on: pw.ColumnExpression,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> WindowJoinResult:
    """Performs a window left join of self with other using a window and join expressions.
    If two records belong to the same window and meet the conditions specified in
    the `on` clause, they will be joined. Note that if a sliding window is used and
    there are pairs of matching records that appear in more than one window,
    they will be included in the result multiple times (equal to the number of
    windows they appear in).

    When using a session window, the function creates sessions by concatenating
    records from both sides of a join. Only pairs of records that meet
    the conditions specified in the `on` clause can be part of the same session.
    The result of a given session will include all records from the left side of
    a join that belong to this session, joined with all records from the right
    side of a join that belong to this session.

    Rows from the left side that didn't match with any record on the right side in
    a given window, are returned with missing values on the right side replaced
    with `None`. The multiplicity of such rows equals the number of windows they
    belong to and don't have a match in them.


    Args:
        other:  the right side of a join.
        self_time: time expression in self.
        other_time: time expression in other.
        window: a window to use.
        on:  a list of column expressions. Each must have == on the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        WindowJoinResult: a result of the window join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 1
    ...   2 | 2
    ...   3 | 3
    ...   4 | 7
    ...   5 | 13
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 2
    ...   2 | 5
    ...   3 | 6
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.window_join_left(t2, t1.t, t2.t, pw.temporal.tumbling(2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    1      |
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    13     |
    >>> t4 = t1.window_join_left(t2, t1.t, t2.t, pw.temporal.sliding(1, 2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t4, include_id=False)
    left_t | right_t
    1      |
    1      | 2
    2      | 2
    2      | 2
    3      |
    3      | 2
    7      | 6
    7      | 7
    7      | 7
    13     |
    13     |
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 2
    ...   3 | 1 | 3
    ...   4 | 1 | 7
    ...   5 | 1 | 13
    ...   6 | 2 | 1
    ...   7 | 2 | 2
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 2
    ...   2 | 1 | 5
    ...   3 | 1 | 6
    ...   4 | 1 | 7
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_left(t2, t1.t, t2.t, pw.temporal.tumbling(2), t1.a == t2.b).select(
    ...     key=t1.a, left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 1      |
    1   | 2      | 2
    1   | 3      | 2
    1   | 7      | 6
    1   | 7      | 7
    1   | 13     |
    2   | 1      |
    2   | 2      | 2
    2   | 2      | 3
    3   | 4      |
    >>>
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | 0
    ...     1 | 5
    ...     2 | 10
    ...     3 | 15
    ...     4 | 17
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | -3
    ...     1 | 2
    ...     2 | 3
    ...     3 | 6
    ...     4 | 16
    ... '''
    ... )
    >>> t3 = t1.window_join_left(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2)
    ... ).select(left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
    0      | 2
    0      | 3
    0      | 6
    5      | 2
    5      | 3
    5      | 6
    10     |
    15     | 16
    17     | 16
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 4
    ...   3 | 1 | 7
    ...   4 | 2 | 0
    ...   5 | 2 | 3
    ...   6 | 2 | 4
    ...   7 | 2 | 7
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | -1
    ...   2 | 1 | 6
    ...   3 | 2 | 2
    ...   4 | 2 | 10
    ...   5 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_left(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2), t1.a == t2.b
    ... ).select(key=t1.a, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 1      | -1
    1   | 4      | 6
    1   | 7      | 6
    2   | 0      | 2
    2   | 3      | 2
    2   | 4      | 2
    2   | 7      |
    3   | 4      |
    """
    return window._join(
        self,
        other,
        self_time,
        other_time,
        *on,
        mode=pw.JoinMode.LEFT,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def window_join_right(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    window: temporal.Window,
    *on: pw.ColumnExpression,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> WindowJoinResult:
    """Performs a window right join of self with other using a window and join expressions.
    If two records belong to the same window and meet the conditions specified in
    the `on` clause, they will be joined. Note that if a sliding window is used and
    there are pairs of matching records that appear in more than one window,
    they will be included in the result multiple times (equal to the number of
    windows they appear in).

    When using a session window, the function creates sessions by concatenating
    records from both sides of a join. Only pairs of records that meet
    the conditions specified in the `on` clause can be part of the same session.
    The result of a given session will include all records from the left side of
    a join that belong to this session, joined with all records from the right
    side of a join that belong to this session.

    Rows from the right side that didn't match with any record on the left side in
    a given window, are returned with missing values on the left side replaced
    with `None`. The multiplicity of such rows equals the number of windows they
    belong to and don't have a match in them.

    Args:
        other:  the right side of a join.
        self_time: time expression in self.
        other_time: time expression in other.
        window: a window to use.
        on:  a list of column expressions. Each must have == on the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        WindowJoinResult: a result of the window join. A method ``.select()``
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 1
    ...   2 | 2
    ...   3 | 3
    ...   4 | 7
    ...   5 | 13
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 2
    ...   2 | 5
    ...   3 | 6
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.window_join_right(t2, t1.t, t2.t, pw.temporal.tumbling(2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
           | 5
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    >>> t4 = t1.window_join_right(t2, t1.t, t2.t, pw.temporal.sliding(1, 2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t4, include_id=False)
    left_t | right_t
           | 5
           | 5
           | 6
    1      | 2
    2      | 2
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    7      | 7
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 2
    ...   3 | 1 | 3
    ...   4 | 1 | 7
    ...   5 | 1 | 13
    ...   6 | 2 | 1
    ...   7 | 2 | 2
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 2
    ...   2 | 1 | 5
    ...   3 | 1 | 6
    ...   4 | 1 | 7
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_right(t2, t1.t, t2.t, pw.temporal.tumbling(2), t1.a == t2.b).select(
    ...     key=t2.b, left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   |        | 5
    1   | 2      | 2
    1   | 3      | 2
    1   | 7      | 6
    1   | 7      | 7
    2   | 2      | 2
    2   | 2      | 3
    4   |        | 3
    >>>
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | 0
    ...     1 | 5
    ...     2 | 10
    ...     3 | 15
    ...     4 | 17
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | -3
    ...     1 | 2
    ...     2 | 3
    ...     3 | 6
    ...     4 | 16
    ... '''
    ... )
    >>> t3 = t1.window_join_right(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2)
    ... ).select(left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
           | -3
    0      | 2
    0      | 3
    0      | 6
    5      | 2
    5      | 3
    5      | 6
    15     | 16
    17     | 16
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 4
    ...   3 | 1 | 7
    ...   4 | 2 | 0
    ...   5 | 2 | 3
    ...   6 | 2 | 4
    ...   7 | 2 | 7
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | -1
    ...   2 | 1 | 6
    ...   3 | 2 | 2
    ...   4 | 2 | 10
    ...   5 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_right(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2), t1.a == t2.b
    ... ).select(key=t2.b, left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 1      | -1
    1   | 4      | 6
    1   | 7      | 6
    2   |        | 10
    2   | 0      | 2
    2   | 3      | 2
    2   | 4      | 2
    4   |        | 3
    """
    return window._join(
        self,
        other,
        self_time,
        other_time,
        *on,
        mode=pw.JoinMode.RIGHT,
        left_instance=left_instance,
        right_instance=right_instance,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@check_arg_types
@trace_user_frame
def window_join_outer(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    window: temporal.Window,
    *on: pw.ColumnExpression,
    left_instance: pw.ColumnReference | None = None,
    right_instance: pw.ColumnReference | None = None,
) -> WindowJoinResult:
    """Performs a window outer join of self with other using a window and join expressions.
    If two records belong to the same window and meet the conditions specified in
    the `on` clause, they will be joined. Note that if a sliding window is used and
    there are pairs of matching records that appear in more than one window,
    they will be included in the result multiple times (equal to the number of
    windows they appear in).

    When using a session window, the function creates sessions by concatenating
    records from both sides of a join. Only pairs of records that meet
    the conditions specified in the `on` clause can be part of the same session.
    The result of a given session will include all records from the left side of
    a join that belong to this session, joined with all records from the right
    side of a join that belong to this session.

    Rows from both sides that didn't match with any record on the other side in
    a given window, are returned with missing values on the other side replaced
    with `None`. The multiplicity of such rows equals the number of windows they
    belong to and don't have a match in them.

    Args:
        other:  the right side of a join.
        self_time: time expression in self.
        other_time: time expression in other.
        window: a window to use.
        on:  a list of column expressions. Each must have == on the top level
            operation and be of the form LHS: ColumnReference == RHS: ColumnReference.
        left_instance/right_instance: optional arguments describing partitioning of the data into separate instances

    Returns:
        WindowJoinResult: a result of the window join. A method `.select()`
        can be called on it to extract relevant columns from the result of a join.

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 1
    ...   2 | 2
    ...   3 | 3
    ...   4 | 7
    ...   5 | 13
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | t
    ...   1 | 2
    ...   2 | 5
    ...   3 | 6
    ...   4 | 7
    ... '''
    ... )
    >>> t3 = t1.window_join_outer(t2, t1.t, t2.t, pw.temporal.tumbling(2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
           | 5
    1      |
    2      | 2
    3      | 2
    7      | 6
    7      | 7
    13     |
    >>> t4 = t1.window_join_outer(t2, t1.t, t2.t, pw.temporal.sliding(1, 2)).select(
    ...     left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t4, include_id=False)
    left_t | right_t
           | 5
           | 5
           | 6
    1      |
    1      | 2
    2      | 2
    2      | 2
    3      |
    3      | 2
    7      | 6
    7      | 7
    7      | 7
    13     |
    13     |
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 2
    ...   3 | 1 | 3
    ...   4 | 1 | 7
    ...   5 | 1 | 13
    ...   6 | 2 | 1
    ...   7 | 2 | 2
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | 2
    ...   2 | 1 | 5
    ...   3 | 1 | 6
    ...   4 | 1 | 7
    ...   5 | 2 | 2
    ...   6 | 2 | 3
    ...   7 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_outer(t2, t1.t, t2.t, pw.temporal.tumbling(2), t1.a == t2.b).select(
    ...     key=pw.coalesce(t1.a, t2.b), left_t=t1.t, right_t=t2.t
    ... )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   |        | 5
    1   | 1      |
    1   | 2      | 2
    1   | 3      | 2
    1   | 7      | 6
    1   | 7      | 7
    1   | 13     |
    2   | 1      |
    2   | 2      | 2
    2   | 2      | 3
    3   | 4      |
    4   |        | 3
    >>>
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | 0
    ...     1 | 5
    ...     2 | 10
    ...     3 | 15
    ...     4 | 17
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...       | t
    ...     0 | -3
    ...     1 | 2
    ...     2 | 3
    ...     3 | 6
    ...     4 | 16
    ... '''
    ... )
    >>> t3 = t1.window_join_outer(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2)
    ... ).select(left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    left_t | right_t
           | -3
    0      | 2
    0      | 3
    0      | 6
    5      | 2
    5      | 3
    5      | 6
    10     |
    15     | 16
    17     | 16
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...     | a | t
    ...   1 | 1 | 1
    ...   2 | 1 | 4
    ...   3 | 1 | 7
    ...   4 | 2 | 0
    ...   5 | 2 | 3
    ...   6 | 2 | 4
    ...   7 | 2 | 7
    ...   8 | 3 | 4
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...     | b | t
    ...   1 | 1 | -1
    ...   2 | 1 | 6
    ...   3 | 2 | 2
    ...   4 | 2 | 10
    ...   5 | 4 | 3
    ... '''
    ... )
    >>> t3 = t1.window_join_outer(
    ...     t2, t1.t, t2.t, pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2), t1.a == t2.b
    ... ).select(key=pw.coalesce(t1.a, t2.b), left_t=t1.t, right_t=t2.t)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    key | left_t | right_t
    1   | 1      | -1
    1   | 4      | 6
    1   | 7      | 6
    2   |        | 10
    2   | 0      | 2
    2   | 3      | 2
    2   | 4      | 2
    2   | 7      |
    3   | 4      |
    4   |        | 3
    """
    return window._join(
        self,
        other,
        self_time,
        other_time,
        *on,
        mode=pw.JoinMode.OUTER,
        left_instance=left_instance,
        right_instance=right_instance,
    )

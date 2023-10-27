# Copyright © 2023 Pathway

from __future__ import annotations

import pathway.internals as pw
from pathway.internals import expression as expr
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
from pathway.internals.join import validate_join_condition
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.thisclass import ThisMetaclass
from pathway.internals.trace import trace_user_frame


class AsofNowJoinResult(DesugaringContext):
    """Result of an asof now join between tables."""

    _original_left: pw.Table
    _left_with_forgetting: pw.Table
    _original_right: pw.Table
    _join_result: pw.JoinResult
    _table_substitution: dict[pw.TableLike, pw.Table]
    _mode: pw.JoinMode
    _id: expr.ColumnReference | None
    _substitution: dict[ThisMetaclass, pw.Joinable]

    def __init__(
        self,
        original_left: pw.Table,
        left: pw.Table,
        right: pw.Table,
        join_result: pw.JoinResult,
        table_substitution: dict[pw.TableLike, pw.Table],
        mode: pw.JoinMode,
        id: expr.ColumnReference | None,
    ):
        self._original_left = original_left
        self._left_with_forgetting = left
        self._original_right = right
        self._join_result = join_result
        self._table_substitution = table_substitution
        self._mode = mode
        self._id = id
        self._substitution = {pw.left: left, pw.right: right, pw.this: join_result}

    @staticmethod
    def _asof_now_join(
        left: pw.Table,
        right: pw.Table,
        *on: expr.ColumnExpression,
        mode: pw.JoinMode,
        id: expr.ColumnReference | None,
    ) -> AsofNowJoinResult:
        # TODO assert that left is append-only

        if mode != pw.JoinMode.INNER and mode != pw.JoinMode.LEFT:
            raise ValueError(
                "asof_now_join can only use modes pathway.JoinMode.INNER or pathway.JoinMode.LEFT"
            )

        left_with_forgetting = left._forget_immediately()
        for cond in on:
            cond_left, _, cond = validate_join_condition(cond, left, right)
            cond._left = left_with_forgetting[cond_left._name]
        if id is not None and id.table == left:
            id = left_with_forgetting[id._name]

        table_substitution: dict[pw.TableLike, pw.Table] = {
            left: left_with_forgetting,
        }
        join_result = left_with_forgetting.join(right, *on, id=id, how=mode)

        return AsofNowJoinResult(
            original_left=left,
            left=left_with_forgetting,
            right=right,
            join_result=join_result,
            table_substitution=table_substitution,
            mode=mode,
            id=id,
        )

    @property
    def _desugaring(self) -> TableSubstitutionDesugaring:
        return TableSubstitutionDesugaring(self._table_substitution)

    @trace_user_frame
    @desugar
    @arg_handler(handler=select_args_handler)
    def select(
        self, *args: expr.ColumnReference, **kwargs: expr.ColumnExpression
    ) -> pw.Table:
        """
        Computes a result of an asof now join.

        Args:
            args: Column references.
            kwargs: Column expressions with their new assigned names.

        Returns:
            Table: Created table.
        """
        result = self._join_result.select(*args, **kwargs)
        result = result._filter_out_results_of_forgetting()
        if (
            self._id is not None
            and self._id._column == self._left_with_forgetting._id_column
        ):
            if self._mode == pw.JoinMode.INNER:
                pw.universes.promise_is_subset_of(result, self._original_left)
            elif self._mode == pw.JoinMode.LEFT:
                # FIXME if original_left is append-only (should be) then result is
                # also append-only (promise that). Then with_universe_of should be able
                # to operate in const memory.
                result = result.with_universe_of(self._original_left)
        return result


@trace_user_frame
@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=True, allow_id=True))
@runtime_type_check
def asof_now_join(
    self: pw.Table,
    other: pw.Table,
    *on: pw.ColumnExpression,
    how: pw.JoinMode = pw.JoinMode.INNER,
    id: expr.ColumnReference | None = None,
) -> AsofNowJoinResult:
    """
    Performs asof now join of self with other using join expressions. Each row of self
    is joined with rows from other at a given processing time. Rows from self are not stored.
    They are joined with rows of other at their processing time. If other is updated
    in the future, rows from self from the past won't be updated.
    Rows from other are stored. They can be joined with future rows of self.

    Args:
        other: the right side of a join.
        on: a list of column expressions. Each must have == as the top level operation
            and be of the form LHS: ColumnReference == RHS: ColumnReference.
        id: optional argument for id of result, can be only self.id or other.id
        how: by default, inner join is performed. Possible values are JoinMode.{INNER,LEFT}
            which correspond to inner and left join respectively.

    Returns:
        AsofNowJoinResult: an object on which `.select()` may be called to extract relevant
        columns from the result of the join.
    """
    return AsofNowJoinResult._asof_now_join(self, other, *on, mode=how, id=id)


@trace_user_frame
@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=True, allow_id=True))
@runtime_type_check
def asof_now_join_inner(
    self: pw.Table,
    other: pw.Table,
    *on: pw.ColumnExpression,
    id: expr.ColumnReference | None = None,
) -> AsofNowJoinResult:
    """
    Performs asof now join of self with other using join expressions. Each row of self
    is joined with rows from other at a given processing time. Rows from self are not stored.
    They are joined with rows of other at their processing time. If other is updated
    in the future, rows from self from the past won't be updated.
    Rows from other are stored. They can be joined with future rows of self.

    Args:
        other: the right side of a join.
        on: a list of column expressions. Each must have == as the top level operation
            and be of the form LHS: ColumnReference == RHS: ColumnReference.
        id: optional argument for id of result, can be only self.id or other.id

    Returns:
        AsofNowJoinResult: an object on which `.select()` may be called to extract relevant
        columns from the result of the join.
    """
    return AsofNowJoinResult._asof_now_join(
        self, other, *on, mode=pw.JoinMode.INNER, id=id
    )


@trace_user_frame
@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=True, allow_id=True))
@runtime_type_check
def asof_now_join_left(
    self: pw.Table,
    other: pw.Table,
    *on: pw.ColumnExpression,
    id: expr.ColumnReference | None = None,
) -> AsofNowJoinResult:
    """
    Performs asof now join of self with other using join expressions. Each row of self
    is joined with rows from other at a given processing time. If there are no matching
    rows in other, missing values on the right side are replaced with `None`.
    Rows from self are not stored. They are joined with rows of other at their processing
    time. If other is updated in the future, rows from self from the past won't be updated.
    Rows from other are stored. They can be joined with future rows of self.

    Args:
        other: the right side of a join.
        on: a list of column expressions. Each must have == as the top level operation
            and be of the form LHS: ColumnReference == RHS: ColumnReference.
        id: optional argument for id of result, can be only self.id or other.id

    Returns:
        AsofNowJoinResult: an object on which `.select()` may be called to extract relevant
        columns from the result of the join.
    """
    return AsofNowJoinResult._asof_now_join(
        self, other, *on, mode=pw.JoinMode.LEFT, id=id
    )

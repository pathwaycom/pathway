# Copyright © 2024 Pathway

from __future__ import annotations

import itertools
from abc import abstractmethod
from collections.abc import Iterable, Iterator
from functools import lru_cache
from typing import TYPE_CHECKING

from pathway.internals.expression_visitor import IdentityTransform
from pathway.internals.trace import trace_user_frame

if TYPE_CHECKING:
    from pathway.internals.joins import JoinResult

import pathway.internals.column as clmn
import pathway.internals.expression as expr
from pathway.internals import table, thisclass
from pathway.internals.arg_handlers import arg_handler, reduce_args_handler
from pathway.internals.decorators import contextualized_operator
from pathway.internals.desugaring import (
    DesugaringContext,
    SubstitutionDesugaring,
    TableReduceDesugaring,
    ThisDesugaring,
    combine_args_kwargs,
    desugar,
)
from pathway.internals.helpers import StableSet
from pathway.internals.operator_input import OperatorInput
from pathway.internals.parse_graph import G
from pathway.internals.universe import Universe


class GroupedJoinable(DesugaringContext, OperatorInput):
    _substitution: dict[thisclass.ThisMetaclass, table.Joinable]
    _joinable_to_group: table.Joinable
    _universe: Universe

    def __init__(self, _universe: Universe, _substitution, _joinable: table.Joinable):
        self._substitution = _substitution
        self._joinable_to_group = _joinable
        self._universe = _universe

    @property
    def _desugaring(self) -> TableReduceDesugaring:
        return TableReduceDesugaring(self)

    @abstractmethod
    def reduce(
        self, *args: expr.ColumnReference, **kwargs: expr.ColumnExpression
    ) -> table.Table: ...

    @abstractmethod
    def _operator_dependencies(self) -> StableSet[table.Table]: ...

    def __getattr__(self, name):
        return getattr(self._joinable_to_group, name)

    def __getitem__(self, name):
        return self._joinable_to_group[name]

    def keys(self):
        return self._joinable_to_group.keys()

    def __iter__(self) -> Iterator[expr.ColumnReference]:
        return iter(self._joinable_to_group)


class GroupedTable(GroupedJoinable, OperatorInput):
    """Result of a groupby operation on a Table.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | dog
    ... 8   | Alice | cat
    ... 7   | Bob   | dog
    ... ''')
    >>> t2 = t1.groupby(t1.pet, t1.owner)
    >>> isinstance(t2, pw.GroupedTable)
    True
    """

    _grouping_columns: StableSet[expr.InternalColRef]
    _last_column_is_instance: bool
    _joinable_to_group: table.Table
    _set_id: bool
    _sort_by: expr.InternalColRef | None
    _filter_out_results_of_forgetting: bool
    _skip_errors: bool
    _is_window: bool

    def __init__(
        self,
        _table: table.Table,
        _grouping_columns: tuple[expr.InternalColRef, ...],
        _last_column_is_instance: bool,
        _set_id: bool = False,
        _sort_by: expr.InternalColRef | None = None,
        _filter_out_results_of_forgetting: bool = False,
        _skip_errors: bool = True,
        _is_window: bool = False,
    ):
        super().__init__(Universe(), {thisclass.this: self}, _table)
        self._grouping_columns = StableSet(_grouping_columns)
        self._last_column_is_instance = _last_column_is_instance
        self._set_id = _set_id
        self._sort_by = _sort_by
        self._filter_out_results_of_forgetting = _filter_out_results_of_forgetting
        self._skip_errors = _skip_errors
        self._is_window = _is_window

    @classmethod
    def create(
        cls,
        table: table.Table,
        grouping_columns: tuple[expr.ColumnReference, ...],
        last_column_is_instance: bool,
        set_id: bool = False,
        sort_by: expr.ColumnReference | None = None,
        _filter_out_results_of_forgetting: bool = False,
        _skip_errors: bool = True,
        _is_window: bool = False,
    ) -> GroupedTable:
        cols = tuple(arg._to_original()._to_internal() for arg in grouping_columns)
        col_sort_by = (
            sort_by._to_original()._to_internal() if sort_by is not None else None
        )
        key = (cls.__name__, table, cols, set_id, col_sort_by)
        if key not in G.cache:
            result = GroupedTable(
                _table=table,
                _grouping_columns=cols,
                _last_column_is_instance=last_column_is_instance,
                _set_id=set_id,
                _sort_by=col_sort_by,
                _filter_out_results_of_forgetting=_filter_out_results_of_forgetting,
                _skip_errors=_skip_errors,
                _is_window=_is_window,
            )
            G.cache[key] = result
        return G.cache[key]

    def _eval(
        self, expression: expr.ColumnExpression, context: clmn.Context
    ) -> clmn.ColumnWithExpression:
        desugared_expression = self._desugaring.eval_expression(expression)
        return self._joinable_to_group._eval(desugared_expression, context)

    @desugar
    @arg_handler(handler=reduce_args_handler)
    @trace_user_frame
    def reduce(
        self, *args: expr.ColumnReference, **kwargs: expr.ColumnExpression
    ) -> table.Table:
        """Reduces grouped table to a table.

        Args:
            args: Column references.
            kwargs: Column expressions with their new assigned names.

        Returns:
            Table: Created table.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t2 = t1.groupby(t1.pet, t1.owner).reduce(t1.owner, t1.pet, ageagg=pw.reducers.sum(t1.age))
        >>> pw.debug.compute_and_print(t2, include_id=False)
        owner | pet | ageagg
        Alice | cat | 8
        Alice | dog | 10
        Bob   | dog | 16
        """

        kwargs = combine_args_kwargs(args, kwargs)

        output_expressions = {}
        state = _ReducerExpressionState()
        splitter = _ReducerExpressionSplitter()
        for name, expression in kwargs.items():
            self._validate_expression(expression)
            output_expressions[name] = splitter.eval_expression(
                expression, eval_state=state
            )

        prepared = self._joinable_to_group.select(**state.below_reducer_expressions)
        desugaring = ThisDesugaring({thisclass.this: prepared})
        desugared_reducers = {
            name: desugaring.eval_expression(reducer)
            for name, reducer in state.reducers.items()
        }
        reduced = self._reduce(**desugared_reducers)
        if self._filter_out_results_of_forgetting:
            reduced = reduced._filter_out_results_of_forgetting()
        return reduced.select(**output_expressions)

    def _maybe_warn(self, expression: expr.ColumnExpression) -> None:
        if self._is_window and isinstance(expression, expr.ReducerExpression):
            expression._reducer.maybe_warn_in_windowby()

    @contextualized_operator
    def _reduce(self, **kwargs: expr.ColumnExpression) -> table.Table:
        reduced_columns: dict[str, clmn.ColumnWithExpression] = {}

        context = clmn.GroupedContext(
            table=self._joinable_to_group,
            grouping_columns=tuple(self._grouping_columns),
            last_column_is_instance=self._last_column_is_instance,
            set_id=self._set_id,
            inner_context=self._joinable_to_group._rowwise_context,
            sort_by=self._sort_by,
            skip_errors=self._skip_errors,
        )

        for column_name, value in kwargs.items():
            self._maybe_warn(value)
            column = self._eval(value, context)
            reduced_columns[column_name] = column

        result: table.Table = table.Table(
            _columns=reduced_columns,
            _context=context,
        )
        G.universe_solver.register_as_equal(self._universe, result._universe)
        return result

    def _validate_expression(self, expression: expr.ColumnExpression):
        for dep in expression._dependencies_above_reducer():
            if (
                not isinstance(dep._table, thisclass.ThisMetaclass)  # allow for ix
                and dep.to_column_expression()._to_original()._to_internal()
                not in self._grouping_columns
            ):
                raise ValueError(
                    f"You cannot use {dep.to_column_expression()} in this reduce statement.\n"
                    + f"Make sure that {dep.to_column_expression()} is used in a groupby or wrap it with a reducer, "
                    + f"e.g. pw.reducers.count({dep.to_column_expression()})"
                )

        for dep in expression._dependencies_below_reducer():
            if (
                self._joinable_to_group._universe
                != dep.to_column_expression()._column.universe
            ):
                raise ValueError(
                    f"You cannot use {dep.to_column_expression()} in this context."
                    + " Its universe is different than the universe of the table the method"
                    + " was called on. You can use <table1>.with_universe_of(<table2>)"
                    + " to assign universe of <table2> to <table1> if you're sure their"
                    + " sets of keys are equal."
                )

    @lru_cache
    def _operator_dependencies(self) -> StableSet[table.Table]:
        # TODO + grouping columns expression dependencies
        return self._joinable_to_group._operator_dependencies()


class GroupedJoinResult(GroupedJoinable):
    _substitution_desugaring: SubstitutionDesugaring
    _groupby: GroupedTable

    def __init__(
        self,
        _join_result: JoinResult,
        _args: Iterable[expr.ColumnExpression],
        _id: expr.ColumnReference | None,
    ):
        super().__init__(
            _join_result._universe,
            {
                **_join_result._substitution,
                thisclass.this: _join_result,
            },
            _join_result,
        )
        tab, subs = _join_result._substitutions()
        self._substitution_desugaring = SubstitutionDesugaring(subs)
        _args = [self._substitution_desugaring.eval_expression(arg) for arg in _args]
        if _id is not None:
            _id = self._substitution_desugaring.eval_expression(_id)

        self._groupby = tab.groupby(*_args, id=_id)

    @desugar
    @arg_handler(handler=reduce_args_handler)
    @trace_user_frame
    def reduce(
        self, *args: expr.ColumnReference, **kwargs: expr.ColumnExpression
    ) -> table.Table:
        """Reduces grouped join result to table.

        Returns:
            Table: Created table.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...    cost  owner  pet
        ... 1   100  Alice    1
        ... 2    90    Bob    1
        ... 3    80  Alice    2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...     cost  owner  pet size
        ... 11   100  Alice    3    M
        ... 12    90    Bob    1    L
        ... 13    80    Tom    1   XL
        ... ''')
        >>> result = (t1.join(t2, t1.owner==t2.owner).groupby(pw.this.owner)
        ...     .reduce(pw.this.owner, pairs = pw.reducers.count()))
        >>> pw.debug.compute_and_print(result, include_id=False)
        owner | pairs
        Alice | 2
        Bob   | 1
        """
        kwargs = combine_args_kwargs(args, kwargs)
        desugared_kwargs = {
            name: self._substitution_desugaring.eval_expression(arg)
            for name, arg in kwargs.items()
        }
        return self._groupby.reduce(**desugared_kwargs)

    @property
    def _desugaring(self) -> TableReduceDesugaring:
        return TableReduceDesugaring(self)

    @lru_cache
    def _operator_dependencies(self) -> StableSet[table.Table]:
        # TODO + grouping columns expression dependencies
        return self._groupby._operator_dependencies()


class _ReducerExpressionState:
    below_reducer_expressions: dict[str, expr.ColumnExpression]
    reducers: dict[str, expr.ColumnExpression]

    def __init__(self) -> None:
        self.below_reducer_expressions = {}
        self.reducers = {}
        self.expressions_count = itertools.count()
        self.reducers_count = itertools.count()

    def add_dependency(self, expression: expr.ColumnExpression) -> expr.ColumnReference:
        name = f"_pw_{next(self.expressions_count)}"
        self.below_reducer_expressions[name] = expression
        return thisclass.this[name]

    def add_reducer(self, expression: expr.ColumnExpression) -> expr.ColumnReference:
        name = f"_pw_{next(self.reducers_count)}"
        self.reducers[name] = expression
        return thisclass.this[name]


class _ReducerExpressionSplitter(IdentityTransform):
    def eval_column_val(
        self,
        expression: expr.ColumnReference,
        eval_state: _ReducerExpressionState | None = None,
        **kwargs,
    ) -> expr.ColumnReference:
        if (
            isinstance(expression.table, thisclass.ThisMetaclass)
            and expression.table._delay_depth() > 0
        ):
            # descend into ix args
            key_expression = expression.table._expression()
            evaluated_key_expression = self.eval_expression(
                key_expression, eval_state=eval_state
            )
            evaluated_table = expression.table._with_new_expression(
                evaluated_key_expression
            )
            return evaluated_table[expression.name]
        assert eval_state is not None
        expression = eval_state.add_dependency(expression)
        return eval_state.add_reducer(expression)

    def eval_reducer(  # type: ignore
        self,
        expression: expr.ReducerExpression,
        eval_state: _ReducerExpressionState | None = None,
        **kwargs,
    ) -> expr.ColumnReference:
        assert eval_state is not None
        col_refs = [eval_state.add_dependency(arg) for arg in expression._args]
        expression = expr.ReducerExpression(expression._reducer, *col_refs)
        return eval_state.add_reducer(expression)

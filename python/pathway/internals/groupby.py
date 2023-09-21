# Copyright © 2023 Pathway

from __future__ import annotations

import itertools
from abc import abstractmethod
from functools import lru_cache
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, Optional, Tuple, cast

from pathway.internals.trace import trace_user_frame

if TYPE_CHECKING:
    from pathway.internals.join import JoinResult

import pathway.internals.column as clmn
import pathway.internals.expression as expr
from pathway.internals import table, table_like, thisclass
from pathway.internals.arg_handlers import arg_handler, reduce_args_handler
from pathway.internals.decorators import contextualized_operator
from pathway.internals.desugaring import (
    DesugaringContext,
    SubstitutionDesugaring,
    TableReduceDesugaring,
    combine_args_kwargs,
    desugar,
)
from pathway.internals.helpers import StableSet
from pathway.internals.operator_input import OperatorInput
from pathway.internals.parse_graph import G
from pathway.internals.universe import Universe


class GroupedJoinable(DesugaringContext, table_like.TableLike, OperatorInput):
    _substitution: Dict[thisclass.ThisMetaclass, table.Joinable]
    _joinable_to_group: table.Joinable

    def __init__(self, _universe: Universe, _substitution, _joinable: table.Joinable):
        super().__init__(_universe)
        self._substitution = _substitution
        self._joinable_to_group = _joinable

    @property
    def _desugaring(self) -> TableReduceDesugaring:
        return TableReduceDesugaring(self)

    @abstractmethod
    def reduce(
        self, *args: expr.ColumnReference, **kwargs: expr.ColumnExpression
    ) -> table.Table:
        ...

    @abstractmethod
    def _operator_dependencies(self) -> StableSet[table.Table]:
        ...

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
    >>> t1 = pw.debug.parse_to_table('''
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

    _context: clmn.GroupedContext
    _columns: Dict[str, clmn.Column]
    _grouping_columns: StableSet[expr.InternalColRef]
    _joinable_to_group: table.Table

    def __init__(
        self,
        table: table.Table,
        grouping_columns: Tuple[expr.InternalColRef, ...],
        set_id: bool = False,
    ):
        super().__init__(Universe(), {thisclass.this: self}, table)
        self._grouping_columns = StableSet(grouping_columns)
        self._context = clmn.GroupedContext(
            table=table,
            universe=self._universe,
            grouping_columns={
                column: table._eval(column.to_colref(), table._context)
                for column in grouping_columns
            },
            set_id=set_id,
            inner_context=table._context,
        )
        self._columns = {
            column._name: table._eval(column.to_colref(), self._context)
            for column in grouping_columns
        }

    @classmethod
    def create(
        cls,
        table: table.Table,
        grouping_columns: Tuple[expr.ColumnReference, ...],
        set_id: bool = False,
    ) -> GroupedTable:
        cols = tuple(arg._to_original_internal() for arg in grouping_columns)
        key = (cls.__name__, table._universe, cols, set_id)
        if key not in G.cache:
            result = GroupedTable(
                table=table,
                grouping_columns=cols,
                set_id=set_id,
            )
            G.cache[key] = result
        return G.cache[key]

    def _eval(
        self, expression: expr.ColumnExpression, context: clmn.Context
    ) -> clmn.ColumnWithExpression:
        desugared_expression = self._desugaring.eval_expression(expression)
        return self._joinable_to_group._eval(desugared_expression, context)

    @trace_user_frame
    @desugar
    @arg_handler(handler=reduce_args_handler)
    @contextualized_operator
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
        >>> t1 = pw.debug.parse_to_table('''
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
        reduced_columns: Dict[str, clmn.ColumnWithExpression] = {}

        kwargs = combine_args_kwargs(args, kwargs)

        for column_name, value in kwargs.items():
            self._validate_expression(value)
            column = self._eval(value, self._context)
            reduced_columns[column_name] = column

        for column in reduced_columns.values():
            deps = (dep.to_original() for dep in column.expression._dependencies())
            self._context.requested_grouping_columns.update(
                dep for dep in deps if dep in self._context.grouping_columns
            )

        return table.Table(
            columns=reduced_columns,
            universe=self._universe,
            pk_columns=self._columns,
            id_column=clmn.IdColumn(self._context),
        )

    def _validate_expression(self, expression: expr.ColumnExpression):
        for dep in expression._dependencies_above_reducer():
            if dep.to_original() in self._grouping_columns:
                pass
            else:
                if not self._universe.is_subset_of(dep.to_colref()._column.universe):
                    raise ValueError(
                        f"You cannot use {dep.to_colref()} in this reduce statement.\n"
                        + f"Make sure that {dep.to_colref()} is used in a groupby or wrap it with a reducer, "
                        + f"e.g. pw.reducers.count({dep.to_colref()})"
                    )

        for dep in expression._dependencies_below_reducer():
            assert self._joinable_to_group._universe.is_subset_of(
                dep.to_colref()._column.universe
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
        *,
        join_result: JoinResult,
        args: Iterable[expr.ColumnExpression],
        id: Optional[expr.ColumnReference],
    ):
        super().__init__(
            join_result._universe,
            {
                **join_result._substitution,
                thisclass.this: join_result,
            },
            join_result,
        )
        tab, subs = join_result._substitutions(itertools.count(0))
        self._substitution_desugaring = SubstitutionDesugaring(subs)
        args = [self._substitution_desugaring.eval_expression(arg) for arg in args]
        if id is not None:
            id = cast(
                expr.ColumnReference, self._substitution_desugaring.eval_expression(id)
            )
        self._groupby = tab.groupby(*args, id=id)

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
        >>> t1 = pw.debug.parse_to_table('''
        ...    cost  owner  pet
        ... 1   100  Alice    1
        ... 2    90    Bob    1
        ... 3    80  Alice    2
        ... ''')
        >>> t2 = pw.debug.parse_to_table('''
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

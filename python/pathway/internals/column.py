# Copyright © 2023 Pathway

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
    get_args,
    get_origin,
)

import numpy as np

import pathway.internals as pw
from pathway.internals import api, trace
from pathway.internals.dtype import DType, types_lca
from pathway.internals.expression import (
    ColumnExpression,
    ColumnReference,
    ColumnRefOrIxExpression,
)
from pathway.internals.helpers import SetOnceProperty, StableSet

if TYPE_CHECKING:
    from pathway.internals.expression import InternalColRef
    from pathway.internals.operator import OutputHandle
    from pathway.internals.table import Table
    from pathway.internals.universe import Universe


@dataclass(eq=False, frozen=True)
class Lineage:
    source: OutputHandle
    """Source handle."""

    @property
    def trace(self) -> trace.Trace:
        return self.source.operator.trace


@dataclass(eq=False, frozen=True)
class ColumnLineage(Lineage):
    name: str
    """Original name of a column."""

    def get_original_column(self):
        if self.name == "id":
            return self.table._id_column
        else:
            return self.table._get_column(self.name)

    @property
    def is_method(self) -> bool:
        return isinstance(self.get_original_column(), MethodColumn)

    @property
    def table(self) -> Table:
        return self.source.value


class Column(ABC):
    dtype: DType
    universe: Universe
    lineage: SetOnceProperty[ColumnLineage] = SetOnceProperty()
    """Lateinit by operator."""

    def __init__(self, dtype: DType, universe: Universe) -> None:
        super().__init__()
        self.dtype = dtype
        self.universe = universe
        self._trace = trace.Trace.from_traceback()

    def column_dependencies(self) -> StableSet[Column]:
        return StableSet([self])

    @property
    def trace(self) -> trace.Trace:
        if hasattr(self, "lineage"):
            return self.lineage.trace
        else:
            return self._trace


class MaterializedColumn(Column):
    """Column not requiring evaluation."""

    pass


class MethodColumn(MaterializedColumn):
    """Column representing an output method in a RowTransformer."""

    pass


class ColumnWithContext(Column, ABC):
    """Column holding a context."""

    context: Context

    def __init__(self, dtype: DType, context: Context, universe: Universe):
        super().__init__(dtype, universe)
        self.context = context

    def column_dependencies(self) -> StableSet[Column]:
        return super().column_dependencies() | self.context.column_dependencies()


class IdColumn(ColumnWithContext):
    def __init__(self, context: Context) -> None:
        super().__init__(DType(api.Pointer), context, context.universe)


class ColumnWithExpression(ColumnWithContext):
    """Column holding expression and context."""

    expression: ColumnExpression
    context: Context

    def __init__(
        self,
        context: Context,
        universe: Universe,
        expression: ColumnExpression,
        lineage: Optional[Lineage] = None,
    ):
        dtype = context.expression_type(expression)
        super().__init__(dtype, context, universe)
        self.expression = expression
        if lineage is not None:
            self.lineage = lineage

    def dereference(self) -> Column:
        raise RuntimeError("expression cannot be dereferenced")

    def column_dependencies(self) -> StableSet[Column]:
        return super().column_dependencies() | self.expression._column_dependencies()


class ColumnWithReference(ColumnWithExpression):
    expression: ColumnRefOrIxExpression

    def __init__(
        self,
        context: Context,
        universe: Universe,
        expression: ColumnRefOrIxExpression,
        lineage: Optional[Lineage] = None,
    ):
        super().__init__(context, universe, expression, lineage)
        self.expression = expression
        if lineage is None:
            lineage = expression._column.lineage
            if lineage.is_method or universe.is_subset_of(expression._column.universe):
                self.lineage = lineage

    def dereference(self) -> Column:
        return self.expression._column

    def column_dependencies(self) -> StableSet[Column]:
        return (
            super().column_dependencies()
            | self.context.reference_column_dependencies(self.expression)
        )


@dataclass(eq=True, frozen=True)
class ContextTable:
    """Simplified table representation used in contexts."""

    columns: Tuple[Column, ...]
    universe: Universe

    def __post_init__(self):
        assert all((column.universe == self.universe) for column in self.columns)


@dataclass(eq=False, frozen=True)
class Context:
    """Context of the column evaluation.

    Context will be mapped to proper evaluator based on its type.
    """

    universe: Universe
    """Resulting universe."""

    def columns_to_eval(self) -> Iterable[Column]:
        return []

    def column_dependencies(self) -> StableSet[Column]:
        deps = (col.column_dependencies() for col in self.columns_to_eval())
        return StableSet.union(*deps)

    def reference_column_dependencies(
        self, ref: ColumnRefOrIxExpression
    ) -> StableSet[Column]:
        return StableSet()

    def _get_type_interpreter(self):
        from pathway.internals.type_interpreter import TypeInterpreter

        return TypeInterpreter()

    def expression_type(self, expression: ColumnExpression) -> DType:
        return self.expression_with_type(expression)._dtype

    def expression_with_type(self, expression: ColumnExpression) -> ColumnExpression:
        from pathway.internals.type_interpreter import TypeInterpreterState

        return self._get_type_interpreter().eval_expression(
            expression, state=TypeInterpreterState()
        )


@dataclass(eq=False, frozen=True)
class RowwiseContext(Context):
    """Context for basic expressions."""


@dataclass(eq=False, frozen=True)
class TableRestrictedRowwiseContext(Context):
    """Restricts expression to specific table."""

    table: pw.Table


@dataclass(eq=False, frozen=True)
class GroupedContext(Context):
    """Context of `table.groupby().reduce() operation."""

    table: pw.Table
    grouping_columns: Dict[InternalColRef, Column]
    set_id: bool
    """Whether id should be set based on grouping column."""
    inner_context: RowwiseContext
    """Original context of grouped table."""
    requested_grouping_columns: StableSet[InternalColRef] = field(
        default_factory=StableSet, compare=False, hash=False
    )

    def columns_to_eval(self) -> Iterable[Column]:
        return self.grouping_columns.values()


@dataclass(eq=False, frozen=True)
class FilterContext(Context):
    """Context of `table.filter() operation."""

    filtering_column: ColumnWithExpression
    universe_to_filter: Universe

    def columns_to_eval(self) -> Iterable[Column]:
        return [self.filtering_column]


@dataclass(eq=False, frozen=True)
class ReindexContext(Context):
    """Context of `table.with_id() operation."""

    reindex_column: ColumnWithExpression

    def columns_to_eval(self) -> Iterable[Column]:
        return [self.reindex_column]


@dataclass(eq=False, frozen=True)
class IntersectContext(Context):
    """Context of `table.intersect() operation."""

    intersecting_universes: Tuple[Universe, ...]

    def __post_init__(self):
        assert len(self.intersecting_universes) > 0


@dataclass(eq=False, frozen=True)
class DifferenceContext(Context):
    """Context of `table.difference() operation."""

    left: Universe
    right: Universe


@dataclass(eq=False, frozen=True)
class HavingContext(Context):
    orig_universe: Universe
    key_column: Column

    def columns_to_eval(self) -> Iterable[Column]:
        return [self.key_column]


@dataclass(eq=False, frozen=True)
class UpdateRowsContext(Context):
    """Context of `table.update_rows()` and related operations."""

    updates: Dict[str, Column]
    union_universes: Tuple[Universe, ...]

    def __post_init__(self):
        assert len(self.union_universes) > 0

    def reference_column_dependencies(
        self, ref: ColumnRefOrIxExpression
    ) -> StableSet[Column]:
        return StableSet([self.updates[ref.name]])

    def expression_type(self, expression: ColumnExpression):
        assert isinstance(expression, ColumnReference)
        t1 = super().expression_type(expression)
        t2 = self.updates[expression.name].dtype
        return types_lca(t1, t2)


@dataclass(eq=False, frozen=True)
class ConcatUnsafeContext(Context):
    """Context of `table.concat_unsafe()`."""

    updates: Tuple[Dict[str, Column], ...]
    union_universes: Tuple[Universe, ...]

    def __post_init__(self):
        assert len(self.union_universes) > 0

    def reference_column_dependencies(
        self, ref: ColumnRefOrIxExpression
    ) -> StableSet[Column]:
        return StableSet([update[ref.name] for update in self.updates])

    def expression_type(self, expression: ColumnExpression):
        assert isinstance(expression, ColumnReference)
        t = super().expression_type(expression)
        for update in self.updates:
            up = update[expression.name].dtype
            t = types_lca(t, up)
        return t


@dataclass(eq=False, frozen=True)
class PromiseSameUniverseContext(Context):
    """Context of table.unsafe_promise_same_universe_as() operation."""

    pass


@dataclass(eq=True, frozen=True)
class JoinContext(Context):
    """Context of `table.join() operation."""

    left_table: pw.Table
    right_table: pw.Table
    on_left: ContextTable
    on_right: ContextTable
    assign_id: bool
    left_ear: bool
    right_ear: bool

    def columns_to_eval(self) -> Iterable[Column]:
        return chain(self.on_left.columns, self.on_right.columns)

    def _get_type_interpreter(self):
        from pathway.internals.type_interpreter import JoinTypeInterpreter

        return JoinTypeInterpreter(
            self.left_table, self.right_table, self.right_ear, self.left_ear
        )


@dataclass(eq=True, frozen=True)
class JoinFilterContext(JoinContext):
    filtering_column: Column

    def columns_to_eval(self) -> Iterable[Column]:
        return chain([self.filtering_column], super().columns_to_eval())


@dataclass(eq=False, frozen=True)
class FlattenContext(Context):
    """Context of `table.flatten() operation."""

    orig_universe: Universe
    flatten_column: ColumnWithExpression
    flatten_result_column: MaterializedColumn
    inner_context: RowwiseContext

    def columns_to_eval(self) -> Iterable[Column]:
        return [self.flatten_column]

    @staticmethod
    def get_flatten_column_dtype(flatten_column: ColumnWithExpression):
        from pathway.internals import type_interpreter

        dtype = type_interpreter.eval_type(flatten_column.expression)
        if get_origin(dtype) == list:
            return get_args(dtype)[0]
        elif get_origin(dtype) == tuple:
            return_dtype = get_args(dtype)[0]
            for single_dtype in get_args(dtype)[1:]:
                if single_dtype != Ellipsis:
                    return_dtype = types_lca(return_dtype, single_dtype)
            return return_dtype
        elif dtype == str:
            return str
        elif dtype in {np.ndarray, Any}:
            return Any
        else:
            raise TypeError(
                f"Cannot flatten column {flatten_column.expression!r} of type {dtype}."
            )


@dataclass(eq=False, frozen=True)
class SortingContext(Context):
    """Context of table.sort() operation."""

    key_column: ColumnWithExpression
    instance_column: ColumnWithExpression
    prev_column: MaterializedColumn
    next_column: MaterializedColumn

    def columns_to_eval(self) -> Iterable[Column]:
        return [self.key_column, self.instance_column]

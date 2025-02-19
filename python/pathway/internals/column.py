# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import cached_property
from itertools import chain
from types import EllipsisType
from typing import TYPE_CHECKING, Any, ClassVar

import pathway.internals as pw
from pathway.engine import ExternalIndexFactory
from pathway.internals import column_properties as cp, dtype as dt, trace
from pathway.internals.datasource import GenericDataSource
from pathway.internals.expression import ColumnExpression, ColumnReference
from pathway.internals.helpers import SetOnceProperty, StableSet
from pathway.internals.parse_graph import G
from pathway.internals.schema import Schema
from pathway.internals.universe import Universe

if TYPE_CHECKING:
    from pathway.internals import api
    from pathway.internals.expression import InternalColRef
    from pathway.internals.operator import OutputHandle
    from pathway.internals.table import Table


@dataclass(frozen=True)
class Lineage:
    source: OutputHandle
    """Source handle."""

    @property
    def trace(self) -> trace.Trace:
        return self.source.operator.trace


@dataclass(frozen=True)
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
    universe: Universe
    lineage: SetOnceProperty[ColumnLineage] = SetOnceProperty()
    """Lateinit by operator."""

    def __init__(self, universe: Universe) -> None:
        super().__init__()
        self.universe = universe
        self._trace = trace.Trace.from_traceback()

    def column_dependencies(self) -> StableSet[Column]:
        return StableSet([])

    @property
    def trace(self) -> trace.Trace:
        if hasattr(self, "lineage"):
            return self.lineage.trace
        else:
            return self._trace

    @property
    @abstractmethod
    def properties(self) -> cp.ColumnProperties: ...

    @property
    def dtype(self) -> dt.DType:
        return self.properties.dtype


class MaterializedColumn(Column):
    """Column not requiring evaluation."""

    def __init__(
        self,
        universe: Universe,
        properties: cp.ColumnProperties,
    ):
        super().__init__(universe)
        self._properties = properties

    @property
    def properties(self) -> cp.ColumnProperties:
        return self._properties


class ExternalMaterializedColumn(MaterializedColumn):
    """Temporary construct to differentiate between internal and external
    MaterializedColumns in pw.iterate. Replace with MaterializedColumn when done"""

    # TODO
    pass


class MethodColumn(MaterializedColumn):
    """Column representing an output method in a RowTransformer."""

    pass


class ColumnWithContext(Column, ABC):
    """Column holding a context."""

    context: Context

    def __init__(
        self,
        context: Context,
        universe: Universe,
    ):
        super().__init__(universe)
        self.context = context

    def column_dependencies(self) -> StableSet[Column]:
        return super().column_dependencies() | self.context.column_dependencies()

    @cached_property
    def properties(self) -> cp.ColumnProperties:
        return self.context.column_properties(self)

    @cached_property
    @abstractmethod
    def context_dtype(self) -> dt.DType: ...


class IdColumn(ColumnWithContext):
    def __init__(self, context: Context) -> None:
        super().__init__(context, context.universe)

    @cached_property
    def context_dtype(self) -> dt.DType:
        return self.context.id_column_type()


class MaterializedIdColumn(IdColumn):
    context: MaterializedContext

    def __init__(
        self, context: MaterializedContext, properties: cp.ColumnProperties
    ) -> None:
        super().__init__(context)
        self._properties = properties

    @property
    def properties(self) -> cp.ColumnProperties:
        return self._properties


class ColumnWithoutExpression(ColumnWithContext):
    _dtype: dt.DType

    def __init__(
        self,
        context: Context,
        universe: Universe,
        dtype: dt.DType,
    ) -> None:
        super().__init__(context, universe)
        self._dtype = dtype

    @cached_property
    def context_dtype(self) -> dt.DType:
        return self._dtype


class ColumnWithExpression(ColumnWithContext):
    """Column holding expression and context."""

    expression: ColumnExpression
    context: Context

    def __init__(
        self,
        context: Context,
        universe: Universe,
        expression: ColumnExpression,
        lineage: Lineage | None = None,
    ):
        super().__init__(context, universe)
        self.expression = expression
        if lineage is not None:
            self.lineage = lineage

    def dereference(self) -> Column:
        raise RuntimeError("expression cannot be dereferenced")

    def column_dependencies(self) -> StableSet[Column]:
        return super().column_dependencies() | self.expression._column_dependencies()

    @cached_property
    def context_dtype(self) -> dt.DType:
        return self.context.expression_type(self.expression)


class ColumnWithReference(ColumnWithExpression):
    expression: ColumnReference

    def __init__(
        self,
        context: Context,
        universe: Universe,
        expression: ColumnReference,
        lineage: Lineage | None = None,
    ):
        super().__init__(context, universe, expression, lineage)
        self.expression = expression
        if lineage is None:
            lineage = expression._column.lineage
            if lineage.is_method or universe == expression._column.universe:
                self.lineage = lineage

    def dereference(self) -> Column:
        return self.expression._column

    def column_dependencies(self) -> StableSet[Column]:
        return super().column_dependencies() | self.reference_column_dependencies()

    def reference_column_dependencies(self) -> StableSet[Column]:
        return self.context.reference_column_dependencies(self.expression)


@dataclass(eq=True, frozen=True)
class ContextTable:
    """Simplified table representation used in contexts."""

    columns: tuple[Column, ...]
    universe: Universe

    def __post_init__(self):
        assert all((column.universe == self.universe) for column in self.columns)


def _create_internal_table(columns: Iterable[Column], context: Context) -> Table:
    from pathway.internals.table import Table

    columns_dict = {f"{i}": column for i, column in enumerate(columns)}
    return Table(columns_dict, _context=context)


@dataclass(eq=False, frozen=True)
class Context(ABC):
    """Context of the column evaluation.

    Context will be mapped to proper evaluator based on its type.
    """

    _column_properties_evaluator: ClassVar[type[cp.ColumnPropertiesEvaluator]]

    @cached_property
    def id_column(self) -> IdColumn:
        return IdColumn(self)

    @property
    @abstractmethod
    def universe(self) -> Universe: ...

    def column_dependencies_external(self) -> Iterable[Column]:
        return []

    def column_dependencies_internal(self) -> Iterable[Column]:
        return []

    def column_dependencies(self) -> StableSet[Column]:
        # columns depend on columns in their context, not dependencies of columns in context
        return StableSet(
            chain(
                self.column_dependencies_external(),
                self.column_dependencies_internal(),
            )
        )

    def reference_column_dependencies(self, ref: ColumnReference) -> StableSet[Column]:
        return StableSet()

    def _get_type_interpreter(self):
        from pathway.internals.type_interpreter import TypeInterpreter

        return TypeInterpreter()

    def expression_type(self, expression: ColumnExpression) -> dt.DType:
        return self.expression_with_type(expression)._dtype

    @abstractmethod
    def id_column_type(self) -> dt.DType: ...

    def expression_with_type(self, expression: ColumnExpression) -> ColumnExpression:
        from pathway.internals.type_interpreter import TypeInterpreterState

        return self._get_type_interpreter().eval_expression(
            expression, state=TypeInterpreterState()
        )

    def intermediate_tables(self) -> Iterable[Table]:
        dependencies = list(self.column_dependencies_internal())
        if len(dependencies) == 0:
            return []
        context = None
        columns: list[ColumnWithContext] = []
        for column in dependencies:
            assert isinstance(
                column, ColumnWithContext
            ), f"Column {column} that is not ColumnWithContext appeared in column_dependencies_internal()"
            assert context is None or context == column.context
            columns.append(column)
            context = column.context
        assert context is not None
        return [_create_internal_table(columns, context)]

    def column_properties(self, column: ColumnWithContext) -> cp.ColumnProperties:
        return self._column_properties_evaluator(self).eval(column)

    def __init_subclass__(
        cls,
        /,
        column_properties_evaluator: type[
            cp.ColumnPropertiesEvaluator
        ] = cp.DefaultPropsEvaluator,
        **kwargs,
    ) -> None:
        super().__init_subclass__(**kwargs)
        cls._column_properties_evaluator = column_properties_evaluator


@dataclass(eq=False, frozen=True)
class RowwiseContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context for basic expressions."""

    _id_column: IdColumn

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self._id_column]

    def id_column_type(self) -> dt.DType:
        return self._id_column.dtype

    @property
    def universe(self) -> Universe:
        return self._id_column.universe


@dataclass(eq=False, frozen=True)
class MaterializedContext(Context):
    _universe: Universe
    _universe_properties: cp.ColumnProperties = cp.ColumnProperties(
        dtype=dt.ANY_POINTER
    )

    @property
    def id_column(self) -> MaterializedIdColumn:
        return MaterializedIdColumn(self, self._universe_properties)

    @property
    def universe(self) -> Universe:
        return self._universe

    def id_column_type(self) -> dt.DType:
        return self._universe_properties.dtype


@dataclass(eq=False, frozen=True)
class SetSchemaContext(RowwiseContext):
    _new_properties: dict[InternalColRef, cp.ColumnProperties]
    _id_column_props: cp.ColumnProperties

    def expression_with_type(self, expression: ColumnExpression) -> ColumnExpression:
        assert isinstance(expression, ColumnReference)
        internal = expression._to_internal()
        if internal._name == "id":
            dtype = self._id_column_props.dtype
        else:
            dtype = self._new_properties[internal].dtype
        ret = internal.to_column_expression()
        ret._dtype = dtype
        return ret

    def input_universe(self) -> Universe:
        return self._id_column.universe

    def column_properties(self, column: ColumnWithContext) -> cp.ColumnProperties:
        if isinstance(column, IdColumn):
            return self._id_column_props
        else:
            assert isinstance(column, ColumnWithExpression)
            expression = column.expression
            assert isinstance(expression, ColumnReference)
            return self._new_properties[expression._to_internal()]

    def id_column_type(self) -> dt.DType:
        return self._id_column_props.dtype


@dataclass(eq=False, frozen=True)
class GradualBroadcastContext(Context):
    orig_id_column: IdColumn
    lower_column: ColumnWithExpression
    value_column: ColumnWithExpression
    upper_column: ColumnWithExpression

    def column_dependencies_internal(self) -> Iterable[Column]:
        return [self.lower_column, self.value_column, self.upper_column]

    def input_universe(self) -> Universe:
        return self.universe

    def id_column_type(self) -> dt.DType:
        return self.orig_id_column.dtype

    @cached_property
    def apx_value_column(self):
        return MaterializedColumn(self.universe, cp.ColumnProperties(dtype=dt.FLOAT))

    @property
    def universe(self) -> Universe:
        return self.orig_id_column.universe


@dataclass(eq=False, frozen=True)
class ExternalIndexAsOfNowContext(Context):
    _index_id_column: IdColumn
    _query_id_column: IdColumn
    index_table: pw.Table
    query_table: pw.Table
    index_column: ColumnWithExpression
    query_column: ColumnWithExpression
    index_factory: ExternalIndexFactory
    query_response_limit_column: ColumnWithExpression | None
    index_filter_data_column: ColumnWithExpression | None
    query_filter_column: ColumnWithExpression | None
    res_type: dt.DType

    @property
    def universe(self) -> Universe:
        return self._query_id_column.universe

    def id_column_type(self) -> dt.DType:
        return self._query_id_column.dtype

    def _index_columns(self) -> list[Column]:
        columns = [self.index_column, self.index_filter_data_column]
        return [col for col in columns if col is not None]

    def _query_columns(self) -> list[Column]:
        columns = [
            self.query_column,
            self.query_response_limit_column,
            self.query_filter_column,
        ]
        return [col for col in columns if col is not None]

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self._index_id_column, self._query_id_column]

    def column_dependencies_internal(self) -> Iterable[Column]:
        return self._index_columns() + self._query_columns()

    def index_universe(self) -> Universe:
        return self.index_table._universe

    def query_universe(self) -> Universe:
        return self.query_table._universe

    def intermediate_tables(self) -> Iterable[Table]:
        index_columns = self._index_columns()
        query_columns = self._query_columns()

        return [
            _create_internal_table(
                index_columns,
                self.index_table._rowwise_context,
            ),
            _create_internal_table(
                query_columns,
                self.query_table._rowwise_context,
            ),
        ]

    @cached_property
    def index_reply(self):
        return MaterializedColumn(
            self.query_table._universe,
            cp.ColumnProperties(
                dtype=self.res_type,
                append_only=self.query_column.properties.append_only,
            ),
        )


@dataclass(eq=False, frozen=True)
class TableRestrictedRowwiseContext(
    RowwiseContext, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Restricts expression to specific table."""

    table: pw.Table


@dataclass(eq=False, frozen=True)
class GroupedContext(Context):
    """Context of `table.groupby().reduce() operation."""

    table: pw.Table
    grouping_columns: tuple[InternalColRef, ...]
    last_column_is_instance: bool
    set_id: bool
    """Whether id should be set based on grouping column."""
    inner_context: RowwiseContext
    """Original context of grouped table."""
    skip_errors: bool
    sort_by: InternalColRef | None = None

    def _get_type_interpreter(self):
        from pathway.internals.type_interpreter import ReducerInterprerer

        return ReducerInterprerer(self.table._id_column.dtype)

    def id_column_type(self) -> dt.DType:
        if self.set_id:
            return self.expression_type(self.grouping_columns[0].to_column_expression())
        else:
            return dt.Pointer(
                *[
                    self.inner_context.expression_type(arg.to_column_expression())
                    for arg in self.grouping_columns
                ]
            )

    def column_dependencies_external(self) -> Iterable[Column]:
        deps = list(self.grouping_columns)
        if self.sort_by is not None:
            deps.append(self.sort_by)
        return [dep._column for dep in deps]

    @cached_property
    def universe(self) -> Universe:
        ret = Universe()
        if self.inner_context.universe.is_empty():
            ret.register_as_empty(no_warn=False)
        return ret


@dataclass(eq=False, frozen=True)
class DeduplicateContext(Context):
    value: ColumnWithExpression
    instance: tuple[ColumnWithExpression, ...]
    acceptor: Callable[[Any, Any], bool]
    orig_id_column: IdColumn
    unique_name: str | None

    def column_dependencies_internal(self) -> Iterable[Column]:
        return (self.value,) + self.instance

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.orig_id_column]

    def input_universe(self) -> Universe:
        return self.orig_id_column.universe

    def id_column_type(self) -> dt.DType:
        return self.orig_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.orig_id_column.universe.subset()


@dataclass(eq=False, frozen=True)
class FilterContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table.filter() operation."""

    filtering_column: ColumnWithExpression
    id_column_to_filter: IdColumn

    def column_dependencies_internal(self) -> Iterable[Column]:
        return [self.filtering_column]

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.id_column_to_filter]

    def input_universe(self) -> Universe:
        return self.id_column_to_filter.universe

    def id_column_type(self) -> dt.DType:
        return self.id_column_to_filter.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.id_column_to_filter.universe.subset()


@dataclass(eq=False, frozen=True)
class TimeColumnContext(Context):
    """Context of operations that use time columns."""

    orig_id_column: IdColumn
    threshold_column: ColumnWithExpression
    time_column: ColumnWithExpression
    instance_column: ColumnWithExpression

    def column_dependencies_internal(self) -> Iterable[Column]:
        return [self.threshold_column, self.time_column, self.instance_column]

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.orig_id_column]

    def input_universe(self) -> Universe:
        return self.orig_id_column.universe

    def id_column_type(self) -> dt.DType:
        return self.orig_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.orig_id_column.universe.subset()


@dataclass(eq=False, frozen=True)
class ForgetContext(TimeColumnContext):
    """Context of `table._forget() operation."""

    mark_forgetting_records: bool


@dataclass(eq=False, frozen=True)
class ForgetImmediatelyContext(Context):
    """Context of `table._forget_immediately operation."""

    orig_id_column: IdColumn

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.orig_id_column]

    def input_universe(self) -> Universe:
        return self.orig_id_column.universe

    def id_column_type(self) -> dt.DType:
        return self.orig_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.orig_id_column.universe.subset()


@dataclass(eq=False, frozen=True)
class FilterOutForgettingContext(Context):
    """Context of `table._filter_out_results_of_forgetting() operation."""

    orig_id_column: IdColumn

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.orig_id_column]

    def input_universe(self) -> Universe:
        return self.orig_id_column.universe

    def id_column_type(self) -> dt.DType:
        return self.orig_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.orig_id_column.universe.superset()


@dataclass(eq=False, frozen=True)
class FreezeContext(
    TimeColumnContext, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table._freeze() operation."""


@dataclass(eq=False, frozen=True)
class BufferContext(
    TimeColumnContext, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table._buffer() operation."""


@dataclass(eq=False, frozen=True)
class ReindexContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table.with_id() operation."""

    reindex_column: ColumnWithExpression

    def column_dependencies_internal(self) -> Iterable[Column]:
        return [self.reindex_column]

    def input_universe(self) -> Universe:
        return self.reindex_column.universe

    def id_column_type(self) -> dt.DType:
        return self.reindex_column.dtype

    @cached_property
    def universe(self) -> Universe:
        ret = Universe()
        if self.input_universe().is_empty():
            ret.register_as_empty(no_warn=False)
        return ret


@dataclass(eq=False, frozen=True)
class IxContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table.ix() operation."""

    key_column: Column
    key_id_column: IdColumn
    orig_id_column: IdColumn
    optional: bool

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.orig_id_column, self.key_column]

    def id_column_type(self) -> dt.DType:
        return self.key_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.key_column.universe


@dataclass(eq=False, frozen=True)
class IntersectContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table.intersect() operation."""

    intersecting_ids: tuple[IdColumn, ...]

    def __post_init__(self):
        assert len(self.intersecting_ids) > 0

    def input_universe(self) -> Universe:
        return self.intersecting_ids[0].universe

    def column_dependencies_external(self) -> Iterable[Column]:
        return self.intersecting_ids

    def id_column_type(self) -> dt.DType:
        return self.intersecting_ids[0].dtype

    def universe_dependencies(self) -> Iterable[Universe]:
        return [c.universe for c in self.intersecting_ids]

    @cached_property
    def universe(self) -> Universe:
        return G.universe_solver.get_intersection(
            *[c.universe for c in self.intersecting_ids]
        )


@dataclass(eq=False, frozen=True)
class RestrictContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table.restrict() operation."""

    orig_id_column: IdColumn
    new_id_column: IdColumn

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.orig_id_column, self.new_id_column]

    def id_column_type(self) -> dt.DType:
        return self.orig_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.new_id_column.universe


@dataclass(eq=False, frozen=True)
class DifferenceContext(Context):
    """Context of `table.difference() operation."""

    left: IdColumn
    right: IdColumn

    def input_universe(self) -> Universe:
        return self.left.universe

    def id_column_type(self) -> dt.DType:
        return self.left.dtype

    @cached_property
    def universe(self) -> Universe:
        return G.universe_solver.get_difference(self.left.universe, self.right.universe)


@dataclass(eq=False, frozen=True)
class HavingContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    orig_id_column: IdColumn
    key_column: Column
    key_id_column: IdColumn

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.key_column, self.orig_id_column]

    def input_universe(self) -> Universe:
        return self.orig_id_column.universe

    def id_column_type(self) -> dt.DType:
        return self.key_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.key_column.universe.subset()


@dataclass(eq=False, frozen=True)
class UpdateRowsContext(
    Context, column_properties_evaluator=cp.UpdateRowsPropsEvaluator
):
    """Context of `table.update_rows()` and related operations."""

    updates: dict[str, Column]
    union_ids: tuple[IdColumn, ...]

    def __post_init__(self):
        assert len(self.union_ids) > 0
        assert all(arg.dtype == self.union_ids[0].dtype for arg in self.union_ids)

    def reference_column_dependencies(self, ref: ColumnReference) -> StableSet[Column]:
        return StableSet([self.updates[ref.name]])

    def input_universe(self) -> Universe:
        return self.union_ids[0].universe

    def id_column_type(self) -> dt.DType:
        return self.union_ids[0].dtype

    def universe_dependencies(self) -> Iterable[Universe]:
        return [c.universe for c in self.union_ids]

    @cached_property
    def universe(self) -> Universe:
        return G.universe_solver.get_union(*[c.universe for c in self.union_ids])


@dataclass(eq=False, frozen=True)
class UpdateCellsContext(
    Context, column_properties_evaluator=cp.UpdateCellsPropsEvaluator
):
    left: IdColumn
    right: IdColumn
    updates: dict[str, Column]

    def reference_column_dependencies(self, ref: ColumnReference) -> StableSet[Column]:
        if ref.name in self.updates:
            return StableSet([self.updates[ref.name]])
        return StableSet()

    @property
    def universe(self) -> Universe:
        return self.left.universe

    def id_column_type(self) -> dt.DType:
        return self.left.dtype


@dataclass(eq=False, frozen=True)
class ConcatUnsafeContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table.concat_unsafe()`."""

    updates: tuple[dict[str, Column], ...]
    union_ids: tuple[IdColumn, ...]

    def __post_init__(self):
        assert len(self.union_ids) > 0
        assert all(arg.dtype == self.union_ids[0].dtype for arg in self.union_ids)

    def column_dependencies_external(self) -> Iterable[Column]:
        return self.union_ids

    def reference_column_dependencies(self, ref: ColumnReference) -> StableSet[Column]:
        return StableSet([update[ref.name] for update in self.updates])

    def input_universe(self) -> Universe:
        return self.union_ids[0].universe

    def id_column_type(self) -> dt.DType:
        return self.union_ids[0].dtype

    def universe_dependencies(self) -> Iterable[Universe]:
        return [c.universe for c in self.union_ids]

    @cached_property
    def universe(self) -> Universe:
        return G.universe_solver.get_union(*[c.universe for c in self.union_ids])


@dataclass(eq=False, frozen=True)
class PromiseSameUniverseContext(
    Context, column_properties_evaluator=cp.PromiseSameUniversePropsEvaluator
):
    """Context of table.unsafe_promise_same_universe_as() operation."""

    orig_id_column: IdColumn
    _id_column: IdColumn

    @cached_property
    def universe(self) -> Universe:
        return self._id_column.universe

    def id_column_type(self) -> dt.DType:
        return self._id_column.dtype


@dataclass(eq=False, frozen=True)
class PromiseSameUniverseAsOfNowContext(
    PromiseSameUniverseContext,
    column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator,
):
    """Context that does to the data stream the underlying data stream the same thing as combined
    ``_forget_immediately()``, ``with_universe_of()``, ``_filter_out_results_of_forgetting()``.
    Applying them explicitly would destroy the effect of ``with_universe_of`` as both
    ``_forget_immediately`` and `_filter_out_results_of_forgetting`` change table universe.
    """

    pass


@dataclass(eq=True, frozen=True)
class JoinContext(Context, column_properties_evaluator=cp.JoinPropsEvaluator):
    """Context for building inner table of a join, where all columns from left and right
    are properly unrolled. Uses JoinTypeInterpreter to properly evaluate which columns
    should be optionalized."""

    _universe: Universe
    left_table: pw.Table
    right_table: pw.Table
    on_left: ContextTable
    on_right: ContextTable
    last_column_is_instance: bool
    assign_id: bool
    left_ear: bool
    right_ear: bool
    exact_match: bool

    def column_dependencies_external(self) -> Iterable[Column]:
        return (self.left_table._id_column, self.right_table._id_column)

    def column_dependencies_internal(self) -> Iterable[Column]:
        return chain(self.on_left.columns, self.on_right.columns)

    def _get_type_interpreter(self):
        from pathway.internals.type_interpreter import JoinTypeInterpreter

        return JoinTypeInterpreter(
            self.left_table,
            self.right_table,
            self.right_ear and (not self.exact_match),
            self.left_ear and (not self.exact_match),
        )

    def intermediate_tables(self) -> Iterable[Table]:
        return [
            _create_internal_table(
                self.on_left.columns,
                self.left_table._table_restricted_context,
            ),
            _create_internal_table(
                self.on_right.columns,
                self.right_table._table_restricted_context,
            ),
        ]

    @cached_property
    def universe(self) -> Universe:
        return self._universe

    def id_column_type(self) -> dt.DType:
        if self.assign_id:
            return self.left_table._id_column.dtype
        else:
            return dt.ANY_POINTER  # Pointer(Pointer,Pointer), but that might change


@dataclass(eq=False, frozen=True)
class JoinRowwiseContext(
    RowwiseContext, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context for actually evaluating join expressions."""

    temporary_column_to_original: dict[InternalColRef, InternalColRef]
    original_column_to_temporary: dict[InternalColRef, ColumnReference]

    @staticmethod
    def from_mapping(
        id_column: IdColumn,
        columns_mapping: dict[InternalColRef, ColumnReference],
    ) -> JoinRowwiseContext:
        temporary_column_to_original = {
            expression._to_internal(): orig_colref
            for orig_colref, expression in columns_mapping.items()
        }
        return JoinRowwiseContext(
            id_column, temporary_column_to_original, columns_mapping.copy()
        )

    def _get_type_interpreter(self):
        from pathway.internals.type_interpreter import JoinRowwiseTypeInterpreter

        return JoinRowwiseTypeInterpreter(
            self.temporary_column_to_original, self.original_column_to_temporary
        )


@dataclass(eq=False, frozen=True)
class FlattenContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `table.flatten() operation."""

    orig_universe: Universe
    flatten_column: Column

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.flatten_column]

    def _get_flatten_column_dtype(self):
        dtype = self.flatten_column.dtype
        if isinstance(dtype, dt.List):
            return dtype.wrapped
        if isinstance(dtype, dt.Tuple):
            if dtype in (dt.ANY_TUPLE, dt.Tuple()):
                return dt.ANY
            assert not isinstance(dtype.args, EllipsisType)
            return_dtype = dtype.args[0]
            for single_dtype in dtype.args[1:]:
                return_dtype = dt.types_lca(return_dtype, single_dtype, raising=False)
            return return_dtype
        elif dtype == dt.STR:
            return dt.STR
        elif dtype == dt.ANY:
            return dt.ANY
        elif isinstance(dtype, dt.Array):
            return dtype.strip_dimension()
        elif dtype == dt.JSON:
            return dt.JSON
        else:
            raise TypeError(f"Cannot flatten column of type {dtype}.")

    @cached_property
    def universe(self) -> Universe:
        ret = Universe()
        if self.orig_universe.is_empty():
            ret.register_as_empty(no_warn=False)
        return ret

    @cached_property
    def flatten_result_column(self) -> Column:
        return MaterializedColumn(
            self.universe,
            cp.ColumnProperties(
                dtype=self._get_flatten_column_dtype(),
                append_only=self.flatten_column.properties.append_only,
            ),
        )

    def id_column_type(self) -> dt.DType:
        return dt.ANY_POINTER  # Pointer(Pointer,Int), but this might change


@dataclass(eq=False, frozen=True)
class SortingContext(Context):
    """Context of table.sort() operation."""

    key_column: ColumnWithExpression
    instance_column: ColumnWithExpression
    original_id_column_dtype: dt.DType

    def column_dependencies_internal(self) -> Iterable[Column]:
        return [self.key_column, self.instance_column]

    @cached_property
    def universe(self) -> Universe:
        return self.key_column.universe

    @cached_property
    def prev_column(self) -> Column:
        return MaterializedColumn(
            self.universe,
            cp.ColumnProperties(dtype=dt.Optional(self.original_id_column_dtype)),
        )

    @cached_property
    def next_column(self) -> Column:
        return MaterializedColumn(
            self.universe,
            cp.ColumnProperties(dtype=dt.Optional(self.original_id_column_dtype)),
        )

    def id_column_type(self) -> dt.DType:
        return self.original_id_column_dtype


@dataclass(eq=False, frozen=True)
class FilterOutValueContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of operations that filter all columns of the table.

    Used in `table.remove_errors()` and ``table.await_futures()`
    """

    orig_id_column: IdColumn
    value_to_filter_out: api.Value

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.orig_id_column]

    def input_universe(self) -> Universe:
        return self.orig_id_column.universe

    @cached_property
    def universe(self) -> Universe:
        return self.orig_id_column.universe.subset()

    def id_column_type(self) -> dt.DType:
        return self.orig_id_column.dtype


@dataclass(eq=False, frozen=True)
class RemoveRetractionsContext(
    Context, column_properties_evaluator=cp.AppendOnlyPropsEvaluator
):
    """Context of `table._remove_retractions() operation."""

    id_column_to_filter: IdColumn

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.id_column_to_filter]

    def input_universe(self) -> Universe:
        return self.id_column_to_filter.universe

    def id_column_type(self) -> dt.DType:
        return self.id_column_to_filter.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.id_column_to_filter.universe.superset()


@dataclass(eq=False, frozen=True)
class AsyncTransformerContext(
    Context, column_properties_evaluator=cp.PreserveDependenciesPropsEvaluator
):
    """Context of `AsyncTransformer` operation."""

    input_id_column: IdColumn
    input_columns: list[Column]
    schema: type[Schema]
    on_change: Callable
    on_time_end: Callable
    on_end: Callable
    datasource: GenericDataSource

    def column_dependencies_external(self) -> Iterable[Column]:
        return [self.input_id_column] + self.input_columns

    def input_universe(self) -> Universe:
        return self.input_id_column.universe

    def id_column_type(self) -> dt.DType:
        return self.input_id_column.dtype

    @cached_property
    def universe(self) -> Universe:
        return self.input_id_column.universe.subset()

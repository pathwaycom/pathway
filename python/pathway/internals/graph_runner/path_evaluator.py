# Copyright © 2024 Pathway

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import ClassVar

import pathway.internals.column as clmn
import pathway.internals.expression as expr
import pathway.internals.operator as op
from pathway.internals.column_path import ColumnPath
from pathway.internals.graph_runner.path_storage import Storage
from pathway.internals.universe import Universe


def compute_paths(
    output_columns: Iterable[clmn.Column],
    input_storages: dict[Universe, Storage],
    operator: op.Operator,
    context: clmn.Context,
):
    evaluator: PathEvaluator
    match operator:
        case op.InputOperator():
            evaluator = FlatStoragePathEvaluator(context)
        case op.RowTransformerOperator():
            evaluator = FlatStoragePathEvaluator(context)
        case op.ContextualizedIntermediateOperator():
            evaluator = PathEvaluator.for_context(context)(context)
        case _:
            raise ValueError(
                f"Operator {operator} in update_storage() but it shouldn't produce tables."
            )
    return evaluator.compute(output_columns, input_storages)


class PathEvaluator(ABC):
    context: clmn.Context

    def __init__(self, context: clmn.Context) -> None:
        super().__init__()
        self.context = context

    @abstractmethod
    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage: ...

    _context_mapping: ClassVar[dict[type[clmn.Context], type[PathEvaluator]]] = {}

    def __init_subclass__(cls, /, context_types=[], **kwargs):
        super().__init_subclass__(**kwargs)
        for context_type in context_types:
            cls._context_mapping[context_type] = cls

    @classmethod
    def for_context(cls, context: clmn.Context) -> type[PathEvaluator]:
        return cls._context_mapping[type(context)]


class FlatStoragePathEvaluator(
    PathEvaluator,
    context_types=[clmn.GroupedContext, clmn.RemoveErrorsContext],
):
    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        return Storage.flat(self.context.universe, output_columns)


class DeduplicatePathEvaluator(
    PathEvaluator,
    context_types=[clmn.DeduplicateContext],
):
    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        return Storage.flat(self.context.universe, output_columns, shift=1)


# WARNING: new columns are assigned column paths in the order of arrival in input_storage.
# ExpressionEvaluator for a given context has to preserve this order.
class AddNewColumnsPathEvaluator(
    PathEvaluator,
    context_types=[
        clmn.TableRestrictedRowwiseContext,
        clmn.RowwiseContext,
        clmn.JoinRowwiseContext,
        clmn.GradualBroadcastContext,
        clmn.ExternalIndexAsOfNowContext,
    ],
):
    def compute_if_all_new_are_references(
        self,
        output_columns: Iterable[clmn.Column],
        input_storage: Storage,
    ) -> Storage | None:
        paths = {}
        for column in output_columns:
            if input_storage.has_column(column):
                paths[column] = input_storage.get_path(column)
            elif (
                isinstance(column, clmn.ColumnWithReference)
                and input_storage.has_column(column.expression._column)
                and input_storage.get_path(column.expression._column) != ColumnPath.KEY
            ):
                paths[column] = input_storage.get_path(column.expression._column)
            else:
                return None
        return Storage(self.context.universe, paths, has_only_references=True)

    def compute_if_old_are_not_required(
        self,
        output_columns: Iterable[clmn.Column],
        input_storage: Storage,
    ) -> Storage | None:
        paths = {}
        for i, column in enumerate(output_columns):
            if input_storage.has_column(column):
                return None
            else:
                paths[column] = ColumnPath((i,))
        return Storage(self.context.universe, paths, has_only_new_columns=True)

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        input_storage = input_storages.get(self.context.universe)
        if input_storage is not None and isinstance(self.context, clmn.RowwiseContext):
            maybe_storage = self.compute_if_all_new_are_references(
                output_columns, input_storage
            )
            if maybe_storage is None:
                maybe_storage = self.compute_if_old_are_not_required(
                    output_columns, input_storage
                )
            if maybe_storage is not None:
                return maybe_storage
        paths = {}
        counter = itertools.count(start=1)
        for column in output_columns:
            if input_storage is not None and input_storage.has_column(column):
                paths[column] = (0,) + input_storage.get_path(column)
            else:
                paths[column] = ColumnPath((next(counter),))
        return Storage(self.context.universe, paths)


class SortingPathEvaluator(PathEvaluator, context_types=[clmn.SortingContext]):
    context: clmn.SortingContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        input_storage = input_storages[self.context.universe]
        paths = {}
        for column in output_columns:
            if column == self.context.prev_column:
                paths[column] = ColumnPath((1,))
            elif column == self.context.next_column:
                paths[column] = ColumnPath((2,))
            else:
                paths[column] = (0,) + input_storage.get_path(column)
        return Storage(self.context.universe, paths)


class NoNewColumnsMultipleSourcesPathEvaluator(
    PathEvaluator,
    context_types=[clmn.UpdateRowsContext, clmn.ConcatUnsafeContext],
):
    context: clmn.ConcatUnsafeContext | clmn.UpdateRowsContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        context = self.context
        output_columns_list = list(output_columns)
        source_universe = context.union_ids[0].universe
        # ensure that keeping structure is possible,
        # i.e. all sources have the same path to required columns
        keep_structure = True
        for column in output_columns_list:
            assert isinstance(column, clmn.ColumnWithReference)
            source_column = column.expression._column
            path = input_storages[source_column.universe].get_path(source_column)
            for dep in column.reference_column_dependencies():
                if input_storages[dep.universe].get_path(dep) != path:
                    keep_structure = False
                    break
            if not keep_structure:
                break

        if isinstance(context, clmn.ConcatUnsafeContext):
            updates = context.updates
        else:
            updates = (context.updates,)

        if not keep_structure:
            names = []
            source_columns: list[list[clmn.Column]] = [[]]
            for column in output_columns_list:
                assert isinstance(column, clmn.ColumnWithExpression)
                assert isinstance(column.expression, expr.ColumnReference)
                names.append(column.expression.name)
                source_columns[0].append(column.dereference())
            for columns in updates:
                source_columns.append([columns[name] for name in names])

            flattened_inputs = []
            assert len(list(self.context.universe_dependencies())) == len(
                source_columns
            )
            for universe, cols in zip(
                self.context.universe_dependencies(), source_columns
            ):
                flattened_storage = Storage.flat(universe, cols)
                flattened_inputs.append(flattened_storage)
        else:
            flattened_inputs = None

        evaluator: PathEvaluator
        if keep_structure:
            evaluator = NoNewColumnsPathEvaluator(context)
        else:
            evaluator = FlatStoragePathEvaluator(context)

        storage = evaluator.compute(
            output_columns_list,
            {source_universe: input_storages[source_universe]},
        )

        return storage.with_flattened_inputs(flattened_inputs)


NoNewColumnsContext = (
    clmn.FilterContext
    | clmn.ReindexContext
    | clmn.IntersectContext
    | clmn.DifferenceContext
    | clmn.HavingContext
    | clmn.ForgetContext
    | clmn.ForgetImmediatelyContext
    | clmn.FilterOutForgettingContext
    | clmn.FreezeContext
    | clmn.BufferContext
    | clmn.ConcatUnsafeContext
    | clmn.UpdateRowsContext
    | clmn.SetSchemaContext
    | clmn.RemoveRetractionsContext
)


class NoNewColumnsPathEvaluator(
    PathEvaluator,
    context_types=[
        clmn.FilterContext,
        clmn.ReindexContext,
        clmn.IntersectContext,
        clmn.DifferenceContext,
        clmn.HavingContext,
        clmn.ForgetContext,
        clmn.ForgetImmediatelyContext,
        clmn.FilterOutForgettingContext,
        clmn.FreezeContext,
        clmn.BufferContext,
        clmn.SetSchemaContext,
        clmn.RemoveRetractionsContext,
    ],
):
    context: NoNewColumnsContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        input_storage = input_storages[self.context.input_universe()]
        paths: dict[clmn.Column, ColumnPath] = {}
        for column in output_columns:
            if (
                isinstance(column, clmn.ColumnWithReference)
                and column.context == self.context
            ):
                source_column = column.expression._column
                paths[column] = input_storage.get_path(source_column)
            else:  # column from the same universe, but not the current table
                paths[column] = input_storage.get_path(column)
        return Storage(self.context.universe, paths)


class UpdateCellsPathEvaluator(PathEvaluator, context_types=[clmn.UpdateCellsContext]):
    context: clmn.UpdateCellsContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        input_storage = input_storages[self.context.universe]
        counter = itertools.count(start=1)
        paths = {}
        for column in output_columns:
            if column in input_storage.get_columns():
                paths[column] = (0,) + input_storage.get_path(column)
            elif (
                isinstance(column, clmn.ColumnWithReference)
                and column.expression.name not in self.context.updates
            ):
                source_column = column.expression._column
                paths[column] = (0,) + input_storage.get_path(source_column)
            else:
                paths[column] = ColumnPath((next(counter),))
        return Storage(self.context.universe, paths)


class JoinPathEvaluator(PathEvaluator, context_types=[clmn.JoinContext]):
    context: clmn.JoinContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        if self.context.assign_id:
            left_universe = self.context.left_table._universe
            input_storage = input_storages[left_universe]
            return AddNewColumnsPathEvaluator(self.context).compute(
                output_columns, {left_universe: input_storage}
            )
        else:
            return FlatStoragePathEvaluator(self.context).compute(
                output_columns, input_storages
            )


class FlattenPathEvaluator(PathEvaluator, context_types=[clmn.FlattenContext]):
    context: clmn.FlattenContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        input_storage = input_storages[self.context.orig_universe]
        paths = {}
        for column in output_columns:
            if column == self.context.flatten_result_column:
                paths[column] = ColumnPath((1,))
            else:
                assert isinstance(column, clmn.ColumnWithReference)
                original_column = column.expression._column
                paths[column] = (0,) + input_storage.get_path(original_column)
        return Storage(self.context.universe, paths)


class PromiseSameUniversePathEvaluator(
    PathEvaluator,
    context_types=[
        clmn.PromiseSameUniverseContext,
        clmn.RestrictContext,
        clmn.IxContext,
    ],
):
    context: clmn.PromiseSameUniverseContext | clmn.RestrictContext | clmn.IxContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> Storage:
        paths: dict[clmn.Column, ColumnPath] = {}
        orig_storage = input_storages[self.context.orig_id_column.universe]
        new_storage = input_storages[self.context.universe]
        for column in output_columns:
            if (
                isinstance(column, clmn.ColumnWithReference)
                and column.context == self.context
            ):
                paths[column] = (1,) + orig_storage.get_path(column.expression._column)
            else:
                paths[column] = (0,) + new_storage.get_path(column)
        return Storage(self.context.universe, paths)

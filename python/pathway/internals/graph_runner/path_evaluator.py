# Copyright © 2024 Pathway

from __future__ import annotations

import itertools
import math
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import ClassVar

import pathway.internals.column as clmn
import pathway.internals.expression as expr
import pathway.internals.operator as op
from pathway.internals.column_path import ColumnPath
from pathway.internals.graph_runner.path_storage import Storage
from pathway.internals.helpers import StableSet
from pathway.internals.universe import Universe


def compute_paths(
    output_columns: Iterable[clmn.Column],
    input_storages: dict[Universe, Storage],
    operator: op.Operator,
    context: clmn.Context,
    table_columns: Iterable[clmn.Column],
):
    evaluator: PathEvaluator
    match operator:
        case op.InputOperator():
            evaluator = FlatOrderedStoragePathEvaluator(context)
        case op.RowTransformerOperator():
            evaluator = FlatStoragePathEvaluator(context)
        case op.ContextualizedIntermediateOperator():
            evaluator = PathEvaluator.for_context(context)(context)
        case _:
            raise ValueError(
                f"Operator {operator} in update_storage() but it shouldn't produce tables."
            )
    output_columns = list(output_columns)
    return evaluator.compute(output_columns, input_storages, table_columns).restrict_to(
        output_columns, require_all=True
    )


def maybe_flatten_input_storage(
    storage: Storage, columns: Iterable[clmn.Column]
) -> Storage:
    columns = StableSet(columns)
    paths = set()
    for column in columns:
        paths.add(storage.get_path(column))

    removable_size = 0.0
    for path, column in storage.get_all_columns():
        # Using get_all_columns (instead of get_columns) is needed to keep track of columns
        # that are not in use but still present in the tuple. Only if we are aware of all
        # fields in a tuple, we can make a conscious decision whether we want to flatten
        # a tuple (and remove columns) or not.
        if path not in paths:
            removable_size += column.dtype.max_size()
    if math.isinf(removable_size) or removable_size > len(columns):
        # if we can remove potentially large column or removable_size is greater than
        # the number of columns we keep (equal to the flattening cost), then flatten the storage
        return Storage.flat(storage._universe, columns)
    else:
        return storage


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
        table_columns: Iterable[clmn.Column],
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
    context_types=[
        clmn.GroupedContext,
        clmn.FilterOutValueContext,
    ],
):
    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
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
        table_columns: Iterable[clmn.Column],
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
        output_columns: list[clmn.Column],
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
        return input_storage.with_updated_paths(paths).with_only_references()

    def compute_if_old_are_not_required(
        self,
        output_columns: list[clmn.Column],
        input_storage: Storage,
    ) -> Storage | None:
        for column in output_columns:
            if input_storage.has_column(column):
                return None
        return Storage.flat(
            self.context.universe, output_columns
        ).with_only_new_columns()

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        input_storage = input_storages[self.context.universe]
        output_columns = list(output_columns)
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
        paths: dict[clmn.Column, ColumnPath] = {}
        counter = itertools.count(start=1)
        for column in output_columns:
            if not input_storage.has_column(column):
                paths[column] = ColumnPath((next(counter),))
        return input_storage.with_prefix((0,)).with_updated_paths(
            paths, universe=self.context.universe
        )


class SortingPathEvaluator(PathEvaluator, context_types=[clmn.SortingContext]):
    context: clmn.SortingContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        input_storage = input_storages[self.context.universe]
        return Storage.merge_storages(
            self.context.universe,
            input_storage,
            Storage.one_column_storage(self.context.prev_column),
            Storage.one_column_storage(self.context.next_column),
        )


class NoNewColumnsMultipleSourcesPathEvaluator(
    PathEvaluator,
    context_types=[clmn.UpdateRowsContext, clmn.ConcatUnsafeContext],
):
    context: clmn.ConcatUnsafeContext | clmn.UpdateRowsContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
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

        names = []
        source_columns: list[list[clmn.Column]] = [[]]
        for column in output_columns_list:
            assert isinstance(column, clmn.ColumnWithExpression)
            assert isinstance(column.expression, expr.ColumnReference)
            names.append(column.expression.name)
            source_columns[0].append(column.dereference())
        for columns in updates:
            source_columns.append([columns[name] for name in names])

        if keep_structure and isinstance(context, clmn.UpdateRowsContext):
            for universe, cols in zip(
                self.context.universe_dependencies(), source_columns, strict=True
            ):
                input_storage = input_storages[universe]
                maybe_flat_storage = maybe_flatten_input_storage(input_storage, cols)
                if maybe_flat_storage is not input_storage:
                    keep_structure = False
                    break

        flattened_inputs = {}
        for i, (universe, cols) in enumerate(
            zip(self.context.universe_dependencies(), source_columns, strict=True)
        ):
            if keep_structure:
                flattened_storage = input_storages[universe]
            else:
                flattened_storage = Storage.flat(universe, cols)

            flattened_inputs[f"{i}"] = flattened_storage

        evaluator: PathEvaluator
        if keep_structure:
            evaluator = NoNewColumnsPathEvaluator(context)
        else:
            evaluator = FlatStoragePathEvaluator(context)

        storage = evaluator.compute(
            output_columns_list,
            {source_universe: input_storages[source_universe]},
            table_columns,
        )

        return storage.with_maybe_flattened_inputs(flattened_inputs)


NoNewColumnsContext = (
    clmn.FilterContext
    | clmn.ReindexContext
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
        table_columns: Iterable[clmn.Column],
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
        return input_storage.with_updated_paths(paths, universe=self.context.universe)


class NoNewColumnsWithDataStoredPathEvaluator(
    PathEvaluator,
    context_types=[
        clmn.IntersectContext,
        clmn.DifferenceContext,
        clmn.HavingContext,
    ],
):
    context: clmn.IntersectContext | clmn.DifferenceContext | clmn.HavingContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        input_storage = input_storages[self.context.input_universe()]
        required_columns: StableSet[clmn.Column] = StableSet()
        for column in output_columns:
            if (
                isinstance(column, clmn.ColumnWithReference)
                and column.context == self.context
            ):
                required_columns.add(column.expression._column)
            else:  # column from the same universe, but not the current table
                required_columns.add(column)
        input_storage = maybe_flatten_input_storage(input_storage, required_columns)
        paths: dict[clmn.Column, ColumnPath] = {}
        for column in output_columns:
            if (
                isinstance(column, clmn.ColumnWithReference)
                and column.context == self.context
            ):
                source_column = column.expression._column
                paths[column] = input_storage.get_path(source_column)
        return input_storage.with_updated_paths(
            paths, universe=self.context.universe
        ).with_maybe_flattened_inputs({"input_storage": input_storage})


class UpdateCellsPathEvaluator(PathEvaluator, context_types=[clmn.UpdateCellsContext]):
    context: clmn.UpdateCellsContext

    def maybe_flatten_input_storages(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> tuple[Storage, Storage]:
        input_storage = input_storages[self.context.universe]
        left_columns: StableSet[clmn.Column] = StableSet()
        right_columns: StableSet[clmn.Column] = StableSet()
        for column in output_columns:
            if input_storage.has_column(column):
                left_columns.add(column)
            else:
                assert isinstance(column, clmn.ColumnWithReference)
                left_columns.add(column.expression._column)
                if column.expression.name in self.context.updates:
                    right_columns.add(self.context.updates[column.expression.name])
        left_storage = maybe_flatten_input_storage(input_storage, left_columns)
        right_storage = maybe_flatten_input_storage(
            input_storages[self.context.right.universe], right_columns
        )

        return (left_storage, right_storage)

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        left_storage, right_storage = self.maybe_flatten_input_storages(
            output_columns, input_storages
        )
        prefixed_left_storage = left_storage.with_prefix((0,))
        counter = itertools.count(start=1)
        paths: dict[clmn.Column, ColumnPath] = {}
        for column in output_columns:
            if (
                isinstance(column, clmn.ColumnWithReference)
                and column.context == self.context
            ):
                if column.expression.name in self.context.updates:
                    paths[column] = ColumnPath((next(counter),))
                else:
                    source_column = column.expression._column
                    paths[column] = prefixed_left_storage.get_path(source_column)
        return prefixed_left_storage.with_updated_paths(
            paths
        ).with_maybe_flattened_inputs(
            {"left_storage": left_storage, "right_storage": right_storage}
        )


class JoinPathEvaluator(PathEvaluator, context_types=[clmn.JoinContext]):
    context: clmn.JoinContext

    def maybe_flatten_input_storages(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
    ) -> tuple[Storage, Storage]:
        exclusive_right_columns = list(
            itertools.chain(
                self.context.right_table._columns.values(),
                self.context.on_right.columns,
            )
        )
        left_input_storage = input_storages[self.context.left_table._universe].remove(
            exclusive_right_columns
        )
        right_input_storage = input_storages[
            self.context.right_table._universe
        ].restrict_to(exclusive_right_columns)

        required_input_columns: list[clmn.Column] = []
        for column in itertools.chain(
            output_columns, self.context.column_dependencies()
        ):
            if (
                isinstance(column, clmn.ColumnWithExpression)
                and column.context == self.context
            ):
                required_input_columns.extend(column.column_dependencies())
            else:
                required_input_columns.append(column)

        left_columns: StableSet[clmn.Column] = StableSet()
        right_columns: StableSet[clmn.Column] = StableSet()
        for column in required_input_columns:
            if left_input_storage.has_column(column):
                left_columns.add(column)
            else:
                assert right_input_storage.has_column(column)
                right_columns.add(column)
        left_input_storage = maybe_flatten_input_storage(
            input_storages[self.context.left_table._universe], left_columns
        )
        right_input_storage = maybe_flatten_input_storage(
            input_storages[self.context.right_table._universe], right_columns
        )
        return (left_input_storage, right_input_storage)

    def merge_storages(self, left_storage: Storage, right_storage: Storage) -> Storage:
        left_id_storage = Storage.one_column_storage(self.context.left_table._id_column)
        right_id_storage = Storage.one_column_storage(
            self.context.right_table._id_column
        )
        right_columns = list(self.context.right_table._columns.values())
        return Storage.merge_storages(
            self.context.universe,
            left_id_storage,
            left_storage.remove(right_columns),
            right_id_storage,
            right_storage.restrict_to(right_columns),
        )

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        output_columns = list(output_columns)
        left_input_storage, right_input_storage = self.maybe_flatten_input_storages(
            output_columns, input_storages
        )
        join_storage = self.merge_storages(left_input_storage, right_input_storage)
        if self.context.assign_id:
            output_storage = AddNewColumnsPathEvaluator(self.context).compute(
                output_columns,
                {self.context.universe: left_input_storage},
                table_columns,
            )
        else:
            output_storage = FlatStoragePathEvaluator(self.context).compute(
                output_columns,
                {},
                table_columns,
            )
        return output_storage.with_maybe_flattened_inputs(
            {
                "left_storage": left_input_storage,
                "right_storage": right_input_storage,
                "join_storage": join_storage,
            }
        )


class FlattenPathEvaluator(PathEvaluator, context_types=[clmn.FlattenContext]):
    context: clmn.FlattenContext

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        prefixed_input_storage = input_storages[self.context.orig_universe].with_prefix(
            (0,)
        )
        paths = {}
        for column in output_columns:
            if column == self.context.flatten_result_column:
                paths[column] = ColumnPath((1,))
            else:
                assert isinstance(column, clmn.ColumnWithReference)
                original_column = column.expression._column
                paths[column] = prefixed_input_storage.get_path(original_column)
        return prefixed_input_storage.with_updated_paths(
            paths, universe=self.context.universe
        )


class PromiseSameUniversePathEvaluator(
    PathEvaluator,
    context_types=[
        clmn.PromiseSameUniverseContext,
        clmn.PromiseSameUniverseAsOfNowContext,
        clmn.RestrictContext,
        clmn.IxContext,
    ],
):
    context: (
        clmn.PromiseSameUniverseContext
        | clmn.PromiseSameUniverseAsOfNowContext
        | clmn.RestrictContext
        | clmn.IxContext
    )

    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        orig_storage_columns: StableSet[clmn.Column] = StableSet()
        newly_created_columns: StableSet[clmn.ColumnWithReference] = StableSet()
        new_storage_columns: StableSet[clmn.Column] = StableSet()
        for column in output_columns:
            if (
                isinstance(column, clmn.ColumnWithReference)
                and column.context == self.context
            ):
                newly_created_columns.add(column)
                orig_storage_columns.add(column.expression._column)
            else:
                new_storage_columns.add(column)

        if isinstance(self.context, clmn.IxContext):
            new_storage_columns.add(self.context.key_column)

        orig_storage = input_storages[self.context.orig_id_column.universe]
        orig_storage = maybe_flatten_input_storage(orig_storage, orig_storage_columns)
        new_storage = input_storages[self.context.universe]
        new_storage = maybe_flatten_input_storage(new_storage, new_storage_columns)

        paths: dict[clmn.Column, ColumnPath] = {}
        for column in newly_created_columns:
            paths[column] = (1,) + orig_storage.get_path(column.expression._column)
        for column in new_storage_columns:
            paths[column] = (0,) + new_storage.get_path(column)
        return (
            Storage.merge_storages(self.context.universe, new_storage, orig_storage)
            .with_updated_paths(paths)
            .with_maybe_flattened_inputs(
                {"orig_storage": orig_storage, "new_storage": new_storage}
            )
        )


class FlatOrderedStoragePathEvaluator(
    PathEvaluator,
    context_types=[
        clmn.AsyncTransformerContext,
    ],
):
    def compute(
        self,
        output_columns: Iterable[clmn.Column],
        input_storages: dict[Universe, Storage],
        table_columns: Iterable[clmn.Column],
    ) -> Storage:
        return Storage.flat(self.context.universe, table_columns)

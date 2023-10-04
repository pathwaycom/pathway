# Copyright © 2023 Pathway

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import chain
from typing import TYPE_CHECKING

from pathway.internals import api
from pathway.internals.column import (
    Column,
    ExternalMaterializedColumn,
    IdColumn,
    MaterializedColumn,
)
from pathway.internals.column_path import ColumnPath
from pathway.internals.column_properties import ColumnProperties
from pathway.internals.graph_runner import path_evaluator
from pathway.internals.graph_runner.path_storage import Storage
from pathway.internals.graph_runner.scope_context import ScopeContext
from pathway.internals.graph_runner.state import ScopeState
from pathway.internals.helpers import StableSet
from pathway.internals.operator import (
    DebugOperator,
    InputOperator,
    IterateOperator,
    Operator,
    OutputOperator,
    RowTransformerOperator,
)
from pathway.internals.table import Table
from pathway.internals.universe import Universe

if TYPE_CHECKING:
    from pathway.internals.graph_runner import GraphRunner


@dataclass
class OperatorStorageGraph:
    scope_context: ScopeContext
    column_deps_changes: dict[Operator, dict[Table, StableSet[Column]]]
    column_deps_at_output: dict[Operator, dict[Table, StableSet[Column]]]
    column_deps_at_end: dict[Universe, StableSet[Column]]  # for inner graph in iterate
    initial_storages: dict[Universe, Storage]  # for inner graph in iterate
    output_storages: dict[Operator, dict[Table, Storage]]
    iterate_subgraphs: dict[Operator, OperatorStorageGraph]
    is_outer_graph: bool
    iterate_subgraphs_external_column_mapping: dict[
        Operator, dict[Column, Column]
    ] = field(default_factory=lambda: defaultdict(dict))
    final_storages: dict[Universe, Storage] | None = None

    def get_iterate_subgraph(self, operator: Operator) -> OperatorStorageGraph:
        return self.iterate_subgraphs[operator]

    @classmethod
    def from_scope_context(
        cls,
        scope_context: ScopeContext,
        graph_builder: GraphRunner,
        output_tables: Iterable[Table],
    ) -> OperatorStorageGraph:
        graph = cls._create_storage_graph(
            scope_context, graph_builder, output_tables=output_tables
        )
        graph._compute_relevant_columns()
        graph._compute_storage_paths()

        return graph

    @classmethod
    def _create_storage_graph(
        cls,
        scope_context: ScopeContext,
        graph_builder: GraphRunner,
        initial_deps: dict[Universe, StableSet[Column]] = {},
        output_tables: Iterable[Table] = [],
        is_outer_graph: bool = True,
    ) -> OperatorStorageGraph:
        deps: dict[Universe, StableSet[Column]] = initial_deps.copy()
        deps_changes: dict[Operator, dict[Table, StableSet[Column]]] = defaultdict(dict)
        column_deps_at_output: dict[
            Operator, dict[Table, StableSet[Column]]
        ] = defaultdict(dict)
        output_storages: dict[Operator, dict[Table, Storage]] = defaultdict(dict)
        initial_storages: dict[Universe, Storage] = {}
        iterate_subgraphs: dict[Operator, OperatorStorageGraph] = {}

        for operator in scope_context.nodes:
            for table in operator.intermediate_and_output_tables:
                if table._universe in deps:
                    deps_changes[operator][table] = deps[table._universe]
                deps[table._universe] = StableSet()
                column_deps_at_output[operator][table] = deps[table._universe]

            if isinstance(operator, IterateOperator):
                inner_tables = (
                    operator.iterated_copy
                    + operator.iterated_with_universe_copy
                    + operator.extra_copy
                )
                inner_deps: dict[Universe, StableSet[Column]] = {}
                for inner_table in inner_tables.values():
                    assert isinstance(inner_table, Table)
                    if inner_table._universe not in inner_deps:
                        inner_deps[inner_table._universe] = StableSet()
                iterate_context = scope_context.iterate_subscope(
                    operator, graph_builder
                )
                iterate_subgraphs[operator] = cls._create_storage_graph(
                    iterate_context,
                    graph_builder,
                    initial_deps=inner_deps,
                    is_outer_graph=False,
                )
        for table in output_tables:
            deps[table._universe].update(table._columns.values())

        graph = cls(
            scope_context=scope_context,
            column_deps_changes=deps_changes,
            column_deps_at_output=column_deps_at_output,
            column_deps_at_end=deps,
            initial_storages=initial_storages,
            output_storages=output_storages,
            iterate_subgraphs=iterate_subgraphs,
            is_outer_graph=is_outer_graph,
        )
        return graph

    def _compute_relevant_columns(
        self, input_universes: StableSet[Universe] = StableSet()
    ) -> None:
        global_column_deps = self.column_deps_at_end.copy()
        operators_reversed = reversed(list(self.scope_context.nodes))
        for operator in operators_reversed:
            if isinstance(operator, OutputOperator) or isinstance(
                operator, DebugOperator
            ):
                column_dependencies = self._compute_column_dependencies_output(operator)
            elif isinstance(operator, RowTransformerOperator):
                column_dependencies = self._compute_column_dependencies_row_transformer(
                    operator, self.scope_context
                )
            elif isinstance(operator, IterateOperator):
                column_dependencies = self._compute_column_dependencies_iterate(
                    operator, self.scope_context
                )
            else:
                column_dependencies = self._compute_column_dependencies_ordinary(
                    operator, self.scope_context
                )

            for table in reversed(list(operator.intermediate_and_output_tables)):
                if table in self.column_deps_changes[operator]:
                    previous_deps = self.column_deps_changes[operator][table]
                    global_column_deps[table._universe] = previous_deps
                else:
                    del global_column_deps[table._universe]

            for universe, columns in column_dependencies.items():
                if len(columns) > 0:
                    if universe in global_column_deps:
                        global_column_deps[universe].update(columns)
                    elif not self._can_skip_universe_with_cols(
                        universe, columns, input_universes
                    ):
                        raise RuntimeError(
                            f"Can't determine the source of columns {columns} from "
                            + f"universe: {universe} in operator: {operator}"
                        )

    def _can_skip_universe_with_cols(
        self,
        universe: Universe,
        columns: Iterable[Column],
        input_universes: StableSet[Universe],
    ) -> bool:
        all_materialized_or_id = all(
            isinstance(column, MaterializedColumn) or isinstance(column, IdColumn)
            for column in columns
        )
        # Inside pw.iterate columns from the outer graph are
        # MaterializedColumns and they are not created inside any
        # Operator in the inner graph. So they have to be skipped.
        return (
            not self.is_outer_graph
            and universe in input_universes
            and all_materialized_or_id
        )

    def _compute_column_dependencies_ordinary(
        self,
        operator: Operator,
        scope_context: ScopeContext,
    ) -> dict[Universe, StableSet[Column]]:
        column_dependencies: dict[Universe, StableSet[Column]] = defaultdict(StableSet)
        # reverse because we traverse the operators list backward
        # and want to process output tables before intermediate tables
        # because we want to propagate dependencies from output tables
        # to intermediate tables
        intermediate_and_output_tables_rev = reversed(
            list(operator.intermediate_and_output_tables)
        )

        for table in intermediate_and_output_tables_rev:
            # add dependencies of downstream tables
            column_dependencies[table._universe].update(
                self.column_deps_at_output[operator][table]
            )
            output_deps = self.column_deps_at_output[operator][table]
            # output columns (not skipped) have to be in the storage
            output_deps.update(
                column
                for column in table._columns.values()
                if not scope_context.skip_column(column)
            )
            # columns with a given universe must be in all intermediate storages
            # in this universe (from creation to last use)
            output_deps.update(column_dependencies.get(table._universe, []))

            # add column dependencies
            for column in chain(table._columns.values(), [table._id_column]):
                if not scope_context.skip_column(column):
                    for dependency in column.column_dependencies():
                        if not isinstance(dependency, IdColumn):
                            column_dependencies[dependency.universe].add(dependency)

            # remove current columns (they are created in this operator)
            column_dependencies[table._universe] -= StableSet(table._columns.values())

        return column_dependencies

    def _compute_column_dependencies_output(
        self,
        operator: OutputOperator | DebugOperator,
    ) -> dict[Universe, StableSet[Column]]:
        column_dependencies: dict[Universe, StableSet[Column]] = defaultdict(StableSet)
        # get columns needed in the output operators
        for table_ in operator.input_tables:
            column_dependencies[table_._universe].update(table_._columns.values())
        return column_dependencies

    def _compute_column_dependencies_row_transformer(
        self,
        operator: RowTransformerOperator,
        scope_context: ScopeContext,
    ) -> dict[Universe, StableSet[Column]]:
        column_dependencies = self._compute_column_dependencies_ordinary(
            operator, scope_context
        )
        # propagate input tables columns as depndencies (they are hard deps)
        for table_ in operator.input_tables:
            column_dependencies[table_._universe].update(table_._columns.values())

        return column_dependencies

    def _compute_column_dependencies_iterate(
        self,
        operator: IterateOperator,
        scope_context: ScopeContext,
    ) -> dict[Universe, StableSet[Column]]:
        column_dependencies = self._compute_column_dependencies_ordinary(
            operator, scope_context
        )
        # FIXME: remove it up when iterate inputs are not hard dependencies

        inner_graph = self.iterate_subgraphs[operator]
        all_columns: dict[Universe, StableSet[Column]] = defaultdict(StableSet)
        output_tables_columns: dict[Universe, StableSet[Column]] = defaultdict(
            StableSet
        )
        # propagate columns existing in iterate
        for name, outer_handle in operator._outputs.items():
            outer_table = outer_handle.value
            if name not in operator.result_iterated:
                continue
            inner_table = operator.result_iterated[name]
            assert isinstance(inner_table, Table)
            inner_deps = inner_graph.column_deps_at_end[inner_table._universe]
            for column_name, outer_column in outer_table._columns.items():
                output_tables_columns[outer_table._universe].add(outer_column)
                inner_column = inner_table._columns[column_name]
                inner_deps.update([inner_column])

            all_columns[outer_table._universe].update(
                self.column_deps_at_output[operator][outer_table]
            )

        columns_mapping = self.iterate_subgraphs_external_column_mapping[operator]
        # propagate columns not existing in iterate but created before iterate and
        # used after iterate and having the same universe as one of iterate outputs
        for universe, columns in all_columns.items():
            columns -= output_tables_columns[universe]
            inner_universe = operator._universe_mapping[universe]
            inner_deps = inner_graph.column_deps_at_end[inner_universe]
            for column in columns:
                inner_column = ExternalMaterializedColumn(
                    inner_universe, ColumnProperties(dtype=column.dtype)
                )
                inner_deps.update([inner_column])
                columns_mapping[column] = inner_column

        inner_tables = (
            operator.iterated_copy
            + operator.iterated_with_universe_copy
            + operator.extra_copy
        )
        # input_universes - so that we know inside which universes are available
        # on iterate input
        input_universes: StableSet[Universe] = StableSet()
        for table in inner_tables.values():
            assert isinstance(table, Table)
            input_universes.add(table._universe)

        inner_graph._compute_relevant_columns(input_universes)

        # propagate input tables columns as depndencies (they are hard deps)
        for table_ in operator.input_tables:
            column_dependencies[table_._universe].update(table_._columns.values())

        return column_dependencies

    def _compute_storage_paths(self):
        storages: dict[Universe, Storage] = self.initial_storages.copy()
        for operator in self.scope_context.nodes:
            if isinstance(operator, InputOperator):
                self._compute_storage_paths_input(operator, storages)
            elif isinstance(operator, IterateOperator):
                self._compute_storage_paths_iterate(operator, storages)
            else:
                self._compute_storage_paths_ordinary(operator, storages)
        self.final_storages = storages

    def _compute_storage_paths_ordinary(
        self, operator: Operator, storages: dict[Universe, Storage]
    ) -> None:
        for table in operator.intermediate_and_output_tables:
            output_columns = self.column_deps_at_output[operator][table]
            universes = table._id_column.context.universe_dependencies()
            input_storages = {universe: storages[universe] for universe in universes}
            path_storage = path_evaluator.compute_paths(
                output_columns,
                input_storages,
                operator,
                table._id_column.context,
            )
            if path_storage.max_depth > 3:
                # TODO: 3 is arbitrarily specified number. Check what's best.
                result_storage = Storage.flat(
                    table._universe, path_storage.get_columns()
                )
                path_storage = path_storage.with_flattened_output(result_storage)
            else:
                result_storage = path_storage
            self.output_storages[operator][table] = path_storage
            storages[table._universe] = result_storage

    def _compute_storage_paths_input(
        self, operator: Operator, storages: dict[Universe, Storage]
    ) -> None:
        for table in operator.output_tables:
            path_storage = path_evaluator.compute_paths(
                table._columns.values(),
                {},
                operator,
                table._id_column.context,
            )
            self.output_storages[operator][table] = path_storage
            storages[table._universe] = path_storage

    def _compute_storage_paths_iterate(
        self, operator: IterateOperator, storages: dict[Universe, Storage]
    ) -> None:
        # for iterate, the structure of input tables has to be the same as the structure
        # of corresponding output tables so that iterate can finish.
        # the only structure changes can be done before input or after output.
        inner_column_paths: dict[Universe, dict[Column, ColumnPath]] = defaultdict(dict)
        outer_tables = (
            operator.iterated + operator.iterated_with_universe + operator.extra
        )
        inner_tables = (
            operator.iterated_copy
            + operator.iterated_with_universe_copy
            + operator.extra_copy
        )
        # map paths of outer columns to paths of inner columns
        for name, outer_table in outer_tables.items():
            assert isinstance(outer_table, Table)
            inner_table = inner_tables[name]
            assert isinstance(inner_table, Table)
            storage = storages[outer_table._universe]
            for column_name, outer_column in outer_table._columns.items():
                inner_column = inner_table._columns[column_name]
                path = storage.get_path(outer_column)
                inner_column_paths[inner_table._universe][inner_column] = path

        # push paths for external columns that have to be propagated through iterate
        columns_mapping = self.iterate_subgraphs_external_column_mapping[operator]
        for outer_column, inner_column in columns_mapping.items():
            path = storages[outer_column.universe].get_path(outer_column)
            inner_column_paths[inner_column.universe][inner_column] = path

        for universe, paths in inner_column_paths.items():
            self.iterate_subgraphs[operator].initial_storages[universe] = Storage(
                universe, paths
            )

        self.iterate_subgraphs[operator]._compute_storage_paths()

        # propagate paths over iterate (not looking inside)
        for name, handle in operator._outputs.items():
            table = handle.value
            output_columns = self.column_deps_at_output[operator][table]
            input_table = operator.get_input(name).value
            assert isinstance(input_table, Table)
            path_storage = path_evaluator.iterate(
                output_columns,
                storages[input_table._universe],
                table,
                input_table,
            )
            self.output_storages[operator][table] = path_storage
            storages[table._universe] = path_storage

    def build_scope(
        self, scope: api.Scope, state: ScopeState, graph_runner: GraphRunner
    ) -> None:
        from pathway.internals.graph_runner.operator_handler import OperatorHandler

        for univ, storage in self.initial_storages.items():
            state.create_table(univ, storage)

        for operator in self.scope_context.nodes:
            handler_cls = OperatorHandler.for_operator(operator)
            handler = handler_cls(
                scope,
                state,
                self.scope_context,
                graph_runner,
                self,
                operator.id,
            )
            handler.run(operator, self.output_storages[operator])

    def get_output_tables(
        self,
        output_tables: Iterable[Table],
        scope_context: ScopeContext,
        state: ScopeState,
    ) -> list[tuple[api.Table, list[ColumnPath]]]:
        tables = []
        for table in output_tables:
            assert self.final_storages is not None
            storage = self.final_storages[table._universe]
            engine_table = state.get_table(storage)
            paths = [
                storage.get_path(column)
                for column in table._columns.values()
                if not scope_context.skip_column(column)
            ]
            tables.append((engine_table, paths))
        return tables

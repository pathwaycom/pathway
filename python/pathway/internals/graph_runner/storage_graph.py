# Copyright © 2024 Pathway

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import chain
from typing import TYPE_CHECKING

from pathway.internals import api
from pathway.internals.column import Column, IdColumn, MaterializedColumn
from pathway.internals.column_path import ColumnPath
from pathway.internals.graph_runner import path_evaluator
from pathway.internals.graph_runner.path_storage import Storage
from pathway.internals.graph_runner.scope_context import ScopeContext
from pathway.internals.graph_runner.state import ScopeState
from pathway.internals.helpers import StableSet
from pathway.internals.operator import InputOperator, IterateOperator, Operator
from pathway.internals.table import Table
from pathway.internals.universe import Universe

if TYPE_CHECKING:
    from pathway.internals.graph_runner import GraphRunner


@dataclass
class OperatorStorageGraph:
    scope_context: ScopeContext
    iterate_subgraphs: dict[Operator, OperatorStorageGraph]
    is_outer_graph: bool
    column_deps_at_output: dict[Operator, dict[Table, StableSet[Column]]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    initial_storages: dict[Universe, Storage] = field(
        default_factory=dict
    )  # for inner graph in iterate
    output_storages: dict[Operator, dict[Table, Storage]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    final_storages: dict[Universe, Storage] | None = None
    table_to_storage: dict[Table, Storage] = field(default_factory=dict)

    def get_iterate_subgraph(self, operator: Operator) -> OperatorStorageGraph:
        return self.iterate_subgraphs[operator]

    def has_column(self, table: Table, column: Column) -> bool:
        return self.table_to_storage[table].has_column(column)

    @classmethod
    def from_scope_context(
        cls,
        scope_context: ScopeContext,
        graph_builder: GraphRunner,
        output_tables: Iterable[Table],
    ) -> OperatorStorageGraph:
        graph = cls._create_storage_graph(scope_context, graph_builder)
        column_dependencies: dict[Universe, StableSet[Column]] = defaultdict(StableSet)
        for table in output_tables:
            column_dependencies[table._universe].update(table._columns.values())
        graph._compute_relevant_columns(column_dependencies)
        graph._compute_storage_paths()

        return graph

    @classmethod
    def _create_storage_graph(
        cls,
        scope_context: ScopeContext,
        graph_builder: GraphRunner,
        is_outer_graph: bool = True,
    ) -> OperatorStorageGraph:
        iterate_subgraphs: dict[Operator, OperatorStorageGraph] = {}
        for operator in scope_context.nodes:
            if isinstance(operator, IterateOperator):
                iterate_context = scope_context.iterate_subscope(
                    operator, graph_builder
                )
                iterate_subgraphs[operator] = cls._create_storage_graph(
                    iterate_context,
                    graph_builder,
                    is_outer_graph=False,
                )

        graph = cls(
            scope_context=scope_context,
            iterate_subgraphs=iterate_subgraphs,
            is_outer_graph=is_outer_graph,
        )
        return graph

    def _compute_relevant_columns(
        self,
        column_dependencies: dict[Universe, StableSet[Column]],
        input_universes: StableSet[Universe] | None = None,
    ) -> None:
        operators_reversed = reversed(list(self.scope_context.nodes))

        for operator in operators_reversed:
            self._compute_column_dependencies_ordinary(
                operator, self.scope_context, column_dependencies
            )

            if isinstance(operator, IterateOperator):
                self._compute_column_dependencies_iterate(operator)

        for universe, columns in column_dependencies.items():
            if len(columns) > 0:
                if not self._can_skip_universe_with_cols(
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
        input_universes: StableSet[Universe] | None,
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
            and input_universes is not None
            and universe in input_universes
            and all_materialized_or_id
        )

    def _compute_column_dependencies_ordinary(
        self,
        operator: Operator,
        scope_context: ScopeContext,
        column_dependencies: dict[Universe, StableSet[Column]],
    ) -> None:
        # reverse because we traverse the operators list backward
        # and want to process output tables before intermediate tables
        # because we want to propagate dependencies from output tables
        # to intermediate tables
        intermediate_and_output_tables_rev = reversed(
            list(operator.intermediate_and_output_tables)
        )

        for table in intermediate_and_output_tables_rev:
            output_deps: StableSet[Column] = StableSet()
            # set columns that have to be produced
            output_deps.update(column_dependencies.get(table._universe, []))
            # columns tree shaking if set to False
            if scope_context.run_all:
                output_deps.update(table._columns.values())
            self.column_deps_at_output[operator][table] = output_deps

            # add column dependencies
            for column in chain(table._columns.values(), [table._id_column]):
                # if the first condition is not met, the column is not needed (tree shaking)
                if column in output_deps or isinstance(column, IdColumn):
                    for dependency in column.column_dependencies():
                        if not isinstance(dependency, IdColumn):
                            column_dependencies[dependency.universe].add(dependency)

            # remove current columns (they are created in this operator)
            column_dependencies[table._universe] -= StableSet(table._columns.values())

        for table in operator.hard_table_dependencies():
            column_dependencies[table._universe].update(table._columns.values())

    def _compute_column_dependencies_iterate(self, operator: IterateOperator) -> None:
        inner_column_dependencies: dict[Universe, StableSet[Column]] = defaultdict(
            StableSet
        )
        all_columns: dict[Universe, StableSet[Column]] = defaultdict(StableSet)
        output_tables_columns: dict[Universe, StableSet[Column]] = defaultdict(
            StableSet
        )
        # propagate columns existing in iterate
        for name, outer_handle in operator._outputs.items():
            outer_table = outer_handle.value
            if name in operator.result_iterated:
                inner_table = operator.result_iterated[name]
            else:
                inner_table = operator.result_iterated_with_universe[name]
            assert isinstance(inner_table, Table)
            inner_deps = inner_column_dependencies[inner_table._universe]
            for column_name, outer_column in outer_table._columns.items():
                output_tables_columns[outer_table._universe].add(outer_column)
                inner_column = inner_table._columns[column_name]
                inner_deps.update([inner_column])

            if name in operator.result_iterated:
                all_columns[outer_table._universe].update(
                    self.column_deps_at_output[operator][outer_table]
                )

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

        self.iterate_subgraphs[operator]._compute_relevant_columns(
            inner_column_dependencies, input_universes
        )

    def _compute_storage_paths(self):
        storages: dict[Universe, Storage] = self.initial_storages.copy()
        for operator in self.scope_context.nodes:
            if isinstance(operator, InputOperator):
                self._compute_storage_paths_input(operator, storages)
            elif isinstance(operator, IterateOperator):
                self._compute_storage_paths_iterate(operator, storages)
            else:
                self._compute_storage_paths_ordinary(operator, storages)
            for table in operator.intermediate_and_output_tables:
                self.table_to_storage[table] = self.output_storages[operator][table]
        self.final_storages = storages

    def _compute_storage_paths_ordinary(
        self, operator: Operator, storages: dict[Universe, Storage]
    ) -> None:
        for table in operator.intermediate_and_output_tables:
            output_columns = self.column_deps_at_output[operator][table]
            path_storage = path_evaluator.compute_paths(
                output_columns,
                storages,
                operator,
                table._id_column.context,
                table._columns.values(),
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
            output_columns = self.column_deps_at_output[operator][table]
            path_storage = path_evaluator.compute_paths(
                output_columns,
                {},
                operator,
                table._id_column.context,
                table._columns.values(),
            )
            self.output_storages[operator][table] = path_storage
            assert table._universe not in storages
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

        for universe, paths in inner_column_paths.items():
            self.iterate_subgraphs[operator].initial_storages[universe] = Storage.flat(
                universe, paths.keys()
            )

        self.iterate_subgraphs[operator]._compute_storage_paths()

        # propagate paths over iterate (not looking inside)
        for name, handle in operator._outputs.items():
            table = handle.value
            output_columns = self.column_deps_at_output[operator][table]
            input_table = operator.get_input(name).value
            assert isinstance(input_table, Table)
            path_storage = Storage.flat(table._universe, output_columns)
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
        state: ScopeState,
    ) -> list[tuple[api.Table, list[ColumnPath]]]:
        tables = []
        for table in output_tables:
            assert self.final_storages is not None
            storage = self.final_storages[table._universe]
            engine_table = state.get_table(table._universe)
            paths = [storage.get_path(column) for column in table._columns.values()]
            tables.append((engine_table, paths))
        return tables

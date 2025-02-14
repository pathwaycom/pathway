# Copyright © 2024 Pathway

"""Operator evaluation strategy.

This module contains handlers for different types of operators.
Handlers should not be constructed directly. Use `Operator.for_operator()` instead.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from pathway.internals import api, trace
from pathway.internals.datasink import CallbackDataSink, ExportDataSink, GenericDataSink
from pathway.internals.datasource import (
    EmptyDataSource,
    ErrorLogDataSource,
    GenericDataSource,
    ImportDataSource,
    PandasDataSource,
)
from pathway.internals.graph_runner.expression_evaluator import ExpressionEvaluator
from pathway.internals.graph_runner.path_storage import Storage
from pathway.internals.graph_runner.scope_context import ScopeContext
from pathway.internals.graph_runner.state import ScopeState
from pathway.internals.graph_runner.storage_graph import OperatorStorageGraph
from pathway.internals.operator import (
    ContextualizedIntermediateOperator,
    DebugOperator,
    InputOperator,
    IterateOperator,
    Operator,
    OutputOperator,
)
from pathway.internals.table import Table

if TYPE_CHECKING:
    from pathway.internals.graph_runner import GraphRunner


T = TypeVar("T", bound=Operator)


class OperatorHandler(ABC, Generic[T]):
    scope: api.Scope
    state: ScopeState
    scope_context: ScopeContext
    graph_builder: GraphRunner
    storage_graph: OperatorStorageGraph
    operator_id: int

    _operator_mapping: ClassVar[dict[type[Operator], type[OperatorHandler]]] = {}
    operator_type: ClassVar[type[Operator]]

    def __init__(
        self,
        scope: api.Scope,
        state: ScopeState,
        context: ScopeContext,
        graph_builder: GraphRunner,
        storage_graph: OperatorStorageGraph,
        operator_id: int,
    ):
        self.scope = scope
        self.state = state
        self.scope_context = context
        self.graph_builder = graph_builder
        self.storage_graph = storage_graph
        self.operator_id = operator_id

    def __init_subclass__(cls, /, operator_type, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.operator_type = operator_type
        cls._operator_mapping[operator_type] = cls

    def run(
        self,
        operator: T,
        output_storages: dict[Table, Storage],
    ):
        with trace.custom_trace(operator.trace):
            self.scope.set_operator_properties(
                self.operator_id, operator.depends_on_error_log
            )
            if operator.error_log and not self.scope_context.inside_iterate:
                self.scope.set_error_log(self.state.get_error_log(operator.error_log))
            self._run(operator, output_storages)

    @abstractmethod
    def _run(
        self,
        operator: T,
        output_storages: dict[Table, Storage],
    ):
        pass

    @classmethod
    def for_operator(cls, operator: Operator) -> type[OperatorHandler]:
        return cls._operator_mapping[type(operator)]


class InputOperatorHandler(OperatorHandler[InputOperator], operator_type=InputOperator):
    def _run(
        self,
        operator: InputOperator,
        output_storages: dict[Table, Storage],
    ):
        datasource = operator.datasource
        if self.graph_builder.debug and operator.debug_datasource is not None:
            if (
                datasource.schema._dtypes()
                != operator.debug_datasource.schema._dtypes()
            ):
                raise ValueError("wrong schema of debug data")
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = api.static_table_from_pandas(
                    scope=self.scope,
                    df=operator.debug_datasource.data,
                    connector_properties=operator.debug_datasource.connector_properties,
                    schema=operator.debug_datasource.schema,
                )
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, PandasDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = api.static_table_from_pandas(
                    scope=self.scope,
                    df=datasource.data,
                    connector_properties=datasource.connector_properties,
                    schema=datasource.schema,
                )
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, GenericDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = self.scope.connector_table(
                    data_source=datasource.datastorage,
                    data_format=datasource.dataformat,
                    properties=datasource.connector_properties,
                )
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, EmptyDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = self.scope.empty_table(
                    datasource.connector_properties
                )
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, ImportDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                exported_table = datasource.callback(self.scope)
                materialized_table = self.scope.import_table(exported_table)
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, ErrorLogDataSource):
            for table in operator.output_tables:
                (materialized_table, error_log) = self.scope.error_log(
                    properties=datasource.connector_properties
                )
                self.state.set_table(output_storages[table], materialized_table)
                self.state.set_error_log(table, error_log)
        else:
            raise RuntimeError("datasource not supported")


class OutputOperatorHandler(
    OperatorHandler[OutputOperator], operator_type=OutputOperator
):
    def _run(
        self,
        operator: OutputOperator,
        output_storages: dict[Table, Storage],
    ):
        datasink = operator.datasink
        table = operator.table
        input_storage = self.state.get_storage(table._universe)
        engine_table = self.state.get_table(table._universe)
        column_paths = [
            input_storage.get_path(column) for column in table._columns.values()
        ]

        if isinstance(datasink, GenericDataSink):
            self.scope.output_table(
                table=engine_table,
                column_paths=column_paths,
                data_sink=datasink.datastorage,
                data_format=datasink.dataformat,
                unique_name=datasink.unique_name,
                sort_by_indices=datasink.sort_by_indices,
            )
        elif isinstance(datasink, CallbackDataSink):
            self.scope.subscribe_table(
                table=engine_table,
                column_paths=column_paths,
                on_change=datasink.on_change,
                on_time_end=datasink.on_time_end,
                on_end=datasink.on_end,
                skip_persisted_batch=datasink.skip_persisted_batch,
                skip_errors=datasink.skip_errors,
                unique_name=datasink.unique_name,
                sort_by_indices=datasink.sort_by_indices(table),
            )
        elif isinstance(datasink, ExportDataSink):
            exported_table = self.scope.export_table(
                table=engine_table, column_paths=column_paths
            )
            datasink.callback(self.scope, exported_table)
        else:
            raise RuntimeError("datasink not supported")


class ContextualizedIntermediateOperatorHandler(
    OperatorHandler[ContextualizedIntermediateOperator],
    operator_type=ContextualizedIntermediateOperator,
):
    def _run(
        self,
        operator: ContextualizedIntermediateOperator,
        output_storages: dict[Table, Storage],
    ):
        for table in operator.intermediate_and_output_tables:
            context = table._id_column.context
            evaluator_cls = ExpressionEvaluator.for_context(context)
            output_storage = output_storages[table]
            evaluator = evaluator_cls(
                context, self.scope, self.state, self.scope_context
            )

            engine_table = evaluator.run(output_storage)
            self.state.set_table(output_storage, engine_table)
            self.scope.probe_table(engine_table, self.operator_id)
            evaluator.flatten_table_storage_if_needed(output_storage)


class DebugOperatorHandler(
    OperatorHandler[DebugOperator],
    operator_type=DebugOperator,
):
    def __init__(
        self,
        scope: api.Scope,
        state: ScopeState,
        context: ScopeContext,
        graph_builder: GraphRunner,
        storage_graph: OperatorStorageGraph,
        operator_id: int,
    ):
        super().__init__(
            scope, state, context, graph_builder, storage_graph, operator_id
        )

    def _run(
        self,
        operator: DebugOperator,
        output_storages: dict[Table, Storage],
    ):
        table = operator.table
        input_storage = self.state.get_storage(table._universe)
        engine_table = self.state.get_table(table._universe)
        for name, column in table._columns.items():
            self.scope.debug_column(
                f"{operator.name}.{name}", engine_table, input_storage.get_path(column)
            )

        self.scope.debug_universe(operator.name, engine_table)


class IterateOperatorHandler(
    OperatorHandler[IterateOperator], operator_type=IterateOperator
):
    def _run(
        self,
        operator: IterateOperator,
        output_storages: dict[Table, Storage],
    ):
        def iterate_logic(
            scope: api.Scope,
            iterated: list[api.LegacyTable],
            iterated_with_universe: list[api.LegacyTable],
            extra: list[api.LegacyTable],
        ) -> tuple[list[api.LegacyTable], list[api.LegacyTable]]:
            iteration_state = ScopeState(scope)
            iteration_state.set_legacy_tables(operator.iterated_copy, iterated)
            iteration_state.set_legacy_tables(
                operator.iterated_with_universe_copy, iterated_with_universe
            )
            iteration_state.set_legacy_tables(operator.extra_copy, extra)

            self.storage_graph.get_iterate_subgraph(operator).build_scope(
                scope, iteration_state, self.graph_builder
            )

            return (
                iteration_state.get_legacy_tables(operator.result_iterated),
                iteration_state.get_legacy_tables(
                    operator.result_iterated_with_universe
                ),
            )

        iterated = self.state.get_legacy_tables(operator.iterated)
        iterated_with_universe = self.state.get_legacy_tables(
            operator.iterated_with_universe
        )
        extra = self.state.get_legacy_tables(operator.extra)

        if operator.iteration_limit == 1:
            result = iterate_logic(
                self.scope,
                iterated=iterated,
                iterated_with_universe=iterated_with_universe,
                extra=extra,
            )
        else:
            result = self.scope.iterate(
                iterated=iterated,
                iterated_with_universe=iterated_with_universe,
                extra=extra,
                logic=iterate_logic,
                limit=operator.iteration_limit,
            )

        # store iteration result in outer scope state
        self.state.set_legacy_tables(
            [source.value for source in operator.outputs], result[0] + result[1]
        )

        for table in operator.output_tables:
            self.state.create_table(table._universe, output_storages[table])

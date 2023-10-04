# Copyright © 2023 Pathway

"""Operator evaluation strategy.

This module contains handlers for different types of operators.
Handlers should not be constructed directly. Use `Operator.for_operator()` instead.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from pathway.internals import api
from pathway.internals import column as clmn
from pathway.internals import trace
from pathway.internals.datasink import CallbackDataSink, GenericDataSink
from pathway.internals.datasource import (
    EmptyDataSource,
    GenericDataSource,
    PandasDataSource,
)
from pathway.internals.graph_runner.expression_evaluator import (
    AutotuplingJoinRowwiseEvaluator,
    AutotuplingRowwiseEvaluator,
    ExpressionEvaluator,
)
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
    operator_id: int | None

    _operator_mapping: ClassVar[dict[type[Operator], type[OperatorHandler]]] = {}
    operator_type: ClassVar[type[Operator]]

    def __init__(
        self,
        scope: api.Scope,
        state: ScopeState,
        context: ScopeContext,
        graph_builder: GraphRunner,
        storage_graph: OperatorStorageGraph,
        operator_id: int | None,
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
                    self.scope,
                    operator.debug_datasource.data,
                    dict(table.schema._dtypes()),
                    operator.debug_datasource.schema.primary_key_columns(),
                    operator.debug_datasource.connector_properties,
                )
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, PandasDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = api.static_table_from_pandas(
                    self.scope,
                    datasource.data,
                    dict(table.schema._dtypes()),
                    datasource.schema.primary_key_columns(),
                    datasource.connector_properties,
                )
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, GenericDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = self.scope.connector_table(
                    datasource.datastorage,
                    datasource.dataformat,
                    datasource.connector_properties,
                )
                self.state.set_table(output_storages[table], materialized_table)
        elif isinstance(datasource, EmptyDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = self.scope.empty_table(
                    table.schema._dtypes().values()
                )
                self.state.set_table(output_storages[table], materialized_table)
        else:
            RuntimeError("datasource not supported")


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
        engine_table = self.state.get_table(input_storage)
        column_paths = [
            input_storage.get_path(column) for column in table._columns.values()
        ]

        if isinstance(datasink, GenericDataSink):
            self.scope.output_table(
                table=engine_table,
                column_paths=column_paths,
                data_sink=datasink.datastorage,
                data_format=datasink.dataformat,
            )
        elif isinstance(datasink, CallbackDataSink):
            self.scope.subscribe_table(
                table=engine_table,
                column_paths=column_paths,
                on_change=datasink.on_change,
                on_end=datasink.on_end,
            )
        else:
            RuntimeError("datasink not supported")


class ContextualizedIntermediateOperatorHandler(
    OperatorHandler[ContextualizedIntermediateOperator],
    operator_type=ContextualizedIntermediateOperator,
):
    def _run_legacy(self, table: Table):
        all_columns_skipped = True
        for _, column in table._columns.items():
            if not (
                self.scope_context.skip_column(column)
                or isinstance(column, clmn.MaterializedColumn)
            ):
                self.evaluate_column(column)
                all_columns_skipped = False

        if all_columns_skipped:
            # evaluate output universe if all columns of table was skipped
            evaluator = self._get_evaluator(table._id_column.context)
            if self.operator_id is not None:
                self.scope.probe_universe(evaluator.output_universe, self.operator_id)
            self.state.set_universe(table._universe, evaluator.output_universe)

        universe = self.state.get_universe(table._universe)
        self.state.set_column(table._id_column, universe.id_column)

        for _, column in table._columns.items():
            assert self._does_not_need_evaluation(
                column
            ), "some columns were not evaluated"

    def _run(
        self,
        operator: ContextualizedIntermediateOperator,
        output_storages: dict[Table, Storage],
    ):
        for table in operator.intermediate_and_output_tables:
            context = table._id_column.context
            evaluator_cls = ExpressionEvaluator.for_context(context)
            output_storage = output_storages[table]

            if isinstance(context, clmn.RowwiseContext):
                evaluator_cls = AutotuplingRowwiseEvaluator
            if isinstance(context, clmn.JoinRowwiseContext):
                evaluator_cls = AutotuplingJoinRowwiseEvaluator

            if evaluator_cls.run == ExpressionEvaluator.run:
                self._run_legacy(table)
                if output_storage.flattened_output is not None:
                    output_storage = output_storage.flattened_output
                self.state.create_table(table._universe, output_storage)
            else:
                # TODO implement run for all evaluators and remove the else branch
                evaluator = evaluator_cls(
                    context, self.scope, self.state, self.scope_context
                )
                storages = []
                for univ in context.universe_dependencies():
                    storage = self.state.get_storage(univ)
                    storages.append(storage)

                output_api_storage = evaluator.run(output_storage, *storages)
                self.state.set_table(output_storage, output_api_storage)
                evaluator.flatten_table_storage_if_needed(output_storage)

    def evaluate_column(self, column: clmn.Column) -> None:
        if self._does_not_need_evaluation(column):
            return

        assert isinstance(
            column, clmn.ColumnWithExpression
        ), "materialized column not evaluated"

        context = column.context
        col_evaluator = self._get_evaluator(context)
        evaluated_column = col_evaluator.eval(column)
        if self.operator_id is not None:
            self.scope.probe_column(evaluated_column, self.operator_id)
        self.state.set_column(column, evaluated_column)

    def _get_evaluator(self, context: clmn.Context) -> ExpressionEvaluator:
        def create_evaluator(context: clmn.Context):
            for col in context.column_dependencies_internal():
                self.evaluate_column(col)
            eval_cls = ExpressionEvaluator.for_context(context)
            return eval_cls(context, self.scope, self.state, self.scope_context)

        return self.state.get_or_create_evaluator(context, create_evaluator)

    def _does_not_need_evaluation(self, column) -> bool:
        return self.state.has_column(column) or self.scope_context.skip_column(column)


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
        operator_id: int | None,
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
        engine_table = self.state.get_table(input_storage)
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

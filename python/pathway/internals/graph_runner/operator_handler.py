# Copyright © 2023 Pathway

"""Operator evaluation strategy.

This module contains handlers for different types of operators.
Handlers should not be constructed directly. Use `Operator.for_operator()` instead.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from pathway.internals import api
from pathway.internals import column as clmn
from pathway.internals import trace
from pathway.internals.datasink import CallbackDataSink, GenericDataSink
from pathway.internals.datasource import (
    EmptyDataSource,
    GenericDataSource,
    PandasDataSource,
)
from pathway.internals.graph_runner.expression_evaluator import ExpressionEvaluator
from pathway.internals.graph_runner.scope_context import ScopeContext
from pathway.internals.graph_runner.state import ScopeState
from pathway.internals.operator import (
    ContextualizedIntermediateOperator,
    DebugOperator,
    InputOperator,
    IterateOperator,
    NonContextualizedIntermediateOperator,
    Operator,
    OutputOperator,
)

if TYPE_CHECKING:
    from pathway.internals.graph_runner import GraphRunner


T = TypeVar("T", bound=Operator)


class OperatorHandler(ABC, Generic[T]):
    scope: api.Scope
    state: ScopeState
    scope_context: ScopeContext
    graph_builder: GraphRunner
    operator_id: Optional[int]

    _operator_mapping: ClassVar[Dict[Type[Operator], Type[OperatorHandler]]] = {}
    operator_type: ClassVar[Type[Operator]]

    def __init__(
        self,
        scope: api.Scope,
        state: ScopeState,
        context: ScopeContext,
        graph_builder: GraphRunner,
        operator_id: Optional[int],
    ):
        self.scope = scope
        self.state = state
        self.scope_context = context
        self.graph_builder = graph_builder
        self.operator_id = operator_id

    def __init_subclass__(cls, /, operator_type, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.operator_type = operator_type
        cls._operator_mapping[operator_type] = cls

    def run(self, operator: T):
        with trace.custom_trace(operator.trace):
            self._run(operator)

    @abstractmethod
    def _run(self, operator: T):
        pass

    @classmethod
    def for_operator(cls, operator: Operator) -> Type[OperatorHandler]:
        return cls._operator_mapping[type(operator)]


class InputOperatorHandler(OperatorHandler[InputOperator], operator_type=InputOperator):
    def _run(self, operator: InputOperator):
        datasource = operator.datasource

        if self.graph_builder.debug and operator.debug_datasource is not None:
            if (
                datasource.schema.as_dict()
                != operator.debug_datasource.schema.as_dict()
            ):
                raise ValueError("wrong schema of debug data")
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = api.static_table_from_pandas(
                    self.scope,
                    operator.debug_datasource.data,
                    table.schema.as_dict(),
                    operator.debug_datasource.schema.primary_key_columns(),
                    operator.debug_datasource.connector_properties,
                )
                self.state.set_table(table, materialized_table)
        elif isinstance(datasource, PandasDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = api.static_table_from_pandas(
                    self.scope,
                    datasource.data,
                    table.schema.as_dict(),
                    datasource.schema.primary_key_columns(),
                    datasource.connector_properties,
                )
                self.state.set_table(table, materialized_table)
        elif isinstance(datasource, GenericDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = self.scope.connector_table(
                    datasource.datastorage,
                    datasource.dataformat,
                    datasource.connector_properties,
                )
                self.state.set_table(table, materialized_table)
        elif isinstance(datasource, EmptyDataSource):
            for table in operator.output_tables:
                assert table.schema is not None
                materialized_table = self.scope.empty_table(
                    table.schema.as_dict().values()
                )
                self.state.set_table(table, materialized_table)
        else:
            RuntimeError("datasource not supported")


class OutputOperatorHandler(
    OperatorHandler[OutputOperator], operator_type=OutputOperator
):
    def _run(self, operator: OutputOperator):
        datasink = operator.datasink
        table = self.state.get_table(operator.table)

        if isinstance(datasink, GenericDataSink):
            self.scope.output_table(
                table=table,
                data_sink=datasink.datastorage,
                data_format=datasink.dataformat,
            )
        elif isinstance(datasink, CallbackDataSink):
            self.scope.subscribe_table(
                table=table, on_change=datasink.on_change, on_end=datasink.on_end
            )
        else:
            RuntimeError("datasink not supported")


class ContextualizedIntermediateOperatorHandler(
    OperatorHandler[ContextualizedIntermediateOperator],
    operator_type=ContextualizedIntermediateOperator,
):
    def _run(
        self,
        operator: ContextualizedIntermediateOperator,
    ):
        for table in operator.output_tables:
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
                    self.scope.probe_universe(
                        evaluator.output_universe, self.operator_id
                    )
                self.state.set_universe(table._universe, evaluator.output_universe)

            universe = self.state.get_universe(table._universe)
            self.state.set_column(table._id_column, universe.id_column)

            for _, column in table._columns.items():
                assert self._does_not_need_evaluation(
                    column
                ), "some columns were not evaluated"

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
            for col in context.columns_to_eval():
                self.evaluate_column(col)
            eval_cls = ExpressionEvaluator.for_context(context)
            return eval_cls(context, self.scope, self.state, self.scope_context)

        return self.state.get_or_create_evaluator(context, create_evaluator)

    def _does_not_need_evaluation(self, column) -> bool:
        return self.state.has_column(column) or self.scope_context.skip_column(column)


class NonContextualizedIntermediateOperatorHandler(
    OperatorHandler[NonContextualizedIntermediateOperator],
    operator_type=NonContextualizedIntermediateOperator,
):
    def _run(
        self,
        operator: NonContextualizedIntermediateOperator,
    ):
        for table in operator.output_tables:
            for _, column in table._columns.items():
                assert self.state.has_column(column) or self.scope_context.skip_column(
                    column
                )
            universe = self.state.get_universe(table._universe)
            self.state.set_column(table._id_column, universe.id_column)


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
        operator_id: Optional[int],
    ):
        super().__init__(scope, state, context, graph_builder, operator_id)

    def _run(
        self,
        operator: DebugOperator,
    ):
        table = operator.table
        for name, column in table._columns.items():
            evaluated_column = self.state.get_column(column)
            self.scope.debug_column(f"{operator.name}.{name}", evaluated_column)

        universe = self.state.get_universe(table._universe)
        self.scope.debug_universe(operator.name, universe)


class IterateOperatorHandler(
    OperatorHandler[IterateOperator], operator_type=IterateOperator
):
    def _run(
        self,
        operator: IterateOperator,
    ):
        def iterate_logic(
            scope: api.Scope,
            iterated: List[api.Table],
            iterated_with_universe: List[api.Table],
            extra: List[api.Table],
        ) -> Tuple[List[api.Table], List[api.Table]]:
            iteration_state = ScopeState()
            iteration_state.set_tables(operator.iterated_copy, iterated)
            iteration_state.set_tables(
                operator.iterated_with_universe_copy, iterated_with_universe
            )
            iteration_state.set_tables(operator.extra_copy, extra)

            nodes, columns = self.graph_builder.tree_shake_tables(
                operator.scope,
                operator.result_iterated + operator.result_iterated_with_universe,
            )
            iteration_context = self.scope_context.subscope(nodes, columns)
            self.graph_builder.build_scope(
                scope,
                iteration_context,
                iteration_state,
            )

            return (
                iteration_state.get_tables(operator.result_iterated),
                iteration_state.get_tables(operator.result_iterated_with_universe),
            )

        iterated = self.state.get_tables(operator.iterated)
        iterated_with_universe = self.state.get_tables(operator.iterated_with_universe)
        extra = self.state.get_tables(operator.extra)

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
        self.state.set_tables(
            [source.value for source in operator.outputs], result[0] + result[1]
        )

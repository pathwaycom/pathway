# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable, Iterable

from pathway.internals import api, parse_graph as graph, table, trace
from pathway.internals.column_path import ColumnPath
from pathway.internals.config import pathway_config
from pathway.internals.graph_runner.async_utils import new_event_loop
from pathway.internals.graph_runner.row_transformer_operator_handler import (  # noqa: registers handler for RowTransformerOperator
    RowTransformerOperatorHandler,
)
from pathway.internals.graph_runner.scope_context import ScopeContext
from pathway.internals.graph_runner.state import ScopeState
from pathway.internals.graph_runner.storage_graph import OperatorStorageGraph
from pathway.internals.helpers import StableSet
from pathway.internals.monitoring import MonitoringLevel, monitor_stats
from pathway.internals.operator import (
    ContextualizedIntermediateOperator,
    InputOperator,
    Operator,
)
from pathway.persistence import Config as PersistenceConfig


class GraphRunner:
    """Runs evaluation of ParseGraph."""

    _graph: graph.ParseGraph
    debug: bool
    ignore_asserts: bool
    runtime_typechecking: bool

    def __init__(
        self,
        input_graph: graph.ParseGraph,
        *,
        debug: bool = False,
        ignore_asserts: bool | None = None,
        monitoring_level: MonitoringLevel = MonitoringLevel.AUTO,
        with_http_server: bool = False,
        default_logging: bool = True,
        persistence_config: PersistenceConfig | None = None,
        runtime_typechecking: bool | None = None,
    ) -> None:
        self._graph = input_graph
        self.debug = debug
        if ignore_asserts is None:
            ignore_asserts = pathway_config.ignore_asserts
        self.ignore_asserts = ignore_asserts
        self.monitoring_level = monitoring_level
        self.with_http_server = with_http_server
        self.default_logging = default_logging
        self.persistence_config = persistence_config or pathway_config.replay_config
        if runtime_typechecking is None:
            self.runtime_typechecking = pathway_config.runtime_typechecking
        else:
            self.runtime_typechecking = runtime_typechecking

    def run_tables(
        self,
        *tables: table.Table,
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ) -> list[api.CapturedStream]:
        nodes = self.tree_shake_tables(self._graph.global_scope, tables)
        context = ScopeContext(nodes, runtime_typechecking=self.runtime_typechecking)
        return self._run(context, output_tables=tables, after_build=after_build)

    def run_all(
        self,
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ) -> list[api.CapturedStream]:
        context = ScopeContext(
            nodes=self._graph.global_scope.nodes,
            run_all=True,
            runtime_typechecking=self.runtime_typechecking,
        )
        return self._run(context, after_build=after_build)

    def run_outputs(
        self,
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ):
        tables = (node.table for node in self._graph.global_scope.output_nodes)
        nodes = self._tree_shake(
            self._graph.global_scope, self._graph.global_scope.output_nodes, tables
        )
        context = ScopeContext(
            nodes=nodes, runtime_typechecking=self.runtime_typechecking
        )
        return self._run(context, after_build=after_build)

    def has_bounded_input(self, table: table.Table) -> bool:
        nodes = self.tree_shake_tables(self._graph.global_scope, [table])

        for node in nodes:
            if isinstance(node, InputOperator) and not node.datasource.is_bounded():
                return False

        return True

    def _run(
        self,
        context: ScopeContext,
        output_tables: Iterable[table.Table] = (),
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ) -> list[api.CapturedStream]:
        storage_graph = OperatorStorageGraph.from_scope_context(
            context, self, output_tables
        )

        def logic(
            scope: api.Scope,
            storage_graph: OperatorStorageGraph = storage_graph,
        ) -> list[tuple[api.Table, list[ColumnPath]]]:
            state = ScopeState(scope)
            storage_graph.build_scope(scope, state, self)
            if after_build is not None:
                after_build(state, storage_graph)
            return storage_graph.get_output_tables(output_tables, state)

        node_names = [
            (operator.id, operator.label())
            for operator in context.nodes
            if isinstance(operator, ContextualizedIntermediateOperator)
        ]
        monitoring_level = self.monitoring_level.to_internal()

        with new_event_loop() as event_loop, monitor_stats(
            monitoring_level, node_names, self.default_logging
        ) as stats_monitor:
            if self.persistence_config:
                self.persistence_config.on_before_run()
                persistence_engine_config = self.persistence_config.engine_config
            else:
                persistence_engine_config = None

            try:
                return api.run_with_new_graph(
                    logic,
                    event_loop=event_loop,
                    ignore_asserts=self.ignore_asserts,
                    stats_monitor=stats_monitor,
                    monitoring_level=monitoring_level,
                    with_http_server=self.with_http_server,
                    persistence_config=persistence_engine_config,
                )
            except api.EngineErrorWithTrace as e:
                error, frame = e.args
                if frame is not None:
                    trace.add_pathway_trace_note(
                        error,
                        trace.Frame(
                            filename=frame.file_name,
                            line_number=frame.line_number,
                            line=frame.line,
                            function=frame.function,
                        ),
                    )
                raise error

    def tree_shake_tables(
        self, graph_scope: graph.Scope, tables: Iterable[table.Table]
    ) -> StableSet[Operator]:
        starting_nodes: Iterable[Operator] = (
            table._source.operator for table in tables
        )
        return self._tree_shake(graph_scope, starting_nodes, tables)

    def _tree_shake(
        self,
        graph_scope: graph.Scope,
        starting_nodes: Iterable[Operator],
        tables: Iterable[table.Table],
    ) -> StableSet[Operator]:
        if self.debug:
            starting_nodes = (*starting_nodes, *graph_scope.debug_nodes)
            tables = (*tables, *(node.table for node in graph_scope.debug_nodes))
        nodes = StableSet(graph_scope.relevant_nodes(*starting_nodes))
        return nodes


__all__ = [
    "GraphRunner",
]

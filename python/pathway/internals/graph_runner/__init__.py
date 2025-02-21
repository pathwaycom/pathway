# Copyright © 2024 Pathway

from __future__ import annotations

import json
import os
import sys
import uuid
import warnings
from collections.abc import Callable, Collection, Iterable
from itertools import chain

import pathway.internals.graph_runner.telemetry as telemetry
from pathway.internals import api, parse_graph as graph, table, trace
from pathway.internals.column_path import ColumnPath
from pathway.internals.config import get_pathway_config
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
from pathway.persistence import (
    Config as PersistenceConfig,
    get_persistence_engine_config,
)


class GraphRunner:
    """Runs evaluation of ParseGraph."""

    _graph: graph.ParseGraph
    debug: bool
    ignore_asserts: bool
    runtime_typechecking: bool
    terminate_on_error: bool

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
        terminate_on_error: bool | None = None,
        _stacklevel: int = 1,
    ) -> None:
        pathway_config = get_pathway_config()
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
        self.license_key = pathway_config.license_key
        if terminate_on_error is None:
            terminate_on_error = pathway_config.terminate_on_error
        self.terminate_on_error = terminate_on_error
        if not self.terminate_on_error:
            warnings.warn(
                "terminate_on_error=False mode is experimental",
                stacklevel=_stacklevel + 1,
            )

    def run_nodes(
        self,
        nodes: Iterable[Operator],
        /,
        *,
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ):
        all_nodes = self._tree_shake(self._graph.global_scope, nodes)
        self._run(all_nodes, after_build=after_build)

    def run_tables(
        self,
        *tables: table.Table,
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ) -> list[api.CapturedStream]:
        nodes = self.tree_shake_tables(self._graph.global_scope, tables)
        return self._run(nodes, output_tables=tables, after_build=after_build)

    def run_all(
        self,
        *,
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ) -> None:
        self._run(
            self._graph.global_scope.normal_nodes, after_build=after_build, run_all=True
        )

    def run_outputs(
        self,
        *,
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
    ) -> None:
        self.run_nodes(self._graph.global_scope.output_nodes, after_build=after_build)

    def has_bounded_input(self, table: table.Table) -> bool:
        nodes = self.tree_shake_tables(self._graph.global_scope, [table])

        for node in nodes:
            if isinstance(node, InputOperator) and not node.datasource.is_bounded():
                return False

        return True

    def _run(
        self,
        nodes: Iterable[Operator],
        /,
        *,
        output_tables: Collection[table.Table] = (),
        after_build: Callable[[ScopeState, OperatorStorageGraph], None] | None = None,
        run_all: bool = False,
    ) -> list[api.CapturedStream]:
        self._graph.mark_all_operators_as_used()
        run_id = self._get_run_id()
        pathway_config = get_pathway_config()
        otel = telemetry.Telemetry.create(
            run_id=run_id,
            license_key=self.license_key,
            monitoring_server=pathway_config.monitoring_server,
        )
        with otel.tracer.start_as_current_span("graph_runner.run"):
            trace_context, trace_parent = telemetry.get_current_context()

            context = ScopeContext(
                nodes=StableSet(nodes),
                runtime_typechecking=self.runtime_typechecking,
                run_all=run_all,
            )

            storage_graph = OperatorStorageGraph.from_scope_context(
                context, self, output_tables
            )

            def logic(
                scope: api.Scope,
                /,
                *,
                storage_graph: OperatorStorageGraph = storage_graph,
                output_tables: Collection[table.Table] = output_tables,
            ) -> list[tuple[api.Table, list[ColumnPath]]]:
                with otel.tracer.start_as_current_span(
                    "graph_runner.build",
                    context=trace_context,
                    attributes=dict(
                        worker_id=scope.worker_index,
                        worker_count=scope.worker_count,
                        graph_statistics=json.dumps(self._graph.statistics()),
                        xpacks_used=telemetry.get_imported_xpacks(),
                    ),
                ):
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

            with (
                new_event_loop() as event_loop,
                monitor_stats(
                    monitoring_level,
                    node_names,
                    default_logging=self.default_logging,
                    process_id=pathway_config.process_id,
                ) as stats_monitor,
                otel.with_logging_handler(),
                get_persistence_engine_config(
                    self.persistence_config
                ) as persistence_engine_config,
            ):
                try:
                    return api.run_with_new_graph(
                        logic,
                        event_loop=event_loop,
                        ignore_asserts=self.ignore_asserts,
                        stats_monitor=stats_monitor,
                        monitoring_level=monitoring_level,
                        with_http_server=self.with_http_server,
                        persistence_config=persistence_engine_config,
                        license_key=self.license_key,
                        monitoring_server=pathway_config.monitoring_server,
                        trace_parent=trace_parent,
                        run_id=run_id,
                        terminate_on_error=self.terminate_on_error,
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
                    raise error from None
                except api.OtherWorkerError:
                    if pathway_config.suppress_other_worker_errors:
                        sys.exit(1)
                    else:
                        raise

    def _get_run_id(self):
        run_id = os.environ.get("PATHWAY_RUN_ID")
        if run_id is None:
            run_id = str(uuid.uuid4())
        return run_id

    def tree_shake_tables(
        self, graph_scope: graph.Scope, tables: Iterable[table.Table]
    ) -> StableSet[Operator]:
        starting_nodes = (table._source.operator for table in tables)
        return self._tree_shake(graph_scope, starting_nodes)

    def _tree_shake(
        self,
        graph_scope: graph.Scope,
        starting_nodes: Iterable[Operator],
    ) -> StableSet[Operator]:
        if self.debug:
            starting_nodes = chain(starting_nodes, graph_scope.debug_nodes)
        nodes = StableSet(graph_scope.relevant_nodes(starting_nodes))
        return nodes


__all__ = [
    "GraphRunner",
]

# Copyright © 2024 Pathway

from __future__ import annotations

import hashlib
import itertools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from pathway.internals import operator, trace
from pathway.internals.helpers import FunctionSpec, StableSet
from pathway.internals.universe_solver import UniverseSolver

if TYPE_CHECKING:
    from pathway.internals import Table


class Scope:
    """Keeps all nodes of one scope."""

    _nodes: dict[int, operator.Operator]

    def __init__(self):
        self.source = []
        self._nodes = {}

    def add_node(self, node: operator.Operator):
        self._nodes[node.id] = node

    @property
    def nodes(self):
        return self._nodes.values()

    @property
    def output_nodes(self):
        return (
            node for node in self.nodes if isinstance(node, operator.OutputOperator)
        )

    @property
    def debug_nodes(self):
        return (node for node in self.nodes if isinstance(node, operator.DebugOperator))

    def relevant_nodes(self, *operators: operator.Operator) -> list[operator.Operator]:
        visited: StableSet[operator.Operator] = StableSet()
        stack: list[operator.Operator] = list(StableSet(operators))
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            for dependency in node.input_operators():
                if dependency not in visited:
                    stack.append(dependency)
        # Iterate over original list of nodes to preserve insertion order
        return [node for node in self.nodes if node in visited]


T = TypeVar("T", bound=operator.Operator)


class ParseGraph:
    """Relates Tables and Operations."""

    node_id_sequence: itertools.count
    scopes: list[Scope]
    _scope_stack: list[Scope]
    universe_solver: UniverseSolver
    cache: dict[Any, Any]
    static_tables_cache: dict[int, Table]

    def __init__(self) -> None:
        self.clear()

    @property
    def _current_scope(self):
        return self._scope_stack[-1]

    @property
    def global_scope(self):
        return self._scope_stack[0]

    @trace.trace_user_frame
    def add_operator(
        self,
        create_node: Callable[[int], T],
        call_operator: Callable[[T], Any],
    ):
        """Adds an operator to current scope.

        Args:
            create_node: factory method creating an operator node, taking node id as a parameter.
            call_operator: operator callback
        """
        node = create_node(next(self.node_id_sequence))
        node.set_graph(self)
        result = call_operator(node)
        self._current_scope.add_node(node)
        return result

    def add_iterate(
        self,
        body: FunctionSpec,
        clb: Callable[[operator.IterateOperator], Any],
        iteration_limit: int | None = None,
    ):
        """Adds iterate operator.

        New scope is added to stack before calling iteration result. This scope is then preserved
        in IterateOperator node and removed from the stack.

        Args:
            body: specification of an iterated body function
            call_operator: iterate opereator callback
        """
        iterate_scope = Scope()
        node = operator.IterateOperator(
            body, next(self.node_id_sequence), iterate_scope, iteration_limit
        )
        node.set_graph(self)
        self._current_scope.add_node(node)

        self.scopes.append(iterate_scope)
        self._scope_stack.append(iterate_scope)
        result = clb(node)
        self._scope_stack.pop()
        return result

    def clear(self):
        self.node_id_sequence = itertools.count()
        global_scope = Scope()
        self._scope_stack = [global_scope]
        self.scopes = [global_scope]
        self.universe_solver = UniverseSolver()
        self.cache = {}
        self.static_tables_cache = {}

    def sig(self):
        return hashlib.sha256(repr(self).encode()).hexdigest()

    def __repr__(self):
        rows = []
        for scope_index, scope in enumerate(self.scopes):
            for node in scope.nodes:
                output_schemas = [table.schema for table in node.output_tables]
                rows.append((scope_index, node, output_schemas))
        return "\n".join(
            [
                f"{scope}, {node.id} [{node.label()}]: {schema}"
                for scope, node, schema in rows
            ]
        )


G = ParseGraph()

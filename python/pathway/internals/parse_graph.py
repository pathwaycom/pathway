# Copyright © 2024 Pathway

# mypy: disallow-untyped-defs, extra-checks, disallow-any-generics, warn-return-any

from __future__ import annotations

import atexit
import hashlib
import itertools
import warnings
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from types import TracebackType
from typing import TYPE_CHECKING, Any, TypeVar

from pathway.internals import operator
from pathway.internals.datasource import ErrorLogDataSource
from pathway.internals.helpers import FunctionSpec, StableSet
from pathway.internals.json import Json
from pathway.internals.schema import Schema
from pathway.internals.universe_solver import UniverseSolver

if TYPE_CHECKING:
    from pathway.internals import Table, interactive


class Scope:
    """Keeps all nodes of one scope."""

    _graph: ParseGraph
    _nodes: StableSet[operator.Operator]
    _normal_nodes: StableSet[operator.Operator]

    def __init__(self, graph: ParseGraph) -> None:
        self._graph = graph
        self._nodes = StableSet()
        self._normal_nodes = StableSet()

    def is_empty(self) -> bool:
        return not self._nodes

    def add_node(self, node: operator.Operator, *, special: bool = False) -> None:
        self._nodes.add(node)
        if not special:
            self._normal_nodes.add(node)

    @property
    def nodes(self) -> Iterator[operator.Operator]:
        return iter(self._nodes)

    @property
    def normal_nodes(self) -> Iterator[operator.Operator]:
        return iter(self._normal_nodes)

    @property
    def output_nodes(self) -> Iterator[operator.OutputOperator]:
        return (
            node
            for node in self.normal_nodes
            if isinstance(node, operator.OutputOperator)
        )

    @property
    def debug_nodes(self) -> Iterator[operator.DebugOperator]:
        return (
            node
            for node in self.normal_nodes
            if isinstance(node, operator.DebugOperator)
        )

    def relevant_nodes(
        self, operators: Iterable[operator.Operator]
    ) -> list[operator.Operator]:
        stack: list[operator.Operator] = list(operators)
        visited: set[operator.Operator] = set(stack)
        while stack:
            node = stack.pop()
            for dependency in node.input_operators():
                if dependency not in visited and dependency in self._nodes:
                    visited.add(dependency)
                    stack.append(dependency)
        # Iterate over original list of nodes to preserve insertion order
        return [node for node in self.nodes if node in visited]

    def __enter__(self) -> Scope:
        self._graph._scope_stack.append(self)
        return self

    def __exit__(
        self,
        /,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._graph._current_scope == self
        self._graph._scope_stack.pop()


O = TypeVar("O", bound=operator.Operator)
T = TypeVar("T")


class ParseGraph:
    """Relates Tables and Operations."""

    node_id_sequence: Iterator[int]
    scopes: list[Scope]
    _scope_stack: list[Scope]
    universe_solver: UniverseSolver
    cache: dict[Any, Any]
    static_tables_cache: dict[int, Table[Any]]
    interactive_mode_controller: interactive.InteractiveModeController | None = None
    error_log_stack: list[Table[ErrorLogSchema]]
    unused_operators: bool

    def __init__(self) -> None:
        self.clear()

    @property
    def _current_scope(self) -> Scope:
        return self._scope_stack[-1]

    @property
    def global_scope(self) -> Scope:
        return self._scope_stack[0]

    def new_scope(self) -> Scope:
        scope = Scope(self)
        self.scopes.append(scope)
        return scope

    def add_operator(
        self,
        create_node: Callable[[int], O],
        call_operator: Callable[[O], T],
        *,
        special: bool = False,
        require_error_log: bool = True,
    ) -> T:
        """Adds an operator to current scope.

        Args:
            create_node: factory method creating an operator node, taking node id as a parameter.
            call_operator: operator callback
        """
        node = create_node(next(self.node_id_sequence))
        node.set_graph(self)
        if require_error_log and not self.error_log_stack:
            self.add_error_log(global_log=True)  # deferred global log creation
        node.set_error_log(self.error_log_stack[-1] if require_error_log else None)
        result = call_operator(node)
        self._current_scope.add_node(node, special=special)
        self.unused_operators = True
        return result

    def add_iterate(
        self,
        body: FunctionSpec,
        call_operator: Callable[[operator.IterateOperator], T],
        iteration_limit: int | None = None,
    ) -> T:
        """Adds iterate operator.

        New scope is added to stack before calling iteration result. This scope is then preserved
        in IterateOperator node and removed from the stack.

        Args:
            body: specification of an iterated body function
            call_operator: iterate opereator callback
        """

        def create_node(id: int) -> operator.IterateOperator:
            return operator.IterateOperator(body, id, iterate_scope, iteration_limit)

        def call_operator_in_scope(o: operator.IterateOperator) -> T:
            with iterate_scope:
                return call_operator(o)

        iterate_scope = self.new_scope()
        return self.add_operator(create_node, call_operator_in_scope)

    def add_error_log(self, global_log: bool = False) -> Table[ErrorLogSchema]:
        datasource = ErrorLogDataSource(schema=ErrorLogSchema)
        from pathway.internals.table import Table

        error_log: Table[ErrorLogSchema] = self.add_operator(
            lambda id: operator.InputOperator(datasource, id),
            lambda operator: operator(Table[ErrorLogSchema]),
            require_error_log=not global_log,
        )
        self.error_log_stack.append(error_log)
        return error_log

    def remove_error_log(self, error_log: Table[ErrorLogSchema]) -> None:
        assert self.error_log_stack[-1] == error_log
        self.error_log_stack.pop()

    def get_global_error_log(self) -> Table[ErrorLogSchema]:
        if not self.error_log_stack:
            self.add_error_log(global_log=True)
        return self.error_log_stack[0]

    def clear(self) -> None:
        self.node_id_sequence = itertools.count()
        global_scope = Scope(self)
        self._scope_stack = [global_scope]
        self.scopes = [global_scope]
        self.universe_solver = UniverseSolver()
        self.cache = {}
        self.static_tables_cache = {}
        self.error_log_stack = []
        self.mark_all_operators_as_used()

    def mark_all_operators_as_used(self) -> None:
        self.unused_operators = False

    def sig(self) -> str:
        return hashlib.sha256(repr(self).encode()).hexdigest()

    def __repr__(self) -> str:
        rows = []
        for scope_index, scope in enumerate(self.scopes):
            for node in scope.nodes:
                output_schemas = [table.schema for table in node.output_tables]
                rows.append((scope_index, node, output_schemas))
        return "\n".join(
            f"{scope}, {node.id} [{node.label()}]: {schema}"
            for scope, node, schema in rows
        )

    def statistics(self) -> dict[str, int]:
        all_nodes = [node for scope in self.scopes for node in scope.nodes]
        counter = Counter(node.operator_type() for node in all_nodes)
        return dict(counter)


class ErrorLogSchema(Schema):
    operator_id: int
    message: str
    trace: Json | None


G = ParseGraph()


def warn_if_some_operators_unused() -> None:
    if G.unused_operators:
        warnings.warn(
            "There are operators in the computation graph that haven't been used."
            + " Use pathway.run() (or similar) to run the computation involving these nodes."
        )


atexit.register(warn_if_some_operators_unused)

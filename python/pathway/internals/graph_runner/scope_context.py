# Copyright © 2024 Pathway

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from pathway.internals import operator
from pathway.internals.helpers import StableSet

if TYPE_CHECKING:
    from pathway.internals.graph_runner import GraphRunner


@dataclass
class ScopeContext:
    nodes: StableSet[operator.Operator]
    run_all: bool = False
    subscopes: dict[operator.Operator, ScopeContext] = field(default_factory=dict)
    runtime_typechecking: bool = False
    inside_iterate: bool = False

    def iterate_subscope(
        self, operator: operator.IterateOperator, graph_builder: GraphRunner
    ) -> ScopeContext:
        if operator not in self.subscopes:
            nodes = graph_builder.tree_shake_tables(
                operator.scope,
                operator.result_iterated + operator.result_iterated_with_universe,
            )
            self.subscopes[operator] = replace(self, nodes=nodes, inside_iterate=True)

        return self.subscopes[operator]

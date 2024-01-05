# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from pathway.internals import operator

if TYPE_CHECKING:
    from pathway.internals.graph_runner import GraphRunner


@dataclass
class ScopeContext:
    nodes: Iterable[operator.Operator]
    run_all: bool = False
    subscopes: dict[operator.Operator, ScopeContext] = field(default_factory=dict)
    runtime_typechecking: bool = False

    def iterate_subscope(
        self, operator: operator.IterateOperator, graph_builder: GraphRunner
    ) -> ScopeContext:
        if operator not in self.subscopes:
            nodes = graph_builder.tree_shake_tables(
                operator.scope,
                operator.result_iterated + operator.result_iterated_with_universe,
            )
            self.subscopes[operator] = replace(self, nodes=nodes)

        return self.subscopes[operator]

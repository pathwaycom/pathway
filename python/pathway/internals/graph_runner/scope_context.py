# Copyright © 2023 Pathway

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from pathway.internals import column as clmn, operator
from pathway.internals.helpers import StableSet

if TYPE_CHECKING:
    from pathway.internals.graph_runner import GraphRunner


@dataclass
class ScopeContext:
    nodes: Iterable[operator.Operator]
    columns: StableSet[clmn.Column] = field(default_factory=StableSet)
    run_all: bool = False
    subscopes: dict[operator.Operator, ScopeContext] = field(default_factory=dict)

    def skip_column(self, column: clmn.Column) -> bool:
        if self.run_all:
            return False
        return column not in self.columns

    def iterate_subscope(
        self, operator: operator.IterateOperator, graph_builder: GraphRunner
    ) -> ScopeContext:
        if operator not in self.subscopes:
            nodes, columns = graph_builder.tree_shake_tables(
                operator.scope,
                operator.result_iterated + operator.result_iterated_with_universe,
            )
            self.subscopes[operator] = replace(self, nodes=nodes, columns=columns)

        return self.subscopes[operator]

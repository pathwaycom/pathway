# Copyright © 2023 Pathway

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Iterable

from pathway.internals import column as clmn
from pathway.internals import operator
from pathway.internals.helpers import StableSet


@dataclass
class ScopeContext:
    nodes: Iterable[operator.Operator]
    columns: StableSet[clmn.Column] = field(default_factory=StableSet)
    run_all: bool = False

    def skip_column(self, column: clmn.Column) -> bool:
        if self.run_all:
            return False
        return column not in self.columns

    def subscope(
        self, nodes: Iterable[operator.Operator], columns: StableSet[clmn.Column]
    ):
        return replace(self, nodes=nodes, columns=columns)

# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.internals import column
from pathway.internals.helpers import SetOnceProperty


class Universe:
    lineage: SetOnceProperty[column.Lineage] = SetOnceProperty()

    def subset(self) -> Universe:
        from pathway.internals.parse_graph import G

        return G.universe_solver.get_subset(self)

    def superset(self) -> Universe:
        from pathway.internals.parse_graph import G

        return G.universe_solver.get_superset(self)

    def is_subset_of(self, other: Universe) -> bool:
        from pathway.internals.parse_graph import G

        return G.universe_solver.query_is_subset(self, other)

    def is_equal_to(self, other: Universe) -> bool:
        return self.is_subset_of(other) and other.is_subset_of(self)

    def is_empty(self) -> bool:
        from pathway.internals.parse_graph import G

        return G.universe_solver.query_is_empty(self)

    def register_as_empty(self, no_warn: bool = True) -> None:
        from pathway.internals.parse_graph import G

        G.universe_solver.register_as_empty(self, no_warn=no_warn)

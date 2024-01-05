# Copyright © 2024 Pathway

"""Auxiliary representation of RowTransformer attributes.

Used during evaluation to correctly match RowTransformer attributes to columns
in the graph and collect all dependencies.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

import pathway.internals.column as clmn
from pathway.internals.helpers import StableSet
from pathway.internals.universe import Universe

if TYPE_CHECKING:
    from pathway.internals import row_transformer as rt
    from pathway.internals.operator import RowTransformerOperator
    from pathway.internals.table import Table


@dataclass(frozen=True)
class TransformerColumn(ABC):
    attribute: rt.AbstractAttribute
    """attribute from which the column was created"""
    operator: RowTransformerOperator
    """operator to which the operator belongs"""

    @property
    @abstractmethod
    def graph_column(self) -> clmn.Column:
        """Returns graph column associated with this column."""
        pass

    def dependencies(self) -> StableSet[TransformerColumn]:
        """Returns all columns that are potential dependencies of this column."""
        return StableSet()


@dataclass(frozen=True)
class InputTransformerColumn(TransformerColumn, ABC):
    input_table: Table

    @cached_property
    def graph_column(self) -> clmn.Column:
        return self.input_table._get_column(self.attribute.name)


@dataclass(frozen=True)
class TransformerColumnWithDependenecies(TransformerColumn, ABC):
    def dependencies(self) -> StableSet[TransformerColumn]:
        result: StableSet[TransformerColumn] = StableSet()
        operator: RowTransformerOperator = (
            self.graph_column.lineage.source.operator  # type:ignore
        )
        stack = operator.all_columns()
        while stack:
            column = stack.pop()
            if isinstance(column, InputMethodTransformerColumn):
                stack += column.dependencies()
            result.add(column)

        return result


@dataclass(frozen=True)
class ToBeComputedTransformerColumn(TransformerColumn):
    attribute: rt.ToBeComputedAttribute

    @cached_property
    def graph_column(self) -> clmn.Column:
        table = self.operator.get_table(self.attribute.class_arg.name)
        return table._get_column(self.attribute.output_name)


@dataclass(frozen=True)
class MethodTransformerColumn(
    TransformerColumnWithDependenecies, ToBeComputedTransformerColumn
):
    attribute: rt.Method


@dataclass(frozen=True)
class OutputAttributeTransformerColumn(ToBeComputedTransformerColumn):
    attribute: rt.OutputAttribute


@dataclass(frozen=True)
class InputMethodTransformerColumn(
    InputTransformerColumn, TransformerColumnWithDependenecies
):
    attribute: rt.InputMethod


@dataclass(frozen=True)
class InputAttributeTransformerColumn(InputTransformerColumn):
    attribute: rt.InputAttribute


@dataclass(frozen=True)
class TransformerTable:
    universe: Universe
    columns: list[TransformerColumn]

    def _input_method_columns(self):
        return [c for c in self.columns if isinstance(c, InputMethodTransformerColumn)]

    @property
    def dependency_table(self) -> TransformerTable:
        columns = StableSet.union(
            *(column.dependencies() for column in self._input_method_columns())
        )
        return TransformerTable(self.universe, list(columns))

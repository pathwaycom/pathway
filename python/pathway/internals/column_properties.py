# Copyright © 2024 Pathway

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pathway.internals import dtype as dt

if TYPE_CHECKING:
    import pathway.internals.column as clmn


@dataclass(frozen=True)
class ColumnProperties:
    dtype: dt.DType
    append_only: bool = False


class ColumnPropertiesEvaluator(ABC):
    def __init__(self, context: clmn.Context) -> None:
        pass

    def eval(self, column: clmn.ColumnWithContext) -> ColumnProperties:
        return ColumnProperties(
            dtype=column.context_dtype, append_only=self._append_only(column)
        )

    @abstractmethod
    def _append_only(self, column: clmn.ColumnWithContext) -> bool: ...


class DefaultPropsEvaluator(ColumnPropertiesEvaluator):
    def _append_only(self, column: clmn.ColumnWithContext) -> bool:
        return False


class PreserveDependenciesPropsEvaluator(ColumnPropertiesEvaluator):
    def _append_only(self, column: clmn.ColumnWithContext):
        return self._has_property(column, "append_only", True)

    def _has_property(self, column: clmn.ColumnWithContext, name: str, value: Any):
        return all(
            getattr(col.properties, name) == value
            for col in column.column_dependencies()
        )


class UpdateRowsPropsEvaluator(ColumnPropertiesEvaluator):
    context: clmn.UpdateRowsContext

    def __init__(self, context) -> None:
        super().__init__(context)
        self.context = context

    def _append_only(self, column: clmn.ColumnWithContext) -> bool:
        if column == self.context.id_column:
            return all(
                id_column.properties.append_only for id_column in self.context.union_ids
            )
        else:
            return False


class UpdateCellsPropsEvaluator(ColumnPropertiesEvaluator):
    context: clmn.UpdateCellsContext

    def __init__(self, context) -> None:
        super().__init__(context)
        self.context = context

    def _append_only(self, column: clmn.ColumnWithContext) -> bool:
        if column == self.context.id_column:
            return self.context.left.properties.append_only
        else:
            return False


class JoinPropsEvaluator(PreserveDependenciesPropsEvaluator):
    context: clmn.JoinContext

    def __init__(self, context) -> None:
        super().__init__(context)
        self.context = context

    def _append_only(self, column: clmn.ColumnWithContext) -> bool:
        if (
            self.context.left_ear or self.context.right_ear
        ) and not self.context.exact_match:
            return False
        else:
            return super()._append_only(column)


class PromiseSameUniversePropsEvaluator(PreserveDependenciesPropsEvaluator):
    context: clmn.PromiseSameUniverseContext

    def __init__(self, context) -> None:
        super().__init__(context)
        self.context = context

    def _append_only(self, column: clmn.ColumnWithContext) -> bool:
        if column == self.context.id_column:
            if (
                self.context.orig_id_column.properties.append_only
                != self.context._id_column.properties.append_only
            ):
                warnings.warn(
                    "Universes in with_universe_of have different value of the append_only"
                    + " property. Setting append_only of the output to True"
                )
            return (
                self.context.orig_id_column.properties.append_only
                or self.context._id_column.properties.append_only
            )
        else:
            return super()._append_only(column)


class AppendOnlyPropsEvaluator(ColumnPropertiesEvaluator):
    def _append_only(self, column: clmn.ColumnWithContext) -> bool:
        return True

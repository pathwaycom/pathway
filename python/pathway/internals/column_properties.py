# Copyright © 2024 Pathway

from __future__ import annotations

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

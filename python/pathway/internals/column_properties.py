# Copyright © 2025 Pathway

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathway.internals import column as clmn, dtype as dt


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
        maybe_append_only = self._check_expression(column)
        return maybe_append_only and self._has_property(column, "append_only", True)

    def _has_property(self, column: clmn.ColumnWithContext, name: str, value: Any):
        return all(
            getattr(col.properties, name) == value
            for col in column.column_dependencies()
        )

    def _check_expression(self, column: clmn.ColumnWithContext) -> bool:
        from pathway.internals.column import ColumnWithExpression
        from pathway.internals.expression_props_evaluator import (
            ExpressionPropsEvaluator,
            PropsEvaluatorState,
        )

        if isinstance(column, ColumnWithExpression):
            evaluator = ExpressionPropsEvaluator()
            props = PropsEvaluatorState(True)
            evaluator.eval_expression(column.expression, props=props)
            return props.append_only
        else:
            return True


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

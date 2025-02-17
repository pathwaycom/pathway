# Copyright © 2025 Pathway

from __future__ import annotations

from dataclasses import dataclass

from pathway.internals import expression as expr
from pathway.internals.expression_visitor import IdentityTransform


@dataclass
class PropsEvaluatorState:
    append_only: bool


class ExpressionPropsEvaluator(IdentityTransform):
    def eval_fully_async_apply(
        self,
        expression: expr.FullyAsyncApplyExpression,
        props: PropsEvaluatorState | None = None,
        **kwargs,
    ) -> expr.FullyAsyncApplyExpression:
        assert props is not None
        props.append_only = False
        return super().eval_fully_async_apply(expression, props=props, **kwargs)

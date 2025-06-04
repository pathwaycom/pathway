# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from pathway.internals import expression as expr

if TYPE_CHECKING:
    from pathway.internals.table import Table


class ExpressionVisitor(ABC):
    def eval_expression(self, expression, **kwargs):
        impl: dict[type, Callable] = {
            expr.ColumnReference: self.eval_column_val,
            expr.ColumnUnaryOpExpression: self.eval_unary_op,
            expr.ColumnBinaryOpExpression: self.eval_binary_op,
            expr.ReducerExpression: self.eval_reducer,
            expr.ApplyExpression: self.eval_apply,
            expr.ColumnConstExpression: self.eval_const,
            expr.ColumnCallExpression: self.eval_call,
            expr.PointerExpression: self.eval_pointer,
            expr.CastExpression: self.eval_cast,
            expr.ConvertExpression: self.eval_convert,
            expr.DeclareTypeExpression: self.eval_declare,
            expr.CoalesceExpression: self.eval_coalesce,
            expr.RequireExpression: self.eval_require,
            expr.IfElseExpression: self.eval_ifelse,
            expr.AsyncApplyExpression: self.eval_async_apply,
            expr.MakeTupleExpression: self.eval_make_tuple,
            expr.GetExpression: self.eval_get,
            expr.MethodCallExpression: self.eval_method_call,
            expr.IsNotNoneExpression: self.eval_not_none,
            expr.IsNoneExpression: self.eval_none,
            expr.UnwrapExpression: self.eval_unwrap,
            expr.FillErrorExpression: self.eval_fill_error,
            expr.FullyAsyncApplyExpression: self.eval_fully_async_apply,
        }
        if not isinstance(expression, expr.ColumnExpression):
            return self.eval_any(expression, **kwargs)
        return impl[type(expression)](expression, **kwargs)

    @abstractmethod
    def eval_column_val(self, expression: expr.ColumnReference): ...

    @abstractmethod
    def eval_unary_op(self, expression: expr.ColumnUnaryOpExpression): ...

    @abstractmethod
    def eval_binary_op(self, expression: expr.ColumnBinaryOpExpression): ...

    @abstractmethod
    def eval_const(self, expression: expr.ColumnConstExpression): ...

    @abstractmethod
    def eval_reducer(self, expression: expr.ReducerExpression): ...

    @abstractmethod
    def eval_apply(self, expression: expr.ApplyExpression): ...

    @abstractmethod
    def eval_async_apply(self, expression: expr.AsyncApplyExpression): ...

    @abstractmethod
    def eval_pointer(self, expression: expr.PointerExpression): ...

    @abstractmethod
    def eval_call(self, expression: expr.ColumnCallExpression): ...

    @abstractmethod
    def eval_cast(self, expression: expr.CastExpression): ...

    @abstractmethod
    def eval_convert(self, expression: expr.ConvertExpression): ...

    @abstractmethod
    def eval_declare(self, expression: expr.DeclareTypeExpression): ...

    @abstractmethod
    def eval_coalesce(self, expression: expr.CoalesceExpression): ...

    @abstractmethod
    def eval_require(self, expression: expr.RequireExpression): ...

    @abstractmethod
    def eval_ifelse(self, expression: expr.IfElseExpression): ...

    @abstractmethod
    def eval_not_none(self, expression: expr.IsNotNoneExpression): ...

    @abstractmethod
    def eval_none(self, expression: expr.IsNoneExpression): ...

    @abstractmethod
    def eval_make_tuple(self, expression: expr.MakeTupleExpression): ...

    @abstractmethod
    def eval_get(self, expression: expr.GetExpression): ...

    @abstractmethod
    def eval_method_call(self, expression: expr.MethodCallExpression): ...

    @abstractmethod
    def eval_unwrap(self, expression: expr.UnwrapExpression): ...

    @abstractmethod
    def eval_fill_error(self, expression: expr.FillErrorExpression): ...

    @abstractmethod
    def eval_fully_async_apply(self, expression: expr.FullyAsyncApplyExpression): ...

    def eval_any(self, expression, **kwargs):
        expression = expr.ColumnConstExpression(expression)
        return self.eval_const(expression, **kwargs)


ColExprT = TypeVar("ColExprT", bound=expr.ColumnExpression)


class IdentityTransform(ExpressionVisitor):
    def eval_expression(self, expression: ColExprT, **kwargs) -> ColExprT:
        ret = super().eval_expression(expression, **kwargs)
        if hasattr(expression, "_dtype"):
            ret._dtype = expression._dtype
        return ret

    def eval_column_val(
        self, expression: expr.ColumnReference, **kwargs
    ) -> expr.ColumnReference:
        return expr.ColumnReference(
            _column=expression._column, _table=expression._table, _name=expression._name
        )

    def eval_unary_op(
        self, expression: expr.ColumnUnaryOpExpression, **kwargs
    ) -> expr.ColumnUnaryOpExpression:
        result = self.eval_expression(expression._expr, **kwargs)
        return expr.ColumnUnaryOpExpression(expr=result, operator=expression._operator)

    def eval_binary_op(
        self, expression: expr.ColumnBinaryOpExpression, **kwargs
    ) -> expr.ColumnBinaryOpExpression:
        left = self.eval_expression(expression._left, **kwargs)
        right = self.eval_expression(expression._right, **kwargs)
        return expr.ColumnBinaryOpExpression(
            left=left, right=right, operator=expression._operator
        )

    def eval_const(
        self, expression: expr.ColumnConstExpression, **kwargs
    ) -> expr.ColumnConstExpression:
        return expr.ColumnConstExpression(expression._val)

    def eval_reducer(
        self, expression: expr.ReducerExpression, **kwargs
    ) -> expr.ReducerExpression:
        args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        return expr.ReducerExpression(expression._reducer, *args, **expression._kwargs)

    def eval_apply(
        self, expression: expr.ApplyExpression, **kwargs
    ) -> expr.ApplyExpression:
        expr_args = tuple(
            self.eval_expression(arg, **kwargs) for arg in expression._args
        )
        expr_kwargs = {
            name: self.eval_expression(arg, **kwargs)
            for name, arg in expression._kwargs.items()
        }
        return expr.ApplyExpression(
            expression._fun,
            expression._return_type,
            propagate_none=expression._propagate_none,
            deterministic=expression._deterministic,
            args=expr_args,
            kwargs=expr_kwargs,
            _check_for_disallowed_types=expression._check_for_disallowed_types,
            max_batch_size=expression._max_batch_size,
        )

    def eval_async_apply(
        self, expression: expr.AsyncApplyExpression, **kwargs
    ) -> expr.AsyncApplyExpression:
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        expr_kwargs = {
            name: self.eval_expression(arg, **kwargs)
            for name, arg in expression._kwargs.items()
        }
        return expr.AsyncApplyExpression(
            expression._fun,
            expression._return_type,
            propagate_none=expression._propagate_none,
            deterministic=expression._deterministic,
            args=tuple(expr_args),
            kwargs=expr_kwargs,
            max_batch_size=expression._max_batch_size,
        )

    def eval_fully_async_apply(
        self, expression: expr.FullyAsyncApplyExpression, **kwargs
    ) -> expr.FullyAsyncApplyExpression:
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        expr_kwargs = {
            name: self.eval_expression(arg, **kwargs)
            for name, arg in expression._kwargs.items()
        }
        return expr.FullyAsyncApplyExpression(
            expression._fun,
            expression._return_type,
            propagate_none=expression._propagate_none,
            deterministic=expression._deterministic,
            autocommit_duration_ms=expression.autocommit_duration_ms,
            args=tuple(expr_args),
            kwargs=expr_kwargs,
            max_batch_size=expression._max_batch_size,
        )

    def eval_pointer(
        self, expression: expr.PointerExpression, **kwargs
    ) -> expr.PointerExpression:
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        if expression._instance is not None:
            instance = self.eval_expression(expression._instance, **kwargs)
        else:
            instance = None
        optional = expression._optional
        return expr.PointerExpression(
            expression._table,
            *expr_args,
            optional=optional,
            instance=instance,
        )

    def eval_call(
        self, expression: expr.ColumnCallExpression, **kwargs
    ) -> expr.ColumnCallExpression:
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        col_expr = self.eval_expression(expression._col_expr, **kwargs)
        assert isinstance(col_expr, expr.ColumnReference)
        return expr.ColumnCallExpression(col_expr=col_expr, args=expr_args)

    def eval_cast(
        self, expression: expr.CastExpression, **kwargs
    ) -> expr.CastExpression:
        result = self.eval_expression(expression._expr, **kwargs)
        return expr.CastExpression(return_type=expression._return_type, expr=result)

    def eval_convert(
        self, expression: expr.ConvertExpression, **kwargs
    ) -> expr.ConvertExpression:
        return expr.ConvertExpression(
            return_type=expression._return_type,
            expr=self.eval_expression(expression._expr, **kwargs),
            default=self.eval_expression(expression._default, **kwargs),
            unwrap=expression._unwrap,
        )

    def eval_declare(
        self, expression: expr.DeclareTypeExpression, **kwargs
    ) -> expr.DeclareTypeExpression:
        result = self.eval_expression(expression._expr, **kwargs)
        return expr.DeclareTypeExpression(
            return_type=expression._return_type, expr=result
        )

    def eval_coalesce(
        self, expression: expr.CoalesceExpression, **kwargs
    ) -> expr.CoalesceExpression:
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        return expr.CoalesceExpression(*expr_args)

    def eval_require(
        self, expression: expr.RequireExpression, **kwargs
    ) -> expr.RequireExpression:
        val = self.eval_expression(expression._val, **kwargs)
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        return expr.RequireExpression(val, *expr_args)

    def eval_ifelse(
        self, expression: expr.IfElseExpression, **kwargs
    ) -> expr.IfElseExpression:
        return expr.IfElseExpression(
            self.eval_expression(expression._if, **kwargs),
            self.eval_expression(expression._then, **kwargs),
            self.eval_expression(expression._else, **kwargs),
        )

    def eval_not_none(
        self, expression: expr.IsNotNoneExpression, **kwargs
    ) -> expr.IsNotNoneExpression:
        return expr.IsNotNoneExpression(
            self.eval_expression(expression._expr, **kwargs)
        )

    def eval_none(
        self, expression: expr.IsNoneExpression, **kwargs
    ) -> expr.IsNoneExpression:
        return expr.IsNoneExpression(self.eval_expression(expression._expr, **kwargs))

    def eval_make_tuple(
        self, expression: expr.MakeTupleExpression, **kwargs
    ) -> expr.MakeTupleExpression:
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        return expr.MakeTupleExpression(*expr_args)

    def eval_get(self, expression: expr.GetExpression, **kwargs) -> expr.GetExpression:
        return expr.GetExpression(
            self.eval_expression(expression._object, **kwargs),
            self.eval_expression(expression._index, **kwargs),
            self.eval_expression(expression._default, **kwargs),
            check_if_exists=expression._check_if_exists,
        )

    def eval_method_call(
        self, expression: expr.MethodCallExpression, **kwargs
    ) -> expr.MethodCallExpression:
        expr_args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        return expr.MethodCallExpression(
            expression._fun_mapping,
            expression._name,
            *expr_args,
        )

    def eval_unwrap(
        self, expression: expr.UnwrapExpression, **kwargs
    ) -> expr.UnwrapExpression:
        result = self.eval_expression(expression._expr, **kwargs)
        return expr.UnwrapExpression(expr=result)

    def eval_fill_error(
        self, expression: expr.FillErrorExpression, **kwargs
    ) -> expr.FillErrorExpression:
        result = self.eval_expression(expression._expr, **kwargs)
        replacement = self.eval_expression(expression._replacement, **kwargs)
        return expr.FillErrorExpression(result, replacement)


class TableCollector(IdentityTransform):
    table_list: list[Table]

    def __init__(self):
        self.table_list = []

    def eval_column_val(self, expression: expr.ColumnReference, **kwargs: Any):
        self.table_list.append(expression.table)
        return super().eval_column_val(expression, **kwargs)


def collect_tables(expression: expr.ColumnExpression) -> list[Table]:
    collector = TableCollector()
    collector.eval_expression(expression)
    return collector.table_list

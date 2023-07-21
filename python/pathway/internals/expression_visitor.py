# Copyright © 2023 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Dict, Type, TypeVar, cast

from pathway.internals import expression as expr


class ExpressionVisitor(ABC):
    def eval_expression(self, expression, **kwargs):
        impl: Dict[Type, Callable] = {
            expr.ColumnReference: self.eval_column_val,
            expr.ColumnUnaryOpExpression: self.eval_unary_op,
            expr.ColumnBinaryOpExpression: self.eval_binary_op,
            expr.ReducerExpression: self.eval_reducer,
            expr.ReducerIxExpression: self.eval_reducer_ix,
            expr.ApplyExpression: self.eval_apply,
            expr.ColumnConstExpression: self.eval_const,
            expr.ColumnIxExpression: self.eval_ix,
            expr.ColumnCallExpression: self.eval_call,
            expr.PointerExpression: self.eval_pointer,
            expr.CastExpression: self.eval_cast,
            expr.DeclareTypeExpression: self.eval_declare,
            expr.CoalesceExpression: self.eval_coalesce,
            expr.RequireExpression: self.eval_require,
            expr.IfElseExpression: self.eval_ifelse,
            expr.NumbaApplyExpression: self.eval_numbaapply,
            expr.AsyncApplyExpression: self.eval_async_apply,
            expr.MakeTupleExpression: self.eval_make_tuple,
            expr.SequenceGetExpression: self.eval_sequence_get,
            expr.MethodCallExpression: self.eval_method_call,
        }
        if not isinstance(expression, expr.ColumnExpression):
            return self.eval_any(expression)
        return impl[type(expression)](expression, **kwargs)

    @abstractmethod
    def eval_column_val(self, expression: expr.ColumnReference):
        ...

    @abstractmethod
    def eval_unary_op(self, expression: expr.ColumnUnaryOpExpression):
        ...

    @abstractmethod
    def eval_binary_op(self, expression: expr.ColumnBinaryOpExpression):
        ...

    @abstractmethod
    def eval_const(self, expression: expr.ColumnConstExpression):
        ...

    @abstractmethod
    def eval_reducer(self, expression: expr.ReducerExpression):
        ...

    @abstractmethod
    def eval_reducer_ix(self, expression: expr.ReducerIxExpression):
        ...

    @abstractmethod
    def eval_apply(self, expression: expr.ApplyExpression):
        ...

    @abstractmethod
    def eval_numbaapply(self, expression: expr.NumbaApplyExpression):
        ...

    @abstractmethod
    def eval_async_apply(self, expression: expr.AsyncApplyExpression):
        ...

    @abstractmethod
    def eval_pointer(self, expression: expr.PointerExpression):
        ...

    @abstractmethod
    def eval_ix(self, expression: expr.ColumnIxExpression):
        ...

    @abstractmethod
    def eval_call(self, expression: expr.ColumnCallExpression):
        ...

    @abstractmethod
    def eval_cast(self, expression: expr.CastExpression):
        ...

    @abstractmethod
    def eval_declare(self, expression: expr.DeclareTypeExpression):
        ...

    @abstractmethod
    def eval_coalesce(self, expression: expr.CoalesceExpression):
        ...

    @abstractmethod
    def eval_require(self, expression: expr.RequireExpression):
        ...

    @abstractmethod
    def eval_ifelse(self, expression: expr.IfElseExpression):
        ...

    @abstractmethod
    def eval_make_tuple(self, expression: expr.MakeTupleExpression):
        ...

    @abstractmethod
    def eval_sequence_get(self, expression: expr.SequenceGetExpression):
        ...

    @abstractmethod
    def eval_method_call(self, expression: expr.MethodCallExpression):
        ...

    def eval_any(self, expression):
        expression = expr.ColumnConstExpression(expression)
        return self.eval_const(expression)


ColExprT = TypeVar("ColExprT", bound=expr.ColumnExpression)


class IdentityTransform(ExpressionVisitor):
    def eval_expression(self, expression: ColExprT, **kwargs) -> ColExprT:
        return super().eval_expression(expression, **kwargs)

    def eval_column_val(self, expression: expr.ColumnReference) -> expr.ColumnReference:
        return expr.ColumnReference(
            column=expression._column, table=expression._table, name=expression._name
        )

    def eval_unary_op(
        self, expression: expr.ColumnUnaryOpExpression
    ) -> expr.ColumnUnaryOpExpression:
        result = self.eval_expression(expression._expr)
        return expr.ColumnUnaryOpExpression(expr=result, operator=expression._operator)

    def eval_binary_op(
        self, expression: expr.ColumnBinaryOpExpression
    ) -> expr.ColumnBinaryOpExpression:
        left = self.eval_expression(expression._left)
        right = self.eval_expression(expression._right)
        return expr.ColumnBinaryOpExpression(
            left=left, right=right, operator=expression._operator
        )

    def eval_const(
        self, expression: expr.ColumnConstExpression
    ) -> expr.ColumnConstExpression:
        return expr.ColumnConstExpression(expression._val)

    def eval_reducer(
        self, expression: expr.ReducerExpression
    ) -> expr.ReducerExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        return expr.ReducerExpression(expression._reducer, *args)

    def eval_reducer_ix(
        self, expression: expr.ReducerIxExpression
    ) -> expr.ReducerIxExpression:
        [arg] = [self.eval_expression(arg) for arg in expression._args]
        return expr.ReducerIxExpression(
            expression._reducer, cast(expr.ColumnIxExpression, arg)
        )

    def eval_apply(self, expression: expr.ApplyExpression) -> expr.ApplyExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        kwargs = {
            name: self.eval_expression(arg) for name, arg in expression._kwargs.items()
        }
        return expr.ApplyExpression(
            expression._fun, expression._return_type, *args, **kwargs
        )

    def eval_numbaapply(
        self, expression: expr.NumbaApplyExpression
    ) -> expr.NumbaApplyExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        kwargs = {
            name: self.eval_expression(arg) for name, arg in expression._kwargs.items()
        }
        return expr.NumbaApplyExpression(
            expression._fun, expression._return_type, *args, **kwargs
        )

    def eval_async_apply(
        self, expression: expr.AsyncApplyExpression
    ) -> expr.AsyncApplyExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        kwargs = {
            name: self.eval_expression(arg) for name, arg in expression._kwargs.items()
        }
        return expr.AsyncApplyExpression(
            expression._fun, expression._return_type, *args, **kwargs
        )

    def eval_pointer(
        self, expression: expr.PointerExpression
    ) -> expr.PointerExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        optional = expression._optional
        return expr.PointerExpression(expression._table, *args, optional=optional)

    def eval_ix(self, expression: expr.ColumnIxExpression) -> expr.ColumnExpression:
        column_expression = self.eval_column_val(expression._column_expression)
        keys_expression = self.eval_expression(expression._keys_expression)
        return expr.ColumnIxExpression(
            column_expression=column_expression,
            keys_expression=keys_expression,
            optional=expression._optional,
        )

    def eval_call(self, expression: expr.ColumnCallExpression) -> expr.ColumnExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        col_expr = self.eval_expression(expression._col_expr)
        assert isinstance(col_expr, expr.ColumnRefOrIxExpression)
        return expr.ColumnCallExpression(col_expr=col_expr, args=args)

    def eval_cast(self, expression: expr.CastExpression) -> expr.CastExpression:
        result = self.eval_expression(expression._expr)
        return expr.CastExpression(return_type=expression._return_type, expr=result)

    def eval_declare(
        self, expression: expr.DeclareTypeExpression
    ) -> expr.DeclareTypeExpression:
        result = self.eval_expression(expression._expr)
        return expr.DeclareTypeExpression(
            return_type=expression._return_type, expr=result
        )

    def eval_coalesce(
        self, expression: expr.CoalesceExpression
    ) -> expr.CoalesceExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        return expr.CoalesceExpression(*args)

    def eval_require(
        self, expression: expr.RequireExpression
    ) -> expr.RequireExpression:
        val = self.eval_expression(expression._val)
        args = [self.eval_expression(arg) for arg in expression._args]
        return expr.RequireExpression(val, *args)

    def eval_ifelse(self, expression: expr.IfElseExpression) -> expr.IfElseExpression:
        return expr.IfElseExpression(
            self.eval_expression(expression._if),
            self.eval_expression(expression._then),
            self.eval_expression(expression._else),
        )

    def eval_make_tuple(
        self, expression: expr.MakeTupleExpression
    ) -> expr.MakeTupleExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        return expr.MakeTupleExpression(*args)

    def eval_sequence_get(
        self, expression: expr.SequenceGetExpression
    ) -> expr.SequenceGetExpression:
        return expr.SequenceGetExpression(
            self.eval_expression(expression._object),
            self.eval_expression(expression._index),
            self.eval_expression(expression._default),
            check_if_exists=expression._check_if_exists,
        )

    def eval_method_call(
        self, expression: expr.MethodCallExpression
    ) -> expr.MethodCallExpression:
        args = [self.eval_expression(arg) for arg in expression._args]
        return expr.MethodCallExpression(
            expression._fun_mapping,
            expression._return_type_fun,
            expression._name,
            *args,
        )

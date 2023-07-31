# Copyright © 2023 Pathway

from __future__ import annotations

import datetime
import warnings
from typing import Any, Optional, Tuple, get_args, get_origin

import numpy as np

from pathway.internals import _reducers
from pathway.internals import expression as expr
from pathway.internals.api import Pointer
from pathway.internals.datetime_types import DateTimeNaive, DateTimeUtc, Duration
from pathway.internals.dtype import DType, dtype_issubclass, types_lca, unoptionalize
from pathway.internals.expression_printer import get_expression_info
from pathway.internals.expression_visitor import ExpressionVisitor
from pathway.internals.graph_runner.operator_mapping import (
    get_binary_operators_mapping,
    get_binary_operators_mapping_optionals,
    get_unary_operators_mapping,
)
from pathway.internals.shadows import inspect


def _test_type(expected, received):
    if not dtype_issubclass(received, expected):
        raise TypeError(f"Expected {expected}, received {received}.")


class TypeInterpreter(ExpressionVisitor):
    def eval_expression(self, expression: expr.ColumnExpression, **kwargs) -> DType:
        assert not kwargs
        return super().eval_expression(expression)

    def eval_column_val(self, expression: expr.ColumnReference) -> DType:
        return expression._column.dtype

    def eval_unary_op(self, expression: expr.ColumnUnaryOpExpression) -> DType:
        operator_fun = expression._operator
        operand_dtype = self.eval_expression(expression._expr)
        if (
            dtype_and_handler := get_unary_operators_mapping(
                operator_fun, operand_dtype
            )
        ) is not None:
            return dtype_and_handler[0]
        sig = inspect.signature(expression._operator)
        _test_type(sig.parameters["expr"].annotation, operand_dtype)
        return sig.return_annotation

    def eval_binary_op(self, expression: expr.ColumnBinaryOpExpression) -> DType:
        left_dtype = self.eval_expression(expression._left)
        right_dtype = self.eval_expression(expression._right)
        if (
            dtype_and_handler := get_binary_operators_mapping(
                expression._operator, left_dtype, right_dtype
            )
        ) is not None:
            return DType(dtype_and_handler[0])

        left_dtype, right_dtype = unoptionalize(left_dtype, right_dtype)

        if (
            dtype_and_handler := get_binary_operators_mapping_optionals(
                expression._operator, left_dtype, right_dtype
            )
        ) is not None:
            return DType(dtype_and_handler[0])

        # I mean it.
        if (
            dtype_and_handler := get_binary_operators_mapping(
                expression._operator, left_dtype, right_dtype
            )
        ) is not None:
            return Optional[dtype_and_handler[0]]  # I also mean it.

        sig = inspect.signature(expression._operator)
        # TODO: never do a fallback
        return sig.return_annotation
        raise RuntimeError(
            f"Pathway does not support using binary operator {expression._operator.__name__}"
            + f" on columns of types {left_dtype}, {right_dtype}"
        )

    def eval_const(self, expression: expr.ColumnConstExpression) -> DType:
        type_ = type(expression._val)
        if isinstance(expression._val, datetime.datetime):
            if expression._val.tzinfo is None:
                type_ = DateTimeNaive
            else:
                type_ = DateTimeUtc
        elif isinstance(expression._val, datetime.timedelta):
            type_ = Duration
        return DType(type_)

    def eval_reducer(self, expr: expr.ReducerExpression) -> DType:
        if expr._reducer in (
            _reducers._min,
            _reducers._max,
            _reducers._unique,
            _reducers._any,
        ):
            return self.eval_expression(expr._args[0])
        sig = inspect.signature(expr._reducer)
        if sig.return_annotation is float and all(
            self.eval_expression(arg) is DType(int) for arg in expr._args
        ):
            return DType(int)
        # assert len(sig.parameters) == len(expr._args)
        # for arg_sig, expr_arg in zip(sig.parameters.values(), expr._args):
        #     annot = arg_sig.annotation
        #     assert annot._name == "Dict"
        #     assert annot.__args__[0] is int
        #     expr_dtype = self.dtype(expr_arg, context)
        #     print(expr_dtype)
        #     print(annot.__args__[1])
        #     assert dtype_issubclass(expr_dtype, annot.__args__[1])
        return sig.return_annotation

    def eval_reducer_ix(self, expr: expr.ReducerIxExpression) -> DType:
        return self.eval_reducer(expr)

    def eval_apply(self, expression: expr.ApplyExpression) -> DType:
        return expression._return_type

    def eval_numbaapply(self, expression: expr.NumbaApplyExpression) -> DType:
        return expression._return_type

    def eval_async_apply(self, expression: expr.AsyncApplyExpression) -> DType:
        return expression._return_type

    def eval_call(self, expression: expr.ColumnCallExpression) -> DType:
        arg_annots, ret = get_args(expression._col_expr._column.dtype)
        if arg_annots is not Ellipsis:
            arg_annots = arg_annots[1:]  # ignoring self
            for arg_annot, arg in zip(arg_annots, expression._args, strict=True):  # type: ignore
                arg_dtype = self.eval_expression(arg)
                assert dtype_issubclass(arg_dtype, arg_annot)
        return ret

    def eval_ix(self, expression: expr.ColumnIxExpression) -> DType:
        if expression._optional:
            return DType(Optional[expression._column.dtype])
        else:
            return expression._column.dtype

    def eval_pointer(self, expression: expr.PointerExpression) -> DType:
        if expression._optional:
            return DType(Optional[Pointer])
        else:
            return DType(Pointer)

    def eval_cast(self, expression: expr.CastExpression) -> DType:
        return expression._return_type

    def eval_declare(self, expression: expr.DeclareTypeExpression) -> DType:
        return expression._return_type

    def eval_coalesce(self, expression: expr.CoalesceExpression) -> DType:
        dtypes = [self.eval_expression(arg) for arg in expression._args]
        ret = dtypes[0]
        for dt in dtypes[1:]:
            ret = types_lca(dt, ret)
        return ret

    def eval_require(self, expression: expr.RequireExpression) -> DType:
        return DType(Optional[self.eval_expression(expression._val)])

    def eval_ifelse(self, expression: expr.IfElseExpression) -> DType:
        return types_lca(
            self.eval_expression(expression._then),
            self.eval_expression(expression._else),
        )

    def eval_make_tuple(self, expression: expr.MakeTupleExpression) -> DType:
        dtypes = tuple([self.eval_expression(arg) for arg in expression._args])
        return DType(Tuple[dtypes])

    def eval_sequence_get(self, expression: expr.SequenceGetExpression) -> DType:
        object_dtype = self.eval_expression(expression._object)
        index_dtype = self.eval_expression(expression._index)
        default_dtype = self.eval_expression(expression._default)

        if (
            get_origin(object_dtype) != tuple
            and object_dtype != Any
            and object_dtype != np.ndarray
        ):
            raise TypeError(f"Object in {expression!r} has to be a sequence.")
        if index_dtype != int:
            raise TypeError(f"Index in {expression!r} has to be an int.")

        if object_dtype == np.ndarray:
            warnings.warn(
                f"Object in {expression!r} is of type numpy.ndarray but its number of"
                + " dimensions is not known. Pathway cannot determine the return type"
                + " and will set Any as the return type. Please use "
                + "pathway.declare_type to set the correct return type."
            )
            return DType(Any)

        dtypes = get_args(object_dtype)

        if object_dtype == Any or len(dtypes) == 0:
            return DType(Any)

        if (
            expression._const_index is None
        ):  # no specified position, index is an Expression
            return_dtype = dtypes[0]
            for dtype in dtypes[1:]:
                return_dtype = types_lca(return_dtype, dtype)
            if expression._check_if_exists:
                return_dtype = types_lca(return_dtype, default_dtype)
            return return_dtype

        try:
            return_dtype = dtypes[expression._const_index]
            if return_dtype == Ellipsis:
                raise IndexError
            return return_dtype
        except IndexError:
            if dtypes[-1] == Ellipsis:
                return_dtype = dtypes[-2]
                if expression._check_if_exists:
                    return_dtype = types_lca(return_dtype, default_dtype)
                return return_dtype
            else:
                message = (
                    f"Index {expression._const_index} out of range for a tuple of"
                    + f" type {object_dtype}."
                )
                if expression._check_if_exists:
                    expression_info = get_expression_info(expression)
                    warnings.warn(
                        message
                        + " It refers to the following expression:\n"
                        + expression_info
                        + "Consider using just the default value without .get()."
                    )
                    return default_dtype
                else:
                    raise IndexError(message)

    def eval_method_call(self, expression: expr.MethodCallExpression) -> DType:
        dtypes = tuple([self.eval_expression(arg) for arg in expression._args])
        if dtypes not in expression._fun_mapping:
            if len(dtypes) > 0:
                with_arguments = f" with arguments of type {dtypes[1:]}"
            else:
                with_arguments = ""
            raise AttributeError(
                f"Column of type {dtypes[0]} has no attribute {expression._name}{with_arguments}."
            )
        return DType(expression._return_type_fun(dtypes))


class JoinTypeInterpreter(TypeInterpreter):
    def __init__(self, left, right, optionalize_left, optionalize_right):
        self.left = left
        self.right = right
        self.optionalize_left = optionalize_left
        self.optionalize_right = optionalize_right

    def eval_column_val(self, expression: expr.ColumnReference) -> DType:
        dtype = expression._column.dtype
        if expression.table == self.left:
            if self.optionalize_left:
                return DType(Optional[dtype])
            else:
                return dtype
        elif expression.table == self.right:
            if self.optionalize_right:
                return DType(Optional[dtype])
            else:
                return dtype
        else:
            return super().eval_column_val(expression)


_type_interpreter = TypeInterpreter()


def eval_type(expression: expr.ColumnExpression) -> DType:
    return _type_interpreter.eval_expression(expression)

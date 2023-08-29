# Copyright © 2023 Pathway

from __future__ import annotations

import datetime
import warnings
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Tuple, TypeVar, get_args, get_origin

import numpy as np

from pathway.internals import expression as expr
from pathway.internals.api import Pointer
from pathway.internals.datetime_types import DateTimeNaive, DateTimeUtc, Duration
from pathway.internals.dtype import (
    DType,
    dtype_issubclass,
    is_optional,
    is_pointer,
    is_tuple_like,
    normalize_tuple_like,
    types_lca,
    unoptionalize,
    unoptionalize_pair,
)
from pathway.internals.expression_printer import get_expression_info
from pathway.internals.expression_visitor import IdentityTransform
from pathway.internals.operator_mapping import (
    common_dtype_in_binary_operator,
    get_binary_operators_mapping,
    get_binary_operators_mapping_optionals,
    get_unary_operators_mapping,
    tuple_handling_operators,
)


@dataclass(eq=True, frozen=True)
class TypeInterpreterState:
    unoptionalize: Iterable[expr.InternalColRef] = ()

    def with_new_col(
        self, new_cols: Iterable[expr.ColumnReference]
    ) -> TypeInterpreterState:
        return TypeInterpreterState(
            [*self.unoptionalize, *[col._to_internal() for col in new_cols]]
        )

    def check_colref_to_unoptionalize_from_colrefs(
        self, colref: expr.ColumnReference
    ) -> bool:
        return colref._to_internal() in self.unoptionalize

    def check_colref_to_unoptionalize_from_tables(
        self, colref: expr.ColumnReference
    ) -> bool:
        return self.check_colref_to_unoptionalize_from_colrefs(colref.table.id)


class TypeInterpreter(IdentityTransform):
    def eval_column_val(
        self,
        expression: expr.ColumnReference,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ColumnReference:
        expression = super().eval_column_val(expression, state=state, **kwargs)
        return _wrap(expression, self._eval_column_val(expression, state))

    def _eval_column_val(
        self,
        expression: expr.ColumnReference,
        state: Optional[TypeInterpreterState] = None,
    ) -> DType:
        dtype = expression._column.dtype
        assert state is not None
        if state.check_colref_to_unoptionalize_from_colrefs(expression):
            return unoptionalize(dtype)
        return dtype

    def eval_unary_op(
        self,
        expression: expr.ColumnUnaryOpExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ColumnUnaryOpExpression:
        expression = super().eval_unary_op(expression, state=state, **kwargs)
        operand_dtype = expression._expr._dtype
        operator_fun = expression._operator
        if (
            dtype := get_unary_operators_mapping(operator_fun, operand_dtype)
        ) is not None:
            return _wrap(expression, dtype)
        expression_info = get_expression_info(expression)
        raise TypeError(
            f"Pathway does not support using unary operator {operator_fun.__name__}"
            + f" on column of type {expression._dtype}.\n"
            + "It refers to the following expression:\n"
            + expression_info
        )

    def eval_binary_op(
        self,
        expression: expr.ColumnBinaryOpExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ColumnBinaryOpExpression:
        expression = super().eval_binary_op(expression, state=state, **kwargs)
        left_dtype = expression._left._dtype
        right_dtype = expression._right._dtype
        return _wrap(
            expression,
            self._eval_binary_op(
                left_dtype, right_dtype, expression._operator, expression
            ),
        )

    def _eval_binary_op(
        self,
        left_dtype: DType,
        right_dtype: DType,
        operator: Any,
        expression: expr.ColumnExpression,
    ) -> DType:
        original_left = left_dtype
        original_right = right_dtype
        if (
            dtype := common_dtype_in_binary_operator(left_dtype, right_dtype)
        ) is not None:
            left_dtype = dtype
            right_dtype = dtype

        if (
            dtype := get_binary_operators_mapping(operator, left_dtype, right_dtype)
        ) is not None:
            return DType(dtype)

        left_dtype, right_dtype = unoptionalize_pair(left_dtype, right_dtype)

        if (
            dtype_and_handler := get_binary_operators_mapping_optionals(
                operator, left_dtype, right_dtype
            )
        ) is not None:
            return DType(dtype_and_handler[0])

        if (
            get_origin(left_dtype) == get_origin(Tuple)
            and get_origin(right_dtype) == get_origin(Tuple)
            and operator in tuple_handling_operators
        ):
            left_args = get_args(left_dtype)
            right_args = get_args(right_dtype)
            if len(left_args) == len(right_args):
                results = tuple(
                    self._eval_binary_op(left_arg, right_arg, operator, expression)
                    for left_arg, right_arg in zip(left_args, right_args)
                )
                assert all(result == results[0] for result in results)
                return DType(results[0])
        expression_info = get_expression_info(expression)
        raise TypeError(
            f"Pathway does not support using binary operator {operator.__name__}"
            + f" on columns of types {original_left}, {original_right}.\n"
            + "It refers to the following expression:\n"
            + expression_info
        )

    def eval_const(
        self,
        expression: expr.ColumnConstExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ColumnConstExpression:
        expression = super().eval_const(expression, state=state, **kwargs)
        type_ = type(expression._val)
        if isinstance(expression._val, datetime.datetime):
            if expression._val.tzinfo is None:
                type_ = DateTimeNaive
            else:
                type_ = DateTimeUtc
        elif isinstance(expression._val, datetime.timedelta):
            type_ = Duration
        return _wrap(expression, DType(type_))

    def eval_reducer(
        self,
        expression: expr.ReducerExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ReducerExpression:
        expression = super().eval_reducer(expression, state=state, **kwargs)
        return _wrap(expression, self._eval_reducer(expression))

    def _eval_reducer(
        self,
        expression: expr.ReducerExpression,
    ) -> DType:
        [arg] = expression._args
        return expression._reducer.return_type(arg._dtype)

    def eval_reducer_ix(
        self,
        expression: expr.ReducerIxExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ReducerIxExpression:
        expression = super().eval_reducer_ix(expression, state=state, **kwargs)
        return _wrap(expression, self._eval_reducer(expression))

    def eval_count(
        self,
        expression: expr.CountExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.CountExpression:
        expression = super().eval_count(expression, state=state, **kwargs)
        return _wrap(expression, DType(int))

    def eval_apply(
        self,
        expression: expr.ApplyExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ApplyExpression:
        expression = super().eval_apply(expression, state=state, **kwargs)
        return _wrap(expression, expression._return_type)

    def eval_numbaapply(
        self,
        expression: expr.NumbaApplyExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.NumbaApplyExpression:
        expression = super().eval_numbaapply(expression, state=state, **kwargs)
        return _wrap(expression, expression._return_type)

    def eval_async_apply(
        self,
        expression: expr.AsyncApplyExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.AsyncApplyExpression:
        expression = super().eval_async_apply(expression, state=state, **kwargs)
        return _wrap(expression, expression._return_type)

    def eval_call(
        self,
        expression: expr.ColumnCallExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ColumnCallExpression:
        expression = super().eval_call(expression, state=state, **kwargs)
        arg_annots, ret_type = get_args(expression._col_expr._column.dtype)
        if arg_annots is not Ellipsis:
            arg_annots = arg_annots[1:]  # ignoring self
            for arg_annot, arg in zip(arg_annots, expression._args, strict=True):  # type: ignore
                arg_dtype = arg._dtype
                assert dtype_issubclass(arg_dtype, arg_annot)
        return _wrap(expression, ret_type)

    def eval_ix(
        self,
        expression: expr.ColumnIxExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.ColumnIxExpression:
        expression = super().eval_ix(expression, state=state, **kwargs)
        key_dtype = expression._keys_expression._dtype
        if expression._optional:
            if not dtype_issubclass(key_dtype, DType(Optional[Pointer])):
                raise TypeError(
                    f"Pathway supports indexing with Pointer type only. The type used was {key_dtype}."
                )
            if not is_optional(key_dtype):
                return _wrap(expression, expression._column.dtype)
            return _wrap(expression, DType(Optional[expression._column.dtype]))
        else:
            if not is_pointer(key_dtype):
                raise TypeError(
                    f"Pathway supports indexing with Pointer type only. The type used was {key_dtype}."
                )
            return _wrap(expression, expression._column.dtype)

    def eval_pointer(
        self,
        expression: expr.PointerExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.PointerExpression:
        expression = super().eval_pointer(expression, state=state, **kwargs)
        arg_types = [arg._dtype for arg in expression._args]
        if expression._optional and any(is_optional(arg) for arg in arg_types):
            return _wrap(expression, DType(Optional[Pointer]))
        else:
            return _wrap(expression, DType(Pointer))

    def eval_cast(
        self,
        expression: expr.CastExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.CastExpression:
        expression = super().eval_cast(expression, state=state, **kwargs)
        return _wrap(expression, expression._return_type)

    def eval_declare(
        self,
        expression: expr.DeclareTypeExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.DeclareTypeExpression:
        expression = super().eval_declare(expression, state=state, **kwargs)
        return _wrap(expression, expression._return_type)

    def eval_coalesce(
        self,
        expression: expr.CoalesceExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.CoalesceExpression:
        expression = super().eval_coalesce(expression, state=state, **kwargs)
        dtypes = [arg._dtype for arg in expression._args]
        ret_type = dtypes[0]
        non_optional_arg = False
        for dt in dtypes:
            ret_type = types_lca(dt, ret_type)
            if not is_optional(dt):
                # FIXME: do we want to be more radical and return now?
                # Maybe with a warning that some args are skipped?
                non_optional_arg = True
        if ret_type is Any and any(dt is not Any for dt in dtypes):
            raise TypeError(
                f"Cannot perform pathway.coalesce on columns of types {dtypes}."
            )
        ret_type = unoptionalize(ret_type) if non_optional_arg else ret_type
        return _wrap(expression, ret_type)

    def eval_require(
        self,
        expression: expr.RequireExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.RequireExpression:
        assert state is not None
        args = [
            self.eval_expression(arg, state=state, **kwargs) for arg in expression._args
        ]
        new_state = state.with_new_col(
            [arg for arg in expression._args if isinstance(arg, expr.ColumnReference)]
        )
        val = self.eval_expression(expression._val, state=new_state, **kwargs)
        expression = expr.RequireExpression(val, *args)
        ret_type = DType(Optional[val._dtype])
        return _wrap(expression, ret_type)

    def eval_not_none(
        self,
        expression: expr.IsNotNoneExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.IsNotNoneExpression:
        ret = super().eval_not_none(expression, state=state, **kwargs)
        return _wrap(ret, DType(bool))

    def eval_none(
        self,
        expression: expr.IsNoneExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.IsNoneExpression:
        ret = super().eval_none(expression, state=state, **kwargs)
        return _wrap(ret, DType(bool))

    def eval_ifelse(
        self,
        expression: expr.IfElseExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.IfElseExpression:
        expression = super().eval_ifelse(expression, state=state, **kwargs)
        if_dtype = expression._if._dtype
        if if_dtype != DType(bool):
            raise TypeError(
                f"First argument of pathway.if_else has to be bool, found {if_dtype}."
            )
        then_dtype = expression._then._dtype
        else_dtype = expression._else._dtype
        lca = types_lca(then_dtype, else_dtype)
        if lca is Any:
            raise TypeError(
                f"Cannot perform pathway.if_else on columns of types {then_dtype} and {else_dtype}."
            )
        return _wrap(expression, lca)

    def eval_make_tuple(
        self,
        expression: expr.MakeTupleExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.MakeTupleExpression:
        expression = super().eval_make_tuple(expression, state=state, **kwargs)
        dtypes = tuple(arg._dtype for arg in expression._args)
        return _wrap(expression, DType(Tuple[dtypes]))

    def eval_sequence_get(
        self,
        expression: expr.SequenceGetExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.SequenceGetExpression:
        expression = super().eval_sequence_get(expression, state=state, **kwargs)
        object_dtype = expression._object._dtype
        index_dtype = expression._index._dtype
        default_dtype = expression._default._dtype

        if is_tuple_like(object_dtype):
            object_dtype = normalize_tuple_like(object_dtype)

        if get_origin(object_dtype) != tuple and object_dtype not in [
            Any,
            np.ndarray,
        ]:
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
            return _wrap(expression, DType(Any))

        dtypes = get_args(object_dtype)

        if object_dtype == Any or len(dtypes) == 0:
            return _wrap(expression, DType(Any))

        if (
            expression._const_index is None
        ):  # no specified position, index is an Expression
            return_dtype = dtypes[0]
            for dtype in dtypes[1:]:
                return_dtype = types_lca(return_dtype, dtype)
            if expression._check_if_exists:
                return_dtype = types_lca(return_dtype, default_dtype)
            return _wrap(expression, return_dtype)

        try:
            return_dtype = dtypes[expression._const_index]
            if return_dtype == Ellipsis:
                raise IndexError
            return _wrap(expression, return_dtype)
        except IndexError:
            if dtypes[-1] == Ellipsis:
                return_dtype = dtypes[-2]
                if expression._check_if_exists:
                    return_dtype = types_lca(return_dtype, default_dtype)
                return _wrap(expression, return_dtype)
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
                    return _wrap(expression, default_dtype)
                else:
                    raise IndexError(message)

    def eval_method_call(
        self,
        expression: expr.MethodCallExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.MethodCallExpression:
        expression = super().eval_method_call(expression, state=state, **kwargs)
        dtypes = tuple([arg._dtype for arg in expression._args])
        if (dtypes_and_handler := expression.get_function(dtypes)) is not None:
            return _wrap(expression, DType(dtypes_and_handler[1]))

        if len(dtypes) > 0:
            with_arguments = f" with arguments of type {dtypes[1:]}"
        else:
            with_arguments = ""
        raise AttributeError(
            f"Column of type {dtypes[0]} has no attribute {expression._name}{with_arguments}."
        )

    def eval_unwrap(
        self,
        expression: expr.UnwrapExpression,
        state: Optional[TypeInterpreterState] = None,
        **kwargs,
    ) -> expr.UnwrapExpression:
        expression = super().eval_unwrap(expression, state=state, **kwargs)
        dtype = expression._expr._dtype
        return _wrap(expression, unoptionalize(dtype))


class JoinTypeInterpreter(TypeInterpreter):
    def __init__(self, left, right, optionalize_left, optionalize_right):
        self.left = left
        self.right = right
        self.optionalize_left = optionalize_left
        self.optionalize_right = optionalize_right
        super().__init__()

    def _eval_column_val(
        self,
        expression: expr.ColumnReference,
        state: Optional[TypeInterpreterState] = None,
    ) -> DType:
        dtype = expression._column.dtype
        assert state is not None
        if (expression.table == self.left and self.optionalize_left) or (
            expression.table == self.right and self.optionalize_right
        ):
            if not state.check_colref_to_unoptionalize_from_tables(expression):
                return DType(Optional[dtype])
        return super()._eval_column_val(expression, state=state)


def eval_type(expression: expr.ColumnExpression) -> DType:
    return (
        TypeInterpreter()
        .eval_expression(expression, state=TypeInterpreterState())
        ._dtype
    )


ColExprT = TypeVar("ColExprT", bound=expr.ColumnExpression)


def _wrap(expression: ColExprT, dtype: DType) -> ColExprT:
    assert not hasattr(expression, "_dtype")
    expression._dtype = dtype
    return expression

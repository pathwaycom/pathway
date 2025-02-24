# Copyright © 2024 Pathway

from __future__ import annotations

import datetime
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from types import EllipsisType
from typing import TYPE_CHECKING, Any, TypeVar

from pathway.internals import dtype as dt, expression as expr
from pathway.internals.expression_printer import get_expression_info
from pathway.internals.expression_visitor import IdentityTransform
from pathway.internals.json import Json
from pathway.internals.operator_mapping import (
    common_dtype_in_binary_operator,
    get_binary_operators_mapping,
    get_binary_operators_mapping_optionals,
    get_unary_operators_mapping,
    tuple_handling_operators,
)

if TYPE_CHECKING:
    from pathway.internals.table import Table


@dataclass(eq=True, frozen=True)
class TypeInterpreterState:
    unoptionalize: Iterable[expr.InternalColRef] = ()

    def with_new_col(
        self, new_cols: Iterable[expr.ColumnReference]
    ) -> TypeInterpreterState:
        return TypeInterpreterState(
            [
                *self.unoptionalize,
                *[col._to_internal() for col in new_cols],
            ],
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
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.ColumnReference:
        expression = super().eval_column_val(expression, state=state, **kwargs)
        return _wrap(expression, self._eval_column_val(expression, state))

    def _eval_column_val(
        self,
        expression: expr.ColumnReference,
        state: TypeInterpreterState | None = None,
    ) -> dt.DType:
        from pathway.internals.table import Table

        assert isinstance(expression._table, Table)
        dtype = expression._column.dtype
        assert state is not None
        assert isinstance(dtype, dt.DType)
        if state.check_colref_to_unoptionalize_from_colrefs(expression):
            return dt.unoptionalize(dtype)
        return dtype

    def eval_unary_op(
        self,
        expression: expr.ColumnUnaryOpExpression,
        state: TypeInterpreterState | None = None,
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
            + f" on column of type {expression._expr._dtype.typehint}.\n"
            + "It refers to the following expression:\n"
            + expression_info
        )

    def eval_binary_op(
        self,
        expression: expr.ColumnBinaryOpExpression,
        state: TypeInterpreterState | None = None,
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
        left_dtype: dt.DType,
        right_dtype: dt.DType,
        operator: Any,
        expression: expr.ColumnExpression,
    ) -> dt.DType:
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
            return dtype

        left_dtype, right_dtype = dt.unoptionalize_pair(left_dtype, right_dtype)

        if (
            dtype_and_handler := get_binary_operators_mapping_optionals(
                operator, left_dtype, right_dtype
            )
        ) is not None:
            return dtype_and_handler[0]

        maybe_dtype = self._eval_binary_op_on_tuples(
            left_dtype, right_dtype, operator, expression
        )
        if maybe_dtype is not None:
            return maybe_dtype

        expression_info = get_expression_info(expression)
        raise TypeError(
            f"Pathway does not support using binary operator {operator.__name__}"
            + f" on columns of types {original_left.typehint}, {original_right.typehint}.\n"
            + "It refers to the following expression:\n"
            + expression_info
        )

    def _eval_binary_op_on_tuples(
        self,
        left_dtype: dt.DType,
        right_dtype: dt.DType,
        operator: Any,
        expression: expr.ColumnExpression,
    ) -> dt.DType | None:
        if (
            isinstance(left_dtype, (dt.Tuple, dt.List))
            and isinstance(right_dtype, (dt.Tuple, dt.List))
            and operator in tuple_handling_operators
        ):
            if left_dtype == dt.ANY_TUPLE or right_dtype == dt.ANY_TUPLE:
                return dt.BOOL
            left_args, right_args = dt.broadcast_tuples(left_dtype, right_dtype)
            if (
                isinstance(left_args, tuple)
                and isinstance(right_args, tuple)
                and len(left_args) == len(right_args)
            ):
                results = []
                is_valid = True
                for left_arg, right_arg in zip(left_args, right_args):
                    if not isinstance(left_arg, EllipsisType) and not isinstance(
                        right_arg, EllipsisType
                    ):
                        try:
                            results.append(
                                self._eval_binary_op(
                                    left_arg, right_arg, operator, expression
                                )
                            )
                        except TypeError:
                            is_valid = False
                            break
                if is_valid:
                    assert all(result == results[0] for result in results)
                    return dt.wrap(results[0])
        return None

    def eval_const(
        self,
        expression: expr.ColumnConstExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.ColumnConstExpression:
        expression = super().eval_const(expression, state=state, **kwargs)
        type_ = type(expression._val)
        if isinstance(expression._val, datetime.datetime):
            if expression._val.tzinfo is None:
                dtype: dt.DType = dt.DATE_TIME_NAIVE
            else:
                dtype = dt.DATE_TIME_UTC
        elif isinstance(expression._val, datetime.timedelta):
            dtype = dt.DURATION
        else:
            dtype = dt.wrap(type_)
        return _wrap(expression, dtype)

    def eval_reducer(
        self,
        expression: expr.ReducerExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.ReducerExpression:
        expression = super().eval_reducer(expression, state=state, **kwargs)
        args_dtypes = [e._dtype for e in expression._args]
        kwargs_dtypes = [e._dtype for e in expression._kwargs.values()]
        self._check_for_disallowed_types(
            f"pathway.reducers.{expression._reducer.name}", *args_dtypes, *kwargs_dtypes
        )
        return _wrap(
            expression,
            expression._reducer.return_type(
                [arg._dtype for arg in expression._args], self._pointer_type()
            ),
        )

    def _pointer_type(self):
        return dt.ANY_POINTER

    def eval_apply(
        self,
        expression: expr.ApplyExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.ApplyExpression:
        expression = super().eval_apply(expression, state=state, **kwargs)
        args_dtypes = [e._dtype for e in expression._args]
        kwargs_dtypes = [e._dtype for e in expression._kwargs.values()]
        if expression._check_for_disallowed_types:
            self._check_for_disallowed_types(
                "pathway.apply", *args_dtypes, *kwargs_dtypes
            )
        return _wrap(expression, expression._maybe_optional_return_type)

    def eval_async_apply(
        self,
        expression: expr.AsyncApplyExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.AsyncApplyExpression:
        expression = super().eval_async_apply(expression, state=state, **kwargs)
        args_dtypes = [e._dtype for e in expression._args]
        kwargs_dtypes = [e._dtype for e in expression._kwargs.values()]
        self._check_for_disallowed_types(
            "pathway.apply_async", *args_dtypes, *kwargs_dtypes
        )
        return _wrap(expression, expression._maybe_optional_return_type)

    def eval_fully_async_apply(
        self,
        expression: expr.FullyAsyncApplyExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.FullyAsyncApplyExpression:
        expression = super().eval_fully_async_apply(expression, state=state, **kwargs)
        return _wrap(expression, dt.Future(expression._maybe_optional_return_type))

    def eval_call(
        self,
        expression: expr.ColumnCallExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.ColumnCallExpression:
        dtype = expression._col_expr._column.dtype
        assert isinstance(dtype, dt.Callable)
        expression = super().eval_call(expression, state=state, **kwargs)
        arg_annots, ret_type = dtype.arg_types, dtype.return_type
        if not isinstance(arg_annots, EllipsisType):
            arg_annots = arg_annots[1:]  # ignoring self
            for arg_annot, arg in zip(arg_annots, expression._args, strict=True):
                arg_dtype = arg._dtype
                assert not isinstance(arg_annot, EllipsisType)
                assert dt.dtype_issubclass(arg_dtype, arg_annot)
        return _wrap(expression, ret_type)

    def eval_pointer(
        self,
        expression: expr.PointerExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.PointerExpression:
        expression = super().eval_pointer(expression, state=state, **kwargs)
        arg_types = [arg._dtype for arg in expression._args]
        if expression._instance is not None:
            arg_types.append(expression._instance._dtype)
        self._check_for_disallowed_types("pathway.pointer_from", *arg_types)
        if expression._optional and any(
            isinstance(arg, dt.Optional) or arg == dt.ANY for arg in arg_types
        ):
            return _wrap(
                expression,
                dt.Optional(dt.Pointer(*[dt.unoptionalize(arg) for arg in arg_types])),
            )
        else:
            return _wrap(expression, dt.Pointer(*arg_types))

    def eval_cast(
        self,
        expression: expr.CastExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.CastExpression:
        expression = super().eval_cast(expression, state=state, **kwargs)
        return _wrap(expression, expression._return_type)

    def eval_convert(
        self,
        expression: expr.ConvertExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.ConvertExpression:
        expression = super().eval_convert(expression, state=state, **kwargs)
        target_type = expression._return_type
        default_type = expression._default._dtype

        if dt.unoptionalize(expression._expr._dtype) is not dt.JSON:
            raise TypeError(
                f"{expression!r} can only be applied to JSON columns, "
                f"but column has type {expression._expr._dtype.typehint}."
            )

        if default_type != dt.NONE and not dt.dtype_issubclass(
            dt.unoptionalize(default_type), target_type
        ):
            raise TypeError(
                f"type of default {default_type.typehint} "
                f"is not compatible with {target_type.typehint}."
            )

        if not expression._unwrap and (
            isinstance(default_type, dt.Optional) or default_type == dt.NONE
        ):
            target_type = dt.Optional(target_type)

        return _wrap(expression, target_type)

    def eval_declare(
        self,
        expression: expr.DeclareTypeExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.DeclareTypeExpression:
        expression = super().eval_declare(expression, state=state, **kwargs)
        left = expression._return_type
        right = expression._expr._dtype
        if not (dt.dtype_issubclass(left, right) or dt.dtype_issubclass(right, left)):
            raise TypeError(
                f"Cannot change type from {right} to {left}.\n"
                + "pw.`declare_type` should be used only for type narrowing or type extending."
            )
        return _wrap(expression, expression._return_type)

    def eval_coalesce(
        self,
        expression: expr.CoalesceExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.CoalesceExpression:
        expression = super().eval_coalesce(expression, state=state, **kwargs)
        dtypes = [arg._dtype for arg in expression._args]
        self._check_for_disallowed_types("pathway.coalesce", *dtypes)
        ret_type = dtypes[0]
        non_optional_arg = False
        for dtype in dtypes:
            try:
                ret_type = dt.types_lca(dtype, ret_type, raising=True)
            except TypeError:
                raise TypeError(
                    "Incompatible types in for a coalesce expression.\n"
                    + f"The types are: {dtypes}. "
                    + "You might try casting the expressions to Any type to circumvent this, "
                    + "but this is most probably an error."
                )
            if not isinstance(dtype, dt.Optional):
                # FIXME: do we want to be more radical and return now?
                # Maybe with a warning that some args are skipped?
                non_optional_arg = True
        if ret_type is dt.ANY and any(dtype is not dt.ANY for dtype in dtypes):
            raise TypeError(
                f"Cannot perform pathway.coalesce on columns of types {[dtype.typehint for dtype in dtypes]}."
            )
        ret_type = dt.unoptionalize(ret_type) if non_optional_arg else ret_type
        return _wrap(expression, ret_type)

    def eval_require(
        self,
        expression: expr.RequireExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.RequireExpression:
        assert state is not None
        args = [
            self.eval_expression(arg, state=state, **kwargs) for arg in expression._args
        ]
        arg_dtypes = [arg._dtype for arg in args]
        new_state = state.with_new_col(
            [arg for arg in expression._args if isinstance(arg, expr.ColumnReference)]
        )
        val = self.eval_expression(expression._val, state=new_state, **kwargs)
        self._check_for_disallowed_types("pathway.require", val._dtype, *arg_dtypes)
        expression = expr.RequireExpression(val, *args)
        ret_type = dt.Optional(val._dtype)
        return _wrap(expression, ret_type)

    def eval_not_none(
        self,
        expression: expr.IsNotNoneExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.IsNotNoneExpression:
        ret = super().eval_not_none(expression, state=state, **kwargs)
        self._check_for_disallowed_types("pathway.is_not_none", ret._expr._dtype)
        return _wrap(ret, dt.BOOL)

    def eval_none(
        self,
        expression: expr.IsNoneExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.IsNoneExpression:
        ret = super().eval_none(expression, state=state, **kwargs)
        self._check_for_disallowed_types("pathway.is_none", ret._expr._dtype)
        return _wrap(ret, dt.BOOL)

    def eval_ifelse(
        self,
        expression: expr.IfElseExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.IfElseExpression:
        assert state is not None
        if_ = self.eval_expression(expression._if, state=state, **kwargs)
        if_dtype = if_._dtype
        if if_dtype != dt.BOOL:
            raise TypeError(
                f"First argument of pathway.if_else has to be bool, found {if_dtype.typehint}."
            )

        if isinstance(if_, expr.IsNotNoneExpression) and isinstance(
            if_._expr, expr.ColumnReference
        ):
            then_ = self.eval_expression(
                expression._then, state=state.with_new_col([if_._expr]), **kwargs
            )
        else:
            then_ = self.eval_expression(expression._then, state=state, **kwargs)

        if isinstance(if_, expr.IsNoneExpression) and isinstance(
            if_._expr, expr.ColumnReference
        ):
            else_ = self.eval_expression(
                expression._else, state=state.with_new_col([if_._expr], **kwargs)
            )
        else:
            else_ = self.eval_expression(expression._else, state=state, **kwargs)

        then_dtype = then_._dtype
        else_dtype = else_._dtype
        try:
            lca = dt.types_lca(then_dtype, else_dtype, raising=True)
        except TypeError:
            raise TypeError(
                f"Cannot perform pathway.if_else on columns of types {then_dtype.typehint} and {else_dtype.typehint}."
            )
        if lca is dt.ANY:
            raise TypeError(
                f"Cannot perform pathway.if_else on columns of types {then_dtype.typehint} and {else_dtype.typehint}."
            )
        expression = expr.IfElseExpression(if_, then_, else_)
        return _wrap(expression, lca)

    def eval_make_tuple(
        self,
        expression: expr.MakeTupleExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.MakeTupleExpression:
        expression = super().eval_make_tuple(expression, state=state, **kwargs)
        dtypes = tuple(arg._dtype for arg in expression._args)
        self._check_for_disallowed_types("pathway.make_tuple", *dtypes)
        return _wrap(expression, dt.Tuple(*dtypes))

    def eval_get(
        self,
        expression: expr.GetExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.GetExpression:
        expression = super().eval_get(expression, state=state, **kwargs)
        object_dtype = expression._object._dtype
        index_dtype = expression._index._dtype
        default_dtype = expression._default._dtype

        if object_dtype == dt.JSON:
            # json
            if not dt.dtype_issubclass(default_dtype, dt.Optional(dt.JSON)):
                raise TypeError(
                    f"Default must be of type {Json | None}, found {default_dtype.typehint}."
                )
            if not expression._check_if_exists or default_dtype == dt.JSON:
                return _wrap(expression, dt.JSON)
            else:
                return _wrap(expression, dt.Optional(dt.JSON))
        elif object_dtype.equivalent_to(dt.Optional(dt.JSON)):
            # optional json
            raise TypeError(f"Cannot get from {Json | None}.")
        else:
            # sequence
            if (
                not isinstance(object_dtype, (dt.Array, dt.Tuple, dt.List))
                and object_dtype != dt.ANY
            ):
                raise TypeError(
                    f"Object in {expression!r} has to be a JSON or sequence."
                )
            if index_dtype != dt.INT:
                raise TypeError(f"Index in {expression!r} has to be an int.")

            if isinstance(object_dtype, dt.Array):
                return _wrap(expression, object_dtype.strip_dimension())
            if object_dtype == dt.ANY:
                return _wrap(expression, dt.ANY)

            if isinstance(object_dtype, dt.List):
                if expression._check_if_exists:
                    return _wrap(expression, dt.Optional(object_dtype.wrapped))
                else:
                    return _wrap(expression, object_dtype.wrapped)
            assert isinstance(object_dtype, dt.Tuple)
            if object_dtype == dt.ANY_TUPLE:
                return _wrap(expression, dt.ANY)

            assert not isinstance(object_dtype.args, EllipsisType)
            dtypes = object_dtype.args

            if (
                expression._const_index is None
            ):  # no specified position, index is an Expression
                assert isinstance(dtypes[0], dt.DType)
                return_dtype = dtypes[0]
                for dtype in dtypes[1:]:
                    if isinstance(dtype, dt.DType):
                        return_dtype = dt.types_lca(return_dtype, dtype, raising=False)
                if expression._check_if_exists:
                    return_dtype = dt.types_lca(
                        return_dtype, default_dtype, raising=False
                    )
                return _wrap(expression, return_dtype)

            if not isinstance(expression._const_index, int):
                raise IndexError("Index n")

            try:
                try_ret = dtypes[expression._const_index]
                return _wrap(expression, try_ret)
            except IndexError:
                message = (
                    f"Index {expression._const_index} out of range for a tuple of"
                    + f" type {object_dtype.typehint}."
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
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.MethodCallExpression:
        expression = super().eval_method_call(expression, state=state, **kwargs)
        dtypes = tuple([arg._dtype for arg in expression._args])
        if (dtypes_and_handler := expression.get_function(dtypes)) is not None:
            return _wrap(expression, dt.wrap(dtypes_and_handler[1]))

        if len(dtypes) > 0:
            with_arguments = (
                f" with arguments of type {[dtype.typehint for dtype in dtypes[1:]]}"
            )
        else:
            with_arguments = ""
        raise AttributeError(
            f"Column of type {dtypes[0].typehint} has no attribute {expression._name}{with_arguments}."
        )

    def eval_unwrap(
        self,
        expression: expr.UnwrapExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.UnwrapExpression:
        expression = super().eval_unwrap(expression, state=state, **kwargs)
        dtype = expression._expr._dtype
        self._check_for_disallowed_types("pathway.unwrap", dtype)
        return _wrap(expression, dt.unoptionalize(dtype))

    def eval_fill_error(
        self,
        expression: expr.FillErrorExpression,
        state: TypeInterpreterState | None = None,
        **kwargs,
    ) -> expr.FillErrorExpression:
        expression = super().eval_fill_error(expression, state=state, **kwargs)
        inner_dtype = expression._expr._dtype
        replacement_dtype = expression._replacement._dtype
        lca = dt.types_lca(inner_dtype, replacement_dtype, raising=False)
        if lca is dt.ANY and inner_dtype is not dt.ANY:
            raise TypeError(
                "Cannot perform pathway.fill_error on columns of types"
                + f" {inner_dtype.typehint} and {replacement_dtype.typehint}."
            )
        return _wrap(expression, lca)

    def _check_for_disallowed_types(self, name: str, *dtypes: dt.DType) -> None:
        disallowed_dtypes: list[dt.DType] = []
        for dtype in dtypes:
            if isinstance(dtype, dt.Future):
                disallowed_dtypes.append(dtype)
        if disallowed_dtypes:
            dtypes_repr = ", ".join(f"{dtype.typehint}" for dtype in disallowed_dtypes)
            # adjust message if more than dt.Future is involved
            raise TypeError(
                f"Cannot perform {name} when column of type {dtypes_repr} is involved."
                + " Consider applying `await_futures()` to the table used here."
            )


class ReducerInterprerer(TypeInterpreter):
    id_column_type: dt.DType

    def __init__(self, id_column_type):
        self.id_column_type = id_column_type
        super().__init__()

    def _pointer_type(self):
        return self.id_column_type


class JoinTypeInterpreter(TypeInterpreter):
    """This type interpreter is used by JoinContext.
    It evaluates only column references, and is used to decide which columns
    to optionalize when unrolling left and right table columns to internal table columns.
    """

    left: Table
    right: Table
    optionalize_left: bool
    optionalize_right: bool

    def __init__(self, left, right, optionalize_left, optionalize_right):
        self.left = left
        self.right = right
        self.optionalize_left = optionalize_left
        self.optionalize_right = optionalize_right
        super().__init__()

    def _eval_column_val(
        self,
        expression: expr.ColumnReference,
        state: TypeInterpreterState | None = None,
    ) -> dt.DType:
        dtype = expression._column.dtype
        assert state is not None
        if (expression.table == self.left and self.optionalize_left) or (
            expression.table == self.right and self.optionalize_right
        ):
            return dt.Optional(dtype)
        return super()._eval_column_val(expression, state=state)


class JoinRowwiseTypeInterpreter(TypeInterpreter):
    """Type interpreter for evaluating expressions in join.
    Colrefs are already properly optionalized (depending on type of join and
    left/right table) and properly unrolled and stored in internal table."""

    temporary_column_to_original: dict[expr.InternalColRef, expr.InternalColRef]
    original_column_to_temporary: dict[expr.InternalColRef, expr.ColumnReference]

    def __init__(
        self,
        temporary_column_to_original: dict[expr.InternalColRef, expr.InternalColRef],
        original_column_to_temporary: dict[expr.InternalColRef, expr.ColumnReference],
    ) -> None:
        self.temporary_column_to_original = temporary_column_to_original
        self.original_column_to_temporary = original_column_to_temporary
        super().__init__()

    def _eval_column_val(
        self,
        expression: expr.ColumnReference,
        state: TypeInterpreterState | None = None,
    ) -> dt.DType:
        assert state is not None
        if expression._to_internal() in self.temporary_column_to_original:
            # if clause needed because of external columns from ix() that are not in
            # self.temporary_column_to_original
            original = self.temporary_column_to_original[expression._to_internal()]
            tmp_id_colref = self.original_column_to_temporary[
                original._table.id._to_internal()
            ]
            if state.check_colref_to_unoptionalize_from_colrefs(tmp_id_colref):
                return original._column.dtype
        return super()._eval_column_val(expression, state=state)


def eval_type(expression: expr.ColumnExpression) -> dt.DType:
    return (
        TypeInterpreter()
        .eval_expression(expression, state=TypeInterpreterState())
        ._dtype
    )


ColExprT = TypeVar("ColExprT", bound=expr.ColumnExpression)


def _wrap(expression: ColExprT, dtype: dt.DType) -> ColExprT:
    assert not hasattr(expression, "_dtype")
    assert isinstance(dtype, dt.DType)
    expression._dtype = dtype
    return expression

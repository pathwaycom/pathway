# Copyright © 2024 Pathway

from collections.abc import Callable, Mapping
from typing import Any

from pathway.internals import api, dtype as dt
from pathway.internals.shadows import operator

UnaryOperator = Callable[[Any], Any]
ApiUnaryOperator = Callable[[api.Expression], api.Expression]
UnaryOperatorMapping = Mapping[
    tuple[UnaryOperator, dt.DType],
    dt.DType,
]

BinaryOperator = Callable[[Any, Any], Any]
ApiBinaryOperator = Callable[[api.Expression, api.Expression], api.Expression]
BinaryOperatorMapping = Mapping[
    tuple[BinaryOperator, dt.DType, dt.DType],
    dt.DType,
]

OptionalMapping = Mapping[
    BinaryOperator,
    tuple[dt.DType, ApiBinaryOperator],
]

_unary_operators_to_engine: Mapping[UnaryOperator, api.UnaryOperator] = {
    operator.inv: api.UnaryOperator.INV,
    operator.neg: api.UnaryOperator.NEG,
}

_binary_operators_to_engine: Mapping[BinaryOperator, api.BinaryOperator] = {
    operator.and_: api.BinaryOperator.AND,
    operator.or_: api.BinaryOperator.OR,
    operator.xor: api.BinaryOperator.XOR,
    operator.eq: api.BinaryOperator.EQ,
    operator.ne: api.BinaryOperator.NE,
    operator.lt: api.BinaryOperator.LT,
    operator.le: api.BinaryOperator.LE,
    operator.gt: api.BinaryOperator.GT,
    operator.ge: api.BinaryOperator.GE,
    operator.add: api.BinaryOperator.ADD,
    operator.sub: api.BinaryOperator.SUB,
    operator.mul: api.BinaryOperator.MUL,
    operator.floordiv: api.BinaryOperator.FLOOR_DIV,
    operator.truediv: api.BinaryOperator.TRUE_DIV,
    operator.mod: api.BinaryOperator.MOD,
    operator.pow: api.BinaryOperator.POW,
    operator.lshift: api.BinaryOperator.LSHIFT,
    operator.rshift: api.BinaryOperator.RSHIFT,
    operator.matmul: api.BinaryOperator.MATMUL,
}

_unary_operators_mapping: UnaryOperatorMapping = {
    (operator.inv, dt.BOOL): dt.BOOL,
    (operator.neg, dt.INT): dt.INT,
    (operator.neg, dt.FLOAT): dt.FLOAT,
    (operator.neg, dt.DURATION): dt.DURATION,
}


def get_unary_operators_mapping(op, operand_dtype, default=None):
    return _unary_operators_mapping.get((op, operand_dtype), default)


def get_unary_expression(expr, op, expr_dtype: dt.DType, default=None):
    op_engine = _unary_operators_to_engine.get(op)
    if op_engine is None:
        return default
    expr_dtype_engine = expr_dtype.to_engine()
    expression = api.Expression.unary_expression(expr, op_engine, expr_dtype_engine)
    return expression if expression is not None else default


_binary_operators_mapping: BinaryOperatorMapping = {
    (operator.and_, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.or_, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.xor, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.eq, dt.INT, dt.INT): dt.BOOL,
    (operator.ne, dt.INT, dt.INT): dt.BOOL,
    (operator.lt, dt.INT, dt.INT): dt.BOOL,
    (operator.le, dt.INT, dt.INT): dt.BOOL,
    (operator.gt, dt.INT, dt.INT): dt.BOOL,
    (operator.ge, dt.INT, dt.INT): dt.BOOL,
    (operator.eq, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.ne, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.lt, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.le, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.gt, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.ge, dt.BOOL, dt.BOOL): dt.BOOL,
    (operator.add, dt.INT, dt.INT): dt.INT,
    (operator.sub, dt.INT, dt.INT): dt.INT,
    (operator.mul, dt.INT, dt.INT): dt.INT,
    (operator.floordiv, dt.INT, dt.INT): dt.INT,
    (operator.truediv, dt.INT, dt.INT): dt.FLOAT,
    (operator.mod, dt.INT, dt.INT): dt.INT,
    (operator.pow, dt.INT, dt.INT): dt.INT,
    (operator.lshift, dt.INT, dt.INT): dt.INT,
    (operator.rshift, dt.INT, dt.INT): dt.INT,
    (operator.and_, dt.INT, dt.INT): dt.INT,
    (operator.or_, dt.INT, dt.INT): dt.INT,
    (operator.xor, dt.INT, dt.INT): dt.INT,
    (operator.eq, dt.FLOAT, dt.FLOAT): dt.BOOL,
    (operator.ne, dt.FLOAT, dt.FLOAT): dt.BOOL,
    (operator.lt, dt.FLOAT, dt.FLOAT): dt.BOOL,
    (operator.le, dt.FLOAT, dt.FLOAT): dt.BOOL,
    (operator.gt, dt.FLOAT, dt.FLOAT): dt.BOOL,
    (operator.ge, dt.FLOAT, dt.FLOAT): dt.BOOL,
    (operator.add, dt.FLOAT, dt.FLOAT): dt.FLOAT,
    (operator.sub, dt.FLOAT, dt.FLOAT): dt.FLOAT,
    (operator.mul, dt.FLOAT, dt.FLOAT): dt.FLOAT,
    (operator.floordiv, dt.FLOAT, dt.FLOAT): dt.FLOAT,
    (operator.truediv, dt.FLOAT, dt.FLOAT): dt.FLOAT,
    (operator.mod, dt.FLOAT, dt.FLOAT): dt.FLOAT,
    (operator.pow, dt.FLOAT, dt.FLOAT): dt.FLOAT,
    (operator.eq, dt.STR, dt.STR): dt.BOOL,
    (operator.ne, dt.STR, dt.STR): dt.BOOL,
    (operator.lt, dt.STR, dt.STR): dt.BOOL,
    (operator.le, dt.STR, dt.STR): dt.BOOL,
    (operator.gt, dt.STR, dt.STR): dt.BOOL,
    (operator.ge, dt.STR, dt.STR): dt.BOOL,
    (operator.add, dt.STR, dt.STR): dt.STR,
    (operator.mul, dt.STR, dt.INT): dt.STR,
    (operator.mul, dt.INT, dt.STR): dt.STR,
    (operator.eq, dt.ANY_POINTER, dt.ANY_POINTER): dt.BOOL,
    (operator.ne, dt.ANY_POINTER, dt.ANY_POINTER): dt.BOOL,
    (operator.lt, dt.ANY_POINTER, dt.ANY_POINTER): dt.BOOL,
    (operator.le, dt.ANY_POINTER, dt.ANY_POINTER): dt.BOOL,
    (operator.gt, dt.ANY_POINTER, dt.ANY_POINTER): dt.BOOL,
    (operator.ge, dt.ANY_POINTER, dt.ANY_POINTER): dt.BOOL,
    (operator.eq, dt.DATE_TIME_NAIVE, dt.DATE_TIME_NAIVE): dt.BOOL,
    (operator.ne, dt.DATE_TIME_NAIVE, dt.DATE_TIME_NAIVE): dt.BOOL,
    (operator.lt, dt.DATE_TIME_NAIVE, dt.DATE_TIME_NAIVE): dt.BOOL,
    (operator.le, dt.DATE_TIME_NAIVE, dt.DATE_TIME_NAIVE): dt.BOOL,
    (operator.gt, dt.DATE_TIME_NAIVE, dt.DATE_TIME_NAIVE): dt.BOOL,
    (operator.ge, dt.DATE_TIME_NAIVE, dt.DATE_TIME_NAIVE): dt.BOOL,
    (operator.sub, dt.DATE_TIME_NAIVE, dt.DATE_TIME_NAIVE): dt.DURATION,
    (operator.add, dt.DATE_TIME_NAIVE, dt.DURATION): dt.DATE_TIME_NAIVE,
    (operator.sub, dt.DATE_TIME_NAIVE, dt.DURATION): dt.DATE_TIME_NAIVE,
    (operator.eq, dt.DATE_TIME_UTC, dt.DATE_TIME_UTC): dt.BOOL,
    (operator.ne, dt.DATE_TIME_UTC, dt.DATE_TIME_UTC): dt.BOOL,
    (operator.lt, dt.DATE_TIME_UTC, dt.DATE_TIME_UTC): dt.BOOL,
    (operator.le, dt.DATE_TIME_UTC, dt.DATE_TIME_UTC): dt.BOOL,
    (operator.gt, dt.DATE_TIME_UTC, dt.DATE_TIME_UTC): dt.BOOL,
    (operator.ge, dt.DATE_TIME_UTC, dt.DATE_TIME_UTC): dt.BOOL,
    (operator.sub, dt.DATE_TIME_UTC, dt.DATE_TIME_UTC): dt.DURATION,
    (operator.add, dt.DATE_TIME_UTC, dt.DURATION): dt.DATE_TIME_UTC,
    (operator.sub, dt.DATE_TIME_UTC, dt.DURATION): dt.DATE_TIME_UTC,
    (operator.eq, dt.DURATION, dt.DURATION): dt.BOOL,
    (operator.ne, dt.DURATION, dt.DURATION): dt.BOOL,
    (operator.lt, dt.DURATION, dt.DURATION): dt.BOOL,
    (operator.le, dt.DURATION, dt.DURATION): dt.BOOL,
    (operator.gt, dt.DURATION, dt.DURATION): dt.BOOL,
    (operator.ge, dt.DURATION, dt.DURATION): dt.BOOL,
    (operator.add, dt.DURATION, dt.DURATION): dt.DURATION,
    (operator.sub, dt.DURATION, dt.DURATION): dt.DURATION,
    (operator.floordiv, dt.DURATION, dt.DURATION): dt.INT,
    (operator.truediv, dt.DURATION, dt.DURATION): dt.FLOAT,
    (operator.mod, dt.DURATION, dt.DURATION): dt.DURATION,
    (operator.add, dt.DURATION, dt.DATE_TIME_NAIVE): dt.DATE_TIME_NAIVE,
    (operator.add, dt.DURATION, dt.DATE_TIME_UTC): dt.DATE_TIME_UTC,
    (operator.mul, dt.DURATION, dt.INT): dt.DURATION,
    (operator.mul, dt.INT, dt.DURATION): dt.DURATION,
    (operator.floordiv, dt.DURATION, dt.INT): dt.DURATION,
    (operator.truediv, dt.DURATION, dt.INT): dt.DURATION,
    (operator.mul, dt.DURATION, dt.FLOAT): dt.DURATION,
    (operator.mul, dt.FLOAT, dt.DURATION): dt.DURATION,
    (operator.truediv, dt.DURATION, dt.FLOAT): dt.DURATION,
    (operator.matmul, dt.ANY_ARRAY_2D, dt.ANY_ARRAY_2D): dt.ANY_ARRAY_2D,
    (operator.matmul, dt.INT_ARRAY_2D, dt.INT_ARRAY_2D): dt.INT_ARRAY_2D,
    (operator.matmul, dt.FLOAT_ARRAY_2D, dt.FLOAT_ARRAY_2D): dt.FLOAT_ARRAY_2D,
    (operator.matmul, dt.ANY_ARRAY_2D, dt.ANY_ARRAY_1D): dt.ANY_ARRAY_1D,
    (operator.matmul, dt.INT_ARRAY_2D, dt.INT_ARRAY_1D): dt.INT_ARRAY_1D,
    (operator.matmul, dt.FLOAT_ARRAY_2D, dt.FLOAT_ARRAY_1D): dt.FLOAT_ARRAY_1D,
    (operator.matmul, dt.ANY_ARRAY_1D, dt.ANY_ARRAY_2D): dt.ANY_ARRAY_1D,
    (operator.matmul, dt.INT_ARRAY_1D, dt.INT_ARRAY_2D): dt.INT_ARRAY_1D,
    (operator.matmul, dt.FLOAT_ARRAY_1D, dt.FLOAT_ARRAY_2D): dt.FLOAT_ARRAY_1D,
    (operator.matmul, dt.ANY_ARRAY_1D, dt.ANY_ARRAY_1D): dt.ANY,
    (operator.matmul, dt.INT_ARRAY_1D, dt.INT_ARRAY_1D): dt.INT,
    (operator.matmul, dt.FLOAT_ARRAY_1D, dt.FLOAT_ARRAY_1D): dt.FLOAT,
    (operator.matmul, dt.ANY_ARRAY, dt.ANY_ARRAY): dt.ANY_ARRAY,
    (operator.matmul, dt.INT_ARRAY, dt.INT_ARRAY): dt.INT_ARRAY,
    (operator.matmul, dt.FLOAT_ARRAY, dt.FLOAT_ARRAY): dt.FLOAT_ARRAY,
}

tuple_handling_operators = {
    operator.eq,
    operator.ne,
    operator.le,
    operator.lt,
    operator.ge,
    operator.gt,
}


def get_binary_operators_mapping(op, left, right):
    if isinstance(left, dt.Array) and isinstance(right, dt.Array):
        left, right = dt.coerce_arrays_pair(left, right)
    if isinstance(left, dt.Pointer) and isinstance(right, dt.Pointer):
        try:
            dt.types_lca(left, right, raising=True)
        except TypeError:
            raise TypeError(
                "Incompatible types in for a binary operator.\n"
                + f"The types are: {left} and {right}. "
                + "You might try casting the expressions to Any type to circumvent this,"
                + " but this is most probably an error."
            )
    return _binary_operators_mapping.get(
        (op, dt.normalize_dtype(left), dt.normalize_dtype(right))
    )


def get_binary_expression(
    left, right, op, left_dtype: dt.DType, right_dtype: dt.DType, default=None
):
    op_engine = _binary_operators_to_engine.get(op)
    left_dtype_engine = left_dtype.to_engine()
    right_dtype_engine = right_dtype.to_engine()
    if op_engine is None:
        return default

    expression = api.Expression.binary_expression(
        left, right, op_engine, left_dtype_engine, right_dtype_engine
    )
    return expression if expression is not None else default


_binary_operators_mapping_optionals: OptionalMapping = {
    operator.eq: (dt.BOOL, api.Expression.eq),
    operator.ne: (dt.BOOL, api.Expression.ne),
}


def get_binary_operators_mapping_optionals(op, left, right, default=None):
    if (
        left == right
        or left == dt.NONE
        or right == dt.NONE
        or (isinstance(left, dt.Pointer) and isinstance(right, dt.Pointer))
    ):
        return _binary_operators_mapping_optionals.get(op, default)
    else:
        return default


def get_cast_operators_mapping(
    expr: api.Expression, source_type: dt.DType, target_type: dt.DType, default=None
) -> api.Expression | None:
    source_type_engine = dt.unoptionalize(source_type).to_engine()
    target_type_engine = dt.unoptionalize(target_type).to_engine()

    if isinstance(source_type, dt.Optional) and isinstance(target_type, dt.Optional):
        fun = api.Expression.cast_optional
    else:
        fun = api.Expression.cast
    expression = fun(
        expr,
        source_type_engine,
        target_type_engine,
    )
    return expression if expression is not None else default


def get_convert_operator(
    expr: api.Expression,
    default: api.Expression,
    source_type: dt.DType,
    target_type: dt.DType,
    unwrap: bool,
) -> api.Expression | None:
    source_type_engine = dt.unoptionalize(source_type).to_engine()
    target_type_engine = dt.unoptionalize(target_type).to_engine()

    return api.Expression.convert(
        expr, default, source_type_engine, target_type_engine, unwrap
    )


def common_dtype_in_binary_operator(
    left_dtype: dt.DType, right_dtype: dt.DType
) -> dt.DType | None:
    if (
        left_dtype in [dt.INT, dt.Optional(dt.INT)]
        and right_dtype in [dt.FLOAT, dt.Optional(dt.FLOAT)]
    ) or (
        left_dtype in [dt.FLOAT, dt.Optional(dt.FLOAT)]
        and right_dtype in [dt.INT, dt.Optional(dt.INT)]
    ):
        return dt.types_lca(left_dtype, right_dtype, raising=False)
    return None

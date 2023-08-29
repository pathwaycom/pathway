from typing import Any, Callable, Mapping, Optional, Tuple

import numpy as np

from pathway.internals import api
from pathway.internals.api import _TYPES_TO_ENGINE_MAPPING, Pointer
from pathway.internals.datetime_types import DateTimeNaive, DateTimeUtc, Duration
from pathway.internals.dtype import (
    DType,
    NoneType,
    is_optional,
    types_lca,
    unoptionalize,
)
from pathway.internals.shadows import operator

UnaryOperator = Callable[[Any], Any]
ApiUnaryOperator = Callable[[api.Expression], api.Expression]
UnaryOperatorMapping = Mapping[
    Tuple[UnaryOperator, Any],
    type,
]

BinaryOperator = Callable[[Any, Any], Any]
ApiBinaryOperator = Callable[[api.Expression, api.Expression], api.Expression]
BinaryOperatorMapping = Mapping[
    Tuple[BinaryOperator, Any, Any],
    type,
]

OptionalMapping = Mapping[
    BinaryOperator,
    Tuple[type, ApiBinaryOperator],
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
    (operator.inv, bool): bool,
    (operator.neg, int): int,
    (operator.neg, float): float,
    (operator.neg, Duration): Duration,
}


def get_unary_operators_mapping(op, operand_dtype, default=None):
    return _unary_operators_mapping.get((op, operand_dtype), default)


def get_unary_expression(expr, op, expr_dtype, default=None):
    op_engine = _unary_operators_to_engine.get(op)
    expr_dtype_engine = _TYPES_TO_ENGINE_MAPPING.get(expr_dtype)
    if op_engine is None or expr_dtype_engine is None:
        return default
    expression = api.Expression.unary_expression(expr, op_engine, expr_dtype_engine)
    return expression if expression is not None else default


_binary_operators_mapping: BinaryOperatorMapping = {
    (operator.and_, bool, bool): bool,
    (operator.or_, bool, bool): bool,
    (operator.xor, bool, bool): bool,
    (operator.eq, int, int): bool,
    (operator.ne, int, int): bool,
    (operator.lt, int, int): bool,
    (operator.le, int, int): bool,
    (operator.gt, int, int): bool,
    (operator.ge, int, int): bool,
    (operator.eq, bool, bool): bool,
    (operator.ne, bool, bool): bool,
    (operator.lt, bool, bool): bool,
    (operator.le, bool, bool): bool,
    (operator.gt, bool, bool): bool,
    (operator.ge, bool, bool): bool,
    (operator.add, int, int): int,
    (operator.sub, int, int): int,
    (operator.mul, int, int): int,
    (operator.floordiv, int, int): int,
    (operator.truediv, int, int): float,
    (operator.mod, int, int): int,
    (operator.pow, int, int): int,
    (operator.lshift, int, int): int,
    (operator.rshift, int, int): int,
    (operator.and_, int, int): int,
    (operator.or_, int, int): int,
    (operator.xor, int, int): int,
    (operator.eq, float, float): bool,
    (operator.ne, float, float): bool,
    (operator.lt, float, float): bool,
    (operator.le, float, float): bool,
    (operator.gt, float, float): bool,
    (operator.ge, float, float): bool,
    (operator.add, float, float): float,
    (operator.sub, float, float): float,
    (operator.mul, float, float): float,
    (operator.floordiv, float, float): float,
    (operator.truediv, float, float): float,
    (operator.mod, float, float): float,
    (operator.pow, float, float): float,
    (operator.eq, str, str): bool,
    (operator.ne, str, str): bool,
    (operator.lt, str, str): bool,
    (operator.le, str, str): bool,
    (operator.gt, str, str): bool,
    (operator.ge, str, str): bool,
    (operator.add, str, str): str,
    (operator.mul, str, int): str,
    (operator.mul, int, str): str,
    (operator.eq, Pointer, Pointer): bool,
    (operator.ne, Pointer, Pointer): bool,
    (operator.lt, Pointer, Pointer): bool,
    (operator.le, Pointer, Pointer): bool,
    (operator.gt, Pointer, Pointer): bool,
    (operator.ge, Pointer, Pointer): bool,
    (operator.eq, DateTimeNaive, DateTimeNaive): bool,
    (operator.ne, DateTimeNaive, DateTimeNaive): bool,
    (operator.lt, DateTimeNaive, DateTimeNaive): bool,
    (operator.le, DateTimeNaive, DateTimeNaive): bool,
    (operator.gt, DateTimeNaive, DateTimeNaive): bool,
    (operator.ge, DateTimeNaive, DateTimeNaive): bool,
    (operator.sub, DateTimeNaive, DateTimeNaive): Duration,
    (operator.add, DateTimeNaive, Duration): DateTimeNaive,
    (operator.sub, DateTimeNaive, Duration): DateTimeNaive,
    (operator.eq, DateTimeUtc, DateTimeUtc): bool,
    (operator.ne, DateTimeUtc, DateTimeUtc): bool,
    (operator.lt, DateTimeUtc, DateTimeUtc): bool,
    (operator.le, DateTimeUtc, DateTimeUtc): bool,
    (operator.gt, DateTimeUtc, DateTimeUtc): bool,
    (operator.ge, DateTimeUtc, DateTimeUtc): bool,
    (operator.sub, DateTimeUtc, DateTimeUtc): Duration,
    (operator.add, DateTimeUtc, Duration): DateTimeUtc,
    (operator.sub, DateTimeUtc, Duration): DateTimeUtc,
    (operator.eq, Duration, Duration): bool,
    (operator.ne, Duration, Duration): bool,
    (operator.lt, Duration, Duration): bool,
    (operator.le, Duration, Duration): bool,
    (operator.gt, Duration, Duration): bool,
    (operator.ge, Duration, Duration): bool,
    (operator.add, Duration, Duration): Duration,
    (operator.add, Duration, DateTimeNaive): DateTimeNaive,
    (operator.add, Duration, DateTimeUtc): DateTimeUtc,
    (operator.sub, Duration, Duration): Duration,
    (operator.mul, Duration, int): Duration,
    (operator.mul, int, Duration): Duration,
    (operator.floordiv, Duration, int): Duration,
    (operator.floordiv, Duration, Duration): int,
    (operator.truediv, Duration, Duration): float,
    (operator.mod, Duration, Duration): Duration,
    (operator.matmul, np.ndarray, np.ndarray): np.ndarray,
}

tuple_handling_operators = {
    operator.eq,
    operator.ne,
    operator.le,
    operator.lt,
    operator.ge,
    operator.gt,
}


def get_binary_operators_mapping(op, left, right, default=None) -> DType:
    return DType(_binary_operators_mapping.get((op, left, right), default))


def get_binary_expression(left, right, op, left_dtype, right_dtype, default=None):
    op_engine = _binary_operators_to_engine.get(op)
    left_dtype_engine = _TYPES_TO_ENGINE_MAPPING.get(left_dtype)
    right_dtype_engine = _TYPES_TO_ENGINE_MAPPING.get(right_dtype)
    if op_engine is None or left_dtype_engine is None or right_dtype_engine is None:
        return default

    expression = api.Expression.binary_expression(
        left, right, op_engine, left_dtype_engine, right_dtype_engine
    )
    return expression if expression is not None else default


_binary_operators_mapping_optionals: OptionalMapping = {
    operator.eq: (bool, api.Expression.eq),
    operator.ne: (bool, api.Expression.ne),
}


def get_binary_operators_mapping_optionals(op, left, right, default=None):
    if left == right or left == NoneType or right == NoneType:
        return _binary_operators_mapping_optionals.get(op, default)
    else:
        return default


def get_cast_operators_mapping(
    expr: api.Expression, source_type: DType, target_type: DType, default=None
) -> Optional[api.Expression]:
    source_type_engine = _TYPES_TO_ENGINE_MAPPING.get(unoptionalize(source_type))
    target_type_engine = _TYPES_TO_ENGINE_MAPPING.get(unoptionalize(target_type))
    if source_type_engine is None or target_type_engine is None:
        return default
    if is_optional(source_type) and is_optional(target_type):
        fun = api.Expression.cast_optional
    else:
        fun = api.Expression.cast
    expression = fun(
        expr,
        source_type_engine,
        target_type_engine,
    )
    return expression if expression is not None else default


def common_dtype_in_binary_operator(
    left_dtype: DType, right_dtype: DType
) -> Optional[DType]:
    if (
        left_dtype in [int, Optional[int]] and right_dtype in [float, Optional[float]]
    ) or (
        left_dtype in [float, Optional[float]] and right_dtype in [int, Optional[int]]
    ):
        return types_lca(left_dtype, right_dtype)
    return None

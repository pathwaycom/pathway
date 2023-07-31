# Copyright © 2023 Pathway

from typing import Any, Callable, Mapping, Tuple

from pathway.internals import api
from pathway.internals.api import Pointer
from pathway.internals.datetime_types import DateTimeNaive, DateTimeUtc, Duration
from pathway.internals.dtype import NoneType
from pathway.internals.shadows import operator

UnaryOperator = Callable[[Any], Any]
ApiUnaryOperator = Callable[[api.Expression], api.Expression]
UnaryOperatorMapping = Mapping[
    Tuple[UnaryOperator, Any],
    Tuple[type, ApiUnaryOperator],
]

BinaryOperator = Callable[[Any, Any], Any]
ApiBinaryOperator = Callable[[api.Expression, api.Expression], api.Expression]
BinaryOperatorMapping = Mapping[
    Tuple[BinaryOperator, Any, Any],
    Tuple[type, ApiBinaryOperator],
]

OptionalMapping = Mapping[
    BinaryOperator,
    Tuple[type, ApiBinaryOperator],
]

_unary_operators_mapping: UnaryOperatorMapping = {
    (operator.inv, bool): (bool, api.Expression.not_),
    (operator.neg, int): (int, api.Expression.int_neg),
    (operator.neg, float): (float, api.Expression.float_neg),
    (operator.neg, Duration): (Duration, api.Expression.duration_neg),
}


def get_unary_operators_mapping(op, operand_dtype, default=None):
    return _unary_operators_mapping.get((op, operand_dtype), default)


_binary_operators_mapping: BinaryOperatorMapping = {
    (operator.and_, bool, bool): (bool, api.Expression.and_),
    (operator.or_, bool, bool): (bool, api.Expression.or_),
    (operator.xor, bool, bool): (bool, api.Expression.xor),
    (operator.eq, int, int): (bool, api.Expression.int_eq),
    (operator.ne, int, int): (bool, api.Expression.int_ne),
    (operator.lt, int, int): (bool, api.Expression.int_lt),
    (operator.le, int, int): (bool, api.Expression.int_le),
    (operator.gt, int, int): (bool, api.Expression.int_gt),
    (operator.ge, int, int): (bool, api.Expression.int_ge),
    (operator.add, int, int): (int, api.Expression.int_add),
    (operator.sub, int, int): (int, api.Expression.int_sub),
    (operator.mul, int, int): (int, api.Expression.int_mul),
    (operator.floordiv, int, int): (int, api.Expression.int_floor_div),
    (operator.truediv, int, int): (float, api.Expression.int_true_div),
    (operator.mod, int, int): (int, api.Expression.int_mod),
    (operator.pow, int, int): (int, api.Expression.int_pow),
    (operator.lshift, int, int): (int, api.Expression.int_lshift),
    (operator.rshift, int, int): (int, api.Expression.int_rshift),
    (operator.and_, int, int): (int, api.Expression.int_and),
    (operator.or_, int, int): (int, api.Expression.int_or),
    (operator.xor, int, int): (int, api.Expression.int_xor),
    (operator.eq, float, float): (bool, api.Expression.float_eq),
    (operator.ne, float, float): (bool, api.Expression.float_ne),
    (operator.lt, float, float): (bool, api.Expression.float_lt),
    (operator.le, float, float): (bool, api.Expression.float_le),
    (operator.gt, float, float): (bool, api.Expression.float_gt),
    (operator.ge, float, float): (bool, api.Expression.float_ge),
    (operator.add, float, float): (float, api.Expression.float_add),
    (operator.sub, float, float): (float, api.Expression.float_sub),
    (operator.mul, float, float): (float, api.Expression.float_mul),
    (operator.floordiv, float, float): (float, api.Expression.float_floor_div),
    (operator.truediv, float, float): (float, api.Expression.float_true_div),
    (operator.mod, float, float): (float, api.Expression.float_mod),
    (operator.pow, float, float): (float, api.Expression.float_pow),
    (operator.eq, str, str): (bool, api.Expression.str_eq),
    (operator.ne, str, str): (bool, api.Expression.str_ne),
    (operator.lt, str, str): (bool, api.Expression.str_lt),
    (operator.le, str, str): (bool, api.Expression.str_le),
    (operator.gt, str, str): (bool, api.Expression.str_gt),
    (operator.ge, str, str): (bool, api.Expression.str_ge),
    (operator.add, str, str): (str, api.Expression.str_add),
    (operator.mul, str, int): (str, api.Expression.str_rmul),
    (operator.mul, int, str): (str, api.Expression.str_lmul),
    (operator.eq, Pointer, Pointer): (bool, api.Expression.ptr_eq),
    (operator.ne, Pointer, Pointer): (bool, api.Expression.ptr_ne),
    (operator.lt, Pointer, Pointer): (bool, api.Expression.ptr_lt),
    (operator.le, Pointer, Pointer): (bool, api.Expression.ptr_le),
    (operator.gt, Pointer, Pointer): (bool, api.Expression.ptr_gt),
    (operator.ge, Pointer, Pointer): (bool, api.Expression.ptr_ge),
    (operator.eq, DateTimeNaive, DateTimeNaive): (
        bool,
        api.Expression.date_time_naive_eq,
    ),
    (operator.ne, DateTimeNaive, DateTimeNaive): (
        bool,
        api.Expression.date_time_naive_ne,
    ),
    (operator.lt, DateTimeNaive, DateTimeNaive): (
        bool,
        api.Expression.date_time_naive_lt,
    ),
    (operator.le, DateTimeNaive, DateTimeNaive): (
        bool,
        api.Expression.date_time_naive_le,
    ),
    (operator.gt, DateTimeNaive, DateTimeNaive): (
        bool,
        api.Expression.date_time_naive_gt,
    ),
    (operator.ge, DateTimeNaive, DateTimeNaive): (
        bool,
        api.Expression.date_time_naive_ge,
    ),
    (operator.sub, DateTimeNaive, DateTimeNaive): (
        Duration,
        api.Expression.date_time_naive_sub,
    ),
    (operator.add, DateTimeNaive, Duration): (
        DateTimeNaive,
        api.Expression.date_time_naive_add_duration,
    ),
    (operator.sub, DateTimeNaive, Duration): (
        DateTimeNaive,
        api.Expression.date_time_naive_sub_duration,
    ),
    (operator.eq, DateTimeUtc, DateTimeUtc): (
        bool,
        api.Expression.date_time_utc_eq,
    ),
    (operator.ne, DateTimeUtc, DateTimeUtc): (
        bool,
        api.Expression.date_time_utc_ne,
    ),
    (operator.lt, DateTimeUtc, DateTimeUtc): (
        bool,
        api.Expression.date_time_utc_lt,
    ),
    (operator.le, DateTimeUtc, DateTimeUtc): (
        bool,
        api.Expression.date_time_utc_le,
    ),
    (operator.gt, DateTimeUtc, DateTimeUtc): (
        bool,
        api.Expression.date_time_utc_gt,
    ),
    (operator.ge, DateTimeUtc, DateTimeUtc): (
        bool,
        api.Expression.date_time_utc_ge,
    ),
    (operator.sub, DateTimeUtc, DateTimeUtc): (
        Duration,
        api.Expression.date_time_utc_sub,
    ),
    (operator.add, DateTimeUtc, Duration): (
        DateTimeUtc,
        api.Expression.date_time_utc_add_duration,
    ),
    (operator.sub, DateTimeUtc, Duration): (
        DateTimeUtc,
        api.Expression.date_time_utc_sub_duration,
    ),
    (operator.eq, Duration, Duration): (bool, api.Expression.duration_eq),
    (operator.ne, Duration, Duration): (bool, api.Expression.duration_ne),
    (operator.lt, Duration, Duration): (bool, api.Expression.duration_lt),
    (operator.le, Duration, Duration): (bool, api.Expression.duration_le),
    (operator.gt, Duration, Duration): (bool, api.Expression.duration_gt),
    (operator.ge, Duration, Duration): (bool, api.Expression.duration_ge),
    (operator.add, Duration, Duration): (Duration, api.Expression.duration_add),
    (operator.add, Duration, DateTimeNaive): (
        DateTimeNaive,
        api.Expression.duration_add_date_time_naive,
    ),
    (operator.add, Duration, DateTimeUtc): (
        DateTimeUtc,
        api.Expression.duration_add_date_time_utc,
    ),
    (operator.sub, Duration, Duration): (Duration, api.Expression.duration_sub),
    (operator.mul, Duration, int): (Duration, api.Expression.duration_mul_by_int),
    (operator.mul, int, Duration): (Duration, api.Expression.int_mul_by_duration),
    (operator.floordiv, Duration, int): (Duration, api.Expression.duration_div_by_int),
    (operator.floordiv, Duration, Duration): (int, api.Expression.duration_floor_div),
    (operator.truediv, Duration, Duration): (float, api.Expression.duration_true_div),
    (operator.mod, Duration, Duration): (Duration, api.Expression.duration_mod),
}


def get_binary_operators_mapping(
    op, left, right, default=None
) -> Tuple[type, ApiBinaryOperator]:
    return _binary_operators_mapping.get((op, left, right), default)


_binary_operators_mapping_optionals: OptionalMapping = {
    operator.eq: (bool, api.Expression.eq),
    operator.ne: (bool, api.Expression.ne),
}


def get_binary_operators_mapping_optionals(op, left, right, default=None):
    if left == right or left == NoneType or right == NoneType:
        return _binary_operators_mapping_optionals.get(op, default)
    else:
        return default


_cast_operators_mapping: Mapping[
    Tuple[Any, Any],
    Callable[[api.Expression], api.Expression],
] = {
    (int, float): api.Expression.int_to_float,
    (int, bool): api.Expression.int_to_bool,
    (int, str): api.Expression.int_to_str,
    (float, int): api.Expression.float_to_int,
    (float, bool): api.Expression.float_to_bool,
    (float, str): api.Expression.float_to_str,
    (bool, int): api.Expression.bool_to_int,
    (bool, float): api.Expression.bool_to_float,
    (bool, str): api.Expression.bool_to_str,
    (str, int): api.Expression.str_to_int,
    (str, float): api.Expression.str_to_float,
    (str, bool): api.Expression.str_to_bool,
}


def get_cast_operators_mapping(
    source_type, target_type, default=None
) -> Callable[[api.Expression], api.Expression]:
    return _cast_operators_mapping.get((source_type, target_type), default)

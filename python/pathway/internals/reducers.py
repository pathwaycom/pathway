# Copyright © 2023 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from warnings import warn

import numpy as np

from pathway.internals import api
from pathway.internals import dtype as dt
from pathway.internals import expression as expr
from pathway.internals.common import apply_with_type


class UnaryReducer(ABC):
    name: str

    def __repr__(self):
        return self.name

    def __init__(self, *, name: str):
        self.name = name

    @abstractmethod
    def return_type(self, arg_type: dt.DType) -> dt.DType:
        ...

    @abstractmethod
    def engine_reducer(self, arg_type: dt.DType) -> api.Reducer:
        ...


class UnaryReducerWithDefault(UnaryReducer):
    _engine_reducer: api.Reducer

    def __init__(self, *, name: str, engine_reducer: api.Reducer):
        super().__init__(name=name)
        self._engine_reducer = engine_reducer

    def engine_reducer(self, arg_type: dt.DType) -> api.Reducer:
        return self._engine_reducer


class FixedOutputUnaryReducer(UnaryReducerWithDefault):
    output_type: dt.DType

    def __init__(
        self, *, output_type: dt.DType, name: str, engine_reducer: api.Reducer
    ):
        super().__init__(name=name, engine_reducer=engine_reducer)
        self.output_type = output_type

    def return_type(self, arg_type: dt.DType) -> dt.DType:
        return self.output_type


class TypePreservingUnaryReducer(UnaryReducerWithDefault):
    def return_type(self, arg_type: dt.DType) -> dt.DType:
        return arg_type


class SumReducer(UnaryReducer):
    def return_type(self, arg_type: dt.DType) -> dt.DType:
        for allowed_dtype in [dt.FLOAT, dt.Array()]:
            if dt.dtype_issubclass(arg_type, allowed_dtype):
                return arg_type
        raise TypeError(
            f"Pathway does not support using reducer {self}"
            + f" on column of type {arg_type}.\n"
        )

    def engine_reducer(self, arg_type: dt.DType) -> api.Reducer:
        if arg_type == dt.INT:
            return api.Reducer.INT_SUM
        elif arg_type == dt.Array():
            return api.Reducer.ARRAY_SUM
        else:
            return api.Reducer.FLOAT_SUM


class TupleWrappingReducer(UnaryReducerWithDefault):
    def return_type(self, arg_type: dt.DType) -> dt.DType:
        return dt.List(arg_type)  # type: ignore[valid-type]


_min = TypePreservingUnaryReducer(name="min", engine_reducer=api.Reducer.MIN)
_max = TypePreservingUnaryReducer(name="max", engine_reducer=api.Reducer.MAX)
_sum = SumReducer(name="sum")
_sorted_tuple = TupleWrappingReducer(
    name="sorted_tuple", engine_reducer=api.Reducer.SORTED_TUPLE
)
_tuple = TupleWrappingReducer(name="tuple", engine_reducer=api.Reducer.TUPLE)
_argmin = FixedOutputUnaryReducer(
    output_type=dt.POINTER,
    name="argmin",
    engine_reducer=api.Reducer.ARG_MIN,
)
_argmax = FixedOutputUnaryReducer(
    output_type=dt.POINTER,
    name="argmax",
    engine_reducer=api.Reducer.ARG_MAX,
)
_unique = TypePreservingUnaryReducer(name="unique", engine_reducer=api.Reducer.UNIQUE)
_any = TypePreservingUnaryReducer(name="any", engine_reducer=api.Reducer.ANY)


def _generate_unary_reducer(reducer: UnaryReducer):
    def wrapper(arg: expr.ColumnExpression) -> expr.ReducerExpression:
        return expr.ReducerExpression(reducer, arg)

    return wrapper


min = _generate_unary_reducer(_min)
max = _generate_unary_reducer(_max)
sum = _generate_unary_reducer(_sum)
sorted_tuple = _generate_unary_reducer(_sorted_tuple)
argmin = _generate_unary_reducer(_argmin)
argmax = _generate_unary_reducer(_argmax)
unique = _generate_unary_reducer(_unique)
any = _generate_unary_reducer(_any)
tuple = _generate_unary_reducer(_tuple)


def npsum(arg):
    warn("Using pathway.reducers.npsum() is deprecated, use pathway.reducers.sum()")
    return sum(arg)


def count(*args):
    if args:
        warn(
            "Passing argument to pathway.reducers.count() is deprecated, use pathway.reducers.count() "
            + "without any arguments."
        )
    return expr.CountExpression()


def avg(expression: expr.ColumnExpression):
    return sum(expression) / count()


def int_sum(expression: expr.ColumnExpression):
    warn(
        "Reducer pathway.reducers.int_sum is deprecated, use pathway.reducers.sum instead."
    )
    return sum(expression)


def ndarray(expression: expr.ColumnExpression):
    return apply_with_type(np.array, np.ndarray, tuple(expression))

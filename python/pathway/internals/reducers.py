# Copyright © 2023 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ParamSpec, Protocol, TypeVar, overload
from warnings import warn

import numpy as np

from pathway.internals import api, dtype as dt, expression as expr
from pathway.internals.column import ColumnExpression, GroupedContext
from pathway.internals.common import apply_with_type


class Reducer(ABC):
    name: str

    def __repr__(self):
        return self.name

    def __init__(self, *, name: str):
        self.name = name

    @abstractmethod
    def return_type(self, arg_types: list[dt.DType]) -> dt.DType:
        ...

    @abstractmethod
    def engine_reducer(self, arg_types: list[dt.DType]) -> api.Reducer:
        ...

    def additional_args_from_context(
        self, context: GroupedContext
    ) -> tuple[ColumnExpression, ...]:
        return ()


class UnaryReducer(Reducer):
    name: str

    def __repr__(self):
        return self.name

    def __init__(self, *, name: str):
        self.name = name

    @abstractmethod
    def return_type_unary(self, arg_type: dt.DType) -> dt.DType:
        ...

    def return_type(self, arg_types: list[dt.DType]) -> dt.DType:
        (arg_type,) = arg_types
        return self.return_type_unary(arg_type)

    @abstractmethod
    def engine_reducer_unary(self, arg_type: dt.DType) -> api.Reducer:
        ...

    def engine_reducer(self, arg_types: list[dt.DType]) -> api.Reducer:
        (arg_type,) = arg_types
        return self.engine_reducer_unary(arg_type)


class UnaryReducerWithDefault(UnaryReducer):
    _engine_reducer: api.Reducer

    def __init__(self, *, name: str, engine_reducer: api.Reducer):
        super().__init__(name=name)
        self._engine_reducer = engine_reducer

    def engine_reducer_unary(self, arg_type: dt.DType) -> api.Reducer:
        return self._engine_reducer


class FixedOutputUnaryReducer(UnaryReducerWithDefault):
    output_type: dt.DType

    def __init__(
        self, *, output_type: dt.DType, name: str, engine_reducer: api.Reducer
    ):
        super().__init__(name=name, engine_reducer=engine_reducer)
        self.output_type = output_type

    def return_type_unary(self, arg_type: dt.DType) -> dt.DType:
        return self.output_type


class TypePreservingUnaryReducer(UnaryReducerWithDefault):
    def return_type_unary(self, arg_type: dt.DType) -> dt.DType:
        return arg_type


class SumReducer(UnaryReducer):
    def return_type_unary(self, arg_type: dt.DType) -> dt.DType:
        for allowed_dtype in [dt.FLOAT, dt.ARRAY]:
            if dt.dtype_issubclass(arg_type, allowed_dtype):
                return arg_type
        raise TypeError(
            f"Pathway does not support using reducer {self}"
            + f" on column of type {arg_type}.\n"
        )

    def engine_reducer_unary(self, arg_type: dt.DType) -> api.Reducer:
        if arg_type == dt.INT:
            return api.Reducer.INT_SUM
        elif arg_type == dt.ARRAY:
            return api.Reducer.ARRAY_SUM
        else:
            return api.Reducer.FLOAT_SUM


class SortedTupleWrappingReducer(UnaryReducerWithDefault):
    _skip_nones: bool

    def __init__(
        self,
        *,
        name: str,
        engine_reducer: api.Reducer,
        skip_nones: bool,
    ):
        super().__init__(name=name, engine_reducer=engine_reducer)
        self._skip_nones = skip_nones

    def return_type_unary(self, arg_type: dt.DType) -> dt.DType:
        if self._skip_nones:
            arg_type = dt.unoptionalize(arg_type)

        return dt.List(arg_type)


class TupleWrappingReducer(Reducer):
    _skip_nones: bool
    _engine_reducer: api.Reducer

    def __init__(
        self,
        *,
        name: str,
        engine_reducer: api.Reducer,
        skip_nones: bool,
    ):
        super().__init__(name=name)
        self._engine_reducer = engine_reducer
        self._skip_nones = skip_nones

    def return_type(self, arg_types: list[dt.DType]) -> dt.DType:
        arg_type = arg_types[0]
        if self._skip_nones:
            arg_type = dt.unoptionalize(arg_type)

        return dt.List(arg_type)

    def engine_reducer(self, arg_types: list[dt.DType]) -> api.Reducer:
        return self._engine_reducer

    def additional_args_from_context(
        self, context: GroupedContext
    ) -> tuple[ColumnExpression, ...]:
        if context.sort_by is not None:
            return (context.sort_by.to_column_expression(),)
        else:
            return ()


class StatefulManyReducer(Reducer):
    name = "stateful_many"
    combine_many: api.CombineMany

    def __init__(self, combine_many: api.CombineMany):
        self.combine_many = combine_many

    def return_type(self, arg_types: list[dt.DType]) -> dt.DType:
        return dt.ANY

    def engine_reducer(self, arg_types: list[dt.DType]) -> api.Reducer:
        return api.Reducer.stateful_many(self.combine_many)


_min = TypePreservingUnaryReducer(name="min", engine_reducer=api.Reducer.MIN)
_max = TypePreservingUnaryReducer(name="max", engine_reducer=api.Reducer.MAX)
_sum = SumReducer(name="sum")


def _sorted_tuple(skip_nones: bool):
    return SortedTupleWrappingReducer(
        name="sorted_tuple",
        engine_reducer=api.Reducer.sorted_tuple(skip_nones),
        skip_nones=skip_nones,
    )


def _tuple(skip_nones: bool):
    return TupleWrappingReducer(
        name="tuple",
        engine_reducer=api.Reducer.tuple(skip_nones),
        skip_nones=skip_nones,
    )


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


def _generate_unary_reducer(reducer: UnaryReducer, **kwargs):
    def wrapper(arg: expr.ColumnExpression) -> expr.ReducerExpression:
        return expr.ReducerExpression(reducer, arg, **kwargs)

    return wrapper


min = _generate_unary_reducer(_min)
max = _generate_unary_reducer(_max)
sum = _generate_unary_reducer(_sum)
argmin = _generate_unary_reducer(_argmin)
argmax = _generate_unary_reducer(_argmax)
unique = _generate_unary_reducer(_unique)
any = _generate_unary_reducer(_any)


def sorted_tuple(arg: expr.ColumnExpression, *, skip_nones: bool = False):
    return _generate_unary_reducer(_sorted_tuple(skip_nones), skip_nones=skip_nones)(
        arg
    )


# Exported as `tuple` by `pathway.reducers` to avoid shadowing the builtin here
def tuple_reducer(arg: expr.ColumnExpression, *, skip_nones: bool = False):
    return _generate_unary_reducer(_tuple(skip_nones), skip_nones=skip_nones)(arg)


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


def ndarray(expression: expr.ColumnExpression, *, skip_nones: bool = False):
    return apply_with_type(
        np.array, np.ndarray, tuple_reducer(expression, skip_nones=skip_nones)
    )


S = TypeVar("S", bound=api.Value)
V1 = TypeVar("V1", bound=api.Value)
V2 = TypeVar("V2", bound=api.Value)


def stateful_many(
    combine_many: api.CombineMany[S], /, *args: expr.ColumnExpression | api.Value
) -> expr.ColumnExpression:
    return expr.ReducerExpression(StatefulManyReducer(combine_many), *args)


P = ParamSpec("P")


class CombineSingle(Protocol[S, P]):
    def __call__(self, state: S | None, /, *args: P.args, **kwargs: P.kwargs) -> S:
        ...


@overload
def stateful_single(combine_single: CombineSingle[S, []], /) -> expr.ColumnExpression:
    ...


@overload
def stateful_single(
    combine_single: CombineSingle[S, [V1]], column: expr.ColumnExpression | V1, /
) -> expr.ColumnExpression:
    ...


@overload
def stateful_single(
    combine_single: CombineSingle[S, [V1, V2]],
    column1: expr.ColumnExpression | V1,
    column2: expr.ColumnExpression | V2,
    /,
) -> expr.ColumnExpression:
    ...


@overload
def stateful_single(
    combine_single: CombineSingle[S, ...],
    /,
    *columns: expr.ColumnExpression | api.Value,
) -> expr.ColumnExpression:
    ...


def stateful_single(
    combine_single: CombineSingle[S, ...], /, *args: expr.ColumnExpression | api.Value
) -> expr.ColumnExpression:
    def wrapper(state: S | None, rows: list[tuple[list[api.Value], int]]) -> S:
        for row, count in rows:
            assert count > 0
            for _ in range(count):
                state = combine_single(state, *row)
        assert state is not None
        return state

    return stateful_many(wrapper, *args)

# Copyright © 2023 Pathway

from __future__ import annotations

import pickle
from abc import ABC, abstractmethod
from collections import Counter
from typing import ParamSpec, Protocol, TypeVar
from warnings import warn

import numpy as np
from typing_extensions import Self

from pathway.internals import api, dtype as dt, expression as expr
from pathway.internals.column import ColumnExpression, GroupedContext
from pathway.internals.common import apply_with_type
from pathway.internals.shadows.inspect import signature


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


class ReducerProtocol(Protocol):
    def __call__(
        self, *args: expr.ColumnExpression | api.Value
    ) -> expr.ColumnExpression:
        ...


def stateful_many(
    combine_many: api.CombineMany[S],
) -> ReducerProtocol:
    def wrapper(*args: expr.ColumnExpression | api.Value) -> expr.ColumnExpression:
        return expr.ReducerExpression(StatefulManyReducer(combine_many), *args)

    return wrapper


P = ParamSpec("P")


class CombineSingle(Protocol[S, P]):
    def __call__(self, state: S | None, /, *args: P.args, **kwargs: P.kwargs) -> S:
        ...


def stateful_single(combine_single: CombineSingle[S, ...]) -> ReducerProtocol:
    def wrapper(state: S | None, rows: list[tuple[list[api.Value], int]]) -> S:
        for row, count in rows:
            assert count > 0
            for _ in range(count):
                state = combine_single(state, *row)
        assert state is not None
        return state

    return stateful_many(wrapper)


def udf_reducer(
    reducer_cls: type[BaseCustomAccumulator],
):
    """Decorator for defining custom reducers. Requires custom accumulator as an argument.
    Custom accumulator should implement `from_row`, `update` and `compute_result`.
    Optionally `neutral` and `retract` can be provided for more efficient processing on
    streams with changing data.

    >>> import pathway as pw
    >>> class CustomAvgAccumulator(pw.BaseCustomAccumulator):
    ...   def __init__(self, sum, cnt):
    ...     self.sum = sum
    ...     self.cnt = cnt
    ...
    ...   @classmethod
    ...   def from_row(self, row):
    ...     [val] = row
    ...     return CustomAvgAccumulator(val, 1)
    ...
    ...   def update(self, other):
    ...     self.sum += other.sum
    ...     self.cnt += other.cnt
    ...
    ...   def compute_result(self) -> float:
    ...     return self.sum / self.cnt
    >>> import sys; sys.modules[__name__].CustomAvgAccumulator = CustomAvgAccumulator # NOSHOW
    >>> custom_avg = pw.reducers.udf_reducer(CustomAvgAccumulator)
    >>> t1 = pw.debug.parse_to_table('''
    ... age | owner | pet | price
    ... 10  | Alice | dog | 100
    ... 9   | Bob   | cat | 80
    ... 8   | Alice | cat | 90
    ... 7   | Bob   | dog | 70
    ... ''')
    >>> t2 = t1.groupby(t1.owner).reduce(t1.owner, avg_price=custom_avg(t1.price))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    owner | avg_price
    Alice | 95.0
    Bob   | 75.0
    """
    neutral_available = _is_overridden(reducer_cls, "neutral")
    retract_available = _is_overridden(reducer_cls, "retract")

    def wrapper(*args: expr.ColumnExpression | api.Value) -> ColumnExpression:
        @stateful_many
        def stateful_wrapper(
            pickled_state: bytes | None, rows: list[tuple[list[api.Value], int]]
        ) -> bytes | None:
            if pickled_state is not None:
                state = pickle.loads(pickled_state)
                if not retract_available:
                    state._positive_updates = list(state._positive_updates)
            else:
                state = None
            positive_updates: list[tuple[api.Value, ...]] = []
            negative_updates = []
            for row, count in rows:
                if count > 0:
                    positive_updates.extend([tuple(row)] * count)
                else:
                    negative_updates.extend([tuple(row)] * (-count))

            if not retract_available and len(negative_updates) > 0:
                if state is not None:
                    positive_updates.extend(state._positive_updates)
                    state._positive_updates = []
                    state = None
                acc = Counter(positive_updates)
                acc.subtract(negative_updates)
                assert all(x >= 0 for x in acc.values())
                positive_updates = list(acc.elements())
                negative_updates = []

            if state is None:
                if neutral_available:
                    state = reducer_cls.neutral()
                    if not retract_available:
                        state._positive_updates = []
                    else:
                        state._cnt = 0
                elif len(positive_updates) == 0:
                    if len(negative_updates) == 0:
                        return None
                    else:
                        raise ValueError(
                            "Unable to process negative update with this custom reducer."
                        )
                else:
                    state = reducer_cls.from_row(list(positive_updates[0]))
                    if not retract_available:
                        state._positive_updates = positive_updates[0:1]
                    else:
                        state._cnt = 1
                    positive_updates = positive_updates[1:]

            for row_up in positive_updates:
                if not retract_available:
                    state._positive_updates.append(row_up)
                else:
                    state._cnt += 1
                val = reducer_cls.from_row(list(row_up))
                state.update(val)

            for row_up in negative_updates:
                if not retract_available:
                    raise ValueError(
                        "Unable to process negative update with this custom reducer."
                    )
                else:
                    state._cnt -= 1
                val = reducer_cls.from_row(list(row_up))
                state.retract(val)

            if not retract_available:
                state._positive_updates = tuple(
                    tuple(x) for x in state._positive_updates
                )
            else:
                if state._cnt == 0:
                    # this is fine in this setting, where we process values one by one
                    # if this ever becomes accumulated in a tree, we have to handle
                    # (A-B) updates, so we have to distinguish `0` from intermediate states
                    # accumulating weighted count (weighted by hash) should do fine here
                    return None

            return pickle.dumps(state)

        def extractor(x: bytes):
            unpickled = pickle.loads(x)
            assert isinstance(unpickled, reducer_cls)
            return unpickled.compute_result()

        return apply_with_type(
            extractor,
            signature(reducer_cls.compute_result).return_annotation,
            stateful_wrapper(*args),
        )

    return wrapper


def mark_stub(fun):
    fun.__pw_stub = True
    return fun


class BaseCustomAccumulator(ABC):
    """Utility class for defining custom accumulators, used for custom reducers.
    Custom accumulators should inherit from this class, and should implement `from_row`,
    `update` and `compute_result`. Optionally `neutral` and `retract` can be provided
    for more efficient processing on streams with changing data.

    >>> import pathway as pw
    >>> class CustomAvgAccumulator(pw.BaseCustomAccumulator):
    ...   def __init__(self, sum, cnt):
    ...     self.sum = sum
    ...     self.cnt = cnt
    ...
    ...   @classmethod
    ...   def from_row(self, row):
    ...     [val] = row
    ...     return CustomAvgAccumulator(val, 1)
    ...
    ...   def update(self, other):
    ...     self.sum += other.sum
    ...     self.cnt += other.cnt
    ...
    ...   def compute_result(self) -> float:
    ...     return self.sum / self.cnt
    >>> import sys; sys.modules[__name__].CustomAvgAccumulator = CustomAvgAccumulator # NOSHOW
    >>> custom_avg = pw.reducers.udf_reducer(CustomAvgAccumulator)
    >>> t1 = pw.debug.parse_to_table('''
    ... age | owner | pet | price
    ... 10  | Alice | dog | 100
    ... 9   | Bob   | cat | 80
    ... 8   | Alice | cat | 90
    ... 7   | Bob   | dog | 70
    ... ''')
    >>> t2 = t1.groupby(t1.owner).reduce(t1.owner, avg_price=custom_avg(t1.price))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    owner | avg_price
    Alice | 95.0
    Bob   | 75.0
    """

    @classmethod
    @mark_stub
    def neutral(cls) -> Self:
        """Neutral element of the accumulator (aggregation of an empty list).

        This function is optional, and allows for more efficient processing on streams
        with changing data."""
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_row(cls, row: list[api.Value]) -> Self:
        """Construct the accumulator from a row of data.
        Row will be passed as a list of values.

        This is a mandatory function."""
        raise NotImplementedError()

    @abstractmethod
    def update(self, other: Self) -> None:
        """Update the accumulator with another one.
        Method does not need to return anything, the change should be in-place.

        This is a mandatory function."""
        raise NotImplementedError()

    @mark_stub
    def retract(self, other: Self) -> None:
        """Update the accumulator by removing the value of another one.

        This function is optional, and allows more efficient reductions on streams
        with changing data.
        """
        raise NotImplementedError()

    @abstractmethod
    def compute_result(self) -> api.Value:
        """Mandatory function to finalize computation.
        Used to extract answer from final state of accumulator.

        Narrowing the type of this function helps better type the output of the reducer.
        """
        raise NotImplementedError()


def _is_overridden(cls: type[BaseCustomAccumulator], name: str) -> bool:
    assert hasattr(BaseCustomAccumulator, name)
    return not hasattr(getattr(cls, name), "__pw_stub")

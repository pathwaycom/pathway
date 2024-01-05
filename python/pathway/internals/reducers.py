# Copyright © 2024 Pathway

from __future__ import annotations

import builtins
from abc import ABC, abstractmethod
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
    ) -> builtins.tuple[ColumnExpression, ...]:
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
        for allowed_dtype in [dt.FLOAT, dt.ANY_ARRAY]:
            if dt.dtype_issubclass(arg_type, allowed_dtype):
                return arg_type
        raise TypeError(
            f"Pathway does not support using reducer {self}"
            + f" on column of type {arg_type}.\n"
        )

    def engine_reducer_unary(self, arg_type: dt.DType) -> api.Reducer:
        if arg_type == dt.INT:
            return api.Reducer.INT_SUM
        elif isinstance(arg_type, dt.Array):
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
    ) -> builtins.tuple[ColumnExpression, ...]:
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


def _apply_unary_reducer(
    reducer: UnaryReducer, arg: expr.ColumnExpression, **kwargs
) -> expr.ReducerExpression:
    return expr.ReducerExpression(reducer, arg, **kwargs)


def min(arg: expr.ColumnExpression) -> expr.ReducerExpression:
    """
    Returns the minimum of the aggregated values.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... colA | colB
    ... valA | -1
    ... valA |  1
    ... valA |  2
    ... valB |  4
    ... valB |  4
    ... valB |  7
    ... ''')
    >>> result = t.groupby(t.colA).reduce(min=pw.reducers.min(t.colB))
    >>> pw.debug.compute_and_print(result, include_id=False)
    min
    -1
    4
    """
    return _apply_unary_reducer(_min, arg)


def max(arg: expr.ColumnExpression) -> expr.ReducerExpression:
    """
    Returns the maximum of the aggregated values.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... colA | colB
    ... valA | -1
    ... valA |  1
    ... valA |  2
    ... valB |  4
    ... valB |  4
    ... valB |  7
    ... ''')
    >>> result = t.groupby(t.colA).reduce(max=pw.reducers.max(t.colB))
    >>> pw.debug.compute_and_print(result, include_id=False)
    max
    2
    7
    """
    return _apply_unary_reducer(_max, arg)


def sum(arg: expr.ColumnExpression) -> expr.ReducerExpression:
    """
    Returns the sum of the aggregated values. Can handle int, float, and array values.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... colA | colB
    ... valA | -1
    ... valA |  1
    ... valA |  2
    ... valB |  4
    ... valB |  4
    ... valB |  7
    ... ''')
    >>> result = t.groupby(t.colA).reduce(sum=pw.reducers.sum(t.colB))
    >>> pw.debug.compute_and_print(result, include_id=False)
    sum
    2
    15


    >>> import pandas as pd
    >>> np_table = pw.debug.table_from_pandas(
    ...     pd.DataFrame(
    ...         {
    ...             "data": [
    ...                 np.array([1, 2, 3]),
    ...                 np.array([4, 5, 6]),
    ...                 np.array([7, 8, 9]),
    ...             ]
    ...         }
    ...     )
    ... )
    >>> result = np_table.reduce(data_sum=pw.reducers.sum(np_table.data))
    >>> pw.debug.compute_and_print(result, include_id=False)
    data_sum
    [12 15 18]
    """
    return _apply_unary_reducer(_sum, arg)


def argmin(arg: expr.ColumnExpression) -> expr.ReducerExpression:
    """
    Returns the index of the minimum aggregated value.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... colA | colB
    ... valA | -1
    ... valA |  1
    ... valA |  2
    ... valB |  4
    ... valB |  4
    ... valB |  7
    ... ''')
    >>> pw.debug.compute_and_print(t)
                | colA | colB
    ^X1MXHYY... | valA | -1
    ^YYY4HAB... | valA | 1
    ^Z3QWT29... | valA | 2
    ^3CZ78B4... | valB | 4
    ^3HN31E1... | valB | 4
    ^3S2X6B2... | valB | 7
    >>> result = t.groupby(t.colA).reduce(argmin=pw.reducers.argmin(t.colB), min=pw.reducers.min(t.colB))
    >>> pw.debug.compute_and_print(result, include_id=False)
    argmin      | min
    ^X1MXHYY... | -1
    ^3CZ78B4... | 4
    """
    return _apply_unary_reducer(_argmin, arg)


def argmax(arg: expr.ColumnExpression) -> expr.ReducerExpression:
    """
    Returns the index of the maximum aggregated value.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... colA | colB
    ... valA | -1
    ... valA |  1
    ... valA |  2
    ... valB |  4
    ... valB |  4
    ... valB |  7
    ... ''')
    >>> pw.debug.compute_and_print(t)
                | colA | colB
    ^X1MXHYY... | valA | -1
    ^YYY4HAB... | valA | 1
    ^Z3QWT29... | valA | 2
    ^3CZ78B4... | valB | 4
    ^3HN31E1... | valB | 4
    ^3S2X6B2... | valB | 7
    >>> result = t.groupby(t.colA).reduce(argmax=pw.reducers.argmax(t.colB), max=pw.reducers.max(t.colB))
    >>> pw.debug.compute_and_print(result, include_id=False)
    argmax      | max
    ^Z3QWT29... | 2
    ^3S2X6B2... | 7
    """
    return _apply_unary_reducer(_argmax, arg)


def unique(arg: expr.ColumnExpression) -> expr.ReducerExpression:
    """
    Returns aggregated value, if all values are identical. If values are not identical, exception is raised.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ...    | colA | colB | colD
    ... 1  | valA |  1   |  3
    ... 2  | valA |  1   |  3
    ... 3  | valA |  1   |  3
    ... 4  | valB |  2   |  4
    ... 5  | valB |  2   |  5
    ... 6  | valB |  2   |  6
    ... ''')
    >>> result = t.groupby(t.colA).reduce(unique_B=pw.reducers.unique(t.colB))
    >>> pw.debug.compute_and_print(result, include_id=False)
    unique_B
    1
    2
    >>> result = t.groupby(t.colA).reduce(unique_D=pw.reducers.unique(t.colD))
    >>> try:
    ...   pw.debug.compute_and_print(result, include_id=False)
    ... except Exception as e:
    ...   print(type(e))
    <class 'pathway.engine.EngineError'>
    """
    return _apply_unary_reducer(_unique, arg)


def any(arg: expr.ColumnExpression) -> expr.ReducerExpression:
    """
    Returns any of the aggregated values. Values are consistent across application to many columns.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ...    | colA | colB | colD
    ... 1  | valA | -1   |  4
    ... 2  | valA |  1   |  7
    ... 3  | valA |  2   | -3
    ... 4  | valB |  4   |  2
    ... 5  | valB |  5   |  6
    ... 6  | valB |  7   |  1
    ... ''')
    >>> result = t.groupby(t.colA).reduce(
    ...     any_B=pw.reducers.any(t.colB),
    ...     any_D=pw.reducers.any(t.colD),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    any_B | any_D
    2     | -3
    7     | 1
    """
    return _apply_unary_reducer(_any, arg)


def sorted_tuple(
    arg: expr.ColumnExpression, *, skip_nones: bool = False
) -> expr.ReducerExpression:
    """
    Return a sorted tuple containing all the aggregated values. If optional argument skip_nones is
    set to True, any Nones in aggregated values are omitted from the result.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ...    | colA | colB | colD
    ... 1  | valA | -1   |  4
    ... 2  | valA |  1   |  7
    ... 3  | valA |  2   | -3
    ... 4  | valB |  4   |
    ... 5  | valB |  4   |  6
    ... 6  | valB |  7   |  1
    ... ''')
    >>> result = t.groupby(t.colA).reduce(
    ...     tuple_B=pw.reducers.sorted_tuple(t.colB),
    ...     tuple_D=pw.reducers.sorted_tuple(t.colD, skip_nones=True),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    tuple_B    | tuple_D
    (-1, 1, 2) | (-3, 4, 7)
    (4, 4, 7)  | (1, 6)
    """
    return _apply_unary_reducer(_sorted_tuple(skip_nones), arg, skip_nones=skip_nones)


def tuple(arg: expr.ColumnExpression, *, skip_nones: bool = False):
    """
    Return a tuple containing all the aggregated values. Order of values inside a tuple
    is consistent across application to many columns. If optional argument skip_nones is
    set to True, any Nones in aggregated values are omitted from the result.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ...    | colA | colB | colC | colD
    ... 1  | valA | -1   |   5  |  4
    ... 2  | valA |  1   |   5  |  7
    ... 3  | valA |  2   |   5  | -3
    ... 4  | valB |  4   |  10  |  2
    ... 5  | valB |  4   |  10  |  6
    ... 6  | valB |  7   |  10  |  1
    ... ''')
    >>> result = t.groupby(t.colA).reduce(
    ...     tuple_B=pw.reducers.tuple(t.colB),
    ...     tuple_C=pw.reducers.tuple(t.colC),
    ...     tuple_D=pw.reducers.tuple(t.colD),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    tuple_B    | tuple_C      | tuple_D
    (-1, 1, 2) | (5, 5, 5)    | (4, 7, -3)
    (4, 4, 7)  | (10, 10, 10) | (2, 6, 1)
    """
    return _apply_unary_reducer(_tuple(skip_nones), arg, skip_nones=skip_nones)


def npsum(arg):
    warn("Using pathway.reducers.npsum() is deprecated, use pathway.reducers.sum()")
    return sum(arg)


def count(*args):
    """
    Returns the number of aggregated elements.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... colA | colB
    ... valA | -1
    ... valA |  1
    ... valA |  2
    ... valB |  4
    ... valB |  4
    ... valB |  7
    ... ''')
    >>> result = t.groupby(t.colA).reduce(count=pw.reducers.count())
    >>> pw.debug.compute_and_print(result, include_id=False)
    count
    3
    3
    """
    if args:
        warn(
            "Passing argument to pathway.reducers.count() is deprecated, use pathway.reducers.count() "
            + "without any arguments."
        )
    return expr.CountExpression()


def avg(expression: expr.ColumnExpression) -> expr.ColumnExpression:
    """
    Returns the average of the aggregated values.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ... colA | colB
    ... valA | -1
    ... valA |  1
    ... valA |  2
    ... valB |  4
    ... valB |  4
    ... valB |  7
    ... ''')
    >>> result = t.groupby(t.colA).reduce(avg=pw.reducers.avg(t.colB))
    >>> pw.debug.compute_and_print(result, include_id=False)
    avg
    0.6666666666666666
    5.0
    """
    return sum(expression) / count()


def int_sum(expression: expr.ColumnExpression):
    warn(
        "Reducer pathway.reducers.int_sum is deprecated, use pathway.reducers.sum instead."
    )
    return sum(expression)


def ndarray(expression: expr.ColumnExpression, *, skip_nones: bool = False):
    """
    Returns an array containing all the aggregated values. Order of values inside an array
    is consistent across application to many columns. If optional argument skip_nones is
    set to True, any Nones in aggregated values are omitted from the result.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown('''
    ...    | colA | colB | colD
    ... 1  | valA | -1   |  4
    ... 2  | valA |  1   |  7
    ... 3  | valA |  2   | -3
    ... 4  | valB |  4   |
    ... 5  | valB |  4   |  6
    ... 6  | valB |  7   |  1
    ... ''')
    >>> result = t.groupby(t.colA).reduce(
    ...     array_B=pw.reducers.ndarray(t.colB),
    ...     array_D=pw.reducers.ndarray(t.colD, skip_nones=True),
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    array_B    | array_D
    [4 4 7]    | [6 1]
    [-1  1  2] | [ 4  7 -3]
    """
    return apply_with_type(
        np.array, np.ndarray, tuple(expression, skip_nones=skip_nones)
    )

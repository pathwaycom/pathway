# Copyright © 2024 Pathway

import pickle
from abc import ABC, abstractmethod
from collections import Counter
from typing import ParamSpec, Protocol, TypeVar

from typing_extensions import Self

from pathway.internals import api, expression as expr
from pathway.internals.column import ColumnExpression
from pathway.internals.common import apply_with_type
from pathway.internals.reducers import StatefulManyReducer
from pathway.internals.shadows.inspect import signature

P = ParamSpec("P")


S = TypeVar("S", bound=api.Value)
V1 = TypeVar("V1", bound=api.Value)
V2 = TypeVar("V2", bound=api.Value)


def mark_stub(fun):
    fun.__pw_stub = True
    return fun


class ReducerProtocol(Protocol):
    def __call__(
        self,
        *args: expr.ColumnExpression | api.Value | tuple[expr.ColumnExpression, ...],
    ) -> expr.ColumnExpression: ...


def stateful_many(
    combine_many: api.CombineMany[S],
) -> ReducerProtocol:
    """Decorator used to create custom stateful reducers.

    A function wrapped with it has to process the previous state and a list of updates
    at a specific time. It has to return a new state. The updates are grouped in batches
    (all updates in a batch have the same processing time, the function is called once
    per batch) and the batches enter the function in order of increasing processing time.

    Example:

    Create a table where ``__time__`` column simulates processing time assigned
    to entries when they enter pathway:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ...      a | b | __time__
    ...      3 | 1 |     2
    ...      4 | 1 |     2
    ...     13 | 2 |     2
    ...     16 | 2 |     4
    ...      2 | 2 |     6
    ...      4 | 1 |     6
    ... '''
    ... )

    Now create a custom stateful reducer. It is going to compute a weird sum.
    It is a sum of even entries incremented by 1 and unchanged odd entries.

    >>> @pw.reducers.stateful_many
    ... def weird_sum(state: int | None, rows: list[tuple[list[int], int]]) -> int:
    ...     if state is None:
    ...         state = 0
    ...     for row, cnt in rows:
    ...         value = row[0]
    ...         if value % 2 == 0:
    ...             state += value + 1
    ...         else:
    ...             state += value
    ...     return state

    ``state`` is ``None`` when the function is called for the first time for a given group.
    To compute a weird sum, you should set it to 0 then.

    ``row`` is a list of values passed to the reducer. When the reducer is called as
    ``weird_sum(pw.this.a)``, the list has only one element, i.e. value from the column a.
    ``cnt`` tells whether the row is an insertion (``cnt == 1``) or deletion (``cnt == -1``).
    You can learn more `here </developers/user-guide/introduction/concepts#the-output-is-a-data-stream>`_.

    You can now use the reducer in ``reduce`` operator and compute the result:

    >>> result = table.groupby(pw.this.b).reduce(pw.this.b, s=weird_sum(pw.this.a))
    >>> pw.debug.compute_and_print(result, include_id=False)
    b | s
    1 | 13
    2 | 33

    ``weird_sum`` is called 2 times for group 1 (at processing times 2 and 6) and 3 times
    for group 2 (at processing times 2, 4, 6).
    """

    def wrapper(
        *args: expr.ColumnExpression | api.Value | tuple[expr.ColumnExpression, ...],
    ) -> expr.ColumnExpression:
        return expr.ReducerExpression(StatefulManyReducer(combine_many), *args)

    return wrapper


class CombineSingle(Protocol[S, P]):
    def __call__(self, state: S | None, /, *args: P.args, **kwargs: P.kwargs) -> S: ...


def stateful_single(combine_single: CombineSingle[S, ...]) -> ReducerProtocol:
    """Decorator used to create custom stateful reducers.

    A function wrapped with it has to process the previous state and a single update.
    It has to return a new state. The function is called with entries in order of
    increasing processing time. If there are multiple entries with the same processing
    time, their order is unspecified.

    The function can only be used on tables with insertions only (no updates or deletions).
    If you need to handle updates/deletions, see
    `stateful_many </developers/api-docs/reducers#pathway.reducers.stateful_many>`_.

    Example:

    Create a table where ``__time__`` column simulates processing time assigned
    to entries when they enter pathway:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ...      a | b | __time__
    ...      3 | 1 |     2
    ...      4 | 1 |     2
    ...     13 | 2 |     2
    ...     16 | 2 |     4
    ...      2 | 2 |     6
    ...      4 | 1 |     6
    ... '''
    ... )

    Create a custom stateful reducer. It is going to compute a weird sum.
    It is a sum of even entries incremented by 1 and unchanged odd entries.

    >>> @pw.reducers.stateful_single
    ... def weird_sum(state: int | None, value) -> int:
    ...     if state is None:
    ...         state = 0
    ...     if value % 2 == 0:
    ...         state += value + 1
    ...     else:
    ...         state += value
    ...     return state

    ``state`` is ``None`` when the function is called for the first time for a given group.
    To compute a weird sum, you should set it to 0 then.

    You can now use the reducer in ``reduce`` operator and compute the result:

    >>> result = table.groupby(pw.this.b).reduce(pw.this.b, s=weird_sum(pw.this.a))
    >>> pw.debug.compute_and_print(result, include_id=False)
    b | s
    1 | 13
    2 | 33
    """

    def wrapper(state: S | None, rows: list[tuple[list[api.Value], int]]) -> S:
        for row, count in rows:
            assert count > 0
            for _ in range(count):
                state = combine_single(state, *row)
        assert state is not None
        return state

    return stateful_many(wrapper)


class BaseCustomAccumulator(ABC):
    """Utility class for defining custom accumulators, used for stateful reducers.
    Custom accumulators should inherit from this class, and should implement ``from_row``,
    ``update`` and ``compute_result``. Optionally ``neutral`` and ``retract`` can be provided
    for more efficient processing on streams with changing data. Additionally, ``serialize``
    and ``deserialize`` can be customized. By default they use ``pickle`` module,
    but if the accumulator state is serializable to pathway value type in an easier way,
    this can be overwritten.

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
    >>> import sys; sys.modules[__name__].CustomAvgAccumulator = CustomAvgAccumulator # NODOCS
    >>> custom_avg = pw.reducers.udf_reducer(CustomAvgAccumulator)
    >>> t1 = pw.debug.table_from_markdown('''
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

    def serialize(self) -> api.Value:
        """Serialize state to pathway value type."""
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, val: api.Value):
        """Deserialize state from pathway value type."""
        assert isinstance(val, bytes)
        return pickle.loads(val)


def udf_reducer(reducer_cls: type[BaseCustomAccumulator]):
    """Decorator for defining stateful reducers. Requires custom accumulator as an argument.
    Custom accumulator should implement ``from_row``, ``update`` and ``compute_result``.
    Optionally ``neutral`` and ``retract`` can be provided for more efficient processing on
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
    >>> import sys; sys.modules[__name__].CustomAvgAccumulator = CustomAvgAccumulator # NODOCS
    >>> custom_avg = pw.reducers.udf_reducer(CustomAvgAccumulator)
    >>> t1 = pw.debug.table_from_markdown('''
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
            packed_state: tuple[api.Value, tuple, int] | None,
            rows: list[tuple[list[api.Value], int]],
        ) -> tuple[api.Value, tuple, int] | None:
            if packed_state is not None:
                serialized_state, _positive_updates_tuple, _cnt = packed_state
                state = reducer_cls.deserialize(serialized_state)
                _positive_updates = list(_positive_updates_tuple)
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
                    assert _positive_updates is not None
                    positive_updates.extend(_positive_updates)
                    _positive_updates = []
                    state = None
                acc = Counter(positive_updates)
                acc.subtract(negative_updates)
                assert all(x >= 0 for x in acc.values())
                positive_updates = list(acc.elements())
                negative_updates = []

            if state is None:
                if neutral_available:
                    state = reducer_cls.neutral()
                    _positive_updates = []
                    _cnt = 0
                elif len(positive_updates) == 0:
                    if len(negative_updates) == 0:
                        return None
                    else:
                        raise ValueError(
                            "Unable to process negative update with this stateful reducer."
                        )
                else:
                    state = reducer_cls.from_row(list(positive_updates[0]))
                    if not retract_available:
                        _positive_updates = positive_updates[0:1]
                        _cnt = 0
                    else:
                        _positive_updates = []
                        _cnt = 1
                    positive_updates = positive_updates[1:]

            for row_up in positive_updates:
                if not retract_available:
                    _positive_updates.append(row_up)
                else:
                    _cnt += 1
                val = reducer_cls.from_row(list(row_up))
                state.update(val)

            for row_up in negative_updates:
                if not retract_available:
                    raise ValueError(
                        "Unable to process negative update with this stateful reducer."
                    )
                else:
                    _cnt -= 1
                val = reducer_cls.from_row(list(row_up))
                state.retract(val)

            _positive_updates_tuple = tuple(tuple(x) for x in _positive_updates)
            if retract_available and _cnt == 0:
                # this is fine in this setting, where we process values one by one
                # if this ever becomes accumulated in a tree, we have to handle
                # (A-B) updates, so we have to distinguish `0` from intermediate states
                # accumulating weighted count (weighted by hash) should do fine here
                return None
            return state.serialize(), _positive_updates_tuple, _cnt

        def extractor(packed: tuple):
            deserialized = reducer_cls.deserialize(packed[0])
            assert isinstance(deserialized, reducer_cls)
            return deserialized.compute_result()

        return apply_with_type(
            extractor,
            signature(reducer_cls.compute_result).return_annotation,
            stateful_wrapper(*args),
        )

    return wrapper


def _is_overridden(cls: type[BaseCustomAccumulator], name: str) -> bool:
    assert hasattr(BaseCustomAccumulator, name)
    return not hasattr(getattr(cls, name), "__pw_stub")

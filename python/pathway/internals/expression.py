# Copyright © 2024 Pathway

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Mapping, cast

from pathway.internals import api, dtype as dt, helpers
from pathway.internals.operator_input import OperatorInput
from pathway.internals.shadows import operator
from pathway.internals.trace import Trace

if TYPE_CHECKING:
    from pathway.internals.api import Value
    from pathway.internals.column import Column, ColumnWithExpression
    from pathway.internals.expressions import (
        DateTimeNamespace,
        NumericalNamespace,
        StringNamespace,
    )
    from pathway.internals.reducers import Reducer
    from pathway.internals.table import Table


@dataclasses.dataclass(frozen=True)
class InternalColExpr:
    kind: type[ColumnExpression]
    args: tuple[Any, ...]
    kwargs: tuple[tuple[str, Any], ...]

    @staticmethod
    def build(kind: type[ColumnExpression], *args, **kwargs) -> InternalColExpr:
        def wrap(arg):
            if isinstance(arg, ColumnExpression):
                return arg._to_internal()
            else:
                return arg

        args = tuple(wrap(arg) for arg in args)
        kwargs = {name: wrap(kwarg) for name, kwarg in kwargs.items()}

        return InternalColExpr(kind, args, tuple(sorted(kwargs.items())))

    def __post_init__(self):
        assert issubclass(self.kind, ColumnExpression)

    def to_column_expression(self) -> ColumnExpression:
        def wrap(arg):
            if isinstance(arg, InternalColExpr):
                return arg.to_column_expression()
            else:
                return arg

        args = tuple(wrap(arg) for arg in self.args)
        kwargs = {name: wrap(kwarg) for name, kwarg in self.kwargs}
        return self.kind(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class InternalColRef(InternalColExpr):
    kind: type[ColumnReference]

    def to_column_expression(self) -> ColumnReference:
        return cast(ColumnReference, super().to_column_expression())

    @staticmethod
    def build(kind, *args, **kwargs) -> InternalColRef:
        assert kind == ColumnReference
        ret = InternalColExpr.build(kind, *args, **kwargs)
        return InternalColRef(kind, args=ret.args, kwargs=ret.kwargs)

    @property
    def _name(self) -> str:
        return self.to_column_expression()._name

    @property
    def _table(self) -> Table:
        return self.to_column_expression()._table

    @property
    def _column(self) -> Column:
        return self.to_column_expression()._column


class ColumnExpression(OperatorInput, ABC):
    _dtype: dt.DType
    _trace: Trace

    def __init__(self):
        self._trace = Trace.from_traceback()

    def __bool__(self):
        raise RuntimeError("Cannot use expression as boolean.")

    @property
    @abstractmethod
    def _deps(self) -> tuple[ColumnExpression, ...]: ...

    @abstractmethod
    def _to_internal(self) -> InternalColExpr: ...

    def __repr__(self):
        from pathway.internals.expression_printer import ExpressionFormatter

        return ExpressionFormatter().eval_expression(self)

    @staticmethod
    def _wrap(
        arg: ColumnExpression | Value | tuple[ColumnExpression, ...]
    ) -> ColumnExpression:
        if isinstance(arg, tuple):
            return MakeTupleExpression(*arg) if arg else ColumnConstExpression(())
        if not isinstance(arg, ColumnExpression):
            return ColumnConstExpression(arg)
        return arg

    @lru_cache
    def _dependencies(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet.union(*[dep._dependencies() for dep in self._deps])

    @lru_cache
    def _dependencies_above_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet.union(
            *[dep._dependencies_above_reducer() for dep in self._deps]
        )

    @lru_cache
    def _dependencies_below_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet.union(
            *[dep._dependencies_below_reducer() for dep in self._deps]
        )

    @lru_cache
    def _operator_dependencies(self) -> helpers.StableSet[Table]:
        return helpers.StableSet(
            expression.to_column_expression()._table
            for expression in self._dependencies()
        )

    @lru_cache
    def _column_dependencies(self) -> helpers.StableSet[Column]:
        expression_dependencies = (
            dep.to_column_expression()._column for dep in self._dependencies()
        )
        return helpers.StableSet(expression_dependencies)

    @property
    def _column_with_expression_cls(self) -> type[ColumnWithExpression]:
        from pathway.internals.column import ColumnWithExpression

        return ColumnWithExpression

    def __add__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.add)

    def __radd__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.add)

    def __sub__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.sub)

    def __rsub__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.sub)

    def __mul__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.mul)

    def __rmul__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.mul)

    def __truediv__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.truediv)

    def __rtruediv__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.truediv)

    def __floordiv__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.floordiv)

    def __rfloordiv__(
        self, other: ColumnExpression | Value
    ) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.floordiv)

    def __mod__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.mod)

    def __rmod__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.mod)

    def __pow__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.pow)

    def __rpow__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.pow)

    def __lshift__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.lshift)

    def __rlshift__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.lshift)

    def __rshift__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.rshift)

    def __rrshift__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.rshift)

    def __eq__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:  # type: ignore[override]
        return ColumnBinaryOpExpression(self, other, operator.eq)

    def __ne__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:  # type: ignore[override]
        return ColumnBinaryOpExpression(self, other, operator.ne)

    def __le__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.le)

    def __ge__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.ge)

    def __lt__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.lt)

    def __gt__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.gt)

    def __and__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.and_)

    def __rand__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.and_)

    def __or__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.or_)

    def __ror__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.or_)

    def __xor__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.xor)

    def __rxor__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.xor)

    def __matmul__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.matmul)

    def __rmatmul__(self, other: ColumnExpression | Value) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.matmul)

    def __neg__(self) -> ColumnUnaryOpExpression:
        return ColumnUnaryOpExpression(self, operator.neg)

    def __invert__(self) -> ColumnExpression:
        match self:
            case ColumnUnaryOpExpression(_operator=operator.inv, _expr=e):
                return e
            case IsNoneExpression(_expr=e):
                return IsNotNoneExpression(e)
            case IsNotNoneExpression(_expr=e):
                return IsNoneExpression(e)
            case _:
                return ColumnUnaryOpExpression(self, operator.inv)

    def __abs__(self) -> ColumnExpression:
        return self.num.abs()

    def __hash__(self):
        return object.__hash__(self)

    def is_none(self) -> IsNoneExpression:
        """Returns true if the value is None.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | owner | pet
        ... 1 | Alice | dog
        ... 2 | Bob   |
        ... 3 | Carol | cat
        ... ''')
        >>> t2 = t1.with_columns(has_no_pet=pw.this.pet.is_none())
        >>> pw.debug.compute_and_print(t2, include_id=False)
        owner | pet | has_no_pet
        Alice | dog | False
        Bob   |     | True
        Carol | cat | False
        """
        return IsNoneExpression(self)

    def is_not_none(self) -> IsNotNoneExpression:
        """Returns true if the value is not None.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | owner | pet
        ... 1 | Alice | dog
        ... 2 | Bob   |
        ... 3 | Carol | cat
        ... ''')
        >>> t2 = t1.with_columns(has_pet=pw.this.pet.is_not_none())
        >>> pw.debug.compute_and_print(t2, include_id=False)
        owner | pet | has_pet
        Alice | dog | True
        Bob   |     | False
        Carol | cat | True
        """
        return IsNotNoneExpression(self)

    # Missing `__iter__` would make Python fall back to `__getitem__, which
    # will not do the right thing.
    __iter__ = None

    def __getitem__(self, index: ColumnExpression | int | str) -> ColumnExpression:
        """Extracts element at `index` from an object. The object has to be a Tuple or Json.

        Index can be effectively `int` for Tuple and `int` or `str` for Json.
        For Tuples, using negative index can be used to access elements at the end, moving backwards.

        if no element is present at `index`:
            - returns `json(null)` for Json
            - raises error for Tuple

        Args:
            index: Position to extract element at.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown(
        ...     '''
        ...   | a | b | c
        ... 1 | 3 | 2 | 1
        ... 2 | 4 | 1 | 0
        ... 3 | 7 | 3 | 1
        ... '''
        ... )
        >>> t2 = t1.with_columns(tup=pw.make_tuple(pw.this.a, pw.this.b))
        >>> t3 = t2.select(x=pw.this.tup[0], y=pw.this.tup[-1], z=pw.this.tup[pw.this.c])
        >>> pw.debug.compute_and_print(t3, include_id=False)
        x | y | z
        3 | 2 | 2
        4 | 1 | 4
        7 | 3 | 3
        """
        return GetExpression(self, index, check_if_exists=False)

    def get(
        self,
        index: ColumnExpression | int | str,
        default: ColumnExpression | Value = None,
    ) -> ColumnExpression:
        """Extracts element at `index` from an object. The object has to be a Tuple or Json.
        If no element is present at `index`, it returns value specified by a `default` parameter.

        Index can be effectively `int` for Tuple and `int` or `str` for Json.
        For Tuples, using negative index can be used to access elements at the end, moving backwards.

        Args:
            index: Position to extract element at.
            default: Value returned when no element is at position `index`. Defaults to None.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown(
        ...     '''
        ...   | a | b | c
        ... 1 | 3 | 2 | 2
        ... 2 | 4 | 1 | 0
        ... 3 | 7 | 3 | 1
        ... '''
        ... )
        >>> t2 = t1.with_columns(tup=pw.make_tuple(pw.this.a, pw.this.b))
        >>> t3 = t2.select(
        ...     x=pw.this.tup.get(1),
        ...     y=pw.this.tup.get(3),
        ...     z=pw.this.tup.get(pw.this.c),
        ...     t=pw.this.tup.get(pw.this.c, default=100),
        ... )
        >>> pw.debug.compute_and_print(t3, include_id=False)
        x | y | z | t
        1 |   | 4 | 4
        2 |   |   | 100
        3 |   | 3 | 3
        """
        return GetExpression(self, index, default, check_if_exists=True)

    @property
    def dt(self) -> DateTimeNamespace:
        from pathway.internals.expressions import DateTimeNamespace

        return DateTimeNamespace(self)

    @property
    def num(self) -> NumericalNamespace:
        from pathway.internals.expressions import NumericalNamespace

        return NumericalNamespace(self)

    @property
    def str(self) -> StringNamespace:
        from pathway.internals.expressions import StringNamespace

        return StringNamespace(self)

    def to_string(self) -> MethodCallExpression:
        """Changes the values to strings.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... val
        ... 1
        ... 2
        ... 3
        ... 4''')
        >>> t1.schema
        <pathway.Schema types={'val': <class 'int'>}, id_type=<class 'pathway.engine.Pointer'>>
        >>> pw.debug.compute_and_print(t1, include_id=False)
        val
        1
        2
        3
        4
        >>> t2 = t1.select(val = pw.this.val.to_string())
        >>> t2.schema
        <pathway.Schema types={'val': <class 'str'>}, id_type=<class 'pathway.engine.Pointer'>>
        >>> pw.debug.compute_and_print(t2.select(val=pw.this.val + "a"), include_id=False)
        val
        1a
        2a
        3a
        4a
        """
        return MethodCallExpression(
            (
                (
                    dt.ANY,
                    dt.STR,
                    api.Expression.to_string,
                ),
            ),
            "to_string",
            self,
        )

    def as_int(
        self, *, default: ColumnExpression | None = None, unwrap: bool = False
    ) -> ConvertExpression:
        """Converts the value to `int`. Currently, this is supported only for JSON columns.

        The resulting column type will be `int | None`, unless `unwrap` is set to `True`
        or `default` is non-optional.

        Raises an exception for incompatible values.

        Args:
            default: The value to return in case of `Json.NULL` or `None`. Defaults to `None`.
            unwrap: If `True`, the result column type will be non-optional. Raises an exception
                if a `Json.NULL` or `None` value is encountered. Defaults to `False`.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> class InputSchema(pw.Schema):
        ...     data: dict
        >>> dt = pd.DataFrame(data={"data": [{"value": 1}, {"value": 2}]})
        >>> table = pw.debug.table_from_pandas(dt, schema=InputSchema)
        >>> result = table.select(result=pw.this.data.get("value").as_int())
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        1
        2
        """
        return ConvertExpression(dt.INT, self, default, unwrap)

    def as_float(
        self, default: ColumnExpression | None = None, unwrap: bool = False
    ) -> ConvertExpression:
        """Converts the value to `float`. Currently, this is supported only for JSON columns.

        The resulting column type will be `float | None`, unless `unwrap` is set to `True`
        or `default` is non-optional.

        Raises an exception for incompatible values.

        Args:
            default: The value to return in case of `Json.NULL` or `None`. Defaults to `None`.
            unwrap: If `True`, the result column type will be non-optional. Raises an exception
                if a `Json.NULL` or `None` value is encountered. Defaults to `False`.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> class InputSchema(pw.Schema):
        ...     data: dict
        >>> dt = pd.DataFrame(data={"data": [{"value": 1.5}, {"value": 3.14}]})
        >>> table = pw.debug.table_from_pandas(dt, schema=InputSchema)
        >>> result = table.select(result=pw.this.data.get("value").as_float())
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        1.5
        3.14
        """
        return ConvertExpression(dt.FLOAT, self, default, unwrap)

    def as_str(
        self, default: ColumnExpression | None = None, unwrap: bool = False
    ) -> ConvertExpression:
        """Converts the value to `str`. Currently, this is supported only for JSON columns.

        The resulting column type will be `str | None`, unless `unwrap` is set to `True`
        or `default` is non-optional.

        Raises an exception for incompatible values.

        Args:
            default: The value to return in case of `Json.NULL` or `None`. Defaults to `None`.
            unwrap: If `True`, the result column type will be non-optional. Raises an exception
                if a `Json.NULL` or `None` value is encountered. Defaults to `False`.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> class InputSchema(pw.Schema):
        ...     data: dict
        >>> dt = pd.DataFrame(data={"data": [{"value": "dog"}, {"value": "cat"}]})
        >>> table = pw.debug.table_from_pandas(dt, schema=InputSchema)
        >>> result = table.select(result=pw.this.data.get("value").as_str())
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        cat
        dog
        """
        return ConvertExpression(dt.STR, self, default, unwrap)

    def as_bool(
        self, default: ColumnExpression | None = None, unwrap: bool = False
    ) -> ConvertExpression:
        """Converts the value to `bool`. Currently, this is supported only for JSON columns.

        The resulting column type will be `bool | None`, unless `unwrap` is set to `True`
        or `default` is non-optional.

        Raises an exception for incompatible values

        Args:
            default: The value to return in case of `Json.NULL` or `None`. Defaults to `None`.
            unwrap: If `True`, the result column type will be non-optional. Raises an exception
                if a `Json.NULL` or `None` value is encountered. Defaults to `False`.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> class InputSchema(pw.Schema):
        ...     data: dict
        >>> dt = pd.DataFrame(data={"data": [{"value": True}, {"value": False}]})
        >>> table = pw.debug.table_from_pandas(dt, schema=InputSchema)
        >>> result = table.select(result=pw.this.data.get("value").as_bool())
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        False
        True
        """
        return ConvertExpression(dt.BOOL, self, default, unwrap)


class ColumnCallExpression(ColumnExpression):
    _args: tuple[ColumnExpression, ...]
    _col_expr: ColumnReference

    def __init__(
        self, col_expr: ColumnReference, args: Iterable[ColumnExpression | Value]
    ):
        super().__init__()
        self._col_expr = col_expr
        self._args = tuple(ColumnExpression._wrap(arg) for arg in args)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._col_expr, self._args)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return self._args


class ColumnConstExpression(ColumnExpression):
    _val: Value

    def __init__(self, val: Value):
        super().__init__()
        self._val = val

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._val)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return ()


class ColumnReference(ColumnExpression):
    """Reference to the column.

    Inherits from ColumnExpression.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ...    age  owner  pet
    ... 1   10  Alice  dog
    ... 2    9    Bob  dog
    ... 3    8  Alice  cat
    ... 4    7    Bob  dog''')
    >>> isinstance(t1.age, pw.ColumnReference)
    True
    >>> isinstance(t1["owner"], pw.ColumnReference)
    True
    """

    _column: Column
    _table: Table
    _name: str

    def __init__(self, _column: Column, _table: Table, _name: str):
        super().__init__()
        self._column = _column
        self._table = _table
        self._name = _name

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return ()

    def _to_internal(self) -> InternalColRef:
        return InternalColRef.build(type(self), self._column, self._table, self._name)

    def _to_original(self) -> ColumnReference:
        return self._column.lineage.table[self._column.lineage.name]

    @property
    def table(self):
        """Table where the referred column belongs to.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...    age  owner  pet
        ... 1   10  Alice  dog
        ... 2    9    Bob  dog
        ... 3    8  Alice  cat
        ... 4    7    Bob  dog''')
        >>> t1.age.table is t1
        True
        """
        return self._table

    @property
    def name(self):
        """Name of the referred column.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...    age  owner  pet
        ... 1   10  Alice  dog
        ... 2    9    Bob  dog
        ... 3    8  Alice  cat
        ... 4    7    Bob  dog''')
        >>> t1.age.name
        'age'
        """
        return self._name

    @lru_cache
    def _dependencies(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet([self._to_internal()])

    @lru_cache
    def _dependencies_above_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet([self._to_internal()])

    @lru_cache
    def _dependencies_below_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet()

    def __call__(self, *args) -> ColumnExpression:
        return ColumnCallExpression(self, args)

    @property
    def _column_with_expression_cls(self) -> type[ColumnWithExpression]:
        from pathway.internals.column import ColumnWithReference

        return ColumnWithReference


class ColumnBinaryOpExpression(ColumnExpression):
    _left: ColumnExpression
    _right: ColumnExpression
    _operator: Callable[[Any, Any], Any]

    def __init__(
        self,
        left: ColumnExpression | Value,
        right: ColumnExpression | Value,
        operator: Callable[[Any, Any], Any],
    ):
        super().__init__()
        self._left = ColumnExpression._wrap(left)
        self._right = ColumnExpression._wrap(right)
        self._operator = operator

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._left, self._right)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self), self._left, self._right, self._operator
        )


class ColumnUnaryOpExpression(ColumnExpression):
    _expr: ColumnExpression
    _operator: Callable[[Any], Any]

    def __init__(self, expr: ColumnExpression | Value, operator: Callable[[Any], Any]):
        super().__init__()
        self._expr = ColumnExpression._wrap(expr)
        self._operator = operator

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr,)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._expr, self._operator)


class ReducerExpression(ColumnExpression):
    _reducer: Reducer
    _args: tuple[ColumnExpression, ...]
    # needed only for repr
    _kwargs: dict[str, ColumnExpression]

    def __init__(
        self,
        reducer: Reducer,
        *args: ColumnExpression | Value | tuple[ColumnExpression, ...],
        **kwargs: ColumnExpression | Value | tuple[ColumnExpression, ...],
    ):
        super().__init__()
        self._reducer = reducer
        self._args = tuple(ColumnExpression._wrap(arg) for arg in args)
        self._kwargs = {k: ColumnExpression._wrap(v) for k, v in kwargs.items()}

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return self._args

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self), self._reducer, *self._args, **self._kwargs
        )

    @lru_cache
    def _dependencies_above_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet()

    @lru_cache
    def _dependencies_below_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet.union(
            *[dep._dependencies_above_reducer() for dep in self._deps]
        )


class ApplyExpression(ColumnExpression):
    _return_type: dt.DType
    _propagate_none: bool
    _deterministic: bool
    _check_for_disallowed_types: bool
    _args: tuple[ColumnExpression, ...]
    _kwargs: dict[str, ColumnExpression]
    _fun: Callable

    def __init__(
        self,
        fun: Callable,
        return_type: Any,
        propagate_none: bool,
        deterministic: bool,
        args: tuple[ColumnExpression | Value, ...],
        kwargs: Mapping[str, ColumnExpression | Value],
        _check_for_disallowed_types: bool = True,
    ):
        super().__init__()
        self._fun = fun
        self._return_type = dt.wrap(return_type)
        self._propagate_none = propagate_none
        self._deterministic = deterministic
        self._check_for_disallowed_types = _check_for_disallowed_types

        self._args = tuple(ColumnExpression._wrap(arg) for arg in args)

        self._kwargs = {k: ColumnExpression._wrap(v) for k, v in kwargs.items()}
        assert len(args) + len(kwargs) > 0

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (*self._args, *self._kwargs.values())

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self),
            self._fun,
            self._return_type,
            self._propagate_none,
            self._deterministic,
            self._check_for_disallowed_types,
            *self._args,
            **self._kwargs,
        )

    @property
    def _maybe_optional_return_type(self) -> dt.DType:
        if self._propagate_none:
            return dt.Optional(self._return_type)
        else:
            return self._return_type


class AsyncApplyExpression(ApplyExpression):
    pass


class FullyAsyncApplyExpression(ApplyExpression):
    autocommit_duration_ms: int | None

    def __init__(
        self,
        fun: Callable,
        return_type: Any,
        propagate_none: bool,
        deterministic: bool,
        autocommit_duration_ms: int | None,
        args: tuple[ColumnExpression | Value, ...],
        kwargs: Mapping[str, ColumnExpression | Value],
    ):
        super().__init__(fun, return_type, propagate_none, deterministic, args, kwargs)
        self.autocommit_duration_ms = autocommit_duration_ms


class CastExpression(ColumnExpression):
    _return_type: dt.DType
    _expr: ColumnExpression

    def __init__(self, return_type: Any, expr: ColumnExpression | Value):
        super().__init__()
        self._return_type = dt.wrap(return_type)
        self._expr = ColumnExpression._wrap(expr)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._return_type, self._expr)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr,)


class ConvertExpression(ColumnExpression):
    _return_type: dt.DType
    _expr: ColumnExpression
    _unwrap: bool
    _default: ColumnExpression

    def __init__(
        self,
        return_type: dt.DType,
        expr: ColumnExpression | Value,
        default: ColumnExpression | Value = None,
        unwrap: bool = False,
    ):
        super().__init__()
        self._return_type = return_type
        self._expr = ColumnExpression._wrap(expr)
        self._default = ColumnExpression._wrap(default)
        self._unwrap = unwrap

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self), self._return_type, self._expr, self._default, self._unwrap
        )

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr, self._default)


class DeclareTypeExpression(ColumnExpression):
    _return_type: Any
    _expr: ColumnExpression

    def __init__(self, return_type: Any, expr: ColumnExpression | Value):
        super().__init__()
        self._return_type = dt.wrap(return_type)
        self._expr = ColumnExpression._wrap(expr)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._return_type, self._expr)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr,)


class CoalesceExpression(ColumnExpression):
    _args: tuple[ColumnExpression, ...]

    def __init__(self, *args: ColumnExpression | Value):
        super().__init__()
        assert len(args) > 0

        def _test_for_none(arg):
            if arg is None:
                return True
            if isinstance(arg, ColumnConstExpression):
                return arg._val is None
            return False

        self._args = tuple(
            ColumnExpression._wrap(arg) for arg in args if not _test_for_none(arg)
        )
        if self._args == ():
            self._args = (ColumnExpression._wrap(None),)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), *self._args)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return self._args


class RequireExpression(ColumnExpression):
    _val: ColumnExpression
    _args: tuple[ColumnExpression, ...]

    def __init__(self, val: ColumnExpression | Value, *args: ColumnExpression | Value):
        super().__init__()
        self._val = ColumnExpression._wrap(val)
        self._args = tuple(ColumnExpression._wrap(arg) for arg in args)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._val, *self._args)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._val, *self._args)


class IfElseExpression(ColumnExpression):
    _if: ColumnExpression
    _then: ColumnExpression
    _else: ColumnExpression

    def __init__(
        self,
        _if: ColumnExpression | Value,
        _then: ColumnExpression | Value,
        _else: ColumnExpression | Value,
    ):
        super().__init__()
        self._if = ColumnExpression._wrap(_if)
        self._then = ColumnExpression._wrap(_then)
        self._else = ColumnExpression._wrap(_else)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._if, self._then, self._else)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._if, self._then, self._else)


class IsNoneExpression(ColumnExpression):
    _expr: ColumnExpression

    def __init__(self, _expr: ColumnExpression | Value):
        super().__init__()
        self._expr = ColumnExpression._wrap(_expr)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._expr)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr,)


class IsNotNoneExpression(ColumnExpression):
    _expr: ColumnExpression

    def __init__(self, _expr: ColumnExpression | Value):
        super().__init__()
        self._expr = ColumnExpression._wrap(_expr)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._expr)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr,)


class PointerExpression(ColumnExpression):
    _table: Table
    _args: tuple[ColumnExpression, ...]
    _instance: ColumnExpression | None
    _optional: bool

    def __init__(
        self,
        table: Table,
        *args: ColumnExpression | Value,
        optional: bool = False,
        instance: ColumnExpression | None,
    ) -> None:
        super().__init__()

        self._args = tuple(ColumnExpression._wrap(arg) for arg in args)
        self._optional = optional
        self._instance = instance
        self._table = table

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self),
            self._table,
            *self._args,
            instance=self._instance,
            optional=self._optional,
        )

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return self._args


class MakeTupleExpression(ColumnExpression):
    _args: tuple[ColumnExpression, ...]

    def __init__(self, *args: ColumnExpression | Value):
        super().__init__()
        self._args = tuple(ColumnExpression._wrap(arg) for arg in args)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), *self._args)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return self._args


class GetExpression(ColumnExpression):
    _object: ColumnExpression
    _index: ColumnExpression
    _default: ColumnExpression
    _check_if_exists: bool
    _const_index: int | str | None

    def __init__(
        self,
        object: ColumnExpression,
        index: ColumnExpression | int | str,
        default: ColumnExpression | Value = None,
        check_if_exists=True,
    ) -> None:
        super().__init__()
        self._object = object
        self._index = ColumnExpression._wrap(index)
        self._default = ColumnExpression._wrap(default)
        self._check_if_exists = check_if_exists
        if isinstance(self._index, ColumnConstExpression) and isinstance(
            self._index._val, (int, str)
        ):
            self._const_index = self._index._val
        else:
            self._const_index = None

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self), self._object, self._index, self._default, self._check_if_exists
        )

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._object, self._index, self._default)


ReturnTypeFunType = Callable[[tuple[Any, ...]], Any]


class MethodCallExpression(ColumnExpression):
    _fun_mapping: tuple[tuple[tuple[dt.DType, ...], dt.DType, Callable], ...]
    _name: str
    _args: tuple[ColumnExpression, ...]
    _args_used_for_repr: tuple[ColumnExpression, ...]

    def __init__(
        self,
        fun_mapping: tuple[
            tuple[tuple[dt.DType, ...] | dt.DType, dt.DType, Callable], ...
        ],
        name: str,
        *args: ColumnExpression | Value,
        args_used_for_repr: Iterable[ColumnExpression | Value] | None = None,
    ) -> None:
        """Creates an Expression that represents a method call on object `args[0]`.

        The implementation can be different depending on the args types. The first
        matching function from fun_mapping is used. The types of the args do not have
        to be an exact match for types in fun_mapping. The keys of fun_mapping are
        analyzed in order and if the args types are castable to a given key,
        the implementation corresponding to this key will be used. No more keys
        will be processed, even if there is an exact match later. Keep that in mind
        when ordering your functions in fun_mapping.

        Args:
            fun_mapping: list of tuples with args types, result type and the
                corresponding API function call. They have to have the form
                (arguments_types, result_type, function).
            name: used to represent the method by `ExpressionFormatter`
            *args: `args[0]` is an object the method is called on `args[1:]` are
                the parameters of the method
            args_used_for_repr: if provided, these will be used when printing
                the expression, rather than `*args`
        """
        super().__init__()
        self._fun_mapping = self._wrap_mapping_key_in_tuple(fun_mapping)
        self._name = name

        self._args = tuple(ColumnExpression._wrap(arg) for arg in args)
        if args_used_for_repr is None:
            self._args_used_for_repr = self._args
        else:
            self._args_used_for_repr = tuple(
                ColumnExpression._wrap(arg) for arg in args_used_for_repr
            )

        assert len(args) > 0
        for key_dtypes, _, _ in self._fun_mapping:
            if len(key_dtypes) != len(self._args):
                raise ValueError(
                    f"In MethodCallExpression the number of args ({len(args)}) has to "
                    + f"be the same as the number of types in key ({len(key_dtypes)})"
                )

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return self._args

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self),
            self._fun_mapping,
            self._name,
            *self._args,
        )

    def _wrap_mapping_key_in_tuple(
        self,
        mapping: tuple[tuple[tuple[dt.DType, ...] | dt.DType, dt.DType, Callable], ...],
    ) -> tuple[tuple[tuple[dt.DType, ...], dt.DType, Callable], ...]:
        return tuple(
            (key if isinstance(key, tuple) else (key,), result_dtype, value)
            for key, result_dtype, value in mapping
        )

    def get_function(
        self, dtypes: tuple[dt.DType, ...]
    ) -> tuple[tuple[dt.DType, ...], dt.DType, Callable] | None:
        for key, target_type, fun in self._fun_mapping:
            assert len(dtypes) == len(key)
            if all(
                dt.dtype_issubclass(arg_dtype, key_dtype)
                for arg_dtype, key_dtype in zip(dtypes, key)
            ):
                return (key, target_type, fun)
        return None


class UnwrapExpression(ColumnExpression):
    _expr: ColumnExpression

    def __init__(self, expr: ColumnExpression | Value):
        super().__init__()
        self._expr = ColumnExpression._wrap(expr)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(
            type(self),
            self._expr,
        )

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr,)


class FillErrorExpression(ColumnExpression):
    _expr: ColumnExpression
    _replacement: ColumnExpression

    def __init__(
        self,
        expr: ColumnExpression | Value,
        replacement: ColumnExpression | Value,
    ):
        super().__init__()
        self._expr = ColumnExpression._wrap(expr)
        self._replacement = ColumnExpression._wrap(replacement)

    def _to_internal(self) -> InternalColExpr:
        return InternalColExpr.build(type(self), self._expr, self._replacement)

    @property
    def _deps(self) -> tuple[ColumnExpression, ...]:
        return (self._expr, self._replacement)


def smart_name(arg: ColumnExpression) -> str | None:
    from pathway.internals.reducers import _any, _unique

    if isinstance(arg, ColumnReference):
        return arg.name
    if isinstance(arg, ReducerExpression) and arg._reducer in [_unique, _any]:
        r_args = arg._args
        if len(r_args) == 1:
            [r_arg] = r_args
            return smart_name(r_arg)
    return None


def get_column_filtered_by_is_none(arg: ColumnExpression) -> ColumnReference | None:
    if isinstance(arg, IsNotNoneExpression) and isinstance(
        filter_col := arg._expr, ColumnReference
    ):
        return filter_col
    return None

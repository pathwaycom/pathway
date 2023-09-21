# Copyright © 2023 Pathway

from __future__ import annotations

import dataclasses
from abc import ABC
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Type,
    Union,
)

from pathway.internals import api
from pathway.internals import dtype as dt
from pathway.internals import helpers
from pathway.internals.api import Value
from pathway.internals.operator_input import OperatorInput
from pathway.internals.shadows import inspect, operator
from pathway.internals.trace import Trace

if TYPE_CHECKING:
    from pathway.internals.column import Column, ColumnWithExpression
    from pathway.internals.expressions import (
        DateTimeNamespace,
        NumericalNamespace,
        StringNamespace,
    )
    from pathway.internals.reducers import UnaryReducer
    from pathway.internals.table import Table


class ColumnExpression(OperatorInput, ABC):
    _deps: helpers.SetOnceProperty[
        Iterable[ColumnExpression]
    ] = helpers.SetOnceProperty(wrapper=tuple)
    _dtype: dt.DType

    def __init__(self):
        self._trace = Trace.from_traceback()

    def __bool__(self):
        raise RuntimeError("Cannot use expression as boolean.")

    def __repr__(self):
        from pathway.internals.expression_printer import ExpressionFormatter

        return ExpressionFormatter().eval_expression(self)

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
            expression._table for expression in self._dependencies()
        )

    @lru_cache
    def _column_dependencies(self) -> helpers.StableSet[Column]:
        expression_dependencies = (dep.to_column() for dep in self._dependencies())
        return helpers.StableSet(expression_dependencies)

    @property
    def _column_with_expression_cls(self) -> Type[ColumnWithExpression]:
        from pathway.internals.column import ColumnWithExpression

        return ColumnWithExpression

    def __add__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.add)

    def __radd__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.add)

    def __sub__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.sub)

    def __rsub__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.sub)

    def __mul__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.mul)

    def __rmul__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.mul)

    def __truediv__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.truediv)

    def __rtruediv__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.truediv)

    def __floordiv__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.floordiv)

    def __rfloordiv__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.floordiv)

    def __mod__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.mod)

    def __rmod__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.mod)

    def __pow__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.pow)

    def __rpow__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.pow)

    def __lshift__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.lshift)

    def __rlshift__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.lshift)

    def __rshift__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.rshift)

    def __rrshift__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.rshift)

    def __eq__(self, other) -> ColumnBinaryOpExpression:  # type: ignore
        return ColumnBinaryOpExpression(self, other, operator.eq)

    def __ne__(self, other) -> ColumnBinaryOpExpression:  # type: ignore
        return ColumnBinaryOpExpression(self, other, operator.ne)

    def __le__(self, other) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.le)

    def __ge__(self, other) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.ge)

    def __lt__(self, other) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.lt)

    def __gt__(self, other) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.gt)

    def __and__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.and_)

    def __rand__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.and_)

    def __or__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.or_)

    def __ror__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.or_)

    def __xor__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.xor)

    def __rxor__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.xor)

    def __matmul__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(self, other, operator.matmul)

    def __rmatmul__(self, other: ColumnExpressionOrValue) -> ColumnBinaryOpExpression:
        return ColumnBinaryOpExpression(other, self, operator.matmul)

    def __neg__(self) -> ColumnUnaryOpExpression:
        return ColumnUnaryOpExpression(self, operator.neg)

    def __invert__(self) -> ColumnUnaryOpExpression:
        return ColumnUnaryOpExpression(self, operator.inv)

    def __hash__(self):
        return object.__hash__(self)

    def is_none(self) -> IsNoneExpression:
        return IsNoneExpression(self)

    def is_not_none(self) -> IsNotNoneExpression:
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
        default: ColumnExpressionOrValue = None,
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
        >>> t1 = pw.debug.parse_to_table('''
        ... val
        ... 1
        ... 2
        ... 3
        ... 4''')
        >>> t1.schema.as_dict()
        {'val': INT}
        >>> pw.debug.compute_and_print(t1, include_id=False)
        val
        1
        2
        3
        4
        >>> t2 = t1.select(val = pw.this.val.to_string())
        >>> t2.schema.as_dict()
        {'val': STR}
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

    def as_int(self) -> ConvertExpression:
        """Converts value to an int or None if not possible.
        Currently works for Json columns only.

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
        return ConvertExpression(dt.INT, self)

    def as_float(self) -> ConvertExpression:
        """Converts value to a float or None if not possible.
        Currently works for Json columns only.

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
        return ConvertExpression(dt.FLOAT, self)

    def as_str(self) -> ConvertExpression:
        """Converts value to a string or None if not possible.
        Currently works for Json columns only.

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
        return ConvertExpression(dt.STR, self)

    def as_bool(self) -> ConvertExpression:
        """Converts value to a bool or None if not possible.
        Currently works for Json columns only.

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
        return ConvertExpression(dt.BOOL, self)


ColumnExpressionOrValue = Union[ColumnExpression, Value]


class ColumnCallExpression(ColumnExpression):
    _args: Tuple[ColumnExpression, ...]
    _col_expr: ColumnReference

    def __init__(
        self, col_expr: ColumnReference, args: Iterable[ColumnExpressionOrValue]
    ):
        super().__init__()
        self._col_expr = col_expr
        self._args = tuple(_wrap_arg(arg) for arg in args)
        self._deps = self._args


class ColumnConstExpression(ColumnExpression):
    _val: Value

    def __init__(self, val: Value):
        super().__init__()
        self._val = val
        self._deps = []


class ColumnReference(ColumnExpression):
    """Reference to the column.

    Inherits from ColumnExpression.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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

    def __init__(self, *, column: Column, table: Table, name: str):
        super().__init__()
        self._column = column
        self._table = table
        self._name = name
        self._deps = []

    def _to_internal(self) -> InternalColRef:
        return InternalColRef(_table=self._table, _name=self._name)

    def _to_original_internal(self) -> InternalColRef:
        return InternalColRef(
            _table=self._column.lineage.table, _name=self._column.lineage.name
        )

    @property
    def table(self):
        """Table where the referred column belongs to.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.parse_to_table('''
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
        >>> t1 = pw.debug.parse_to_table('''
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
    def _column_with_expression_cls(self) -> Type[ColumnWithExpression]:
        from pathway.internals.column import ColumnWithReference

        return ColumnWithReference


@dataclasses.dataclass(frozen=True)
class InternalColRef:
    _table: Table
    _name: str

    def to_colref(self) -> ColumnReference:
        return self._table[self._name]

    def to_original(self) -> InternalColRef:
        return self.to_colref()._to_original_internal()

    def to_column(self) -> Column:
        if self._name == "id":
            return self._table._id_column
        return self._table._columns[self._name]


class ColumnBinaryOpExpression(ColumnExpression):
    _left: ColumnExpression
    _right: ColumnExpression
    _operator: Callable[[Any, Any], Any]

    def __init__(
        self,
        left: ColumnExpressionOrValue,
        right: ColumnExpressionOrValue,
        operator: Callable[[Any, Any], Any],
    ):
        super().__init__()
        self._left = _wrap_arg(left)
        self._right = _wrap_arg(right)
        self._operator = operator
        self._deps = [self._left, self._right]


class ColumnUnaryOpExpression(ColumnExpression):
    _expr: ColumnExpression
    _operator: Callable[[Any], Any]

    def __init__(self, expr: ColumnExpressionOrValue, operator: Callable[[Any], Any]):
        super().__init__()
        self._expr = _wrap_arg(expr)
        self._operator = operator
        self._deps = [self._expr]


class ReducerExpression(ColumnExpression):
    _reducer: UnaryReducer
    _arg: ColumnExpression

    def __init__(self, reducer: UnaryReducer, *args: ColumnExpressionOrValue):
        super().__init__()
        self._reducer = reducer
        self._args = tuple(_wrap_arg(arg) for arg in args)
        self._deps = self._args

    @lru_cache
    def _dependencies_above_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet()

    @lru_cache
    def _dependencies_below_reducer(self) -> helpers.StableSet[InternalColRef]:
        return helpers.StableSet.union(
            *[dep._dependencies_above_reducer() for dep in self._deps]
        )


class CountExpression(ColumnExpression):
    def __init__(self):
        super().__init__()
        self._deps = []


class ApplyExpression(ColumnExpression):
    _return_type: Any
    _args: Tuple[ColumnExpression, ...]
    _kwargs: Dict[str, ColumnExpression]
    _fun: Callable

    def __init__(
        self,
        fun: Callable,
        return_type=None,
        *args: ColumnExpressionOrValue,
        **kwargs: ColumnExpressionOrValue,
    ):
        super().__init__()
        self._fun = fun
        if return_type is None:
            try:
                return_type = inspect.signature(self._fun).return_annotation
            except ValueError:
                return_type = Any
        self._return_type = dt.wrap(return_type)

        self._args = tuple(_wrap_arg(arg) for arg in args)

        self._kwargs = {k: _wrap_arg(v) for k, v in kwargs.items()}
        self._deps = [*self._args, *self._kwargs.values()]
        assert len(args) + len(kwargs) > 0


class NumbaApplyExpression(ApplyExpression):
    def __init__(
        self,
        fun: Callable,
        return_type: Any,
        *args: ColumnExpressionOrValue,
        **kwargs: ColumnExpressionOrValue,
    ):
        super().__init__(
            fun,
            dt.wrap(return_type),
            *args,
            **kwargs,
        )


class AsyncApplyExpression(ApplyExpression):
    def __init__(
        self,
        fun: Callable,
        return_type: Any,
        *args: ColumnExpressionOrValue,
        **kwargs: ColumnExpressionOrValue,
    ):
        super().__init__(
            fun,
            dt.wrap(return_type) if return_type is not None else None,
            *args,
            **kwargs,
        )


class CastExpression(ColumnExpression):
    _return_type: dt.DType
    _expr: ColumnExpression

    def __init__(self, return_type: Any, expr: ColumnExpressionOrValue):
        super().__init__()
        self._return_type = dt.wrap(return_type)
        self._expr = _wrap_arg(expr)
        self._deps = [self._expr]


class ConvertExpression(ColumnExpression):
    _return_type: dt.DType
    _expr: ColumnExpression

    def __init__(self, return_type: dt.DType, expr: ColumnExpressionOrValue):
        super().__init__()
        self._return_type = dt.Optional(return_type)
        self._expr = _wrap_arg(expr)
        self._deps = [self._expr]


class DeclareTypeExpression(ColumnExpression):
    _return_type: Any
    _expr: ColumnExpression

    def __init__(self, return_type: Any, expr: ColumnExpressionOrValue):
        super().__init__()
        self._return_type = dt.wrap(return_type)
        self._expr = _wrap_arg(expr)
        self._deps = [self._expr]


class CoalesceExpression(ColumnExpression):
    _args: Iterable[ColumnExpression]

    def __init__(self, *args: ColumnExpressionOrValue):
        super().__init__()
        assert len(args) > 0

        def _test_for_none(arg):
            if arg is None:
                return True
            if isinstance(arg, ColumnConstExpression):
                return arg._val is None
            return False

        self._args = tuple(_wrap_arg(arg) for arg in args if not _test_for_none(arg))
        if self._args == ():
            self._args = (_wrap_arg(None),)
        self._deps = self._args


class RequireExpression(ColumnExpression):
    _val: ColumnExpression
    _args: Iterable[ColumnExpression]

    def __init__(self, val: ColumnExpressionOrValue, *args: ColumnExpressionOrValue):
        super().__init__()
        self._val = _wrap_arg(val)
        self._args = tuple(_wrap_arg(arg) for arg in args)
        self._deps = [self._val, *self._args]


class IfElseExpression(ColumnExpression):
    _if: ColumnExpression
    _then: ColumnExpression
    _else: ColumnExpression

    def __init__(
        self,
        _if: ColumnExpressionOrValue,
        _then: ColumnExpressionOrValue,
        _else: ColumnExpressionOrValue,
    ):
        super().__init__()
        self._if = _wrap_arg(_if)
        self._then = _wrap_arg(_then)
        self._else = _wrap_arg(_else)
        self._deps = [self._if, self._then, self._else]


class IsNoneExpression(ColumnExpression):
    _expr: ColumnExpression

    def __init__(self, _expr: ColumnExpressionOrValue):
        super().__init__()
        self._expr = _wrap_arg(_expr)
        self._deps = [self._expr]


class IsNotNoneExpression(ColumnExpression):
    _expr: ColumnExpression

    def __init__(self, _expr: ColumnExpressionOrValue):
        super().__init__()
        self._expr = _wrap_arg(_expr)
        self._deps = [self._expr]


class PointerExpression(ColumnExpression):
    _table: Table
    _args: Tuple[ColumnExpression, ...]
    _optional: bool

    def __init__(
        self, table: Table, *args: ColumnExpressionOrValue, optional=False
    ) -> None:
        super().__init__()

        self._args = tuple(_wrap_arg(arg) for arg in args)
        self._deps = self._args
        self._optional = optional
        self._table = table


class MakeTupleExpression(ColumnExpression):
    _args: Tuple[ColumnExpression, ...]

    def __init__(self, *args: ColumnExpressionOrValue):
        super().__init__()
        self._args = tuple(_wrap_arg(arg) for arg in args)
        self._deps = self._args


class GetExpression(ColumnExpression):
    _object: ColumnExpression
    _index: ColumnExpression
    _default: ColumnExpression
    _check_if_exists: bool
    _const_index: Optional[int | str]

    def __init__(
        self,
        object: ColumnExpression,
        index: ColumnExpression | int | str,
        default: ColumnExpressionOrValue = None,
        check_if_exists=True,
    ) -> None:
        super().__init__()
        self._object = object
        self._index = _wrap_arg(index)
        self._default = _wrap_arg(default)
        self._check_if_exists = check_if_exists
        if isinstance(self._index, ColumnConstExpression) and isinstance(
            self._index._val, (int, str)
        ):
            self._const_index = self._index._val
        else:
            self._const_index = None
        self._deps = (self._object, self._index, self._default)


ReturnTypeFunType = Callable[[Tuple[Any, ...]], Any]


class MethodCallExpression(ColumnExpression):
    _fun_mapping: Tuple[Tuple[tuple[dt.DType, ...], dt.DType, Callable], ...]
    _name: str
    _args: Tuple[ColumnExpression, ...]

    def __init__(
        self,
        fun_mapping: Tuple[
            Tuple[tuple[dt.DType, ...] | dt.DType, dt.DType, Callable], ...
        ],
        name: str,
        *args: ColumnExpressionOrValue,
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
        """
        super().__init__()
        self._fun_mapping = self._wrap_mapping_key_in_tuple(fun_mapping)
        self._name = name

        self._args = tuple(_wrap_arg(arg) for arg in args)

        self._deps = self._args
        assert len(args) > 0
        for key_dtypes, _, _ in self._fun_mapping:
            if len(key_dtypes) != len(self._args):
                raise ValueError(
                    f"In MethodCallExpression the number of args ({len(args)}) has to "
                    + f"be the same as the number of types in key ({len(key_dtypes)})"
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
    ) -> Optional[tuple[tuple[dt.DType, ...], dt.DType, Callable]]:
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

    def __init__(self, expr: ColumnExpressionOrValue):
        super().__init__()
        self._expr = _wrap_arg(expr)
        self._deps = [self._expr]


def _wrap_arg(arg: ColumnExpressionOrValue) -> ColumnExpression:
    if not isinstance(arg, ColumnExpression):
        return ColumnConstExpression(arg)
    return arg


def smart_name(arg: ColumnExpression) -> Optional[str]:
    from pathway.internals.reducers import _any, _unique

    if isinstance(arg, ColumnReference):
        return arg.name
    if isinstance(arg, ReducerExpression) and arg._reducer in [_unique, _any]:
        r_args = arg._args
        if len(r_args) == 1:
            [r_arg] = r_args
            return smart_name(r_arg)
    return None


def get_column_filtered_by_is_none(arg: ColumnExpression) -> Optional[ColumnReference]:
    if isinstance(arg, IsNotNoneExpression) and isinstance(
        filter_col := arg._expr, ColumnReference
    ):
        return filter_col
    return None

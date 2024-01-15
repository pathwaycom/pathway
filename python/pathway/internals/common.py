# Copyright © 2024 Pathway

from __future__ import annotations

import inspect
from collections import defaultdict
from collections.abc import Callable, Mapping
from functools import wraps
from typing import (
    Any,
    ParamSpec,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from pathway.internals import (
    dtype as dt,
    expression as expr,
    operator as op,
    schema,
    table,
)
from pathway.internals.api import Value
from pathway.internals.helpers import function_spec
from pathway.internals.parse_graph import G
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame

T = TypeVar("T")
P = ParamSpec("P")


@check_arg_types
def iterate(
    func,
    iteration_limit: int | None = None,
    **kwargs: table.Table | op.iterate_universe,
):
    """Iterate function until fixed point.
    Function has to take only Table arguments.
    Function has to return a single Table, a tuple of Tables, or a dict of Tables.
    Iterate returns the same shape of arguments as the ``func`` function:
    either a single Table, a tuple of Tables, or a dict of Tables, respectively.
    Initial arguments to function are passed through kwargs.

    Example:

    >>> import pathway as pw
    >>> def collatz_transformer(iterated):
    ...     def collatz_step(x: int) -> int:
    ...         if x == 1:
    ...             return 1
    ...         elif x % 2 == 0:
    ...             return x // 2
    ...         else:
    ...             return 3 * x + 1
    ...     return iterated.select(val=pw.apply(collatz_step, iterated.val))
    >>> tab = pw.debug.table_from_markdown('''
    ... val
    ...   1
    ...   2
    ...   3
    ...   4
    ...   5
    ...   6
    ...   7
    ...   8''')
    >>> ret = pw.iterate(collatz_transformer, iterated=tab)
    >>> pw.debug.compute_and_print(ret, include_id=False)
    val
    1
    1
    1
    1
    1
    1
    1
    1
    """
    if iteration_limit is not None and iteration_limit < 1:
        raise ValueError("wrong iteration limit")
    fn_spec = function_spec(func)
    return G.add_iterate(
        fn_spec, lambda node: node(**kwargs), iteration_limit=iteration_limit
    )


@check_arg_types
@trace_user_frame
def apply(
    fun: Callable,
    *args: expr.ColumnExpression | Value,
    **kwargs: expr.ColumnExpression | Value,
) -> expr.ColumnExpression:
    """Applies function to column expressions, column-wise.
    Output column type deduced from type-annotations of a function.

    Example:

    >>> import pathway as pw
    >>> def concat(left: str, right: str) -> str:
    ...   return left+right
    >>> t1 = pw.debug.table_from_markdown('''
    ... age  owner  pet
    ...  10  Alice  dog
    ...   9    Bob  dog
    ...   8  Alice  cat
    ...   7    Bob  dog''')
    >>> t2 = t1.select(col = pw.apply(concat, t1.owner, t1.pet))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    Alicecat
    Alicedog
    Bobdog
    Bobdog
    """
    return expr.ApplyExpression(fun, None, *args, **kwargs)


@check_arg_types
@trace_user_frame
def numba_apply(
    fun: Callable,
    numba_signature: str,
    *args: expr.ColumnExpression | Value,
    **kwargs: expr.ColumnExpression | Value,
) -> expr.ColumnExpression:
    """Applies function to column expressions, column-wise.
    Function has to be numba compilable.

    Currently only a few signatures are supported:
    - function has to be unary or binary
    - arguments and return type has to be either int64 or float64

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ...    val
    ... 1    1
    ... 2    3
    ... 3    5
    ... 4    7''')
    >>> t2 = t1.select(col = pw.numba_apply(lambda x: x*x-2*x+1, "int64(int64,)", t1.val))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    0
    4
    16
    36
    """
    ret_type = {
        "int64": int,
        "int32": int,
        "int128": int,
        "float128": float,
        "float64": float,
        "float32": float,
        "bool": bool,
    }[numba_signature.split("(")[0]]
    try:
        import numba
    except ImportError:
        return expr.ApplyExpression(fun, ret_type, *args, **kwargs)

    try:
        # Disabling nopython should result in compiling more functions, but with a speed penalty
        fun = numba.cfunc(numba_signature, nopython=True)(fun)
        return expr.NumbaApplyExpression(fun, ret_type, *args, **kwargs)
    except Exception as e:
        raise ValueError("Numba compilation failed!") from e


def apply_with_type(
    fun: Callable,
    ret_type: type | dt.DType,
    *args: expr.ColumnExpression | Value,
    **kwargs: expr.ColumnExpression | Value,
) -> expr.ColumnExpression:
    """Applies function to column expressions, column-wise.
    Output column type is provided explicitly.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ...    age  owner  pet
    ... 1   10  Alice  dog
    ... 2    9    Bob  dog
    ... 3    8  Alice  cat
    ... 4    7    Bob  dog''')
    >>> t2 = t1.select(col = pw.apply_with_type(lambda left, right: left+right, str, t1.owner, t1.pet))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    Alicecat
    Alicedog
    Bobdog
    Bobdog
    """
    return expr.ApplyExpression(fun, ret_type, *args, **kwargs)


@check_arg_types
@trace_user_frame
def apply_async(
    fun: Callable,
    *args: expr.ColumnExpression | Value,
    **kwargs: expr.ColumnExpression | Value,
) -> expr.ColumnExpression:
    r"""Applies function asynchronously to column expressions, column-wise.
    Output column type deduced from type-annotations of a function.
    Either a regular or async function can be passed.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> async def concat(left: str, right: str) -> str:
    ...   await asyncio.sleep(0.1)
    ...   return left+right
    >>> t1 = pw.debug.table_from_markdown('''
    ... age  owner  pet
    ...  10  Alice  dog
    ...   9    Bob  dog
    ...   8  Alice  cat
    ...   7    Bob  dog''')
    >>> t2 = t1.select(col = pw.apply_async(concat, t1.owner, t1.pet))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    Alicecat
    Alicedog
    Bobdog
    Bobdog
    """
    return expr.AsyncApplyExpression(fun, None, *args, **kwargs)


# declare_type used to demand that target_type is of type 'type'
# however, it should also accept Optional (something like type | Optional[anytype])
# best we can do at the moment is to set the type of target_type to Any


def declare_type(
    target_type, col: expr.ColumnExpression | Value
) -> expr.DeclareTypeExpression:
    """Used to change the type of a column to a particular type.
    Disclaimer: it only changes type in a schema, it does not affect values stored.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ...    val
    ... 1   10
    ... 2    9.5
    ... 3    8
    ... 4    7''')
    >>> t1.schema
    <pathway.Schema types={'val': <class 'float'>}>
    >>> t2 = t1.filter(t1.val == pw.cast(int, t1.val))
    >>> t2.schema
    <pathway.Schema types={'val': <class 'float'>}>
    >>> t3 = t2.select(val = pw.declare_type(int, t2.val))
    >>> t3.schema
    <pathway.Schema types={'val': <class 'int'>}>
    """
    return expr.DeclareTypeExpression(target_type, col)


def cast(target_type: Any, col: expr.ColumnExpression | Value) -> expr.CastExpression:
    """Changes the type of the column to target_type and converts the data of this column

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ...   val
    ... 1   10
    ... 2    9
    ... 3    8
    ... 4    7''')
    >>> t1.schema
    <pathway.Schema types={'val': <class 'int'>}>
    >>> pw.debug.compute_and_print(t1, include_id=False)
    val
    7
    8
    9
    10
    >>> t2 = t1.select(val = pw.cast(float, t1.val))
    >>> t2.schema
    <pathway.Schema types={'val': <class 'float'>}>
    >>> pw.debug.compute_and_print(t2, include_id=False)
    val
    7.0
    8.0
    9.0
    10.0
    """
    return expr.CastExpression(target_type, col)


@check_arg_types
@trace_user_frame
def coalesce(*args: expr.ColumnExpression | Value) -> expr.ColumnExpression:
    """For arguments list arg_1, arg_2, ..., arg_n returns first not-None value.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... colA   colB
    ...      |   10
    ...    2 |
    ...      |
    ...    4 |    7''')
    >>> t2 = t1.select(t1.colA, t1.colB, col=pw.coalesce(t1.colA, t1.colB))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    colA | colB | col
         |      |
         | 10   | 10
    2    |      | 2
    4    | 7    | 4
    """
    return expr.CoalesceExpression(*args)


@check_arg_types
@trace_user_frame
def require(val, *deps: expr.ColumnExpression | Value) -> expr.ColumnExpression:
    """Returns val iff every dep in deps is not-None.
    Returns None otherwise.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... colA   colB
    ...      |   10
    ...    2 |
    ...      |
    ...    4 |    7''')
    >>> t2 = t1.select(t1.colA, t1.colB, col=pw.require(t1.colA + t1.colB, t1.colA, t1.colB))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    colA | colB | col
         |      |
         | 10   |
    2    |      |
    4    | 7    | 11
    """
    return expr.RequireExpression(val, *deps)


@check_arg_types
@trace_user_frame
def if_else(
    if_clause: expr.ColumnExpression | Value,
    then_clause: expr.ColumnExpression | Value,
    else_clause: expr.ColumnExpression | Value,
) -> expr.ColumnExpression:
    """Equivalent to::

        if (if_clause):
            return (then_clause)
        else:
            return (else_clause)

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... colA   colB
    ...    1 |    0
    ...    2 |    2
    ...    6 |    3''')
    >>> t2 = t1.select(res = pw.if_else(t1.colB != 0, t1.colA // t1.colB, 0))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    res
    0
    1
    2
    """

    return expr.IfElseExpression(if_clause, then_clause, else_clause)


@check_arg_types
@trace_user_frame
def make_tuple(*args: expr.ColumnExpression | Value) -> expr.ColumnExpression:
    """
    Creates a tuple from the provided expressions.

    Args:
        args: a list of expressions to be put in a tuple

    Returns:
        tuple

    Note:
        - Each cell in the output column will be a tuple containing the corresponding values from the input \
        columns.
        - The order of values in each tuple will match the order of the input columns.
        - If any of the input columns have missing values, the resulting tuples will contain None for those \
        positions.

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ... a | b  | c
    ... 1 | 10 | a
    ... 2 | 20 |
    ... 3 | 30 | c
    ... '''
    ... )
    >>> table_with_tuple = table.select(res=pw.make_tuple(pw.this.a, pw.this.b, pw.this.c))
    >>> pw.debug.compute_and_print(table_with_tuple, include_id=False)
    res
    (1, 10, 'a')
    (2, 20, None)
    (3, 30, 'c')
    """
    return expr.MakeTupleExpression(*args)


def unwrap(col: expr.ColumnExpression | Value) -> expr.ColumnExpression:
    """Changes the type of the column from Optional[T] to T. If there is any None in the
    column this operation will raise an exception.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... colA | colB
    ... 1    | 5
    ... 2    | 9
    ... 3    | None
    ... 4    | 15''')
    >>> t1.schema
    <pathway.Schema types={'colA': <class 'int'>, 'colB': int | None}>
    >>> pw.debug.compute_and_print(t1, include_id=False)
    colA | colB
    1    | 5
    2    | 9
    3    |
    4    | 15
    >>> t2 = t1.filter(t1.colA < 3)
    >>> t2.schema
    <pathway.Schema types={'colA': <class 'int'>, 'colB': int | None}>
    >>> pw.debug.compute_and_print(t2, include_id=False)
    colA | colB
    1    | 5
    2    | 9
    >>> t3 = t2.select(colB = pw.unwrap(t2.colB))
    >>> t3.schema
    <pathway.Schema types={'colB': <class 'int'>}>
    >>> pw.debug.compute_and_print(t3, include_id=False)
    colB
    5
    9
    """
    return expr.UnwrapExpression(col)


def assert_table_has_schema(
    table: table.Table,
    schema: type[schema.Schema],
    *,
    allow_superset: bool = True,
    ignore_primary_keys: bool = True,
) -> None:
    """
    Asserts that the schema of the table is equivalent to the schema given as an argument.

    Args:
        table: Table for which we are asserting schema.
        schema: Schema, which we assert that the Table has.
        allow_superset: if True, the columns of the table can be a superset of columns
            in schema. The default value is True.
        ignore_primary_keys: if True, the assert won't check whether table and schema
            have the same primary keys. The default value is True.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | dog
    ... 8   | Alice | cat
    ... 7   | Bob   | dog
    ... ''')
    >>> t2 = t1.select(pw.this.owner, age = pw.cast(float, pw.this.age))
    >>> schema = pw.schema_builder(
    ...     {"age": pw.column_definition(dtype=float), "owner": pw.column_definition(dtype=str)}
    ... )
    >>> pw.assert_table_has_schema(t2, schema)
    """
    table.schema.assert_equal_to(
        schema, allow_superset=allow_superset, ignore_primary_keys=ignore_primary_keys
    )


@overload
def table_transformer(func: Callable[P, T]) -> Callable[P, T]:
    ...


@overload
def table_transformer(
    *,
    allow_superset: bool | Mapping[str, bool] = True,
    ignore_primary_keys: bool | Mapping[str, bool] = True,
    locals: dict[str, Any] | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    ...


def table_transformer(
    func: Callable[P, T] | None = None,
    *,
    allow_superset: bool | Mapping[str, bool] = True,
    ignore_primary_keys: bool | Mapping[str, bool] = True,
    locals: dict[str, Any] | None = None,
) -> Callable[P, T] | Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator for marking that a function performs operations on Tables. As a consequence,
    arguments and return value, which are annotated to have type pw.Table[S]
    will be checked whether they indeed have schema S.

    Args:
        allow_superset: if True, the columns of the table can be a superset of columns
            in schema. Can be given either as a bool, and this value is then used for
            all tables, or for each argument separately, by providing a dict whose keys
            are names of arguments, and values are bools specifying value of allow_superset
            for this argument. In the latter case to provide value for return value, provide
            value for key "return". The default value is True.
        ignore_primary_keys: if True, the assert won't check whether table and schema
            have the same primary keys. Can be given either as a bool, and this value is then used for
            all tables, or for each argument separately, by providing a dict whose keys
            are names of arguments, and values are bools specifying value of ignore_primary_keys
            for this argument. The default value is True.
        locals: when Schema class, which is used as a parameter to `pw.Table` is defined locally,
            you need to pass locals() as locals argument.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... A | B
    ... 1 | 6
    ... 3 | 8
    ... 5 | 2
    ... ''')
    >>> schema = pw.schema_from_types(A=int, B=int)
    >>> result_schema = pw.schema_from_types(A=int, B=int, C=int)
    >>> @pw.table_transformer
    ... def sum_columns(t: pw.Table[schema]) -> pw.Table[result_schema]:
    ...     result = t.with_columns(C=pw.this.A + pw.this.B)
    ...     return result
    >>> pw.debug.compute_and_print(sum_columns(t1), include_id=False)
    A | B | C
    1 | 6 | 7
    3 | 8 | 11
    5 | 2 | 7
    """

    def decorator(f):
        annotations = get_type_hints(f, localns=locals)
        signature = inspect.signature(f)

        if isinstance(allow_superset, bool):
            allow_superset_dict: Mapping[str, bool] = defaultdict(
                lambda: allow_superset
            )
        else:
            allow_superset_dict = allow_superset

        if isinstance(ignore_primary_keys, bool):
            ignore_primary_keys_dict: Mapping[str, bool] = defaultdict(
                lambda: ignore_primary_keys
            )
        else:
            ignore_primary_keys_dict = ignore_primary_keys

        def check_annotation(name, value):
            annotation = annotations.get(name, None)
            if get_origin(annotation) == table.Table and get_args(annotation):
                try:
                    assert_table_has_schema(
                        value,
                        get_args(annotation)[0],
                        allow_superset=allow_superset_dict.get(name, True),
                        ignore_primary_keys=ignore_primary_keys_dict.get(name, True),
                    )
                except AssertionError as exc:
                    raise AssertionError(
                        f"argument {name} has incorrect schema"
                    ) from exc

        @wraps(f)
        def wrapper(*args, **kwargs):
            bound_signature = signature.bind(*args, **kwargs)
            for name, arg in bound_signature.arguments.items():
                check_annotation(name, arg)

            return_value = f(*args, **kwargs)
            check_annotation("return", return_value)
            return return_value

        return wrapper

    if func is not None:
        return decorator(func)
    else:
        return decorator

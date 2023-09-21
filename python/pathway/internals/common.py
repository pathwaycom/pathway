# Copyright © 2023 Pathway

from __future__ import annotations

import functools
from typing import Any, Callable, Optional, Type, Union, overload

from pathway.internals import dtype as dt
from pathway.internals import expression as expr
from pathway.internals import operator as op
from pathway.internals import schema, table
from pathway.internals.asynchronous import (
    AsyncRetryStrategy,
    CacheStrategy,
    async_options,
)
from pathway.internals.helpers import function_spec
from pathway.internals.parse_graph import G
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame


@runtime_type_check
def iterate(
    func,
    iteration_limit: Optional[int] = None,
    **kwargs: Union[table.Table, op.iterate_universe],
):
    """Iterate function until fixed point.
    Function has to take only named arguments, Tables, and return a dict of Tables.
    Initial arguments to function are passed through kwargs.

    Example:

    >>> import pathway as pw
    >>> def collatz_transformer(iterated):
    ...     def collatz_step(x: int) -> int:
    ...         if x == 1:
    ...             return 1
    ...         elif x % 2 == 0:
    ...             return x / 2
    ...         else:
    ...             return 3 * x + 1
    ...     new_iterated = iterated.select(val=pw.apply(collatz_step, iterated.val))
    ...     return dict(iterated=new_iterated)
    >>> tab = pw.debug.parse_to_table('''
    ... val
    ...   1
    ...   2
    ...   3
    ...   4
    ...   5
    ...   6
    ...   7
    ...   8''')
    >>> ret = pw.iterate(collatz_transformer, iterated=tab).iterated
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


@runtime_type_check
@trace_user_frame
def apply(
    fun: Callable,
    *args: expr.ColumnExpressionOrValue,
    **kwargs: expr.ColumnExpressionOrValue,
) -> expr.ColumnExpression:
    """Applies function to column expressions, column-wise.
    Output column type deduced from type-annotations of a function.

    Example:

    >>> import pathway as pw
    >>> def concat(left: str, right: str) -> str:
    ...   return left+right
    >>> t1 = pw.debug.parse_to_table('''
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


def udf(fun: Callable):
    """Create a Python UDF (universal data function) out of a callable.

    The output type of the UDF is determined based on its type annotation.

    Example:

    >>> import pathway as pw
    >>> @pw.udf
    ... def concat(left: str, right: str) -> str:
    ...     return left+right
    ...
    >>> t1 = pw.debug.parse_to_table('''
    ... age  owner  pet
    ...     10  Alice  dog
    ...     9    Bob  dog
    ...     8  Alice  cat
    ...     7    Bob  dog''')
    >>> t2 = t1.select(col = concat(t1.owner, t1.pet))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    Alicecat
    Alicedog
    Bobdog
    Bobdog
    """

    @functools.wraps(fun)
    def udf_fun(
        *args: expr.ColumnExpressionOrValue,
        **kwargs: expr.ColumnExpressionOrValue,
    ):
        return apply(fun, *args, **kwargs)

    return udf_fun


@overload
def udf_async(fun: Callable) -> Callable:
    ...


@overload
def udf_async(
    *,
    capacity: Optional[int] = None,
    retry_strategy: Optional[AsyncRetryStrategy] = None,
    cache_strategy: Optional[CacheStrategy] = None,
) -> Callable[[Callable], Callable]:
    ...


def udf_async(
    fun: Optional[Callable] = None,
    *,
    capacity: Optional[int] = None,
    retry_strategy: Optional[AsyncRetryStrategy] = None,
    cache_strategy: Optional[CacheStrategy] = None,
):
    r"""Create a Python asynchronous UDF (universal data function) out of a callable.

    Output column type deduced from type-annotations of a function.
    Can be applied to a regular or asynchronous function.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> @pw.udf_async
    ... async def concat(left: str, right: str) -> str:
    ...   await asyncio.sleep(0.1)
    ...   return left+right
    >>> t1 = pw.debug.parse_to_table('''
    ... age  owner  pet
    ...  10  Alice  dog
    ...   9    Bob  dog
    ...   8  Alice  cat
    ...   7    Bob  dog''')
    >>> t2 = t1.select(col = concat(t1.owner, t1.pet))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    Alicecat
    Alicedog
    Bobdog
    Bobdog
    """

    def apply_wrapper(fun, *args, **kwargs):
        fun = async_options(
            capacity=capacity,
            retry_strategy=retry_strategy,
            cache_strategy=cache_strategy,
        )(fun)
        return apply_async(fun, *args, **kwargs)

    def decorator(fun: Callable) -> Callable:
        return functools.wraps(fun)(functools.partial(apply_wrapper, fun))

    if fun is None:
        return decorator
    else:
        if not callable(fun):
            raise TypeError("udf_async should be used with keyword arguments only")

        return decorator(fun)


@runtime_type_check
@trace_user_frame
def numba_apply(
    fun: Callable,
    numba_signature: str,
    *args: expr.ColumnExpressionOrValue,
    **kwargs: expr.ColumnExpressionOrValue,
) -> expr.ColumnExpression:
    """Applies function to column expressions, column-wise.
    Function has to be numba compilable.

    Currently only a few signatures are supported:
    - function has to be unary or binary
    - arguments and return type has to be either int64 or float64

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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
    *args: expr.ColumnExpressionOrValue,
    **kwargs: expr.ColumnExpressionOrValue,
) -> expr.ColumnExpression:
    """Applies function to column expressions, column-wise.
    Output column type is provided explicitly.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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


@runtime_type_check
@trace_user_frame
def apply_async(
    fun: Callable,
    *args: expr.ColumnExpressionOrValue,
    **kwargs: expr.ColumnExpressionOrValue,
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
    >>> t1 = pw.debug.parse_to_table('''
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
    target_type, col: expr.ColumnExpressionOrValue
) -> expr.DeclareTypeExpression:
    """Used to change the type of a column to a particular type.
    Disclaimer: it only changes type in a schema, it does not affect values stored.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
    ...    val
    ... 1   10
    ... 2    9.5
    ... 3    8
    ... 4    7''')
    >>> t1.schema.as_dict()
    {'val': FLOAT}
    >>> t2 = t1.filter(t1.val == pw.cast(int, t1.val))
    >>> t2.schema.as_dict()
    {'val': FLOAT}
    >>> t3 = t2.select(val = pw.declare_type(int, t2.val))
    >>> t3.schema.as_dict()
    {'val': INT}
    """

    return expr.DeclareTypeExpression(target_type, col)


def cast(target_type: Any, col: expr.ColumnExpressionOrValue) -> expr.CastExpression:
    """Changes the type of the column to target_type and converts the data of this column

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
    ...   val
    ... 1   10
    ... 2    9
    ... 3    8
    ... 4    7''')
    >>> t1.schema.as_dict()
    {'val': INT}
    >>> pw.debug.compute_and_print(t1, include_id=False)
    val
    7
    8
    9
    10
    >>> t2 = t1.select(val = pw.cast(float, t1.val))
    >>> t2.schema.as_dict()
    {'val': FLOAT}
    >>> pw.debug.compute_and_print(t2, include_id=False)
    val
    7.0
    8.0
    9.0
    10.0
    """
    return expr.CastExpression(target_type, col)


@runtime_type_check
@trace_user_frame
def coalesce(*args: expr.ColumnExpressionOrValue) -> expr.ColumnExpression:
    """For arguments list arg_1, arg_2, ..., arg_n returns first not-None value.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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


@runtime_type_check
@trace_user_frame
def require(val, *deps: expr.ColumnExpressionOrValue) -> expr.ColumnExpression:
    """Returns val iff every dep in deps is not-None.
    Returns None otherwise.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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


@runtime_type_check
@trace_user_frame
def if_else(
    if_clause: expr.ColumnExpressionOrValue,
    then_clause: expr.ColumnExpressionOrValue,
    else_clause: expr.ColumnExpressionOrValue,
) -> expr.ColumnExpression:
    """Equivalent to::

        if (if_clause):
            return (then_clause)
        else:
            return (else_clause)

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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


@runtime_type_check
@trace_user_frame
def make_tuple(*args: expr.ColumnExpressionOrValue) -> expr.ColumnExpression:
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
    ... A | B  | C
    ... 1 | 10 | a
    ... 2 | 20 |
    ... 3 | 30 | c
    ... '''
    ... )
    >>> table_with_tuple = table.select(res=pw.make_tuple(pw.this.A, pw.this.B, pw.this.C))
    >>> pw.debug.compute_and_print(table_with_tuple, include_id=False)
    res
    (1, 10, 'a')
    (2, 20, None)
    (3, 30, 'c')
    """
    return expr.MakeTupleExpression(*args)


def unwrap(col: expr.ColumnExpressionOrValue) -> expr.ColumnExpression:
    """Changes the type of the column from Optional[T] to T. If there is any None in the
    column this operation will raise an exception.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
    ... colA | colB
    ... 1    | 5
    ... 2    | 9
    ... 3    | None
    ... 4    | 15''')
    >>> t1.schema.as_dict()
    {'colA': INT, 'colB': Optional(INT)}
    >>> pw.debug.compute_and_print(t1, include_id=False)
    colA | colB
    1    | 5
    2    | 9
    3    |
    4    | 15
    >>> t2 = t1.filter(t1.colA < 3)
    >>> t2.schema.as_dict()
    {'colA': INT, 'colB': Optional(INT)}
    >>> pw.debug.compute_and_print(t2, include_id=False)
    colA | colB
    1    | 5
    2    | 9
    >>> t3 = t2.select(colB = pw.unwrap(t2.colB))
    >>> t3.schema.as_dict()
    {'colB': INT}
    >>> pw.debug.compute_and_print(t3, include_id=False)
    colB
    5
    9
    """
    return expr.UnwrapExpression(col)


def assert_table_has_schema(
    table: table.Table,
    schema: Type[schema.Schema],
    *,
    allow_superset: bool = False,
    ignore_primary_keys: bool = True,
) -> None:
    """
    Asserts that the schema of the table is equivalent to the schema given as an argument.

    Args:
        table: Table for which we are asserting schema.
        schema: Schema, which we assert that the Table has.
        allow_superset: if True, the columns of the table can be a superset of columns
            in schema.
        ignore_primary_keys: if True, the assert won't check whether table and schema
            have the same primary keys. The default value is True.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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

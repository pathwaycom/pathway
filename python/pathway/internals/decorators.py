# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathway import Table
    from pathway.internals.datasink import DataSink
    from pathway.internals.schema import Schema

from pathway.internals import dtype as dt, operator as op, row_transformer as rt
from pathway.internals.datasource import EmptyDataSource, StaticDataSource
from pathway.internals.helpers import function_spec, with_optional_kwargs
from pathway.internals.parse_graph import G


def contextualized_operator(func):
    return _operator_wrapper(func, op.ContextualizedIntermediateOperator)


def _operator_wrapper(func: Callable, operator_cls: type[op.OperatorFromDef]):
    fn_spec = function_spec(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        return G.add_operator(
            lambda id: operator_cls(fn_spec, id),
            lambda operator: operator(*args, **kwargs),
        )

    return wrapper


def input_attribute(type=float):
    """Returns new input_attribute. To be used inside class transformers.

    Example:

    >>> import pathway as pw
    >>> @pw.transformer
    ... class simple_transformer:
    ...     class table(pw.ClassArg):
    ...         arg = pw.input_attribute()
    ...
    ...         @pw.output_attribute
    ...         def ret(self) -> float:
    ...             return self.arg + 1
    ...
    >>> t1 = pw.debug.table_from_markdown('''
    ... age
    ... 10
    ... 9
    ... 8
    ... 7''')
    >>> t2 = simple_transformer(table=t1.select(arg=t1.age)).table
    >>> pw.debug.compute_and_print(t1 + t2, include_id=False)
    age | ret
    7   | 8
    8   | 9
    9   | 10
    10  | 11
    """
    return rt.InputAttribute(dtype=dt.wrap(type))


def input_method(type=float):
    """Decorator for defining input methods in class transformers.

    Example:

    >>> import pathway as pw
    >>> @pw.transformer
    ... class first_transformer:
    ...     class table(pw.ClassArg):
    ...         a: float = pw.input_attribute()
    ...
    ...         @pw.method
    ...         def fun(self, arg) -> int:
    ...             return self.a * arg
    ...
    >>> @pw.transformer
    ... class second_transformer:
    ...     class table(pw.ClassArg):
    ...         m = pw.input_method(int)
    ...
    ...         @pw.output_attribute
    ...         def val(self):
    ...             return self.m(2)
    ...
    >>> t1 = pw.debug.table_from_markdown('''
    ... age
    ... 10
    ... 9
    ... 8
    ... 7''')
    >>> t2 = first_transformer(table=t1.select(a=t1.age)).table
    >>> t2.schema
    <pathway.Schema types={'fun': typing.Callable[..., int]}>
    >>> t3 = second_transformer(table=t2.select(m=t2.fun)).table
    >>> pw.debug.compute_and_print(t1 + t3, include_id=False)
    age | val
    7   | 14
    8   | 16
    9   | 18
    10  | 20
    """
    return rt.InputMethod(dtype=dt.wrap(type))


@with_optional_kwargs
def attribute(func, **kwargs):
    """Decorator for creation of attributes.

    Example:

    >>> import pathway as pw
    >>> @pw.transformer
    ... class simple_transformer:
    ...     class table(pw.ClassArg):
    ...         arg = pw.input_attribute()
    ...
    ...         @pw.attribute
    ...         def attr(self) -> float:
    ...             return self.arg*2
    ...
    ...         @pw.output_attribute
    ...         def ret(self) -> float:
    ...             return self.attr + 1
    ...
    >>> t1 = pw.debug.table_from_markdown('''
    ... age
    ... 10
    ... 9
    ... 8
    ... 7''')
    >>> t2 = simple_transformer(table=t1.select(arg=t1.age)).table
    >>> pw.debug.compute_and_print(t1 + t2, include_id=False)
    age | ret
    7   | 15
    8   | 17
    9   | 19
    10  | 21
    """
    return rt.Attribute(func, **kwargs)


@with_optional_kwargs
def output_attribute(func, **kwargs):
    """Decorator for creation of output_attributes.

    Example:

    >>> import pathway as pw
    >>> @pw.transformer
    ... class simple_transformer:
    ...     class table(pw.ClassArg):
    ...         arg = pw.input_attribute()
    ...
    ...         @pw.output_attribute
    ...         def ret(self) -> float:
    ...             return self.arg + 1
    ...
    >>> t1 = pw.debug.table_from_markdown('''
    ... age
    ... 10
    ... 9
    ... 8
    ... 7''')
    >>> t2 = simple_transformer(table=t1.select(arg=t1.age)).table
    >>> pw.debug.compute_and_print(t1 + t2, include_id=False)
    age | ret
    7   | 8
    8   | 9
    9   | 10
    10  | 11
    """
    return rt.OutputAttribute(func, **kwargs)


@with_optional_kwargs
def method(func, **kwargs):
    """Decorator for creation methods in class transformers.

    Example:

    >>> import pathway as pw
    >>> @pw.transformer
    ... class simple_transformer:
    ...     class table(pw.ClassArg):
    ...         a: float = pw.input_attribute()
    ...
    ...         @pw.output_attribute
    ...         def b(self) -> float:
    ...             return self.fun(self.a)
    ...
    ...         @method
    ...         def fun(self, arg) -> float:
    ...             return self.a * arg
    ...
    >>> t1 = pw.debug.table_from_markdown('''
    ... age
    ... 10
    ... 9
    ... 8
    ... 7''')
    >>> t2 = simple_transformer(table=t1.select(a=t1.age)).table
    >>> t2.schema
    <pathway.Schema types={'b': <class 'float'>, 'fun': typing.Callable[..., float]}>
    >>> pw.debug.compute_and_print(t1 + t2.select(t2.b), include_id=False)
    age | b
    7   | 49
    8   | 64
    9   | 81
    10  | 100
    >>> pw.debug.compute_and_print(t1 + t2.select(out = t2.fun(t2.b)), include_id=False)
    age | out
    7   | 343
    8   | 512
    9   | 729
    10  | 1000
    """
    return rt.Method(func, **kwargs)


def table_from_datasource(
    datasource,
    debug_datasource: StaticDataSource | None = None,
) -> Table:
    return G.add_operator(
        lambda id: op.InputOperator(datasource, id, debug_datasource),
        lambda operator: operator(),
    )


def table_to_datasink(table: Table, datasink: DataSink):
    return G.add_operator(
        lambda id: op.OutputOperator(datasink, id),
        lambda operator: operator(table),
    )


def transformer(cls):
    """Decorator that wraps the outer class when defining class transformers.

    Example:

    >>> import pathway as pw
    >>> @pw.transformer
    ... class simple_transformer:
    ...     class table(pw.ClassArg):
    ...         arg = pw.input_attribute()
    ...
    ...         @pw.output_attribute
    ...         def ret(self) -> float:
    ...             return self.arg + 1
    ...
    >>> t1 = pw.debug.table_from_markdown('''
    ... age
    ... 10
    ... 9
    ... 8
    ... 7''')
    >>> t2 = simple_transformer(table=t1.select(arg=t1.age)).table
    >>> pw.debug.compute_and_print(t1 + t2, include_id=False)
    age | ret
    7   | 8
    8   | 9
    9   | 10
    10  | 11
    """
    return rt.RowTransformer.from_class(cls)


def empty_from_schema(schema: type[Schema]) -> Table:
    return table_from_datasource(EmptyDataSource(schema=schema))

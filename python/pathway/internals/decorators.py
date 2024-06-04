# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable
from functools import wraps

from pathway.internals import dtype as dt, operator as op, row_transformer as rt
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
    """Returns new input_attribute. To be used inside class transformers."""
    return rt.InputAttribute(dtype=dt.wrap(type))


def input_method(type=float):
    """Decorator for defining input methods in class transformers."""
    return rt.InputMethod(dtype=dt.wrap(type))


@with_optional_kwargs
def attribute(func, **kwargs):
    """Decorator for creation of attributes."""
    return rt.Attribute(func, **kwargs)


@with_optional_kwargs
def output_attribute(func, **kwargs):
    """Decorator for creation of output_attributes."""
    return rt.OutputAttribute(func, **kwargs)


@with_optional_kwargs
def method(func, **kwargs):
    """Decorator for creation methods in class transformers."""
    return rt.Method(func, **kwargs)


def transformer(cls):
    """Decorator that wraps the outer class when defining class transformers."""
    return rt.RowTransformer.from_class(cls)

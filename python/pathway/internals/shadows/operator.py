# Copyright © 2024 Pathway

from __future__ import annotations

import operator
from operator import *  # noqa
from typing import Any


def _binary_arithmetic_wrap(op, symbol: str):
    def wrapped(left: float, right: float) -> float:
        return op(left, right)

    wrapped.__name__ = op.__name__
    wrapped._symbol = symbol  # type: ignore[attr-defined]

    return wrapped


add = _binary_arithmetic_wrap(operator.add, "+")
sub = _binary_arithmetic_wrap(operator.sub, "-")
mul = _binary_arithmetic_wrap(operator.mul, "*")
truediv = _binary_arithmetic_wrap(operator.truediv, "/")
floordiv = _binary_arithmetic_wrap(operator.floordiv, "//")
mod = _binary_arithmetic_wrap(operator.mod, "%")
pow = _binary_arithmetic_wrap(operator.pow, "**")


def _binary_cmp_wrap(op, symbol):
    def wrapped(left: Any, right: Any) -> bool:
        return op(left, right)

    wrapped.__name__ = op.__name__
    wrapped._symbol = symbol  # type: ignore[attr-defined]

    return wrapped


eq = _binary_cmp_wrap(operator.eq, "==")
ne = _binary_cmp_wrap(operator.ne, "!=")
le = _binary_cmp_wrap(operator.le, "<=")
lt = _binary_cmp_wrap(operator.lt, "<")
ge = _binary_cmp_wrap(operator.ge, ">=")
gt = _binary_cmp_wrap(operator.gt, ">")


def _binary_boolean_wrap(op, symbol):
    def wrapped(left: bool, right: bool) -> bool:
        return op(left, right)

    wrapped.__name__ = op.__name__
    wrapped._symbol = symbol  # type: ignore[attr-defined]

    return wrapped


and_ = _binary_boolean_wrap(operator.and_, "&")
or_ = _binary_boolean_wrap(operator.or_, "|")
xor = _binary_boolean_wrap(operator.xor, "^")


def neg(expr: float) -> float:  # type: ignore  # we replace the other signature
    return operator.neg(expr)


neg._symbol = "-"  # type: ignore[attr-defined]


def inv(expr: bool) -> bool:  # type: ignore  # we overwrite the behavior
    return not expr


inv._symbol = "~"  # type: ignore[attr-defined]


def itemgetter(*items, target_type=Any):  # type: ignore  # we replace the other signature
    def wrapped(x):
        return operator.itemgetter(*items)(x)

    wrapped.__annotations__["return"] = target_type
    return wrapped

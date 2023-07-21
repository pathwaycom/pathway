# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Any, Dict

import numpy as np

import pathway.internals as pw


def generate_reducer(fun):
    def wrapper(*args):
        if len(args) == 1 and type(args[0]) is pw.ColumnIxExpression:
            return pw.ReducerIxExpression(fun, *args)
        else:
            return pw.ReducerExpression(fun, *args)

    return wrapper


def _min(col: Dict[int, Any]) -> Any:
    return min(col.values())


def _min_int(col: Dict[int, float]) -> float:
    return min(col.values())


def _max(col: Dict[int, Any]) -> Any:
    return max(col.values())


def _max_int(col: Dict[int, float]) -> float:
    return max(col.values())


def _sum(col: Dict[int, float]) -> float:
    return sum(col.values())


def _int_sum(col: Dict[int, int]) -> int:
    return sum(col.values())


def _npsum(col: Dict[int, np.ndarray]) -> np.ndarray:
    return sum(col.values())  # type: ignore


def _sorted_tuple(col: Dict[int, Any]) -> Any:
    return tuple(sorted(col.values()))


def _count(col: Dict[int, Any]) -> int:
    return len(col.values())


def _argmin(col: Dict[int, Any]) -> Any:
    return min(col.items(), key=lambda i: (i[1], i[0]))[0]


def _argmax(col: Dict[int, Any]) -> Any:
    return max(col.items(), key=lambda i: (i[1], -int(i[0])))[0]


def _unique(col: Dict[int, Any]) -> Any:
    assert len(col.values()) > 0
    val = next(iter(col.values()))
    for it in col.values():
        if it != val:
            raise ValueError(
                "More than one distinct value passed to the unique reducer."
            )
    return val


def _any(col: Dict[int, Any]) -> Any:
    return min(col.items())[1]


class reducers:
    min = staticmethod(generate_reducer(_min))
    min_int = staticmethod(generate_reducer(_min_int))
    max = staticmethod(generate_reducer(_max))
    max_int = staticmethod(generate_reducer(_max_int))

    sum = staticmethod(generate_reducer(_sum))
    int_sum = staticmethod(generate_reducer(_int_sum))

    npsum = staticmethod(generate_reducer(_npsum))

    sorted_tuple = staticmethod(generate_reducer(_sorted_tuple))

    count = staticmethod(generate_reducer(_count))

    argmin = staticmethod(generate_reducer(_argmin))
    argmax = staticmethod(generate_reducer(_argmax))

    unique = staticmethod(generate_reducer(_unique))
    any = staticmethod(generate_reducer(_any))

    @classmethod
    def avg(cls, expression):
        return cls.sum(expression) / cls.count()

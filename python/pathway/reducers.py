# Copyright © 2024 Pathway
"""Reducers are used in `reduce` to compute the aggregated results obtained by a `groupby`.

Typical use:

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
"""

from pathway.internals.custom_reducers import (
    stateful_many,
    stateful_single,
    udf_reducer,
)
from pathway.internals.reducers import (
    any,
    argmax,
    argmin,
    avg,
    count,
    earliest,
    latest,
    max,
    min,
    ndarray,
    sorted_tuple,
    sum,
    tuple,
    unique,
)

__all__ = [
    "any",
    "argmax",
    "argmin",
    "avg",
    "count",
    "earliest",
    "latest",
    "max",
    "min",
    "ndarray",
    "sorted_tuple",
    "stateful_many",
    "stateful_single",
    "sum",
    "tuple",
    "udf_reducer",
    "unique",
]

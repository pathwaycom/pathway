# Copyright © 2023 Pathway

from __future__ import annotations

import datetime
from typing import Dict, Generic, Tuple, TypeVar, Union

import numpy as np
import pandas as pd

from pathway.engine import *
from pathway.internals.schema import Schema

Value = Union[
    None,
    int,
    float,
    str,
    bool,
    BasePointer,
    datetime.datetime,
    datetime.timedelta,
    Tuple["Value", ...],
]
CapturedTable = Dict[BasePointer, Tuple[Value, ...]]
TSchema = TypeVar("TSchema", bound=Schema)


# XXX: engine calls return BasePointer, not Pointer
class Pointer(BasePointer, Generic[TSchema]):
    """Pointer to row type.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | dog
    ... 8   | Alice | cat
    ... 7   | Bob   | dog
    ... ''')
    >>> t2 = t1.select(col=t1.id)
    >>> t2.schema['col'] is pw.Pointer
    True
    """

    pass


def static_table_from_pandas(
    scope, df, dtypes=None, id_from=None, unsafe_trusted_ids=True
) -> Table:
    dtypes = dtypes or {}

    def denumpify_inner(x):
        if pd.api.types.is_scalar(x) and pd.isna(x):
            return None
        if isinstance(x, np.generic):
            return x.item()
        return x

    def denumpify(x):
        v = denumpify_inner(x)
        if isinstance(v, str):
            return v.encode("utf-8", "ignore").decode("utf-8")
        else:
            return v

    if id_from is None:
        if unsafe_trusted_ids:
            ids = {k: unsafe_make_pointer(k) for k in df.index}
        else:
            ids = {k: ref_scalar(k) for k in df.index}
    else:
        ids = {k: ref_scalar(*args) for (k, *args) in df[list(id_from)].itertuples()}

    universe = scope.static_universe(ids.values())
    columns = []
    for c in df.columns:
        data = {ids[k]: denumpify(v) for k, v in df[c].items()}
        if c in dtypes:
            dtype = dtypes[c]
        else:
            dtype = int
            for v in data.values():
                if v is not None:
                    dtype = type(v)
                    break
        columns.append(scope.static_column(universe, list(data.items()), dtype))
    return Table(universe, columns)

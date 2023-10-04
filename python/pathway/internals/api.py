# Copyright © 2023 Pathway

from __future__ import annotations

import datetime
from typing import Any, Generic, TypeVar, Union

import numpy as np
import pandas as pd

from pathway.engine import *
from pathway.internals.schema import Schema

Value = Union[
    None,
    int,
    float,
    str,
    bytes,
    bool,
    BasePointer,
    datetime.datetime,
    datetime.timedelta,
    np.ndarray,
    tuple["Value", ...],
]
CapturedTable = dict[BasePointer, tuple[Value, ...]]
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
    >>> t2.typehints()['col']
    <class 'pathway.internals.api.Pointer'>
    """

    pass


def static_table_from_pandas(
    scope,
    df: pd.DataFrame,
    dtypes: dict[str, Any] | None = None,
    id_from: list[str] | None = None,
    connector_properties: ConnectorProperties | None = None,
) -> Table:
    connector_properties = connector_properties or ConnectorProperties()

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
        if connector_properties.unsafe_trusted_ids:
            ids = {k: unsafe_make_pointer(k) for k in df.index}
        else:
            ids = {k: ref_scalar(k) for k in df.index}
    else:
        ids = {k: ref_scalar(*args) for (k, *args) in df[id_from].itertuples()}

    dtypes_list = []
    all_data: list[tuple[BasePointer, list[Value]]] = [
        (key, []) for key in ids.values()
    ]
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
        dtypes_list.append(dtype)
        for (key, values), (column_key, value) in zip(
            all_data, data.items(), strict=True
        ):
            assert key == column_key
            values.append(value)
    return scope.static_table(all_data, dtypes_list)

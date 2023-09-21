# Copyright © 2023 Pathway

from __future__ import annotations

import datetime
from typing import Any, Dict, Generic, List, Mapping, Optional, Tuple, TypeVar, Union

import numpy as np
import pandas as pd

from pathway.engine import *
from pathway.internals import dtype as dt
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
    >>> t2.schema['col']
    Pointer
    """

    pass


def static_table_from_pandas(
    scope,
    df: pd.DataFrame,
    dtypes: Optional[Dict[str, Any]] = None,
    id_from: Optional[List[str]] = None,
    connector_properties: Optional[ConnectorProperties] = None,
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


_TYPES_TO_ENGINE_MAPPING: Mapping[Any, PathwayType] = {
    dt.BOOL: PathwayType.BOOL,
    dt.INT: PathwayType.INT,
    dt.FLOAT: PathwayType.FLOAT,
    dt.POINTER: PathwayType.POINTER,
    dt.STR: PathwayType.STRING,
    dt.DATE_TIME_NAIVE: PathwayType.DATE_TIME_NAIVE,
    dt.DATE_TIME_UTC: PathwayType.DATE_TIME_UTC,
    dt.DURATION: PathwayType.DURATION,
    dt.Array(): PathwayType.ARRAY,
    dt.ANY: PathwayType.ANY,
    dt.JSON: PathwayType.JSON,
}

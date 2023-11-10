# Copyright © 2023 Pathway

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeAlias, TypeVar, Union

import numpy as np
import pandas as pd

from pathway.engine import *
from pathway.internals import dtype as dt, json

if TYPE_CHECKING:
    _Value: TypeAlias = "Value"
else:
    # Beartype has problems with the recursive definition
    _Value: TypeAlias = Any

Value: TypeAlias = Union[
    None,
    int,
    float,
    str,
    bytes,
    bool,
    Pointer,
    datetime.datetime,
    datetime.timedelta,
    np.ndarray,
    json.Json,
    dict[str, _Value],
    tuple[_Value, ...],
]
CapturedTable = dict[Pointer, tuple[Value, ...]]

S = TypeVar("S", bound=Value)


class CombineMany(Protocol, Generic[S]):
    def __call__(self, state: S | None, rows: list[tuple[list[Value], int]]) -> S:
        ...


def denumpify(x):
    def denumpify_inner(x):
        if pd.api.types.is_scalar(x) and pd.isna(x):
            return None
        if isinstance(x, np.generic):
            return x.item()
        return x

    v = denumpify_inner(x)
    if isinstance(v, str):
        return v.encode("utf-8", "ignore").decode("utf-8")
    else:
        return v


def ids_from_pandas(
    df: pd.DataFrame,
    connector_properties: ConnectorProperties | None,
    id_from: list[str] | None,
) -> dict[Any, Pointer]:
    if id_from is None:
        if connector_properties is not None and connector_properties.unsafe_trusted_ids:
            return {k: unsafe_make_pointer(k) for k in df.index}
        else:
            return {k: ref_scalar(k) for k in df.index}
    else:
        return {k: ref_scalar(*args) for (k, *args) in df[id_from].itertuples()}


def static_table_from_pandas(
    scope,
    df: pd.DataFrame,
    connector_properties: ConnectorProperties | None = None,
    id_from: list[str] | None = None,
) -> Table:
    ids = ids_from_pandas(df, connector_properties, id_from)

    all_data: list[tuple[Pointer, list[Value]]] = [(key, []) for key in ids.values()]

    data = {}
    for c in df.columns:
        data[c] = {ids[k]: denumpify(v) for k, v in df[c].items()}

    if connector_properties is None:
        column_properties = []
        for c in df.columns:
            dtype: type = int
            for v in data[c].values():
                if v is not None:
                    dtype = type(v)
                    break
            column_properties.append(
                ColumnProperties(dtype=dt.wrap(dtype).map_to_engine())
            )
        connector_properties = ConnectorProperties(column_properties=column_properties)

    assert len(connector_properties.column_properties) == len(
        df.columns
    ), "prrovided connector properties do not match the dataframe"

    for c in df.columns:
        for (key, values), (column_key, value) in zip(
            all_data, data[c].items(), strict=True
        ):
            assert key == column_key
            values.append(value)
    return scope.static_table(all_data, connector_properties)

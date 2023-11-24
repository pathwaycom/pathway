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
CapturedStream = list[DataRow]

S = TypeVar("S", bound=Value)


class CombineMany(Protocol, Generic[S]):
    def __call__(
        self, state: S | None, rows: list[tuple[list[Value], int]], /
    ) -> S | None:
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


TIME_PSEUDOCOLUMN = "__time__"
DIFF_PSEUDOCOLUMN = "__diff__"
SHARD_PSEUDOCOLUMN = "__shard__"
PANDAS_PSEUDOCOLUMNS = {TIME_PSEUDOCOLUMN, DIFF_PSEUDOCOLUMN, SHARD_PSEUDOCOLUMN}


def static_table_from_pandas(
    scope,
    df: pd.DataFrame,
    connector_properties: ConnectorProperties | None = None,
    id_from: list[str] | None = None,
) -> Table:
    ids = ids_from_pandas(df, connector_properties, id_from)

    data = {}
    for c in df.columns:
        data[c] = [denumpify(v) for _, v in df[c].items()]
        # df[c].items() is used because df[c].values is a numpy array
    ordinary_columns = [
        column for column in df.columns if column not in PANDAS_PSEUDOCOLUMNS
    ]

    if connector_properties is None:
        column_properties = []
        for c in ordinary_columns:
            dtype: type = int
            for v in data[c]:
                if v is not None:
                    dtype = type(v)
                    break
            column_properties.append(
                ColumnProperties(dtype=dt.wrap(dtype).map_to_engine())
            )
        connector_properties = ConnectorProperties(column_properties=column_properties)

    assert len(connector_properties.column_properties) == len(
        ordinary_columns
    ), "prrovided connector properties do not match the dataframe"

    input_data: CapturedStream = []
    for i, index in enumerate(df.index):
        key = ids[index]
        values = [data[c][i] for c in ordinary_columns]
        time = data[TIME_PSEUDOCOLUMN][i] if TIME_PSEUDOCOLUMN in data else 0
        diff = data[DIFF_PSEUDOCOLUMN][i] if DIFF_PSEUDOCOLUMN in data else 1
        if diff not in [-1, 1]:
            raise ValueError(f"Column {DIFF_PSEUDOCOLUMN} can only contain 1 and -1.")
        shard = data[SHARD_PSEUDOCOLUMN][i] if SHARD_PSEUDOCOLUMN in data else None
        input_row = DataRow(key, values, time=time, diff=diff, shard=shard)
        input_data.append(input_row)

    return scope.static_table(input_data, connector_properties)


def squash_updates(updates: CapturedStream) -> CapturedTable:
    state: CapturedTable = {}
    updates.sort(key=lambda row: (row.time, row.diff))
    for row in updates:
        if row.diff == 1:
            assert row.key not in state, f"duplicated entries for key {row.key}"
            state[row.key] = tuple(row.values)
        elif row.diff == -1:
            assert state[row.key] == tuple(
                row.values
            ), f"deleting non-existing entry {row.values}"
            del state[row.key]
        else:
            raise AssertionError(f"Invalid diff value: {row.diff}")

    return state

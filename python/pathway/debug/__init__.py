# Copyright © 2023 Pathway

from __future__ import annotations

import functools
import io
import re
from os import PathLike
from typing import List, Optional, Type, Union

import pandas as pd

from pathway.internals import Json, api, parse_graph
from pathway.internals.datasource import PandasDataSource
from pathway.internals.decorators import table_from_datasource
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.schema import Schema, schema_from_pandas
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@runtime_type_check
def table_to_dicts(table: Table):
    [captured] = GraphRunner(
        parse_graph.G, debug=True, monitoring_level=MonitoringLevel.NONE
    ).run_tables(table)
    keys = list(captured.keys())
    columns = {
        name: {key: captured[key][index] for key in keys}
        for index, name in enumerate(table._columns.keys())
    }
    return keys, columns


@functools.total_ordering
class _NoneAwareComparisonWrapper:
    def __init__(self, inner):
        if isinstance(inner, dict | Json):
            self.inner = str(inner)
        else:
            self.inner = inner

    def __eq__(self, other):
        if not isinstance(other, _NoneAwareComparisonWrapper):
            return NotImplemented
        return self.inner == other.inner

    def __lt__(self, other):
        if not isinstance(other, _NoneAwareComparisonWrapper):
            return NotImplemented
        if self.inner is None:
            return other.inner is not None
        if other.inner is None:
            return False
        return self.inner < other.inner


@runtime_type_check
@trace_user_frame
def compute_and_print(table: Table, *, include_id=True, short_pointers=True):
    keys, columns = table_to_dicts(table)

    if not columns and not include_id:
        return

    if include_id or len(columns) > 1:
        none = ""
    else:
        none = "None"

    def _format(x):
        if x is None:
            return none
        if isinstance(x, api.BasePointer) and short_pointers:
            s = str(x)
            if len(s) > 8:
                s = s[:8] + "..."
            return s
        return str(x)

    def _key(id):
        return tuple(
            _NoneAwareComparisonWrapper(column[id]) for column in columns.values()
        )

    try:
        keys = sorted(keys, key=_key)
    except ValueError:
        pass  # Some values (like arrays) cannot be sorted this way, so just don't sort them.
    data = []
    if include_id:
        if columns:
            name = ""
        else:
            name = "id"
        data.append([name] + [_format(k) for k in keys])
    for name, column in columns.items():
        data.append([name] + [_format(column[k]) for k in keys])
    max_lens = [max(len(row) for row in column) for column in data]
    max_lens[-1] = 0
    for row in zip(*data):
        formatted = " | ".join(
            value.ljust(max_len) for value, max_len in zip(row, max_lens)
        )
        print(formatted.rstrip())


@runtime_type_check
@trace_user_frame
def table_to_pandas(table: Table):
    keys, columns = table_to_dicts(table)
    res = pd.DataFrame(columns, index=keys)
    return res


@runtime_type_check
@trace_user_frame
def table_from_pandas(
    df: pd.DataFrame,
    id_from: Optional[List[str]] = None,
    unsafe_trusted_ids: bool = False,
    schema: Optional[Type[Schema]] = None,
) -> Table:
    if id_from is not None and schema is not None:
        raise ValueError("parameters `schema` and `id_from` are mutually exclusive")

    if schema is None:
        schema = schema_from_pandas(df, id_from=id_from)
    elif list(df.columns) != schema.column_names():
        raise ValueError("schema does not match given dataframe")

    return table_from_datasource(
        PandasDataSource(
            schema=schema,
            data=df.copy(),
            connector_properties=api.ConnectorProperties(
                unsafe_trusted_ids=unsafe_trusted_ids,
                append_only=schema.properties().append_only,
            ),
        )
    )


def _markdown_to_pandas(table_def):
    table_def = table_def.lstrip("\n")
    sep = r"(?:\s*\|\s*)|\s+"
    header = table_def.partition("\n")[0].strip()
    column_names = re.split(sep, header)
    for index, name in enumerate(column_names):
        if name in ("", "id"):
            index_col = index
            break
    else:
        index_col = None
    return pd.read_table(
        io.StringIO(table_def),
        sep=sep,
        index_col=index_col,
        engine="python",
        na_values=("", "None", "NaN", "nan", "NA", "NULL"),
        keep_default_na=False,
    ).convert_dtypes()


def parse_to_table(
    table_def,
    id_from=None,
    unsafe_trusted_ids=False,
    schema: Optional[Type[Schema]] = None,
) -> Table:
    df = _markdown_to_pandas(table_def)
    return table_from_pandas(
        df, id_from=id_from, unsafe_trusted_ids=unsafe_trusted_ids, schema=schema
    )


@runtime_type_check
def table_from_parquet(
    path: Union[str, PathLike],
    id_from=None,
    unsafe_trusted_ids=False,
) -> Table:
    """
    Reads a Parquet file into a pandas DataFrame and then converts that into a Pathway table.
    """

    df = pd.read_parquet(path)
    return table_from_pandas(df, id_from=None, unsafe_trusted_ids=False)


@runtime_type_check
def table_to_parquet(table: Table, filename: Union[str, PathLike]):
    """
    Converts a Pathway Table into a pandas DataFrame and then writes it to Parquet
    """
    df = table_to_pandas(table)
    df = df.reset_index()
    df = df.drop(["index"], axis=1)
    return df.to_parquet(filename)


# XXX: clean this up
table_from_markdown = parse_to_table

# Copyright © 2023 Pathway

from __future__ import annotations

import pathway._engine_finder  # noqa: F401  # isort: split

import os

from pathway.internals.dtype import DATE_TIME_NAIVE, DATE_TIME_UTC, DURATION

# flake8: noqa: E402

if "PYTEST_CURRENT_TEST" not in os.environ:
    from warnings import filterwarnings

    from beartype.roar import BeartypeDecorHintPep585DeprecationWarning

    filterwarnings("ignore", category=BeartypeDecorHintPep585DeprecationWarning)


import pathway.dt as dt
import pathway.reducers as reducers
from pathway import debug, demo, io
from pathway.internals import (
    ClassArg,
    ColumnExpression,
    ColumnReference,
    FilteredJoinResult,
    GroupedJoinResult,
    GroupedTable,
    Joinable,
    JoinMode,
    JoinResult,
    Json,
    MonitoringLevel,
    Pointer,
    Schema,
    SchemaProperties,
    Table,
    TableLike,
    TableSlice,
    __version__,
    apply,
    apply_async,
    apply_with_type,
    assert_table_has_schema,
    asynchronous,
    attribute,
    cast,
    coalesce,
    column_definition,
    declare_type,
    if_else,
    input_attribute,
    input_method,
    iterate,
    iterate_universe,
    left,
    make_tuple,
    method,
    numba_apply,
    output_attribute,
    require,
    right,
    run,
    run_all,
    schema_builder,
    schema_from_types,
    sql,
    this,
    transformer,
    udf,
    udf_async,
    universes,
    unwrap,
)
from pathway.internals.api import PathwayType as Type
from pathway.stdlib import graphs, indexing, ml, ordered, statistical, temporal, utils
from pathway.stdlib.utils.async_transformer import AsyncTransformer
from pathway.stdlib.utils.pandas_transformer import pandas_transformer

__all__ = [
    "asynchronous",
    "ClassArg",
    "graphs",
    "utils",
    "debug",
    "indexing",
    "ml",
    "apply",
    "udf",
    "udf_async",
    "apply_async",
    "apply_with_type",
    "attribute",
    "declare_type",
    "cast",
    "GroupedTable",
    "input_attribute",
    "input_method",
    "iterate",
    "iterate_universe",
    "JoinResult",
    "FilteredJoinResult",
    "IntervalJoinResult",
    "method",
    "output_attribute",
    "pandas_transformer",
    "AsyncTransformer",
    "reducers",
    "transformer",
    "schema_from_types",
    "Table",
    "TableLike",
    "ColumnReference",
    "ColumnExpression",
    "Schema",
    "Pointer",
    "MonitoringLevel",
    "WindowJoinResult",
    "this",
    "left",
    "right",
    "Joinable",
    "OuterJoinResult",
    "coalesce",
    "require",
    "sql",
    "run",
    "run_all",
    "if_else",
    "make_tuple",
    "Type",
    "numba_apply",
    "__version__",
    "io",
    "universes",
    "window",
    "JoinMode",
    "GroupedJoinResult",
    "AsofJoinResult",
    "temporal",
    "statistical",
    "schema_builder",
    "column_definition",
    "TableSlice",
    "demo",
    "DATE_TIME_NAIVE",
    "DATE_TIME_UTC",
    "DURATION",
    "unwrap",
    "SchemaProperties",
    "assert_table_has_schema",
    "Json",
]


def __getattr__(name: str):
    from warnings import warn

    old_io_names = [
        "csv",
        "debezium",
        "elasticsearch",
        "http",
        "jsonlines",
        "kafka",
        "logstash",
        "null",
        "plaintext",
        "postgres",
        "python",
        "redpanda",
        "subscribe",
        "s3_csv",
    ]

    legacy_names = [
        "ALL",
        "FILES",
        "FOLDERS",
        "File",
        "Folder",
        "PathError",
        "PathObject",
        "SomethingElse",
        "new",
    ]

    if name in old_io_names:
        old_name = f"{__name__}.{name}"
        new_name = f"{__name__}.io.{name}"
        warn(f"{old_name} has been moved to {new_name}", stacklevel=3)
        return getattr(io, name)

    error = f"module {__name__!r} has no attribute {name!r}"
    warning = None

    if name in legacy_names:
        warning = "For help with legacy packages, reach out to the team at pathway.com."
        error += "\n" + warning
        warn(warning, stacklevel=3)

    raise AttributeError(error)


Table.asof_join = temporal.asof_join
Table.asof_join_left = temporal.asof_join_left
Table.asof_join_right = temporal.asof_join_right
Table.asof_join_outer = temporal.asof_join_outer

Table.window_join = temporal.window_join
Table.window_join_inner = temporal.window_join_inner
Table.window_join_left = temporal.window_join_left
Table.window_join_right = temporal.window_join_right
Table.window_join_outer = temporal.window_join_outer

Table.interval_join = temporal.interval_join
Table.interval_join_inner = temporal.interval_join_inner
Table.interval_join_left = temporal.interval_join_left
Table.interval_join_right = temporal.interval_join_right
Table.interval_join_outer = temporal.interval_join_outer

Table.interpolate = statistical.interpolate
Table.windowby = temporal.windowby
Table.sort = indexing.sort
Table.diff = ordered.diff

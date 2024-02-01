# Copyright © 2024 Pathway

from __future__ import annotations

import pathway._engine_finder  # noqa: F401  # isort: split

import pathway.reducers as reducers
import pathway.universes as universes
from pathway import debug, demo, io
from pathway.internals import (
    UDF,
    ClassArg,
    ColumnExpression,
    ColumnReference,
    DateTimeNaive,
    DateTimeUtc,
    Duration,
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
    UDFAsync,
    UDFSync,
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
    groupby,
    if_else,
    input_attribute,
    input_method,
    iterate,
    iterate_universe,
    join,
    join_inner,
    join_left,
    join_outer,
    join_right,
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
    schema_from_csv,
    schema_from_dict,
    schema_from_types,
    sql,
    table_transformer,
    this,
    transformer,
    udf,
    udf_async,
    unwrap,
)
from pathway.internals.api import PathwayType as Type, PersistenceMode
from pathway.internals.custom_reducers import BaseCustomAccumulator
from pathway.internals.dtype import DATE_TIME_NAIVE, DATE_TIME_UTC, DURATION
from pathway.stdlib import (
    graphs,
    indexing,
    ml,
    ordered,
    stateful,
    statistical,
    temporal,
    utils,
    viz,
)
from pathway.stdlib.utils.async_transformer import AsyncTransformer
from pathway.stdlib.utils.pandas_transformer import pandas_transformer

import pathway.persistence as persistence  # isort: skip

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
    "UDF",
    "UDFAsync",
    "UDFSync",
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
    "schema_from_csv",
    "schema_from_dict",
    "assert_table_has_schema",
    "DateTimeNaive",
    "DateTimeUtc",
    "Duration",
    "Json",
    "table_transformer",
    "BaseCustomAccumulator",
    "stateful",
    "viz",
    "PersistenceMode",
    "join",
    "join_inner",
    "join_left",
    "join_right",
    "join_outer",
    "groupby",
    "persistence",
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
        warn(
            f"{old_name!r} has been moved to {new_name!r}",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(io, name)

    error = f"module {__name__!r} has no attribute {name!r}"
    warning = None

    if name in legacy_names:
        warning = "For help with legacy packages, reach out to the team at pathway.com."
        error += "\n" + warning
        warn(warning, stacklevel=2)

    raise AttributeError(error)


Table.asof_join = temporal.asof_join
Table.asof_join_left = temporal.asof_join_left
Table.asof_join_right = temporal.asof_join_right
Table.asof_join_outer = temporal.asof_join_outer

Table.asof_now_join = temporal.asof_now_join
Table.asof_now_join_inner = temporal.asof_now_join_inner
Table.asof_now_join_left = temporal.asof_now_join_left

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
Table.diff = ordered.diff

Table.plot = viz.plot
Table.show = viz.show
Table._repr_mimebundle_ = viz._repr_mimebundle_

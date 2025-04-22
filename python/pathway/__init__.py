# Copyright © 2025 Pathway

from __future__ import annotations

import pathway._engine_finder  # noqa: F401  # isort: split
from pathway.internals import warnings  # noqa: F401  # isort: split

import pathway.reducers as reducers
import pathway.universes as universes
from pathway import debug, demo, io, udfs
from pathway.internals import (
    UDF,
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
    LiveTable,
    MonitoringLevel,
    Pointer,
    PyObjectWrapper,
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
    cast,
    coalesce,
    column_definition,
    declare_type,
    enable_interactive_mode,
    fill_error,
    global_error_log,
    groupby,
    if_else,
    iterate,
    iterate_universe,
    join,
    join_inner,
    join_left,
    join_outer,
    join_right,
    left,
    load_yaml,
    local_error_log,
    make_tuple,
    require,
    right,
    run,
    run_all,
    schema_from_csv,
    schema_from_dict,
    schema_from_types,
    set_license_key,
    set_monitoring_config,
    sql,
    table_transformer,
    this,
    udf,
    unwrap,
    wrap_py_object,
)
from pathway.internals.api import PathwayType as Type, PersistenceMode
from pathway.internals.custom_reducers import BaseCustomAccumulator
from pathway.schema import schema_builder
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
    "udfs",
    "graphs",
    "utils",
    "debug",
    "indexing",
    "ml",
    "apply",
    "udf",
    "UDF",
    "apply_async",
    "apply_with_type",
    "declare_type",
    "cast",
    "GroupedTable",
    "iterate",
    "iterate_universe",
    "JoinResult",
    "IntervalJoinResult",
    "pandas_transformer",
    "AsyncTransformer",
    "reducers",
    "schema_from_types",
    "Table",
    "TableLike",
    "ColumnReference",
    "ColumnExpression",
    "Schema",
    "Pointer",
    "PyObjectWrapper",
    "wrap_py_object",
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
    "unwrap",
    "fill_error",
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
    "enable_interactive_mode",
    "LiveTable",
    "persistence",
    "set_license_key",
    "set_monitoring_config",
    "global_error_log",
    "local_error_log",
    "load_yaml",
]

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

Table.inactivity_detection = temporal.inactivity_detection

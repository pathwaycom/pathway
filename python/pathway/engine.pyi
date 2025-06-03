# Copyright © 2024 Pathway

"""Variant of API with immediate evaluation in Python."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable, Iterable
from enum import Enum
from typing import Any, Generic, TypeVar, TypeVarTuple, Union, final

from pathway.internals.api import (
    CapturedStream,
    CombineMany,
    PyObjectWrapperSerializer,
    S,
    Value,
)
from pathway.internals.column_path import ColumnPath
from pathway.internals.dtype import DType
from pathway.internals.monitoring import StatsMonitor

_T = TypeVarTuple("_T")

@final
class Pointer(Generic[*_T]):
    pass

def ref_scalar(*args, optional=False) -> Pointer: ...
def ref_scalar_with_instance(*args, instance: Value, optional=False) -> Pointer: ...

class PathwayType:
    ANY: PathwayType
    STRING: PathwayType
    INT: PathwayType
    BOOL: PathwayType
    FLOAT: PathwayType
    POINTER: PathwayType
    DATE_TIME_NAIVE: PathwayType
    DATE_TIME_UTC: PathwayType
    DURATION: PathwayType
    @staticmethod
    def array(dim: int | None, wrapped: PathwayType) -> PathwayType: ...
    JSON: PathwayType
    @staticmethod
    def tuple(*args: PathwayType) -> PathwayType: ...
    @staticmethod
    def list(arg: PathwayType) -> PathwayType: ...
    BYTES: PathwayType
    PY_OBJECT_WRAPPER: PathwayType
    @staticmethod
    def optional(arg: PathwayType) -> PathwayType: ...
    @staticmethod
    def future(arg: PathwayType) -> PathwayType: ...

class ConnectorMode(Enum):
    STATIC: ConnectorMode
    STREAMING: ConnectorMode

class ReadMethod(Enum):
    BY_LINE: ReadMethod
    FULL: ReadMethod

class DebeziumDBType(Enum):
    POSTGRES: DebeziumDBType
    MONGO_DB: DebeziumDBType

class KeyGenerationPolicy(Enum):
    ALWAYS_AUTOGENERATE: KeyGenerationPolicy
    PREFER_MESSAGE_KEY: KeyGenerationPolicy

class Universe:
    pass

@dataclasses.dataclass(frozen=True)
class Trace:
    file_name: str
    line_number: int
    line: str
    function: str

@dataclasses.dataclass(frozen=True)
class ColumnProperties:
    dtype: PathwayType | None = None
    trace: Trace | None = None
    append_only: bool = False

class TableProperties:
    @staticmethod
    def column(column_prroperties: ColumnProperties) -> TableProperties: ...
    @staticmethod
    def from_column_properties(
        column_properties: Iterable[tuple[ColumnPath, ColumnProperties]],
        trace: Trace | None = None,
    ) -> TableProperties: ...

@dataclasses.dataclass(frozen=True)
class ConnectorProperties:
    commit_duration_ms: int | None = None
    unsafe_trusted_ids: bool | None = False
    column_properties: list[ColumnProperties] = []
    unique_name: str | None = None
    synchronization_group: ConnectorGroupDescriptor | None = None

class Column:
    """A Column holds data and conceptually is a Dict[Universe elems, dt]

    Columns should not be constructed directly, but using methods of the scope.
    All fields are private.
    """

    @property
    def universe(self) -> Universe: ...

class LegacyTable:
    """A `LegacyTable` is a thin wrapper over a list of Columns.

    universe and columns are public fields - tables can be constructed
    """

    def __init__(self, universe: Universe, columns: list[Column]): ...
    @property
    def universe(self) -> Universe: ...
    @property
    def columns(self) -> list[Column]: ...

class Table:
    """Table with tuples containing values from multiple columns."""

class ErrorLog:
    """Error log you can append messages to."""

class DataRow:
    """Row of data for static_table"""

    key: Pointer
    values: list[Value]
    time: int
    diff: int
    shard: int | None

    def __init__(
        self,
        key: Pointer,
        values: list[Value],
        *,
        time: int = 0,
        diff: int = 1,
        shard: int | None = None,
        dtypes: list[PathwayType],
    ) -> None: ...

class MissingValueError(BaseException):
    "Marker class to indicate missing attributes"

class EngineError(Exception):
    "Marker class to indicate engine error"

class EngineErrorWithTrace(Exception):
    "Marker class to indicate engine error with trace"
    args: tuple[Exception, Trace | None]

class OtherWorkerError(Exception):
    "Marker class to indicate engine error resulting from other worker failure"

class Reducer:
    ARG_MIN: Reducer
    MIN: Reducer
    ARG_MAX: Reducer
    MAX: Reducer
    FLOAT_SUM: Reducer
    ARRAY_SUM: Reducer
    INT_SUM: Reducer
    @staticmethod
    def sorted_tuple(skip_nones: bool) -> Reducer: ...
    @staticmethod
    def tuple(skip_nones: bool) -> Reducer: ...
    UNIQUE: Reducer
    ANY: Reducer
    COUNT: Reducer
    @staticmethod
    def stateful_many(combine_many: CombineMany[S]) -> Reducer: ...
    EARLIEST: Reducer
    LATEST: Reducer

class ExpressionData:
    def __init__(
        self,
        expression: Expression,
        properties: TableProperties,
        append_only: bool,
        deterministic: bool,
    ) -> None: ...

@dataclasses.dataclass
class ReducerData:
    reducer: Reducer
    skip_errors: bool
    column_paths: list[ColumnPath]
    trace: Trace | None

class UnaryOperator:
    INV: UnaryOperator
    NEG: UnaryOperator

class BinaryOperator:
    AND: BinaryOperator
    OR: BinaryOperator
    XOR: BinaryOperator
    EQ: BinaryOperator
    NE: BinaryOperator
    LT: BinaryOperator
    LE: BinaryOperator
    GT: BinaryOperator
    GE: BinaryOperator
    ADD: BinaryOperator
    SUB: BinaryOperator
    MUL: BinaryOperator
    FLOOR_DIV: BinaryOperator
    TRUE_DIV: BinaryOperator
    MOD: BinaryOperator
    POW: BinaryOperator
    LSHIFT: BinaryOperator
    RSHIFT: BinaryOperator
    MATMUL: BinaryOperator

class Expression:
    @staticmethod
    def const(value: Value, dtype: PathwayType) -> Expression: ...
    @staticmethod
    def argument(index: int) -> Expression: ...
    @staticmethod
    def apply(
        fun: Callable,
        /,
        *args: Expression,
        dtype: PathwayType,
        propagate_none: bool = False,
    ) -> Expression: ...
    @staticmethod
    def is_none(expr: Expression) -> Expression: ...
    @staticmethod
    def unary_expression(
        expr: Expression, operator: UnaryOperator, expr_dtype: PathwayType
    ) -> Expression | None: ...
    @staticmethod
    def binary_expression(
        lhs: Expression,
        rhs: Expression,
        operator: BinaryOperator,
        left_dtype: PathwayType,
        right_dtype: PathwayType,
    ) -> Expression | None: ...
    @staticmethod
    def eq(lhs: Expression, rhs: Expression) -> Expression: ...
    @staticmethod
    def ne(lhs: Expression, rhs: Expression) -> Expression: ...
    @staticmethod
    def int_abs(lhs: Expression, rhs: Expression) -> Expression: ...
    @staticmethod
    def float_abs(lhs: Expression, rhs: Expression) -> Expression: ...
    @staticmethod
    def cast(
        expr: Expression, source_type: PathwayType, target_type: PathwayType
    ) -> Expression | None: ...
    @staticmethod
    def cast_optional(
        expr: Expression, source_type: PathwayType, target_type: PathwayType
    ) -> Expression | None: ...
    def convert(
        expr: Expression,
        default: Expression,
        source_type: PathwayType,
        target_type: PathwayType,
        unwrap: bool,
    ) -> Expression | None: ...
    @staticmethod
    def if_else(if_: Expression, then: Expression, else_: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_nanosecond(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_microsecond(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_millisecond(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_second(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_minute(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_hour(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_day(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_month(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_year(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_timestamp_ns(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_timestamp(expr: Expression, unit: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_weekday(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_strptime(expr: Expression, fmt: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_strftime(expr: Expression, fmt: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_from_timestamp(
        expr: Expression, unit: Expression
    ) -> Expression: ...
    @staticmethod
    def date_time_naive_from_float_timestamp(
        expr: Expression, unit: Expression
    ) -> Expression: ...
    @staticmethod
    def date_time_naive_to_utc(
        expr: Expression, from_timezone: Expression
    ) -> Expression: ...
    @staticmethod
    def date_time_naive_round(expr: Expression, duration: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_floor(expr: Expression, duration: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_nanosecond(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_microsecond(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_millisecond(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_second(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_minute(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_hour(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_day(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_month(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_year(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_timestamp_ns(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_timestamp(expr: Expression, unit: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_weekday(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_strptime(expr: Expression, fmt: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_strftime(expr: Expression, fmt: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_to_naive(
        expr: Expression, to_timezone: Expression
    ) -> Expression: ...
    @staticmethod
    def date_time_utc_round(expr: Expression, duration: Expression) -> Expression: ...
    @staticmethod
    def date_time_utc_floor(expr: Expression, duration: Expression) -> Expression: ...
    @staticmethod
    def to_duration(expr: Expression, unit: Expression) -> Expression: ...
    @staticmethod
    def duration_nanoseconds(expr: Expression) -> Expression: ...
    @staticmethod
    def duration_microseconds(expr: Expression) -> Expression: ...
    @staticmethod
    def duration_milliseconds(expr: Expression) -> Expression: ...
    @staticmethod
    def duration_seconds(expr: Expression) -> Expression: ...
    @staticmethod
    def duration_minutes(expr: Expression) -> Expression: ...
    @staticmethod
    def duration_hours(expr: Expression) -> Expression: ...
    @staticmethod
    def duration_days(expr: Expression) -> Expression: ...
    @staticmethod
    def duration_weeks(expr: Expression) -> Expression: ...
    @staticmethod
    def parse_int(expr: Expression, optional: bool) -> Expression: ...
    @staticmethod
    def parse_float(expr: Expression, optional: bool) -> Expression: ...
    @staticmethod
    def parse_bool(
        expr: Expression, true_list: list[str], false_list: list[str], optional: bool
    ) -> Expression: ...
    @staticmethod
    def pointer_from(
        *args: Expression,
        optional: bool,
        instance: Expression | None = None,
    ) -> Expression: ...
    @staticmethod
    def make_tuple(*args: Expression) -> Expression: ...
    @staticmethod
    def sequence_get_item_checked(
        expr: Expression, index: Expression, default: Expression
    ) -> Expression: ...
    @staticmethod
    def sequence_get_item_unchecked(
        expr: Expression, index: Expression
    ) -> Expression: ...
    @staticmethod
    def json_get_item_checked(
        expr: Expression, index: Expression, default: Expression
    ) -> Expression: ...
    @staticmethod
    def json_get_item_unchecked(expr: Expression, index: Expression) -> Expression: ...
    @staticmethod
    def unwrap(expr: Expression) -> Expression: ...
    @staticmethod
    def to_string(expr: Expression) -> Expression: ...
    @staticmethod
    def fill_error(expr: Expression, replacement: Expression) -> Expression: ...

class MonitoringLevel(Enum):
    NONE = 0
    IN_OUT = 1
    ALL = 2

class Context:
    # "Location" of the current attribute in the transformer computation
    this_row: Pointer
    data: tuple[Value, Pointer]

    def raising_get(self, column: int, row: Pointer, *args: Value) -> Value: ...

class Computer:
    @classmethod
    def from_raising_fun(
        cls,
        fun: Callable[[Context], Value],
        *,
        dtype: DType,
        is_output: bool,
        is_method: bool,
        universe: Universe,
        data: Value = None,
        data_column: Column | None = None,
    ) -> Computer: ...

ComplexColumn = Union[Column, Computer]

class Scope:
    @property
    def parent(self) -> Scope | None: ...
    @property
    def worker_index(self) -> int: ...
    @property
    def worker_count(self) -> int: ...
    @property
    def thread_count(self) -> int: ...
    @property
    def process_count(self) -> int: ...
    def empty_table(self, properties: ConnectorProperties) -> Table: ...
    def iterate(
        self,
        iterated: list[LegacyTable],
        iterated_with_universe: list[LegacyTable],
        extra: list[LegacyTable],
        logic: Callable[
            [Scope, list[LegacyTable], list[LegacyTable], list[LegacyTable]],
            tuple[list[LegacyTable], list[LegacyTable]],
        ],
        *,
        limit: int | None = None,
    ) -> tuple[list[LegacyTable], list[LegacyTable]]:
        """Fixed-point iteration

        logic is called with a new scope, clones of tables from iterated,
        clones of tables from extra.
        logic should not use any other outside tables.
        logic must return a list of tables corresponding to iterated:
        result[i] is the result of single iteration on iterated[i]
        """
        ...
    # Evaluators for expressions

    def static_universe(self, keys: Iterable[Pointer]) -> Universe: ...
    def static_column(
        self, universe: Universe, rows: Iterable[tuple[Pointer, Any]], dt: DType
    ) -> Column: ...
    def static_table(
        self,
        universe: Universe,
        rows: Iterable[DataRow],
        dt: DType,
    ) -> Table: ...
    def map_column(
        self,
        table: LegacyTable,
        function: Callable[[tuple[Value, ...]], Value],
        properties: ColumnProperties,
    ) -> Column: ...
    def expression_table(
        self,
        table: Table,
        column_paths: list[ColumnPath],
        expressions: list[ExpressionData],
        deterministic: bool,
    ) -> Table: ...
    def table_properties(self, table: Table, path: ColumnPath) -> TableProperties: ...
    def columns_to_table(self, universe: Universe, columns: list[Column]) -> Table: ...
    def table_column(
        self, universe: Universe, table: Table, column_path: ColumnPath
    ) -> Column: ...
    def table_universe(self, table: Table) -> Universe: ...
    def flatten_table_storage(
        self, table: Table, column_paths: list[ColumnPath]
    ) -> Table: ...
    def async_apply_table(
        self,
        table: Table,
        column_paths: list[ColumnPath],
        function: Callable[..., Value],
        propagate_none: bool,
        deterministic: bool,
        properties: TableProperties,
        dtype: PathwayType,
    ) -> Table: ...
    def gradual_broadcast(
        self,
        input_table_storage: Table,
        threshold_table_storage: Table,
        lower_column: ColumnPath,
        value_column: ColumnPath,
        upper_column: ColumnPath,
        table_properties: TableProperties,
    ) -> Table: ...
    def filter_table(
        self, table: Table, path: ColumnPath, table_properties: TableProperties
    ) -> Table: ...
    def forget(
        self,
        table: Table,
        threshold_time_path: ColumnPath,
        current_time_path: ColumnPath,
        instance_path: ColumnPath,
        mark_forgetting_records: bool,
        table_properties: TableProperties,
    ) -> Table: ...
    def forget_immediately(
        self,
        table: Table,
        table_properties: TableProperties,
    ) -> Table: ...
    def filter_out_results_of_forgetting(
        self,
        table: Table,
        table_properties: TableProperties,
    ) -> Table: ...
    def freeze(
        self,
        table: Table,
        threshold_time_path: ColumnPath,
        current_time_path: ColumnPath,
        instance_path: ColumnPath,
        table_properties: TableProperties,
    ) -> Table: ...
    def buffer(
        self,
        table: Table,
        threshold_time_path: ColumnPath,
        current_time_path: ColumnPath,
        instance_path: ColumnPath,
        table_properties: TableProperties,
    ) -> Table: ...
    def intersect_tables(
        self, table: Table, tables: Iterable[Table], table_properties: TableProperties
    ) -> Table: ...
    def subtract_table(
        self, left_table: Table, right_table: Table, table_properties: TableProperties
    ) -> Table: ...
    def restrict_column(
        self,
        universe: Universe,
        column: Column,
    ) -> Column: ...
    def restrict_table(
        self, orig_table: Table, new_table: Table, table_properties: TableProperties
    ) -> Table: ...
    def override_table_universe(
        self, orig_table: Table, new_table: Table, table_properties: TableProperties
    ) -> Table: ...
    def reindex_table(
        self,
        table: Table,
        reindexing_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> Table: ...
    def connector_table(
        self,
        data_source: DataStorage,
        data_format: DataFormat,
        properties: ConnectorProperties,
    ) -> Table: ...
    @staticmethod
    def table(universe: Universe, columns: list[Column]) -> LegacyTable: ...

    # Grouping and joins

    def group_by_table(
        self,
        table: Table,
        grouping_columns: list[ColumnPath],
        last_column_is_instance: bool,
        reducers: list[ReducerData],
        by_id: bool,
        table_properties: TableProperties,
    ) -> Table: ...
    def deduplicate(
        self,
        table: Table,
        grouping_columns: list[ColumnPath],
        reduced_columns: list[ColumnPath],
        combine: Callable[[Any, Any], Any],
        unique_name: str | None,
        table_properties: TableProperties,
    ) -> Table: ...
    def ix_table(
        self,
        to_ix_table: Table,
        key_table: Table,
        key_column_path: ColumnPath,
        optional: bool,
        strict: bool,
        table_properties: TableProperties,
    ) -> Table: ...
    def join_tables(
        self,
        left_storage: Table,
        right_storage: Table,
        left_paths: list[ColumnPath],
        right_paths: list[ColumnPath],
        *,
        last_column_is_instance: bool,
        table_properties: TableProperties,
        assign_id: bool = False,
        left_ear: bool = False,
        right_ear: bool = False,
    ) -> Table: ...
    def use_external_index_as_of_now(
        self,
        index: ExternalIndexData,
        queries: ExternalIndexQuery,
        table_properties: TableProperties,
        external_index_factory: ExternalIndexFactory,
    ) -> Table: ...

    # Transformers

    def complex_columns(self, inputs: list[ComplexColumn]) -> list[Column]: ...

    # Updates

    def update_rows_table(
        self, table: Table, update: Table, table_properties: TableProperties
    ) -> Table: ...
    def update_cells_table(
        self,
        table: Table,
        update: Table,
        table_columns: list[ColumnPath],
        update_columns: list[ColumnPath],
        table_properties: TableProperties,
    ) -> Table: ...
    def debug_table(
        self, name: str, table: Table, columns: list[tuple[str, ColumnPath]]
    ): ...
    def concat_tables(
        self, tables: Iterable[Table], table_properties: TableProperties
    ) -> Table: ...
    def flatten_table(
        self, table: Table, path: ColumnPath, table_properties: TableProperties
    ) -> Table: ...
    def sort_table(
        self,
        table: Table,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> Table: ...
    def probe_table(self, table: Table, operator_id: int): ...
    def subscribe_table(
        self,
        table: Table,
        column_paths: Iterable[ColumnPath],
        skip_persisted_batch: bool,
        skip_errors: bool,
        on_change: Callable,
        on_time_end: Callable,
        on_end: Callable,
        unique_name: str | None = None,
        sort_by_indices: Iterable[int] | None = None,
    ): ...
    def output_table(
        self,
        table: Table,
        column_paths: Iterable[ColumnPath],
        data_sink: DataStorage,
        data_format: DataFormat,
        unique_name: str | None = None,
        sort_by_indices: Iterable[int] | None = None,
    ): ...
    def export_table(
        self, table: Table, column_paths: Iterable[ColumnPath]
    ) -> ExportedTable: ...
    def import_table(self, table: ExportedTable) -> Table: ...
    def error_log(self, properties: ConnectorProperties) -> tuple[Table, ErrorLog]: ...
    def set_error_log(self, error_log: ErrorLog | None) -> None: ...
    def set_operator_properties(self, id: int, depends_on_error_log: bool) -> None: ...
    def remove_value_from_table(
        self,
        table: Table,
        column_paths: Iterable[ColumnPath],
        value: Value,
        table_properties: TableProperties,
    ) -> Table: ...
    def remove_retractions_from_table(
        self,
        table: Table,
        table_properties: TableProperties,
    ) -> Table: ...
    def async_transformer(
        self,
        table: Table,
        column_paths: Iterable[ColumnPath],
        on_change: Callable,
        on_time_end: Callable,
        on_end: Callable,
        data_source: DataStorage,
        data_format: DataFormat,
        table_properties: ConnectorProperties,
        skip_errors: bool,
    ) -> Table: ...

class Error: ...

ERROR: Error

class Pending: ...

PENDING: Pending

class Done:
    def __lt__(self, other: Frontier) -> bool: ...
    def __le__(self, other: Frontier) -> bool: ...
    def __gt__(self, other: Frontier) -> bool: ...
    def __ge__(self, other: Frontier) -> bool: ...

DONE: Done

Frontier = int | Done

class ExportedTable:
    def failed(self) -> bool: ...
    def frontier(self) -> Frontier: ...
    def snapshot_at(self, frontier: Frontier) -> list[tuple[Pointer, list[Value]]]: ...

def run_with_new_graph(
    logic: Callable[[Scope], Iterable[tuple[Table, list[ColumnPath]]]],
    event_loop: asyncio.AbstractEventLoop,
    stats_monitor: StatsMonitor | None = None,
    *,
    ignore_asserts: bool = False,
    monitoring_level: MonitoringLevel = MonitoringLevel.NONE,
    with_http_server: bool = False,
    persistence_config: PersistenceConfig | None = None,
    license_key: str | None = None,
    monitoring_server: str | None = None,
    trace_parent: str | None = None,
    run_id: str | None = None,
    terminate_on_error: bool = True,
) -> list[CapturedStream]: ...
def unsafe_make_pointer(arg) -> Pointer: ...

class DataFormat:
    value_fields: list[ValueField]

    def __init__(self, *args, **kwargs): ...
    def is_native_session_used(self) -> bool: ...

class BackfillingThreshold:
    field: str
    threshold: Value
    comparison_op: str

    def __init__(self, *args, **kwargs): ...

class DataStorage:
    storage_type: str
    path: str | None
    rdkafka_settings: dict[str, str] | None
    topic: str | None
    connection_string: str | None
    csv_parser_settings: CsvParserSettings | None
    mode: ConnectorMode
    read_method: ReadMethod
    aws_s3_settings: AwsS3Settings | None
    elasticsearch_params: ElasticSearchParams | None
    parallel_readers: int | None
    python_subject: PythonSubject | None
    unique_name: str | None
    max_batch_size: int | None
    object_pattern: str
    mock_events: dict[tuple[str, int], list[SnapshotEvent]] | None
    table_name: str | None
    sql_writer_init_mode: SqlWriterInitMode
    topic_name_index: int | None
    partition_columns: list[str] | None

    def __init__(self, *args, **kwargs): ...
    def delta_s3_storage_options(self, *args, **kwargs): ...

class CsvParserSettings:
    def __init__(self, *args, **kwargs): ...

class AwsS3Settings:
    def __init__(self, *args, **kwargs): ...

class AzureBlobStorageSettings:
    def __init__(self, *args, **kwargs): ...

class ValueField:
    name: str
    def __init__(self, name: str, type_: PathwayType): ...
    def set_default(self, *args, **kwargs): ...
    def set_metadata(self, *args, **kwargs): ...

class PythonSubject:
    def __init__(self, *args, **kwargs): ...

class ElasticSearchAuth:
    def __init__(self, *args, **kwargs): ...

class ElasticSearchParams:
    def __init__(self, *args, **kwargs): ...

class PersistenceConfig:
    def __init__(self, *args, **kwargs): ...

class ConnectorGroupDescriptor:
    def __init__(self, *args, **kwargs): ...

class PersistenceMode(Enum):
    BATCH: PersistenceMode
    SPEEDRUN_REPLAY: PersistenceMode
    REALTIME_REPLAY: PersistenceMode
    PERSISTING: PersistenceMode
    SELECTIVE_PERSISTING: PersistenceMode
    UDF_CACHING: PersistenceMode
    OPERATOR_PERSISTING: PersistenceMode

class SnapshotAccess(Enum):
    RECORD: SnapshotAccess
    REPLAY: SnapshotAccess
    FULL: SnapshotAccess
    OFFSETS_ONLY: SnapshotAccess

class PythonConnectorEventType(Enum):
    INSERT: PythonConnectorEventType
    DELETE: PythonConnectorEventType
    EXTERNAL_OFFSET: PythonConnectorEventType

class SessionType(Enum):
    NATIVE: SessionType
    UPSERT: SessionType

class SqlWriterInitMode(Enum):
    DEFAULT: SqlWriterInitMode
    CREATE_IF_NOT_EXISTS: SqlWriterInitMode
    REPLACE: SqlWriterInitMode

class SnapshotEvent:
    @staticmethod
    def insert(key: Pointer, values: list[Value]) -> SnapshotEvent: ...
    @staticmethod
    def delete(key: Pointer, values: list[Value]) -> SnapshotEvent: ...
    @staticmethod
    def advance_time(timestamp: int) -> SnapshotEvent: ...
    FINISHED: SnapshotEvent

class LocalBinarySnapshotWriter:
    def __init__(self, path: str, unique_name: str, worker_id: int): ...
    def write(self, events: list[SnapshotEvent]): ...

class TelemetryConfig:
    logging_servers: list[str]
    tracing_servers: list[str]
    metrics_servers: list[str]
    service_name: str | None
    service_version: str | None
    service_namespace: str | None
    service_instance_id: str | None
    run_id: str
    license_key: str | None
    @staticmethod
    def create(
        *,
        run_id: str,
        license_key: str | None = None,
        monitoring_server: str | None = None,
    ) -> TelemetryConfig: ...

class ExternalIndexFactory:
    @staticmethod
    def usearch_knn_factory(
        *,
        dimensions: int,
        reserved_space: int,
        metric: USearchMetricKind,
        connectivity: int,
        expansion_add: int,
        expansion_search: int,
    ) -> ExternalIndexFactory: ...
    @staticmethod
    def tantivy_factory(
        *,
        ram_budget: int,
        in_memory_index: bool,
    ) -> ExternalIndexFactory: ...
    @staticmethod
    def brute_force_knn_factory(
        *,
        dimensions: int,
        reserved_space: int,
        auxiliary_space: int,
        metric: BruteForceKnnMetricKind,
    ) -> ExternalIndexFactory: ...

@dataclasses.dataclass(frozen=True)
class ExternalIndexData:
    table: Table
    data_column: ColumnPath
    filter_data_column: ColumnPath | None

@dataclasses.dataclass(frozen=True)
class ExternalIndexQuery:
    table: Table
    query_column: ColumnPath
    limit_column: ColumnPath | None
    filter_column: ColumnPath | None

class USearchMetricKind(Enum):
    IP: USearchMetricKind
    L2SQ: USearchMetricKind
    COS: USearchMetricKind
    PEARSON: USearchMetricKind
    HAVERSINE: USearchMetricKind
    DIVERGENCE: USearchMetricKind
    HAMMING: USearchMetricKind
    TANIMOTO: USearchMetricKind
    SORENSEN: USearchMetricKind

class BruteForceKnnMetricKind(Enum):
    L2SQ: BruteForceKnnMetricKind
    COS: BruteForceKnnMetricKind

def check_entitlements(
    *,
    license_key: str | None,
    entitlements: list[str],
): ...
def deserialize(data: bytes) -> Value: ...
def serialize(value: Value) -> bytes: ...

T = TypeVar("T")

@dataclasses.dataclass(frozen=True)
class PyObjectWrapper(Generic[T]):
    """A wrapper over python objects of any type that enables passing them to the engine.

    If you want to specify a custom serializer, ``pw.wrap_py_object`` should be used.

    Args:
        value: a python object to be wrapped

    Example:

    >>> import pathway as pw
    >>> from dataclasses import dataclass
    >>>
    >>> @dataclass
    ... class Simple:
    ...     a: int
    ...     def add(self, x: int) -> int:
    ...         return self.a + x
    ...
    >>> @pw.udf
    ... def create_py_object(a: int) -> pw.PyObjectWrapper[Simple]:
    ...     return pw.PyObjectWrapper(Simple(a))
    ...
    >>> @pw.udf
    ... def use_py_object(a: int, b: pw.PyObjectWrapper[Simple]) -> int:
    ...     return b.value.add(a)
    ...
    >>> t = pw.debug.table_from_markdown(
    ...     '''
    ...     a
    ...     1
    ...     2
    ...     3
    ... '''
    ... ).with_columns(b=create_py_object(pw.this.a))
    >>> res = t.select(res=use_py_object(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(res, include_id=False)
    res
    2
    4
    6
    """

    value: T

    @staticmethod
    def _create_with_serializer(
        value: T, *, serializer: PyObjectWrapperSerializer | None = None
    ) -> PyObjectWrapper[T]: ...

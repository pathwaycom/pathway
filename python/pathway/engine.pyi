# Copyright © 2023 Pathway

"""Variant of API with immediate evaluation in Python."""

from __future__ import annotations

import asyncio
import dataclasses
from enum import Enum
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

from pathway.internals.api import CapturedTable, Value
from pathway.internals.column_path import ColumnPath
from pathway.internals.dtype import DType
from pathway.internals.monitoring import StatsMonitor

class BasePointer:
    pass

def ref_scalar(*args, optional=False) -> BasePointer: ...

class PathwayType(Enum):
    ANY: PathwayType
    STRING: PathwayType
    INT: PathwayType
    BOOL: PathwayType
    FLOAT: PathwayType
    POINTER: PathwayType
    DATE_TIME_NAIVE: PathwayType
    DATE_TIME_UTC: PathwayType
    DURATION: PathwayType
    ARRAY: PathwayType
    JSON: PathwayType

class ConnectorMode(Enum):
    STATIC: ConnectorMode
    SIMPLE_STREAMING: ConnectorMode
    STREAMING_WITH_DELETIONS: ConnectorMode

class ReadMethod(Enum):
    BY_LINE: ReadMethod
    FULL: ReadMethod

class Universe:
    @property
    def id_column(self) -> Column: ...

@dataclasses.dataclass(frozen=True)
class Trace:
    file_name: str
    line_number: int
    line: str
    function: str

@dataclasses.dataclass(frozen=True)
class EvalProperties:
    dtype: Optional[DType] = None
    trace: Optional[Trace] = None
    append_only: bool = False

@dataclasses.dataclass(frozen=True)
class ConnectorProperties:
    commit_duration_ms: Optional[int] = None
    unsafe_trusted_ids: Optional[bool] = False
    append_only: bool = False

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

    def __init__(self, universe: Universe, columns: List[Column]): ...
    @property
    def universe(self) -> Universe: ...
    @property
    def columns(self) -> List[Column]: ...

class Table:
    """Table with tuples containing values from multiple columns."""

class MissingValueError(BaseException):
    "Marker class to indicate missing attributes"

class EngineError(Exception):
    "Marker class to indicate engine error"

class EngineErrorWithTrace(Exception):
    "Marker class to indicate engine error with trace"
    args: tuple[Exception, Optional[Trace]]

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
    def const(value: Value) -> Expression: ...
    @staticmethod
    def argument(index: int) -> Expression: ...
    @staticmethod
    def apply(fun: Callable, /, *args: Expression) -> Expression: ...
    @staticmethod
    def unsafe_numba_apply(fun: Callable, /, *args: Expression) -> Expression: ...
    @staticmethod
    def is_none(expr: Expression) -> Expression: ...
    @staticmethod
    def unary_expression(
        expr: Expression, operator: UnaryOperator, expr_dtype: PathwayType
    ) -> Optional[Expression]: ...
    @staticmethod
    def binary_expression(
        lhs: Expression,
        rhs: Expression,
        operator: BinaryOperator,
        left_dtype: PathwayType,
        right_dtype: PathwayType,
    ) -> Optional[Expression]: ...
    @staticmethod
    def eq(lhs: Expression, rhs: Expression) -> Expression: ...
    @staticmethod
    def ne(lhs: Expression, rhs: Expression) -> Expression: ...
    @staticmethod
    def cast(
        expr: Expression, source_type: PathwayType, target_type: PathwayType
    ) -> Optional[Expression]: ...
    @staticmethod
    def cast_optional(
        expr: Expression, source_type: PathwayType, target_type: PathwayType
    ) -> Optional[Expression]: ...
    def convert_optional(
        expr: Expression, source_type: PathwayType, target_type: PathwayType
    ) -> Optional[Expression]: ...
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
    def date_time_naive_timestamp(expr: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_strptime(expr: Expression, fmt: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_strftime(expr: Expression, fmt: Expression) -> Expression: ...
    @staticmethod
    def date_time_naive_from_timestamp(
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
    def date_time_utc_timestamp(expr: Expression) -> Expression: ...
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
    def pointer_from(*args: Expression, optional: bool) -> Expression: ...
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

class MonitoringLevel(Enum):
    NONE = 0
    IN_OUT = 1
    ALL = 2

class Context:
    # "Location" of the current attribute in the transformer computation
    this_row: BasePointer
    data: Tuple[Value, BasePointer]

    def raising_get(self, column: int, row: BasePointer, *args: Value) -> Value: ...

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
        data_column: Optional[Column] = None,
    ) -> Computer: ...

ComplexColumn = Union[Column, Computer]

class VennUniverses:
    def only_left(self) -> Universe: ...
    def only_right(self) -> Universe: ...
    def both(self) -> Universe: ...

class Scope:
    @property
    def parent(self) -> Optional[Scope]: ...
    def empty_table(self, dtypes: Iterable[DType]) -> Table: ...
    def iterate(
        self,
        iterated: List[LegacyTable],
        iterated_with_universe: List[LegacyTable],
        extra: List[LegacyTable],
        logic: Callable[
            [Scope, List[LegacyTable], List[LegacyTable], List[LegacyTable]],
            Tuple[List[LegacyTable], List[LegacyTable]],
        ],
        *,
        limit: Optional[int] = None,
    ) -> Tuple[List[LegacyTable], List[LegacyTable]]:
        """Fixed-point iteration

        logic is called with a new scope, clones of tables from iterated,
        clones of tables from extra.
        logic should not use any other outside tables.
        logic must return a list of tables corresponding to iterated:
        result[i] is the result of single iteration on iterated[i]
        """
        ...
    # Evaluators for expressions

    def static_universe(self, keys: Iterable[BasePointer]) -> Universe: ...
    def static_column(
        self, universe: Universe, rows: Iterable[Tuple[BasePointer, Any]], dt: DType
    ) -> Column: ...
    def static_table(
        self,
        universe: Universe,
        rows: Iterable[Tuple[BasePointer, List[Value]]],
        dt: DType,
    ) -> Table: ...
    def map_column(
        self,
        table: LegacyTable,
        function: Callable[[Tuple[Value, ...]], Value],
        properties: EvalProperties,
    ) -> Column: ...
    def expression_column(
        self, table: LegacyTable, expression: Expression, properties: EvalProperties
    ) -> Column: ...
    def expression_table(
        self,
        table: Table,
        column_paths: List[ColumnPath],
        expressions: List[Tuple[Expression, EvalProperties]],
    ) -> Table: ...
    def columns_to_table(
        self, universe: Universe, columns: List[Tuple[Column, ColumnPath]]
    ) -> Table: ...
    def table_column(
        self, universe: Universe, table: Table, column_path: ColumnPath
    ) -> Column: ...
    def table_universe(self, table: Table) -> Universe: ...
    def flatten_table_storage(
        self, table: Table, column_paths: List[ColumnPath]
    ) -> Table: ...
    def async_apply_table(
        self,
        table: Table,
        column_paths: List[ColumnPath],
        function: Callable[..., Value],
        properties: EvalProperties,
    ) -> Table: ...
    def filter_universe(self, universe: Universe, column: Column) -> Universe: ...
    def filter_table(self, table: Table, path: ColumnPath) -> Table: ...
    def forget(
        self,
        table: Table,
        threshold_time_path: ColumnPath,
        current_time_path: ColumnPath,
    ) -> Table: ...
    def freeze(
        self,
        table: Table,
        threshold_time_path: ColumnPath,
        current_time_path: ColumnPath,
    ) -> Table: ...
    def buffer(
        self,
        table: Table,
        threshold_time_path: ColumnPath,
        current_time_path: ColumnPath,
    ) -> Table: ...
    def intersect_universe(
        self, universe: Universe, *universes: Universe
    ) -> Universe: ...
    def intersect_tables(self, table: Table, tables: Iterable[Table]) -> Table: ...
    def union_universe(self, universe: Universe, *universes: Universe) -> Universe: ...
    def venn_universes(
        self, left_universe: Universe, right_universe: Universe
    ) -> VennUniverses: ...
    def subtract_table(self, left_table: Table, right_table: Table) -> Table: ...
    def reindex_universe(self, column: Column) -> Universe: ...
    def restrict_column(
        self,
        universe: Universe,
        column: Column,
    ) -> Column: ...
    def restrict_table(
        self,
        orig_table: Table,
        new_table: Table,
    ) -> Table: ...
    def override_column_universe(
        self, universe: Universe, column: Column
    ) -> Column: ...
    def override_table_universe(
        self,
        orig_table: Table,
        new_table: Table,
    ) -> Table: ...
    def reindex_column(
        self,
        column_to_reindex: Column,
        reindexing_column: Column,
        reindexing_universe: Universe,
    ) -> Column: ...
    def reindex_table(
        self, table: Table, reindexing_column_path: ColumnPath
    ) -> Table: ...
    def connector_table(
        self,
        data_source: DataStorage,
        data_format: DataFormat,
        properties: ConnectorProperties,
    ) -> Table: ...
    @staticmethod
    def table(universe: Universe, columns: List[Column]) -> LegacyTable: ...

    # Grouping and joins

    def group_by(
        self, table: LegacyTable, requested_columns: List[Column], set_id: bool = False
    ) -> Grouper:
        """
        Args:
            table: a list of columns to group by.
        """
        ...
    def ix(
        self,
        keys_column: Column,
        input_universe: Universe,
        strict: bool,
        optional: bool,
    ) -> Ixer: ...
    def join_tables(
        self,
        left_storage: Table,
        right_storage: Table,
        left_paths: List[ColumnPath],
        right_paths: List[ColumnPath],
        assign_id: bool = False,
        left_ear: bool = False,
        right_ear: bool = False,
    ) -> Table: ...

    # Transformers

    def complex_columns(self, inputs: List[ComplexColumn]) -> List[Column]: ...

    # Updates

    def update_rows_table(self, table: Table, update: Table) -> Table: ...
    def update_cells_table(
        self,
        table: Table,
        update: Table,
        table_columns: List[ColumnPath],
        update_columns: List[ColumnPath],
    ) -> Table: ...
    def debug_universe(self, name: str, table: Table): ...
    def debug_column(self, name: str, table: Table, column_path: ColumnPath): ...
    def concat(self, universes: Iterable[Universe]) -> Concat: ...
    def concat_tables(self, tables: Iterable[Table]) -> Table: ...
    def flatten(self, flatten_column: Column) -> Flatten: ...
    def flatten_table(self, table: Table, path: ColumnPath) -> Table: ...
    def sort(
        self, key_column: Column, instance_column: Column
    ) -> Tuple[Column, Column]: ...
    def sort_table(
        self,
        table: Table,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
    ) -> Table: ...
    def probe_universe(self, universe: Universe, operator_id: int): ...
    def probe_column(self, column: Column, operator_id: int): ...
    def subscribe_table(
        self,
        table: Table,
        column_paths: Iterable[ColumnPath],
        on_change: Callable,
        on_end: Callable,
    ): ...
    def output_table(
        self,
        table: Table,
        column_paths: Iterable[ColumnPath],
        data_sink: DataStorage,
        data_format: DataFormat,
    ): ...

class Ixer:
    @property
    def universe(self) -> Universe: ...
    def ix_column(self, column: Column) -> Column: ...

class Grouper:
    @property
    def universe(self) -> Universe: ...
    def input_column(self, column: Column) -> Column: ...
    def count_column(self) -> Column: ...
    def reducer_column(self, reducer: Reducer, columns: List[Column]) -> Column: ...

class Concat:
    @property
    def universe(self) -> Universe: ...
    def concat_column(self, columns: List[Column]) -> Column: ...

class Flatten:
    @property
    def universe(self) -> Universe: ...
    def get_flattened_column(self) -> Column: ...
    def explode_column(self, column: Column) -> Column: ...

def run_with_new_graph(
    logic: Callable[[Scope], Iterable[tuple[Table, list[ColumnPath]]]],
    event_loop: asyncio.AbstractEventLoop,
    stats_monitor: Optional[StatsMonitor] = None,
    *,
    ignore_asserts: bool = False,
    monitoring_level: MonitoringLevel = MonitoringLevel.NONE,
    with_http_server: bool = False,
    persistence_config: Optional[PersistenceConfig] = None,
) -> List[CapturedTable]: ...
def unsafe_make_pointer(arg) -> BasePointer: ...

class DataFormat:
    value_fields: Any

    def __init__(self, *args, **kwargs): ...

class DataStorage:
    def __init__(self, *args, **kwargs): ...

class CsvParserSettings:
    def __init__(self, *args, **kwargs): ...

class AwsS3Settings:
    def __init__(self, *args, **kwargs): ...

class ValueField:
    def __init__(self, *args, **kwargs): ...
    def set_default(self, *args, **kwargs): ...

class PythonSubject:
    def __init__(self, *args, **kwargs): ...

class ElasticSearchAuth:
    def __init__(self, *args, **kwargs): ...

class ElasticSearchParams:
    def __init__(self, *args, **kwargs): ...

class PersistenceConfig:
    def __init__(self, *args, **kwargs): ...

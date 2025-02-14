# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable

from pathway.internals import api
from pathway.internals.expression import ColumnReference

if TYPE_CHECKING:
    from pathway.internals.table import Table


class DataSink(ABC):
    @property
    def name(self) -> str:
        return type(self).__qualname__.lower().removesuffix("datasink")

    def check_sort_by_columns(self, table: Table):
        sort_by = getattr(self, "sort_by", None)
        if sort_by is None:
            return
        for column in sort_by:
            if column._table != table:
                raise ValueError(
                    f"The column {column} doesn't belong to the target table {table}"
                )


@dataclass(frozen=True)
class GenericDataSink(DataSink):
    datastorage: api.DataStorage
    dataformat: api.DataFormat
    datasink_name: str
    unique_name: str | None
    sort_by: Iterable[ColumnReference] | None = None

    @property
    def name(self) -> str:
        return self.datasink_name

    @property
    def sort_by_indices(self):
        if self.sort_by is None:
            return None
        column_index: dict[str, int] = {}
        for index, column in enumerate(self.dataformat.value_fields):
            column_index[column.name] = index
        return [column_index[column.name] for column in self.sort_by]


@dataclass(frozen=True, kw_only=True)
class CallbackDataSink(DataSink):
    on_change: Callable[[api.Pointer, list[api.Value], int, int], None]
    on_time_end: Callable[[int], None]
    on_end: Callable[[], None]
    skip_persisted_batch: bool
    skip_errors: bool
    unique_name: str | None
    sort_by: Iterable[ColumnReference] | None = None

    def sort_by_indices(self, table: Table):
        if self.sort_by is None:
            return None
        column_index: dict[str, int] = {}
        for index, column in enumerate(table._columns):
            column_index[column] = index
        return [column_index[column.name] for column in self.sort_by]


@dataclass(frozen=True)
class ExportDataSink(DataSink):
    callback: Callable[[api.Scope, api.ExportedTable], None]

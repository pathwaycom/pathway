# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass

from pathway.internals import api


class DataSink(ABC):
    @property
    def name(self) -> str:
        return type(self).__qualname__.lower().removesuffix("datasink")


@dataclass(frozen=True)
class GenericDataSink(DataSink):
    datastorage: api.DataStorage
    dataformat: api.DataFormat
    datasink_name: str

    @property
    def name(self) -> str:
        return self.datasink_name


@dataclass(frozen=True, kw_only=True)
class CallbackDataSink(DataSink):
    on_change: Callable[[api.Pointer, list[api.Value], int, int], None]
    on_time_end: Callable[[int], None]
    on_end: Callable[[], None]
    skip_persisted_batch: bool
    skip_errors: bool


@dataclass(frozen=True)
class ExportDataSink(DataSink):
    callback: Callable[[api.Scope, api.ExportedTable], None]

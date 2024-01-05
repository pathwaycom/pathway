# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pathway.internals.api import Pointer


class DataSink(ABC):
    pass


@dataclass(frozen=True)
class GenericDataSink(DataSink):
    datastorage: Any  # api.DataStorage
    dataformat: Any  # api.DataFormat


@dataclass(frozen=True)
class CallbackDataSink(DataSink):
    on_change: Callable[[Pointer, list[Any], int, int], None]
    on_time_end: Callable[[int], None]
    on_end: Callable[[], None]
    skip_persisted_batch: bool

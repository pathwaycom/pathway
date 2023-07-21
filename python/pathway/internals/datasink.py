# Copyright © 2023 Pathway

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Callable, List


class DataSink(ABC):
    pass


@dataclass(frozen=True)
class GenericDataSink(DataSink):
    datastorage: Any  # api.DataStorage
    dataformat: Any  # api.DataFormat


@dataclass(frozen=True)
class CallbackDataSink(DataSink):
    on_change: Callable[[str, List[Any], int, int], Any]
    on_end: Callable[[], Any]

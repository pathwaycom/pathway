# Copyright © 2023 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Type

import pandas as pd

from pathway.internals import api
from pathway.internals.schema import Schema, schema_from_pandas, schema_from_types


class DataSource(ABC):
    @property
    @abstractmethod
    def schema(self) -> Type[Schema]:
        ...


class StaticDataSource(DataSource, ABC):
    data: Any


@dataclass(frozen=True)
class PandasDataSource(StaticDataSource):
    data: pd.DataFrame

    @property
    def schema(self) -> Type[Schema]:
        return schema_from_pandas(self.data)


@dataclass(frozen=True)
class GenericDataSource(DataSource):
    datastorage: api.DataStorage
    dataformat: api.DataFormat
    commit_frequency_ms: Any

    @property
    def schema(self) -> Type[Schema]:
        schema_types = {}
        for value_field in self.dataformat.value_fields:
            schema_types[value_field.name] = value_field.type_.get_python_schema_type()
        return schema_from_types(None, **schema_types)


@dataclass(frozen=True)
class EmptyDataSource(DataSource):
    _schema: Type[Schema]

    @property
    def schema(self) -> Type[Schema]:
        return self._schema


def debug_datasource(debug_data) -> Optional[StaticDataSource]:
    if debug_data is None:
        return None
    elif isinstance(debug_data, pd.DataFrame):
        return PandasDataSource(debug_data)
    else:
        raise TypeError("not supported type of debug data")

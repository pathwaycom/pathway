# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from pathway.internals import api
from pathway.internals.schema import Schema, schema_from_pandas


@dataclass(frozen=True)
class DataSourceOptions:
    commit_duration_ms: int | None = None
    unsafe_trusted_ids: bool | None = False
    unique_name: str | None = None


@dataclass(frozen=True, kw_only=True)
class DataSource(ABC):
    schema: type[Schema]
    data_source_options: DataSourceOptions = DataSourceOptions()

    @property
    def connector_properties(self) -> api.ConnectorProperties:
        columns: list[api.ColumnProperties] = []
        for column in self.schema.columns().values():
            columns.append(
                api.ColumnProperties(
                    dtype=column.dtype.to_engine(),
                    append_only=column.append_only,
                )
            )

        return api.ConnectorProperties(
            commit_duration_ms=self.data_source_options.commit_duration_ms,
            unsafe_trusted_ids=self.data_source_options.unsafe_trusted_ids,
            column_properties=columns,
            unique_name=self.data_source_options.unique_name,
        )

    def get_effective_schema(self) -> type[Schema]:
        if self.is_append_only():
            return self.schema.update_properties(append_only=True)
        return self.schema

    @abstractmethod
    def is_bounded(self) -> bool: ...

    @abstractmethod
    def is_append_only(self) -> bool: ...

    @property
    def name(self) -> str:
        return type(self).__qualname__.lower().removesuffix("datasource")


class StaticDataSource(DataSource, ABC):
    data: Any

    def is_bounded(self) -> bool:
        return True


@dataclass(frozen=True)
class PandasDataSource(StaticDataSource):
    data: pd.DataFrame

    def is_append_only(self) -> bool:
        return api.DIFF_PSEUDOCOLUMN not in self.data.columns or all(
            self.data[api.DIFF_PSEUDOCOLUMN] == 1
        )


@dataclass(frozen=True)
class GenericDataSource(DataSource):
    datastorage: api.DataStorage
    dataformat: api.DataFormat
    datasource_name: str
    append_only: bool = False

    def is_bounded(self) -> bool:
        return self.datastorage.mode == api.ConnectorMode.STATIC

    def is_append_only(self) -> bool:
        return self.append_only

    @property
    def name(self) -> str:
        return self.datasource_name


@dataclass(frozen=True)
class EmptyDataSource(DataSource):
    def is_bounded(self) -> bool:
        return True

    def is_append_only(self) -> bool:
        return True


@dataclass(frozen=True)
class ImportDataSource(DataSource):
    callback: Callable[[api.Scope], api.ExportedTable]

    def is_bounded(self) -> bool:
        return False

    def is_append_only(self) -> bool:
        return False


def debug_datasource(debug_data) -> StaticDataSource | None:
    if debug_data is None:
        return None
    elif isinstance(debug_data, pd.DataFrame):
        return PandasDataSource(
            data=debug_data.copy(), schema=schema_from_pandas(debug_data)
        )
    else:
        raise TypeError("not supported type of debug data")


@dataclass(frozen=True)
class ErrorLogDataSource(DataSource):
    def is_bounded(self) -> bool:
        return False

    def is_append_only(self) -> bool:
        return True

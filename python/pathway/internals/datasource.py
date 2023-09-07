# Copyright © 2023 Pathway

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Optional, Type

import pandas as pd

from pathway.internals import api
from pathway.internals.schema import Schema, schema_from_pandas


@dataclass(frozen=True)
class DataSource(ABC):
    schema: Type[Schema]


class StaticDataSource(DataSource, ABC):
    data: Any
    connector_properties: api.ConnectorProperties = api.ConnectorProperties()


@dataclass(frozen=True)
class PandasDataSource(StaticDataSource):
    data: pd.DataFrame
    connector_properties: api.ConnectorProperties = api.ConnectorProperties()


@dataclass(frozen=True)
class GenericDataSource(DataSource):
    datastorage: api.DataStorage
    dataformat: api.DataFormat
    connector_properties: api.ConnectorProperties = api.ConnectorProperties()


@dataclass(frozen=True)
class EmptyDataSource(DataSource):
    ...


def debug_datasource(debug_data) -> Optional[StaticDataSource]:
    if debug_data is None:
        return None
    elif isinstance(debug_data, pd.DataFrame):
        return PandasDataSource(
            data=debug_data.copy(), schema=schema_from_pandas(debug_data)
        )
    else:
        raise TypeError("not supported type of debug data")

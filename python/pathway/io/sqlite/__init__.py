# Copyright © 2024 Pathway

from __future__ import annotations

from os import PathLike, fspath
from typing import Any

from pathway.internals import api, datasource
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import read_schema


@check_arg_types
@trace_user_frame
def read(
    path: PathLike | str,
    table_name: str,
    schema: type[Schema],
    *,
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    debug_data: Any = None,
) -> Table:
    """Reads a table from a rowid table in `SQLite <https://www.sqlite.org/>`_ database.

    Args:
        path: Path to the database file.
        table_name: Name of the table in the database to be read.
        schema: Schema of the resulting table.
        autocommit_duration_ms: The maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.

    Returns:
        Table: The table read.
    """
    schema, api_schema = read_schema(schema)

    data_storage = api.DataStorage(
        storage_type="sqlite",
        path=fspath(path),
        table_name=table_name,
        mode=api.ConnectorMode.STREAMING,
    )
    data_format = api.DataFormat(
        format_type="transparent",
        **api_schema,
    )

    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=name,
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            schema=schema,
            data_source_options=data_source_options,
            datasource_name="sqlite",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )

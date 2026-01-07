# Copyright © 2026 Pathway

from __future__ import annotations

from typing import Any, Iterable, TypeVar, overload

from pathway.internals import (
    datasink as datasinks,
    datasource as datasources,
    operator as operators,
    parse_graph as parse_graphs,
    schema as schemas,
    table as tables,
)
from pathway.internals.schema import PAYLOAD_SOURCE_COMPONENT

TTable = TypeVar("TTable", bound=tables.Table)


def empty_from_schema(schema: type[schemas.Schema]) -> tables.Table:
    return table_from_datasource(datasources.EmptyDataSource(schema=schema))


@overload
def table_from_datasource(
    datasource: datasources.DataSource,
    debug_datasource: datasources.StaticDataSource | None = None,
    *,
    supported_components: Iterable[str] = ...,
) -> tables.Table[Any]: ...


@overload
def table_from_datasource(
    datasource: datasources.DataSource,
    debug_datasource: datasources.StaticDataSource | None = None,
    table_cls: type[TTable] = ...,
    *,
    supported_components: Iterable[str] = ...,
) -> TTable: ...


def table_from_datasource(
    datasource: datasources.DataSource,
    debug_datasource: datasources.StaticDataSource | None = None,
    table_cls: type[tables.Table] = tables.Table,
    *,
    supported_components: Iterable[str] = (PAYLOAD_SOURCE_COMPONENT,),
) -> tables.Table:
    for column_name, column_data in datasource.schema.columns().items():
        if column_data.source_component not in supported_components:
            error_message = (
                f"The field '{column_name}' in the schema of the source '{datasource.name}' "
                f"has unsupported source component: '{column_data.source_component}'"
            )
            raise ValueError(error_message)

    return parse_graphs.G.add_operator(
        lambda id: operators.InputOperator(datasource, id, debug_datasource),
        lambda operator: operator(table_cls),
    )


def table_to_datasink(
    table: tables.Table, datasink: datasinks.DataSink, *, special: bool = False
) -> operators.OutputOperator:
    datasink.check_sort_by_columns(table)
    return parse_graphs.G.add_operator(
        lambda id: operators.OutputOperator(datasink, id),
        lambda operator: operator(table),
        special=special,
    )

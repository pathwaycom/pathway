# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Any, TypeVar, overload

from pathway.internals import (
    datasink as datasinks,
    datasource as datasources,
    operator as operators,
    parse_graph as parse_graphs,
    schema as schemas,
    table as tables,
)

TTable = TypeVar("TTable", bound=tables.Table)


def empty_from_schema(schema: type[schemas.Schema]) -> tables.Table:
    return table_from_datasource(datasources.EmptyDataSource(schema=schema))


@overload
def table_from_datasource(
    datasource: datasources.DataSource,
    debug_datasource: datasources.StaticDataSource | None = None,
) -> tables.Table[Any]: ...


@overload
def table_from_datasource(
    datasource: datasources.DataSource,
    debug_datasource: datasources.StaticDataSource | None = None,
    table_cls: type[TTable] = ...,
) -> TTable: ...


def table_from_datasource(
    datasource: datasources.DataSource,
    debug_datasource: datasources.StaticDataSource | None = None,
    table_cls: type[tables.Table] = tables.Table,
) -> tables.Table:
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

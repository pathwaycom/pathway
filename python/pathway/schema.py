# Copyright © 2024 Pathway
from pathway.internals import (
    ColumnDefinition,
    Pointer,
    Schema,
    SchemaProperties,
    dtype as dt,
    schema_builder as _schema_builder,
)


def schema_builder(
    columns: dict[str, ColumnDefinition],
    *,
    name: str | None = None,
    properties: SchemaProperties = SchemaProperties(),
    id_type: type = Pointer,
) -> type[Schema]:
    """Allows to build schema inline, from a dictionary of column definitions.

    Args:
        columns: dictionary of column definitions.
        name: schema name.
        properties: schema properties.

    Returns:
        Schema

    Example:

    >>> import pathway as pw
    >>> pw.schema_builder(columns={
    ...   'key': pw.column_definition(dtype=int, primary_key=True),
    ...   'data': pw.column_definition(dtype=int, default_value=0)
    ... }, name="my_schema")
    <pathway.Schema types={'key': <class 'int'>, 'data': <class 'int'>}, id_type=pathway.engine.Pointer[int]>
    """
    return _schema_builder(
        columns, name=name, properties=properties, id_dtype=dt.wrap(id_type)
    )

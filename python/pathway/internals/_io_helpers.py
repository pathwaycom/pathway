# Copyright © 2023 Pathway

from __future__ import annotations

from typing import List, Type

from pathway.internals import api
from pathway.internals import dtype as dt
from pathway.internals import schema
from pathway.internals.table import Table


def _format_output_value_fields(table: Table) -> List[api.ValueField]:
    value_fields = []
    for column_name in table._columns.keys():
        value_fields.append(api.ValueField(column_name, api.PathwayType.ANY))

    return value_fields


def _form_value_fields(schema: Type[schema.Schema]) -> List[api.ValueField]:
    schema.default_values()
    default_values = schema.default_values()
    result = []

    # XXX fix mapping schema types to PathwayType
    types = {
        name: dt.unoptionalize(dtype).to_engine()
        for name, dtype in schema.as_dict().items()
    }

    for f in schema.column_names():
        simple_type = types.get(f, api.PathwayType.ANY)
        value_field = api.ValueField(f, simple_type)
        if f in default_values:
            value_field.set_default(default_values[f])
        result.append(value_field)

    return result

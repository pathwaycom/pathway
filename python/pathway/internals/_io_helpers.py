# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pathway.internals import api
from pathway.internals.helpers import StableSet
from pathway.internals.table import Table


def _format_output_value_fields(table: Table) -> List[api.ValueField]:
    value_fields = []
    for column_name in table._columns.keys():
        value_fields.append(api.ValueField(column_name, api.PathwayType.ANY))

    return value_fields


def _form_value_fields(
    id_fields: Optional[List[str]],
    value_fields: Optional[List[str]],
    schema_types: Optional[Dict[str, api.PathwayType]],
    default_values: Optional[Dict[str, Any]],
) -> List[api.ValueField]:
    all_field_names = StableSet((id_fields or []) + (value_fields or []))
    schema_types = schema_types or {}
    default_values = default_values or {}
    result = []

    for f in all_field_names:
        simple_type = schema_types.get(f, api.PathwayType.ANY)
        value_field = api.ValueField(f, simple_type)
        if f in default_values:
            value_field.set_default(default_values[f])
        result.append(value_field)

    return result

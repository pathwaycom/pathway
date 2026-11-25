# Copyright © 2026 Pathway

"""Nested JSON structure descriptions for input validation and typed field access."""

from __future__ import annotations

import json
import types
import typing
from typing import Any

from pathway.internals import api, dtype as dt
from pathway.internals.json import _JsonEncoder


class _NestedJsonEncoder(_JsonEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            # Use the connector's encoding for bytes, arrays, pointers and wrappers.
            return json.loads(api.value_to_json(obj))


def encode_default(value: Any) -> Any:
    return json.loads(json.dumps(value, cls=_NestedJsonEncoder))


def unoptionalize(schema: dict) -> dict:
    return schema["wrapped"] if schema["type"] == "optional" else schema


def for_dtype(schema: dict | None, dtype: dt.DType) -> dict | None:
    """Adjust a JSON structure description when a column becomes nullable."""
    if schema is None or dt.unoptionalize(dtype) != dt.JSON:
        return None
    schema = unoptionalize(schema)
    if isinstance(dtype, dt.Optional):
        return {"type": "optional", "wrapped": schema}
    return schema


def _contains_schema(annotation: Any) -> bool:
    from pathway.internals.schema import SchemaMetaclass

    # These parameters describe references or opaque Python objects, not JSON fields.
    if typing.get_origin(annotation) in (api.Pointer, api.PyObjectWrapper):
        return False
    return isinstance(annotation, SchemaMetaclass) or any(
        _contains_schema(arg) for arg in typing.get_args(annotation)
    )


def parse_annotation(annotation: Any) -> tuple[dt.DType, dict | None]:
    if not _contains_schema(annotation):
        return dt.wrap(annotation), None

    validation = _from_annotation(annotation)
    dtype = dt.Optional(dt.JSON) if validation["type"] == "optional" else dt.JSON
    return dtype, validation


def _from_annotation(annotation: Any) -> dict:
    from pathway.internals.schema import SchemaMetaclass

    if isinstance(annotation, SchemaMetaclass):
        if "__columns__" not in annotation.__dict__:
            raise TypeError("Recursive nested JSON schemas are not supported.")
        fields = []
        for name, column in annotation.columns().items():
            if column.primary_key or column.source_component != "payload":
                raise ValueError(
                    "Fields inside a nested JSON schema cannot specify "
                    "primary_key or source_component='key'."
                )
            field = {
                "name": name,
                "schema": column.json_schema or _from_dtype(column.dtype),
            }
            if column.has_default_value():
                # A wrapper distinguishes an explicit null default from no default.
                field["default"] = {"value": encode_default(column.default_value)}
            fields.append(field)
        return {"type": "object", "fields": fields}

    if not _contains_schema(annotation):
        return _from_dtype(dt.wrap(annotation))

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin in (typing.Union, types.UnionType) and len(args) == 2:
        non_null = [arg for arg in args if arg is not type(None)]
        if len(non_null) == 1:
            return {"type": "optional", "wrapped": _from_annotation(non_null[0])}
    if origin is list:
        return {"type": "list", "items": _from_annotation(args[0])}
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return {"type": "list", "items": _from_annotation(args[0])}
        return {"type": "tuple", "items": [_from_annotation(arg) for arg in args]}
    raise TypeError(
        "Nested JSON schemas can be used directly, in lists or tuples, "
        "or with Optional. Other containers and unions are not supported."
    )


def _from_dtype(dtype: dt.DType) -> dict:
    if isinstance(dtype, dt.Optional):
        return {"type": "optional", "wrapped": _from_dtype(dtype.wrapped)}
    if isinstance(dtype, dt.List):
        return {"type": "list", "items": _from_dtype(dtype.wrapped)}
    if isinstance(dtype, dt.Tuple):
        return {"type": "tuple", "items": [_from_dtype(arg) for arg in dtype.args]}
    if isinstance(dtype, dt.Array):
        if dtype.wrapped not in (dt.INT, dt.FLOAT):
            raise TypeError(
                "Arrays inside nested JSON schemas require int or float elements."
            )
        return {
            "type": "array",
            "n_dim": dtype.n_dim,
            "items": _from_dtype(dtype.wrapped),
        }
    if isinstance(dtype, dt.Pointer):
        return {"type": "pointer"}
    if isinstance(dtype, dt.PyObjectWrapper):
        return {"type": "py_object_wrapper"}
    name = {
        dt.BOOL: "bool",
        dt.INT: "int",
        dt.FLOAT: "float",
        dt.STR: "string",
        dt.BYTES: "bytes",
        dt.DATE_TIME_NAIVE: "date_time_naive",
        dt.DATE_TIME_UTC: "date_time_utc",
        dt.DURATION: "duration",
        dt.JSON: "json",
        dt.ANY: "json",
        dt.NONE: "null",
    }.get(dtype)
    if name is None:
        raise TypeError(
            f"Type {dtype} is not supported inside a nested JSON schema. "
            "Use types supported by the JSON input connector, lists, tuples, "
            "optional types, and nested schemas."
        )
    return {"type": name}


def leaf_dtype(schema: dict) -> dt.DType | None:
    if schema["type"] == "array":
        return dt.Array(schema["n_dim"], leaf_dtype(schema["items"]))
    return {
        "bool": dt.BOOL,
        "int": dt.INT,
        "float": dt.FLOAT,
        "string": dt.STR,
        "bytes": dt.BYTES,
        "date_time_naive": dt.DATE_TIME_NAIVE,
        "date_time_utc": dt.DATE_TIME_UTC,
        "duration": dt.DURATION,
        "pointer": dt.ANY_POINTER,
        "py_object_wrapper": dt.ANY_PY_OBJECT_WRAPPER,
    }.get(schema["type"])

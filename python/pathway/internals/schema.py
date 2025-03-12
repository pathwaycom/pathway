# Copyright © 2024 Pathway

from __future__ import annotations

import base64
import csv
import dataclasses
import itertools
from collections import ChainMap
from collections.abc import Callable, Iterable, KeysView, Mapping
from dataclasses import dataclass
from os import PathLike
from pydoc import locate
from types import MappingProxyType, UnionType
from typing import TYPE_CHECKING, Any, NoReturn, get_type_hints
from warnings import warn

import numpy as np
import pandas as pd

from pathway.internals import api, dtype as dt, trace
from pathway.internals.column_properties import ColumnProperties
from pathway.internals.helpers import StableSet
from pathway.internals.runtime_type_check import check_arg_types

if TYPE_CHECKING:
    import pathway.internals.expression as expr
    from pathway.internals import column as clmn


def _cls_fields(cls):
    return {k: v for k, v in cls.__dict__.items() if not k.startswith("__")}


def schema_from_columns(
    columns: Mapping[str, clmn.Column],
    id_column: clmn.IdColumn,
) -> type[Schema]:
    _name = "schema_from_columns(" + str(list(columns.keys())) + ")"

    return schema_builder(
        columns={
            name: ColumnDefinition.from_properties(c.properties)
            for name, c in columns.items()
        },
        name=_name,
        id_dtype=id_column.dtype,
    )


def _type_converter(series: pd.Series) -> dt.DType:
    if series.empty:
        return dt.ANY

    if series.apply(lambda x: isinstance(x, dict)).all():
        return dt.JSON
    if series.apply(lambda x: isinstance(x, (tuple, list))).all():
        proposed_len = len(series[0])
        if (series.apply(lambda x: len(x) == proposed_len)).all():
            dtypes = [
                _type_converter(series.apply(lambda x: x[i]))
                for i in range(proposed_len)
            ]
            return dt.Tuple(*dtypes)
        else:
            exploded = pd.Series([x for element in series for x in element])
            to_wrap = _type_converter(exploded)
            return dt.List(to_wrap)
    if (series.isna() | series.isnull()).all():
        return dt.NONE
    if (series.apply(lambda x: isinstance(x, np.ndarray))).all():
        if series.apply(lambda x: np.issubdtype(x.dtype, np.integer)).all():
            wrapped = dt.INT
        elif series.apply(lambda x: np.issubdtype(x.dtype, np.floating)).all():
            wrapped = dt.FLOAT
        else:
            wrapped = dt.ANY
        n_dim: int | None = len(series[0].shape)
        if not series.apply(lambda x: len(x.shape) == n_dim).all():
            n_dim = None

        return dt.Array(n_dim=n_dim, wrapped=wrapped)
    if pd.api.types.is_integer_dtype(series.dtype):
        ret_type: dt.DType = dt.INT
    elif pd.api.types.is_float_dtype(series.dtype):
        ret_type = dt.FLOAT
    elif pd.api.types.is_bool_dtype(series.dtype):
        ret_type = dt.BOOL
    elif pd.api.types.is_string_dtype(series.dtype):
        ret_type = dt.STR
    elif pd.api.types.is_datetime64_ns_dtype(series.dtype):
        if series.dt.tz is None:
            ret_type = dt.DATE_TIME_NAIVE
        else:
            ret_type = dt.DATE_TIME_UTC
    elif pd.api.types.is_timedelta64_dtype(series.dtype):
        ret_type = dt.DURATION
    elif pd.api.types.is_object_dtype(series.dtype):
        ret_type = dt.ANY
    else:
        ret_type = dt.ANY
    if series.isna().any() or series.isnull().any():
        return dt.Optional(ret_type)
    else:
        return ret_type


def schema_from_pandas(
    dframe: pd.DataFrame,
    *,
    id_from: list[str] | None = None,
    name: str | None = None,
    exclude_columns: set[str] = set(),
) -> type[Schema]:
    if name is None:
        name = "schema_from_pandas(" + str(dframe.columns) + ")"
    if id_from is None:
        id_from = []
    columns: dict[str, ColumnDefinition] = {
        name: column_definition(dtype=_type_converter(dframe[name]))
        for name in dframe.columns
        if name not in exclude_columns
    }
    for name in id_from:
        columns[name] = dataclasses.replace(columns[name], primary_key=True)
    return schema_builder(columns=columns, name=name)


@check_arg_types
def schema_from_types(
    _name: str | None = None,
    **kwargs,
) -> type[Schema]:
    """Constructs schema from kwargs: field=type.

    Example:

    >>> import pathway as pw
    >>> s = pw.schema_from_types(foo=int, bar=str)
    >>> s
    <pathway.Schema types={'foo': <class 'int'>, 'bar': <class 'str'>}, id_type=<class 'pathway.engine.Pointer'>>
    >>> issubclass(s, pw.Schema)
    True
    """
    id_dtype = kwargs.pop("id", dt.ANY_POINTER)
    if _name is None:
        _name = "schema(" + str(kwargs) + ")"
    __dict = {
        "__metaclass__": SchemaMetaclass,
        "__annotations__": kwargs,
    }
    return _schema_builder(_name, __dict, id_dtype=id_dtype)


def schema_add(*schemas: type[Schema]) -> type[Schema]:
    # TODO: we are dropping properties here
    annots_list = [get_type_hints(schema) for schema in schemas]
    annotations = dict(ChainMap(*annots_list))

    assert len(annotations) == sum([len(annots) for annots in annots_list])

    fields_list = [_cls_fields(schema) for schema in schemas]
    fields = dict(ChainMap(*fields_list))

    assert len(fields) == sum([len(f) for f in fields_list])

    # TODO: id_dtype should be an LCA od all id_types

    return _schema_builder(
        "_".join(schema.__name__ for schema in schemas),
        {
            "__metaclass__": SchemaMetaclass,
            "__annotations__": annotations,
            "__orig__": {f"__arg{i}__": arg for i, arg in enumerate(schemas)},
            **fields,
        },
    )


def _create_column_definitions(
    schema: SchemaMetaclass, schema_properties: SchemaProperties
) -> dict[str, ColumnSchema]:
    localns = locals()
    #  Update locals to handle recursive Schema definitions
    localns[schema.__name__] = schema
    annotations = get_type_hints(schema, localns=localns)
    fields = _cls_fields(schema)

    columns = {}

    for column_name, annotation in annotations.items():
        col_dtype = dt.wrap(annotation)
        column = fields.pop(column_name, column_definition(dtype=col_dtype))

        if not isinstance(column, ColumnDefinition):
            raise ValueError(
                f"`{column_name}` should be a column definition, found {type(column)}"
            )

        dtype = column.dtype
        if dtype is None:
            dtype = col_dtype

        if col_dtype != dtype:
            raise TypeError(
                f"type annotation of column `{column_name}` does not match column definition"
            )

        column_name = column.name or column_name

        def _get_column_property(property_name: str, default: Any) -> Any:
            match (
                getattr(column, property_name),
                getattr(schema_properties, property_name),
            ):
                case (None, None):
                    return default
                case (None, schema_property):
                    return schema_property
                case (column_property, None):
                    return column_property
                case (column_property, schema_property):
                    if column_property != schema_property:
                        raise ValueError(
                            f"ambiguous property; schema property `{property_name}` has"
                            + f" value {schema_property!r} but column"
                            + f" `{column_name}` got {column_property!r}"
                        )
                    return column_property

        columns[column_name] = ColumnSchema(
            primary_key=column.primary_key,
            default_value=column.default_value,
            dtype=dt.wrap(dtype),
            name=column_name,
            append_only=_get_column_property("append_only", False),
            description=column.description,
            example=column.example,
        )

    if fields:
        names = ", ".join(fields.keys())
        raise ValueError(f"definitions of columns {names} lack type annotation")

    return columns


def _universe_properties(
    columns: list[ColumnSchema],
    schema_properties: SchemaProperties,
    dtype: dt.DType,
    append_only: bool | None = None,
) -> ColumnProperties:
    if append_only is None:
        append_only = False
        if len(columns) > 0:
            append_only = any(c.append_only for c in columns)
        elif schema_properties.append_only is not None:
            append_only = schema_properties.append_only
    return ColumnProperties(dtype=dtype, append_only=append_only)


@dataclass(frozen=True)
class SchemaProperties:
    append_only: bool | None = None


class SchemaMetaclass(type):
    __columns__: dict[str, ColumnSchema]
    __dtypes__: dict[str, dt.DType]
    __types__: dict[str, Any]
    __universe_properties__: ColumnProperties

    @trace.trace_user_frame
    def __init__(
        self,
        *args,
        append_only: bool | None = None,
        id_dtype: dt.DType = dt.ANY_POINTER,
        id_append_only: bool | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        schema_properties = SchemaProperties(append_only=append_only)
        self.__columns__ = _create_column_definitions(self, schema_properties)
        pk_dtypes = [col.dtype for col in self.__columns__.values() if col.primary_key]
        if len(pk_dtypes) > 0:
            derived_type = dt.Pointer(*pk_dtypes)
            assert id_dtype in [derived_type, dt.ANY_POINTER]
            id_dtype = derived_type
        self.__universe_properties__ = _universe_properties(
            list(self.__columns__.values()),
            schema_properties,
            dtype=id_dtype,
            append_only=id_append_only,
        )
        self.__dtypes__ = {
            name: column.dtype for name, column in self.__columns__.items()
        }
        self.__types__ = {k: v.typehint for k, v in self.__dtypes__.items()}

    def __call__(self) -> NoReturn:
        raise TypeError(
            "Schemas should not be called. Use `table.schema` not `table.schema()."
        )

    def __or__(self, other: type[Schema]) -> type[Schema]:  # type: ignore
        return schema_add(self, other)  # type: ignore

    def columns(self) -> Mapping[str, ColumnSchema]:
        return MappingProxyType(self.__columns__)

    def column_names(self) -> list[str]:
        return list(self.keys())

    def columns_to_json_serializable_dict(self) -> dict:
        columns = {}
        for column_name, column_schema in self.columns().items():
            columns[column_name] = column_schema.to_json_serializable_dict()
        return columns

    def to_json_serializable_dict(self) -> dict:
        """
        Serialize schema in a JSON-serializable dict that can be further parsed back
        with `schema_from_dict`.
        """
        return {"columns": self.columns_to_json_serializable_dict()}

    @property
    def _id_dtype(self):
        return self.id.dtype

    @property
    def id_type(self):
        return self.id.dtype.typehint

    @property
    def id(self):
        return self.__universe_properties__

    @property
    def universe_properties(self) -> ColumnProperties:
        return self.__universe_properties__

    def column_properties(self, name: str) -> ColumnProperties:
        column = self.__columns__[name]
        return ColumnProperties(dtype=column.dtype, append_only=column.append_only)

    def primary_key_columns(self) -> list[str] | None:
        # There is a distinction between an empty set of columns denoting
        # the primary key and None. If any (including empty) set of keys if provided,
        # then it will be used to compute the primary key.
        #
        # For the autogeneration one needs to specify None

        pkey_fields = [
            name for name, column in self.__columns__.items() if column.primary_key
        ]
        return pkey_fields if pkey_fields else None

    def default_values(self) -> dict[str, Any]:
        return {
            name: column.default_value
            for name, column in self.__columns__.items()
            if column.has_default_value()
        }

    def keys(self) -> KeysView[str]:
        return self.__columns__.keys()

    def with_types(self, **kwargs) -> type[Schema]:
        columns: dict[str, ColumnDefinition] = {
            col.name: col.to_definition() for col in self.__columns__.values()
        }
        for name, dtype in kwargs.items():
            if name not in columns:
                raise ValueError(
                    f"Schema.with_types() argument name has to be an existing column name, received f{name}."
                )
            columns[name] = dataclasses.replace(columns[name], dtype=dt.wrap(dtype))

        return schema_builder(columns=columns, id_dtype=self.id.dtype)

    def without(self, *args: str | expr.ColumnReference) -> type[Schema]:
        columns: dict[str, ColumnDefinition] = {
            col.name: col.to_definition() for col in self.__columns__.values()
        }
        for arg in args:
            if isinstance(arg, str):
                name = arg
            else:
                name = arg._name
            try:
                columns.pop(name)
            except KeyError:
                raise ValueError(
                    f"Schema.without() argument {name!r} has to refer to an existing column."
                )
        return schema_builder(columns=columns, id_dtype=self.id.dtype)

    def with_id_type(self, type, *, append_only: bool | None = None):
        type = dt.wrap(type)
        assert isinstance(type, dt.Pointer)
        columns: dict[str, ColumnDefinition] = {
            col.name: col.to_definition() for col in self.__columns__.values()
        }
        return schema_builder(
            columns=columns, id_dtype=type, id_append_only=append_only
        )

    def update_properties(self, **kwargs) -> type[Schema]:
        columns: dict[str, ColumnDefinition] = {
            col.name: dataclasses.replace(col.to_definition(), **kwargs)
            for col in self.__columns__.values()
        }
        properties = SchemaProperties(
            **{
                name: value
                for name, value in kwargs.items()
                if name in SchemaProperties.__annotations__
            }
        )
        return schema_builder(
            columns=columns, name=self.__name__, properties=properties
        )

    def __getitem__(self, name) -> ColumnSchema:
        return self.__columns__[name]

    def _dtypes(self) -> Mapping[str, dt.DType]:
        return MappingProxyType(self.__dtypes__)

    def typehints(self) -> Mapping[str, Any]:
        return MappingProxyType(self.__types__)

    def __repr__(self):
        return f"<pathway.Schema types={self.__types__}, id_type={self.id_type}>"

    def __str__(self):
        col_names = [k for k in self.keys()]
        max_lens = [max(len("id"), len(str(self._id_dtype)))] + [
            max(len(column_name), len(str(self.__dtypes__[column_name])))
            for column_name in col_names
        ]
        res = " | ".join(
            ["id".ljust(max_lens[0])]
            + [
                column_name.ljust(max_len)
                for (column_name, max_len) in zip(col_names, max_lens[1:])
            ]
        )
        res = res.rstrip() + "\n"
        res = res + " | ".join(
            [str(self._id_dtype).ljust(max_lens[0])]
            + [
                (str(self.__dtypes__[column_name])).ljust(max_len)
                for (column_name, max_len) in zip(col_names, max_lens[1:])
            ]
        )
        return res

    def _as_tuple(self):
        return tuple(self.__columns__.items())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SchemaMetaclass):
            return self._as_tuple() == other._as_tuple()
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._as_tuple())

    def generate_class(
        self, class_name: str | None = None, generate_imports: bool = False
    ) -> str:
        """Generates class with the definition of given schema and returns it as a string.

        Args:
            class_name: name of the class with the schema. If not provided, name created
                during schema generation will be used.
            generate_imports: whether the string should start with necessary imports to
                run code (default is False)
        """

        def get_type_definition_and_modules(_type: object) -> tuple[str, list[str]]:
            modules = []
            if _type in {type(None), None}:
                type_repr = "None"
            elif not hasattr(_type, "__qualname__"):
                type_repr = repr(_type)
            elif _type.__module__ != "builtins":
                type_repr = f"{_type.__module__}.{_type.__qualname__}"
                modules = [_type.__module__]
            else:
                type_repr = _type.__qualname__

            if hasattr(_type, "__args__"):
                args = []
                for arg in _type.__args__:
                    arg_repr, arg_modules = get_type_definition_and_modules(arg)
                    args.append(arg_repr)
                    modules += arg_modules

                if isinstance(_type, UnionType):
                    return (" | ".join(args), modules)
                else:
                    return (
                        f"{type_repr}[{', '.join(args)}]",
                        modules,
                    )
            else:
                return type_repr, modules

        required_modules: StableSet[str] = StableSet()

        def render_column_definition(name: str, definition: ColumnSchema):
            properties = [
                f"{field.name}={repr(getattr(definition, field.name))}"
                for field in dataclasses.fields(definition)
                if field.name not in ("name", "dtype")
                and getattr(definition, field.name) != field.default
            ]

            type_definition, modules = get_type_definition_and_modules(
                self.__types__[name]
            )
            required_modules.update(modules)

            column_definition = f"    {name}: {type_definition}"
            if properties:
                column_definition += f" = pw.column_definition({','.join(properties)})"
            return column_definition

        if class_name is None:
            class_name = self.__name__

        if not class_name.isidentifier():
            warn(
                f'Name {class_name} is not a valid name for a class. Using "CustomSchema" instead'
            )
            class_name = "CustomSchema"

        class_definition = f"class {class_name}(pw.Schema):\n"

        class_definition += "\n".join(
            [
                render_column_definition(name, definition)
                for name, definition in self.__columns__.items()
            ]
        )

        if generate_imports:
            class_definition = (
                "\n".join([f"import {module}" for module in required_modules])
                + "\nimport pathway as pw\n\n"
                + class_definition
            )

        return class_definition

    def generate_class_to_file(
        self,
        path: str | PathLike,
        class_name: str | None = None,
        generate_imports: bool = False,
    ) -> None:
        """Generates class with the definition of given schema and saves it to a file.
        Used for persisting definition for schemas, which were automatically generated.

        Args:
            path: path of the file, in which the schema class definition will be saved.
            class_name: name of the class with the schema. If not provided, name created
                during schema generation will be used.
            generate_imports: whether the string should start with necessary imports to
                run code (default is False)
        """
        class_definition = self.generate_class(
            class_name, generate_imports=generate_imports
        )

        with open(path, mode="w") as f:
            f.write(class_definition)

    def assert_matches_schema(
        self,
        other: type[Schema],
        *,
        allow_superset: bool = True,
        ignore_primary_keys: bool = True,
        allow_subtype: bool = True,
    ) -> None:
        self_dict = self._dtypes()
        other_dict = other._dtypes()

        # Check if self has all columns of other
        if self_dict.keys() < other_dict.keys():
            missing_columns = other_dict.keys() - self_dict.keys()
            raise AssertionError(f"schema does not have columns {missing_columns}")

        # Check if types of columns are the same
        for col in other_dict:
            assert other_dict[col] == self_dict[col] or (
                allow_subtype and dt.dtype_issubclass(self_dict[col], other_dict[col])
            ), (
                f"type of column {col} does not match - its type is {self_dict[col]} in {self.__name__}",
                f" and {other_dict[col]} in {other.__name__}",
            )

        # When allow_superset=False, check that self does not have extra columns
        if not allow_superset and self_dict.keys() > other_dict.keys():
            extra_columns = self_dict.keys() - other_dict.keys()
            raise AssertionError(
                f"there are extra columns: {extra_columns} which are not present in the provided schema"
            )

        # Check whether primary keys are the same
        if not ignore_primary_keys:
            assert self.primary_key_columns() == other.primary_key_columns(), (
                f"primary keys in the schemas do not match - they are {self.primary_key_columns()} in {self.__name__}",
                f" and {other.primary_key_columns()} in {other.__name__}",
            )


def _schema_builder(
    _name: str,
    _dict: dict[str, Any],
    *,
    properties: SchemaProperties = SchemaProperties(),
    id_dtype: dt.DType = dt.ANY_POINTER,
    id_append_only: bool | None = None,
) -> type[Schema]:
    schema = SchemaMetaclass(
        _name,
        (Schema,),
        _dict,
        append_only=properties.append_only,
        id_dtype=id_dtype,
        id_append_only=id_append_only,
    )
    assert issubclass(schema, Schema)
    return schema


def is_subschema(left: type[Schema], right: type[Schema]):
    if left.keys() != right.keys():
        return False
    for k in left.keys():
        if not dt.dtype_issubclass(left.__dtypes__[k], right.__dtypes__[k]):
            return False
    # TODO something about id_dtype
    return True


class _Undefined:
    def __repr__(self):
        return "undefined"


_no_default_value_marker = _Undefined()


@dataclass(frozen=True)
class ColumnSchema:
    dtype: dt.DType
    name: str
    primary_key: bool = False
    default_value: Any = _no_default_value_marker
    append_only: bool = False
    description: str | None = None  # used in OpenAPI schema autogeneration
    example: Any = None  # used in OpenAPI schema autogeneration

    def has_default_value(self) -> bool:
        return not isinstance(self.default_value, _Undefined)

    def to_definition(self) -> ColumnDefinition:
        return ColumnDefinition(
            primary_key=self.primary_key,
            default_value=self.default_value,
            dtype=self.dtype,
            name=self.name,
            append_only=self.append_only,
            description=self.description,
            example=self.example,
        )

    def to_json_serializable_dict(self) -> dict:
        result = {
            "dtype": self.dtype.to_dict(),
            "name": self.name,
            "primary_key": self.primary_key,
            "append_only": self.append_only,
            "description": self.description,
        }
        if not isinstance(self.default_value, _Undefined):
            default_value_base64 = base64.b64encode(
                api.serialize(self.default_value)
            ).decode("UTF-8")
            result["_serialized_default_value"] = default_value_base64
        if self.example is not None:
            example_base64 = base64.b64encode(api.serialize(self.example)).decode(
                "UTF-8"
            )
            result["_serialized_example"] = example_base64
        return result

    @property
    def typehint(self):
        return self.dtype.typehint


@dataclass(frozen=True)
class ColumnDefinition:
    primary_key: bool = False
    default_value: Any | None = _no_default_value_marker
    dtype: dt.DType | None = dt.ANY
    name: str | None = None
    append_only: bool | None = None
    description: str | None = None  # used in OpenAPI schema autogeneration
    example: Any = None  # used in OpenAPI schema autogeneration

    def __post_init__(self):
        assert self.dtype is None or isinstance(self.dtype, dt.DType)

    @classmethod
    def from_properties(cls, properties: ColumnProperties) -> ColumnDefinition:
        return cls(dtype=properties.dtype, append_only=properties.append_only)


def column_definition(
    *,
    primary_key: bool = False,
    default_value: Any | None = _no_default_value_marker,
    dtype: Any | None = None,
    name: str | None = None,
    append_only: bool | None = None,
    description: str | None = None,
    example: Any = None,
    _serialized_default_value: Any | None = None,
    _serialized_example: Any | None = None,
) -> Any:  # Return any so that mypy does not complain
    """Creates column definition

    Args:
        primary_key: should column be a part of a primary key.
        default_value: default value replacing blank entries. The default value of the
            column must be specified explicitly,
            otherwise there will be no default value.
        dtype: data type. When used in schema class,
            will be deduced from the type annotation.
        name: name of a column. When used in schema class,
            will be deduced from the attribute name.
        append_only: whether column is append-only. if unspecified, defaults to False
            or to value specified at the schema definition level
        description: human-readable description for the column. Used by HTTP input
            connector in automated OpenAPI schema generation.
        example: example of the column value. Used by HTTP input connector in automated
            OpenAPI schema generation.

    Returns:
        Column definition.

    Example:

    >>> import pathway as pw
    >>> class NewSchema(pw.Schema):
    ...   key: int = pw.column_definition(primary_key=True)
    ...   timestamp: str = pw.column_definition(name="@timestamp")
    ...   data: str
    >>> NewSchema
    <pathway.Schema types={'key': <class 'int'>, '@timestamp': <class 'str'>, 'data': <class 'str'>}, \
id_type=pathway.engine.Pointer[int]>
    """

    if _serialized_default_value is not None:
        if not isinstance(default_value, _Undefined):
            raise ValueError(
                "Maximum one of {'default_value', '_serialized_default_value'} must be specified"
            )
        default_value = api.deserialize(base64.b64decode(_serialized_default_value))
    if _serialized_example is not None:
        if example is not None:
            raise ValueError(
                "Maximum one of {'example', '_serialized_example'} must be specified"
            )
        example = api.deserialize(base64.b64decode(_serialized_example))

    return ColumnDefinition(
        dtype=dt.wrap(dtype) if dtype is not None else None,
        primary_key=primary_key,
        default_value=default_value,
        name=name,
        append_only=append_only,
        description=description,
        example=example,
    )


def schema_builder(
    columns: dict[str, ColumnDefinition],
    *,
    name: str | None = None,
    properties: SchemaProperties = SchemaProperties(),
    id_dtype: dt.DType = dt.ANY_POINTER,
    id_append_only: bool | None = None,
) -> type[Schema]:
    if name is None:
        name = "custom_schema(" + str(list(columns.keys())) + ")"

    __annotations = {name: c.dtype or Any for name, c in columns.items()}

    __dict: dict[str, Any] = {
        "__metaclass__": SchemaMetaclass,
        "__annotations__": __annotations,
        **columns,
    }

    return _schema_builder(
        name,
        __dict,
        properties=properties,
        id_dtype=id_dtype,
        id_append_only=id_append_only,
    )


def schema_from_dict(
    columns: dict,
    *,
    name: str | None = None,
    properties: dict | SchemaProperties = SchemaProperties(),
) -> type[Schema]:
    """Allows to build schema inline, from a dictionary of column definitions.
    Compared to pw.schema_builder, this one uses simpler structure of the dictionary,
    which allows it to be loaded from JSON file.

    Args:
        columns: dictionary of column definitions. The keys in this dictionary are names
            of the columns, and the values are either:
            - type of the column
            - dictionary with keys: "dtype", "primary_key", "default_value" and values,
            respectively, type of the column, whether it is a primary key, and column's
            default value.
            The type can be given both by python class, or string with class name - that
            is both int and "int" are accepted.
        name: schema name.
        properties: schema properties, given either as instance of SchemaProperties class
            or a dict specifying arguments of SchemaProperties class.

    Returns:
        Schema

    Example:

    >>> import pathway as pw
    >>> pw.schema_from_dict(columns={
    ...   'key': {"dtype": "int", "primary_key": True},
    ...   'data': {"dtype": "int", "default_value": 0}
    ... }, name="my_schema")
    <pathway.Schema types={'key': <class 'int'>, 'data': <class 'int'>}, id_type=pathway.engine.Pointer[int]>
    """

    def get_dtype(dtype) -> dt.DType:
        if isinstance(dtype, str):
            dtype = locate(dtype)
        if isinstance(dtype, dict):
            dtype = dt.parse_dtype_from_dict(dtype)
        return dt.wrap(dtype)

    def create_column_definition(entry):
        if not isinstance(entry, dict):
            entry = {"dtype": entry}
        entry["dtype"] = get_dtype(entry.get("dtype", Any))

        return column_definition(**entry)

    column_definitions = {
        column_name: create_column_definition(value)
        for (column_name, value) in columns.items()
    }

    if isinstance(properties, dict):
        properties = SchemaProperties(**properties)

    return schema_builder(column_definitions, name=name, properties=properties)


def _is_parsable_to(s: str, parse_fun: Callable):
    try:
        parse_fun(s)
        return True
    except ValueError:
        return False


def schema_from_csv(
    path: str,
    *,
    name: str | None = None,
    properties: SchemaProperties = SchemaProperties(),
    delimiter: str = ",",
    quote: str = '"',
    comment_character: str | None = None,
    escape: str | None = None,
    double_quote_escapes: bool = True,
    num_parsed_rows: int | None = None,
):
    """Allows to generate schema based on a CSV file.
    The names of the columns are taken from the header of the CSV file.
    Types of columns are inferred from the values, by checking if they can be parsed.
    Currently supported types are str, int and float.

    Args:
        path: path to the CSV file.
        name: schema name.
        properties: schema properties.
        delimiter: delimiter used in CSV file. Defaults to ",".
        quote: quote character used in CSV file. Defaults to '"'.
        comment_character: character used in CSV file to denote comments.
          Defaults to None
        escape: escape character used in CSV file. Defaults to None.
        double_quote_escapes: enable escapes of double quotes. Defaults to True.
        num_parsed_rows: number of rows, which will be parsed when inferring types. When
            set to None, all rows will be parsed. When set to 0, types of all columns
            will be set to str. Defaults to None.

    Returns:
        Schema
    """

    def remove_comments_from_file(f: Iterable[str], comment_char: str | None):
        for line in f:
            if line.lstrip()[0] != comment_char:
                yield line

    with open(path) as f:
        csv_reader = csv.DictReader(
            remove_comments_from_file(f, comment_character),
            delimiter=delimiter,
            escapechar=escape,
            quoting=csv.QUOTE_ALL,
            quotechar=quote,
            doublequote=double_quote_escapes,
        )
        if csv_reader.fieldnames is None:
            raise ValueError("can't generate Schema based on an empty CSV file")
        column_names = csv_reader.fieldnames
        if num_parsed_rows is None:
            csv_data = list(csv_reader)
        else:
            csv_data = list(itertools.islice(csv_reader, num_parsed_rows))

    def choose_type(entries: list[str]):
        if len(entries) == 0:
            return Any
        if all(_is_parsable_to(s, int) for s in entries):
            return int
        if all(_is_parsable_to(s, float) for s in entries):
            return float
        return str

    column_types = {
        column_name: choose_type([row[column_name] for row in csv_data])
        for column_name in column_names
    }

    columns = {
        column_name: column_definition(dtype=column_types[column_name])
        for column_name in column_names
    }

    return schema_builder(columns, name=name, properties=properties)


# do not move this class to any other place than the end of this file
# or otherwise bad things happen (circular imports)
class Schema(metaclass=SchemaMetaclass):
    """Base class to inherit from when creating schemas.
    All schemas should be subclasses of this one.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ...    age  owner  pet
    ... 1   10  Alice  dog
    ... 2    9    Bob  dog
    ... 3    8  Alice  cat
    ... 4    7    Bob  dog''')
    >>> t1.schema
    <pathway.Schema types={'age': <class 'int'>, 'owner': <class 'str'>, 'pet': <class 'str'>}, \
id_type=<class 'pathway.engine.Pointer'>>
    >>> issubclass(t1.schema, pw.Schema)
    True
    >>> class NewSchema(pw.Schema):
    ...   foo: int
    >>> SchemaSum = NewSchema | t1.schema
    >>> SchemaSum
    <pathway.Schema types={'age': <class 'int'>, 'owner': <class 'str'>, 'pet': <class 'str'>, \
'foo': <class 'int'>}, id_type=<class 'pathway.engine.Pointer'>>
    """

    def __init_subclass__(
        cls,
        /,
        append_only: bool | None = None,
        id_dtype: dt.DType = dt.ANY_POINTER,  # FIXME, pw.Pointer
        id_append_only: bool | None = None,
        **kwargs,
    ) -> None:
        super().__init_subclass__(**kwargs)

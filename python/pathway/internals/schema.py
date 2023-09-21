# Copyright © 2023 Pathway

from __future__ import annotations

import dataclasses
from collections import ChainMap
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    KeysView,
    List,
    Mapping,
    Optional,
    Type,
    ValuesView,
    cast,
    get_type_hints,
)

import numpy as np
import pandas as pd

from pathway.internals import dtype as dt
from pathway.internals import trace
from pathway.internals.column_properties import ColumnProperties
from pathway.internals.runtime_type_check import runtime_type_check

if TYPE_CHECKING:
    from pathway.internals import column as clmn


def _cls_fields(cls):
    return {k: v for k, v in cls.__dict__.items() if not k.startswith("__")}


def schema_from_columns(
    columns: Mapping[str, clmn.Column],
    _name: Optional[str] = None,
) -> Type[Schema]:
    if _name is None:
        _name = "schema_from_columns(" + str(list(columns.keys())) + ")"
    __dict = {
        "__metaclass__": SchemaMetaclass,
        "__annotations__": {name: c.dtype for name, c in columns.items()},
    }
    return _schema_builder(_name, __dict)


def _type_converter(series) -> dt.DType:
    if series.apply(lambda x: isinstance(x, (tuple, list))).all():
        return dt.ANY_TUPLE
    if (series.isna() | series.isnull()).all():
        return dt.NONE
    if (series.apply(lambda x: isinstance(x, np.ndarray))).all():
        return dt.Array()
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
        ret_type = dt.ANY  # type: ignore
    else:
        ret_type = dt.ANY  # type: ignore
    if series.isna().any() or series.isnull().any():
        return dt.Optional(ret_type)
    else:
        return ret_type


def schema_from_pandas(
    dframe: pd.DataFrame,
    *,
    id_from: Optional[List[str]] = None,
    name: Optional[str] = None,
) -> Type[Schema]:
    if name is None:
        name = "schema_from_pandas(" + str(dframe.columns) + ")"
    if id_from is None:
        id_from = []
    columns: Dict[str, ColumnDefinition] = {
        name: column_definition(dtype=_type_converter(dframe[name]))
        for name in dframe.columns
    }
    for name in id_from:
        columns[name] = dataclasses.replace(columns[name], primary_key=True)

    return schema_builder(
        columns=columns, properties=SchemaProperties(append_only=True), name=name
    )


@runtime_type_check
def schema_from_types(
    _name: Optional[str] = None,
    **kwargs,
) -> Type[Schema]:
    """Constructs schema from kwargs: field=type.

    Example:

    >>> import pathway as pw
    >>> s = pw.schema_from_types(foo=int, bar=str)
    >>> s.as_dict()
    {'foo': INT, 'bar': STR}
    >>> issubclass(s, pw.Schema)
    True
    """
    if _name is None:
        _name = "schema(" + str(kwargs) + ")"
    __dict = {
        "__metaclass__": SchemaMetaclass,
        "__annotations__": kwargs,
    }
    return _schema_builder(_name, __dict)


def schema_add(*schemas: Type[Schema]) -> Type[Schema]:
    annots_list = [get_type_hints(schema) for schema in schemas]
    annotations = dict(ChainMap(*annots_list))

    assert len(annotations) == sum([len(annots) for annots in annots_list])

    fields_list = [_cls_fields(schema) for schema in schemas]
    fields = dict(ChainMap(*fields_list))

    assert len(fields) == sum([len(f) for f in fields_list])

    return _schema_builder(
        "_".join(schema.__name__ for schema in schemas),
        {
            "__metaclass__": SchemaMetaclass,
            "__annotations__": annotations,
            "__orig__": {f"__arg{i}__": arg for i, arg in enumerate(schemas)},
            **fields,
        },
    )


def _create_column_definitions(schema: SchemaMetaclass):
    localns = locals()
    #  Update locals to handle recursive Schema definitions
    localns[schema.__name__] = schema
    annotations = get_type_hints(schema, localns=localns)
    fields = _cls_fields(schema)

    columns = {}

    for name, annotation in annotations.items():
        col_dtype = dt.wrap(annotation)
        column = fields.pop(name, column_definition(dtype=col_dtype))

        if not isinstance(column, ColumnDefinition):
            raise ValueError(
                f"`{name}` should be a column definition, found {type(column)}"
            )

        dtype = column.dtype
        if dtype is None:
            dtype = col_dtype

        if col_dtype != dtype:
            raise TypeError(
                f"type annotation of column `{name}` does not match column definition"
            )

        name = column.name or name

        columns[name] = ColumnSchema(
            primary_key=column.primary_key,
            default_value=column.default_value,
            dtype=dt.wrap(dtype),
            name=name,
            append_only=schema.__properties__.append_only,
        )

    if fields:
        names = ", ".join(fields.keys())
        raise ValueError(f"definitions of columns {names} lack type annotation")

    return columns


@dataclass(frozen=True)
class SchemaProperties:
    append_only: bool = False


class SchemaMetaclass(type):
    __columns__: Dict[str, ColumnSchema]
    __properties__: SchemaProperties
    __types__: Dict[str, dt.DType]

    @trace.trace_user_frame
    def __init__(self, *args, append_only=False, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.__properties__ = SchemaProperties(append_only=append_only)
        self.__columns__ = _create_column_definitions(self)
        self.__types__ = {
            name: cast(dt.DType, column.dtype)
            for name, column in self.__columns__.items()
        }

    def __or__(self, other: Type[Schema]) -> Type[Schema]:  # type: ignore
        return schema_add(self, other)  # type: ignore

    def properties(self) -> SchemaProperties:
        return self.__properties__

    def columns(self) -> Dict[str, ColumnSchema]:
        return dict(self.__columns__)

    def column_names(self) -> list[str]:
        return list(self.keys())

    def column_properties(self, name: str) -> ColumnProperties:
        column = self.__columns__[name]
        return ColumnProperties(dtype=column.dtype, append_only=column.append_only)

    def primary_key_columns(self) -> Optional[list[str]]:
        # There is a distinction between an empty set of columns denoting
        # the primary key and None. If any (including empty) set of keys if provided,
        # then it will be used to compute the primary key.
        #
        # For the autogeneration one needs to specify None

        pkey_fields = [
            name for name, column in self.__columns__.items() if column.primary_key
        ]
        return pkey_fields if pkey_fields else None

    def default_values(self) -> Dict[str, Any]:
        return {
            name: column.default_value
            for name, column in self.__columns__.items()
            if column.has_default_value()
        }

    def types(self) -> list[Any]:
        return list(self.__types__.values())

    def keys(self) -> KeysView[str]:
        return self.__columns__.keys()

    def values(self) -> ValuesView[Any]:
        return self.__types__.values()

    def update_types(self, **kwargs) -> Type[Schema]:
        columns: Dict[str, ColumnDefinition] = {
            col.name: col.to_definition() for col in self.__columns__.values()
        }
        for name, dtype in kwargs.items():
            if name not in columns:
                raise ValueError(
                    "Schema.update_types() argument name has to be an existing column name."
                )
            columns[name] = dataclasses.replace(columns[name], dtype=dt.wrap(dtype))

        return schema_builder(columns=columns, properties=self.__properties__)

    def __getitem__(self, name):
        return self.__types__[name]

    def as_dict(self):
        return self.__types__.copy()

    def __repr__(self):
        return self.__name__ + str(self.__types__)

    def __str__(self):
        col_names = [k for k in self.keys()]
        max_lens = [
            max(len(column_name), len(str(self[column_name])))
            for column_name in col_names
        ]
        res = " | ".join(
            [
                column_name.ljust(max_len)
                for (column_name, max_len) in zip(col_names, max_lens)
            ]
        )
        res = res.rstrip() + "\n"
        res = res + " | ".join(
            [
                (str(self[column_name])).ljust(max_len)
                for (column_name, max_len) in zip(col_names, max_lens)
            ]
        )
        return res

    def _as_tuple(self):
        return (self.__properties__, tuple(self.__columns__.items()))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SchemaMetaclass):
            return self._as_tuple() == other._as_tuple()
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._as_tuple())

    def assert_equal_to(
        self,
        other: Type[Schema],
        *,
        allow_superset: bool = False,
        ignore_primary_keys: bool = True,
    ) -> None:
        self_dict = self.as_dict()
        other_dict = other.as_dict()

        # Check if self has all columns of other
        if self_dict.keys() < other_dict.keys():
            missing_columns = other_dict.keys() - self_dict.keys()
            raise AssertionError(f"schema does not have columns {missing_columns}")

        # Check if types of columns are the same
        for col in other_dict:
            assert other_dict[col] == self_dict[col], (
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


class Schema(metaclass=SchemaMetaclass):
    """Base class to inherit from when creating schemas.
    All schemas should be subclasses of this one.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
    ...    age  owner  pet
    ... 1   10  Alice  dog
    ... 2    9    Bob  dog
    ... 3    8  Alice  cat
    ... 4    7    Bob  dog''')
    >>> t1.schema.as_dict()
    {'age': INT, 'owner': STR, 'pet': STR}
    >>> issubclass(t1.schema, pw.Schema)
    True
    >>> class NewSchema(pw.Schema):
    ...   foo: int
    >>> SchemaSum = NewSchema | t1.schema
    >>> SchemaSum.as_dict()
    {'age': INT, 'owner': STR, 'pet': STR, 'foo': INT}
    """

    def __init_subclass__(cls, /, append_only: bool = False, **kwargs) -> None:
        super().__init_subclass__(**kwargs)


def _schema_builder(
    _name: str,
    _dict: dict[str, Any],
    *,
    properties: SchemaProperties = SchemaProperties(),
) -> Type[Schema]:
    return SchemaMetaclass(_name, (Schema,), _dict, append_only=properties.append_only)  # type: ignore


def is_subschema(left: Type[Schema], right: Type[Schema]):
    if left.keys() != right.keys():
        return False
    for k in left.keys():
        if not dt.dtype_issubclass(left[k], right[k]):
            return False
    return True


class _Undefined:
    def __repr__(self):
        return "undefined"


_no_default_value_marker = _Undefined()


@dataclass(frozen=True)
class ColumnSchema:
    primary_key: bool
    default_value: Optional[Any]
    dtype: dt.DType
    name: str
    append_only: bool

    def has_default_value(self) -> bool:
        return self.default_value != _no_default_value_marker

    def to_definition(self) -> ColumnDefinition:
        return ColumnDefinition(
            primary_key=self.primary_key,
            default_value=self.default_value,
            dtype=self.dtype,
            name=self.name,
        )


@dataclass(frozen=True)
class ColumnDefinition:
    primary_key: bool = False
    default_value: Optional[Any] = _no_default_value_marker
    dtype: Optional[dt.DType] = dt.ANY
    name: Optional[str] = None

    def __post_init__(self):
        assert self.dtype is None or isinstance(self.dtype, dt.DType)


def column_definition(
    *,
    primary_key: bool = False,
    default_value: Optional[Any] = _no_default_value_marker,
    dtype: Optional[Any] = None,
    name: Optional[str] = None,
) -> Any:  # Return any so that mypy does not complain
    """Creates column definition

    Args:
        primary_key: should column be a part of a primary key.
        default_value: default valuee replacing blank entries. The default value of the
            column must be specified explicitly,
            otherwise there will be no default value.
        dtype: data type. When used in schema class,
            will be deduced from the type annotation.
        name: name of a column. When used in schema class,
            will be deduced from the attribute name.

    Returns:
        Column definition.

    Example:

    >>> import pathway as pw
    >>> class NewSchema(pw.Schema):
    ...   key: int = pw.column_definition(primary_key=True)
    ...   timestamp: str = pw.column_definition(name="@timestamp")
    ...   data: str
    >>> NewSchema.as_dict()
    {'key': INT, '@timestamp': STR, 'data': STR}
    """
    from pathway.internals import dtype as dt

    return ColumnDefinition(
        dtype=dt.wrap(dtype) if dtype is not None else None,
        primary_key=primary_key,
        default_value=default_value,
        name=name,
    )


def schema_builder(
    columns: Dict[str, ColumnDefinition],
    *,
    name: Optional[str] = None,
    properties: SchemaProperties = SchemaProperties(),
) -> Type[Schema]:
    """Allows to build schema inline, from a dictionary of column definitions.

    Args:
        columns: dictionary of column definitions.
        name: schema name.
        properties: schema properties.

    Returns:
        Schema

    Example:

    >>> import pathway as pw
    >>> schema = pw.schema_builder(columns={
    ...   'key': pw.column_definition(dtype=int, primary_key=True),
    ...   'data': pw.column_definition(dtype=int, default_value=0)
    ... }, name="my_schema")
    >>> schema.as_dict()
    {'key': INT, 'data': INT}
    """

    if name is None:
        name = "custom_schema(" + str(list(columns.keys())) + ")"

    __annotations = {name: c.dtype or Any for name, c in columns.items()}

    __dict: Dict[str, Any] = {
        "__metaclass__": SchemaMetaclass,
        "__annotations__": __annotations,
        **columns,
    }

    return _schema_builder(name, __dict, properties=properties)

# Copyright © 2024 Pathway

from __future__ import annotations

import datetime
import warnings
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeAlias, TypeVar, Union

import numpy as np
import pandas as pd

from pathway.engine import *
from pathway.internals import dtype as dt, json
from pathway.internals.schema import Schema

if TYPE_CHECKING:
    _Value: TypeAlias = "Value"
else:
    # Beartype has problems with the recursive definition
    _Value: TypeAlias = Any

Value: TypeAlias = Union[
    None,
    int,
    float,
    str,
    bytes,
    bool,
    Pointer,
    datetime.datetime,
    datetime.timedelta,
    np.ndarray,
    json.Json,
    dict[str, _Value],
    tuple[_Value, ...],
    Error,
    Pending,
]
CapturedTable = dict[Pointer, tuple[Value, ...]]
CapturedStream = list[DataRow]

S = TypeVar("S", bound=Value)


class CombineMany(Protocol, Generic[S]):
    def __call__(
        self, state: S | None, rows: list[tuple[list[Value], int]], /
    ) -> S | None: ...


def denumpify(x, type_from_schema: dt.DType | None = None):
    def denumpify_inner(x):
        if pd.api.types.is_scalar(x) and pd.isna(x):
            return None
        if isinstance(x, np.generic):
            return x.item()
        return x

    def _is_instance_of_simple_type(x):
        return (
            dt.INT.is_value_compatible(x)
            or dt.BOOL.is_value_compatible(x)
            or dt.STR.is_value_compatible(x)
            or dt.BYTES.is_value_compatible(x)
            or dt.FLOAT.is_value_compatible(x)
        )

    def fix_possibly_misassigned_type(entry, type_from_schema):
        assert (
            (type_from_schema.is_value_compatible(entry))
            # the only exception for str should be conversion to bytes; however,
            # some places use schema_from_pandas, which considers some complex types
            # as str, which means we enter here, as it looks like simple type STR even
            # though it's not, below the exception that should be here
            # or (isinstance(v, str) and type_from_schema.wrapped == bytes)
            or type_from_schema.wrapped == str
        )

        if type_from_schema == dt.STR and _is_instance_of_simple_type(entry):
            return str(entry)

        if type_from_schema == dt.FLOAT:
            return float(entry)

        if isinstance(entry, str) and type_from_schema == dt.BYTES:
            return entry.encode("utf-8")

        return entry

    v = denumpify_inner(x)

    if isinstance(type_from_schema, dt._SimpleDType):
        v = fix_possibly_misassigned_type(v, type_from_schema)
    elif (
        isinstance(type_from_schema, dt.Optional)
        and isinstance(type_from_schema.wrapped, dt._SimpleDType)
        and not dt.NONE.is_value_compatible(v)
    ):
        # pandas stores optional ints as floats
        if isinstance(v, float) and type_from_schema.wrapped == dt.INT:
            assert v.is_integer()
            v = fix_possibly_misassigned_type(int(v), type_from_schema.wrapped)
        else:
            v = fix_possibly_misassigned_type(v, type_from_schema.wrapped)

    if isinstance(v, str):
        return v.encode("utf-8", "ignore").decode("utf-8")
    else:
        return v


def ids_from_pandas(
    df: pd.DataFrame,
    connector_properties: ConnectorProperties | None,
    id_from: list[str] | None,
) -> dict[Any, Pointer]:
    if id_from is None:
        if connector_properties is not None and connector_properties.unsafe_trusted_ids:
            return {k: unsafe_make_pointer(k) for k in df.index}
        else:
            return {k: ref_scalar(k) for k in df.index}
    else:
        return {k: ref_scalar(*args) for (k, *args) in df[id_from].itertuples()}


TIME_PSEUDOCOLUMN = "__time__"
DIFF_PSEUDOCOLUMN = "__diff__"
SHARD_PSEUDOCOLUMN = "__shard__"
PANDAS_PSEUDOCOLUMNS = {TIME_PSEUDOCOLUMN, DIFF_PSEUDOCOLUMN, SHARD_PSEUDOCOLUMN}


def static_table_from_pandas(
    scope,
    df: pd.DataFrame,
    connector_properties: ConnectorProperties | None = None,
    id_from: list[str] | None = None,
    schema: type[Schema] | None = None,
) -> Table:
    if schema is not None and id_from is not None:
        assert schema.primary_key_columns() == id_from

    if id_from is None and schema is not None:
        id_from = schema.primary_key_columns()

    ids = ids_from_pandas(df, connector_properties, id_from)
    column_types: dict[str, dt.DType] | None = None
    if schema is not None:
        column_types = dict(schema.__dtypes__)
        for column in PANDAS_PSEUDOCOLUMNS:
            column_types[column] = dt.INT

    data = {}
    for c in df.columns:
        type_from_schema = None if column_types is None else column_types[c]
        data[c] = [denumpify(v, type_from_schema) for _, v in df[c].items()]
        # df[c].items() is used because df[c].values is a numpy array
    ordinary_columns = [
        column for column in df.columns if column not in PANDAS_PSEUDOCOLUMNS
    ]
    if column_types:
        dtypes = [column_types[c].to_engine() for c in ordinary_columns]
    else:
        dtypes = [PathwayType.ANY] * len(ordinary_columns)

    if connector_properties is None:
        column_properties = []
        for c in ordinary_columns:
            dtype: type = int
            for v in data[c]:
                if v is not None:
                    dtype = type(v)
                    break
            column_properties.append(ColumnProperties(dtype=dt.wrap(dtype).to_engine()))
        connector_properties = ConnectorProperties(column_properties=column_properties)

    assert len(connector_properties.column_properties) == len(
        ordinary_columns
    ), "provided connector properties do not match the dataframe"

    input_data: CapturedStream = []
    for i, index in enumerate(df.index):
        key = ids[index]
        values = [data[c][i] for c in ordinary_columns]
        time = data[TIME_PSEUDOCOLUMN][i] if TIME_PSEUDOCOLUMN in data else 0
        diff = data[DIFF_PSEUDOCOLUMN][i] if DIFF_PSEUDOCOLUMN in data else 1
        if diff not in [-1, 1]:
            raise ValueError(f"Column {DIFF_PSEUDOCOLUMN} can only contain 1 and -1.")
        shard = data[SHARD_PSEUDOCOLUMN][i] if SHARD_PSEUDOCOLUMN in data else None
        input_row = DataRow(
            key, values, time=time, diff=diff, shard=shard, dtypes=dtypes
        )
        input_data.append(input_row)

    return scope.static_table(input_data, connector_properties)


def squash_updates(
    updates: CapturedStream, *, terminate_on_error: bool = True
) -> CapturedTable:
    state: CapturedTable = {}
    updates.sort(key=lambda row: (row.time, row.diff))

    def handle_error(row: DataRow, msg: str):
        if terminate_on_error:
            raise KeyError(msg)
        else:
            warnings.warn(msg)
            t: tuple[Value, ...] = (ERROR,) * len(row.values)
            state[row.key] = t

    for row in updates:
        if row.diff == 1:
            if row.key in state:
                handle_error(row, f"duplicated entries for key {row.key}")
                continue
            state[row.key] = tuple(row.values)
        elif row.diff == -1:
            if state[row.key] != tuple(row.values):
                handle_error(row, f"deleting non-existing entry {row.values}")
                continue
            del state[row.key]
        else:
            handle_error(row, f"invalid diff value: {row.diff}")
            continue

    return state


class PyObjectWrapperSerializerProtocol(Protocol):
    @staticmethod
    def dumps(object: Any) -> bytes: ...

    @staticmethod
    def loads(data: bytes) -> Any: ...


class PyObjectWrapperSerializer:
    def __init__(self, serializer: PyObjectWrapperSerializerProtocol) -> None:
        # making sure that the serializer contains appropriate methods
        # and taking only needed methods from it to make it serializable if it is a whole module
        self._loads = serializer.loads
        self._dumps = serializer.dumps

    def dumps(self, object: Any) -> bytes:
        return self._dumps(object)

    def loads(self, data: bytes) -> Any:
        return self._loads(data)


def wrap_serializer(
    serializer: PyObjectWrapperSerializerProtocol,
) -> PyObjectWrapperSerializer:
    return PyObjectWrapperSerializer(serializer)


def wrap_py_object(
    object: T, *, serializer: PyObjectWrapperSerializerProtocol | None = None
) -> PyObjectWrapper[T]:
    """A function wrapping python objects of any type to enable passing them to the engine.

    Args:
        value: a python object to be wrapped
        serializer: a custom serializer. Has to implement ``PyObjectWrapperSerializerProtocol``.
            If not set, ``pickle`` is used.

    Example:

    >>> import pathway as pw
    >>> import dill
    >>> from dataclasses import dataclass
    >>>
    >>> @dataclass
    ... class Simple:
    ...     a: int
    ...     def add(self, x: int) -> int:
    ...         return self.a + x
    ...
    >>> @pw.udf
    ... def create_py_object(a: int) -> pw.PyObjectWrapper[Simple]:
    ...     return pw.wrap_py_object(Simple(a), serializer=dill)
    ...
    >>> @pw.udf
    ... def use_py_object(a: int, b: pw.PyObjectWrapper[Simple]) -> int:
    ...     return b.value.add(a)
    ...
    >>> t = pw.debug.table_from_markdown(
    ...     '''
    ...     a
    ...     1
    ...     2
    ...     3
    ... '''
    ... ).with_columns(b=create_py_object(pw.this.a))
    >>> res = t.select(res=use_py_object(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(res, include_id=False) # doctest: +SKIP
    res
    2
    4
    6
    """
    if serializer is None:
        serializer_wrapped = None
    else:
        serializer_wrapped = wrap_serializer(serializer)
    return PyObjectWrapper._create_with_serializer(
        object, serializer=serializer_wrapped
    )

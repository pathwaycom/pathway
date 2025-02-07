# Copyright © 2024 Pathway

from __future__ import annotations

import datetime
import json as _json  # otherwise its easy to mistake `json` and `Json`
import operator
from dataclasses import dataclass
from functools import cached_property
from typing import Any, ClassVar, Iterator, TypeVar

import pandas as pd

import pathway.internals.datetime_types as dt


class _JsonEncoder(_json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Json):
            return obj.value
        if isinstance(obj, (pd.Timedelta, dt.Duration)):
            return obj.value
        if isinstance(obj, datetime.datetime):
            obj = pd.Timestamp(obj)
        if isinstance(obj, (pd.Timestamp, dt.DateTimeNaive, dt.DateTimeUtc)):
            return obj.isoformat(timespec="nanoseconds")
        return super().default(obj)


@dataclass(frozen=True, eq=True)
class Json:
    """Represents JSON values.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... a    | b | c
    ... True | 2 | manul
    ... ''')
    >>> @pw.udf
    ... def to_json(val) -> pw.Json:
    ...     return pw.Json(val)
    >>> result = t1.select(**{c: to_json(pw.this[c]) for c in t1.column_names()})
    >>> pw.debug.compute_and_print(result, include_id=False)
    a    | b | c
    true | 2 | "manul"
    """

    NULL: ClassVar[Json]

    _value: JsonValue

    def __str__(self) -> str:
        return _json.dumps(self.value)

    def __float__(self) -> float:
        return float(self.value)  # type:ignore[arg-type]

    def __int__(self) -> int:
        return int(self.value)  # type:ignore[arg-type]

    def __bool__(self) -> bool:
        return bool(self.value)

    def __repr__(self) -> str:
        return f"pw.Json({self.value!r})"

    def __getitem__(self, key: int | str) -> Json:
        return Json(self.value[key])  # type:ignore[index]

    def __iter__(self) -> Iterator[Json]:
        for item in self.value:  # type:ignore[union-attr]
            yield Json(item)

    def __len__(self) -> int:
        return len(self.value)  # type:ignore[arg-type]

    def __index__(self) -> int:
        return operator.index(self.value)  # type:ignore[arg-type]

    def __reversed__(self) -> Iterator[Json]:
        for item in reversed(self.value):  # type:ignore[arg-type]
            yield Json(item)

    @cached_property
    def value(self) -> JsonValue:
        if isinstance(self._value, Json):
            return self._value.value
        else:
            return self._value

    @staticmethod
    def parse(value: str | bytes | bytearray) -> Json:
        return Json(_json.loads(value))

    @staticmethod
    def dumps(obj: Any) -> str:
        return _json.dumps(obj, cls=_JsonEncoder)

    def as_int(self) -> int:
        """Returns Json value as an int if possible.

        Example:

        >>> import pathway as pw
        >>> import sys; sys.modules[__name__].pw = pw # NODOCS
        >>> class InputSchema(pw.Schema):
        ...     data: pw.Json
        ...
        >>> @pw.udf
        ... def extract(data: pw.Json) -> int:
        ...     return data["value"].as_int()
        ...
        >>> table = pw.debug.table_from_rows(schema=InputSchema, rows=[({"value": 42},)])
        >>> result = table.select(result=extract(pw.this.data))
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        42
        """

        return self._as_type(int)

    def as_str(self) -> str:
        """Returns Json value as a string if possible.

        Example:

        >>> import pathway as pw
        >>> import sys; sys.modules[__name__].pw = pw # NODOCS
        >>> class InputSchema(pw.Schema):
        ...     data: pw.Json
        ...
        >>> @pw.udf
        ... def extract(data: pw.Json) -> str:
        ...     return data["value"].as_str()
        ...
        >>> table = pw.debug.table_from_rows(schema=InputSchema, rows=[({"value": "foo"},)])
        >>> result = table.select(result=extract(pw.this.data))
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        foo
        """

        return self._as_type(str)

    def as_float(self) -> float:
        """Returns Json value as a float if possible.

        Example:

        >>> import pathway as pw
        >>> import sys; sys.modules[__name__].pw = pw # NODOCS
        >>> class InputSchema(pw.Schema):
        ...     data: pw.Json
        ...
        >>> @pw.udf
        ... def extract(data: pw.Json) -> float:
        ...     return data["value"].as_float()
        ...
        >>> table = pw.debug.table_from_rows(schema=InputSchema, rows=[({"value": 3.14},)])
        >>> result = table.select(result=extract(pw.this.data))
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        3.14
        """

        if isinstance(self.value, int):
            return float(self.value)
        else:
            return self._as_type(float)

    def as_bool(self) -> bool:
        """Returns Json value as a float if possible.

        Example:

        >>> import pathway as pw
        >>> import sys; sys.modules[__name__].pw = pw # NODOCS
        >>> class InputSchema(pw.Schema):
        ...     data: pw.Json
        ...
        >>> @pw.udf
        ... def extract(data: pw.Json) -> bool:
        ...     return data["value"].as_bool()
        ...
        >>> table = pw.debug.table_from_rows(schema=InputSchema, rows=[({"value": True},)])
        >>> result = table.select(result=extract(pw.this.data))
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        True
        """

        return self._as_type(bool)

    def as_list(self) -> list:
        """Returns Json value as a list if possible.

        Example:

        >>> import pathway as pw
        >>> import sys; sys.modules[__name__].pw = pw # NODOCS
        >>> class InputSchema(pw.Schema):
        ...     data: pw.Json
        ...
        >>> @pw.udf
        ... def extract(data: pw.Json) -> int:
        ...     return data["value"].as_list()[-1]
        ...
        >>> table = pw.debug.table_from_rows(schema=InputSchema, rows=[({"value": [1,2,3]},)])
        >>> result = table.select(result=extract(pw.this.data))
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        3
        """

        return self._as_type(list)

    def as_dict(self) -> dict:
        """Returns Json value as a dict if possible.

        Example:

        >>> import pathway as pw
        >>> import sys; sys.modules[__name__].pw = pw # NODOCS
        >>> class InputSchema(pw.Schema):
        ...     data: pw.Json
        ...
        >>> @pw.udf
        ... def extract(data: pw.Json) -> tuple:
        ...     return tuple(data["value"].as_dict().values())
        ...
        >>> table = pw.debug.table_from_rows(schema=InputSchema, rows=[({"value": {"inner": 42}},)])
        >>> result = table.select(result=extract(pw.this.data))
        >>> pw.debug.compute_and_print(result, include_id=False)
        result
        (42,)
        """
        return self._as_type(dict)

    def _as_type(self, type: type[J]) -> Any:
        if isinstance(self.value, type):
            return self.value
        else:
            raise ValueError(f"Cannot convert Json {self.value} to {type}")


JsonValue = (
    int | float | str | bool | list["JsonValue"] | dict[str, "JsonValue"] | None | Json
)

J = TypeVar("J", bound=JsonValue)

Json.NULL = Json(None)

__all__ = ["Json", "JsonValue"]

# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import collections
import datetime
import functools
import math
import typing
from abc import ABC, abstractmethod
from enum import Enum
from functools import cached_property
from types import EllipsisType, NoneType, UnionType

import numpy as np
import numpy.typing as npt
import pandas as pd

from pathway.internals import api, datetime_types, json as js
from pathway.internals.api import PathwayType

if typing.TYPE_CHECKING:
    from pathway.internals.schema import SchemaMetaclass


class DType(ABC):
    _cache: dict[typing.Any, DType] = {}

    @abstractmethod
    def to_engine(self) -> api.PathwayType: ...

    @abstractmethod
    def is_value_compatible(self, arg) -> bool: ...

    @abstractmethod
    def _set_args(self, *args): ...

    def __new__(cls, *args):
        key = (cls, args)
        if key not in DType._cache:
            ret = super().__new__(cls)
            ret._set_args(*args)
            cls._cache[key] = ret
        return DType._cache[key]

    def __class_getitem__(cls, args):
        if isinstance(args, tuple):
            return cls(*args)
        else:
            return cls(args)

    def equivalent_to(self, other: DType) -> bool:
        return dtype_equivalence(self, other)

    @property
    @abstractmethod
    def typehint(self) -> typing.Any: ...

    @abstractmethod
    def max_size(self) -> float:
        """The maximal size of a DType measured in the number of Value enums"""
        ...

    def to_dict(self) -> dict:
        """Serialize DType into a json-serializable dict."""
        return {
            "type": repr(self),
        }


class _SimpleDType(DType):
    wrapped: type

    def __repr__(self):
        return {
            INT: "INT",
            BOOL: "BOOL",
            STR: "STR",
            FLOAT: "FLOAT",
            BYTES: "BYTES",
        }[self]

    def _set_args(self, wrapped):
        self.wrapped = wrapped

    def __new__(cls, wrapped: type) -> _SimpleDType:
        return super().__new__(cls, wrapped)

    def is_value_compatible(self, arg):
        if self.wrapped == float:
            return np.issubdtype(type(arg), np.floating) or np.issubdtype(
                type(arg), np.integer
            )
        elif self.wrapped == int:
            return np.issubdtype(type(arg), np.integer)
        elif self.wrapped == bool:
            return isinstance(arg, (bool, np.bool_))
        elif self.wrapped == bytes:
            return isinstance(arg, (bytes, str))
        else:
            return isinstance(arg, self.wrapped)

    def to_engine(self) -> api.PathwayType:
        return {
            INT: api.PathwayType.INT,
            BOOL: api.PathwayType.BOOL,
            STR: api.PathwayType.STRING,
            FLOAT: api.PathwayType.FLOAT,
            BYTES: api.PathwayType.BYTES,
        }[self]

    @property
    def typehint(self) -> type:
        return self.wrapped

    def max_size(self) -> float:
        if self.wrapped in {str, bytes}:
            return math.inf
        else:
            return 1


INT: DType = _SimpleDType(int)
BOOL: DType = _SimpleDType(bool)
STR: DType = _SimpleDType(str)
BYTES: DType = _SimpleDType(bytes)
FLOAT: DType = _SimpleDType(float)


class _NoneDType(DType):
    def __repr__(self):
        return "NONE"

    def _set_args(self):
        pass

    def __new__(cls) -> _NoneDType:
        return super().__new__(cls)

    def is_value_compatible(self, arg):
        return arg is None or isinstance(arg, pd._libs.missing.NAType)

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.ANY

    @property
    def typehint(self) -> None:
        return None

    def max_size(self) -> float:
        return 1


NONE: DType = _NoneDType()


class _AnyDType(DType):
    def __repr__(self):
        return "ANY"

    def _set_args(self):
        pass

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.ANY

    def __new__(cls) -> _AnyDType:
        return super().__new__(cls)

    def is_value_compatible(self, arg):
        return True

    @property
    def typehint(self) -> typing.Any:
        return typing.Any

    def max_size(self) -> float:
        return math.inf


ANY: DType = _AnyDType()


class Callable(DType):
    arg_types: EllipsisType | tuple[DType, ...]
    return_type: DType

    def to_dict(self) -> dict:
        raise RuntimeError(
            "to_dict is implemented and must be used only for engine types"
        )

    def __repr__(self):
        if isinstance(self.arg_types, EllipsisType):
            return f"Callable(..., {self.return_type})"
        else:
            return f"Callable({self.arg_types}, {self.return_type})"

    def _set_args(self, arg_types, return_type):
        self.arg_types = arg_types
        self.return_type = return_type

    def __new__(
        cls,
        arg_types: EllipsisType | tuple[DType | EllipsisType, ...],
        return_type: DType,
    ) -> Callable:
        if not isinstance(arg_types, EllipsisType):
            arg_types = tuple(wrap(dtype) for dtype in arg_types)
        return_type = wrap(return_type)
        return super().__new__(cls, arg_types, return_type)

    def is_value_compatible(self, arg):
        return callable(arg)

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.ANY  # also passed to the engine as column properties

    @cached_property
    def typehint(self) -> typing.Any:
        if isinstance(self.arg_types, EllipsisType):
            return typing.Callable[..., self.return_type.typehint]
        else:
            return typing.Callable[
                [dtype.typehint for dtype in self.arg_types],
                self.return_type.typehint,
            ]

    def max_size(self) -> float:
        return math.inf


class Array(DType):
    n_dim: int | None
    wrapped: DType

    def __repr__(self):
        args = []
        if self.n_dim is not None:
            args.append(repr(self.n_dim))
        if self.wrapped != ANY:
            args.append(repr(self.wrapped))
        args_str = ", ".join(args)
        if args_str:
            return f"Array({args_str})"
        else:
            return "Array"

    def to_dict(self) -> dict:
        return {
            "type": "ARRAY",
            "n_dim": self.n_dim,
            "wrapped": self.wrapped.to_dict(),
        }

    def _set_args(self, n_dim, wrapped):
        self.wrapped = wrapped
        self.n_dim = n_dim

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.array(self.n_dim, self.wrapped.to_engine())

    def __new__(cls, n_dim, wrapped) -> Array:
        dtype = wrap(wrapped)
        if isinstance(dtype, Array) and dtype.n_dim is not None:
            return Array(n_dim=dtype.n_dim + n_dim, wrapped=dtype.wrapped)
        else:
            return super().__new__(cls, n_dim, dtype)

    def is_value_compatible(self, arg):
        if isinstance(arg, np.ndarray):
            if self.n_dim is not None and self.n_dim != len(arg.shape):
                return False
            x: np.ndarray
            for x in np.nditer(arg, flags=["zerosize_ok"]):  # type: ignore[assignment]
                if not self.wrapped.is_value_compatible(x[()]):
                    return False
            return True
        else:
            if self.n_dim is not None:
                return False
            return self.wrapped.is_value_compatible(arg)

    @property
    def typehint(self) -> type[np.ndarray]:
        return npt.NDArray[self.wrapped.typehint]  # type: ignore[name-defined]

    def strip_dimension(self) -> DType:
        if self.n_dim is None:
            return Array(n_dim=None, wrapped=self.wrapped)
        elif self.n_dim > 1:
            return Array(n_dim=self.n_dim - 1, wrapped=self.wrapped)
        else:
            return self.wrapped

    def max_size(self) -> float:
        return math.inf


T = typing.TypeVar("T")


class Pointer(DType):
    args: tuple[DType, ...] | None

    def __repr__(self):
        if self.args is None:
            return "ANY_POINTER"
        else:
            return f"Pointer({', '.join(repr(arg) for arg in self.args)})"

    def to_dict(self) -> dict:
        return {
            "type": "POINTER",
        }

    def _set_args(self, args):
        self.args = args

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.POINTER

    def __new__(cls, *args: DType | EllipsisType | SchemaMetaclass) -> Pointer:
        if args == (...,):
            return super().__new__(cls, None)  # ANY_POINTER
        else:
            from pathway.internals.schema import SchemaMetaclass

            if len(args) == 1 and isinstance(args[0], SchemaMetaclass):
                return args[0]._id_dtype
            else:
                return super().__new__(cls, tuple(wrap(arg) for arg in args))

    def is_value_compatible(self, arg):
        return isinstance(arg, api.Pointer)

    @cached_property
    def typehint(self) -> type[api.Pointer]:
        if self.args is None:
            return api.Pointer
        else:
            return api.Pointer[tuple(arg.typehint for arg in self.args)]  # type: ignore[misc]

    def max_size(self) -> float:
        return 1


ANY_POINTER: DType = Pointer(...)


class Optional(DType):
    wrapped: DType

    def __init__(self, arg):
        super().__init__()

    def __repr__(self):
        return f"Optional({self.wrapped})"

    def to_dict(self) -> dict:
        return {
            "type": "OPTIONAL",
            "wrapped": self.wrapped.to_dict(),
        }

    def _set_args(self, wrapped):
        self.wrapped = wrapped

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.optional(self.wrapped.to_engine())

    def __new__(cls, arg: DType) -> DType:  # type:ignore[misc]
        arg = wrap(arg)
        if arg == NONE or isinstance(arg, Optional) or arg == ANY:
            return arg
        return super().__new__(cls, arg)

    def is_value_compatible(self, arg):
        if arg is None:
            return True
        return self.wrapped.is_value_compatible(arg)

    @cached_property
    def typehint(self) -> UnionType:
        return self.wrapped.typehint | None

    def max_size(self) -> float:
        return self.wrapped.max_size()


class Tuple(DType):
    args: tuple[DType, ...]

    def __init__(self, *args):
        super().__init__()

    def __repr__(self):
        return f"Tuple({', '.join(str(arg) for arg in self.args)})"

    def to_dict(self) -> dict:
        return {"type": "TUPLE", "wrapped": [arg.to_dict() for arg in self.args]}

    def _set_args(self, args):
        self.args = args

    def to_engine(self) -> PathwayType:
        return api.PathwayType.tuple(*[arg.to_engine() for arg in self.args])

    def __new__(cls, *args: DType | EllipsisType) -> Tuple | List:  # type: ignore[misc]
        if any(isinstance(arg, EllipsisType) for arg in args):
            arg, placeholder = args
            assert isinstance(placeholder, EllipsisType)
            assert isinstance(arg, DType)
            return List(arg)
        else:
            return super().__new__(cls, tuple(wrap(arg) for arg in args))

    def is_value_compatible(self, arg):
        if not isinstance(arg, (tuple, list)):
            return False
        elif len(self.args) != len(arg):
            return False
        else:
            return all(
                subdtype.is_value_compatible(subvalue)
                for subdtype, subvalue in zip(self.args, arg)
            )

    @cached_property
    def typehint(self) -> type[tuple]:
        return tuple[tuple(arg.typehint for arg in self.args)]  # type: ignore[misc]

    def max_size(self) -> float:
        return sum(arg.max_size() for arg in self.args) + 1


class Json(DType):
    def __new__(cls) -> Json:
        return super().__new__(cls)

    def _set_args(self):
        pass

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.JSON

    def __repr__(self) -> str:
        return "Json"

    def is_value_compatible(self, arg):
        return isinstance(arg, js.Json)

    @property
    def typehint(self) -> type[js.Json]:
        return js.Json

    def max_size(self) -> float:
        return math.inf


JSON: DType = Json()


class List(DType):
    wrapped: DType

    def __repr__(self):
        return f"List({self.wrapped})"

    def to_dict(self) -> dict:
        return {"type": "LIST", "wrapped": self.wrapped.to_dict()}

    def __new__(cls, wrapped: DType) -> List:
        return super().__new__(cls, wrap(wrapped))

    def _set_args(self, wrapped):
        self.wrapped = wrapped

    def to_engine(self) -> PathwayType:
        return api.PathwayType.list(self.wrapped.to_engine())

    def is_value_compatible(self, arg):
        return isinstance(arg, (tuple, list)) and all(
            self.wrapped.is_value_compatible(val) for val in arg
        )

    @cached_property
    def typehint(self) -> type[list]:
        return list[self.wrapped.typehint]  # type: ignore[name-defined]

    def max_size(self) -> float:
        return math.inf


class Future(DType):
    wrapped: DType

    def __repr__(self):
        return f"Future({self.wrapped})"

    def to_dict(self) -> dict:
        return {"type": "FUTURE", "wrapped": self.wrapped.to_dict()}

    def __new__(cls, arg: DType) -> Future:
        arg = wrap(arg)
        if isinstance(arg, Future):
            return arg
        return super().__new__(cls, arg)

    def _set_args(self, wrapped):
        self.wrapped = wrapped

    def to_engine(self) -> PathwayType:
        return api.PathwayType.future(self.wrapped.to_engine())

    def is_value_compatible(self, arg):
        return arg is api.PENDING or self.wrapped.is_value_compatible(arg)

    @cached_property
    def typehint(self) -> type[asyncio.Future]:
        return asyncio.Future[self.wrapped.typehint]  # type: ignore[name-defined]

    def max_size(self) -> float:
        return self.wrapped.max_size()


class _DateTimeNaive(DType):
    def __repr__(self):
        return "DATE_TIME_NAIVE"

    def _set_args(self):
        pass

    def __new__(cls) -> _DateTimeNaive:
        return super().__new__(cls)

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.DATE_TIME_NAIVE

    def is_value_compatible(self, arg):
        return isinstance(arg, datetime.datetime) and arg.tzinfo is None

    @property
    def typehint(self) -> type[datetime_types.DateTimeNaive]:
        return datetime_types.DateTimeNaive

    def max_size(self) -> float:
        return 1


DATE_TIME_NAIVE = _DateTimeNaive()


class _DateTimeUtc(DType):
    def __repr__(self):
        return "DATE_TIME_UTC"

    def _set_args(self):
        pass

    def __new__(cls) -> _DateTimeUtc:
        return super().__new__(cls)

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.DATE_TIME_UTC

    def is_value_compatible(self, arg):
        return isinstance(arg, datetime.datetime) and arg.tzinfo is not None

    @property
    def typehint(self) -> type[datetime_types.DateTimeUtc]:
        return datetime_types.DateTimeUtc

    def max_size(self) -> float:
        return 1


DATE_TIME_UTC = _DateTimeUtc()


class _Duration(DType):
    def __repr__(self):
        return "DURATION"

    def _set_args(self):
        pass

    def __new__(cls) -> _Duration:
        return super().__new__(cls)

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.DURATION

    def is_value_compatible(self, arg):
        return isinstance(arg, datetime.timedelta)

    @property
    def typehint(self) -> type[datetime_types.Duration]:
        return datetime_types.Duration

    def max_size(self) -> float:
        return 1


DURATION = _Duration()


class PyObjectWrapper(DType):
    wrapped: type

    def __new__(cls, wrapped: type) -> PyObjectWrapper:
        return super().__new__(cls, wrapped)

    def _set_args(self, wrapped: type) -> None:
        self.wrapped = wrapped

    def to_engine(self) -> api.PathwayType:
        return api.PathwayType.PY_OBJECT_WRAPPER

    def __repr__(self) -> str:
        return f"PyObjectWrapper({self.wrapped})"

    def to_dict(self) -> dict:
        return {"type": "PY_OBJECT_WRAPPER"}

    def is_value_compatible(self, arg):
        return isinstance(arg, api.PyObjectWrapper) and isinstance(
            arg.value, self.wrapped
        )

    @property
    def typehint(self) -> type[api.PyObjectWrapper]:
        return api.PyObjectWrapper[self.wrapped]  # type: ignore[name-defined]

    def max_size(self) -> float:
        return math.inf


ANY_PY_OBJECT_WRAPPER: DType = PyObjectWrapper(object)


def wrap(input_type) -> DType:
    assert input_type != Optional
    assert input_type != Tuple
    assert input_type != Callable
    assert input_type != Array
    assert input_type != List
    assert input_type != Json
    assert input_type != PyObjectWrapper
    assert input_type != ...

    from pathway.internals.schema import ColumnSchema

    if isinstance(input_type, ColumnSchema):
        return input_type.dtype

    if isinstance(input_type, DType):
        return input_type
    if typing.get_origin(input_type) == np.dtype:
        (input_type,) = typing.get_args(input_type)
    if input_type in (NoneType, None):
        return NONE
    elif input_type == typing.Any:
        return ANY
    elif input_type == api.Pointer:
        return ANY_POINTER
    elif typing.get_origin(input_type) == api.Pointer:
        args = typing.get_args(input_type)
        return Pointer(*args)
    elif isinstance(input_type, str):
        return ANY  # TODO: input_type is annotation for class
    elif typing.get_origin(input_type) == collections.abc.Callable:
        c_args = get_args(input_type)
        if c_args == ():
            return Callable(..., ANY)
        arg_types, ret_type = c_args
        if isinstance(arg_types, Tuple):
            callable_args: tuple[DType, ...] | EllipsisType = arg_types.args
        else:
            assert isinstance(arg_types, EllipsisType)
            callable_args = arg_types
        assert isinstance(ret_type, DType), type(ret_type)
        return Callable(callable_args, ret_type)
    elif (
        typing.get_origin(input_type) in (typing.Union, UnionType)
        and len(typing.get_args(input_type)) == 2
        and isinstance(None, typing.get_args(input_type)[1])
    ):
        arg, _ = get_args(input_type)
        assert isinstance(arg, DType)
        return Optional(arg)
    elif input_type in [list, tuple, typing.List, typing.Tuple]:
        return ANY_TUPLE
    elif (
        input_type == js.Json
        or input_type == dict
        or typing.get_origin(input_type) == dict
    ):
        return JSON
    elif typing.get_origin(input_type) == list:
        args = get_args(input_type)
        (arg,) = args
        return List(wrap(arg))
    elif typing.get_origin(input_type) == tuple:
        args = get_args(input_type)
        if args[-1] == ...:
            arg, _ = args
            return List(wrap(arg))
        else:
            return Tuple(*[wrap(arg) for arg in args])
    elif input_type == np.ndarray:
        return ANY_ARRAY
    elif typing.get_origin(input_type) == np.ndarray:
        dims, wrapped = get_args(input_type)
        if dims == typing.Any:
            return Array(n_dim=None, wrapped=wrapped)
        return Array(n_dim=len(typing.get_args(dims)), wrapped=wrapped)
    elif input_type == api.PyObjectWrapper:
        return ANY_PY_OBJECT_WRAPPER
    elif typing.get_origin(input_type) == api.PyObjectWrapper:
        (inner,) = typing.get_args(input_type)
        return PyObjectWrapper(inner)
    elif isinstance(input_type, type) and issubclass(input_type, Enum):
        return ANY
    elif input_type == datetime.datetime:
        raise TypeError(
            f"Unsupported type {input_type}, use pw.DATE_TIME_UTC or pw.DATE_TIME_NAIVE"
        )
    elif input_type == datetime.timedelta:
        raise TypeError(f"Unsupported type {input_type}, use pw.DURATION")
    elif typing.get_origin(input_type) == asyncio.Future:
        args = get_args(input_type)
        (arg,) = args
        return Future(wrap(arg))
    else:
        dtype = {
            int: INT,
            bool: BOOL,
            str: STR,
            float: FLOAT,
            datetime_types.Duration: DURATION,
            datetime_types.DateTimeNaive: DATE_TIME_NAIVE,
            datetime_types.DateTimeUtc: DATE_TIME_UTC,
            np.int32: INT,
            np.int64: INT,
            np.float32: FLOAT,
            np.float64: FLOAT,
            bytes: BYTES,
        }.get(input_type, None)
        if dtype is None:
            raise TypeError(f"Unsupported type {input_type!r}.")
        return dtype


ANY_TUPLE: DType = List(ANY)
ANY_ARRAY: DType = Array(n_dim=None, wrapped=ANY)
ANY_ARRAY_1D: DType = Array(n_dim=1, wrapped=ANY)
ANY_ARRAY_2D: DType = Array(n_dim=2, wrapped=ANY)
INT_ARRAY: DType = Array(n_dim=None, wrapped=INT)
INT_ARRAY_1D: DType = Array(n_dim=1, wrapped=INT)
INT_ARRAY_2D: DType = Array(n_dim=2, wrapped=INT)
FLOAT_ARRAY: DType = Array(n_dim=None, wrapped=FLOAT)
FLOAT_ARRAY_1D: DType = Array(n_dim=1, wrapped=FLOAT)
FLOAT_ARRAY_2D: DType = Array(n_dim=2, wrapped=FLOAT)


def dtype_equivalence(
    left: DType, right: DType, int_float_compatible: bool = True
) -> bool:
    return dtype_issubclass(
        left, right, int_float_compatible=int_float_compatible
    ) and dtype_issubclass(right, left, int_float_compatible=int_float_compatible)


def broadcast_tuples(
    left: Tuple | List, right: Tuple | List
) -> tuple[tuple[DType, ...], tuple[DType, ...]]:
    largs: tuple[DType, ...]
    rargs: tuple[DType, ...]
    if isinstance(left, List) and isinstance(right, List):
        largs = (left.wrapped,)
        rargs = (right.wrapped,)
    elif isinstance(left, List):
        assert isinstance(right, Tuple)
        assert not isinstance(right.args, EllipsisType)
        rargs = right.args
        largs = tuple(left.wrapped for _arg in rargs)
    elif isinstance(right, List):
        assert isinstance(left, Tuple)
        assert not isinstance(left.args, EllipsisType)
        largs = left.args
        rargs = tuple(right.wrapped for _arg in largs)
    else:
        assert isinstance(left, Tuple)
        assert isinstance(right, Tuple)
        assert not isinstance(left.args, EllipsisType)
        assert not isinstance(right.args, EllipsisType)
        largs = left.args
        rargs = right.args
    return (largs, rargs)


def dtype_tuple_issubclass(
    left: Tuple | List, right: Tuple | List, int_float_compatible: bool = True
) -> bool:
    if left == ANY_TUPLE or right == ANY_TUPLE:
        return True
    largs, rargs = broadcast_tuples(left, right)
    if len(largs) != len(rargs):
        return False
    return all(
        dtype_issubclass(l_arg, r_arg, int_float_compatible=int_float_compatible)
        for l_arg, r_arg in zip(largs, rargs)
    )


def dtype_pointer_issubclass(left: Pointer, right: Pointer) -> bool:
    if left.args is None or right.args is None:
        return True
    largs = left.args
    rargs = right.args
    if len(largs) != len(rargs):
        return False
    return all(
        dtype_issubclass(l_arg, r_arg, int_float_compatible=False)
        for l_arg, r_arg in zip(largs, rargs)
    )


def dtype_array_equivalence(left: Array, right: Array) -> bool:
    dim_compatible = (
        left.n_dim is None or right.n_dim is None or left.n_dim == right.n_dim
    )
    wrapped_compatible = (
        left.wrapped == ANY or right.wrapped == ANY or left.wrapped == right.wrapped
    )
    return dim_compatible and wrapped_compatible


def dtype_issubclass(
    left: DType, right: DType, *, int_float_compatible: bool = True
) -> bool:
    if right == ANY:  # catch the case, when left=Optional[T] and right=Any
        return True
    elif isinstance(left, Optional):
        if isinstance(right, Optional):
            return dtype_issubclass(
                unoptionalize(left),
                unoptionalize(right),
                int_float_compatible=int_float_compatible,
            )
        else:
            return False
    elif left == NONE:
        return isinstance(right, Optional) or right == NONE
    elif isinstance(right, Optional):
        return dtype_issubclass(
            left, unoptionalize(right), int_float_compatible=int_float_compatible
        )
    elif isinstance(left, (Tuple, List)) and isinstance(right, (Tuple, List)):
        return dtype_tuple_issubclass(
            left, right, int_float_compatible=int_float_compatible
        )
    elif isinstance(left, Array) and isinstance(right, Array):
        return dtype_array_equivalence(left, right)
    elif isinstance(left, Pointer) and isinstance(right, Pointer):
        return dtype_pointer_issubclass(left, right)
    elif isinstance(left, _SimpleDType) and isinstance(right, _SimpleDType):
        if left == INT and right == FLOAT:
            return int_float_compatible
        elif left == BOOL and right == INT:
            return False
        else:
            return issubclass(left.wrapped, right.wrapped)
    elif isinstance(left, Callable) and isinstance(right, Callable):
        return True
    return left == right


def types_lca(
    left: DType, right: DType, *, raising: bool, int_float_compatible: bool = True
) -> DType:
    """LCA of two types."""
    if isinstance(left, Optional) or isinstance(right, Optional):
        return Optional(
            types_lca(
                unoptionalize(left),
                unoptionalize(right),
                raising=raising,
                int_float_compatible=int_float_compatible,
            )
        )
    elif isinstance(left, (Tuple, List)) and isinstance(right, (Tuple, List)):
        if left == ANY_TUPLE or right == ANY_TUPLE:
            return ANY_TUPLE
        if isinstance(left, List) and isinstance(right, List):
            return List(
                types_lca(
                    left.wrapped,
                    right.wrapped,
                    raising=raising,
                    int_float_compatible=int_float_compatible,
                )
            )
        largs, rargs = broadcast_tuples(left, right)
        if len(largs) != len(rargs):
            if raising:
                raise TypeError
            else:
                return ANY_TUPLE
        return Tuple(
            *[
                types_lca(
                    l_arg,
                    r_arg,
                    raising=raising,
                    int_float_compatible=int_float_compatible,
                )
                for l_arg, r_arg in zip(largs, rargs)
            ]
        )
    elif isinstance(left, Array) and isinstance(right, Array):
        if left.n_dim is None or right.n_dim is None:
            n_dim = None
        elif left.n_dim == right.n_dim:
            n_dim = left.n_dim
        else:
            n_dim = None
        if left.wrapped == ANY or right.wrapped == ANY:
            wrapped = ANY
        elif left.wrapped == right.wrapped:
            wrapped = left.wrapped
        else:
            if raising:
                raise TypeError
            else:
                wrapped = ANY
        return Array(n_dim=n_dim, wrapped=wrapped)
    elif isinstance(left, Pointer) and isinstance(right, Pointer):
        if left.args is None or right.args is None:
            return ANY_POINTER
        if len(left.args) != len(right.args):
            if raising:
                raise TypeError
            else:
                return ANY_POINTER
        return Pointer(
            *[
                types_lca(left, right, raising=raising, int_float_compatible=False)
                for left, right in zip(left.args, right.args)
            ]
        )
    if dtype_issubclass(left, right, int_float_compatible=int_float_compatible):
        return right
    elif dtype_issubclass(right, left, int_float_compatible=int_float_compatible):
        return left

    if left == NONE:
        return Optional(right)
    elif right == NONE:
        return Optional(left)
    elif left == ANY or right == ANY:
        return ANY
    else:
        if raising:
            raise TypeError
        else:
            return ANY


def types_lca_many(*args: DType, raising: bool) -> DType:
    return functools.reduce(
        lambda x, y: types_lca(x, y, raising=raising, int_float_compatible=True), args
    )


def unoptionalize(dtype: DType) -> DType:
    return dtype.wrapped if isinstance(dtype, Optional) else dtype


def normalize_dtype(dtype: DType) -> DType:
    if isinstance(dtype, Pointer):
        return ANY_POINTER
    if isinstance(dtype, Array):
        return ANY_ARRAY
    return dtype


def normalize_pointers(dtype: DType) -> DType:
    if isinstance(dtype, Pointer):
        return ANY_POINTER
    if isinstance(dtype, List):
        return List(normalize_pointers(dtype.wrapped))
    if isinstance(dtype, Tuple):
        return Tuple(*[normalize_pointers(arg) for arg in dtype.args])
    if isinstance(dtype, Optional):
        return Optional(normalize_pointers(dtype.wrapped))
    return dtype


def coerce_arrays_pair(left: Array, right: Array) -> tuple[Array, Array]:
    if left.wrapped == ANY and right.wrapped != ANY:
        left = Array(n_dim=left.n_dim, wrapped=right.wrapped)
    if right.wrapped == ANY and left.wrapped != ANY:
        right = Array(n_dim=right.n_dim, wrapped=left.wrapped)
    if left.n_dim is None and right.n_dim is not None:
        right = Array(n_dim=None, wrapped=right.wrapped)
    if right.n_dim is None and left.n_dim is not None:
        left = Array(n_dim=None, wrapped=left.wrapped)
    return left, right


def unoptionalize_pair(left_dtype: DType, right_dtype: DType) -> tuple[DType, DType]:
    """
    Unpacks type out of typing.Optional and matches
    a second type with it if it is an EmptyType.
    """
    if left_dtype == NONE and isinstance(right_dtype, Optional):
        left_dtype = right_dtype
    if right_dtype == NONE and isinstance(left_dtype, Optional):
        right_dtype = left_dtype

    return unoptionalize(left_dtype), unoptionalize(right_dtype)


def get_args(dtype: typing.Any) -> tuple[EllipsisType | DType, ...]:
    arg_types = typing.get_args(dtype)
    return tuple(wrap(arg) if arg != ... else ... for arg in arg_types)


_HASHABLE_DTYPES = {
    INT,
    BOOL,
    STR,
    BYTES,
    FLOAT,
    NONE,
    DATE_TIME_NAIVE,
    DATE_TIME_UTC,
    DURATION,
}


def is_hashable_in_python(dtype: DType) -> bool:
    if isinstance(dtype, Optional):
        return is_hashable_in_python(unoptionalize(dtype))
    if isinstance(dtype, Tuple):
        return all(is_hashable_in_python(d) for d in dtype.args)
    if isinstance(dtype, List):
        return is_hashable_in_python(dtype.wrapped)
    if isinstance(dtype, Pointer):
        return True
    return dtype in _HASHABLE_DTYPES


COMPOSITE_DTYPES = {
    "ARRAY": lambda d: Array(d["n_dim"], parse_dtype_from_dict(d["wrapped"])),
    "LIST": lambda d: List(parse_dtype_from_dict(d["wrapped"])),
    "OPTIONAL": lambda d: Optional(parse_dtype_from_dict(d["wrapped"])),
    "TUPLE": lambda d: Tuple(*(parse_dtype_from_dict(w) for w in d["wrapped"])),
    "FUTURE": lambda d: Future(parse_dtype_from_dict(d["wrapped"])),
}
NON_COMPOSITE_DTYPES = {
    "STR": STR,
    "BOOL": BOOL,
    "INT": INT,
    "FLOAT": FLOAT,
    "BYTES": BYTES,
    "DATE_TIME_NAIVE": DATE_TIME_NAIVE,
    "DATE_TIME_UTC": DATE_TIME_UTC,
    "DURATION": DURATION,
    "Json": JSON,
    "NONE": NONE,
    "ANY": ANY,
    "PY_OBJECT_WRAPPER": ANY_PY_OBJECT_WRAPPER,
    "POINTER": ANY_POINTER,
}


def parse_dtype_from_dict(data: dict) -> DType:
    type_name = data["type"]
    if type_name in NON_COMPOSITE_DTYPES:
        return NON_COMPOSITE_DTYPES[type_name]
    if type_name in COMPOSITE_DTYPES:
        return COMPOSITE_DTYPES[type_name](data)
    raise ValueError(f"Failed to parse DType from dict: {data}")

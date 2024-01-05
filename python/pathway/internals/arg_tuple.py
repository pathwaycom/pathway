# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable, MutableMapping, Sequence
from typing import Any


def wrap_arg_tuple(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        return as_arg_tuple(result).scalar_or_tuple()

    return wrapper


def as_arg_tuple(obj) -> ArgTuple:
    if isinstance(obj, ArgTuple):
        return obj
    elif isinstance(obj, MutableMapping):
        return MappingArgTuple(obj)
    elif isinstance(obj, Sequence):
        result = {f"{i}": v for i, v in enumerate(obj)}
        return TupleArgTuple(result)
    else:
        return ScalarArgTuple({"0": obj})


class ArgTuple:
    _content: MutableMapping[str, Any]

    def __init__(self, args: MutableMapping[str, Any]):
        self._content = args

    @classmethod
    def empty(cls) -> ArgTuple:
        return cls({})

    def __iter__(self):
        return iter(self._content.values())

    def __getitem__(self, k):
        return self._content.__getitem__(k)

    def __setitem__(self, k, value):
        return self.__setattr__(k, value)

    def __contains__(self, item):
        return self._content.__contains__(item)

    def __getattr__(self, item):
        return self._content.__getitem__(item)

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "_content":
            super().__setattr__(__name, __value)
        else:
            self._content[__name] = __value

    def __len__(self) -> int:
        return len(self._content)

    def __add__(self, other: ArgTuple) -> ArgTuple:
        return type(self)({**self._content, **other._content})

    def single(self) -> bool:
        return len(self._content) == 1

    def scalar_or_tuple(self):
        if self.single():
            return self.first()
        else:
            return self

    def keys(self):
        return self._content.keys()

    def values(self):
        return self._content.values()

    def items(self):
        return self._content.items()

    def first(self):
        return next(iter(self._content.values()))

    def map_values(self, mapfn: Callable) -> ArgTuple:
        mapped_values = {k: mapfn(v) for k, v in self._content.items()}
        return type(self)(mapped_values)

    def intersect_keys(self, other: ArgTuple) -> ArgTuple:
        return type(self)({k: v for k, v in self.items() if k in other})

    def subtract_keys(self, other: ArgTuple) -> ArgTuple:
        return type(self)({k: v for k, v in self.items() if k not in other})

    def with_same_order(self, other: ArgTuple) -> ArgTuple:
        assert self.is_key_subset_of(other)
        ordered_content = {k: self[k] for k in other.keys() if k in self}
        return type(self)(ordered_content)

    def with_keys_of(self, other: ArgTuple) -> ArgTuple:
        assert len(self) <= len(other)
        ordered_content = dict(zip(other.keys(), self.values(), strict=False))
        return type(self)(ordered_content)

    def is_key_subset_of(self, other: ArgTuple) -> bool:
        return self.keys() <= other.keys()

    def to_output(self):
        return self

    def process_input(self, other):
        return self.with_keys_of(other)


class MappingArgTuple(ArgTuple):
    def scalar_or_tuple(self):
        return self

    def process_input(self, other):
        return self.with_same_order(other)


class TupleArgTuple(ArgTuple):
    def to_output(self):
        return tuple(self.values())


class ScalarArgTuple(ArgTuple):
    def to_output(self):
        return self.first()

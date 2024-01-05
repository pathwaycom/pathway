# Copyright © 2024 Pathway


from __future__ import annotations

from collections import namedtuple
from collections.abc import Iterable, Iterator, MutableSet
from functools import partial, wraps
from typing import Generic, TypeVar

from pathway.internals import arg_tuple
from pathway.internals.shadows import inspect

T = TypeVar("T")
T2 = TypeVar("T2")


class SetOnceProperty(Generic[T]):
    def __init__(self, wrapper=None):
        self._wrapper = wrapper

    def __set_name__(self, owner, name: str):
        self._name = f"_{name}"

    def __get__(self, obj, owner=None) -> T:
        # if obj is None:
        #     return self
        return getattr(obj, self._name)

    def __set__(self, obj, value):
        try:
            object.__getattribute__(obj, self._name)
        except AttributeError:
            if self._wrapper is not None:
                value = self._wrapper(value)
            setattr(obj, self._name, value)
            return
        raise AttributeError("trying to overwrite read-only property")

    def __delete___(self, obj):
        raise AttributeError("trying to delete read-only property")


def with_optional_kwargs(decorator):
    @wraps(
        decorator
    )  # necessary for doctests to work, see https://www.rosipov.com/blog/python-doctests-and-decorators-bug/
    def wrapper(func=None, **kwargs):
        if func is None:
            return partial(wrapper, **kwargs)

        return decorator(func, **kwargs)

    return wrapper


FunctionSpec = namedtuple("FunctionSpec", ["func", "arg_names", "defaults"])


def function_spec(fn):
    fn = inspect.unwrap(fn)
    fullspec = inspect.getfullargspec(fn)
    defaults = {}
    if fullspec.defaults is not None:
        for index, default in enumerate(reversed(fullspec.defaults)):
            defaults[fullspec.args[-index - 1]] = default
    arg_names = fn.__code__.co_varnames[: fn.__code__.co_argcount]
    return FunctionSpec(fn, arg_names, defaults)


def fn_arg_tuple(fn_spec: FunctionSpec, args, kwargs):
    """Returns arguments passed to function wrapped in ArgTuple"""

    args_dict = dict(zip(fn_spec.arg_names, args))
    for index in range(len(args_dict), len(args)):
        args_dict[str(index)] = args[index]
    result = {**fn_spec.defaults, **args_dict, **kwargs}
    return arg_tuple.ArgTuple(result)


class StableSet(MutableSet[T]):
    _inner: dict[T, None]

    def __init__(self, /, iterable: Iterable[T] = ()):
        self._inner = dict.fromkeys(iterable)

    def __contains__(self, element: object) -> bool:
        return element in self._inner

    def __iter__(self) -> Iterator[T]:
        return iter(self._inner.keys())

    def __len__(self) -> int:
        return len(self._inner)

    def __repr__(self) -> str:
        type_name = type(self).__name__
        if self._inner:
            values = repr(list(self._inner.keys()))
        else:
            values = ""
        return f"{type_name}({values})"

    def add(self, element: T) -> None:
        self._inner[element] = None

    def discard(self, element: T) -> None:
        self._inner.pop(element, None)

    def copy(self) -> StableSet[T]:
        return StableSet(self)

    def __or__(self, other: Iterable[T2]) -> StableSet[T | T2]:
        return super().__or__(other)  # type: ignore

    def __ior__(self, other: Iterable[T2]) -> StableSet[T | T2]:
        return super().__ior__(other)  # type: ignore

    def update(self, *sets: Iterable[T]) -> None:
        for s in sets:
            self |= s

    def union(*sets: Iterable[T]) -> StableSet[T]:
        res: StableSet[T] = StableSet()
        res.update(*sets)
        return res

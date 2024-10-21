# Copyright © 2024 Pathway

from __future__ import annotations

import abc
import functools
import inspect
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, ClassVar, ParamSpec, TypeVar, overload

import async_lru
import diskcache

from pathway.internals import api, trace
from pathway.internals.runtime_type_check import check_arg_types

T = TypeVar("T")
P = ParamSpec("P")


class CacheStrategy(abc.ABC):
    """Base class used to represent caching strategy."""

    @abc.abstractmethod
    def wrap_async(
        self, func: Callable[P, Awaitable[T]]
    ) -> Callable[P, Awaitable[T]]: ...

    @abc.abstractmethod
    def wrap_sync(self, func: Callable[P, T]) -> Callable[P, T]: ...


class DiskCache(CacheStrategy):
    """On disk cache."""

    _cache: diskcache.Cache
    _name: str | None
    _size_limit: int

    _custom_names: ClassVar[set[str]] = set()

    @trace.trace_user_frame
    def __init__(self, name: str | None = None, size_limit=2**30) -> None:
        """
        Args:
            name: name of the cache. When multiple caches have the same name, they share a storage.
            size_limit: a memory limit of the cache in bytes.
        """
        super().__init__()
        if name is not None:
            if name in self._custom_names:
                raise ValueError(f"cache name `{name}` used more than once")
            self._custom_names.add(name)
        self._name = name
        self._cache = None
        self._size_limit = size_limit

    def make_key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        return str(api.ref_scalar(args, tuple(kwargs.items())))

    def wrap_async(self, func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            cache = self._get_cache(func)
            key = self.make_key(args, kwargs)
            if cache is None:
                return await func(*args, **kwargs)
            if key not in cache:
                result = await func(*args, **kwargs)
                cache[key] = result
            return cache[key]

        return wrapper

    def wrap_sync(self, func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            cache = self._get_cache(func)
            key = self.make_key(args, kwargs)
            if cache is None:
                return func(*args, **kwargs)
            if key not in cache:
                result = func(*args, **kwargs)
                cache[key] = result
            return cache[key]

        return wrapper

    def _get_cache(self, func: Callable) -> diskcache.Cache | None:
        if self._cache is None:
            if self._name is None:
                func = inspect.unwrap(func)
                self._name = f"{func.__module__}_{func.__qualname__}"
            storage_root = os.environ.get("PATHWAY_PERSISTENT_STORAGE")
            if storage_root is None:
                raise RuntimeError(
                    "no persistent storage configured for the disk cache"
                )
            cache_dir = Path(storage_root) / "runtime_calls"
            self._cache = diskcache.Cache(
                cache_dir / self._name, size_limit=self._size_limit
            )
        return self._cache


class DefaultCache(DiskCache):
    """
    The default caching strategy.
    Persistence layer will be used if enabled. Otherwise, cache will be disabled.
    """

    def _get_cache(self, func: Callable) -> diskcache.Cache | None:
        if "PATHWAY_PERSISTENT_STORAGE" not in os.environ:
            return None
        return super()._get_cache(func)


class InMemoryCache(CacheStrategy):
    """In-memory LRU cache. It is not persisted between runs."""

    max_size: int | None

    def __init__(self, max_size: int | None = None) -> None:
        """
        Args:
            max_size: Maximum size of the cache (the number of entries).
            If set to None, it is unlimited.
        """
        self.max_size = max_size

    def wrap_async(self, func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        return async_lru.alru_cache(self.max_size)(func)  # type: ignore[arg-type]

    def wrap_sync(self, func: Callable[P, T]) -> Callable[P, T]:
        return functools.lru_cache(self.max_size)(func)  # type: ignore[return-value]


@overload
def with_cache_strategy(
    func: Callable[P, T], cache_strategy: CacheStrategy
) -> Callable[P, T]: ...


@overload
def with_cache_strategy(
    func: Callable[P, Awaitable[T]], cache_strategy: CacheStrategy
) -> Callable[P, Awaitable[T]]: ...


@check_arg_types
def with_cache_strategy(
    func: Callable[P, Awaitable[T]] | Callable[P, T], cache_strategy: CacheStrategy
):
    """
    Returns a function with applied cache strategy.

    Args:
        cache_strategy: Defines the caching mechanism.
    Returns:
        Callable/Coroutine
    """

    if inspect.iscoroutinefunction(func):
        return cache_strategy.wrap_async(func)
    else:
        return cache_strategy.wrap_sync(func)

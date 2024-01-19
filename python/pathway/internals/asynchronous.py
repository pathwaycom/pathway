# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import functools
import inspect
import os
import random
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import ClassVar

import diskcache

from pathway.internals import trace
from pathway.internals.runtime_type_check import check_arg_types


@check_arg_types
def with_capacity(func: Callable, capacity: int):
    """
    Limits the number of simultaneous calls of the specified function.
    Regular function will be wrapper to run in async executor.

    Args:
        capacity: Maximum number of concurrent operations.
    Returns:
        Coroutine
    """

    func = coerce_async(func)

    semaphore = asyncio.Semaphore(capacity)

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        async with semaphore:
            return await func(*args, **kwargs)

    return wrapper


@check_arg_types
def with_retry_strategy(func: Callable, retry_strategy: AsyncRetryStrategy) -> Callable:
    """
    Returns an asynchronous function with applied retry strategy.
    Regular function will be wrapper to run in async executor.

    Args:
        retry_strategy: Defines how failures will be handled.
    Returns:
        Coroutine
    """

    func = coerce_async(func)

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await retry_strategy.invoke(func, *args, **kwargs)

    return wrapper


@check_arg_types
def with_cache_strategy(func, cache_strategy: CacheStrategy) -> Callable:
    """
    Returns an asynchronous function with applied cache strategy.
    Regular function will be wrapper to run in async executor.

    Args:
        cache_strategy: Defines the caching mechanism.
    Returns:
        Coroutine
    """
    func = coerce_async(func)

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await cache_strategy.invoke(func, *args, **kwargs)

    return wrapper


def async_options(
    capacity: int | None = None,
    retry_strategy: AsyncRetryStrategy | None = None,
    cache_strategy: CacheStrategy | None = None,
) -> Callable:
    """
    Decorator applying async options to a provided function.
    Regular function will be wrapper to run in async executor.

    Args:
        capacity: Maximum number of concurrent operations.
            Defaults to None, indicating no specific limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None.
        cache_strategy: Defines the caching mechanism. If set to None
            and a persistency is enabled, operations will be cached using the
            persistence layer. Defaults to None.
    Returns:
        Coroutine
    """

    def decorator(func):
        if retry_strategy is not None:
            func = with_retry_strategy(func, retry_strategy)
        if capacity is not None:
            func = with_capacity(func, capacity)
        if cache_strategy is not None:
            func = with_cache_strategy(func, cache_strategy)

        return func

    return decorator


@check_arg_types
def coerce_async(func: Callable) -> Callable:
    """
    Wraps a regular function to be executed in async executor.
    It acts as a noop if the provided function is already a coroutine.
    """

    if asyncio.iscoroutinefunction(func):
        return func
    else:

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            event_loop = asyncio.get_event_loop()
            assert event_loop.is_running(), "event loop should be running"
            pfunc = functools.partial(func, *args, **kwargs)
            return await event_loop.run_in_executor(None, func=pfunc)

        return wrapper


class AsyncRetryStrategy(ABC):
    """Class representing strategy of delays or backoffs for the retries."""

    @abstractmethod
    async def invoke(self, func: Callable, /, *args, **kwargs):
        ...


class NoRetryStrategy(AsyncRetryStrategy):
    async def invoke(self, func: Callable, /, *args, **kwargs):
        return await func(*args, **kwargs)


class ExponentialBackoffRetryStrategy(AsyncRetryStrategy):
    """Retry strategy with exponential backoff with jitter and maximum retries."""

    _max_retries: int
    _initial_delay: float
    _backoff_factor: float
    _jitter: float

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: int = 1_000,
        backoff_factor: float = 2,
        jitter_ms: int = 300,
    ) -> None:
        """
        Args:
            max_retries: Maximum number of retries. As int.
            initial_delay: First delay in milliseconds.
            backoff_factor: Factor by which the delay between retries increases exponentially. Set as float.
            jitter_ms: Maximum random jitter added to the delay between retries in milliseconds.
        Returns:
            None"""
        self._initial_delay = initial_delay / 1_000
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._jitter = jitter_ms / 1_000

    async def invoke(self, func: Callable, /, *args, **kwargs):
        delay = self._initial_delay

        for n_attempt in range(0, self._max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception:
                if n_attempt == self._max_retries:
                    raise
            await asyncio.sleep(delay)
            delay = self._next_delay(delay)

    def _next_delay(self, current_delay: float) -> float:
        current_delay *= self._backoff_factor
        current_delay += random.random() * self._jitter
        return current_delay


class FixedDelayRetryStrategy(ExponentialBackoffRetryStrategy):
    """Retry strategy with fixed delay and maximum retries."""

    def __init__(self, max_retries: int = 3, delay_ms: int = 1000) -> None:
        super().__init__(
            max_retries=max_retries,
            initial_delay=delay_ms,
            backoff_factor=1,
            jitter_ms=0,
        )


class CacheStrategy(ABC):
    """Base class used to represent caching strategy."""

    @abstractmethod
    async def invoke(self, func: Callable, /, *args, **kwargs):
        ...


class DiskCache(CacheStrategy):
    _cache: diskcache.Cache
    _name: str | None

    _custom_names: ClassVar[set[str]] = set()

    @trace.trace_user_frame
    def __init__(self, name: str | None = None) -> None:
        super().__init__()
        if name is not None:
            if name in self._custom_names:
                raise ValueError(f"cache name `{name}` used more than once")
            self._custom_names.add(name)
        self._name = name
        self._cache = None

    async def invoke(self, func: Callable, /, *args, **kwargs):
        cache = self._get_cache(func)
        key = str((args, kwargs))
        if cache is None:
            return await func(*args, **kwargs)
        if key not in cache:
            result = await func(*args, **kwargs)
            cache[key] = result
        return cache[key]

    def _get_cache(self, func):
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
            self._cache = diskcache.Cache(cache_dir / self._name)
        return self._cache


class DefaultCache(DiskCache):
    """
    The default caching strategy.
    Persistence layer will be used if enabled. Otherwise, cache will be disabled.
    """

    def _get_cache(self, func):
        if "PATHWAY_PERSISTENT_STORAGE" not in os.environ:
            return None
        return super()._get_cache(func)

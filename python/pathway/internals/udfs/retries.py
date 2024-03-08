# Copyright © 2024 Pathway

from __future__ import annotations

import abc
import asyncio
import functools
import random
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.udfs.utils import coerce_async

T = TypeVar("T")
P = ParamSpec("P")


@check_arg_types
def with_retry_strategy(
    func: Callable[P, Awaitable[T]], retry_strategy: AsyncRetryStrategy
) -> Callable[P, Awaitable[T]]:
    """
    Returns an asynchronous function with applied retry strategy.
    Regular function will be wrapped to run in async executor.

    Args:
        retry_strategy: Defines how failures will be handled.
    Returns:
        Coroutine
    """

    func = coerce_async(func)

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        return await retry_strategy.invoke(func, *args, **kwargs)

    return wrapper


class AsyncRetryStrategy(abc.ABC):
    """Class representing strategy of delays or backoffs for the retries."""

    @abc.abstractmethod
    async def invoke(
        self, func: Callable[P, Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> T: ...


class NoRetryStrategy(AsyncRetryStrategy):
    async def invoke(
        self, func: Callable[P, Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> T:
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

    async def invoke(
        self, func: Callable[P, Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> T:
        delay = self._initial_delay

        for n_attempt in range(0, self._max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception:
                if n_attempt == self._max_retries:
                    raise
            await asyncio.sleep(delay)
            delay = self._next_delay(delay)
        raise ValueError(f"incorrect max_retries: {self._max_retries}")

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

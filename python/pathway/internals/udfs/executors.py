# Copyright © 2024 Pathway

from __future__ import annotations

import abc
import asyncio
import functools
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar

import pathway.internals.expression as expr
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.udfs.caches import CacheStrategy, with_cache_strategy
from pathway.internals.udfs.retries import AsyncRetryStrategy, with_retry_strategy
from pathway.internals.udfs.utils import coerce_async


class Executor(abc.ABC):
    """
    Base class executors of Pathway UDFs (user-defined functions).
    """

    ...

    @abc.abstractmethod
    def _wrap(self, fun: Callable) -> Callable: ...

    @property
    @abc.abstractmethod
    def _apply_expression_type(self) -> type[expr.ApplyExpression]: ...

    def additional_expression_args(self) -> dict[str, Any]:
        return {}


@dataclass
class AutoExecutor(Executor):
    def _wrap(self, fun: Callable) -> Callable:
        raise ValueError("You can't wrap a function using AutoExecutor.")

    @property
    def _apply_expression_type(self) -> type[expr.ApplyExpression]:
        raise ValueError("AutoExecutor has no apply expression type.")


def auto_executor() -> Executor:
    """
    Returns the automatic executor of Pathway UDF. It deduces whether the execution
    should be synchronous or asynchronous from the function signature. If the function
    is a coroutine, then the execution is asynchronous. Otherwise, it is synchronous.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> import time
    >>> t = pw.debug.table_from_markdown(
    ...     '''
    ...     a | b
    ...     1 | 2
    ...     3 | 4
    ...     5 | 6
    ... '''
    ... )
    >>>
    >>> @pw.udf(executor=pw.udfs.auto_executor())
    ... def mul(a: int, b: int) -> int:
    ...     return a * b
    ...
    >>> result_1 = t.select(res=mul(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(result_1, include_id=False)
    res
    2
    12
    30
    >>>
    >>> @pw.udf(executor=pw.udfs.auto_executor())
    ... async def long_running_async_function(a: int, b: int) -> int:
    ...     await asyncio.sleep(0.1)
    ...     return a * b
    ...
    >>> result_2 = t.select(res=long_running_async_function(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(result_2, include_id=False)
    res
    2
    12
    30
    """
    return AutoExecutor()


@dataclass
class SyncExecutor(Executor):
    def _wrap(self, fun: Callable) -> Callable:
        return fun

    @property
    def _apply_expression_type(self) -> type[expr.ApplyExpression]:
        return expr.ApplyExpression


def sync_executor() -> Executor:
    """
    Returns the synchronous executor for Pathway UDFs.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ...     '''
    ...     a | b
    ...     1 | 2
    ...     3 | 4
    ...     5 | 6
    ... '''
    ... )
    >>>
    >>> @pw.udf(executor=pw.udfs.sync_executor())
    ... def mul(a: int, b: int) -> int:
    ...     return a * b
    ...
    >>> result = t.select(res=mul(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(result, include_id=False)
    res
    2
    12
    30
    """
    return SyncExecutor()


@dataclass(frozen=True, kw_only=True)
class AsyncExecutor(Executor):
    capacity: int | None = None
    timeout: float | None = None
    retry_strategy: AsyncRetryStrategy | None = None

    def _wrap(self, fun: Callable) -> Callable:
        return async_options(
            capacity=self.capacity,
            timeout=self.timeout,
            retry_strategy=self.retry_strategy,
        )(fun)

    @property
    def _apply_expression_type(self) -> type[expr.ApplyExpression]:
        return expr.AsyncApplyExpression


def async_executor(
    *,
    capacity: int | None = None,
    timeout: float | None = None,
    retry_strategy: AsyncRetryStrategy | None = None,
) -> Executor:
    """
    Returns the asynchronous executor for Pathway UDFs.

    Can be applied to a regular or an asynchronous function. If applied to a regular
    function, it is executed in ``asyncio`` loop's ``run_in_executor``.

    The asynchronous UDFs are asynchronous *within a single batch* with batch defined as
    all entries with equal processing times assigned. The UDFs are started for all entries
    in the batch and the execution of further batches is blocked until all UDFs
    for a given batch have finished.

    Args:
        capacity: Maximum number of concurrent operations allowed.
            Defaults to None, indicating no specific limit.
        timeout: Maximum time (in seconds) to wait for the function result. When both
            ``timeout`` and ``retry_strategy`` are used, timeout applies to a single retry.
            Defaults to None, indicating no time limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None, meaning no retries.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> import time
    >>> t = pw.debug.table_from_markdown(
    ...     '''
    ...     a | b
    ...     1 | 2
    ...     3 | 4
    ...     5 | 6
    ... '''
    ... )
    >>>
    >>> @pw.udf(
    ...     executor=pw.udfs.async_executor(
    ...         capacity=2, retry_strategy=pw.udfs.ExponentialBackoffRetryStrategy()
    ...     )
    ... )
    ... async def long_running_async_function(a: int, b: int) -> int:
    ...     await asyncio.sleep(0.1)
    ...     return a * b
    ...
    >>> result_1 = t.select(res=long_running_async_function(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(result_1, include_id=False)
    res
    2
    12
    30
    >>>
    >>> @pw.udf(executor=pw.udfs.async_executor())
    ... def long_running_function(a: int, b: int) -> int:
    ...     time.sleep(0.1)
    ...     return a * b
    ...
    >>> result_2 = t.select(res=long_running_function(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(result_2, include_id=False)
    res
    2
    12
    30
    """
    return AsyncExecutor(
        capacity=capacity, timeout=timeout, retry_strategy=retry_strategy
    )


@dataclass(frozen=True, kw_only=True)
class FullyAsyncExecutor(AsyncExecutor):
    autocommit_duration_ms: int | None

    @property
    def _apply_expression_type(self) -> type[expr.ApplyExpression]:
        return expr.FullyAsyncApplyExpression

    def additional_expression_args(self) -> dict[str, Any]:
        return dict(autocommit_duration_ms=self.autocommit_duration_ms)


def fully_async_executor(
    *,
    capacity: int | None = None,
    timeout: float | None = None,
    retry_strategy: AsyncRetryStrategy | None = None,
    autocommit_duration_ms: int | None = 1500,
) -> Executor:
    """
    Returns the fully asynchronous executor for Pathway UDFs.

    Can be applied to a regular or an asynchronous function. If applied to a regular
    function, it is executed in ``asyncio`` loop's ``run_in_executor``.

    In contrast to regular asynchronous UDFs, these UDFs are fully asynchronous.
    It means that computations from the next batch can start even if the previous batch hasn't
    finished yet. When a UDF is started, instead of a result, a special ``Pending`` value
    is emitted. When the function finishes, an update with the true return value is produced.

    Using fully asynchronous UDFs allows processing time to advance even if the function
    doesn't return. As a result downstream computations are not blocked.

    The data type of column returned from the fully async UDF is ``Future[return_type]`` to
    allow for ``Pending`` values. Columns of this type can be propagated further, but can't
    be used in most expressions (e.g. arithmetic operations). They can be passed to the next
    fully async UDF though. To strip the ``Future`` wrapper and wait for the result, you can
    use :py:meth:`pathway.Table.await_futures` method on :py:class:`pathway.Table`. In practice,
    it filters out the ``Pending`` values and produces a column with the data type as returned
    by the fully async UDF.

    Args:
        capacity: Maximum number of concurrent operations allowed.
            Defaults to None, indicating no specific limit.
        timeout: Maximum time (in seconds) to wait for the function result. When both
            ``timeout`` and ``retry_strategy`` are used, timeout applies to a single retry.
            Defaults to None, indicating no time limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None, meaning no retries.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>>
    >>> t = pw.debug.table_from_markdown(
    ...     '''
    ...     a | b | __time__
    ...     1 | 2 |     2
    ...     3 | 4 |     4
    ...     5 | 6 |     4
    ... '''
    ... )
    >>>
    >>> @pw.udf(executor=pw.udfs.fully_async_executor())
    ... async def long_running_async_function(a: int, b: int) -> int:
    ...     c = a * b
    ...     await asyncio.sleep(0.1 * c)
    ...     return c
    ...
    >>>
    >>> result = t.with_columns(res=long_running_async_function(pw.this.a, pw.this.b))
    >>> pw.debug.compute_and_print(result, include_id=False)
    a | b | res
    1 | 2 | 2
    3 | 4 | 12
    5 | 6 | 30
    >>>
    >>> pw.debug.compute_and_print_update_stream(result, include_id=False) # doctest: +SKIP
    a | b | res     | __time__      | __diff__
    1 | 2 | Pending | 2             | 1
    3 | 4 | Pending | 4             | 1
    5 | 6 | Pending | 4             | 1
    1 | 2 | Pending | 1739290145300 | -1
    1 | 2 | 2       | 1739290145300 | 1
    3 | 4 | Pending | 1739290146300 | -1
    3 | 4 | 12      | 1739290146300 | 1
    5 | 6 | Pending | 1739290148100 | -1
    5 | 6 | 30      | 1739290148100 | 1
    """
    return FullyAsyncExecutor(
        capacity=capacity,
        timeout=timeout,
        retry_strategy=retry_strategy,
        autocommit_duration_ms=autocommit_duration_ms,
    )


T = TypeVar("T")
P = ParamSpec("P")


@check_arg_types
def with_capacity(
    func: Callable[P, Awaitable[T]], capacity: int
) -> Callable[P, Awaitable[T]]:
    """
    Limits the number of simultaneous calls of the specified function.
    Regular function will be wrapped to run in async executor.

    Args:
        capacity: Maximum number of concurrent operations.
    Returns:
        Coroutine
    """

    func = coerce_async(func)

    semaphore = asyncio.Semaphore(capacity)

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async with semaphore:
            return await func(*args, **kwargs)

    return wrapper


@check_arg_types
def with_timeout(
    func: Callable[P, Awaitable[T]], timeout: float
) -> Callable[P, Awaitable[T]]:
    """
    Limits the time spent waiting on the result of the function.
    If the time limit is exceeded, the task is canceled and an Error is raised.
    Regular function will be wrapped to run in async executor.

    Args:
        timeout: Maximum time (in seconds) to wait for the function result.
            Defaults to None, indicating no time limit.
    Returns:
        Coroutine
    """

    func = coerce_async(func)

    if sys.version_info < (3, 11):

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)

    else:

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            async with asyncio.timeout(timeout):
                return await func(*args, **kwargs)

    return wrapper


def async_options(
    capacity: int | None = None,
    timeout: float | None = None,
    retry_strategy: AsyncRetryStrategy | None = None,
    cache_strategy: CacheStrategy | None = None,
) -> Callable:
    """
    Decorator applying async options to a provided function.
    Regular function will be wrapped to run in async executor.

    Args:
        capacity: Maximum number of concurrent operations.
            Defaults to None, indicating no specific limit.
        timeout: Maximum time (in seconds) to wait for the function result. When both
            ``timeout`` and ``retry_strategy`` are used, timeout applies to a single retry.
            Defaults to None, indicating no time limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None, meaning no retries.
        cache_strategy: Defines the caching mechanism. If set to None
            and a persistency is enabled, operations will be cached using the
            persistence layer. Defaults to None.
    Returns:
        Coroutine
    """

    def decorator(
        f: Callable[P, T] | Callable[P, Awaitable[T]]
    ) -> Callable[P, Awaitable[T]]:
        func = coerce_async(f)
        if timeout is not None:
            func = with_timeout(func, timeout)
        if retry_strategy is not None:
            func = with_retry_strategy(func, retry_strategy)
        if capacity is not None:
            func = with_capacity(func, capacity)
        if cache_strategy is not None:
            func = with_cache_strategy(func, cache_strategy)

        return func

    return decorator

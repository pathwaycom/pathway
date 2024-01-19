# Copyright © 2024 Pathway

from __future__ import annotations

import abc
import functools
from collections.abc import Callable
from typing import overload

from pathway.internals import asynchronous, common

__all__ = ["udf", "udf_async", "UDF", "UDFSync", "UDFAsync"]


class UDF(abc.ABC):
    """
    Base class for Pathway UDF (user defined functions).

    Please use the wrappers `udf` and `udf_async` to create UDFs out of Python functions.
    Please subclass `UDFSync` and `UDFAsync` to define UDF's using python classes.
    """

    __wrapped__: Callable

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        ...


class UDFSync(UDF):
    """
    UDF's that are executed as regular python functions.

    To implement your own UDF as a class please implement the `__wrapped__` function.
    """

    __wrapped__: Callable

    def __init__(self) -> None:
        super().__init__()

    def __call__(self, *args, **kwargs):
        return common.apply(self.__wrapped__, *args, **kwargs)


class UDFSyncFunction(UDFSync):
    """Create a Python UDF (universal data function) out of a callable.

    The output type of the UDF is determined based on its type annotation.

    Example:

    >>> import pathway as pw
    >>> @pw.udf
    ... def concat(left: str, right: str) -> str:
    ...     return left+right
    ...
    >>> t1 = pw.debug.table_from_markdown('''
    ... age  owner  pet
    ...     10  Alice  dog
    ...     9    Bob  dog
    ...     8  Alice  cat
    ...     7    Bob  dog''')
    >>> t2 = t1.select(col = concat(t1.owner, t1.pet))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    Alicecat
    Alicedog
    Bobdog
    Bobdog
    """

    def __init__(self, func: Callable):
        super().__init__()
        # this sets __wrapped__
        functools.update_wrapper(self, func)


# define alias, for analogy with the AsyncUDF class and decorator
udf = UDFSyncFunction


class UDFAsync(UDF):
    """
    UDF's that are executed as python async functions.

    To implement your own UDF as a class please implement the `__wrapped__` async function.
    """

    __wrapped__: Callable
    capacity: int | None
    retry_strategy: asynchronous.AsyncRetryStrategy | None
    cache_strategy: asynchronous.CacheStrategy | None

    def __init__(
        self,
        *,
        capacity: int | None = None,
        retry_strategy: asynchronous.AsyncRetryStrategy | None = None,
        cache_strategy: asynchronous.CacheStrategy | None = None,
    ) -> None:
        """Init UDFAsync.

        Args:
            capacity: Maximum number of concurrent operations allowed.
                Defaults to None, indicating no specific limit.
            retry_strategy: Strategy for handling retries in case of failures.
                Defaults to None.
            cache_strategy: Defines the caching mechanism. If set to None and a persistency
                is enabled, operations will be cached using the persistence layer.
                Defaults to None.
        """

        super().__init__()
        if cache_strategy is None:
            cache_strategy = asynchronous.DefaultCache()
        self.capacity = capacity
        self.retry_strategy = retry_strategy
        self.cache_strategy = cache_strategy

    def __call__(self, *args, **kwargs):
        func = asynchronous.async_options(
            capacity=self.capacity,
            retry_strategy=self.retry_strategy,
            cache_strategy=self.cache_strategy,
        )(self.__wrapped__)

        return common.apply_async(func, *args, **kwargs)


class UDFAsyncFunction(UDFAsync):
    """
    UDF's that wrap Python's async functions.
    """

    def __init__(self, func: Callable, **kwargs):
        super().__init__(**kwargs)

        # this sets __wrapped__
        functools.update_wrapper(self, func)


@overload
def udf_async(fun: Callable) -> Callable:
    ...


@overload
def udf_async(
    *,
    capacity: int | None = None,
    retry_strategy: asynchronous.AsyncRetryStrategy | None = None,
    cache_strategy: asynchronous.CacheStrategy | None = None,
) -> Callable[[Callable], Callable]:
    ...


def udf_async(
    fun: Callable | None = None,
    *,
    capacity: int | None = None,
    retry_strategy: asynchronous.AsyncRetryStrategy | None = None,
    cache_strategy: asynchronous.CacheStrategy | None = None,
):
    r"""Create a Python asynchronous UDF (universal data function) out of a callable.

    Output column type deduced from type-annotations of a function.
    Can be applied to a regular or asynchronous function.

    Args:
        capacity: Maximum number of concurrent operations allowed.
            Defaults to None, indicating no specific limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None.
        cache_strategy: Defines the caching mechanism. If set to None and a persistency
            is enabled, operations will be cached using the persistence layer.
            Defaults to None.
    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> @pw.udf_async
    ... async def concat(left: str, right: str) -> str:
    ...   await asyncio.sleep(0.1)
    ...   return left+right
    >>> t1 = pw.debug.table_from_markdown('''
    ... age  owner  pet
    ...  10  Alice  dog
    ...   9    Bob  dog
    ...   8  Alice  cat
    ...   7    Bob  dog''')
    >>> t2 = t1.select(col = concat(t1.owner, t1.pet))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    col
    Alicecat
    Alicedog
    Bobdog
    Bobdog
    """

    def decorator(fun: Callable) -> Callable:
        return UDFAsyncFunction(
            fun,
            capacity=capacity,
            retry_strategy=retry_strategy,
            cache_strategy=cache_strategy,
        )

    if fun is None:
        return decorator
    else:
        if not callable(fun):
            raise TypeError("udf_async should be used with keyword arguments only")

        return decorator(fun)

# Copyright © 2024 Pathway
"""Methods and classes for controlling the behavior of UDFs (User-Defined Functions) in Pathway.

Typical use:

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
from pathway.internals.udfs import (
    UDF,
    AsyncRetryStrategy,
    CacheStrategy,
    DefaultCache,
    DiskCache,
    ExponentialBackoffRetryStrategy,
    FixedDelayRetryStrategy,
    InMemoryCache,
    NoRetryStrategy,
    async_executor,
    async_options,
    auto_executor,
    coerce_async,
    fully_async_executor,
    sync_executor,
    udf,
    with_cache_strategy,
    with_capacity,
    with_retry_strategy,
    with_timeout,
)

__all__ = [
    "auto_executor",
    "async_executor",
    "sync_executor",
    "fully_async_executor",
    "CacheStrategy",
    "DefaultCache",
    "DiskCache",
    "InMemoryCache",
    "AsyncRetryStrategy",
    "ExponentialBackoffRetryStrategy",
    "FixedDelayRetryStrategy",
    "NoRetryStrategy",
    "async_options",
    "coerce_async",
    "with_cache_strategy",
    "with_capacity",
    "with_retry_strategy",
    "with_timeout",
    "udf",
    "UDF",
]

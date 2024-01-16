# Copyright © 2024 Pathway
"""
Helper methods and classes used along with :py:func:`~pathway.udf_async` and :py:func:`~pathway.AsyncTransformer`.

Typical use:

>>> import pathway as pw
>>> import asyncio
>>> @pw.udf_async(retry_strategy=pw.asynchronous.FixedDelayRetryStrategy(max_retries=5))
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

from pathway.internals.asynchronous import (
    AsyncRetryStrategy,
    CacheStrategy,
    DefaultCache,
    ExponentialBackoffRetryStrategy,
    FixedDelayRetryStrategy,
    NoRetryStrategy,
    async_options,
    coerce_async,
    with_cache_strategy,
    with_capacity,
    with_retry_strategy,
)

__all__ = [
    "with_capacity",
    "with_retry_strategy",
    "with_cache_strategy",
    "async_options",
    "coerce_async",
    "AsyncRetryStrategy",
    "NoRetryStrategy",
    "ExponentialBackoffRetryStrategy",
    "FixedDelayRetryStrategy",
    "CacheStrategy",
    "DefaultCache",
]

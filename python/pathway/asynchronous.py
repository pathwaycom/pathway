# Copyright © 2024 Pathway
"""
This module is DEPRECATED. Its content has been moved to `pathway.udfs </developers/api-docs/udfs/>`_ .

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

from warnings import warn

from pathway.internals import udfs


def __getattr__(name):
    warn(
        "pathway.asynchronous module is deprecated. Its content has been moved to pathway.udfs.",
        DeprecationWarning,
        stacklevel=2,
    )
    try:
        return getattr(udfs, name)
    except AttributeError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

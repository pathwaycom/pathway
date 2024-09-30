# ---
# title: User-defined Functions
# description: An article exploring concepts related to user defined functions in Pathway.
# date: '2024-02-21'
# thumbnail: ''
# tags: ['tutorial', 'engineering']
# keywords: ['python', 'udf', 'function', 'apply', 'transformation', 'cache', 'timeout']
# notebook_export_path: notebooks/tutorials/udf.ipynb
# ---

# %% [markdown]
# # User-defined Functions (UDFs) in Pathway
# Pathway supports a wide range of expressions that allow you to operate on individual rows. <!-- TODO: link article when done -->
# However, not all operations can be expressed that way.
# To address this problem, Pathway allows you to write a user-defined function (UDF) in Python.
# Such function is then applied to each row of your data individually in the same way as the predefined expressions.
# UDFs can be customized in various ways and all of them are explored in this guide.


# ## Simple UDFs
# In the beginning, let's consider a simple case. You want to write a function that increments a number by 1.
# Just write a regular function and decorate it with [`pw.udf`](/developers/api-docs/pathway#pathway.udf).

# %%
import pathway as pw


@pw.udf
def inc(x: int) -> int:
    return x + 1


# %% [markdown]
# and that's everything you need.
# Now you can use it as an ordinary Pathway expression, as in the example shown below.
# %%
table = pw.debug.table_from_markdown(
    """
    value
      1
      2
     13
"""
)

result = table.with_columns(value_inc=inc(table.value))
pw.debug.compute_and_print(result)

# %% [markdown]
# It works! The printed table contains two columns - `value` and `value_inc` with the result of `inc` function.
#
# Note that we annotated the return type of `inc` (`int` in this case).
# It is important information for pathway as it is used to infer the type of `value_inc` column.
# Let's make sure that the return type is correct by printing the schema of the `result` table.

# %%
print(result.schema)


# %% [markdown]
# It is correct. The return type can also be set in a decorator:
# %%
@pw.udf(return_type=int)
def inc_2(x: int):
    return x + 1


result_2 = table.with_columns(value_inc=inc_2(table.value))
pw.debug.compute_and_print(result_2)
print(result_2.schema)


# %% [markdown]
# In this case, it is also set correctly.
# If a UDF is not annotated and the `return_type` is not set, Pathway can't determine the return type of the column and sets it as `Any`.
# It is an undesirable situation as many expressions can't be called on columns of type `Any`.
# For example, you can't add a column with type `Any` to a column of type `int` (you also can't add `Any` to `Any`), but you can add a column of type `int` to a column of type `int`.
# %%
@pw.udf
def inc_3(x: int):
    return x + 1


result_3 = table.with_columns(value_inc=inc_3(table.value))
pw.debug.compute_and_print(result_3)
print(result_3.schema)

# %% [markdown]
# As you can see, this time the type of `value_inc` column is `Any`.

# %% [markdown]
# Python functions can also be called on data by using [`pw.apply`](/developers/api-docs/pathway#pathway.apply)/[`pw.apply_with_type`](/developers/api-docs/pathway#pathway.apply_with_type) functions.

# %%
result_4 = table.with_columns(
    value_inc=pw.apply_with_type(lambda x: x + 1, int, table.value)
)
pw.debug.compute_and_print(result_4)
print(result_4.schema)

# %% [markdown]
# *Remark:* to keep the examples as simple as possible, the code pieces in this guide use `table_from_markdown` to define the example tables and `compute_and_print` to run the computations.
# Those functions use Pathway in the static mode.
# However, Pathway is a streaming data processing system and can work on dynamically changing data.
# See [Pathway modes](/developers/user-guide/introduction/streaming-and-static-modes/) for more info on this topic.
#
# Also note that the `inc` function is only present in this guide for demonstration purposes.
# It is possible to get the same result using Pathway native operations and this is the recommended way as then the computations are performed in Rust, not Python.
#
# The UDFs are useful for more complicated solutions that cannot be fully expressed in Pathway but the functions in the guide are kept simple to focus on UDFs usage and configuration options.

# %%
result_5 = table.with_columns(value_inc=table.value + 1)
pw.debug.compute_and_print(result_5)

# %% [markdown]
# ## Calling library functions
# The UDF mechanism allows you to also call external functions. You can, for example, use [`scipy`](https://scipy.org/) to compute quantiles of the normal distribution.

# %%
from scipy.stats import norm

table = pw.debug.table_from_markdown(
    """
    q
    0.02
    0.5
    0.84
    0.99
"""
)
quantiles = table.with_columns(value=pw.apply_with_type(norm.ppf, float, table.q))
pw.debug.compute_and_print(quantiles)

# %% [markdown]
# Using [`norm.ppf`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.norm.html) is convenient because it is a function, not a method.
# To use an external method it is better to create a wrapper.
# Below you can find an example of such a wrapper that extracts the [`pandas.Timestamp.is_leap_year`](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.is_leap_year.html) property and puts its value to the column `leap_year`.

# %%
import pandas as pd

table = pw.debug.table_from_markdown(
    """
    date_str
    2023-01-01T12:00:00
    2024-01-01T12:00:00
    2025-01-01T12:00:00
"""
).select(date_time=pw.this.date_str.dt.strptime("%Y-%m-%dT%H:%M:%S"))


@pw.udf
def is_leap_year(date_time: pd.Timestamp) -> bool:
    return date_time.is_leap_year


result = table.with_columns(leap_year=is_leap_year(pw.this.date_time))
pw.debug.compute_and_print(result)

# %% [markdown]
# ## Propagating Nones
# By default, Pathway UDFs are called on all rows, however it may not always be desired.
# In particular, if you have a function that requires values to be non-optional but your data has some missing entries, you may want to return `None` immediately instead of calling a function.
# In Pathway, you can enable such mechanism with the `propagate_none` parameter of `pw.udf`.
# By default, it is set to `False`. Setting it  to `True` makes Pathway to look at the inputs of the UDF, and if at least one of the arguments is `None`, then the function is not called, and `None` is returned instead.

# %%
table = pw.debug.table_from_markdown(
    """
     a |  b
    12 |  3
     3 |
    23 | 42
       | 12
"""
)


@pw.udf(propagate_none=True)
def mul(a: int, b: int) -> int:
    return a * b


result = table.with_columns(c=mul(pw.this.a, pw.this.b))
pw.debug.compute_and_print(result)

# %% [markdown]
# If `propagate_none` was not set, the `mul` function would error.

# %% [markdown]
# ## Determinism
# Pathway assumes that a UDF is not deterministic unless told otherwise.
# In this context, being deterministic means that the function always returns the same value for the same arguments.
# Pathway requires this information for consistency reasons.
# If you're sure that your function is deterministic, you can set `deterministic=True` as it usually improves the speed and memory requirements of the computation.
# However sometimes the function may be nondeterministic in a non-obvious way. For example, some linear algebra operations on floating point numbers that use multithreading under the hood can return slightly different results across runs. Such functions are not deterministic.
# If this explanation is enough for you, feel free to skip to the next section.
# If you want to learn more about how Pathway handles non-deterministic functions, dive in.
#
# To maintain consistency, it'll memoize the result of a UDF call until the corresponding input row is deleted.
# Being able to produce deleting rows is the reason why Pathway has to store the results of UDFs.
# The values in the inserted and deleted entries have to be the same so that they can cancel out.
# If a UDF is non-deterministic, it can produce a different value and the entries can't cancel out as they are not equal.
# To get the same values at row deletion as at insertion, the results have to be remembered.
# When the row is deleted, there is no more need to remember the result of the call and it can be discarded.
#
# When you're sure that the function is deterministic, you can avoid storing the results by setting `deterministic=True` in the `pw.udf` decorator.
# It'll usually improve the speed and memory requirements of the computation (especially for fast functions).
# It is recommended to set it always when the function is deterministic.
#
# If the function is slow, setting `deterministic=False` might result in a faster execution, but it's not recommended if the function is deterministic. It's better to use [caching](#caching).
# Caching can help with slow functions even if you call the function with each argument only once.
# It because Pathway has to evaluate the function also on deletion and when it is cached, the value can be taken from cache instead of evaluating it.
#
# Let's see the effects of `deterministic` parameter in practice. To do that, let's simulate a stream.
# It contains special columns: `id` that sets the id of the row (a deletion has to have the same `id` as the insertion it removes),
# `__time__` that simulates the arrival time to the engine and `__diff__` that tells whether the entry is an insertion ($1$) or deletion ($-1$).
# At time $2$ two rows are inserted.
# At time $4$ one row is upserted (with an upsert represented as a deletion and insertion with the same time and id).

# %%
table = pw.debug.table_from_markdown(
    """
    id | a | b | __time__ | __diff__
     1 | 2 | 3 |     2    |     1
     2 | 4 | 1 |     2    |     1
     1 | 2 | 3 |     4    |    -1
     1 | 3 | 3 |     4    |     1
"""
)
# %% [markdown]
# You apply a UDF with default parameters (`deterministic=False`) first.


# %%
@pw.udf
def add(a: int, b: int) -> int:
    print(f"add({a}, {b})")
    return a + b


result_default = table.select(pw.this.a, pw.this.b, c=add(pw.this.a, pw.this.b))
pw.debug.compute_and_print(result_default)

# %% [markdown]
# As you can see from the printed messages, the function is called three times.
# It is because the function was not called on deletion.
#
# This time, let's tell Pathway that the function is deterministic.


# %%
@pw.udf(deterministic=True)
def add_deterministic(a: int, b: int) -> int:
    print(f"add_deterministic({a}, {b})")
    return a + b


result_default = table.select(
    pw.this.a, pw.this.b, c=add_deterministic(pw.this.a, pw.this.b)
)
pw.debug.compute_and_print(result_default)

# %% [markdown]
# This time, the function was called four times (once for each entry) and there was no need to remember anything!
# The function is truly deterministic and the result is consistent.

# %% [markdown]
# ## UDFs should not be used for side effects
# UDFs are Python functions so you can capture non-local variables and modify them inside the functions.
# From the UDF it is also possible to call external services and modify their state.
# This is, however, strongly discouraged.
# There's no guarantee that Pathway will run a UDF exactly once for each row.
# If the function is non-deterministic it might not always be called (see above).
# Also if caching is set, the functions will be called less frequently.
#
# Note that we sometimes produce side effects in this tutorial by using the `print` function.
# However, we use it to show the behavior of the system, not to use the printed messages in some other computation.
#
# If you want to produce side effects, [`pw.io.subscribe`](/developers/api-docs/pathway-io#pathway.io.subscribe) should be used instead.

# %% [markdown]
# ## Caching
# If the function you call is expensive and you call it frequently, you may want to cache its results.
# To do this, you can set `cache_strategy` in `pw.udf` decorator. Currently, the supported caching strategies are [`DiskCache`](/developers/api-docs/udfs#pathway.udfs.DiskCache) and [`InMemoryCache`](/developers/api-docs/udfs#pathway.udfs.InMemoryCache).
# The `DiskCache` requires the persistence to be enabled. It caches the results in the persistent storage.
# As a consequence, the results can be reused after the program restart.
# The `InMemoryStorage` caches results in memory.
# As a result, it does not need persistence config but the results are not available after the computations restart.
#
# Let's first run the example without caching:
# %%

table = pw.debug.table_from_markdown(
    """
    value | __time__
      1   |     2
      2   |     2
     13   |     2
      1   |     2
      2   |     2
      1   |     2
"""
)


@pw.udf(deterministic=True)
def inc_no_cache(x: int) -> int:
    print(f"inc({x})")
    return x + 1


result_no_cache = table.with_columns(value_inc=inc_no_cache(table.value))
pw.debug.compute_and_print(result_no_cache)

# %% [markdown]
# As you can see from printed messages, the UDF was called 6 times.
#
# Let's use `InMemoryCache` this time:


# %%
@pw.udf(deterministic=True, cache_strategy=pw.udfs.InMemoryCache())
def inc_in_memory_cache(x: int) -> int:
    print(f"inc({x})")
    return x + 1


result_in_memory_cache = table.with_columns(value_inc=inc_in_memory_cache(table.value))
pw.debug.compute_and_print(result_in_memory_cache)

# %% [markdown]
# This time, the function was called only three times.
# Other results were extracted from the cache.
# If you run that piece of code as a separate program, it'd compute the results from scratch at every restart (because the results are stored in memory).
# In a notebook you have to restart the runtime to see the effect.
#
# This behavior might be problematic if you want to keep the results between restarts.
# This is where `DiskCache` comes in.
# It stores the results of the calls in persistent storage.
# In the example, it is located in the `./Cache` directory.
# To read more about setting up persistence see the [persistence guide](/developers/user-guide/deployment/persistence).


# %%
@pw.udf(deterministic=True, cache_strategy=pw.udfs.DiskCache())
def inc_disk_cache(x: int) -> int:
    print(f"inc({x})")
    return x + 1


persistence_config = pw.persistence.Config.simple_config(
    pw.persistence.Backend.filesystem("./Cache"),
    persistence_mode=pw.PersistenceMode.UDF_CACHING,
)
result_disk_cache = table.with_columns(value_inc=inc_disk_cache(table.value))
pw.debug.compute_and_print(result_disk_cache, persistence_config=persistence_config)

# %% [markdown]
# If, instead of printing output on the screen, you want to use one of the [output connectors](/developers/user-guide/connect/connectors/csv_connectors/), you need to put `persistence_config` in `pw.run`, like this:

# %%
pw.io.csv.write(result_disk_cache, "result_disk_cache.csv")
pw.run(persistence_config=persistence_config)

# %% [markdown]
# ## Asynchronous UDFs
# By default, Pathway UDFs are synchronous and blocking.
# If one worker is used, only one UDF call is active at a time and it has to finish for the next UDF call to start.
# If more workers are used, the maximal number of UDFs that have started and haven't finished is equal to the number of workers.
# It is a good situation for CPU bound tasks.
# If you want, however, to execute I/O bound tasks, like calling external services, it is better to have more than one task started per worker. Pathway provides asynchronous UDFs for it.
#
# Asynchronous UDFs can be defined in Pathway using [Python coroutines](https://docs.python.org/3/library/asyncio-task.html#id2) with the `async`/`await` keywords.
# The asynchronous UDFs are asynchronous *within a single batch*.
# In this context, we define a batch as all entries with equal processing times assigned.
# The UDFs are started for all entries in the batch and the execution of further batches is blocked until all UDFs for a given batch have finished.
# Thanks to that, the processing time of the entries remains unchanged and the output remains consistent.
# If you require a fully asynchronous non-blocking mechanism take a look at [`AsyncTransformer`](/developers/user-guide/data-transformation/asynchronous-transformations/).
#
# To define an asynchronous UDF it is enough to decorate a coroutine with `pw.udf`. Let's start with a simple example.
# %%
import asyncio


@pw.udf
async def inc_async(x: float) -> float:
    print(f"inc_async({x}) starting")
    await asyncio.sleep(x)
    print(f"inc_async({x}) finishing")
    return x + 1


table = pw.debug.table_from_markdown(
    """
    value
     0.2
     0.6
     2.0
     1.2
"""
)

result = table.select(value=inc_async(pw.this.value))
pw.debug.compute_and_print(result)

# %% [markdown]
# From the printed messages, you can see that the calls are executed asynchronously.
#
# Note that accidentally you created a sleepsort. Values in the `finishing` messages are sorted! As an exercise, you can try sorting also other values.
#
# As a more advanced example, you can create a UDF that queries [REST Countries](https://restcountries.com/) service to get the capital of a country.
# It uses `requests` library that on its own is not asynchronous. However, if you set `executor=pw.udfs.async_executor()`
# even though `requests.get` is not a coroutine, the function `find_capital` will be executed in a [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor),
# so it'll be possible to have more than one function started at once.

# %%
import requests


@pw.udf(executor=pw.udfs.async_executor())
def find_capital(country: str) -> str:
    result = requests.get(
        f"https://restcountries.com/v3.1/name/{country}?fields=capital",
        timeout=1,
    )
    result.raise_for_status()
    return result.json()[0]["capital"][0]


# _MD_COMMENT_START_


@pw.udf
def find_capital(country: str) -> str:
    return {
        "Poland": "Warsaw",
        "Germany": "Berlin",
        "Austria": "Vienna",
        "USA": "Washington, D.C.",
        "France": "Paris",
    }[country]


# _MD_COMMENT_END_

countries = pw.debug.table_from_markdown(
    """
    country
    Poland
    Germany
    Austria
    USA
    France
"""
)
countries_with_capitals = countries.with_columns(capital=find_capital(pw.this.country))
pw.debug.compute_and_print(countries_with_capitals)

# %% [markdown]
# ### AsyncExecutor
# It is possible to control the behavior of asynchronous UDFs using the parameters of `async_executor`:
# - `capacity` - the maximum number of concurrent operations,
# - `timeout` - the maximum time (in seconds) to wait for the function result,
# - `retry_strategy` - the strategy for handling retries in case of failures.
# The available strategies are [`ExponentialBackoffRetryStrategy`](/developers/api-docs/udfs#pathway.udfs.ExponentialBackoffRetryStrategy) and [`FixedDelayRetryStrategy`](/developers/api-docs/udfs#pathway.udfs.FixedDelayRetryStrategy).
# The exponential backoff strategy increases the waiting time between retries exponentially by multiplying the waiting time by `backoff_factor`.
# The fixed delay strategy does not increase the waiting time between retries.
# Both strategies add a random jitter to the waiting times.
#
# When both `timeout` and `retry_strategy` are used, all retries have to finish within a specified `timeout`. <!-- maybe we want to change it - that timeout applies only to a single retry -->
# You can see the application of a retry strategy in the example below.
# The UDF has a 10% chance of failing.
# It fails two times but the retry strategy executes the function with the arguments that failed again.

# %%
import random

random.seed(2)


@pw.udf(
    executor=pw.udfs.async_executor(
        retry_strategy=pw.udfs.FixedDelayRetryStrategy(max_retries=10, delay_ms=100),
    )
)
async def inc_async(x: float) -> float:
    print(f"inc_async({x})")
    if random.random() < 0.1:
        raise ValueError("err")
    await asyncio.sleep(x)
    return x + 1


table = pw.debug.table_from_markdown(
    """
    value
     0.2
     0.6
     2.0
     1.2
"""
)

result = table.select(value=inc_async(pw.this.value))
pw.debug.compute_and_print(result)

# %% [markdown]
# Of course, the retry strategy does not have to be used only to mitigate the effects of runtime errors.
# It can, for example, be used to query a service multiple times in the case of its temporary unavailability.
#
# The parameters that can be used with regular UDFs can also be used with asynchronous UDFs.
# For instance, you can cache its results or set that it is deterministic.


# %%
@pw.udf(deterministic=True, cache_strategy=pw.udfs.InMemoryCache())
async def inc_async(x: float) -> float:
    print(f"inc_async({x})")
    await asyncio.sleep(x)
    return x + 1


table = pw.debug.table_from_markdown(
    """
    value
     0.2
     0.6
     2.0
     1.2
     0.6
     1.2
"""
)

result = table.select(value=inc_async(pw.this.value))
pw.debug.compute_and_print(result)


# %% [markdown]
# ## Conclusions
# In this guide, you've learned how to define Python functions (UDFs) to process data in Pathway.
# The functions process a single row in a single call.
# It is possible to define the behavior of the functions by using UDF's parameters,
# like `deterministic`, `propagate_none`, `cache_strategy`, `executor`, etc.
# A friendly reminder - if your function is deterministic, set `deterministic=True` as it'll help with performance.

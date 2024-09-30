# ---
# title: Asynchronous Transformations
# description: An article explaining how to perform asynchronous data transformations in Pathway
# date: '2024-02-20'
# thumbnail: ''
# tags: ['tutorial', 'engineering']
# keywords: ['python', 'function', 'asynchronous', 'transformation', 'query', 'AsyncTransformer']
# notebook_export_path: notebooks/tutorials/asynctransformer.ipynb
# ---

# %% [markdown]
# # AsyncTransformer
# One way of transforming data in Pathway, when simple transformations are not enough, is using [UDFs](/developers/user-guide/data-transformation/user-defined-functions).
# However, if the flexibility of the UDFs is still not enough, you can use even more general and flexible `AsyncTransformer`, useful especially for asynchronous computations.
#
# `AsyncTransformer` is a different mechanism than UDFs.
# It acts on the whole Pathway Table and returns a new Table.
# In contrast to UDFs, it is fully asynchronous.
# It starts the `invoke` method for every row that arrives,
# without waiting for the previous batches to finish.
# When the call is finished, its result is returned to the engine with a new processing time.
#
# To write an `AsyncTransformer` you need to inherit from [`pw.AsyncTransformer`](/developers/api-docs/pathway#pathway.AsyncTransformer) and implement the [`invoke` method](/developers/api-docs/pathway#pathway.AsyncTransformer.invoke) (it is a coroutine). The names of the arguments of the method have to be the same as the columns in the input table. You can use additional arguments but then you have to specify their default value (it might be useful if you want to use the same `AsyncTransformer` on multiple Pathway tables with different sets of columns). You have to use all columns from the input table. The order of columns/arguments doesn't matter as they are passed to the method as keyword arguments.
#
# You also need to define the schema of a table that is produced. The `invoke` method has to return a dictionary containing values to put in all columns of the output table. The keys in the dictionary has to match fields from the output schema.
# Let's create a simple `AsyncTransformer` that produces a Table with two output columns - `value` and `ret`.


# %%
import pathway as pw
import asyncio


class OutputSchema(pw.Schema):
    value: int
    ret: int


class SimpleAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
    async def invoke(self, value: int) -> dict:
        await asyncio.sleep(value / 10)
        return dict(value=value, ret=value + 1)


# %% [markdown]
# Let's use the transformer on the example input table.
# The result table containing only successful calls can be retrieved from the [`successful`](/developers/api-docs/pathway#pathway.AsyncTransformer.successful) property of the transformer.

# %%
table = pw.debug.table_from_markdown(
    """
    value
      12
       6
       2
       2
       6

"""
)

result = SimpleAsyncTransformer(input_table=table).successful
pw.debug.compute_and_print(result)

# %% [markdown]
# The result is correct. Now let's take a look at the output times:

# %%
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# Even though all values have equal processing times initially,
# the output times are different between the rows.
# It is the effect of `AsyncTransformer` not waiting for other rows to finish.
# Thanks to that some rows can be processed downstream quicker.
# If you want some rows to wait for some other rows to finish,
# take a look at the [`instance`](#asynctransformer-consistency) parameter.

# %% [markdown]
# ## Failing calls
# The `invoke` method is usually written by an external user (like you) and it can contain bugs (unless you write bug-free code).
# When the `invoke` call raises an exception or times out (see the [next section](#controlling-asynctransformer-behavior) for that), its output won't be included in the `successful` table.
# The failed rows are put in the table accessible by the [`failed`](/developers/api-docs/pathway#pathway.AsyncTransformer.failed) property.
# Let's define a new `AsyncTransformer` to check that.
# Maybe we don't like the value $12$ and we fail our function whenever we get it as an argument.


# %%
class SometimesFailingAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
    async def invoke(self, value: int) -> dict:
        if value == 12:
            raise ValueError("incorrect value")
        return dict(value=value, ret=value + 1)


t = SometimesFailingAsyncTransformer(input_table=table)
pw.debug.compute_and_print(t.successful)
pw.debug.compute_and_print(t.failed)

# %% [markdown]
# In the failed table you only get the ids of failed rows (other columns contain `None`).
# Because the `invoke` call failed it was impossible to return any values.
# You can check which values have failed by joining with the input table:

# %%
failed = t.failed.join(table, pw.left.id == pw.right.id, id=pw.left.id).select(
    pw.right.value
)
pw.debug.compute_and_print(failed)

# %% [markdown]
# Now, you can see that the failed row actually has $12$ in the `value` column.

# %% [markdown]
# ## Controlling AsyncTransformer behavior
# It is possible to control the behavior of `AsyncTransformer` using parameters similar to those in UDFs.
# They can be passed to [`with_options`](/developers/api-docs/pathway#pathway.AsyncTransformer.with_options) method.
# The available options are:
# - `capacity` - the maximum number of concurrent operations,
# - `timeout` - the maximum time (in seconds) to wait for the function result,
# - `retry_strategy` - the strategy for handling retries in case of failures.
# The same strategies as for asynchronous UDFs can be used.
# Examples: [`ExponentialBackoffRetryStrategy`](/developers/api-docs/udfs#pathway.udfs.ExponentialBackoffRetryStrategy), [`FixedDelayRetryStrategy`](/developers/api-docs/udfs#pathway.udfs.FixedDelayRetryStrategy),
# - `cache_strategy` - the caching strategy. The same strategies as for UDFs can be used. Examples: [`DiskCache`](/developers/api-docs/udfs#pathway.udfs.DiskCache), [`InMemoryCache`](/developers/api-docs/udfs#pathway.udfs.InMemoryCache).

# In the following example, you add a timeout to the `SimpleAsyncTransformer` defined above.
# It is set to $0.9$ seconds.
# %%
t = SimpleAsyncTransformer(input_table=table).with_options(timeout=0.9)

pw.debug.compute_and_print(t.successful)
failed = t.failed.join(table, pw.left.id == pw.right.id).select(pw.right.value)
pw.debug.compute_and_print(failed)

# %% [markdown]
# Recall that the transformer sleeps the `invoke` method for a time passed as the method argument divided by $10$.
# That's why calls with `value` less than $9$ were successful, and calls with `value` greater than $9$ failed.

# %% [markdown]
# ## AsyncTransformer consistency
# By default, `AsyncTransformer` preserves order for a given key.
# It means that if some row is still executed by `AsyncTransformer` and its update starts being executed and finishes earlier than the original row,
# it'll wait for the completion of the original row processing before being returned to the engine.
# The update cannot have an earlier time assigned than the original row as it would break the correctness of the computations.
#
# Let's analyze this case by computing the sums of entries from the stream.
# You want to compute the sum for each `group` independently.

# %%
table = pw.debug.table_from_markdown(
    """
    group | value | __time__
      1   |   2   |     2
      1   |   3   |     2
      2   |   1   |     2
      1   |  -3   |     4
      2   |   2   |     4
"""
)
sums = table.groupby(pw.this.group).reduce(
    pw.this.group, value=pw.reducers.sum(pw.this.value)
)

pw.debug.compute_and_print_update_stream(sums)

# %% [markdown]
# The sums computed in time $2$ are $5$ and $1$.
# They are deleted in time $4$ and replaced with sums $2$ and $3$.
# Let's modify `SimpleAsyncTransformer` to propagate the `group` column as well and apply it to the `sums` table.


# %%
class OutputWithGroupSchema(pw.Schema):
    group: int
    value: int
    ret: int


class GroupAsyncTransformer(pw.AsyncTransformer, output_schema=OutputWithGroupSchema):
    async def invoke(self, value: int, group: int) -> dict:
        await asyncio.sleep(value / 10)
        return dict(group=group, value=value, ret=value + 1)


result = GroupAsyncTransformer(input_table=sums).successful
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# All rows reach `GroupAsyncTransformer` at approximately the same time.
# In group $2$, the value at time $2$ is $1$, and at time $4$ is $3$.
# The first value is processed faster and returned to the engine.
# When a call for the next value finishes, the old value is removed and a new value is returned to the engine.
#
# The situation for group $1$ is different.
# The value at time $2$ is greater than the value at time $4$ ($5 > 2$).
# Because of that, the second call to `invoke` finishes earlier and has to wait for the first call to finish.
# When the first call finishes, they are both returned to the engine.
# The value from the second call is newer and immediately replaces the old value.

# %% [markdown]
# ### Partial consistency
# Sometimes, the consistency for rows with a single key might not be enough for you.
# If you want to guarantee an order within a group of records, you can use the `instance` parameter of the `AsyncTransformer`.
# Rows within a single `instance` are ordered.
# It means that the results for rows with higher initial processing times can't overtake the results for rows with lower initial processing times.
# All results within a single instance with equal processing times wait for all rows with this time to finish.
# Using the `instance` parameter does not block new calls from starting. Only the results of the calls get synchronized.
# To demonstrate the synchronization, we create a new table with more data:

# %%
table = pw.debug.table_from_markdown(
    """
    group | value | __time__
      1   |   2   |     2
      1   |   3   |     2
      2   |   1   |     2
      3   |   1   |     2
      4   |   3   |     2
      1   |  -3   |     4
      2   |   3   |     4
      3   |   1   |     4
      4   |  -1   |     4
"""
)
sums = table.groupby(pw.this.group).reduce(
    pw.this.group, value=pw.reducers.sum(pw.this.value)
)

pw.debug.compute_and_print_update_stream(sums)

# %% [markdown]
# Now, you have four groups, with one row for each group.
# You want to guarantee consistency separately for even and odd groups.
# To do that, you need to set the `instance` of `GroupAsyncTransformer` appropriately.

# %%
result = GroupAsyncTransformer(input_table=sums, instance=pw.this.group % 2).successful
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# The updates for groups $2,4$ are bundled together.
# Group $2$ at time $2$ could finish earlier, but it waits for group $4$.
# Groups $1,3$ are also dependent on each other.
# Group $3$ could finish quicker, but it waits for group $1$ to finish.
#
# You can have a look at how the updates would proceed if no `instance` was specified:

# %%
result = GroupAsyncTransformer(input_table=sums).successful
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# As you can see, only ordering within a group is preserved.

# %% [markdown]
# ### Full consistency
# By using the `instance` parameter, it is possible to make the output preserve the temporal ordering of the input.
# It is enough to set `instance` to the same value for all rows, for example by using a constant.
# Then results for rows with a given time will wait for all previous times to finish before being returned to the engine.
# Rows with a given time are returned all at once and have the same time assigned.
# The new calls are not blocked from starting. Only the results get synchronized.
#
# Let's use constant `instance` in the example from the previous section.

# %%
result = GroupAsyncTransformer(input_table=sums, instance=0).successful
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# All rows are returned at the same time.
# There are also no updates because calls for time $2$ are finished later than calls for time $4$.
# You can play with the data to make time $2$ finish before time $4$ and see that the update happens once.

# %% [markdown]
# ### Failing calls consistency
# If the `instance` parameter is used and the call for a given instance fails, the instance is in the failed state from this time.
# `AsyncTransformer` requires all calls with a given `(instance, processing time)` pair to finish successfully.
# If at least one call fails, returning other rows could leave the instance in an inconsistent state.
# Let's take a look at what happens if `group` $4$ fails at time $4$.


# %%
class SuspiciousGroupAsyncTransformer(
    pw.AsyncTransformer, output_schema=OutputWithGroupSchema
):
    async def invoke(self, value: int, group: int) -> dict:
        if group == 4 and value == 2:
            raise ValueError("err")
        await asyncio.sleep(value / 10)
        return dict(group=group, value=value, ret=value + 1)


result = SuspiciousGroupAsyncTransformer(
    input_table=sums, instance=pw.this.group % 2
).successful
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# New values for the even instance (groups $2,4$) coming from the entries at time $4$ are not inserted because group $4$ fails and hence the whole instance fails. None of the entries in the odd instance (groups $1,3$) fail so it is updated normally.

# %% [markdown]
# ## Conclusions
# In this guide, you've learned how to create your own `AsyncTransformer`
# when you need to process the data asynchronously in Pathway.
# You know how to control its behavior by setting parameters like `timeout`, `cache_strategy` and `retry_strategy`.
# You can control the tradeoff between the speed and the consistency of the results.
#
# Now, you also understand the difference between asynchronous UDFs and AsyncTransformer.
# The former is asynchronous only within a single batch of data
# and can return values only to a single column,
# while the latter is fully asynchronous and can return multiple columns.
# It also allows for specifying the consistency level by using the `instance` parameter.

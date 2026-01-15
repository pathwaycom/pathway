# ---
# title: Writing declarative over imperative pipelines
# description: An article exploring concepts related to iterative computation in Pathway.
# date: '2025-11-25'
# thumbnail: ''
# tags: ['tutorial', 'engineering']
# keywords: ['declarative', 'imperative', 'ordered', 'splitting', 'iterate']
# notebook_export_path: notebooks/tutorials/declarative_vs_imperative.ipynb
# ---

# %% [markdown]
# # Writing declarative over imperative pipelines
#
# Many real-world data processing tasks — such as those in logistics, supply chain management, or event stream analysis—rely on the order of events to extract meaningful insights. For example, tracking shipments, monitoring sensor data, or processing sequences of user actions all require careful handling of ordered streams. Pathway's declarative approach makes it easy to express such order-dependent logic, enabling robust solutions for these domains.
#
# In data processing, imperative pipelines require you to specify step-by-step instructions for how data should be transformed, often leading to complex and less maintainable code. Declarative pipelines, on the other hand, focus on describing what the desired outcome is, letting the system determine the best way to achieve it. Pathway encourages a declarative approach, as demonstrated in the examples below, allowing you to express complex data transformations and iterative computations in a concise and readable manner. This leads to more robust, scalable, and maintainable workflows.

# %% [markdown]
# ## Splitting ordered stream into chunks
#
# The following simple example demonstrates how to split an ordered stream of events into chunks based on a flag column, i.e. for the input:
# ```
#     event_time | flag
#              0 | True
#              1 | False
#              2 | False
#              3 | True
#              4 | False
#              5 | False
#              6 | False
#              7 | True
#              8 | False
#              9 | True
# ```
# we would expect three "finished" chunks: `(0,1,2)`, `(3,4,5,6)`, `(7,8)` and one unfinished chunk `(9,...)`.
#
# One way to do this would be imperative style: go through rows one-by-one in order storing current chunk in a state and emitting it whenever `flag` is equal to True, while clearing the state.
# Even though, its not recommended approach, let's see how to code it in Pathway.

# %%
import pathway as pw

t = pw.debug.table_from_markdown(
    """
    event_time | flag  | __time__
             0 | True  | 2
             1 | False | 4
             2 | False | 6
             3 | True  | 8
             4 | False | 10
             5 | False | 12
             6 | False | 14
             7 | True  | 16
             8 | False | 18
             9 | True  | 20
    """
)


def split_by_flag_imperative(input_t: pw.Table) -> pw.Table:
    @pw.reducers.stateful_many  # type: ignore
    def split_by_flag(
        state: tuple[tuple[int], tuple[tuple[int]]] | None,
        rows: list[tuple[list[int], int]],
    ) -> int:
        if state is None:
            state = [[], []]
        else:
            state = [(state[0]), []]  # clear emitted rows
        current_chunk, chunks_to_emit = state
        rows = sorted(rows, key=lambda x: x[0][0])  # sort batch by event_time
        # imperative logic for splitting into chunks
        for row, _ in rows:
            event_time, flag = row

            if flag and len(current_chunk) > 0:
                chunks_to_emit = list(chunks_to_emit)
                chunks_to_emit.append(current_chunk)
                current_chunk = [event_time]
                state = (current_chunk, chunks_to_emit)
            else:
                current_chunk = list(current_chunk)
                current_chunk.append(event_time)
                state = (current_chunk, chunks_to_emit)
        return state

    res = (
        input_t.groupby()
        .reduce(coupled=split_by_flag(pw.this.event_time, pw.this.flag))
        .select(chunk=pw.this.coupled[1])
    )
    return res.flatten(pw.this.chunk)._remove_retractions().with_id_from(pw.this.chunk)


pw.debug.compute_and_print_update_stream(split_by_flag_imperative(t))

# %% [markdown]
#

# %% [markdown]
# Instead of manually managing state and control flow, Pathway allows you to define such logic using declarative constructs like `sort`, `iterate`, `groupby`. The result is a clear and concise pipeline that emits chunks of event times splitting the flag, showcasing the power and readability of declarative data processing.
#
# In the following, we tell Pathway to propagate the starting time of each chunk across the rows. This is done by declaring a simple local rule: take the starting time of a chunk from previous row or use current event time. This rule is then iterated until fixed-point, so that the information is spread until all rows know the starting time of their chunk.
#
# Then we can just group rows by starting time of the chunk to get a table of chunks.

# %%
import pathway as pw


def split_by_flag_declarative(input_t: pw.Table) -> pw.Table:
    t_sorted = input_t + input_t.sort(input_t.event_time)

    def _step(tab: pw.Table) -> pw.Table:
        tab_prev = tab.ix(tab.prev, optional=True)
        return tab.with_columns(
            prev_flag_time=pw.coalesce(tab.prev_flag_time, tab_prev.prev_flag_time)
        )

    res = pw.iterate(
        _step,
        tab=t_sorted.with_columns(
            prev_flag_time=pw.if_else(pw.this.flag, pw.this.event_time, None)
        ),
    ).without(pw.this.prev, pw.this.next)
    res = (
        res.groupby(pw.this.prev_flag_time)
        .reduce(chunk=pw.reducers.sorted_tuple(pw.this.event_time))
        .with_id_from(pw.this.chunk)
    )
    return res


pw.debug.compute_and_print(split_by_flag_declarative(t))

# %% [markdown]
# To illustrate the advantages of declarative solution over imperative one, consider situation which occurs very often in the real world - when input events arrive out of order.
# The declarative solution works without any changes:

# %%
t_out_of_order = pw.debug.table_from_markdown(
    """
    event_time | flag  | __time__
             0 | True  | 2
             1 | False | 4
             2 | False | 6
             3 | True  | 8
             4 | False | 20
             5 | False | 18
             6 | False | 16
             7 | True  | 14
             8 | False | 12
             9 | True  | 10
    """
)

pw.debug.compute_and_print(split_by_flag_declarative(t_out_of_order))

# %% [markdown]
# while the imperative one fails to produce correct results:

# %%
pw.debug.compute_and_print(split_by_flag_imperative(t_out_of_order))

# %% [markdown]
# To fix this, one would have to adapt the imperative code by implementing additional buffering, handling insertion of rows at correct place in the chunk and delaying emitting results until one's sure that no earlier events will arrive. This would significantly complicate the code.
#
# Pathway's declarative approach on the other hand keeps the code clean, simple and maintainable.

# %% [markdown]
# ### Managing memory with forgetting
# In the example above, the declarative pipeline keeps accumulating state as more data arrives.
# Specifically, the sort, groupby, iterate, ix operators are stateful.

# For large streaming data, we would want to keep the memory usage bounded.
# Pathway provides a forgetting mechanism that can be used to limit the amount of state kept by these operators.
# The solution would be to wrap the input table with `_forget` method, specifying the window of interest.

# %%
WINDOW = 7  # set it to appropriate value for your use-case: here max(chunk size, out-of-orderness)

t_out_of_order_tail = t_out_of_order.forget(
    pw.this.event_time, WINDOW, mark_forgetting_records=True
)  # emit special "-1"'s for older entries to clean up obsolete state
result = split_by_flag_declarative(
    t_out_of_order_tail
)  # transformation wrapped around forgetting
result = (
    result.filter_out_results_of_forgetting()
)  # clean-up entries emitted due to forgetting to get a stream containing all final results
pw.debug.compute_and_print(result)

# %%

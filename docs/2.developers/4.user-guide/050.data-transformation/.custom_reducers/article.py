# ---
# title: Writing Custom Reducers
# description: An article explaining how to write custom reducers in Pathway
# date: '2024-01-17'
# thumbnail: '/assets/content/blog/th-json.png'
# tags: ['tutorial']
# keywords: ['reducers', 'aggregate', 'sumofsquares', 'median']
# notebook_export_path: notebooks/tutorials/custom_reducers.ipynb
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Writing Simple Custom Reducer in Pathway
#
# Pathway supports natively aggregation using a wide range of [reducers](/developers/api-docs/reducers/), e.g., [`sum`](/developers/api-docs/reducers#pathway.reducers.sum), [`count`](/developers/api-docs/reducers#pathway.reducers.count), or [`max`](/developers/api-docs/reducers#pathway.reducers.max). However, those might not cover all the necessary ways of aggregating values. In this tutorial, you learn how to write reducers implementing custom logic.
#
# For example, let's implement a custom `stdev` reducer that computes the standard deviation.
# %%
import pathway as pw

SHOW_DEBUG = False


class StdDevAccumulator(pw.BaseCustomAccumulator):
    def __init__(self, cnt, sum, sum_sq):
        self.cnt = cnt
        self.sum = sum
        self.sum_sq = sum_sq

    @classmethod
    def from_row(cls, row):
        [val] = row
        if SHOW_DEBUG:
            print("from_row()")
        return cls(1, val, val**2)

    def update(self, other):
        self.cnt += other.cnt
        self.sum += other.sum
        self.sum_sq += other.sum_sq
        if SHOW_DEBUG:
            print("update()")

    def compute_result(self) -> float:
        mean = self.sum / self.cnt
        mean_sq = self.sum_sq / self.cnt
        if SHOW_DEBUG:
            print("compute_result()")
        return mean_sq - mean**2


stddev = pw.reducers.udf_reducer(StdDevAccumulator)
# %% [markdown]
# Above, the [`pw.BaseCustomAccumulator`](/developers/api-docs/pathway#pathway.BaseCustomAccumulator) class is used as a base for the `StdDevAccumulator`, which describes the logic of the underlying accumulator. The accumulator class requires a few methods:
# * [`from_row`](/developers/api-docs/pathway#pathway.BaseCustomAccumulator.from_row), which constructs an accumulator from the values of a single row of a table (here, a single value since our reducer applies to a single column),
# * [`update`](/developers/api-docs/pathway#pathway.BaseCustomAccumulator.update), which updates one accumulator by another accumulator,
# * [`compute_result`](/developers/api-docs/pathway#pathway.BaseCustomAccumulator.compute_result), which produces the output based on the accumulator state,
# * [`retract`](/developers/api-docs/pathway#pathway.BaseCustomAccumulator.retract), is an optional method, which processes negative updates,
# * [`neutral`](/developers/api-docs/pathway#pathway.BaseCustomAccumulator.neutral), is an optional method, which returns state corresponding to consuming 0 rows.
#
# Now, let's see the reducer in action.
# %%
temperature_data = pw.debug.table_from_markdown(
    """
date       |   temperature
2023-06-06 |   28.0
2023-06-07 |   23.1
2023-06-08 |   24.5
2023-06-09 |   26.0
2023-06-10 |   28.3
2023-06-11 |   25.7
"""
)

temperature_statistics = temperature_data.reduce(
    avg=pw.reducers.avg(pw.this.temperature), stddev=stddev(pw.this.temperature)
)

pw.debug.compute_and_print(temperature_statistics)
# %% [markdown]
# However, with this logic, our reducer is not smartly processing negative updates: it starts the computation from scratch whenever a negative update is encountered.
# You can see this in action by enabling debug information and processing table where row removal happens. Let's insert several values at time 0 and then remove one already inserted value and add another at time 2.
# %%
SHOW_DEBUG = True
temperature_data_with_updates = pw.debug.table_from_markdown(
    """
date       |   temperature | __time__ | __diff__
2023-06-06 |   28.0        |        0 |        1
2023-06-07 |   23.1        |        0 |        1
2023-06-08 |   24.5        |        0 |        1
2023-06-09 |   26.0        |        0 |        1
2023-06-10 |   28.3        |        0 |        1
2023-06-11 |   25.7        |        0 |        1
2023-06-11 |   25.7        |        2 |       -1
2023-06-11 |   25.9        |        2 |        1
"""
)

temperature_statistics_with_updates = temperature_data_with_updates.reduce(
    avg=pw.reducers.avg(pw.this.temperature), stddev=stddev(pw.this.temperature)
)

pw.debug.compute_and_print(temperature_statistics_with_updates)


# %% [markdown]
# It can be alleviated by extending our reducer and providing a method for processing negative updates.
# %%
class ImprovedStdDevAccumulator(StdDevAccumulator):
    def retract(self, other):
        self.cnt -= other.cnt
        self.sum -= other.sum
        self.sum_sq -= other.sum_sq
        if SHOW_DEBUG:
            print("retract()")


improved_stddev = pw.reducers.udf_reducer(ImprovedStdDevAccumulator)
# %% [markdown]
# And now you can test the improved reducer in action.
# %%

temperature_statistics_improved = temperature_data_with_updates.reduce(
    avg=pw.reducers.avg(pw.this.temperature),
    stddev=improved_stddev(pw.this.temperature),
)

pw.debug.compute_and_print(temperature_statistics_improved)

# %% [markdown]
# In the example above, 10x calls to `update()` and 12x calls to `from_row()` are replaced with 6x calls to `update()`, 1x call to `retract()` and 8x calls to `from_row()`.
#
# This comes from the fact that former reducer:
# * had to call `from_row()` for each row of the table, wrapping each single value into separate `StdDevAccumulator` object,
# * had to call `update()` for each row of the table except the first consumed,
# * had to restart from scratch after the update to the table, thus it had to pay the cost twice.
#
# While the latter reducer aggregated the table at time 0 in the same way as former one, but processed the update differently:
# * had to wrap both delete and insert updates with `from_row()` calls
# * called once `retract()` and once `update()`.

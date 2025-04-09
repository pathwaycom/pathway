# ---
# title: Windowby Reduce
# description: Windowby Reduce manu[a]l
# notebook_export_path: notebooks/tutorials/windowby_manual.ipynb
# ---

# # Windowby - Reduce
# In this manu\[a\]l, you will learn how to aggregate data with the windowby-reduce scheme.
#
# Pathway offers powerful features for time series data manipulation. One such feature is the `windowby` function, which allows for intricate data segmentation based on specified criteria.
#
# The `windowby` function can operate in three distinct modes—session, sliding, and tumbling—which are determined by the type of windowing function you pass to it.
# * Session Window: Groups adjacent elements based on activity and inactivity periods.
# * Sliding Window: Groups elements in overlapping windows of a specified length.
# * Tumbling Window: Groups elements in non-overlapping windows of a specified length.
#
#
#
# ::article-img
# ---
# src: 'assets/content/documentation/table-operations/windowby-types.png'
# alt: 'Illustration of Window types'
# class: 'mx-auto'
# ---
# ::
#
# This guide focuses on exploring these different types, demonstrating how each one can be used to achieve unique and important data analysis tasks.
#
# The data we're going to use is about... drumroll please... chocolate consumption! Let's suppose we have a dataset that tracks the amount of chocolate eaten during the day by a group of chocoholics. So, without further ado, let's get started.

import pathway as pw

# +
fmt = "%Y-%m-%dT%H:%M:%S"

table = pw.debug.table_from_markdown(
    """
    | time                  | name            | chocolate_bars
 0  | 2023-06-22T09:12:34   | Fudge_McChoc    | 2
 1  | 2023-06-22T09:23:56   | Ganache_Gobbler | 2
 2  | 2023-06-22T09:45:20   | Truffle_Muncher | 1
 3  | 2023-06-22T09:06:30   | Fudge_McChoc    | 1
 4  | 2023-06-22T10:11:42   | Ganache_Gobbler | 2
 5  | 2023-06-22T10:32:55   | Truffle_Muncher | 2
 6  | 2023-06-22T11:07:18   | Fudge_McChoc    | 3
 7  | 2023-06-22T11:23:12   | Ganache_Gobbler | 1
 8  | 2023-06-22T11:49:29   | Truffle_Muncher | 2
 9  | 2023-06-22T12:03:37   | Fudge_McChoc    | 4
 10 | 2023-06-22T12:21:05   | Ganache_Gobbler | 3
 11 | 2023-06-22T13:38:44   | Truffle_Muncher | 3
 12 | 2023-06-22T14:04:12   | Fudge_McChoc    | 1
 13 | 2023-06-22T15:26:39   | Ganache_Gobbler | 4
 14 | 2023-06-22T15:55:00   | Truffle_Muncher | 1
 15 | 2023-06-22T16:18:24   | Fudge_McChoc    | 2
 16 | 2023-06-22T16:32:50   | Ganache_Gobbler | 1
 17 | 2023-06-22T17:58:06   | Truffle_Muncher | 2
"""
).with_columns(time=pw.this.time.dt.strptime(fmt))
# -

# ## Temporal Session Windowing
# The `session` windowing function is designed for grouping together adjacent time events based on a specific condition. This can either be a maximum time difference between events or a custom condition defined by you.
#
# For instance, let's say you are curious about the binge-eating sessions of the chocoholics. You'd want to group all consecutive records where the gap between the chocolate eating times is less than or equal to some period of time.
#
# Let's check out an example:

# +
from datetime import timedelta

result = table.windowby(
    table.time,
    window=pw.temporal.session(max_gap=timedelta(hours=2)),
    instance=table.name,
).reduce(
    pw.this.name,
    session_start=pw.this._pw_window_start,
    session_end=pw.this._pw_window_end,
    chocolate_bars=pw.reducers.sum(pw.this.chocolate_bars),
)

# Print the result
pw.debug.compute_and_print(result, include_id=False)
# -

# ## Temporal Sliding Windowing
#
# Next, let's slide into sliding windows. Sliding windows move through your data at a specific step (hop) and create a window of a specific duration. This is like sliding a magnifying glass over your data to focus on specific chunks at a time.
#
# Let's find the chocolate consumption within sliding windows of duration 10 hours, sliding every 3 hours. This could be handy for identifying peak chocolate-eating times!

# +
result = table.windowby(
    table.time,
    window=pw.temporal.sliding(duration=timedelta(hours=10), hop=timedelta(hours=3)),
    instance=table.name,
).reduce(
    name=pw.this._pw_instance,
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    chocolate_bars=pw.reducers.sum(pw.this.chocolate_bars),
)

# Print the result
pw.debug.compute_and_print(result, include_id=False)

# -

# This gives you detailed insights about chocolate consumption over different time windows.

# ## Temporal Tumbling Windowing
#
# Finally, let's tumble through tumbling windows. Tumbling windows divide our data into distinct, non-overlapping intervals of a given length.
#
# Let's divide the time series into tumbling windows of 5 hours each to see how our chocolate consumption varies over distinct periods.

# +
result = table.windowby(
    table.time,
    window=pw.temporal.tumbling(duration=timedelta(hours=5)),
    instance=table.name,
).reduce(
    name=pw.this._pw_instance,
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    chocolate_bars=pw.reducers.sum(pw.this.chocolate_bars),
)

# Print the result
pw.debug.compute_and_print(result, include_id=False)
# -

# ## Conclusion
#
# In this guide, you've mastered the use of the windowby-reduce scheme in the Pathway library, a robust tool for time-series data aggregation. The three types of window functions—session, sliding, and tumbling—have been unveiled, each with its unique way of segmenting data. A playful example of chocolate consumption illuminated their practical application. As you continue to delve into data analysis, check out the tutorial [Detecting suspicious user activity with Tumbling Window group-by](/developers/templates/etl/suspicious_activity_tumbling_window), which utilizes the tumbling window function to spot unusual user behavior. Continue exploring, and elevate your data analysis prowess.

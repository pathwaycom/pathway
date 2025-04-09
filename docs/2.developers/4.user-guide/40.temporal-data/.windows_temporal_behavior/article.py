# ---
# title: Controlling Temporal Behavior of Windows
# description: An article exploring concepts related to temporal behavior of windows.
# date: '2024-01-08'
# thumbnail: 'assets/content/tutorials/clickstream_window_join/clickstream-window-join-th.png'
# keywords: ['windowby', 'behavior', 'late data', 'delay', 'cutoff', 'out-of-order data']
# notebook_export_path: notebooks/tutorials/windows_temporal_behavior.ipynb
# ---

# # Controlling Temporal Behavior of Windows

# In this article you will learn how to use windows effectively, by specifying their temporal behaviors.

# Temporal behaviors in Pathway are crucial for bounding memory consumption, maintaining proper tradeoff between latency and efficiency, or adjusting windows for your needs. To read more about behaviors and the motivation behind them read our [guide](/developers/user-guide/temporal-data/behaviors/). This article goes into detail on how to define the behavior using `common_behavior` and `exactly_once_behavior` and what impact they have on the result of [windows](/developers/user-guide/temporal-data/windows-manual).

# The examples in this article use the sliding windows, but you can also use behaviors with tumbling windows.

# ## Event Time vs Processing Time

# In the context of temporal behavior it is important to distinguish between an event time and a processing time. The event time is when the event happens, e.g. if your data are orders in the online shop, the event time is the time when the order happened. This information has to be present in your data because Pathway doesn't know when the event happened. Thus event time can be any time you assign to your data.
#
# The only time Pathway is aware of is when the record arrives to the Pathway engine. This time is called processing time. While the processing time of entries in a stream is always nondecreasing (because the time goes forward), due to latency the event time may be out of order.  In extreme cases, this can manifest via events with _high_ latency between their event time and processing time, which we shortly call _late data_.
#
# When grouping data in windows, you usually want to consider the event time, and the temporal behavior is based on it, but the order in which the events are processed impacts the results.

# ![Event time vs processing time](/assets/content/documentation/behavior-guide/event-time-vs-processing-time.svg)

# <!-- canva link: https://www.canva.com/design/DAF3tvptQG4/TGTqc9sHoWUrDHYs5bP_dg/edit-->

# ## Dataset

# To try out the temporal behaviors of windows you need an example Pathway Table with both processing time and event time. You can generate it using `pw.debug.table_from_markdown`, which takes a table specification in markdown format. If it has a column named `__time__`, Pathway will use it as a processing time, which allows you to see how the temporality of your data affects the outcome of the computation. The following code creates a table with logs. Other than the `__time__` column, it also has the `event_time`, which says when the event described by the log happened, and the `message` column. In this case, both `__time__` and `event_time` are given as timestamps.
#
# Remarks:
# - while the processing time for the `table_from_markdown` method always needs to be given as a timestamp, the event_time can be any of [various types that are supported by the windowing mechanism](/developers/api-docs/pathway-stdlib-temporal#pathway.stdlib.temporal.windowby)
# - the `table_from_markdown` method needs the processing time to be passed in a column with a special name `__time__`, but the column holding event_time is passed as a parameter to the [`windowby`](/developers/api-docs/pathway-stdlib-temporal#pathway.stdlib.temporal.windowby) function, and here it is called event_time just to keep the example self-explanatory.

import pathway as pw

t = pw.debug.table_from_markdown(
    """
    event_time  |           message                | __time__
      360       | Processing_started               |   362
      362       | Task_completed_successfully      |   362
      366       | Error_occurred_during_processing |   368
      370       | Data_received_from_sensor        |   410
      370       | Database_connection_established  |   370
      370       | File_saved_successfully          |   372
      372       | Processing_completed             |   374
      376       | Request_received_from_user       |   396
      382       | Task_in_progress                 |   382
      382       | Warning_Low_memory               |   392
"""
)

# Consider the following example scenario - you are given the table as defined above, and you need to count the number of logs that fall into 10-second windows, with windows starting every 4 seconds.
#
# To that end, you can use sliding windows. To keep things simple, start with a piece of code that only groups data into windows, without specifying temporal behaviors. As you can see in the code snippet below, you can do that using `windowby` with `sliding` window of `duration` set to 10 and `hop` set to 4. For the result, keep information about the start and the end of each window and the number of logs that are in those windows.

result = t.windowby(
    t.event_time,
    window=pw.temporal.sliding(duration=10, hop=4),
).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    n_logs=pw.reducers.count(),
)

# When you use [`pw.debug_compute_and_print`](/developers/api-docs/debug#pathway.debug.compute_and_print) to print the results, you will only get the final result, after all input rows are processed.

pw.debug.compute_and_print(result)

# To understand how the result changed when new rows were processed, it is useful to use [`pw.debug.compute_and_print_update_stream`](/developers/api-docs/debug#pathway.debug.compute_and_print_update_stream) function. It shows you every change made to the Table, with column `__diff__` denoting whether the row was added or removed.

pw.debug.compute_and_print_update_stream(result)

# ## What time is it?

# The behaviors depend on the "current time" of an operator, in this article denoted as _now_. It is defined as the maximum already seen time by an operator in the already processed data (when a new batch of data arrives it is processed using the value of _now_ obtained from previous batches). In the context of windows, this time is taken from the column you use for grouping data in windows - usually event time. For example, `delay` sets a shift in time, and the window will be computed once _now_ is at least `delay` after the beginning of the window.

# ## Common Behavior

# The general way to define temporal behaviors in Pathway is by using `pw.temporal.common_behavior`. It allows you to set `delay`, `cutoff` and `keep_results` parameters. The `delay` and `cutoff` parameters represent time duration and their type should be compatible with the time column passed to `windowby`. This means that if your time column has type `int` or `float` then `delay` and `cutoff` should also have type, respectively, int or float. If instead, the time column has type [`DatetimeUtc`](/developers/api-docs/pathway#pathway.DateTimeUtc) or [`DatetimeNaive`](/developers/api-docs/pathway#pathway.DateTimeNaive), then `delay` and `cutoff` should have type [`Duration`](/developers/api-docs/pathway#pathway.Duration). To understand the motivation of these parameters read our [guide on behaviors](/developers/user-guide/temporal-data/behaviors/).

# ### Delay

# When you set the `delay` to be non-zero, the engine will wait before first calculating the result of each window. To be more precise, the window will be calculated, when _now_ is at least `window_start + delay`. If `delay` is not provided, it defaults to `None` which disables the delay mechanism.

# ![Illustration of delay](/assets/content/tutorials/windows_behavior/window-behavior-delay.svg)

# <!-- canva link: https://www.canva.com/design/DAF5SOHMKWA/Wpn4FY_RswyOaFaSpRzIgA/edit -->

# You can use it to stagger calculations - this allows for more rows to be processed at once, rather than recomputing the result after each row arrives to the engine. If you set the `delay` in the log example to be 4, you will see that the update stream becomes shorter.

result_delay = t.windowby(
    t.event_time,
    window=pw.temporal.sliding(duration=10, hop=4),
    behavior=pw.temporal.common_behavior(delay=4),
).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    n_logs=pw.reducers.count(),
)
pw.debug.compute_and_print_update_stream(result_delay)

# You can notice in the `__time__` column an unexpected timestamp, that is `18446744073709551614`. That is because of the use of debug mode. As the input ended, the engine triggers the computation of the last window by setting _now_ to be maximum possible time. It won't happen in the streaming mode because the processing there never ends.

# ### Cutoff

# Cutoff determines when the result of the window will no longer be updated, even if there is a change to a data point inside that window. This should not be before the windows closes - in such case you would shorten the window. When the `cutoff` is set, the window is no longer updated when _now_ is later than `window_end + cutoff`. If the `cutoff` is not provided, it defaults to `None` which disables the cutoff mechanism.

# ![Illustration of cutoff](/assets/content/tutorials/windows_behavior/window-behavior-cutoff.svg)

# <!-- canva link: https://www.canva.com/design/DAF5TPP1830/asKKfh7ff1RWl7F90UMJbg/edit -->

# Now add `cutoff=4` to the log example. You should see that the row that has processing time `410` no longer impacts the results. When you use `cutoff` omitting such late points means that you get different results than if you processed everything in batch, as the data that comes after the cutoff of a window will not be used in calculations for this window. This, however, is necessary for efficient memory consumption - without setting `cutoff` all data that ever was processed needs to be kept in memory, in case some very late event arrives and a window needs to be recomputed. When you use the cutoff mechanism you inform the engine when it can clear the memory.

result_cutoff = t.windowby(
    t.event_time,
    window=pw.temporal.sliding(duration=10, hop=4),
    behavior=pw.temporal.common_behavior(cutoff=4),
).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    n_logs=pw.reducers.count(),
)
pw.debug.compute_and_print_update_stream(result_cutoff)

# Note that since the time when cutoff triggers is based only on the window end and `cutoff` value, an event belonging to multiple windows can be late - and ignored in calculations - for one window, but on time for another. In the above example, you can notice that at time `396`. At this time the event with `event_time` equal to `376` arrives to the engine, so it belongs to 3 windows - starting at times `368`, `372` and `376`. But since for the first of these windows, we are past its cutoff when this event arrives, only the other two windows are recalculated.

# ### Keep_results

# The final argument of `common_behavior` - `keep_results` is only relevant if you use the cutoff mechanism. When set to `True`, its default value, the rows corresponding to windows already past cutoff are kept in the output table. You can see that by looking at the final state of the `result_cutoff` Table from the previous Section - it contains a record for each window.

pw.debug.compute_and_print(result_cutoff)

# If you set `keep_results=False`, however, once the window is past its cutoff, the record for this window is removed from the result Table, so, in the end, you are left only with the last few windows. The example use case is [log monitoring](/developers/templates/etl/realtime-log-monitoring), where you want to raise alerts based only on very recent windows.

result_keep_results = t.windowby(
    t.event_time,
    window=pw.temporal.sliding(duration=10, hop=4, origin=360),
    behavior=pw.temporal.common_behavior(cutoff=4, keep_results=False),
).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    n_logs=pw.reducers.count(),
)
pw.debug.compute_and_print(result_keep_results)

# By checking the output of `compute_and_print_update_stream` you can see that each window was calculated at some point, but some of them were later removed.

pw.debug.compute_and_print_update_stream(result_keep_results)

# ## Exactly Once Behavior

# For windows that you want to calculate exactly once, Pathway offers an easier way of defining behavior with `pw.temporal.exactly_once_behavior` function. It takes one optional argument, `shift`. Then a window will be calculated at time `_pw_window_end + shift`, and after that all changes to this window will be ignored. It is equivalent to using `pw.temporal.common_behavior` with `delay` set to `duration + shift` (`duration` is an argument to both [sliding](/developers/api-docs/temporal#pathway.stdlib.temporal.sliding) and [tumbling](/developers/api-docs/temporal#pathway.stdlib.temporal.tumbling) windows for setting the length of the window) and `cutoff` to `shift`.

result_exactly_once = t.windowby(
    t.event_time,
    window=pw.temporal.sliding(duration=10, hop=4, origin=360),
    behavior=pw.temporal.exactly_once_behavior(shift=2),
).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    n_logs=pw.reducers.count(),
)
pw.debug.compute_and_print_update_stream(result_exactly_once)

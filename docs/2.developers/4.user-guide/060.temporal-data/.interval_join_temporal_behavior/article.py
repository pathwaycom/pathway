# ---
# title: Controlling Temporal Behavior of Interval Join
# description: An article exploring concepts related to temporal behavior of interval join.
# date: '2023-11-28'
# thumbnail: 'assets/content/tutorials/fleet_eta_interval_join/fleet-eta-interval-join-th.png'
# tags: ['tutorial', 'engineering']
# keywords: ['interval join', 'behavior', 'late data', 'delay', 'cutoff', 'out-of-order data']
# notebook_export_path: notebooks/tutorials/interval_join_temporal_behavior.ipynb
# ---

# %% [markdown]
# # Controlling Temporal Behavior of Interval Join
# This article explores concepts related to the temporal behavior of Pathway's interval join, like filtering out
# late records, freeing memory that is no longer needed, or decreasing the frequency of updates.
#
# Interval join is a temporal join that joins events within a specified time interval.
# You can read more about it in [Performing Interval Joins](/developers/user-guide/temporal-data/interval-join) article.
#
# Let's consider a scenario in which you track customers ordering products. Each order is placed at some specific time (`order_time`) and reaches the tracking system written in Pathway at time `__time__`.
# In a real-world scenario, you don't have perfect control over the time between the moment the order is placed and the moment it reaches the data processing engine. Packets can take different routes over the network, or, in rare cases, a sneaky Georgian woman can cut off internet access for [a whole country](https://www.theguardian.com/world/2011/apr/06/georgian-woman-cuts-web-access). As such, times `order_time` can arrive in the processing system out of order, very late, or even not arrive at all.

# To deliver streaming data in a production system, you can use a connector like Kafka or Redpanda (you can read more about them in [Kafka connector](/developers/user-guide/connect/connectors/kafka_connectors/) or [Redpanda connector](/developers/user-guide/connect/connectors/switching-to-redpanda/) articles). Below, `pw.debug.table_from_markdown` with `__time__` column simulates a connector that delivers data out-of-order to demonstrate configurable _temporal behavior_ of interval join.

# %% tags=[]
import pathway as pw

orders = pw.debug.table_from_markdown(
    """
    customer | product  | order_time | __time__
    Austin   | mouse    |     120    |    122
    Brenda   | keyboard |     120    |    122
    Carl     | mouse    |     124    |    124
    Evelyn   | cable    |     128    |    128
    Frank    | mouse    |     120    |    130
    Min      | mouse    |     124    |    130
    Nicole   | cable    |     130    |    132
    Sam      | keyboard |     128    |    134
    Theresa  | keyboard |     134    |    136
    Yichen   | cable    |     136    |    138
"""
)

discounts = pw.debug.table_from_markdown(
    """
    price | product  | start_time | __time__
      42  | mouse    |    120     |    122
     128  | keyboard |    118     |    122
       8  | cable    |    128     |    134
     135  | keyboard |    132     |    140
      10  | cable    |    122     |    150
"""
)

# %% [markdown]
# The store gives discounts on some products and they all last six time units since `start_time` (incl. `start_time`).
# The clients place orders but they get a guarantee that the order is executed only if the product is discounted.
#
# ![Customers orders with their event and processing times](assets/content/tutorials/interval_join_temporal_behavior/orders.svg)
#
# To get the price paid by a customer, you can perform interval join between `orders` and `product` streams.
# An order, to be associated with the product, has to come no earlier than the product
# was discounted and no later than the discount ended. So, it has to satisfy
# `start_time` $\le$ `order_time` $\le$ `start_time+5`. This can be expressed as an interval join:


# %% tags=[]
result = discounts.interval_join(
    orders,
    discounts.start_time,
    orders.order_time,
    pw.temporal.interval(0, 5),
    discounts.product == orders.product,
).select(
    orders.customer,
    orders.product,
    discounts.price,
    orders.order_time,
    discounts.start_time,
)
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# As time progresses, new orders and products arrive, and you get new information about the prices paid for the products.
# Note that two customers were not able to purchase a product:
# - Sam wanted to buy a keyboard at time $128$, but it was not discounted then. It was discounted only at times $[118, 122]$ and $[132, 136]$.
# - Yichen wanted to buy a cable at time $136$, but it also wasn't discounted.
#
# As such, their orders are not present in the `result` table. If you want to include all orders (also those that can't be executed), you can use `interval_join_right`.

# %% [markdown]
# ## Forgetting old records
# As mentioned before, you don't have any control over the time between the event creation (an example event can be a store that registers that a product X is available at price Y, starting from time Z) and the event arrival at the processing engine. In our scenario, an example of such an event is the `cable` entry in the `products` table that was discounted at time $122$ and only reached Pathway at time $150$.

# In principle, you don't know if more old records won't come in the future. As such, to guarantee that a join on such possibly late data returns correct answers, the processing engine needs to store all the records in its memory.

# Practically, keeping all the old records just to handle some very overdue orders may be a price you are not willing to pay, and it's better to ignore such orders while cleaning memory from some old entries.

# To make that trade-off possible, Pathway provides the `behavior` parameter for `interval_join`, which defines its temporal behavior. Roughly speaking, it allows you to tell Pathway to ignore the records that are too late.  Subsequently, that allows you to forget the records that you know won't be joined with any new incoming records in the future.
# To be more precise: if you set the `behavior` to e.g. `pw.temporal.common_behavior(cutoff=6)`, Pathway will ignore all records that have times less or equal to maximal already seen time minus $6$. Small remark: the maximal already seen time is held separately for each side of the join, as it allows expressing a join with historical data slightly easier.

# %% tags=[]
result = discounts.interval_join(
    orders,
    discounts.start_time,
    orders.order_time,
    pw.temporal.interval(0, 5),
    discounts.product == orders.product,
    behavior=pw.temporal.common_behavior(cutoff=6),
).select(
    orders.customer,
    orders.product,
    discounts.price,
    orders.order_time,
    discounts.start_time,
)
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# Let's see what happens in this case. The final result doesn't contain Frank's order.
# His order was performed at time $120$ and could be joined with the mouse being discounted at time $120$.
# However, the maximal seen time in the `orders` stream when Frank's order arrived was $128$
# (`order_time` of Evelyn's order). All new records with `order_time` less
# or equal to $128-6=122$ had to be ignored. Note that Min's order came to Pathway at
# the same time, but its `order_time` was $124$ $(>122)$, so it was not ignored.
#
# The `cutoff` threshold doesn't have to be that tight. Setting a higher `cutoff` will
# allow you to process more late records, but the memory footprint of an interval join might
# increase then. It'll just store all records that are above the maximal seen time minus `cutoff`.

# %% [markdown]
# ## Keeping only the most up-to-date data
# Imagine you want to create a dashboard with the most recent orders. You don't want to
# display (or even store) old orders. Don't worry! Pathway can solve this problem too.
# It is enough to set the `keep_results` parameter of `common_behavior` to `False`.
# Then, all records with event time no larger than the maximal seen time minus `cutoff` will be removed from the output. Let's have a look at how it works:

# %% tags=[]
result = discounts.interval_join(
    orders,
    discounts.start_time,
    orders.order_time,
    pw.temporal.interval(0, 5),
    discounts.product == orders.product,
    behavior=pw.temporal.common_behavior(cutoff=8, keep_results=False),
).select(
    orders.customer,
    orders.product,
    discounts.price,
    orders.order_time,
    discounts.start_time,
)
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# In the end, the maximal seen time in the `orders` stream is $136$. That's why all
# records with a time less or equal to $136-8=128$ are forgotten.

# %% [markdown]
# ## Stabilizing the stream
# Another feature of temporal behavior is the ability to delay the results production.
# It can be useful if the input stream is unstable, with several possible updates to the input records, and the output that should not contain every intermediate result.
# Let's return to our shop scenario and consider a situation in which product prices are updated. For instance, it can be caused by a store employee entering an incorrect price first and later fixing it.
#
# In this example, a special column `__diff__` is used to tell Pathway whether a record
# should be added ($1$) or removed ($-1$). Remember that `pw.debug.table_from_markdown`
# is used to simulate a streaming behavior. In a real system, the way of deleting entries
# depends on an input connector you plan to use.


# %% tags=[]
discounts_with_updates = pw.debug.table_from_markdown(
    """
    id | price | product  | start_time | __time__ | __diff__
     1 |   42  | mouse    |     120    |    122   |     1
     2 |  128  | keyboard |     118    |    122   |     1
     1 |   42  | mouse    |     120    |    124   |    -1
     1 |   43  | mouse    |     120    |    124   |     1
     3 |    8  | cable    |     128    |    134   |     1
     3 |    8  | cable    |     128    |    138   |    -1
     3 |   10  | cable    |     128    |    138   |     1
     4 |  135  | keyboard |     132    |    140   |     1
     5 |   10  | cable    |     122    |    150   |     1
"""
)

result = discounts_with_updates.interval_join(
    orders,
    discounts_with_updates.start_time,
    orders.order_time,
    pw.temporal.interval(0, 5),
    discounts_with_updates.product == orders.product,
).select(
    orders.customer,
    orders.product,
    discounts_with_updates.price,
    orders.order_time,
    discounts_with_updates.start_time,
)
pw.debug.compute_and_print_update_stream(result)


# %% [markdown]
# The above script is run without any delays. Because of that, in the output the price Austin
# paid for a mouse is updated from $42$ at time $122$ to $43$ at time $124$. A similar situation
# happens to Evelyn - one price is present at times $134, 136$, and a new price is present from time $138$.
# If you are willing to wait until the results stabilize, you can
# use the `delay` parameter of temporal behavior to buffer the results before producing them.
# Let's set it to $4$ and see what happens.

# %% tags=[]
result = discounts_with_updates.interval_join(
    orders,
    discounts_with_updates.start_time,
    orders.order_time,
    pw.temporal.interval(0, 5),
    discounts_with_updates.product == orders.product,
    behavior=pw.temporal.common_behavior(delay=4),
).select(
    orders.customer,
    orders.product,
    discounts_with_updates.price,
    orders.order_time,
    discounts_with_updates.start_time,
)
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# Now, records wait for maximal seen time to become at least `record_time+4` before being
# joined. (By `record_time+4` I mean `order_time+4` for `orders` stream, `start_time+4`
# for `products` stream). Thanks to the delay, the stream can stabilize, and there are
# no price fluctuations in the output. Of course, you should choose a proper value of `delay`
# parameter depending on the times in your application.

# %% [markdown]
# You could also combine `delay` and `cutoff` to stabilize the stream and forget old records.
#
# Thank you for following this tutorial. I hope you now have a better understanding of
# the ways to control the temporal behavior of the interval join.

# ---
# title: Uncovering hidden user relationships in crypto exchanges with Fuzzy Join on streaming data
# description: An example of a cryptocurrency exchange
# author: 'przemek'
# aside: true
# article:
#   date: '2023-01-09'
#   thumbnail: '/assets/content/blog/th-mining-hidden-user-pair-activity-with-fuzzy-join.png'
#   tags: ['tutorial', 'data-pipeline']
# keywords: ['fuzzy join', 'alert', 'cryptocurrency', 'bitcoin', 'BTC', 'ETH', 'Ethereum']
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Uncovering hidden user relationships in crypto exchanges with Fuzzy Join on streaming data
#
# In this article, we want to analyze a stream of transactions in a crypto exchange.
# We find all the pairs of users A and B such as A sells B the ETH coin, and buys from B BTC in a separate transaction.
#
# First we import Pathway and load the two transactions logs.

# %%
import pathway as pw

# _MD_COMMENT_START_

# DO NOT MODIFY THIS WITHOUT MODIFYING the following file:
# public/pathway/python/pathway/tests/test_gist_user_pair_fuzzy_join.py # noqa E501


def table_from_kafka(**kwargs):
    return pw.Table.empty(
        sender=str,
        receiver=str,
        currency=str,
        amount=int,
        timestamp=pw.DateTimeUtc,
        usd_estimate=int,
    )


pw.io.kafka.read = table_from_kafka
del table_from_kafka


def table_to_postgres(*args, **kwargs):
    pass


pw.io.postgres.write = table_to_postgres
del table_to_postgres
# _MD_COMMENT_END_
transactions = pw.io.kafka.read(
    rdkafka_settings={
        "group.id": "$GROUP_NAME",
        "bootstrap.servers": "clean-panther-8776-eu1-kafka.upstash.io:9092",
        "session.timeout.ms": "6000",
    },
    topics=["eth_transactions"],
)

eth_transactions = transactions.filter(pw.this.currency == "ETH")
btc_transactions = transactions.filter(pw.this.currency == "BTC")

# %% [markdown]
# Now we just need to find all the pairs of buyers/sellers in both transactions and use our `fuzzy_match_tables` to extract the matching pairs.

# %%
# ETH movements one way
eth_movement_totals = eth_transactions.groupby(pw.this.sender, pw.this.receiver).reduce(
    user_A=pw.this.sender,
    user_B=pw.this.receiver,
    usd_total_estimate=pw.reducers.sum(pw.this.usd_estimate),
)
# BTC movements the other way
btc_movement_totals = btc_transactions.groupby(pw.this.sender, pw.this.receiver).reduce(
    user_A=pw.this.receiver,
    user_B=pw.this.sender,
    usd_total_estimate=pw.reducers.sum(pw.this.usd_estimate),
)
# We run fuzzy join on the two aggregate transaction tables.
# We project users into different spaces to avoid to
# catch a user sending both ETH and BTC to the same user.
left_projection = {"user_A": "C1", "user_B": "C2"}
right_projection = {"user_A": "C1", "user_B": "C2"}
matches = pw.ml.smart_table_ops.fuzzy_match_tables(
    eth_movement_totals,
    btc_movement_totals,
    left_projection=left_projection,
    right_projection=right_projection,
)

# The matched user pairs are now output to an output postgres table.
matched_users = matches.select(
    btc_sender=btc_movement_totals.ix(matches.right).user_B,
    btc_receiver=btc_movement_totals.ix(matches.right).user_A,
    eth_sender=eth_movement_totals.ix(matches.left).user_A,
    eth_receiver=eth_movement_totals.ix(matches.left).user_B,
    confidence=matches.weight,
)

# %% [markdown]
# We can now store the resulting table in postgres, or any other database supported by Pathway.

# %%
pw.io.postgres.write(
    matched_users,
    postgres_settings={
        "host": "localhost",
        "port": "5432",
        "dbname": "transactions",
        "user": "pathway",
        "password": "my_password",
    },
    table_name="matched_users_btc_eth_swapping",
)
"do not print cell output _MD_SKIP_";  # fmt: skip
# %% [markdown]
# Would you like to find users that match within given time-window? Take a look at
# recipes on [group-by with a tumbling window](/developers/templates/etl/suspicious_activity_tumbling_window).

# ---
# title: 'Data Model Consistency'
# description: 'Computations in Pathway are expressed as if static data were loaded into the system. Pathway delivers consistent results by explicitly reasoning about time: every processed input message bears a timestamp, and each output message specifies exactly for which input times it was computed.'
# notebook_export_path: notebooks/tutorials/consistency.ipynb
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
# # Consistency of the Pathway Data Model
#
# Computations in Pathway are expressed as if static data were loaded into the system. When streaming changes, Pathway produces inputs consistent with the state of all inputs at a given point in time.
#
# Pathway delivers consistent results by explicitly reasoning about time: every processed input message bears a timestamp, and each output message specifies exactly for which input times it was computed. In other words, each output produced by Pathway is the final answer that would have been given if all sources were read up to the indicated cutoff times, and the computation was carried in entirety. No intermediate results are shown. Updates to the outputs will be sent only when new data is input into the system.
#
# This consistency behavior requires specifying an update schedule for each input. For instance, an interactive system may react to user input every 500 milliseconds and update the data to be displayed every 10 seconds. Then, fast interactive manipulations are possible and the data shown lags by at most 10 seconds.
# %% [markdown]
# ## Running example

# Consider a Kafka topic in which each message contains simultaneous events in the banking system. Think of a cash transfer - two things occur at once: money leaves account A (a debit) and lands in account B (a credit).
# Clearly, a transaction touches two accounts and consists of two events that must be processed atomically (either both or none at all), or some outputs, e.g., account balances, will be inconsistent.
# Kafka is atomic with respect to individual messages. Thus, this event-pack design ensures atomic delivery of all events that form a transaction.
# However, processing event-packs is complex, and logic is much easier if the event-pack stream is unpacked into a single message stream (e.g., we can then group events by account and compute the balances).

# Since Kafka only guarantees the atomic processing of single messages, consistency is lost once the event-packs are flattened into a stream of individual events.
# On the other hand, Pathway guarantees consistency.
# If a Pathway computation unpacks the event-pack into individual events, all messages that form a transaction will be consistently processed, ensuring that every produced output depends on either all or no events that were grouped together into a single event-pack.
#
# Let’s simulate such a scenario by implementing a simple connector.


# %%
import pathway as pw
import time
import random

random.seed(42)


class InputStream(pw.io.python.ConnectorSubject):
    def run(self):
        for _ in range(0, 10):
            a, b = random.sample(range(1, 10), 2)
            amount = random.randint(1, 100)
            self.next_json(
                {
                    "events": [
                        {"account_id": a, "amount": -amount, "event_type": "debit"},
                        {"account_id": b, "amount": amount, "event_type": "credit"},
                    ]
                }
            )
            time.sleep(random.choice((0, 1)))


# %% [markdown]
#
# Each message within this stream encapsulates a series of events occurring within the banking system during transaction executions.
#
# Now, let's bring this data into a table using the code below:


# %%
class Schema(pw.Schema):
    events: pw.Json


input = pw.io.python.read(InputStream(), schema=Schema)
# %% [markdown]
#
# So far so good, each transaction has been loaded simultaneously into a single row of the stream.
# To make things more complicated, let's calculate the totals of all inflows and outflows for each account individually. To do so, you can first flatten all the events, divide them by type, and then group them by account number.

# %%
# pw.unwrap changes the type of the column from Optional[T] to T
events = input.flatten(pw.this.events).select(
    event_type=pw.unwrap(pw.this.events["event_type"].as_str()),
    account_id=pw.unwrap(pw.this.events["account_id"].as_int()),
    amount=pw.unwrap(pw.this.events["amount"].as_int()),
)

credits = (
    events.filter(pw.this.event_type == "credit")
    .groupby(pw.this.account_id)
    .reduce(pw.this.account_id, balance=pw.reducers.sum(pw.this.amount))
)

debits = (
    events.filter(pw.this.event_type == "debit")
    .groupby(pw.this.account_id)
    .reduce(pw.this.account_id, balance=pw.reducers.sum(pw.this.amount))
)
# %% [markdown]
#
# Events from the same transactions have been divided into independent streams. However, this doesn't imply a loss of consistency. Pathway's secret sauce lies in keeping everything consistent across dataflow nodes.
# To illustrate this fact, let's calculate the balance for each account.

# %%
account_balance = credits.join_outer(
    debits, pw.left.account_id == pw.right.account_id
).select(
    pw.this.account_id,
    balance=pw.coalesce(pw.left.balance, 0) + pw.coalesce(pw.right.balance, 0),
)

# %% [markdown]
# Given that money was transferred from one account to another without any external funds entering the system, the total balance across all accounts should sum to 0. We could, of course, use a `reduce` expression, but for a clearer understanding of the system's functioning, let's utilize Python output instead.
#
# %%
import logging

logging.basicConfig(level=logging.INFO, force=True)


class TotalBalance:
    account_balance: dict[int, int] = {}

    def on_change(self, key, row, time, is_addition):
        account_id = row["account_id"]
        balance = row["balance"]

        # When a row changes, two messages are created:
        # one indicating the removal of an old value
        # and another indicating the insertion of a new value.
        if is_addition:
            logging.info(f"Account {account_id} balance: {balance}")
            self.account_balance[account_id] = balance
        else:
            logging.info(f"Removing account {account_id} balance: {balance}")
            assert self.account_balance[account_id] == balance
            del self.account_balance[account_id]

    def on_time_end(self, time):
        logging.info(f"Total: {sum(self.account_balance.values())}")

    def on_end(self):
        self.account_balance.clear()


output = TotalBalance()

pw.io.subscribe(
    account_balance,
    on_change=output.on_change,
    on_time_end=output.on_time_end,
    on_end=output.on_end,
)


# %% [markdown]
# `TotalBalance` stores the current balance for each account. Method `on_change` updates and prints the status of a specific account whenever a change occurs in the table. Method `on_time_end` is invoked when a processing time ends.

# %% [markdown]
# All that's left is to run the pipeline:

# %%
# _MD_COMMENT_START_
pw.run(monitoring_level=pw.MonitoringLevel.NONE)
# _MD_COMMENT_END_
# _MD_SHOW_pw.run()

# %% [markdown]
# You can observe that Pathway processed all the events intended to be simultaneous at the same time, and the sum of operations always equals 0.
#
# # Persistency guarantees
#
# Pathway persists intermediate results recording the state of inputs with each saved datum. When restarted from a checkpoint, the saved state is loaded into memory first. Then all inputs are replayed starting at times recorded in the checkpoint. To avoid data loss, all streaming inputs should be buffered into a persistent message queue which allows multiple reads to recent items, such as a topic in Apache Kafka.
#
# Pathway gives "at least once" output data delivery guarantee for the data output in the different runs. More precisely, if some of the lines were outputted to a data sink in a non-closed Pathway's data batch, these output lines may appear on the output after the program has been re-run.
#
# The enterprise version of Pathway supports "exactly once" message delivery on selected combinations of input and output connectors which enable the use of the 2-phase commit protocol.

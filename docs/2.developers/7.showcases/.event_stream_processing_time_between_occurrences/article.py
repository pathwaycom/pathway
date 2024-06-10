# ---
# title:  "Out-of-Order Event Streams: Calculating Time Deltas with grouping by topic"
# description: Event stream processing
# author: 'przemek'
# aside: true
# article:
#   date: '2022-11-01'
#   thumbnail: '/assets/content/blog/th-time-between-events-in-a-multi-topic-event-stream.png'
#   tags: ['tutorial', 'data-pipeline']
# keywords: ['event stream', 'multi-topic', 'Debezium', 'ordering', 'sort']
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
# # Out-of-Order Event Streams: Calculating Time Deltas with grouping by topic
#
# We are processing a stream of events on different topics.
# We want to compute, for each event, how much time has passed since the previous event on the same topic.
# The issue is that the events do not appear *in the order of their timestamps at input*!
# With Pathway there is no need to worry about that!
#
# First we use Debezium to synchronize the input data from a database table with the following columns:
# * timestamp - logical date-time when the event happened
# * topic_id - topic in question
# * message - message content.

# %%
import pathway as pw

# _MD_COMMENT_START_
# DO NOT MODIFY THIS WITHOUT MODIFYING the following file:
# public/pathway/python/pathway/tests/test_gist_event_streaming_time_between_occurrences.py # noqa E501


def table_from_debezium(**kwargs):
    return pw.Table.empty(
        timestamp=int,  # datetime.datetime
        message=str,
        topic_id=int,
    )


pw.io.debezium.read = table_from_debezium
del table_from_debezium


def table_to_postgres(*args, **kwargs):
    pass


pw.io.postgres.write = table_to_postgres
del table_to_postgres
# _MD_COMMENT_END_
events = pw.io.debezium.read(
    rdkafka_settings={
        "group.id": "$GROUP_NAME",
        "bootstrap.servers": "clean-panther-8776-eu1-kafka.upstash.io:9092",
        "session.timeout.ms": "6000",
    },
    topics=["important_events"],
)

# %% [markdown]
# Then we sort the events from the table. Pathway provides a `sort` function to sort a table according to its `key` column: in this case we are going to sort according to the timestamps of the events. In addition, each topic is mapped to an `instance` field, which allows us to work on different streams simultaneously.
# The `prev` and `next` pointers are automatically extracted.

# %%
sorted_events = events.sort(key=events.timestamp, instance=events.topic_id)

# %% [markdown]
# Finally, we process events in order of their timestamps at input.
# %%
events_with_prev = events.with_columns(
    prev_timestamp=events.ix(sorted_events.prev, optional=True).timestamp
).filter(pw.this.prev_timestamp.is_not_none())

differences = events_with_prev.select(delta=pw.this.timestamp - pw.this.prev_timestamp)

pw.io.postgres.write(
    differences,
    postgres_settings={
        "host": "localhost",
        "port": "5432",
        "dbname": "transactions",
        "user": "pathway",
        "password": "my_password",
    },
    table_name="events_processed",
)
"do not print cell output _MD_SKIP_";  # fmt: skip

# Copyright © 2024 Pathway

import math
import random

import pandas as pd

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index


# DO NOT MODIFY THIS WITHOUT MODIFYING the following file:
# public/website3/content/2.developers/6.tutorials/.event_stream_processing_time_between_occurrences/article.py # noqa E501
def get_differences(events: pw.Table):
    sorted_events = events.sort(key=events.timestamp, instance=events.topic_id)

    events_with_prev = events.with_columns(
        prev_timestamp=events.ix(sorted_events.prev, optional=True).timestamp
    ).filter(pw.this.prev_timestamp.is_not_none())

    differences = events_with_prev.select(
        delta=pw.this.timestamp - pw.this.prev_timestamp
    )
    return sorted_events, differences


def convert_table(table):
    prev_next_table = table.select(
        pw.this.key,
        next=pw.require(pw.cast(int, pw.this.next), pw.this.next),
        prev=pw.require(pw.cast(int, pw.this.prev), pw.this.prev),
    )
    result_table = prev_next_table.join_left(
        table, prev_next_table.prev == table.key
    ).select(*prev_next_table, prev_id=table.id)
    result_table = result_table.join_left(table, result_table.next == table.key).select(
        *result_table, next_id=table.id
    )
    result_table = result_table.with_id_from(pw.this.key)

    result_table = result_table.select(
        prev=pw.this.prev_id,
        next=pw.this.next_id,
    )

    return result_table


def test_basic_example():
    events = T(
        """
            | timestamp | topic_id | name
        1   | 1000000   | topic_A  | event_B
        2   | 0         | topic_A  | event_A
        """
    )
    results_events, results_differences = get_differences(events)
    expected_results_events_pandas = pd.DataFrame(
        {
            "key": [1, 2],
            "next": [float("nan"), 1.0],
            "prev": [2.0, float("nan")],
        },
        index=[1, 2],
    )
    expected_results_events = T(
        expected_results_events_pandas, format="pandas", id_from=["key"]
    )
    expected_results_events = convert_table(expected_results_events)

    expected_results_differences_pandas = pd.DataFrame(
        {
            "delta": [1000000],
        },
        # index=[1],
    )
    expected_results_differences = T(
        expected_results_differences_pandas, format="pandas"
    )
    assert_table_equality_wo_index(results_events, expected_results_events)
    assert_table_equality_wo_index(results_differences, expected_results_differences)


def test_already_sorted():
    number_events = 10

    topic_ID = "topic"
    timestamp = 0
    timestamp_step = 100

    timestamp_array = [timestamp + i * timestamp_step for i in range(number_events)]
    topic_array = [topic_ID for i in range(number_events)]
    name_array = ["event_" + str(i) for i in range(number_events)]
    index = [i + 1 for i in range(number_events)]

    events_pandas = pd.DataFrame(
        {"timestamp": timestamp_array, "topic_id": topic_array, "name": name_array},
        index=index,
    )
    events = T(events_pandas, format="pandas")

    results_events, results_differences = get_differences(events)

    prev_array = [i - 1 for i in index]
    prev_array[0] = float("nan")
    next_array = [i + 1 for i in index]
    next_array[-1] = float("nan")

    expected_results_events_pandas = pd.DataFrame(
        {"key": index, "next": next_array, "prev": prev_array}, index=index
    )
    expected_results_events = T(expected_results_events_pandas, format="pandas")
    expected_results_events = convert_table(expected_results_events)

    assert_table_equality_wo_index(results_events, expected_results_events)

    delta_array = [timestamp_step for i in range(number_events - 1)]
    expected_results_differences_pandas = pd.DataFrame({"delta": delta_array})
    expected_results_differences = T(
        expected_results_differences_pandas, format="pandas"
    )
    assert_table_equality_wo_index(results_differences, expected_results_differences)


def test_different_topics():
    number_events = 10

    timestamp = 0
    timestamp_step = 100
    start_index_B = 10000

    timestamp_array = [timestamp + i * timestamp_step for i in range(number_events)]
    timestamp_array += [timestamp + i * timestamp_step for i in range(number_events)]

    topic_array = ["topic_A" for i in range(number_events)]
    topic_array += ["topic_B" for i in range(number_events)]

    name_array = ["event_A_" + str(i) for i in range(number_events)]
    name_array += ["event_B_" + str(i) for i in range(number_events)]

    index = [i + 1 for i in range(number_events)]
    index += [i + start_index_B + 1 for i in range(number_events)]

    events_pandas = pd.DataFrame(
        {
            "timestamp": timestamp_array,
            "topic_id": topic_array,
            "name": name_array,
        },
        index=index,
    )
    events = T(events_pandas, format="pandas")

    results_events, results_differences = get_differences(events)

    prev_array = [i - 1 for i in index]
    prev_array[0] = float("nan")
    prev_array[number_events] = float("nan")
    next_array = [i + 1 for i in index]
    next_array[-1] = float("nan")
    next_array[number_events - 1] = float("nan")

    expected_results_events_pandas = pd.DataFrame(
        {"key": index, "next": next_array, "prev": prev_array}, index=index
    )
    expected_results_events = T(expected_results_events_pandas, format="pandas")
    expected_results_events = convert_table(expected_results_events)

    assert_table_equality_wo_index(results_events, expected_results_events)

    delta_array = [timestamp_step for i in range(2 * number_events - 2)]
    index_delta = [i + 2 for i in range(number_events - 1)]
    index_delta += [i + 2 + start_index_B for i in range(number_events - 1)]
    expected_results_differences_pandas = pd.DataFrame(
        {"delta": delta_array}, index=index_delta
    )
    expected_results_differences = T(
        expected_results_differences_pandas, format="pandas"
    )
    assert_table_equality_wo_index(results_differences, expected_results_differences)


def test_randomly_sorted_different_streams():
    number_events = 10
    number_topics = 3

    timestamp_step = 100

    timestamp_array = []
    topic_array = []
    name_array = []

    for topic_index in range(number_topics):
        timestamp_array += [
            topic_index * (number_events * timestamp_step) + i * timestamp_step
            for i in range(number_events)
        ]
        topic_array += ["topic_" + str(topic_index) for i in range(number_events)]
        name_array += [
            "event_" + str(topic_index) + "_" + str(i) for i in range(number_events)
        ]
        # index_sorted += [
        #     topic_index * number_events + i + 1 for i in range(number_events)
        # ]

    index = [i + 1 for i in range(number_topics * number_events)]

    index_shuffled = [i + 1 for i in range(number_topics * number_events)]
    random.seed(0)
    random.shuffle(index_shuffled)
    timestamp_array_shuffled = [timestamp_array[i - 1] for i in index_shuffled]
    topic_array_shuffled = [topic_array[i - 1] for i in index_shuffled]
    name_array_shuffled = [name_array[i - 1] for i in index_shuffled]
    events_pandas = pd.DataFrame(
        {
            "timestamp": timestamp_array_shuffled,
            "topic_id": topic_array_shuffled,
            "name": name_array_shuffled,
        },
        index=index,
    )
    events = T(events_pandas, format="pandas")

    results_events, results_differences = get_differences(events)

    prev_array = []
    next_array = []

    for topic_index in range(number_topics):
        prev_array_aux = [topic_index * number_events + i for i in range(number_events)]
        prev_array_aux[0] = float("nan")
        prev_array += prev_array_aux
        next_array_aux = [
            topic_index * number_events + i + 2 for i in range(number_events)
        ]
        next_array_aux[-1] = float("nan")
        next_array += next_array_aux

    def get_shuffled_version(i, array):
        if math.isnan(array[i]):
            return array[i]
        else:
            return index_shuffled.index(int(array[i])) + 1

    prev_array_shuffled = [
        get_shuffled_version(i - 1, prev_array) for i in index_shuffled
    ]
    next_array_shuffled = [
        get_shuffled_version(i - 1, next_array) for i in index_shuffled
    ]

    expected_results_events_pandas = pd.DataFrame(
        {"key": index, "next": next_array_shuffled, "prev": prev_array_shuffled},
        index=index,
    )
    expected_results_events = T(expected_results_events_pandas, format="pandas")
    expected_results_events = convert_table(expected_results_events)
    assert_table_equality_wo_index(results_events, expected_results_events)

    delta_array = [timestamp_step for i in range(number_topics * (number_events - 1))]
    index_delta = []
    for topic in range(number_topics):
        index_delta += [topic * number_events + i for i in range(number_events - 1)]
    expected_results_differences_pandas = pd.DataFrame(
        {"delta": delta_array}, index=index_delta
    )
    expected_results_differences = T(
        expected_results_differences_pandas, format="pandas"
    )
    assert_table_equality_wo_index(results_differences, expected_results_differences)

# Copyright © 2024 Pathway

from __future__ import annotations

import typing

import pathway as pw
from pathway.internals import api
from pathway.tests.utils import (
    DiffEntry,
    assert_key_entries_in_stream_consistent,
    assert_stream_equal,
    run,
)


class TimeColumnInputSchema(pw.Schema):
    time: int
    value: int


def get_windows(duration: int, hop: int, time: int):
    lowest_time = time - duration
    lower_time = lowest_time - lowest_time % hop + hop

    ret: list[tuple[int, int]] = []
    while lower_time <= time:
        ret.append((lower_time, lower_time + duration))
        lower_time += hop

    return ret


def generate_buffer_output(
    input_stream: list,
    duration,
    hop,
    delay,
    cutoff,
):
    now = 0
    buffer = {}
    output = []
    for entry in input_stream:
        last_time = now
        now = max(now, entry["time"])

        to_process: list = []
        windows = get_windows(duration, hop, entry["time"])
        for _pw_window_start, _pw_window_end in windows:
            shard = None
            window = (shard, _pw_window_start, _pw_window_end)
            freeze_threshold = window[2] + cutoff
            if freeze_threshold <= now:
                continue

            threshold = window[1] + delay

            if threshold <= now:
                to_process.append((window, entry))
            else:
                key = (window, entry["value"])
                buffer[key] = entry

        bufkeys = list(buffer.keys())

        for window, value in bufkeys:
            entry = buffer[(window, value)]
            threshold = window[1] + delay
            if last_time != now and threshold <= now and threshold > last_time:
                to_process.append((window, entry))
                buffer.pop((window, value))

        output.extend(to_process)

    # flush buffer
    bufkeys = list(buffer.keys())
    for window, value in bufkeys:
        entry = buffer.pop((window, value))
        output.append((window, entry))

    return output


def test_keep_results_manual():
    value_functions = {
        "time": lambda x: (x // 2) % 17,
        "value": lambda x: x,
    }

    # 68 is 4*17, 17 is a nice number I chose arbitrarily
    # 4 comes from the fact that I wanted 2 old entries and two fresh (possibly late)
    # entries in a window

    t = pw.demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=68,
        autocommit_duration_ms=5,
        input_rate=25,
    )

    gb = t.windowby(
        t.time,
        window=pw.temporal.sliding(duration=5, hop=3),
        behavior=pw.temporal.common_behavior(delay=0, cutoff=0, keep_results=True),
    )

    expected_entries = []
    simulated_state: dict[api.Pointer, DiffEntry] = {}
    row: dict[str, api.Value]
    max_global_time = 0
    for i in range(68):
        time = (i // 2) % 17
        max_global_time = max(time, max_global_time)

        value = i
        order = i
        window_borders = get_windows(duration=5, hop=3, time=time)

        for _pw_window_start, _pw_window_end in window_borders:
            shard = None
            window = (shard, _pw_window_start, _pw_window_end)
            pk_row = {
                "_pw_window": window,
                "_pw_window_start": _pw_window_start,
                "_pw_window_end": _pw_window_end,
                "_pw_instance": shard,
            }

            entry_id = DiffEntry.create_id_from(gb, pk_row)

            max_value = value
            max_time = time

            old_entry_state = simulated_state.get(entry_id)

            if old_entry_state is not None:
                # cutoff
                if max_global_time < typing.cast(
                    int, old_entry_state.row["_pw_window_end"]
                ):
                    expected_entries.append(
                        DiffEntry.create(
                            gb,
                            pk_row,
                            order,
                            False,
                            old_entry_state.row,
                        )
                    )
                max_value = max(
                    max_value, typing.cast(int, old_entry_state.row["max_value"])
                )
                max_time = max(
                    max_time, typing.cast(int, old_entry_state.row["max_time"])
                )

            row = {
                "_pw_window_end": _pw_window_end,
                "max_value": max_value,
                "max_time": max_time,
            }
            insert_entry = DiffEntry.create(gb, pk_row, order, True, row)

            if max_global_time < typing.cast(int, insert_entry.row["_pw_window_end"]):
                simulated_state[entry_id] = insert_entry
                expected_entries.append(insert_entry)

    result = gb.reduce(
        pw.this._pw_window_end,
        max_time=pw.reducers.max(pw.this.time),
        max_value=pw.reducers.max(pw.this.value),
    )
    assert_key_entries_in_stream_consistent(expected_entries, result)

    run()


def create_windowby_scenario(duration, hop, delay, cutoff, keep_results):
    value_functions = {
        "time": lambda x: (x // 2) % 17,
        "value": lambda x: x,
    }

    # 68 is 4*17, 17 is a nice number I chose arbitrarily
    # 4 comes from the fact that I wanted 2 old entries and two fresh (possibly late)
    # entries in a window

    t = pw.demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=68,
        autocommit_duration_ms=5,
        input_rate=25,
    )

    gb = t.windowby(
        t.time,
        window=pw.temporal.sliding(duration=duration, hop=hop),
        behavior=pw.temporal.common_behavior(
            delay=delay, cutoff=cutoff, keep_results=keep_results
        ),
    )

    result = gb.reduce(
        pw.this._pw_window_end,
        max_time=pw.reducers.max(pw.this.time),
        max_value=pw.reducers.max(pw.this.value),
    )
    result.debug("res")
    return result


def generate_expected(duration, hop, delay, cutoff, keep_results, result_table):
    entries = []
    for i in range(68):
        entries.append({"value": i, "time": (i // 2) % 17})
    buf_out = generate_buffer_output(
        entries, duration=duration, hop=hop, delay=delay, cutoff=cutoff
    )

    simulated_state: dict[pw.Pointer, DiffEntry] = {}
    expected_entries = []
    max_global_time = 0

    order = 0

    for (
        window,
        in_entry,
    ) in buf_out:
        pk_row = {
            "_pw_window": window,
            "_pw_window_start": window[1],
            "_pw_window_end": window[2],
            "_pw_instance": window[0],
        }

        entry_id = DiffEntry.create_id_from(result_table, pk_row)

        order = in_entry["value"]
        max_value = in_entry["value"]
        max_window_time = in_entry["time"]
        max_global_time = max(max(in_entry["time"], window[1] + delay), max_global_time)
        old_entry_state = simulated_state.get(entry_id)

        if old_entry_state is not None:
            expected_entries.append(
                DiffEntry.create(
                    result_table,
                    pk_row,
                    order,
                    False,
                    old_entry_state.row,
                )
            )

            max_value = max(
                max_value, typing.cast(int, old_entry_state.row["max_value"])
            )
            max_window_time = max(
                max_window_time, typing.cast(int, old_entry_state.row["max_time"])
            )

        row = {
            "_pw_window_end": window[2],
            "max_value": max_value,
            "max_time": max_window_time,
        }
        insert_entry = DiffEntry.create(result_table, pk_row, order, True, row)

        simulated_state[entry_id] = insert_entry
        expected_entries.append(insert_entry)
    if not keep_results:
        for entry in simulated_state.values():
            if entry.row["_pw_window_end"] + cutoff <= max_global_time:
                expected_entries.append(entry.final_cleanup_entry())
    return expected_entries


def parameterized_test(duration, hop, delay, cutoff, keep_results):
    result_table = create_windowby_scenario(duration, hop, delay, cutoff, keep_results)
    expected = generate_expected(
        duration, hop, delay, cutoff, keep_results, result_table
    )
    assert_key_entries_in_stream_consistent(expected, result_table)
    run()


def test_keep_results():
    parameterized_test(5, 3, 0, 0, True)


def test_remove_results():
    parameterized_test(5, 3, 0, 0, False)


def test_non_zero_delay_keep_results():
    parameterized_test(5, 3, 1, 0, True)


def test_non_zero_delay_remove_results():
    parameterized_test(5, 3, 1, 0, False)


def test_non_zero_buffer_keep_results():
    parameterized_test(5, 3, 0, 1, True)


def test_non_zero_buffer_remove_results():
    parameterized_test(5, 3, 0, 1, False)


def test_non_zero_delay_non_zero_buffer_keep_results():
    parameterized_test(5, 3, 1, 1, True)


def test_high_delay_high_buffer_keep_results():
    parameterized_test(5, 3, 5, 6, True)


def test_non_zero_delay_non_zero_buffer_remove_results():
    parameterized_test(5, 3, 1, 1, False)


# method below creates expected output for exactly once tests(also below)
# it's pretty much hardcoded output, wrapped in a method to avoid code duplication
# adjusting general simulator aboveseemed pointless, given that rework of tests (that
# will also properly cover generating expected for exactly once scenarios) is
# already work in progress
def _create_expected_for_exactly_once(result):
    expected = []
    duration = 5
    for i, window_end in enumerate([2, 5, 8, 11, 14]):
        pk_row: dict = {
            "_pw_window": (None, window_end - duration, window_end),
            "_pw_window_start": window_end - duration,
            "_pw_window_end": window_end,
            "_pw_instance": None,
        }

        row: dict = {
            "_pw_window_end": window_end,
            "max_time": window_end - 1,
            "max_value": 2 * window_end - 1,
        }

        expected.append(DiffEntry.create(result, pk_row, i, True, row))

    # flush buffer
    row: dict = {
        "_pw_window_end": 17,
        "max_time": 16,
        "max_value": 67,
    }
    pk_row: dict = {
        "_pw_window": (None, 17 - duration, 17),
        "_pw_window_start": 17 - duration,
        "_pw_window_end": 17,
        "_pw_instance": None,
    }
    expected.append(DiffEntry.create(result, pk_row, 17, True, row))

    row: dict = {
        "_pw_window_end": 20,
        "max_time": 16,
        "max_value": 67,
    }
    pk_row: dict = {
        "_pw_window": (None, 20 - duration, 20),
        "_pw_window_start": 20 - duration,
        "_pw_window_end": 20,
        "_pw_instance": None,
    }
    expected.append(DiffEntry.create(result, pk_row, 20, True, row))
    return expected


def test_exactly_once():
    duration = 5
    hop = 3
    delay = 6
    cutoff = 1
    keep_results = True
    result = create_windowby_scenario(duration, hop, delay, cutoff, keep_results)
    expected = _create_expected_for_exactly_once(result)
    assert_stream_equal(expected, result)
    run()


def test_exactly_once_from_behavior():
    p = 17
    duration = 5
    hop = 3
    value_functions = {
        "time": lambda x: (x // 2) % p,
        "value": lambda x: x,
    }

    # 68 is 4*17, 17 is a nice number I chose arbitrarily
    # 4 comes from the fact that I wanted 2 old entries and two fresh (possibly late)
    # entries in a window

    t = pw.demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=4 * p,
        autocommit_duration_ms=5,
        input_rate=25,
    )
    gb = t.windowby(
        t.time,
        window=pw.temporal.sliding(duration=duration, hop=hop),
        behavior=pw.temporal.exactly_once_behavior(),
    )

    result = gb.reduce(
        pw.this._pw_window_end,
        max_time=pw.reducers.max(pw.this.time),
        max_value=pw.reducers.max(pw.this.value),
    )
    expected = _create_expected_for_exactly_once(result)
    assert_stream_equal(expected, result)
    run()

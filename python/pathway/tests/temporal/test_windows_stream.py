# Copyright © 2023 Pathway

from __future__ import annotations

import typing

import pathway as pw
from pathway.internals import api, run
from pathway.tests.utils import DiffEntry, assert_key_entries_in_stream_consistent


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
            threshold = window[1] + delay
            if threshold <= now:
                to_process.append((window, entry))
            else:
                key = (window, entry["value"])
                buffer[key] = entry

        for window, value in buffer.keys():
            entry = buffer[(window, value)]
            threshold = window[1] + delay
            if last_time != now and threshold <= now and threshold > last_time:
                to_process.append((window, entry))
        output.extend(to_process)

    # print(buffer)
    return output


def test_keep_results_manual():
    value_functions = {
        "time": lambda x: (x // 2) % 17,
        "value": lambda x: x,
    }

    # 68 is 4*17, 1
    # 7 is a nice number I chose arbitrarily
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
        behavior=pw.temporal.window_behavior(delay=0, cutoff=0, keep_results=True),
    )

    expected_entries = []
    simulated_state: dict[api.BasePointer, DiffEntry] = {}
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
                "_pw_shard": shard,
                "_pw_window_start": _pw_window_start,
                "_pw_window_end": _pw_window_end,
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
                        DiffEntry.create(gb, pk_row, order, False, old_entry_state.row)
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
    ).select(pw.this._pw_window_end, pw.this.max_time, pw.this.max_value)

    assert_key_entries_in_stream_consistent(expected_entries, result)

    run(debug=True)


def parametrized_test(duration, hop, delay, cutoff, keep_results):
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
        behavior=pw.temporal.window_behavior(
            delay=delay, cutoff=cutoff, keep_results=keep_results
        ),
    )

    entries = []
    for i in range(68):
        entries.append({"value": i, "time": (i // 2) % 17})
    buf_out = generate_buffer_output(entries, duration=duration, hop=hop, delay=delay)

    simulated_state: dict = {}
    expected_entries = []
    max_global_time = 0

    order = 0
    print(buf_out)
    for (
        window,
        in_entry,
    ) in buf_out:
        pk_row = {
            "_pw_window": window,
            "_pw_shard": window[0],
            "_pw_window_start": window[1],
            "_pw_window_end": window[2],
        }

        entry_id = DiffEntry.create_id_from(gb, pk_row)

        order = in_entry["value"]
        max_value = in_entry["value"]
        max_window_time = in_entry["time"]
        max_global_time = max(max_window_time, max_global_time)
        old_entry_state = simulated_state.get(entry_id)

        if old_entry_state is not None:
            # cutoff
            if max_global_time < typing.cast(
                int, old_entry_state.row["_pw_window_end"] + cutoff
            ):
                expected_entries.append(
                    DiffEntry.create(gb, pk_row, order, False, old_entry_state.row)
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
        insert_entry = DiffEntry.create(gb, pk_row, order, True, row)

        if (
            max_global_time
            < typing.cast(int, insert_entry.row["_pw_window_end"]) + cutoff
        ):
            simulated_state[entry_id] = insert_entry
            expected_entries.append(insert_entry)

    if not keep_results:
        for entry in simulated_state.values():
            if entry.row["_pw_window_end"] + cutoff <= max_global_time:
                expected_entries.append(entry.final_cleanup_entry())

    result = gb.reduce(
        pw.this._pw_window_end,
        max_time=pw.reducers.max(pw.this.time),
        max_value=pw.reducers.max(pw.this.value),
    ).select(pw.this._pw_window_end, pw.this.max_time, pw.this.max_value)

    assert_key_entries_in_stream_consistent(expected_entries, result)

    run(debug=True)


def test_keep_results():
    parametrized_test(5, 3, 0, 0, True)


def test_remove_results():
    parametrized_test(5, 3, 0, 0, False)


def test_non_zero_delay_keep_results():
    parametrized_test(5, 3, 1, 0, True)


def test_non_zero_delay_remove_results():
    parametrized_test(5, 3, 1, 0, False)


def test_non_zero_buffer_keep_results():
    parametrized_test(5, 3, 0, 1, True)


def test_non_zero_buffer_remove_results():
    parametrized_test(5, 3, 0, 1, False)


def test_non_zero_delay_non_zero_buffer_keep_results():
    parametrized_test(5, 3, 1, 1, True)


def test_non_zero_delay_non_zero_buffer_remove_results():
    parametrized_test(5, 3, 1, 1, False)

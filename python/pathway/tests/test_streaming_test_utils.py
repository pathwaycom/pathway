# Copyright © 2023 Pathway
from __future__ import annotations

import pytest

import pathway as pw
from pathway import demo
from pathway.internals import Schema, api
from pathway.tests.utils import (
    DiffEntry,
    assert_key_entries_in_stream_consistent,
    assert_values_in_stream_consistent,
    run,
)


def test_stream_success():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int

    value_functions = {
        "number": lambda x: x + 1,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=50,
    )

    gb = t.groupby(t.parity).reduce(t.parity, cnt=pw.reducers.count())

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 16):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    run()


def test_subscribe_consistency():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int
        number_div: int

    value_functions = {
        "number": lambda x: x + 1,
        "number_div": lambda x: (x + 1) // 2,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=100,
    )

    gb = t.groupby(t.parity).reduce(
        t.parity,
        cnt=pw.reducers.count(),
        max_number=pw.reducers.max(t.number),
        max_div=pw.reducers.max(t.number_div),
    )

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity, "max_number": i, "max_div": i // 2}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 16):
        parity = i % 2
        row = {
            "cnt": (i - 1) // 2,
            "parity": parity,
            "max_number": i - 2,
            "max_div": (i - 2) // 2,
        }
        list.append(DiffEntry.create(gb, {"parity": parity}, i, False, row))
        row = {
            "cnt": (i + 1) // 2,
            "parity": parity,
            "max_number": i,
            "max_div": i // 2,
        }
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    run()


def test_stream_test_util_should_fail_q_none():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int

    value_functions = {
        "number": lambda x: x + 1,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=50,
    )

    gb = t.groupby(t.parity).reduce(t.parity, cnt=pw.reducers.count())

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 7):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i + 7, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i + 7, True, row))
    for i in range(7, 16):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i - 4, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i - 4, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    with pytest.raises(AssertionError):
        run()


def test_stream_test_util_should_fail_empty_final_state():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int

    value_functions = {
        "number": lambda x: x + 1,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=50,
    )

    gb = t.groupby(t.parity).reduce(t.parity, cnt=pw.reducers.count())

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 18):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    with pytest.raises(AssertionError):
        run()


class ValueInputSchema(pw.Schema):
    value: int
    instance: int


def test_values_consistency_checker_1():
    value_functions = {"value": lambda x: 200 - x, "instance": lambda x: x % 3}

    t1 = pw.demo.generate_custom_stream(
        value_functions,
        schema=ValueInputSchema,
        nb_rows=200,
        autocommit_duration_ms=10,
        input_rate=400,
    )

    res = t1.groupby(pw.this.instance).reduce(
        pw.this.instance, min=pw.reducers.min(pw.this.value)
    )

    assert_values_in_stream_consistent(res, [(0, 2), (1, 1), (2, 3)])


def test_values_consistency_checker_2():
    value_functions = {"value": lambda x: x, "instance": lambda x: x % 3}

    t1 = pw.demo.generate_custom_stream(
        value_functions,
        schema=ValueInputSchema,
        nb_rows=200,
        autocommit_duration_ms=10,
        input_rate=400,
    )

    res = t1.groupby(pw.this.instance).reduce(
        pw.this.instance, max=pw.reducers.max(pw.this.value)
    )

    assert_values_in_stream_consistent(res, [(0, 198), (1, 199), (2, 197)])


def test_values_consistency_checker_raises():
    value_functions = {"value": lambda x: 200 - x, "instance": lambda x: x % 3}

    t1 = pw.demo.generate_custom_stream(
        value_functions,
        schema=ValueInputSchema,
        nb_rows=200,
        autocommit_duration_ms=10,
        input_rate=400,
    )

    res = t1.groupby(pw.this.instance).reduce(
        pw.this.instance, min=pw.reducers.min(pw.this.value)
    )

    with pytest.raises(AssertionError):
        assert_values_in_stream_consistent(res, [(0, 2), (1, 1), (2, 0)])

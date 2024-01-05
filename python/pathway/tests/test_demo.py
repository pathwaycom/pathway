# Copyright © 2024 Pathway

import pathlib

import pytest

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index, write_csv


def test_generate_custom_stream():
    value_functions = {
        "number": lambda x: x + 1,
        "name": lambda x: f"Person_{x}",
        "age": lambda x: 20 + x,
    }

    class InputSchema(pw.Schema):
        number: int
        name: str
        age: int

    table = pw.demo.generate_custom_stream(
        value_functions, schema=InputSchema, nb_rows=5, input_rate=1000
    )

    expected_md = """
        number | name | age
        1 | Person_0 | 20
        2 | Person_1 | 21
        3 | Person_2 | 22
        4 | Person_3 | 23
        5 | Person_4 | 24
    """
    expected = T(expected_md)
    assert_table_equality_wo_index(table, expected)


def test_generate_range_stream():
    table = pw.demo.range_stream(nb_rows=5, input_rate=1000)

    expected_md = """
        value
        0.0
        1.0
        2.0
        3.0
        4.0
    """
    expected = T(expected_md)
    expected = expected.select(value=pw.cast(float, pw.this.value))
    assert_table_equality_wo_index(table, expected)


def test_generate_range_stream_offset():
    table = pw.demo.range_stream(nb_rows=5, offset=10, input_rate=1000)

    expected_md = """
        value
        10.0
        11.0
        12.0
        13.0
        14.0
    """
    expected = T(expected_md)
    expected = expected.select(value=pw.cast(float, pw.this.value))
    assert_table_equality_wo_index(table, expected)


def test_generate_range_stream_negative_offset():
    table = pw.demo.range_stream(nb_rows=5, offset=-10, input_rate=1000)

    expected_md = """
        value
        -10.0
        -9.0
        -8.0
        -7.0
        -6.0
    """
    expected = T(expected_md)
    expected = expected.select(value=pw.cast(float, pw.this.value))
    assert_table_equality_wo_index(table, expected)


def test_generate_noisy_linear_stream():
    table = pw.demo.noisy_linear_stream(nb_rows=5, input_rate=1000)

    expected_md = """
        x
        0.0
        1.0
        2.0
        3.0
        4.0
    """
    expected = T(expected_md)
    # We only compare the x values because the y are random and are "high precision" floats
    expected = expected.select(x=pw.cast(float, pw.this.x))
    table = table.select(pw.this.x)
    assert_table_equality_wo_index(table, expected)


def test_demo_replay(tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int
        v: str

    table = pw.demo.replay_csv(
        str(input_path),
        schema=InputSchema,
        input_rate=1000,
    )
    expected = T(data)
    assert_table_equality_wo_index(table, expected)


def test_demo_replay_with_time(tmp_path: pathlib.Path):
    data = """
        k | v   | time
        1 | foo | 0
        2 | bar | 2
        3 | baz | 4
    """
    input_path = tmp_path / "input.csv"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int
        v: str
        time: int

    table = pw.demo.replay_csv_with_time(
        str(input_path),
        schema=InputSchema,
        time_column="time",
        unit="ns",
        autocommit_ms=10,
    )
    expected = T(data)
    assert_table_equality_wo_index(table, expected)


def test_demo_replay_with_time_wrong_schema():
    class InputSchema(pw.Schema):
        time: str
        k: int
        v: str

    with pytest.raises(ValueError, match="Invalid schema. Time columns must be int."):
        pw.demo.replay_csv_with_time(
            "/tmp",
            schema=InputSchema,
            time_column="time",
            unit="ns",
            autocommit_ms=10,
        )

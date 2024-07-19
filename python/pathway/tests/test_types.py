# Copyright © 2024 Pathway

from typing import Any

import numpy as np
import pandas as pd
import pytest

import pathway as pw
import pathway.internals.dtype as dt
from pathway.internals.schema import schema_from_types
from pathway.tests.utils import T, assert_table_equality_wo_index


def test_date_time_naive_schema():
    table = T(
        """
      |         t1          |         t2
    0 | 2023-05-15T10:13:00 | 2023-05-15T10:13:23
    """
    )
    fmt = "%Y-%m-%dT%H:%M:%S"
    table_with_datetimes = table.select(
        t1=table.t1.dt.strptime(fmt=fmt), t2=table.t2.dt.strptime(fmt=fmt)
    )
    table_with_datetimes = table_with_datetimes.with_columns(
        diff=pw.this.t1 - pw.this.t2
    )
    assert table_with_datetimes.schema._dtypes() == {
        "t1": dt.DATE_TIME_NAIVE,
        "t2": dt.DATE_TIME_NAIVE,
        "diff": dt.DURATION,
    }


def test_date_time_utc_schema():
    table = T(
        """
      |            t1             |            t2
    0 | 2023-05-15T10:13:00+01:00 | 2023-05-15T10:13:23+01:00
    """
    )
    fmt = "%Y-%m-%dT%H:%M:%S%z"
    table_with_datetimes = table.select(
        t1=table.t1.dt.strptime(fmt=fmt), t2=table.t2.dt.strptime(fmt=fmt)
    )
    table_with_datetimes = table_with_datetimes.with_columns(
        diff=pw.this.t1 - pw.this.t2
    )
    assert table_with_datetimes.schema._dtypes() == {
        "t1": dt.DATE_TIME_UTC,
        "t2": dt.DATE_TIME_UTC,
        "diff": dt.DURATION,
    }


def test_markdown_type_float():
    class TestInputSchema(pw.Schema):
        float_num: float
        should_be_float_num: float

    class TestOutputSchema(pw.Schema):
        float_num: float
        should_be_float_num: float
        test1: float
        test2: float

    t = pw.debug.table_from_markdown(
        """
        | float_num | should_be_float_num
    1   | 2.7       | 1
    2   | 3.1       | 2

    """,
        schema=TestInputSchema,
    )

    t = t.with_columns(test1=2 * t.float_num, test2=2 * t.should_be_float_num)

    expected = pw.debug.table_from_markdown(
        """
    float_num | should_be_float_num | test1 | test2
    2.7       | 1.0                 | 5.4   | 2.0
    3.1       | 2.0                 | 6.2   | 4.0
    """,
        schema=TestOutputSchema,
    )

    assert_table_equality_wo_index(t, expected)


def test_markdown_type_optional_float():
    class TestInputSchema(pw.Schema):
        float_num: float
        should_be_float_num: float | None

    class TestOutputSchema(pw.Schema):
        float_num: float
        should_be_float_num: float
        test1: float
        test2: float

    t = pw.debug.table_from_markdown(
        """
        | float_num | should_be_float_num
    1   | 2.7       | 1
    2   | 3.1       | 2
    2   | 3.1       | None

    """,
        schema=TestInputSchema,
    )

    t = t.filter(t.should_be_float_num.is_not_none())
    t = t.with_columns(test1=2 * t.float_num, test2=2 * t.should_be_float_num)

    expected = pw.debug.table_from_markdown(
        """
    float_num | should_be_float_num | test1 | test2
    2.7       | 1.0                 | 5.4   | 2.0
    3.1       | 2.0                 | 6.2   | 4.0
    """,
        schema=TestOutputSchema,
    )

    assert_table_equality_wo_index(t, expected)


def test_markdown_type_bytes():
    class TestInputSchema(pw.Schema):
        text: str
        text_bytes: bytes

    class TestOutputSchema(pw.Schema):
        text: str
        text_bytes: bytes
        bytes_as_text: str

    t = pw.debug.table_from_markdown(
        """
      | text    | text_bytes
    1 | aa      | aa
    2 | bb      | bb

    """,
        schema=TestInputSchema,
    )

    t = t.with_columns(
        bytes_as_text=pw.apply_with_type(lambda x: x.decode("utf-8"), str, t.text_bytes)
    )

    expected = pw.debug.table_from_markdown(
        """
    text | text_bytes | bytes_as_text
    aa   | aa         | aa
    bb   | bb         | bb
    """,
        schema=TestOutputSchema,
    )

    assert_table_equality_wo_index(t, expected)


def test_markdown_type_str():
    class InputNumbersAsString(pw.Schema):
        number_as_string: str

    class OutputNumbersAsString(pw.Schema):
        number_as_string: str
        ext_str: str
        converted_to_int: int

    t = pw.debug.table_from_markdown(
        """
        | number_as_string
    1 | 2
    2 | 3
    """,
        schema=InputNumbersAsString,
    )

    t = t.with_columns(ext_str=t.number_as_string + "a")
    t = t.with_columns(converted_to_int=pw.cast(int, t.number_as_string) + 1)

    expected = pw.debug.table_from_markdown(
        """
    number_as_string | ext_str | converted_to_int
    2                | 2a      | 3
    3                | 3a      | 4
    """,
        schema=OutputNumbersAsString,
    )

    assert_table_equality_wo_index(t, expected)


@pytest.mark.parametrize(
    "data,dtype",
    [
        ([0.5, 1, 2, 3], float),
        ([[1, 2, 3], [0.4, 0.4, 2], [0.1, 0.2, 0.3]], list[float]),
        ([[1, 2], [3, 4.2], [5, 6.2]], tuple[int, float]),
        (
            [1, "a", "xyz", "", [4, 3, 2], {"a": 2, "b": 3}, pw.Json(10), True, 13.5],
            pw.Json,
        ),
        ([np.array([1, 2, 3]), np.array([2.3, 3.4, 1.2])], dt.Array(None, float)),
    ],
)
def test_udfs_and_python_connectors_take_type_into_account(
    data: list[Any], dtype: type
):
    internal_dtype = dt.wrap(dtype)
    schema = schema_from_types(a=dtype)

    class Subject(pw.io.python.ConnectorSubject):
        def run(self):
            for entry in data:
                self.next(a=entry)

    @pw.udf(return_type=dtype)
    def producer(index: int):
        return data[index]

    @pw.udf(return_type=dtype)
    def assert_type_is_correct(entry):
        print(entry)
        internal_dtype.is_value_compatible(entry)
        return entry

    t1 = pw.io.python.read(Subject(), schema=schema).select(
        a=assert_type_is_correct(pw.this.a)
    )
    t2 = (
        pw.debug.table_from_pandas(pd.DataFrame({"a": np.arange(len(data))}))
        .select(a=producer(pw.this.a))
        .select(a=assert_type_is_correct(pw.this.a))
    )

    assert_table_equality_wo_index(t1, t2)

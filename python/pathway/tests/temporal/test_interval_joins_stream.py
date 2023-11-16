import pytest

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index


class TimeInputSchema(pw.Schema):
    t: int


@pytest.mark.parametrize("keep_results", [True, False])
def test_forgetting(keep_results: bool):
    if keep_results:
        pytest.xfail(reason="Records merged non-deterministically into batches")
    value_functions = {"t": lambda x: x % 5}

    t1 = pw.demo.generate_custom_stream(
        value_functions,
        schema=TimeInputSchema,
        nb_rows=10,
        autocommit_duration_ms=20,
        input_rate=20,
    )

    t2 = pw.demo.generate_custom_stream(
        value_functions,
        schema=TimeInputSchema,
        nb_rows=10,
        autocommit_duration_ms=20,
        input_rate=20,
    )

    result = t1.interval_join(
        t2,
        t1.t,
        t2.t,
        pw.temporal.interval(-0.1, 0.1),
        behavior=pw.temporal.common_behavior(0, 2, keep_results=keep_results),
    ).select(left_t=pw.left.t, right_t=pw.right.t)
    if keep_results:
        expected = T(
            """
            left_t | right_t
               0   |    0
               1   |    1
               2   |    2
               3   |    3
               3   |    3
               3   |    3
               3   |    3
               4   |    4
               4   |    4
               4   |    4
               4   |    4
            """
        )
    else:
        expected = T(
            """
            left_t | right_t
               3   |    3
               3   |    3
               3   |    3
               3   |    3
               4   |    4
               4   |    4
               4   |    4
               4   |    4
            """
        )
    assert_table_equality_wo_index(result, expected)


class TimeValueInputSchema(pw.Schema):
    t: int
    v: int


@pytest.mark.parametrize("keep_results", [True, False])
def test_forgetting_sharded(keep_results: bool):
    if keep_results:
        pytest.xfail(reason="Records merged non-deterministically into batches")
    value_functions = {"t": lambda x: (x // 2) % 5, "v": lambda x: x % 2}

    t1 = pw.demo.generate_custom_stream(
        value_functions,
        schema=TimeValueInputSchema,
        nb_rows=20,
        autocommit_duration_ms=20,
        input_rate=20,
    )

    t2 = pw.demo.generate_custom_stream(
        value_functions,
        schema=TimeValueInputSchema,
        nb_rows=20,
        autocommit_duration_ms=20,
        input_rate=20,
    )

    result = t1.interval_join(
        t2,
        t1.t,
        t2.t,
        pw.temporal.interval(-0.1, 0.1),
        t1.v == t2.v,
        behavior=pw.temporal.common_behavior(0, 2, keep_results=keep_results),
    ).select(v=pw.this.v, left_t=pw.left.t, right_t=pw.right.t)
    if keep_results:
        expected = T(
            """
            v | left_t | right_t
            0 |   0    |    0
            0 |   1    |    1
            0 |   2    |    2
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            1 |   0    |    0
            1 |   1    |    1
            1 |   2    |    2
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            """
        )
    else:
        expected = T(
            """
            v | left_t | right_t
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   3    |    3
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            0 |   4    |    4
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   3    |    3
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            1 |   4    |    4
            """
        )
    assert_table_equality_wo_index(result, expected)

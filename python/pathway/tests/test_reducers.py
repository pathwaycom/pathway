# Copyright © 2024 Pathway

from __future__ import annotations

import json
import math

import numpy as np
import pytest

import pathway as pw
from pathway.tests.utils import (
    T,
    assert_stream_equality,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_types,
)


class CustomCntAccumulator(pw.BaseCustomAccumulator):
    def __init__(self, cnt):
        self.cnt = cnt

    @classmethod
    def from_row(cls, val):
        return cls(1)

    def update(self, other):
        self.cnt += other.cnt

    def compute_result(self) -> int:
        return self.cnt


custom_cnt = pw.reducers.udf_reducer(CustomCntAccumulator)


def test_custom_count_static():
    left = T(
        """
            pet  |  owner  | age
            dog  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    left_res = left.groupby(left.pet).reduce(left.pet, cnt=custom_cnt())

    assert_table_equality(
        left_res,
        T(
            """
                pet | cnt
                dog | 3
                cat | 1
            """,
            id_from=["pet"],
        ),
    )


def test_custom_count_dynamic():
    left = T(
        """
            pet  |  owner  | age | __time__ | __diff__
            dog  | Alice   | 10  | 0        | 1
            dog  | Bob     | 9   | 0        | 1
            cat  | Alice   | 8   | 0        | 1
            dog  | Bob     | 7   | 0        | 1
            dog  | Bob     | 7   | 2        | -1
            cat  | Bob     | 9   | 4        | 1
        """
    )

    left_res = left.groupby(left.pet).reduce(left.pet, cnt=custom_cnt())

    assert_table_equality(
        left_res,
        T(
            """
                pet | cnt
                dog | 2
                cat | 2
            """,
            id_from=["pet"],
        ),
    )


def test_custom_count_null():
    left = T(
        """
            pet  |  owner  | age | __time__ | __diff__
            dog  | Alice   | 10  | 0        | 1
            dog  | Alice   | 10  | 2        | -1
        """
    )

    left_res = left.groupby(left.pet).reduce(cnt=custom_cnt())

    assert_table_equality(left_res, pw.Table.empty(cnt=int))


class CustomCntWithRetractAccumulator(CustomCntAccumulator):
    def retract(self, other) -> None:
        self.cnt -= other.cnt


custom_cnt_with_retract = pw.reducers.udf_reducer(CustomCntWithRetractAccumulator)


def test_custom_count_retract_dynamic():
    left = T(
        """
            pet  |  owner  | age | __time__ | __diff__
            dog  | Alice   | 10  | 0        | 1
            dog  | Bob     | 9   | 0        | 1
            cat  | Alice   | 8   | 0        | 1
            dog  | Bob     | 7   | 0        | 1
            dog  | Bob     | 7   | 2        | -1
            cat  | Bob     | 9   | 4        | 1
        """
    )

    left_res = left.groupby(left.pet).reduce(left.pet, cnt=custom_cnt_with_retract())

    assert_table_equality(
        left_res,
        T(
            """
                pet | cnt
                dog | 2
                cat | 2
            """,
            id_from=["pet"],
        ),
    )


def test_custom_count_retract_null():
    left = T(
        """
            pet  |  owner  | age | __time__ | __diff__
            dog  | Alice   | 10  | 0        | 1
            dog  | Alice   | 10  | 2        | -1
        """
    )

    left_res = left.groupby(left.pet).reduce(cnt=custom_cnt_with_retract())

    assert_table_equality(left_res, pw.Table.empty(cnt=int))


class CustomMeanStdevAccumulator(pw.BaseCustomAccumulator):
    def __init__(self, sum, sum2, count):
        self.sum = sum
        self.sum2 = sum2
        self.count = count

    @classmethod
    def from_row(cls, row):
        [a] = row
        return CustomMeanStdevAccumulator(a, a * a, 1)

    def update(self, other):
        self.sum += other.sum
        self.sum2 += other.sum2
        self.count += other.count

    def compute_result(self) -> tuple[float, float]:
        mean = self.sum / self.count
        stdev = math.sqrt(self.sum2 / self.count - mean**2)
        return mean, stdev


custom_mean_stdev = pw.reducers.udf_reducer(CustomMeanStdevAccumulator)


def test_custom_mean_stdev():
    left = T(
        """
            pet  |  owner  | age
            cat  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    left_res = left.groupby(left.pet).reduce(
        left.pet, mean_stdev=custom_mean_stdev(pw.this.age)
    )

    left_res = left_res.with_columns(
        mean=pw.this.mean_stdev[0], stdev=pw.this.mean_stdev[1]
    ).without(pw.this.mean_stdev)

    assert_table_equality_wo_types(
        left_res,
        T(
            """
                pet | mean | stdev
                dog | 8    | 1
                cat | 9    | 1
            """,
            id_from=["pet"],
        ),
    )


class CustomProductWithAdditionAccumulator(pw.BaseCustomAccumulator):
    """Simple reducer for which the result depends on order of updates."""

    def __init__(self, factor: int, summand: int):
        self.factor = factor
        self.summand = summand
        self.result = factor + summand

    @classmethod
    def from_row(cls, row):
        [factor, summand, event_time] = row
        return CustomProductWithAdditionAccumulator(factor, summand)

    def update(self, other):
        self.result = self.result * other.factor + other.summand

    def compute_result(self) -> int:
        return self.result

    @classmethod
    def sort_by(cls, row):
        [factor, summand, event_time] = row
        return event_time


def test_custom_sorting():
    t = T(
        """
        i |  a |  b | t | __time__
        1 |  3 |  1 | 1 |     2
        1 |  2 | -1 | 2 |     2
        1 |  2 | -2 | 4 |     4
        1 |  4 |  2 | 3 |     4
        2 | -1 |  2 | 1 |     2
        2 |  5 |  1 | 2 |     2
        2 |  3 |  2 | 3 |     2
        2 |  2 |  1 | 4 |     2
    """
    )

    custom_product_with_addition = pw.reducers.udf_reducer(
        CustomProductWithAdditionAccumulator
    )
    res = t.groupby(pw.this.i).reduce(
        pw.this.i, result=custom_product_with_addition(pw.this.a, pw.this.b, pw.this.t)
    )
    expected = T(
        """
        i | result
        1 | 58
        2 | 41
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_stateful_single_nullary():
    left = T(
        """
            pet  |  owner  | age
            dog  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    @pw.reducers.stateful_single
    def count(state: int | None) -> int:
        return state + 1 if state is not None else 1

    left_res = left.groupby(left.pet).reduce(left.pet, cnt=count())

    assert_table_equality_wo_types(
        left_res,
        T(
            """
                pet | cnt
                dog | 3
                cat | 1
            """,
            id_from=["pet"],
        ),
    )


def test_stateful_many_nullary():
    left = T(
        """
            pet  |  owner  | age
            dog  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    @pw.reducers.stateful_many
    def count(state: int | None, rows) -> int | None:
        new_state = state if state is not None else 0
        for row, cnt in rows:
            new_state += cnt
        return new_state if new_state != 0 else None

    left_res = left.groupby(left.pet).reduce(left.pet, cnt=count())

    assert_table_equality_wo_types(
        left_res,
        T(
            """
                pet | cnt
                dog | 3
                cat | 1
            """,
            id_from=["pet"],
        ),
    )


def test_stateful_single_unary():
    left = T(
        """
            pet  |  owner  | age
            dog  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    @pw.reducers.stateful_single
    def lens(state: int | None, val: str) -> int:
        if state is None:
            return len(val)
        else:
            return state + len(val)

    left_res = left.groupby(left.pet).reduce(left.pet, lens=lens(left.owner))

    assert_table_equality_wo_types(
        left_res,
        T(
            """
                pet | lens
                dog | 11
                cat | 5
            """,
            id_from=["pet"],
        ),
    )


def test_stateful_many_unary():
    left = T(
        """
            pet  |  owner  | age
            dog  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    @pw.reducers.stateful_many
    def lens(state: int | None, rows) -> int | None:
        new_state = state if state is not None else 0
        for [data], cnt in rows:
            new_state += len(data) * cnt
        return new_state if new_state != 0 else None

    left_res = left.groupby(left.pet).reduce(left.pet, lens=lens(left.owner))

    assert_table_equality_wo_types(
        left_res,
        T(
            """
                pet | lens
                dog | 11
                cat | 5
            """,
            id_from=["pet"],
        ),
    )


def test_stateful_single_binary():
    left = T(
        """
            pet  |  owner  | age
            dog  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    @pw.reducers.stateful_single
    def lens(state: int | None, s: str, i: int) -> int:
        if state is None:
            return len(s) * i
        else:
            return state + len(s) * i

    left_res = left.groupby(left.pet).reduce(left.pet, lens=lens(left.owner, left.age))

    assert_table_equality_wo_types(
        left_res,
        T(
            """
                pet | lens
                dog | 98
                cat | 40
            """,
            id_from=["pet"],
        ),
    )


def test_stateful_many_binary():
    left = T(
        """
            pet  |  owner  | age
            dog  | Alice   | 10
            dog  | Bob     | 9
            cat  | Alice   | 8
            dog  | Bob     | 7
        """
    )

    @pw.reducers.stateful_many
    def lens(state: int | None, rows) -> int | None:
        new_state = state if state is not None else 0
        for [s, i], cnt in rows:
            new_state += len(s) * i * cnt
        return new_state if new_state != 0 else None

    left_res = left.groupby(left.pet).reduce(left.pet, lens=lens(left.owner, left.age))

    assert_table_equality_wo_types(
        left_res,
        T(
            """
                pet | lens
                dog | 98
                cat | 40
            """,
            id_from=["pet"],
        ),
    )


def test_non_append_only_reducers():
    t = T(
        """
          | instance | value | source | __time__ | __diff__
        1 |     1    |   2   |    1   |     2    |     1
        2 |     1    |   3   |    2   |     2    |     1
        3 |     2    |   3   |    3   |     2    |     1
        4 |     2    |   1   |    4   |     2    |     1
        1 |     1    |   2   |    1   |     4    |    -1
        3 |     2    |   3   |    3   |     4    |    -1
        5 |     1    |  10   |    5   |     6    |     1
    """
    )
    res = t.groupby(pw.this.instance).reduce(
        instance=pw.this.instance,
        min=pw.reducers.min(pw.this.value),
        source_min=t.ix(pw.reducers.argmin(pw.this.value)).source,
        max=pw.reducers.max(pw.this.value),
        source_max=t.ix(pw.reducers.argmax(pw.this.value)).source,
        any=pw.reducers.any(pw.this.value),
        sum_i=pw.reducers.sum(pw.this.value),
        sum_f=pw.reducers.sum(pw.this.value + 0.5),
    )
    expected = T(
        """
        instance | min | source_min | max | source_max | any | sum_i | sum_f | __time__ | __diff__
            1    |  2  |      1     |  3  |     2      |  2  |   5   |  6.0  |     2    |     1
            2    |  1  |      4     |  3  |     3      |  1  |   4   |  5.0  |     2    |     1
            1    |  2  |      1     |  3  |     2      |  2  |   5   |  6.0  |     4    |    -1
            1    |  3  |      2     |  3  |     2      |  3  |   3   |  3.5  |     4    |     1
            2    |  1  |      4     |  3  |     3      |  1  |   4   |  5.0  |     4    |    -1
            2    |  1  |      4     |  1  |     4      |  1  |   1   |  1.5  |     4    |     1
            1    |  3  |      2     |  3  |     2      |  3  |   3   |  3.5  |     6    |    -1
            1    |  3  |      2     | 10  |     5      | 10  |  13   | 14.0  |     6    |     1
    """,
        id_from=["instance"],
    )
    assert_stream_equality(res, expected)


def test_append_only_reducers():
    t = T(
        """
          | instance | value | source
        1 |     1    |   2   |    1
        2 |     1    |   3   |    2
        3 |     2    |   3   |    3
        4 |     2    |   1   |    4
    """
    )
    assert t.is_append_only
    res = t.groupby(pw.this.instance).reduce(
        instance=pw.this.instance,
        min=pw.reducers.min(pw.this.value),
        source_min=t.ix(pw.reducers.argmin(pw.this.value)).source,
        max=pw.reducers.max(pw.this.value),
        source_max=t.ix(pw.reducers.argmax(pw.this.value)).source,
        any=pw.reducers.any(pw.this.value),
        sum_i=pw.reducers.sum(pw.this.value),
        sum_f=pw.reducers.sum(pw.this.value + 0.2, strict=True),
    )
    expected = T(
        """
        instance | min | source_min | max | source_max | any | sum_i | sum_f
            1    |  2  |      1     |  3  |     2      |  2  |   5   |  5.4
            2    |  1  |      4     |  3  |     3      |  1  |   4   |  4.4
    """,
        id_from=["instance"],
    )
    assert_table_equality(res, expected)


def test_reducers_on_partially_append_only_table():
    class InputSchema(pw.Schema):
        instance: int = pw.column_definition(append_only=True)
        value: int = pw.column_definition(append_only=True)
        source: int

    t = T(
        """
          | instance | value | source | __time__ | __diff__
        1 |     1    |   2   |    1   |     2    |     1
        2 |     1    |   3   |    2   |     2    |     1
        3 |     2    |   3   |    3   |     2    |     1
        4 |     2    |   1   |    4   |     2    |     1
        1 |     1    |   2   |    1   |     4    |    -1
        1 |     1    |   2   |    2   |     4    |     1
        3 |     2    |   3   |    3   |     4    |    -1
        3 |     2    |   3   |    5   |     4    |     1
    """,
        schema=InputSchema,
    )
    assert not t.is_append_only
    res = t.groupby(pw.this.instance).reduce(
        instance=pw.this.instance,
        min=pw.reducers.min(pw.this.value),
        source_min=t.ix(pw.reducers.argmin(pw.this.value)).source,
        max=pw.reducers.max(pw.this.value),
        source_max=t.ix(pw.reducers.argmax(pw.this.value)).source,
        any=pw.reducers.any(pw.this.value),
        sum=pw.reducers.sum(pw.this.source),
    )
    expected = T(
        """
        instance | min | source_min | max | source_max | any | sum | __time__ | __diff__
            1    |  2  |      1     |  3  |     2      |  2  |   3 |     2    |     1
            2    |  1  |      4     |  3  |     3      |  1  |   7 |     2    |     1
            1    |  2  |      1     |  3  |     2      |  2  |   3 |     4    |    -1
            1    |  2  |      2     |  3  |     2      |  2  |   4 |     4    |     1
            2    |  1  |      4     |  3  |     3      |  1  |   7 |     4    |    -1
            2    |  1  |      4     |  3  |     5      |  1  |   9 |     4    |     1
    """,
        id_from=["instance"],
    )
    assert_stream_equality(res, expected)


def test_distinct():
    t = T(
        """
        a | b
        1 | 2
        3 | 4
        3 | 5
        5 | 5
        5 | 6
    """
    )
    result = t.groupby().reduce(
        empty=pw.reducers.count_distinct(),
        only_a=pw.reducers.count_distinct(pw.this.a),
        only_b=pw.reducers.count_distinct(pw.this.b),
        both=pw.reducers.count_distinct(pw.this.a, pw.this.b),
    )
    expected = T(
        """
        empty | only_a | only_b | both
          1   |    3   |    4   |   5
    """
    )
    assert_table_equality_wo_index(result, expected)


def test_distinct_approximate():
    t = T(
        """
        a | b
        1 | 2
        3 | 4
        3 | 5
        5 | 5
        5 | 6
    """
    )
    result = t.groupby().reduce(
        empty=pw.reducers.count_distinct_approximate(),
        only_a=pw.reducers.count_distinct_approximate(pw.this.a),
        only_b=pw.reducers.count_distinct_approximate(pw.this.b),
        both=pw.reducers.count_distinct_approximate(pw.this.a, pw.this.b),
    )
    expected = T(
        """
        empty | only_a | only_b | both
          1   |    3   |    4   |   5
    """
    )
    assert_table_equality_wo_index(result, expected)


@pytest.mark.parametrize("precision", [4, 8, 12, 16])
def test_distinct_approximation_quality(precision, tmp_path):
    input_path = tmp_path / "input.jl"

    class InputSchema(pw.Schema):
        a: int
        g: int

    n = 100_000
    np.random.seed(42)
    g = np.random.randint(0, 10, size=n)
    a = np.random.randint(0, n // 10, size=n)
    with open(input_path, "w") as f:
        for i in range(n):
            json.dump({"a": int(a[i]), "g": int(g[i])}, f)
            f.write("\n")
    t = pw.io.jsonlines.read(
        input_path, schema=InputSchema, mode="static", autocommit_duration_ms=100
    )
    result = t.groupby(pw.this.g).reduce(
        cd=pw.reducers.count_distinct(pw.this.a),
        cda=pw.reducers.count_distinct_approximate(pw.this.a, precision=precision),
    )
    result = result.select(err=(pw.this.cd - pw.this.cda).num.abs() / pw.this.cd)
    result = result.select(err_within_margin=pw.this.err < 1.8 / 2 ** (precision / 2))
    ok_count = result.reduce(
        ok=pw.reducers.sum(pw.if_else(pw.this.err_within_margin, 1, 0))
    )
    expected = T(
        """
             ok
             10
    """
    )
    assert_table_equality_wo_index(ok_count, expected)


@pytest.mark.parametrize("strict", [False, True])
def test_float_sum_precision(strict):

    t = T(
        """
          a
        -10.1
        -10.1
          1
          1
         10.1
         10.1
    """
    )
    s = t.select(a=pw.this.a**21)
    r = s.reduce(s=pw.reducers.sum(pw.this.a, strict=strict))

    class ExpectedSchema(pw.Schema):
        s: float

    expected = T(
        """
        s
        2.0
    """,
        schema=ExpectedSchema,
    )
    assert_table_equality_wo_index(r, expected)

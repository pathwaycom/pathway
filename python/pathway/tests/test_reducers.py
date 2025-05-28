# Copyright © 2024 Pathway

from __future__ import annotations

import math

import pathway as pw
from pathway.tests.utils import (
    T,
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

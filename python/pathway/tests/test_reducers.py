# Copyright © 2023 Pathway

from __future__ import annotations

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_types


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

    def count(state: int | None) -> int:
        return state + 1 if state is not None else 1

    left_res = left.groupby(left.pet).reduce(
        left.pet, cnt=pw.reducers.stateful_single(count)
    )

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

    def count(state: int | None, rows) -> int:
        new_state = state if state is not None else 0
        for row, cnt in rows:
            new_state += cnt
        return new_state

    left_res = left.groupby(left.pet).reduce(
        left.pet, cnt=pw.reducers.stateful_many(count)
    )

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

    def lens(state: int | None, val: str) -> int:
        if state is None:
            return len(val)
        else:
            return state + len(val)

    left_res = left.groupby(left.pet).reduce(
        left.pet, lens=pw.reducers.stateful_single(lens, left.owner)
    )

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

    def lens(state: int | None, rows) -> int:
        new_state = state if state is not None else 0
        for [data], cnt in rows:
            new_state += len(data) * cnt
        return new_state

    left_res = left.groupby(left.pet).reduce(
        left.pet, lens=pw.reducers.stateful_many(lens, left.owner)
    )

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

    def lens(state: int | None, s: str, i: int) -> int:
        if state is None:
            return len(s) * i
        else:
            return state + len(s) * i

    left_res = left.groupby(left.pet).reduce(
        left.pet, lens=pw.reducers.stateful_single(lens, left.owner, left.age)
    )

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

    def lens(state: int | None, rows) -> int:
        new_state = state if state is not None else 0
        for [s, i], cnt in rows:
            new_state += len(s) * i * cnt
        return new_state

    left_res = left.groupby(left.pet).reduce(
        left.pet, lens=pw.reducers.stateful_many(lens, left.owner, left.age)
    )

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

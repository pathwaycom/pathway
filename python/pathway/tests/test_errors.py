# Copyright © 2024 Pathway

import datetime
import logging
import re
from pathlib import Path
from unittest import mock

import pytest
from dateutil import tz

import pathway as pw
from pathway.internals import table_io
from pathway.internals.parse_graph import ErrorLogSchema, G
from pathway.tests.utils import (
    T,
    assert_stream_equality_wo_index,
    assert_table_equality,
    assert_table_equality_wo_index,
    run,
)


@pw.udf
def extract_trace_line(trace: str) -> str:
    match = re.search(r"^\s*Line:\s*(.*)$", trace, re.MULTILINE)
    return match.group(1) if match else ""


def global_errors() -> pw.Table:
    return pw.global_error_log().select(
        pw.this.message, line=extract_trace_line(pw.this.trace)
    )


def test_division_by_zero():
    t1 = T(
        """
        a | b | c
        3 | 3 | 1
        4 | 0 | 2
        5 | 5 | 0
        6 | 2 | 3
    """
    )

    t2 = t1.select(x=pw.this.a // pw.this.b)
    t3 = t1.select(y=pw.this.a // pw.this.c)

    t4 = t1.select(pw.this.a, x=pw.fill_error(t2.x, -1), y=pw.fill_error(t3.y, -1))

    expected = T(
        """
        a |  x |  y
        3 |  1 |  3
        4 | -1 |  2
        5 |  1 | -1
        6 |  3 |  2
    """
    )
    expected_errors = T(
        """
        message          | line
        division by zero | t2 = t1.select(x=pw.this.a // pw.this.b)
        division by zero | t3 = t1.select(y=pw.this.a // pw.this.c)
    """,
        split_on_whitespace=False,
    )

    assert_table_equality_wo_index(
        (t4, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_removal_of_error():
    t1 = T(
        """
          | a | b | __time__ | __diff__
        1 | 6 | 2 |     2    |     1
        2 | 5 | 0 |     4    |     1
        3 | 4 | 2 |     6    |     1
        2 | 5 | 0 |     8    |    -1
    """
    )

    t2 = t1.with_columns(c=pw.this.a // pw.this.b)

    expected = T(
        """
        a | b | c
        4 | 2 | 2
        6 | 2 | 3
    """
    )
    expected_errors = T(
        """
        message          | line
        division by zero | t2 = t1.with_columns(c=pw.this.a // pw.this.b)
        division by zero | t2 = t1.with_columns(c=pw.this.a // pw.this.b)
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (t2, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_filter_with_error_in_condition():
    t1 = pw.debug.table_from_markdown(
        """
        a | b
        6 | 2
        5 | 5
        4 | 0
        3 | 3
    """
    )

    t2 = t1.with_columns(x=pw.this.a // pw.this.b)
    res = t2.filter(pw.this.x > 0)

    expected = T(
        """
        a | b | x
        3 | 3 | 1
        5 | 5 | 1
        6 | 2 | 3
    """
    )
    expected_errors = T(
        """
        message          | line
        division by zero | t2 = t1.with_columns(x=pw.this.a // pw.this.b)
        Error value encountered in filter condition, skipping the row | res = t2.filter(pw.this.x > 0)
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_filter_with_error_in_other_column():
    t1 = pw.debug.table_from_markdown(
        """
        a | b
        3 | 3
        4 | 0
        5 | 5
        6 | 2
    """
    )

    t2 = t1.with_columns(x=pw.this.a // pw.this.b)
    res = t2.filter(pw.this.a > 0)

    expected = T(
        """
        a | b |  x
        3 | 3 |  1
        4 | 0 | -1
        5 | 5 |  1
        6 | 2 |  3
    """
    )
    expected_errors = T(
        """
        message          | line
        division by zero | t2 = t1.with_columns(x=pw.this.a // pw.this.b)
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (
            res.with_columns(x=pw.fill_error(pw.this.x, -1)),
            global_errors(),
        ),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_inner_join_with_error_in_condition():
    t1 = pw.debug.table_from_markdown(
        """
        a | c
        1 | 1
        2 | 0
        3 | 1
    """
    )
    t1 = t1.with_columns(a=pw.this.a // pw.this.c)
    t2 = pw.debug.table_from_markdown(
        """
        b
        1
        1
        2
    """
    )
    res = t1.join(t2, pw.left.a == pw.right.b)
    res = res.select(pw.left.a, pw.left.c, pw.right.b)

    expected = T(
        """
        a | c | b
        1 | 1 | 1
        1 | 1 | 1
    """
    )
    expected_errors = T(
        """
        message          | line
        division by zero | t1 = t1.with_columns(a=pw.this.a // pw.this.c)
        Error value encountered in join condition, skipping the row | res = t1.join(t2, pw.left.a == pw.right.b)
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_left_join_with_error_in_condition():
    t1 = pw.debug.table_from_markdown(
        """
        a | c
        1 | 1
        2 | 0
        3 | 1
    """
    )
    t1 = t1.with_columns(a=pw.this.a // pw.this.c)
    t2 = pw.debug.table_from_markdown(
        """
        b
        1
        1
        1
        2
    """
    )
    res = t1.join_left(t2, pw.left.a == pw.right.b)
    res = res.select(a=pw.fill_error(pw.left.a, -1), c=pw.left.c, b=pw.right.b)

    expected = T(
        """
        a | c | b
        1 | 1 | 1
        1 | 1 | 1
        1 | 1 | 1
       -1 | 0 |
        3 | 1 |
    """
    )
    expected_errors = T(
        """
        message          | line
        division by zero | t1 = t1.with_columns(a=pw.this.a // pw.this.c)
        Error value encountered in join condition, skipping the row | res = t1.join_left(t2, pw.left.a == pw.right.b)
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_local_logs():
    t1 = T(
        """
        a | b | c
        3 | 3 | a
        4 | 0 | 2
        5 | 5 | 0
        6 | 2 | 3
    """
    )

    with pw.local_error_log() as error_log_1:
        t2 = t1.select(x=pw.this.a // pw.this.b)

    with pw.local_error_log() as error_log_2:
        t3 = t1.select(y=pw.this.c.str.parse_int())

    t4 = t1.select(
        pw.this.a,
        x=pw.fill_error(t2.x, -1),
        y=pw.fill_error(t3.y, -1),
        z=pw.this.a // t3.y,
    )

    assert_table_equality_wo_index(
        (
            t4.with_columns(z=pw.fill_error(pw.this.z, -1)),
            pw.global_error_log().select(pw.this.message),
            error_log_1.select(pw.this.message),
            error_log_2.select(pw.this.message),
        ),
        (
            T(
                """
            a |  x |  y |  z
            3 |  1 | -1 | -1
            4 | -1 |  2 |  2
            5 |  1 |  0 | -1
            6 |  3 |  3 |  2
            """
            ),
            T(
                """
            message
            division by zero
            """,
                split_on_whitespace=False,
            ),
            T(
                """
            message
            division by zero
            """,
                split_on_whitespace=False,
            ),
            T(
                """
            message
            parse error: cannot parse "a" to int: invalid digit found in string
            """,
                split_on_whitespace=False,
            ),
        ),
        terminate_on_error=False,
    )


def test_subscribe():
    t1 = T(
        """
        a | b
        3 | 3
        4 | 0
        5 | 5
        6 | 2
    """
    )

    t2 = t1.with_columns(x=pw.this.a // pw.this.b)
    on_change = mock.Mock()
    pw.io.subscribe(t2, on_change=on_change)
    run(terminate_on_error=False)
    assert on_change.call_count == 3


@pytest.mark.parametrize("sync", [True, False])
def test_udf(sync: bool) -> None:
    t1 = T(
        """
        a | b
        3 | 3
        4 | 0
        5 | 5
        6 | 2
    """
    )

    if sync:

        @pw.udf(deterministic=True)
        def div(a: int, b: int) -> int:
            return a // b

    else:

        @pw.udf(deterministic=True)
        async def div(a: int, b: int) -> int:
            return a // b

    t2 = t1.select(pw.this.a, x=div(pw.this.a, pw.this.b))

    res = t2.with_columns(x=pw.fill_error(pw.this.x, -1))

    expected = T(
        """
        a |  x
        3 |  1
        4 | -1
        5 |  1
        6 |  3
    """
    )
    expected_errors = T(
        """
        message | line
        ZeroDivisionError: integer division or modulo by zero | t2 = t1.select(pw.this.a, x=div(pw.this.a, pw.this.b))
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_udf_return_type():
    @pw.udf
    def f(a: int) -> str:
        if a % 2 == 0:
            return str(a) + "x"
        else:
            return a  # type: ignore[return-value]

    res = T(
        """
            a
            1
            2
            3
            4
            """
    )
    res = res.select(a=f(pw.this.a))
    res = res.select(a=pw.fill_error(pw.this.a, "xx"))

    expected = T(
        """
        a
        xx
        2x
        xx
        4x
    """
    )
    expected_err = T(
        """
        message | line
        TypeError: cannot create an object of type String from value 1 | res = res.select(a=f(pw.this.a))
        TypeError: cannot create an object of type String from value 3 | res = res.select(a=f(pw.this.a))
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_err),
        terminate_on_error=False,
    )


def test_concat():
    t1 = pw.debug.table_from_markdown(
        """
          | a | b
        1 | 1 | 2
        2 | 2 | 5
        3 | 3 | 1
    """
    )

    t2 = pw.debug.table_from_markdown(
        """
          | a | b
        1 | 1 | 3
        4 | 4 | 3
        5 | 5 | 1
    """
    )
    pw.universes.promise_are_pairwise_disjoint(t1, t2)
    res = t1.concat(t2).with_columns(
        a=pw.fill_error(pw.this.a, -1), b=pw.fill_error(pw.this.b, -1)
    )
    expected = pw.debug.table_from_markdown(
        """
         a | b  | e
        -1 | -1 | 0
         2 | 5  | 1
         3 | 1  | 1
         4 | 3  | 1
         5 | 1  | 1
    """
    ).select(a=pw.this.a // pw.this.e, b=pw.this.b // pw.this.e)

    # column e used to produce ERROR in the first row
    with pytest.warns(
        UserWarning,
        match=re.escape("duplicated entries for key ^YYY4HABTRW7T8VX2Q429ZYV70W"),
    ):
        assert_table_equality_wo_index(res, expected, terminate_on_error=False)


def test_left_join_preserving_id():
    t1 = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
    """
    )
    t2 = pw.debug.table_from_markdown(
        """
        b
        1
        1
        1
        2
    """
    )
    res = (
        t1.join_left(t2, pw.left.a == pw.right.b, id=pw.left.id)
        .select(pw.left.a, pw.right.b)
        .with_columns(b=pw.fill_error(pw.this.b, -1))
    )
    expected = pw.debug.table_from_markdown(
        """
        a |  b
        1 | -1
        2 |  2
        3 |
    """
    )
    expected_errors = T(
        """
        message | line
        duplicate key: ^X1MXHYYG4YM0DB900V28XN5T4W | t1.join_left(t2, pw.left.a == pw.right.b, id=pw.left.id)
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_restrict():
    t1 = pw.debug.table_from_markdown(
        """
          | a | b
        1 | 6 | 2
        2 | 5 | 5
        3 | 4 | 1
        4 | 3 | 3
    """
    )
    t2 = pw.debug.table_from_markdown(
        """
          | c
        1 | 1
        2 | 2
        3 | 3
        5 | 4
    """
    )
    pw.universes.promise_is_subset_of(t2, t1)
    res = t1.restrict(t2)
    res = res.select(a=pw.fill_error(res.a, -1), b=pw.fill_error(res.b, -1), c=t2.c)
    expected = pw.debug.table_from_markdown(
        """
          |  a |  b | c
        1 |  6 |  2 | 1
        2 |  5 |  5 | 2
        3 |  4 |  1 | 3
        5 | -1 | -1 | 4
    """
    )
    expected_errors = T(
        """
        message | line
        key missing in output table: ^3S2X6B265PV8BRY8MZJ91KQ0Z4 | res = t1.restrict(t2)
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_with_universe_of():
    t1 = pw.debug.table_from_markdown(
        """
          | a | b
        1 | 6 | 2
        2 | 5 | 5
        3 | 4 | 1
        4 | 3 | 3
    """
    )

    t2 = pw.debug.table_from_markdown(
        """
          | c
        1 | 1
        2 | 2
        3 | 3
        5 | 5
    """
    )
    res = t1.with_universe_of(t2)
    res = res.select(a=pw.fill_error(res.a, -1), b=pw.fill_error(res.b, -1), c=t2.c)
    expected = pw.debug.table_from_markdown(
        """
          |  a |  b | c
        1 |  6 |  2 | 1
        2 |  5 |  5 | 2
        3 |  4 |  1 | 3
        5 | -1 | -1 | 5
    """
    )
    expected_errors = T(
        """
        message | line
        key missing in output table: ^3S2X6B265PV8BRY8MZJ91KQ0Z4 | res = t1.with_universe_of(t2)
        key missing in input table: ^3S2X6B265PV8BRY8MZJ91KQ0Z4  | res = t1.with_universe_of(t2)
        key missing in output table: ^3HN31E1PBT7YHH5PWVKTZCPRJ8 | res = t1.with_universe_of(t2)
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_ix():
    t1 = pw.debug.table_from_markdown(
        """
          | a
        1 | 1
        2 | 3
        3 | 2
        4 | 2
    """
    ).with_columns(ap=pw.this.pointer_from(pw.this.a))

    t2 = pw.debug.table_from_markdown(
        """
          | c
        1 | 10
        2 | 13
    """
    )
    res = t1.select(pw.this.a, c=t2.ix(pw.this.ap).c)
    res = res.select(pw.this.a, c=pw.fill_error(res.c, -1))
    expected = pw.debug.table_from_markdown(
        """
          | a |  c
        1 | 1 | 10
        2 | 3 | -1
        3 | 2 | 13
        5 | 2 | 13
    """
    )
    expected_errors = T(
        """
        message | line
        key missing in output table: ^Z3QWT294JQSHPSR8KTPG9ECE4W | res = t1.select(pw.this.a, c=t2.ix(pw.this.ap).c)
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_remove_errors():
    t1 = T(
        """
        a | b | c
        3 | 3 | 1
        4 | 0 | 2
        5 | 5 | 0
        6 | 2 | 3
    """
    )

    t2 = t1.select(x=pw.this.a // pw.this.b)
    t3 = t1.select(y=pw.this.a // pw.this.c)

    t4 = t1.select(pw.this.a, x=t2.x, y=t3.y)

    res = t4.remove_errors()

    assert_table_equality_wo_index(
        res,
        T(
            """
            a | x | y
            3 | 1 | 3
            6 | 3 | 2
            """
        ),
        terminate_on_error=False,
    )


def test_remove_errors_identity():
    t1 = T(
        """
        a | b | c
        3 | 3 | 1
        4 | 1 | 2
        5 | 5 | 1
        6 | 2 | 3
    """
    )

    t2 = t1.select(x=pw.this.a // pw.this.b)
    t3 = t1.select(y=pw.this.a // pw.this.c)

    t4 = t1.select(pw.this.a, x=t2.x, y=t3.y)

    res = t4.remove_errors()

    assert_table_equality_wo_index(
        res,
        T(
            """
            a | x | y
            3 | 1 | 3
            4 | 4 | 2
            5 | 1 | 5
            6 | 3 | 2
            """
        ),
        terminate_on_error=False,
    )


def test_reindex_with_duplicate_key():
    t = pw.debug.table_from_markdown(
        """
        a | b
        1 | 3
        2 | 4
        3 | 5
        3 | 6
    """
    )
    res = t.with_id_from(pw.this.a).with_columns(
        a=pw.fill_error(pw.this.a, -1), b=pw.fill_error(pw.this.b, -1)
    )
    expected = (
        pw.debug.table_from_markdown(
            """
        a | b  | e
        1 | 3  | 1
        2 | 4  | 1
        3 | -1 | 0
    """
        )
        .with_id_from(pw.this.a)
        .select(a=pw.this.a // pw.this.e, b=pw.this.b // pw.this.e)
    )
    # column e used to produce ERROR in the first row
    with pytest.warns(
        UserWarning,
        match=re.escape("duplicated entries for key ^3CZ78B48PASGNT231ZECWPER90"),
    ):
        assert_table_equality_wo_index(res, expected, terminate_on_error=False)


def test_groupby_with_error_in_grouping_column():
    t1 = T(
        """
        a | b | c
        3 | 3 | 1
        4 | 0 | 2
        5 | 5 | 0
        6 | 2 | 3
        6 | 6 | 2
    """
    )
    t2 = t1.select(x=pw.this.a // pw.this.b, y=pw.this.a // pw.this.c)
    trace_line_1 = "t2 = t1.select(x=pw.this.a // pw.this.b, y=pw.this.a // pw.this.c)"
    res = t2.groupby(pw.this.x, pw.this.y)
    res = res.reduce(pw.this.x, pw.this.y, cnt=pw.reducers.count())
    trace_line_2 = "res = res.reduce(pw.this.x, pw.this.y, cnt=pw.reducers.count())"

    expected = T(
        """
        x | y | cnt
        1 | 3 |  2
        3 | 2 |  1
    """
    )
    expected_errors = T(
        f"""
        message | line
        division by zero | {trace_line_1}
        division by zero | {trace_line_1}
        Error value encountered in grouping columns, skipping the row | {trace_line_2}
        Error value encountered in grouping columns, skipping the row | {trace_line_2}
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_deduplicate_with_error_in_instance():
    t1 = T(
        """
        a | b | __time__
        2 | 1 |     2
        2 | 2 |     4
        5 | 0 |     6
        3 | 2 |     8
        1 | 1 |    10
    """
    )

    def acceptor(new_value, old_value) -> bool:
        return new_value > old_value

    res = t1.deduplicate(value=pw.this.a, instance=2 / pw.this.b, acceptor=acceptor)
    trace_line = "res = t1.deduplicate(value=pw.this.a, instance=2 / pw.this.b, acceptor=acceptor)"

    expected = T(
        """
        a | b
        3 | 2
        2 | 1
    """
    )
    expected_errors = T(
        f"""
        message | line
        division by zero | {trace_line}
        Error value encountered in deduplicate instance, skipping the row | {trace_line}
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_groupby_skip_errors():
    @pw.reducers.stateful_single  # type: ignore[arg-type]
    def stateful_sum(state: int | None, val: int) -> int:
        if state is None:
            return val
        return state + val

    t = T(
        """
        a | b |  c  | d | e
        1 | 1 | 1.5 | 1 | 1
        1 | 2 | 2.5 | 0 | 1
        1 | 3 | 3.5 | 1 | 0
        2 | 4 | 4.5 | 1 | 1
        2 | 5 | 5.5 | 1 | 0
    """
    )
    t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
    res = (
        t.groupby(
            pw.this.a,
            _skip_errors=True,
        )
        .reduce(
            pw.this.a,
            i_sum=pw.reducers.sum(pw.this.b),
            i_avg=pw.reducers.avg(pw.this.b),
            i_min=pw.reducers.min(pw.this.b),
            f_sum=pw.reducers.sum(pw.this.c),
            f_avg=pw.reducers.avg(pw.this.c),
            f_min=pw.reducers.min(pw.this.c),
            cnt=pw.reducers.count(),
            st_sum=stateful_sum(pw.this.b),
        )
        .update_types(st_sum=int)
    )
    expected = T(
        """
        a | i_sum | i_avg | i_min | f_sum | f_avg | f_min | cnt | st_sum
        1 |   4   |   2   |   1   |   4   |   2   |  1.5  |  3  |   4
        2 |   9   |  4.5  |   4   |  4.5  |  4.5  |  4.5  |  2  |   9
    """
    )
    expected_errors = T(
        """
        message          | line
        division by zero | t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
        division by zero | t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
        division by zero | t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()), (expected, expected_errors), terminate_on_error=False
    )


def test_groupby_propagate_errors():

    @pw.reducers.stateful_single  # type: ignore[arg-type]
    def stateful_sum(state: int | None, val: int) -> int:
        if state is None:
            return val
        return state + val

    t = T(
        """
        a | b |  c  | d | e
        1 | 1 | 1.5 | 1 | 1
        1 | 2 | 2.5 | 0 | 1
        1 | 3 | 3.5 | 1 | 0
        2 | 4 | 4.5 | 1 | 1
        2 | 5 | 5.5 | 1 | 0
    """
    )
    t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
    res = (
        t.groupby(pw.this.a, _skip_errors=False)
        .reduce(
            pw.this.a,
            i_sum=pw.fill_error(pw.reducers.sum(pw.this.b), -1),
            i_avg=pw.fill_error(pw.reducers.avg(pw.this.b), -1),
            i_min=pw.fill_error(pw.reducers.min(pw.this.b), -1),
            f_sum=pw.fill_error(pw.reducers.sum(pw.this.c), -1),
            f_avg=pw.fill_error(pw.reducers.avg(pw.this.c), -1),
            f_min=pw.fill_error(pw.reducers.min(pw.this.c), -1),
            cnt=pw.reducers.count(),
            st_sum=pw.fill_error(stateful_sum(pw.this.b), -1),
        )
        .update_types(st_sum=int)
    )
    expected = T(
        """
        a | i_sum | i_avg | i_min | f_sum | f_avg | f_min | cnt | st_sum
        1 |  -1   |  -1   |  -1   |  -1   |  -1   |  -1   |  3  |  -1
        2 |   9   |  4.5  |   4   |  -1   |  -1   |  -1   |  2  |   9
    """
    ).update_types(f_sum=float, f_avg=float, f_min=float)
    expected_errors = T(
        """
        message          | line
        division by zero | t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
        division by zero | t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
        division by zero | t = t.with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
        """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()), (expected, expected_errors), terminate_on_error=False
    )


def test_groupby_stateful_with_error():
    @pw.reducers.stateful_single  # type: ignore[arg-type]
    def stateful_sum(state: int | None, val: int) -> int:
        if val == 2:
            raise ValueError("Value 2 encountered")
        if state is None:
            return val
        return state + val

    t = T(
        """
        a | b
        1 | 1
        2 | 2
        1 | 3
        2 | 4
        1 | 5
    """
    )

    res = t.groupby(pw.this.a)
    res = res.reduce(pw.this.a, b=pw.fill_error(stateful_sum(pw.this.b), -1))
    res = res.update_types(b=int)

    expected = T(
        """
        a |  b
        1 |  9
        2 | -1
    """
    )
    expected_errors = T(
        """
        message | line
        ValueError: Value 2 encountered | res = res.reduce(pw.this.a, b=pw.fill_error(stateful_sum(pw.this.b), -1))
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_groupby_recovers_from_errors():

    @pw.reducers.stateful_single  # type: ignore[arg-type]
    def stateful_sum(state: int | None, val: int) -> int:
        if state is None:
            return val
        return state + val

    t = T(
        """
          | b |  c  | d | e | __time__ | __diff__
        1 | 1 | 1.5 | 1 | 1 |     2    |     1
        2 | 2 | 2.5 | 0 | 1 |     4    |     1
        3 | 3 | 3.5 | 1 | 0 |     6    |     1
        2 | 2 | 2.5 | 0 | 1 |     8    |    -1
        3 | 3 | 3.5 | 1 | 0 |    10    |    -1
    """
    ).with_columns(b=pw.this.b // pw.this.d, c=pw.this.c / pw.this.e)
    res = (
        t.groupby(_skip_errors=False)
        .reduce(
            i_sum=pw.fill_error(pw.reducers.sum(pw.this.b), -1),
            i_avg=pw.fill_error(pw.reducers.avg(pw.this.b), -1),
            i_min=pw.fill_error(pw.reducers.min(pw.this.b), -1),
            f_sum=pw.fill_error(pw.reducers.sum(pw.this.c), -1),
            f_avg=pw.fill_error(pw.reducers.avg(pw.this.c), -1),
            f_min=pw.fill_error(pw.reducers.min(pw.this.c), -1),
            cnt=pw.reducers.count(),
            st_sum=pw.fill_error(
                stateful_sum(pw.this.b), -1
            ),  # does not recover from errors
        )
        .update_types(st_sum=int)
    )
    expected = T(
        """
          | i_sum | i_avg | i_min | f_sum | f_avg | f_min | cnt | st_sum | __time__ | __diff__
        1 |   1   |   1   |   1   |  1.5  |  1.5  |  1.5  |  1  |   1    |     2    |     1
        1 |   1   |   1   |   1   |  1.5  |  1.5  |  1.5  |  1  |   1    |     4    |    -1
        1 |  -1   |  -1   |  -1   |  4.0  |  2.0  |  1.5  |  2  |  -1    |     4    |     1
        1 |  -1   |  -1   |  -1   |  4.0  |  2.0  |  1.5  |  2  |  -1    |     6    |    -1
        1 |  -1   |  -1   |  -1   | -1.0  | -1.0  | -1.0  |  3  |  -1    |     6    |     1
        1 |  -1   |  -1   |  -1   | -1.0  | -1.0  | -1.0  |  3  |  -1    |     8    |    -1
        1 |   4   |   2   |   1   | -1.0  | -1.0  | -1.0  |  2  |  -1    |     8    |     1
        1 |   4   |   2   |   1   | -1.0  | -1.0  | -1.0  |  2  |  -1    |    10    |    -1
        1 |   1   |   1   |   1   |  1.5  |  1.5  |  1.5  |  1  |  -1    |    10    |     1
    """
    ).update_types(i_avg=float)
    assert_stream_equality_wo_index(res, expected, terminate_on_error=False)


def test_deduplicate_with_error_in_value():
    t1 = T(
        """
        a | b | __time__
        2 | 1 |     2
        4 | 0 |     4
        3 | 1 |     6
    """
    ).select(a=pw.this.a // pw.this.b)

    def acceptor(new_value, old_value) -> bool:
        return new_value > old_value

    res = t1.deduplicate(value=pw.this.a, acceptor=acceptor)
    expected = T(
        """
          | a | __time__ | __diff__
        1 | 2 |     2    |     1
        1 | 2 |     6    |    -1
        1 | 3 |     6    |     1
    """
    )
    assert_table_equality_wo_index(res, expected, terminate_on_error=False)


def test_deduplicate_with_error_in_acceptor():
    t1 = T(
        """
        a | __time__
        2 |     2
        4 |     4
        3 |     6
    """
    )

    def acceptor(new_value, old_value) -> bool:
        if new_value == 4:
            raise ValueError("encountered 4")
        return new_value > old_value

    res = t1.deduplicate(value=pw.this.a, acceptor=acceptor)
    expected = T(
        """
          | a | __time__ | __diff__
        1 | 2 |     2    |     1
        1 | 2 |     6    |    -1
        1 | 3 |     6    |     1
    """
    )
    expected_errors = T(
        """
        message | line
        ValueError: encountered 4 | res = t1.deduplicate(value=pw.this.a, acceptor=acceptor)
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, global_errors()),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_unique_reducer():
    t = T(
        """
          | a | __time__ | __diff__
        1 | 1 |     2    |     1
        2 | 1 |     2    |     1
        3 | 2 |     4    |     1
        3 | 2 |     6    |    -1
    """
    )
    res = t.groupby().reduce(a=pw.fill_error(pw.reducers.unique(pw.this.a), -1))
    expected = T(
        """
          |  a | __time__ | __diff__
        1 |  1 |     2    |     1
        1 |  1 |     4    |    -1
        1 | -1 |     4    |     1
        1 | -1 |     6    |    -1
        1 |  1 |     6    |     1
    """
    )
    assert_stream_equality_wo_index(res, expected, terminate_on_error=False)


def test_update_cells():
    old = T(
        """
            | pet  | owner   | age
        1   |  1   | Alice   | 10
        2   |  2   | Alice   | 10
        """
    )
    update = T(
        """
            | pet  | owner   | age
        5   |  0   | Eve     | 10
        2   |  2   | Eve     | 10
        2   |  3   | Eve     | 10
        """
    )
    old.promise_universe_is_equal_to(update)
    res = old.update_cells(update)
    res = res.remove_errors()

    expected = T(
        """
            | pet  | owner   | age
        1   |  1   | Alice   | 10
        """
    )
    expected_errors = T(
        """
        message | line
        updating a row that does not exist, key: ^3S2X6B265PV8BRY8MZJ91KQ0Z4 | res = old.update_cells(update)
        duplicate key: ^Z3QWT294JQSHPSR8KTPG9ECE4W | res = old.update_cells(update)
        """,
        split_on_whitespace=False,
    )

    assert_table_equality_wo_index(
        (res, global_errors()), (expected, expected_errors), terminate_on_error=False
    )


def test_update_rows():
    t1 = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
    """
    ).with_id_from(pw.this.a)
    t2 = pw.debug.table_from_markdown(
        """
        a
        1
        1
        2
    """
    ).with_id_from(pw.this.a)

    res = t1.update_rows(t2)

    expected = T(
        """
        a
        2
        3
        """
    )
    expected_errors = T(
        """
        message | line
        duplicate key: ^YYY4HABTRW7T8VX2Q429ZYV70W | res = t1.update_rows(t2)
        """,
        split_on_whitespace=False,
    )

    assert_table_equality_wo_index(
        (res, global_errors()), (expected, expected_errors), terminate_on_error=False
    )


def test_with_id_from():
    table = T(
        """
        a | b
        1 | 0
        2 | 1
        """
    ).with_id_from(pw.this.a / pw.this.b)

    expected = T(
        """
        a | b
        2 | 1
        """
    )

    expected_errors = T(
        """
        message | line
        division by zero | ).with_id_from(pw.this.a / pw.this.b)
        Error value encountered in reindex as new id, skipping the row | ).with_id_from(pw.this.a / pw.this.b)
        """,
        split_on_whitespace=False,
    )

    assert_table_equality_wo_index(
        (table, global_errors()), (expected, expected_errors), terminate_on_error=False
    )


def generate_csv(path: Path):
    with open(path, "w") as f:
        f.write(
            """a,b,c
1,2,3
2,x,3
1,3,y
6,z,t
"""
        )


def generate_jsonlines(path: Path):
    with open(path, "w") as f:
        f.write(
            """{"a": 1, "b": 2, "c": 3}
{"a": 2, "b": "x", "c": 3}
{"a": 1, "b": 3, "c": "y"}
{"a": 6, "b": "1", "c": "t"}
{"a": 7, "b": 1, "c": null}
"""
        )


def test_csv_reading(tmp_path):
    class InputSchema(pw.Schema):
        a: int
        b: int
        c: int

    path = tmp_path / "input.csv"
    generate_csv(path)
    input = pw.io.csv.read(path, schema=InputSchema, mode="static")
    result = input.with_columns(
        b=pw.fill_error(pw.this.b, 0), c=pw.fill_error(pw.this.c, 0)
    )
    expected = T(
        """
        a | b | c
        1 | 2 | 3
        2 | 0 | 3
        1 | 3 | 0
        6 | 0 | 0
    """
    )
    expected_errors = T(
        """
        message
        failed to parse value "t" at field "c" according to the type int in schema: invalid digit found in string
        failed to parse value "x" at field "b" according to the type int in schema: invalid digit found in string
        failed to parse value "y" at field "c" according to the type int in schema: invalid digit found in string
        failed to parse value "z" at field "b" according to the type int in schema: invalid digit found in string
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (result, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_csv_reading_pk(tmp_path):
    class InputSchema(pw.Schema):
        a: int = pw.column_definition(primary_key=True)
        b: int = pw.column_definition(primary_key=True)
        c: int

    path = tmp_path / "input.csv"
    generate_csv(path)
    input = pw.io.csv.read(path, schema=InputSchema, mode="static")
    result = input.with_columns(
        b=pw.fill_error(pw.this.b, 0), c=pw.fill_error(pw.this.c, 0)
    )
    expected = T(
        """
        a | b | c
        1 | 2 | 3
        1 | 3 | 0
    """
    )
    expected_errors = T(
        """
        message
        error in primary key, skipping the row: failed to parse value "x" at field "b" \
according to the type int in schema: invalid digit found in string
        failed to parse value "y" at field "c" according to the type int in schema: invalid digit found in string
        error in primary key, skipping the row: failed to parse value "z" at field "b" \
according to the type int in schema: invalid digit found in string
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (result, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_jsonlines_reading(tmp_path):
    class InputSchema(pw.Schema):
        a: int
        b: int
        c: int | None

    path = tmp_path / "input.jsonlines"
    generate_jsonlines(path)
    input = pw.io.jsonlines.read(path, schema=InputSchema, mode="static")
    result = input.with_columns(
        b=pw.fill_error(pw.this.b, 0), c=pw.fill_error(pw.this.c, 0)
    )
    expected = T(
        """
        a | b | c
        1 | 2 | 3
        2 | 0 | 3
        1 | 3 | 0
        6 | 0 | 0
        7 | 1 |
    """
    )
    expected_errors = T(
        """
        message
        failed to create a field "b" with type int from json payload: "x"
        failed to create a field "b" with type int from json payload: "1"
        failed to create a field "c" with type int / None from json payload: "t"
        failed to create a field "c" with type int / None from json payload: "y"
    """,
        split_on_whitespace=False,
    ).select(message=pw.this.message.str.replace("/", "|"))
    # can't use | because it's a column sep

    assert_table_equality_wo_index(
        (result, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_jsonlines_reading_pk(tmp_path):
    class InputSchema(pw.Schema):
        a: int = pw.column_definition(primary_key=True)
        b: int = pw.column_definition(primary_key=True)
        c: int | None

    path = tmp_path / "input.jsonlines"
    generate_jsonlines(path)
    input = pw.io.jsonlines.read(path, schema=InputSchema, mode="static")
    result = input.with_columns(
        b=pw.fill_error(pw.this.b, 0), c=pw.fill_error(pw.this.c, 0)
    )
    expected = T(
        """
        a | b | c
        1 | 2 | 3
        1 | 3 | 0
        7 | 1 |
    """
    )
    expected_errors = T(
        """
        message
        error in primary key, skipping the row: failed to create a field "b" with type int from json payload: "x"
        error in primary key, skipping the row: failed to create a field "b" with type int from json payload: "1"
        failed to create a field "c" with type int / None from json payload: "y"
    """,
        split_on_whitespace=False,
    ).select(message=pw.this.message.str.replace("/", "|"))
    # can't use | because it's a column sep

    assert_table_equality_wo_index(
        (result, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_python_connector():

    class TestSchema(pw.Schema):
        a: int
        b: str

    class TestSubject(pw.io.python.ConnectorSubject):

        def run(self):
            self.next(a=10, b="a")
            self.next(a=2.3, b="cdef")
            self.next(a=3, b=11)
            self.next(a=2)

    t = pw.io.python.read(TestSubject(), schema=TestSchema).select(
        a=pw.fill_error(pw.this.a, -1), b=pw.fill_error(pw.this.b, "e")
    )
    expected = T(
        """
         a | b
        10 | a
        -1 | cdef
         3 | e
         2 | e
    """
    )
    msg_line_1 = (
        """cannot create a field "a" with type int from value 2.3. Original error: TypeError: cannot create"""
        + """ an object of type Int from value 2.3"""
    )
    msg_line_2 = (
        """cannot create a field "b" with type str from value 11. Original error: TypeError: cannot create an"""
        + """ object of type String from value 11"""
    )
    expected_errors = T(
        f"""
        message
        {msg_line_1}
        {msg_line_2}
        no value for "b" field and no default specified
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (t, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_python_connector_2():

    class TestSchema(pw.Schema):
        a: pw.DateTimeNaive
        b: pw.DateTimeUtc

    class TestSubject(pw.io.python.ConnectorSubject):

        def run(self):
            datetime_naive = datetime.datetime(2025, 5, 1, 12, 0, 0, tzinfo=None)
            datetime_utc = datetime.datetime(
                2025, 5, 1, 12, 0, 0, tzinfo=tz.gettz("Europe/Warsaw")
            )
            self.next(a=datetime_naive, b=datetime_utc)
            self.next(a=datetime_utc, b=datetime_utc)
            self.next(a=datetime_naive, b=datetime_naive)

    datetime_naive = pw.DateTimeNaive("2025-05-27T13:14:15")
    datetime_utc = pw.DateTimeUtc("2025-05-27T13:14:15+02:00")

    t = pw.io.python.read(TestSubject(), schema=TestSchema).select(
        a=pw.fill_error(pw.this.a, datetime_naive),
        b=pw.fill_error(pw.this.b, datetime_utc),
    )
    expected = T(
        """
        a                   | b
        2025-05-01T12:00:00 | 2025-05-01T12:00:00+02:00
        2025-05-01T12:00:00 | 2025-05-27T13:14:15+02:00
        2025-05-27T13:14:15 | 2025-05-01T12:00:00+02:00
    """
    ).select(
        a=pw.this.a.dt.strptime("%Y-%m-%dT%H:%M:%S"),
        b=pw.this.b.dt.strptime("%Y-%m-%dT%H:%M:%S%z"),
    )
    msg_line_1 = (
        """cannot create a field "a" with type DateTimeNaive from value 2025-05-01 12:00:00+02:00. Original"""
        + """ error: ValueError: cannot create DateTimeNaive from a datetime with timezone information. Pass a"""
        + """ datetime without timezone information or change the type to DateTimeUtc"""
    )
    msg_line_2 = (
        """cannot create a field "b" with type DateTimeUtc from value 2025-05-01 12:00:00. Original error:"""
        + """ ValueError: cannot create DateTimeUtc from a datetime without timezone information. Pass a datetime"""
        + """ with timezone information or change the type to DateTimeNaive"""
    )
    expected_errors = T(
        f"""
        message
        {msg_line_1}
        {msg_line_2}
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (t, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_python_connector_pk():
    class TestSchema(pw.Schema):
        a: int
        b: str = pw.column_definition(primary_key=True)

    class TestSubject(pw.io.python.ConnectorSubject):

        def run(self):
            self.next(a=10, b="a")
            self.next(a=2.3, b="cdef")
            self.next(a=3, b=11)
            self.next(a=2)

    t = pw.io.python.read(TestSubject(), schema=TestSchema).select(
        a=pw.fill_error(pw.this.a, -1), b=pw.fill_error(pw.this.b, "e")
    )
    expected = T(
        """
         a | b
        10 | a
        -1 | cdef
    """
    )
    msg_line_1 = (
        """cannot create a field "a" with type int from value 2.3. Original error: TypeError: cannot create"""
        + """ an object of type Int from value 2.3"""
    )
    msg_line_2 = (
        """error in primary key, skipping the row: cannot create a field "b" with type str from value 11."""
        + """ Original error: TypeError: cannot create an object of type String from value 11"""
    )
    msg_line_3 = """error in primary key, skipping the row: no value for "b" field and no default specified"""
    expected_errors = T(
        f"""
        message
        {msg_line_1}
        {msg_line_2}
        {msg_line_3}
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (t, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_global_error_first_operator():
    assert_table_equality(
        pw.global_error_log(), table_io.empty_from_schema(ErrorLogSchema)
    )


def test_clear():
    t1 = T(
        """
        a | b
        1 | 0
    """
    )

    res = t1.select(x=pw.this.a // pw.this.b).select(x=pw.fill_error(pw.this.x, -1))

    expected = T(
        """
         x
        -1
    """
    )
    expected_errors = T(
        """
        message
        division by zero
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )
    assert_table_equality_wo_index(
        (res, pw.global_error_log().select(pw.this.message)),
        (expected, expected_errors),
        terminate_on_error=False,
    )
    G.clear()
    assert_table_equality_wo_index(
        pw.global_error_log().select(pw.this.message), pw.Table.empty(message=str)
    )


def test_error_log_filtering():
    t1 = T(
        """
        a | b | c
        3 | 3 | a
        4 | 0 | 2
        5 | 5 | 0
    """
    )

    res = t1.select(
        a=pw.this.a, x=pw.this.a // pw.this.b, y=pw.this.c.str.parse_int()
    ).with_columns(x=pw.fill_error(pw.this.x, -2), y=pw.fill_error(pw.this.y, -3))
    res_errors = (
        pw.global_error_log()
        .filter(pw.this.message != "division by zero")
        .select(pw.this.message)
    )
    expected = T(
        """
    a |  x |  y
    3 |  1 | -3
    4 | -2 |  2
    5 |  1 |  0
    """
    )
    expected_errors = T(
        """
    message
    parse error: cannot parse "a" to int: invalid digit found in string
    """,
        split_on_whitespace=False,
    )
    assert_table_equality_wo_index(
        (res, res_errors),
        (expected, expected_errors),
        terminate_on_error=False,
    )


def test_error_in_error_log(caplog):
    t1 = T(
        """
        a | b
        1 | 0
    """
    )

    res = t1.select(x=pw.this.a // pw.this.b).select(x=pw.fill_error(pw.this.x, -1))

    res_errors = (
        pw.global_error_log()
        .select(m_int=pw.this.message.str.parse_int())
        .select(m_int=pw.fill_error(pw.this.m_int, -1))
    )
    expected = T(
        """
         x
        -1
    """
    )
    expected_errors = T(
        """
        m_int
         -1
    """
    )
    assert_table_equality_wo_index(
        (res, res_errors),
        (expected, expected_errors),
        terminate_on_error=False,
    )
    error_messages = [
        "division by zero",
        'parse error: cannot parse "division by zero" to int: invalid digit found in string',
    ]
    error_records = [
        record for record in caplog.records if record.levelno == logging.ERROR
    ]
    for error_message, record in zip(error_messages, error_records, strict=True):
        assert error_message in record.getMessage()


def test_repeated_keys_for_nondeterministic_udf():
    class InputSchema(pw.Schema):
        a: int = pw.column_definition(primary_key=True)
        b: int

    t = pw.debug.table_from_markdown(
        """
        a | b | __time__ | __diff__
        1 | 2 |     2    |     1
        2 | 3 |     2    |     1
        1 | 4 |     4    |     1
        2 | 3 |     6    |    -1

    """,
        schema=InputSchema,
    )

    @pw.udf
    def foo(x: int) -> int:
        return x + 1

    t.select(b=foo(pw.this.b))
    with pytest.raises(
        Exception,
        match=re.escape(
            "Expected deletion of a row with key: ^YYY4HABTRW7T8VX2Q429ZYV70W, but got insertion instead."
        ),
    ):
        pw.run_all(monitoring_level=pw.MonitoringLevel.NONE)

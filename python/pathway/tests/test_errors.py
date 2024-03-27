# Copyright © 2024 Pathway

from unittest import mock

import pytest

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index, run


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

    assert_table_equality_wo_index(
        (t4, pw.global_error_log().select(pw.this.message)),
        (
            T(
                """
            a |  x |  y
            3 |  1 |  3
            4 | -1 |  2
            5 |  1 | -1
            6 |  3 |  2
            """
            ),
            T(
                """
            message
            division by zero
            division by zero
            """,
                split_on_whitespace=False,
            ),
        ),
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

    assert_table_equality_wo_index(
        (t2, pw.global_error_log().select(pw.this.message)),
        (
            T(
                """
            a | b | c
            4 | 2 | 2
            6 | 2 | 3
        """
            ),
            T(
                """
            message
            division by zero
            division by zero
            """,
                split_on_whitespace=False,
            ),
        ),
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
    assert_table_equality_wo_index(
        (res, pw.global_error_log().select(pw.this.message)),
        (
            T(
                """
            a | b | x
            3 | 3 | 1
            5 | 5 | 1
            6 | 2 | 3
        """
            ),
            T(
                """
            message
            division by zero
            Error value encountered in filter condition, skipping the row
            """,
                split_on_whitespace=False,
            ),
        ),
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
    assert_table_equality_wo_index(
        (
            res.with_columns(x=pw.fill_error(pw.this.x, -1)),
            pw.global_error_log().select(pw.this.message),
        ),
        (
            T(
                """
            a | b |  x
            3 | 3 |  1
            4 | 0 | -1
            5 | 5 |  1
            6 | 2 |  3
        """
            ),
            T(
                """
            message
            division by zero
            """,
                split_on_whitespace=False,
            ),
        ),
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
    ).with_columns(a=pw.this.a // pw.this.c)
    t2 = pw.debug.table_from_markdown(
        """
        b
        1
        1
        2
    """
    )
    res = t1.join(t2, pw.left.a == pw.right.b).select(pw.left.a, pw.left.c, pw.right.b)
    assert_table_equality_wo_index(
        (res, pw.global_error_log().select(pw.this.message)),
        (
            T(
                """
                 a | c | b
                 1 | 1 | 1
                 1 | 1 | 1
        """
            ),
            T(
                """
            message
            division by zero
            Error value encountered in join condition, skipping the row
            """,
                split_on_whitespace=False,
            ),
        ),
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
    ).with_columns(a=pw.this.a // pw.this.c)
    t2 = pw.debug.table_from_markdown(
        """
        b
        1
        1
        1
        2
    """
    )
    res = t1.join_left(t2, pw.left.a == pw.right.b).select(
        a=pw.fill_error(pw.left.a, -1), c=pw.left.c, b=pw.right.b
    )
    assert_table_equality_wo_index(
        (res, pw.global_error_log().select(pw.this.message)),
        (
            T(
                """
                 a | c | b
                 1 | 1 | 1
                 1 | 1 | 1
                 1 | 1 | 1
                -1 | 0 |
                 3 | 1 |
        """
            ),
            T(
                """
            message
            division by zero
            Error value encountered in join condition, skipping the row
            """,
                split_on_whitespace=False,
            ),
        ),
        terminate_on_error=False,
    )

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

    assert_table_equality_wo_index(
        (t4, pw.global_error_log().select(pw.this.message)),
        (
            T(
                """
            a |  x |  y
            3 |  1 |  3
            4 | -1 |  2
            5 |  1 | -1
            6 |  3 |  2
            """
            ),
            T(
                """
            message
            division by zero
            division by zero
            """,
                split_on_whitespace=False,
            ),
        ),
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

    assert_table_equality_wo_index(
        (res, pw.global_error_log().select(pw.this.message)),
        (
            T(
                """
            a |  x
            3 |  1
            4 | -1
            5 |  1
            6 |  3
            """
            ),
            T(
                """
            message
            ZeroDivisionError: integer division or modulo by zero
            """,
                split_on_whitespace=False,
            ),
        ),
        terminate_on_error=False,
    )

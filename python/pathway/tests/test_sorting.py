# Copyright © 2024 Pathway

from __future__ import annotations

from pathway import reducers, this
from pathway.tests.utils import T, assert_table_equality, assert_table_equality_wo_index


def test_argmin():
    t = T(
        """
        hash
        931894100059286216
        1339595727108001898
        1793254503348522670
        97653197660818656
        301593703415097707
        """,
    )
    r = t.reduce(key=reducers.argmin(t.hash))
    assert_table_equality_wo_index(
        r,
        T(
            """
            key
            3
            """,
        ).with_columns(key=t.pointer_from(this.key)),
    )


def test_prevnext_single_instance():
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    nodes = T(
        """
            | key | instance
        1 |  1  | 42
        2 |  5  | 42
        3 |  3  | 42
        4 |  8  | 42
        5 |  2  | 42
        """
    )
    result = nodes.sort(key=nodes.key, instance=nodes.instance)

    assert_table_equality(
        result,
        T(
            """
                | next | prev
            1   |  5   |
            2   |  4   | 3
            3   |  2   | 5
            4   |      | 2
            5   |  3   | 1
            """,
        ).select(
            prev=nodes.pointer_from(this.prev, optional=True),
            next=nodes.pointer_from(this.next, optional=True),
        ),
    )


def test_prevnext_many_instance():
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    nodes = T(
        """
          | key | instance
        1 |  1  | 42
        2 |  1  | 28
        3 |  5  | 42
        4 |  5  | 28
        5 |  3  | 42
        6 |  3  | 28
        7 |  8  | 42
        8 |  8  | 28
        9 |  2  | 42
        10|  2  | 28
        """
    )
    result = nodes.sort(key=nodes.key, instance=nodes.instance)

    assert_table_equality(
        result,
        T(
            """
                | next | prev
            1   |  9   |
            2   |  10   |
            3   |  7   | 5
            4   |  8   | 6
            5   |  3   | 9
            6   |  4   | 10
            7   |      | 3
            8   |      | 4
            9   |  5   | 1
            10   |  6   | 2
            """,
        ).select(
            prev=nodes.pointer_from(this.prev, optional=True),
            next=nodes.pointer_from(this.next, optional=True),
        ),
    )

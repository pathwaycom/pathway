# Copyright © 2024 Pathway

from __future__ import annotations

from random import randint
from typing import Any

import pandas as pd
import pytest

from pathway import this
from pathway.debug import table_to_pandas
from pathway.internals.api import unsafe_make_pointer
from pathway.tests.utils import T, assert_table_equality, assert_table_equality_wo_types

from .examples.linked_list import linked_list_transformer, reverse_linked_list
from .examples.skiplist import compute_shortcuts


@pytest.mark.xfail(reason="passing lambda inside a transformer")
def test_skip_list_short():
    nodes = T(
        """
      | next | val
    1 | 2    | 10
    2 | 3    | 20
    3 | 4    | 30
    4 | 5    | 40
    5 | 6    | 50
    6 | 7    | 60
    7 | 8    | 70
    8 |      | 80
    """,
    )
    nodes = nodes.select(
        next=nodes.pointer_from(this.next, optional=True), val=this.val
    )
    expected = T(
        """
      | shortcut_next | shortcut_val
    1 | 6 | 150
    2 | 6 | 140
    3 | 4 | 30
    4 | 6 | 90
    5 | 6 | 50
    6 |   | 210
    7 | 8 | 70
    8 |   | 80
    """,
    ).with_columns(shortcut_next=nodes.pointer_from(this.shortcut_next, optional=True))

    ret = compute_shortcuts(nodes, agg_fun=lambda x, y: x + y)
    expected = expected.with_universe_of(ret)
    assert_table_equality_wo_types(ret, expected)


def _generate_list(num_rows, val_gen=lambda: randint(0, 1000)):
    df = pd.DataFrame(
        {
            "next": pd.array(
                [unsafe_make_pointer(i + 1) for i in range(1, num_rows)] + [None],
                dtype=object,
            ),
        }
    )
    df.index += 1
    nodes = T(
        df,
        format="pandas",
        unsafe_trusted_ids=True,
    )

    if val_gen is None:
        return nodes

    df = pd.DataFrame(
        {
            "val": [val_gen() for i in range(num_rows)],
        }
    )
    df.index += 1

    vals = T(
        df,
        format="pandas",
        unsafe_trusted_ids=True,
    )

    return nodes + vals.with_universe_of(nodes)


@pytest.mark.xfail(reason="passing lambda inside a transformer")
def test_skip_list_big():
    num_rows = 1000
    mod = 100000007
    nodes = _generate_list(num_rows)
    ret = compute_shortcuts(nodes, agg_fun=lambda x, y: (x + y) % mod)
    ret = table_to_pandas(ret)
    assert set(map(int, ret.index)) == set(range(1, num_rows + 1))
    for val in ret["shortcut_val"]:
        assert val >= 0 and val < mod


def test_linked_list_len():
    nodes = T(
        """
      | next
    1 | 2
    2 | 3
    3 | 4
    4 | 5
    5 | 6
    6 | 7
    7 | 8
    8 |
    """,
    )
    nodes = nodes.select(next=nodes.pointer_from(this.next, optional=True))
    expected = T(
        """
      | len
    1 | 8
    2 | 7
    3 | 6
    4 | 5
    5 | 4
    6 | 3
    7 | 2
    8 | 1
    """
    )

    ret = linked_list_transformer(linked_list=nodes).linked_list

    assert_table_equality_wo_types(ret.select(ret.len), expected)


def test_linked_list_reversal():
    nodes = T(
        """
      | next
    1 | 2
    2 | 3
    3 | 4
    4 | 5
    5 | 6
    6 | 7
    7 | 8
    8 |
    """,
    )
    nodes = nodes.select(next=nodes.pointer_from(this.next, optional=True))
    expected = T(
        """
      | next
    1 |
    2 | 1
    3 | 2
    4 | 3
    5 | 4
    6 | 5
    7 | 6
    8 | 7
    """,
    ).select(next=nodes.pointer_from(this.next, optional=True))

    ret = reverse_linked_list(nodes)

    assert_table_equality(ret, expected)


def test_linked_list_forward():
    nodes = T(
        """
      | next
    1 | 2
    2 | 3
    3 | 4
    4 | 5
    5 | 6
    6 | 7
    7 | 8
    8 |
    """,
    )
    nodes = nodes.select(next=nodes.pointer_from(this.next, optional=True))

    linked_list = linked_list_transformer(linked_list=nodes).linked_list

    queries = T(
        """
    node_id | steps
    1       | 0
    2       | 1
    6       | 2
    6       | 3
    8       | 0
    8       | 2
        """,
    ).with_columns(node_id=nodes.pointer_from(this.node_id))
    ret = queries.select(result=linked_list.ix(queries.node_id).forward(queries.steps))
    expected = (
        T(
            """
      | result
    0 | 1
    1 | 3
    2 | 8
    3 |
    4 | 8
    5 |
    """,
        )
        .update_types(result=Any)
        .select(result=nodes.pointer_from(this.result, optional=True))
    )
    assert_table_equality(ret, expected)

# Copyright © 2024 Pathway

from __future__ import annotations

from pathway import (
    ClassArg,
    Table,
    attribute,
    input_attribute,
    output_attribute,
    reducers,
    this,
    transformer,
)
from pathway.internals import dtype as dt
from pathway.stdlib.indexing.sorting import (
    binsearch_oracle,
    build_sorted_index,
    filter_cmp_helper,
    filter_smallest_k,
    prefix_sum_oracle,
)
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


@transformer
class _verify_tree:
    class tree(ClassArg):
        parent = input_attribute()
        left = input_attribute()
        right = input_attribute()
        instance = input_attribute()
        key = input_attribute()

        @output_attribute
        def test(self) -> bool:
            if self.left is not None:
                assert self.transformer.tree[self.left].max_in_subtree < self.key
                assert self.transformer.tree[self.left].parent == self.id
                assert self.transformer.tree[self.left].instance == self.instance
            if self.right is not None:
                assert self.transformer.tree[self.right].min_in_subtree > self.key
                assert self.transformer.tree[self.right].parent == self.id
                assert self.transformer.tree[self.right].parent == self.id
            return True

        @attribute
        def max_in_subtree(self) -> int:
            ret = self.key
            if self.left is not None:
                ret = max(ret, self.transformer.tree[self.left].max_in_subtree)
            if self.right is not None:
                ret = max(ret, self.transformer.tree[self.right].max_in_subtree)
            return ret

        @attribute
        def min_in_subtree(self) -> int:
            ret = self.key
            if self.left is not None:
                ret = min(ret, self.transformer.tree[self.left].min_in_subtree)
            if self.right is not None:
                ret = min(ret, self.transformer.tree[self.right].min_in_subtree)
            return ret


def test_build_search_tree_single_instance():
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    nodes = T(
        """
          | key | instance
        1 |  11 | 42
        2 |  12 | 42
        3 |  13 | 42
        4 |  14 | 42
        5 |  15 | 42
        """,
    )
    result = build_sorted_index(nodes)

    assert_table_equality(_verify_tree(tree=result.index).tree, nodes.select(test=True))
    assert_table_equality_wo_index(
        nodes.select(this.key, this.instance),
        result.index.select(this.key, this.instance),
    )
    assert_table_equality(
        result.index.filter(this.parent.is_none())
        .groupby(this.instance)
        .reduce(cnt=reducers.count()),
        nodes.groupby(this.instance).reduce(cnt=1),
    )


def test_build_search_tree_many_instances():
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    nodes = T(
        """
          | key | instance
        0 |  10 | 42
        1 |  11 | 28
        2 |  12 | 42
        3 |  13 | 28
        4 |  14 | 42
        5 |  15 | 28
        6 |  16 | 42
        7 |  17 | 28
        8 |  18 | 42
        9 |  19 | 28
        """,
    )
    result = build_sorted_index(nodes)
    assert_table_equality(_verify_tree(tree=result.index).tree, nodes.select(test=True))
    assert_table_equality_wo_index(
        nodes.select(this.key, this.instance),
        result.index.select(this.key, this.instance),
    )
    assert_table_equality(
        result.index.filter(this.parent.is_none())
        .groupby(this.instance)
        .reduce(cnt=reducers.count()),
        nodes.groupby(this.instance).reduce(cnt=1),
    )


def test_cmp_helper_single_instance():
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    nodes = T(
        """
          | key | instance
        1 |  11 | 42
        2 |  12 | 42
        3 |  13 | 42
        4 |  14 | 42
        5 |  15 | 42
        """
    )
    filter_val = T(
        """
    instance  | val
          42  | 13
    """,
    )
    result = filter_cmp_helper(
        filter_val=filter_val.with_id_from(filter_val.instance),
        **build_sorted_index(nodes),
    )
    assert_table_equality(
        result,
        T(
            """
comparison_ret
1    -1
2    -1
3    0
4    1
5    1
    """
        ),
    )


def test_cmp_helper_many_instances():
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    nodes = T(
        """
          | key | instance
        0 | 10  | 42
        1 | 11  | 28
        2 | 12  | 42
        3 | 13  | 28
        4 | 14  | 42
        5 | 15  | 28
        6 | 16  | 42
        7 | 17  | 28
        8 | 18  | 42
        9 | 19  | 28
        """
    )
    filter_val = T(
        """
    instance  | val
    42 | 14
    28 | 17
    """,
    )
    result = filter_cmp_helper(
        filter_val=filter_val.with_id_from(filter_val.instance),
        **build_sorted_index(nodes),
    )
    assert_table_equality(
        result,
        T(
            """
comparison_ret
0    -1
1    -1
2    -1
3    -1
4    0
5    -1
6    1
7    0
8    1
9    1
    """
        ),
    )


def test_prefix_sum_oracle_single_instance():
    items = T(
        """
            | key | val | instance
        1   |  11 |  15 | 42
        2   |  12 |  14 | 42
        3   |  13 |  13 | 42
        4   |  14 |  12 | 42
        5   |  15 |  11 | 42
        """
    )
    search_tree = build_sorted_index(nodes=items)
    search_tree.index += Table.from_columns(items.val)
    oracle = prefix_sum_oracle(**search_tree)

    query = T(
        """
            | query | instance
        1   | 28    | 42
        2   | 29    | 42
        3   | 30    | 42
        4   | 0     | 42
        5   | 10000 | 42
        """
    )

    oracle_by_query = query.select(
        prefix_sum_upperbound=oracle.ix_ref(query.instance).prefix_sum_upperbound
    )
    assert_table_equality(
        query.select(res=oracle_by_query.prefix_sum_upperbound(query.query)),
        T(
            """
                | res
            1   | 2
            2   | 3
            3   | 3
            4   | 1
            5   |
            """,
        ).select(res=items.pointer_from(this.res, optional=True)),
    )


def test_prefix_sum_oracle_many_instances():
    items = T(
        """
            | key | val | instance
        0   |  1  |  15 | 1
        1   |  2  |  14 | 1
        2   |  3  |  13 | 1
        3   |  4  |  12 | 1
        4   |  5  |  11 | 1
        5   |  1  |  14 | 2
        6   |  2  |  14 | 2
        7   |  3  |  13 | 2
        8   |  4  |  12 | 2
        9   |  5  |  11 | 2
        """
    )
    search_tree = build_sorted_index(nodes=items)
    search_tree.index += Table.from_columns(items.val)
    oracle = prefix_sum_oracle(**search_tree)

    query = T(
        """
            | query | instance
        0   | 28    | 1
        1   | 29    | 1
        2   | 30    | 1
        3   | 0     | 1
        4   | 10000 | 1
        5   | 28    | 2
        6   | 29    | 2
        7   | 30    | 2
        8   | 0     | 2
        9   | 10000 | 2
        """
    )
    assert_table_equality(
        query.select(
            res=oracle.ix_ref(query.instance).prefix_sum_upperbound(query.query)
        ),
        T(
            """
                | res
            0   | 1
            1   | 2
            2   | 2
            3   | 0
            4   |
            5   | 7
            6   | 7
            7   | 7
            8   | 5
            9   |
            """
        ).select(res=items.pointer_from(this.res, optional=True)),
    )


def test_prefix_sum_oracle2():
    items = T(
        """
            | key | val | instance
        1   |  21 | 15  |  42
        2   |  22 | 14  |  42
        3   |  23 | 13  |  42
        4   |  24 | 12  |  42
        5   |  25 | 11  |  42
        """
    )
    search_tree = build_sorted_index(nodes=items)
    search_tree.index += Table.from_columns(items.val)
    oracle = prefix_sum_oracle(**search_tree)

    query = T(
        """
        query | instance
        30    | 42
        """,
    )

    result = filter_cmp_helper(
        filter_val=query.select(
            val=items.ix(
                oracle.ix_ref(query.instance).prefix_sum_upperbound(query.query),
                optional=True,
            ).key
        ).with_id_from(
            query.instance,
        ),
        **search_tree,
    )
    assert_table_equality(
        result,
        T(
            """
                | comparison_ret
            1   | -1
            2   | -1
            3   | 0
            4   | 1
            5   | 1
            """
        ),
    )


def test_comparisons():
    items = T(
        """
    | val | instance
    1 | 15  | 42
    2 | 14  | 42
    3 | 13  | 42
    4 | 12  | 42
    5 | 11  | 42
"""
    )
    sorted_index = build_sorted_index(nodes=items.select(items.instance, key=items.val))

    filter_val = T(
        """
    instance | val
    42        | 13
    """,
    )
    filter_val = filter_val.with_id_from(filter_val.instance)

    result = filter_cmp_helper(filter_val=filter_val, **sorted_index)

    assert_table_equality(
        result,
        T(
            """
    comparison_ret
1                 1
2                 1
3                 0
4                -1
5                -1
        """
        ),
    )


def test_binsearch_oracle_single_instance():
    items = T(
        """
            | key | instance
        1   |  15 | 42
        2   |  14 | 42
        3   |  13 | 42
        4   |  12 | 42
        5   |  11 | 42
        """
    )
    search_tree = build_sorted_index(nodes=items)

    oracle = binsearch_oracle(**search_tree)

    query = T(
        """
            | query | instance
        0   | 10    | 42
        1   | 11    | 42
        2   | 12    | 42
        3   | 15    | 42
        4   | 16    | 42
        """
    )

    indexed = oracle.ix_ref(query.instance)
    oracle_by_query = query.select(
        lowerbound=indexed.lowerbound,
        upperbound=indexed.upperbound,
    )

    assert_table_equality(
        query.select(res=oracle_by_query.lowerbound(query.query)),
        T(
            """
                | res
            0   |
            1   | 5
            2   | 4
            3   | 1
            4   | 1
            """,
        ).select(res=items.pointer_from(this.res, optional=True)),
    )

    assert_table_equality(
        query.select(res=oracle_by_query.upperbound(query.query)),
        T(
            """
                | res
            0   | 5
            1   | 5
            2   | 4
            3   | 1
            4   |
            """,
        ).select(res=items.pointer_from(this.res, optional=True)),
    )


def test_compute_max_key():
    items = T(
        """
            | key | val | instance
        1   | 21  | 15  |  42
        2   | 22  | 14  |  42
        3   | 23  | 13  |  42
        4   | 24  | 12  |  42
        5   | 25  | 11  |  42
        """
    )
    search_tree = build_sorted_index(nodes=items)
    search_tree.index += items.select(val=items.val)
    oracle = prefix_sum_oracle(**search_tree)

    query = T(
        """
            | query | instance
        0   | 30    | 42
        """
    )
    oracle_by_query = query.select(
        prefix_sum_upperbound=oracle.ix_ref(query.instance).prefix_sum_upperbound
    )
    result = query.select(
        val=oracle_by_query.prefix_sum_upperbound(query.query),
    )

    expected = T(
        """
                | val
            0   | 3
            """
    ).with_columns(val=items.pointer_from(this.val))
    expected = expected.update_types(val=dt.Optional(expected.typehints()["val"]))

    assert_table_equality(
        result,
        expected,
    )


def test_filter_smallest_k():
    t1 = T(
        """
            dist    | instance
            7       | 42
            6       | 42
            5       | 42
            4       | 28
            3       | 28
            2       | 28
            1       | 28
            9       | 123
            8       | 123
        """,
    )
    ks = T(
        """
        instance | k
        42       | 2
        28       | 3
        123      | 5
        """,
    )
    result = filter_smallest_k(t1.dist, t1.instance, ks)
    assert_table_equality(
        result,
        T(
            """
                | dist | instance
            1   |  6   | 42
            2   |  5   | 42
            4   |  3   | 28
            5   |  2   | 28
            6   |  1   | 28
            7   |  9   | 123
            8   |  8   | 123
            """
        ),
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

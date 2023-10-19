# Copyright © 2023 Pathway

from __future__ import annotations

import math

import pandas as pd

import pathway as pw
from pathway.stdlib.graphs.bellman_ford import bellman_ford
from pathway.stdlib.graphs.common import Edge, Graph, Vertex, Weight
from pathway.stdlib.graphs.louvain_communities.impl import (
    _one_step,
    exact_modularity,
    louvain_level,
)
from pathway.stdlib.graphs.pagerank import pagerank
from pathway.stdlib.graphs.utils import (
    contracted_to_multi_graph,
    contracted_to_simple_graph,
    extended_to_full_clustering,
    without_self_loops,
)
from pathway.tests.utils import (
    T,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_index_types,
)


def test_page_rank1():
    vertices = T(
        """
          |
        a |
        b |
        c |
        """
    ).select()
    # directed graph
    edges = T(
        """
        u | v
        a | b
        b | c
        c | a
        c | b
        """,
    ).select(u=vertices.pointer_from(pw.this.u), v=vertices.pointer_from(pw.this.v))

    res = pagerank(edges, 20)

    expected = T(
        """
                |   rank
        a       |   3896
        b       |   7142
        c       |   6951
        """,
    )
    assert_table_equality(res, expected)


def test_page_rank2():
    vertices = T(
        """
          |
        a |
        b |
        c |
        d |
        """
    ).select()
    edges = T(
        """
        u | v
        a | b
        b | c
        c | a
        c | b
        d | a
        """,
    ).select(u=vertices.pointer_from(pw.this.u), v=vertices.pointer_from(pw.this.v))
    res = pagerank(edges, 20)

    expected = T(
        """
                |   rank
        a       |   5393
        b       |   9053
        c       |   8543
        d       |   1000
        """,
    )

    assert_table_equality(res, expected)


def test_page_rank_zero_outdegree_nodes():
    vertices = T(
        """
          |
        a |
        b |
        """
    ).select()
    edges = T(
        """
            u  | v
            a  | b
        """,
    ).select(u=vertices.pointer_from(pw.this.u), v=vertices.pointer_from(pw.this.v))
    res = pagerank(edges, 10)
    assert_table_equality(
        res,
        T(
            """
                 rank
            a  | 1000
            b  | 1833
            """,
        ),
    )


def test_page_rank_one_node_edge_case():
    edges = pw.Table.empty(u=pw.Pointer, v=pw.Pointer)
    res = pagerank(edges, 50)
    assert_table_equality(res, pw.Table.empty(rank=int))


def test_bellman_ford():
    # directed graph
    vertices = T(
        """
      | is_source
    1 | True
    2 | False
    3 | False
    4 | False
    5 | False
    6 | False
    7 | False
    """
    )
    edges = T(
        """
       | u  | v | dist
    11 | 1  | 2 | 100
    12 | 1  | 3 | 200
    13 | 1  | 4 | 300
    14 | 3  | 5 | 100
    15 | 3  | 6 | 500
    16 | 5  | 6 | 100
    17 | 6  | 3 | -50
    """
    ).with_columns(
        u=vertices.pointer_from(pw.this.u),
        v=vertices.pointer_from(pw.this.v),
        dist=pw.cast(float, pw.this.dist),
    )
    res = bellman_ford(vertices, edges)
    expected = T(
        pd.DataFrame(
            {
                "dist_from_source": {
                    1: 0.0,
                    2: 100.0,
                    3: 200.0,
                    4: 300.0,
                    5: 300.0,
                    6: 400.0,
                    7: math.inf,
                }
            }
        ),
        format="pandas",
    )
    assert_table_equality(res, expected)


def test_remove_self_loops_01():
    V = T(
        """
        | dummy
    1   | 1
    2   | 1
    3   | 1
    4   | 1
    5   | 1
    """
    ).select()

    E = T(
        """
        | u | v
    100 | 1 | 2
    101 | 1 | 3
    102 | 1 | 2
    103 | 2 | 2
    104 | 2 | 3
    105 | 5 | 5
    106 | 5 | 5
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))
    G = Graph(V, E)
    G_ = without_self_loops(G)

    E_wo_loops = T(
        """
        | u | v
    100 | 1 | 2
    101 | 1 | 3
    102 | 1 | 2
    104 | 2 | 3
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))

    assert_table_equality(E_wo_loops, G_.E)


def test_extended_to_full_clustering_01():
    # dummy + select is here to generate id-only table
    # simply saying that I have table
    #
    # 1
    # 2
    # 3
    # 4
    # 5
    # didn't work
    # previous test had same problem, but it didn't manifest, as
    # is didn't rely on all ID-s being computed

    V = T(
        """
        | dummy
    1   | 1
    2   | 1
    3   | 1
    4   | 1
    5   | 1
    """
    ).select()

    # TODO: discuss pointer-only-tables mixed with class inheritance;
    # details: I have cluster class, which extends a vertex class;
    # I want to be able consistently generate cluster ID-s from vertex Id-s
    #
    # Now it does not relevant, as id computation does not care for types of columns
    # or tables that are used to invoke pointer_from. In the future it might,
    # then this case should be revisited / considered.

    CM = T(
        """
      | c
    1 | 1
    2 | 1
    4 | 4
    """
    ).with_columns(c=V.pointer_from(pw.this.c))
    expected = T(
        """
        | c
      1 | 1
      2 | 1
      4 | 4
      3 | 3
      5 | 5
    """
    ).with_columns(c=V.pointer_from(pw.this.c))

    ret = extended_to_full_clustering(V, CM)

    # it seems that types are a mess; update columns seems to keep type
    # from function I got Any (I expected Optional[Pointer], which still would have to
    # be fixed to Pointer)
    assert_table_equality_wo_index(ret, expected)


def test_contracted_to_multi_graph_01():
    V = T(
        """
        | dummy
    1   | 1
    2   | 1
    3   | 1
    4   | 1
    5   | 1
    """
    ).select()
    CM = T(
        """
        | c
      1 | 1
      2 | 1
      4 | 4
    """
    ).with_columns(c=V.pointer_from(pw.this.c))

    E = T(
        """
        | u | v
    100 | 1 | 2
    101 | 1 | 3
    102 | 1 | 2
    103 | 2 | 2
    104 | 2 | 3
    105 | 5 | 5
    106 | 5 | 5
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))

    G = Graph(V, E)
    G_ = contracted_to_multi_graph(G, CM)

    E_expected = T(
        """
        | u | v
    100 | 1 | 1
    101 | 1 | 3
    102 | 1 | 1
    103 | 1 | 1
    104 | 1 | 3
    105 | 5 | 5
    106 | 5 | 5
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))
    V_expected = T(
        """
        | dummy
    1   | 1
    3   | 1
    4   | 1
    5   | 1
    """
    ).select()

    assert_table_equality(G_.E, E_expected)
    assert_table_equality(G_.V, V_expected)


def test_contracted_to_simple_graph_01():
    V = T(
        """
        | dummy
    1   | 1
    2   | 1
    3   | 1
    4   | 1
    5   | 1
    """
    ).select()
    CM = T(
        """
        | c
      1 | 1
      2 | 1
      4 | 4
    """
    ).with_columns(c=V.pointer_from(pw.this.c))

    E = T(
        """
        | u | v
    100 | 1 | 2
    101 | 1 | 3
    102 | 1 | 2
    103 | 2 | 2
    104 | 2 | 3
    105 | 5 | 5
    106 | 5 | 5
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))

    G = Graph(V, E)
    G_ = contracted_to_simple_graph(G, CM)

    E_expected = T(
        """
        | u | v
    100 | 1 | 1
    101 | 1 | 3
    106 | 5 | 5
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))
    V_expected = T(
        """
        | dummy
    1   | 1
    3   | 1
    4   | 1
    5   | 1
    """
    ).select()

    assert_table_equality_wo_index(G_.E, E_expected)
    assert_table_equality(G_.V, V_expected)


def test_contracted_to_simple_graph_02():
    V = T(
        """
        | dummy
    1   | 1
    2   | 1
    3   | 1
    4   | 1
    5   | 1
    """
    ).select()
    CM = T(
        """
        | c
      1 | 1
      2 | 1
      4 | 4
    """
    ).with_columns(c=V.pointer_from(pw.this.c))

    E = T(
        """
        | u | v | weight
    100 | 1 | 2 | 1
    101 | 1 | 3 | 1
    102 | 1 | 2 | 1
    103 | 2 | 2 | 1
    104 | 2 | 3 | 1
    105 | 5 | 5 | 1
    106 | 5 | 5 | 1
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))

    G = Graph(V, E)
    G_ = contracted_to_simple_graph(G, CM, **{"weight": pw.reducers.sum(E.weight)})

    E_expected = T(
        """
        | u | v | weight
    100 | 1 | 1 | 3
    101 | 1 | 3 | 2
    106 | 5 | 5 | 2
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))

    assert_table_equality_wo_index(G_.E, E_expected)


def test_exact_modularity_01():
    vertices = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).select()

    clustering = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).with_columns(c=pw.this.pointer_from(pw.this.c))

    weighted_edges: pw.Table[Edge | Weight] = T(
        """
        | u | v | weight
    1   | 1 | 2 | 5.0
    2   | 2 | 1 | 5.0
    3   | 3 | 4 | 5.0
    4   | 4 | 3 | 5.0
    5   | 1 | 4 | 15.0
    6   | 4 | 1 | 15.0
    7   | 5 | 1 | 0.5
    8   | 5 | 4 | 0.5
    9   | 1 | 5 | 0.5
    10  | 4 | 5 | 0.5

    """
    ).with_columns(u=pw.this.pointer_from(pw.this.u), v=pw.this.pointer_from(pw.this.v))

    # computed by hand, for singleton clusters
    expected = T(
        """
        | modularity
    1   | -0.3296967456
    """
    )

    res = exact_modularity(Graph(vertices, weighted_edges), clustering, round_digits=10)
    assert_table_equality_wo_index(res, expected)


def test_exact_modularity_02():
    vertices = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).select()

    clustering = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 1
    5   | 5
    """
    ).with_columns(c=pw.this.pointer_from(pw.this.c))

    weighted_edges: pw.Table[Edge | Weight] = T(
        """
        | u | v | weight
    1   | 1 | 2 | 5.0
    2   | 2 | 1 | 5.0
    3   | 3 | 4 | 5.0
    4   | 4 | 3 | 5.0
    5   | 1 | 4 | 15.0
    6   | 4 | 1 | 15.0
    7   | 5 | 1 | 0.5
    8   | 5 | 4 | 0.5
    9   | 1 | 5 | 0.5
    10  | 4 | 5 | 0.5

    """
    ).with_columns(u=pw.this.pointer_from(pw.this.u), v=pw.this.pointer_from(pw.this.v))

    # computed by hand, for given clustering
    expected = T(
        """
        | modularity
    1   | -0.063609467
    """
    )

    res = exact_modularity(Graph(vertices, weighted_edges), clustering, round_digits=9)
    assert_table_equality_wo_index(res, expected)


"""
one_step from louvain communities implementation is a little bit tricky to test;
it is a randomized function, and even if we use pseudorandomness, if someone
wants to improve the function in the future, it does not necessarily
mean that returned clustering needs to be the same

As such, the tests only check, whether the new clustering indeed has better
modularity, as this is the only basic rule that cannot be broken (i.e.,
if implementation does not necessarily increase it, it might end up in an infinite
loop, unless there are some other guarantees it finishes (also, then this test should
be replaced with something else))
"""


def test_louvain_one_step_01():
    vertices: pw.Table[Vertex] = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).select()

    clustering = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).with_columns(c=pw.this.pointer_from(pw.this.c))

    weighted_edges: pw.Table[Edge | Weight] = T(
        """
        | u | v | weight
    1   | 1 | 2 | 5.0
    2   | 2 | 1 | 5.0
    3   | 3 | 4 | 5.0
    4   | 4 | 3 | 5.0
    5   | 1 | 4 | 15.0
    6   | 4 | 1 | 15.0
    7   | 5 | 1 | 0.5
    8   | 5 | 4 | 0.5
    9   | 1 | 5 | 0.5
    10  | 4 | 5 | 0.5

    """
    ).with_columns(u=pw.this.pointer_from(pw.this.u), v=pw.this.pointer_from(pw.this.v))

    total_weight = T(
        """
        | m
      1 | 52
        """
    )

    res = _one_step(Graph(vertices, weighted_edges), clustering, total_weight, 7)

    ini_mod = exact_modularity(Graph(vertices, weighted_edges), clustering)
    res_mod = exact_modularity(Graph(vertices, weighted_edges), res)
    cmp = ini_mod.join(res_mod).select(res=(ini_mod.modularity <= res_mod.modularity))
    expected = T(
        """
        | res
      1 | True
    """
    )

    assert_table_equality_wo_index(cmp, expected)


def test_louvain_one_step_02():
    vertices: pw.Table[Vertex] = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).select()

    clustering = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 1
    4   | 2
    5   | 5
    """
    ).with_columns(c=pw.this.pointer_from(pw.this.c))

    weighted_edges: pw.Table[Edge | Weight] = T(
        """
        | u | v | weight
    1   | 1 | 2 | 50.0
    2   | 2 | 1 | 50.0
    3   | 3 | 4 | 50.0
    4   | 4 | 3 | 50.0
    5   | 1 | 4 | 0.5
    6   | 4 | 1 | 0.5
    7   | 5 | 1 | 0.5
    8   | 5 | 4 | 0.5
    9   | 1 | 5 | 0.5
    10  | 4 | 5 | 0.5
    """
    ).with_columns(u=pw.this.pointer_from(pw.this.u), v=pw.this.pointer_from(pw.this.v))

    total_weight = T(
        """
        | m
      1 | 52
        """
    )

    res = _one_step(Graph(vertices, weighted_edges), clustering, total_weight, 7)

    ini_mod = exact_modularity(Graph(vertices, weighted_edges), clustering)
    res_mod = exact_modularity(Graph(vertices, weighted_edges), res)
    cmp = ini_mod.join(res_mod).select(res=(ini_mod.modularity <= res_mod.modularity))
    expected = T(
        """
        | res
      1 | True
    """
    )

    assert_table_equality_wo_index(cmp, expected)


def test_louvain_level_01():
    vertices: pw.Table[Vertex] = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).select()

    weighted_edges: pw.Table[Edge | Weight] = T(
        """
        | u | v | weight
    1   | 1 | 2 | 5.0
    2   | 2 | 1 | 5.0
    3   | 3 | 4 | 5.0
    4   | 4 | 3 | 5.0
    5   | 1 | 4 | 15.0
    6   | 4 | 1 | 15.0
    7   | 5 | 1 | 0.5
    8   | 5 | 4 | 0.5
    9   | 1 | 5 | 0.5
    10  | 4 | 5 | 0.5
    """
    ).with_columns(u=pw.this.pointer_from(pw.this.u), v=pw.this.pointer_from(pw.this.v))

    total_weight = T(
        """
        | m
      1 | 52
        """
    )

    expected_mod = T(
        """
        | modularity
      1 | 0.0
        """
    )

    iter_res = louvain_level(Graph(vertices, weighted_edges), total_weight)

    assert_table_equality_wo_index_types(
        exact_modularity(Graph(vertices, weighted_edges), iter_res, round_digits=16),
        expected_mod,
    )


def test_louvain_level_02():
    vertices: pw.Table[Vertex] = T(
        """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    ).select()

    weighted_edges: pw.Table[Edge | Weight] = T(
        """
        | u | v | weight
    1   | 1 | 2 | 50.0
    2   | 2 | 1 | 50.0
    3   | 3 | 4 | 50.0
    4   | 4 | 3 | 50.0
    5   | 1 | 4 | 0.5
    6   | 4 | 1 | 0.5
    7   | 5 | 1 | 0.5
    8   | 5 | 4 | 0.5
    9   | 1 | 5 | 0.5
    10  | 4 | 5 | 0.5
    """
    ).with_columns(u=pw.this.pointer_from(pw.this.u), v=pw.this.pointer_from(pw.this.v))

    total_weight = T(
        """
        | m
      1 | 203
        """
    )

    expected_mod = T(
        """
        | modularity
      1 | 0.4901356499793734
        """
    )

    iter_res = louvain_level(Graph(vertices, weighted_edges), total_weight)

    assert_table_equality_wo_index(
        exact_modularity(Graph(vertices, weighted_edges), iter_res, round_digits=16),
        expected_mod,
    )

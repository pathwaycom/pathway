# Copyright © 2024 Pathway

from __future__ import annotations

import math
import os
from dataclasses import dataclass

import community
import networkx as nx
import pandas as pd
import pytest

import pathway as pw
from pathway.stdlib.graphs.bellman_ford.impl import bellman_ford
from pathway.stdlib.graphs.common import Edge, Vertex, Weight
from pathway.stdlib.graphs.graph import (
    Graph,
    WeightedGraph,
    _extended_to_full_clustering,
)
from pathway.stdlib.graphs.louvain_communities.impl import (
    _approximate_total_weight,
    _louvain_level,
    _louvain_level_fixed_iterations,
    _one_step,
    exact_modularity,
    louvain_communities_fixed_iterations,
)
from pathway.stdlib.graphs.pagerank import pagerank
from pathway.tests.utils import T, assert_table_equality, assert_table_equality_wo_index


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
    G_ = G.without_self_loops()

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
    # Now it is not really relevant, as id computation does not care for types of columns
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

    ret = _extended_to_full_clustering(V, CM)

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
    G_ = G.contracted_to_multi_graph(CM)

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
    G_ = G.contracted_to_unweighted_simple_graph(CM)

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

    WE = T(
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

    G = WeightedGraph.from_vertices_and_weighted_edges(V, WE)
    G_ = G.contracted_to_weighted_simple_graph(
        CM, **{"weight": pw.reducers.sum(WE.weight)}
    )

    E_expected = T(
        """
        | u | v | weight
    100 | 1 | 1 | 3
    101 | 1 | 3 | 2
    106 | 5 | 5 | 2
    """
    ).with_columns(u=V.pointer_from(pw.this.u), v=V.pointer_from(pw.this.v))

    assert_table_equality_wo_index(G_.WE, E_expected)


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

    res = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        clustering,
        round_digits=10,
    )
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

    res = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        clustering,
        round_digits=9,
    )
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
      1 | 52.0
        """
    ).select(m=pw.cast(float, pw.this.m))

    clustering = clustering.join(total_weight, id=clustering.id).select(
        *pw.left, total_weight=pw.right.m
    )

    res = _one_step(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        clustering,
        iter=7,
    )

    ini_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        clustering,
    )
    res_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges), res
    )
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
      1 | 52.0
        """
    ).select(m=pw.cast(float, pw.this.m))

    clustering = clustering.join(total_weight, id=clustering.id).select(
        *pw.left, total_weight=pw.right.m
    )

    res = _one_step(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        clustering,
        iter=7,
    )

    ini_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        clustering,
    )
    res_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges), res
    )
    cmp = ini_mod.join(res_mod).select(res=(ini_mod.modularity <= res_mod.modularity))
    expected = T(
        """
        | res
      1 | True
    """
    )

    assert_table_equality_wo_index(cmp, expected)


def test_louvain_level_01():
    vertices: pw.Table[Vertex] = (
        T(
            """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
        )
        .with_id_from(pw.this.c)
        .select()
    )

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
      1 | 52.0
        """
    )

    class FloatModularitySchema(pw.Schema):
        modularity: float

    expected_mod = T(
        """
        | modularity
      1 | 0.0
        """,
        schema=FloatModularitySchema,
    )
    vertices = vertices.join(total_weight, id=vertices.id).select(
        *pw.left, apx_value=pw.right.m
    )

    iter_res = _louvain_level(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
    )
    pw.debug.compute_and_print(iter_res)

    assert_table_equality_wo_index(
        exact_modularity(
            WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
            iter_res,
            round_digits=16,
        ),
        expected_mod,
    )


def test_louvain_level_02():
    vertices: pw.Table[Vertex] = (
        T(
            """
        | c
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
        )
        .with_id_from(pw.this.c)
        .select()
    )

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
      1 | 203.0
        """
    )

    expected_mod = T(
        """
        | modularity
      1 | 0.4901356499793734
        """
    )
    vertices = vertices.join(total_weight, id=vertices.id).select(
        *pw.left, apx_value=pw.right.m
    )

    iter_res = _louvain_level(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
    )

    assert_table_equality_wo_index(
        exact_modularity(
            WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
            iter_res,
            round_digits=16,
        ),
        expected_mod,
    )


def test_louvain_level_fixed_iter_01():
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
      1 | 203.0
        """
    ).select(m=pw.cast(float, pw.this.m))

    vertices = vertices.join(total_weight, id=vertices.id).select(
        *pw.left, apx_value=pw.right.m
    )

    expected_mod = T(
        """
        | modularity
      1 | 0.4901356499793734
        """
    )

    iter_res = _louvain_level_fixed_iterations(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        10,
    )

    assert_table_equality_wo_index(
        exact_modularity(
            WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
            iter_res,
            round_digits=16,
        ),
        expected_mod,
    )


def test_louvain_communities_01():
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

    expected_mod = T(
        """
        | modularity
      1 | 0.4901356499793734
        """
    )

    _res = louvain_communities_fixed_iterations(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges), 0.1, 1
    )

    res = _res.clustering_levels.filter(_res.clustering_levels.level == 1).with_id(
        pw.this.v
    )

    cmp = (
        exact_modularity(
            WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
            res,
        )
        .join(expected_mod)
        .select(
            cmp=(pw.left.modularity >= 0.99 * pw.right.modularity),
        )
    )

    expected = T(
        """
        | cmp
      1 | True
    """
    )

    assert_table_equality_wo_index(
        cmp,
        expected,
    )


def test_compare_standalone_iteration_with_louvain_one_level_iteration():
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
      1 | 203.0
        """
    ).select(m=pw.cast(float, pw.this.m))

    vertices_ext = vertices.join(total_weight, id=vertices.id).select(
        *pw.left, apx_value=pw.right.m
    )

    _res = louvain_communities_fixed_iterations(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges), 0.1, 1
    )

    communities_res = (
        _res.clustering_levels.filter(_res.clustering_levels.level == 1)
        .with_id(pw.this.v)
        .select(pw.this.c)
    )

    iter_res = _louvain_level_fixed_iterations(
        WeightedGraph.from_vertices_and_weighted_edges(vertices_ext, weighted_edges),
        10,
    )

    total_weights_computed = WeightedGraph.from_vertices_and_weighted_edges(
        vertices, weighted_edges
    ).WE.reduce(m=pw.reducers.sum(pw.this.weight))
    assert_table_equality_wo_index(total_weight, total_weights_computed)

    comm_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        communities_res,
    )

    iter_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
        iter_res,
    )

    expected_mod = T(
        """
        | modularity
      1 | 0.4901356499793734
        """
    )

    cmp_iter = iter_mod.join(expected_mod).select(
        cmp=(pw.left.modularity >= 0.99 * pw.right.modularity),
    )

    cmp_comm = comm_mod.join(expected_mod).select(
        cmp=(pw.left.modularity >= 0.99 * pw.right.modularity),
    )

    expected = T(
        """
        | cmp
      1 | True
    """
    )

    assert_table_equality_wo_index(
        cmp_iter,
        expected,
    )

    assert_table_equality_wo_index(
        cmp_comm,
        expected,
    )


def test_compare_clustering_induced_by_apx_weight_and_predefined_weight():
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

    true_total_weight_fixed = T(
        """
        | m
      1 | 203.0
        """
    ).select(m=pw.cast(float, pw.this.m))

    v1 = vertices.join(true_total_weight_fixed, id=vertices.id).select(
        *pw.left, apx_value=pw.right.m
    )

    apx_total_weight = _approximate_total_weight(weighted_edges, 0.001).select(
        m=pw.this.lower
    )

    v2 = vertices.join(apx_total_weight, id=vertices.id).select(
        *pw.left, apx_value=pw.right.m
    )
    expected_mod = T(
        """
        | modularity
      1 | 0.4901356499793734
        """
    )

    iter_res_01 = _louvain_level_fixed_iterations(
        WeightedGraph.from_vertices_and_weighted_edges(v1, weighted_edges),
        10,
    )

    iter_res_02 = _louvain_level_fixed_iterations(
        WeightedGraph.from_vertices_and_weighted_edges(v2, weighted_edges),
        10,
    )

    assert_table_equality(iter_res_01, iter_res_02)

    assert_table_equality_wo_index(
        exact_modularity(
            WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
            iter_res_01,
            round_digits=16,
        ),
        exact_modularity(
            WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
            iter_res_02,
            round_digits=16,
        ),
    )

    assert_table_equality_wo_index(
        exact_modularity(
            WeightedGraph.from_vertices_and_weighted_edges(vertices, weighted_edges),
            iter_res_01,
            round_digits=16,
        ),
        expected_mod,
    )


@dataclass
class InputEdge(Edge):
    edge_visits: int


@dataclass
class InputVertex(Vertex):
    visits: int


def from_dicts(*args, **kwargs):
    df = pd.DataFrame.from_dict(kwargs)
    return pw.debug.table_from_pandas(df)


def compute_weights(edges: pw.Table[InputEdge], vertices: pw.Table[InputVertex]):
    # arbitrary definition of importance / weight of an edge, depending on visits
    def edge_importance(deg_u, deg_v, visits):
        return visits / (deg_u + deg_v)

    return edges.select(
        edges.u,
        edges.v,
        weight=edge_importance(
            vertices.ix(edges.u).visits, vertices.ix(edges.v).visits, edges.edge_visits
        ),
    )


def test_clustering_quality():
    dataset_path = "public/pathway/python/pathway/tests/data/louvain_graph.gexf"
    # TODO (in another PR):
    # - add another type of xfail, that is allowed to fail when tested on environments
    # that does not do dvc pull (requires xfail, and perhaps setting some env_variable
    # for environments that do dvc_pull)
    # - separate this test to some different batch of tests that are executed
    # periodically (as it is some kind of clustering quality benchmark), one won't make
    # it slow, but adding such tests en masse will make default jenkins run slow

    if not os.path.isfile(dataset_path):
        pytest.skip("dvc file with the input graph is unavailable")
    graph = nx.read_gexf(dataset_path)

    input_vertices = dict()
    input_id = dict()
    for str_id, visits in graph.nodes(data="visits"):
        input_id[int(str_id)] = int(str_id)
        input_vertices[int(str_id)] = visits

    cnt = 1
    input_edges_u = dict()
    input_edges_v = dict()
    input_edges_visits = dict()
    for stru, strv, visits in graph.edges(data="visits"):
        input_edges_u[cnt] = int(stru)
        input_edges_v[cnt] = int(strv)
        input_edges_visits[cnt] = visits
        cnt += 1

    vertices = (
        from_dicts(idd=input_id, visits=input_vertices)
        .with_id_from(pw.this.idd)
        .without(pw.this.idd)
    )

    edges = from_dicts(
        u=input_edges_u, v=input_edges_v, edge_visits=input_edges_visits
    ).with_columns(
        u=vertices.pointer_from(pw.this.u), v=vertices.pointer_from(pw.this.v)
    )

    weighted_edges = compute_weights(edges, vertices)
    doubled_edges = pw.Table.concat_reindex(
        weighted_edges,
        weighted_edges.select(
            u=weighted_edges.v, v=weighted_edges.u, weight=weighted_edges.weight
        ),
    )
    MAX_LVL = 10
    ret = louvain_communities_fixed_iterations(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, doubled_edges),
        0.1,
        MAX_LVL,
    )
    our_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, doubled_edges),
        ret.clustering_levels.filter(ret.clustering_levels.level == MAX_LVL).with_id(
            pw.this.v
        ),
    )

    # comparison with some other clustering implementation,

    modified_graph = nx.Graph()
    modified_graph.add_nodes_from(graph)

    for stru, strv, visits in graph.edges(data="visits"):
        modified_graph.add_edge(
            stru,
            strv,
            weight=visits / (input_vertices[int(stru)] + input_vertices[int(strv)]),
        )

    other_clustering = dict()
    tmp = community.best_partition(modified_graph, random_state=1)

    for stru in tmp.keys():
        other_clustering[int(stru)] = tmp[stru]

    vc = (
        from_dicts(c=other_clustering, idd=input_id)
        .with_columns(c=vertices.pointer_from(pw.this.c))
        .with_id_from(pw.this.idd)
    )

    mod_from_external_lib_as_number = community.modularity(tmp, modified_graph)

    def compare_mod(mod):
        return f"{mod:.14f}" == f"{mod_from_external_lib_as_number:.14f}"

    external_mod = exact_modularity(
        WeightedGraph.from_vertices_and_weighted_edges(vertices, doubled_edges), vc
    )
    check_mod_cmp = external_mod.select(
        res=pw.apply_with_type(compare_mod, bool, external_mod.modularity)
    )

    # here we ask to be at most 1 percent worse than the external lib
    # checked the performance of the external lib, it gives the results with modularity
    # around that of ours, with slight variation depending on random seed
    # (in both algorithms)

    cmp = our_mod.join(external_mod).select(
        res=(our_mod.modularity >= 0.99 * external_mod.modularity),
    )

    expected = T(
        """
        | res
      1 | True
    """
    )
    # compare modularities (test our modularity computing function)
    assert_table_equality_wo_index(check_mod_cmp, expected)
    # compare clustering quality (test our modularity computing function)v
    assert_table_equality_wo_index(cmp, expected)

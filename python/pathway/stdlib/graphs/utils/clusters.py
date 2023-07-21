# Copyright © 2023 Pathway

from __future__ import annotations

import pathway.internals as pw

from ..common import Clustering, Edge, Graph, Vertex


def extended_to_full_clustering(
    vertices: pw.Table[Vertex], clustering: pw.Table[Clustering]
) -> pw.Table[Clustering]:
    r"""
    This function, given a set of vertices and a partial clustering,
    i.e., a clustering in which not eery vertex has assigned a cluster,
    creates extended clustering in which those vertices are in singleton clusters.

    The id of the new singleton cluster is the same as id of vertex

    """

    return vertices.select(c=vertices.id).update_rows(clustering)


def without_self_loops(G: Graph) -> Graph:
    return Graph(G.V, G.E.filter(G.E.u != G.E.v))


def contracted_to_simple_graph(
    G: Graph,
    clustering: pw.Table[Clustering],
    **reducer_expressions: dict[str, pw.ReducerExpression],
) -> Graph:
    contracted_graph = contracted_to_multi_graph(G, clustering)
    contracted_graph.E = contracted_graph.E.groupby(
        contracted_graph.E.u, contracted_graph.E.v
    ).reduce(contracted_graph.E.u, contracted_graph.E.v, **reducer_expressions)
    return contracted_graph


def contracted_to_multi_graph(
    G: Graph,
    clustering: pw.Table[Clustering],
) -> Graph:
    full_clustering = extended_to_full_clustering(G.V, clustering)
    return _contract(G.E, full_clustering)


def _contract(edges: pw.Table[Edge], clustering: pw.Table[Clustering]) -> Graph:
    r"""
    This function contracts the clusters of the graph,
    under the assumption that it was given a full clustering,
    i.e., all vertices have exactly one cluster in clustering

    Returns:
        a graph in which:
        - each vertex is a cluster from the clustering,
        - each original edge now points to clusters containing the original endpoints

    """

    new_vertices = (
        clustering.groupby(clustering.c)
        .reduce(v=clustering.c)
        .with_id(pw.this.v)
        .select()
    )
    new_edges = new_edges = edges.select(
        u=clustering.ix(edges.u).c, v=clustering.ix(edges.v).c
    )
    return Graph(new_vertices, new_edges)

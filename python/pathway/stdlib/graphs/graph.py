# Copyright © 2024 Pathway


from __future__ import annotations

from dataclasses import dataclass

import pathway.internals as pw

from .common import Clustering, Edge, Vertex, Weight


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
    new_edges = edges.select(u=clustering.ix(edges.u).c, v=clustering.ix(edges.v).c)
    return Graph(new_vertices, new_edges)


def _contract_weighted(
    edges: pw.Table[Edge | Weight], clustering: pw.Table[Clustering]
) -> WeightedGraph:
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
    new_edges = edges.select(u=clustering.ix(edges.u).c, v=clustering.ix(edges.v).c)
    return WeightedGraph.from_vertices_and_weighted_edges(new_vertices, new_edges)


def _extended_to_full_clustering(
    vertices: pw.Table[Vertex], clustering: pw.Table[Clustering]
) -> pw.Table[Clustering]:
    r"""
    This function, given a set of vertices and a partial clustering,
    i.e., a clustering in which not every vertex has assigned a cluster,
    creates extended clustering in which those vertices are in singleton clusters.

    The id of the new singleton cluster is the same as id of vertex

    """

    return vertices.select(c=vertices.id).update_rows(clustering)


@dataclass
class Graph:
    r"""
    Basic class representing undirected, unweighted (multi)graph.
    """

    V: pw.Table[Vertex]
    E: pw.Table[Edge]

    def contracted_to_unweighted_simple_graph(
        self,
        clustering: pw.Table[Clustering],
        **reducer_expressions: pw.ReducerExpression,
    ) -> Graph:
        contracted_graph = self.contracted_to_multi_graph(clustering)
        contracted_graph.E = contracted_graph.E.groupby(
            contracted_graph.E.u, contracted_graph.E.v
        ).reduce(contracted_graph.E.u, contracted_graph.E.v)

        return contracted_graph

    def contracted_to_weighted_simple_graph(
        self,
        clustering: pw.Table[Clustering],
        **reducer_expressions: pw.ReducerExpression,
    ) -> WeightedGraph:
        contracted_graph = self.contracted_to_multi_graph(clustering)
        WE = contracted_graph.E.groupby(
            contracted_graph.E.u, contracted_graph.E.v
        ).reduce(contracted_graph.E.u, contracted_graph.E.v, **reducer_expressions)

        return WeightedGraph.from_vertices_and_weighted_edges(contracted_graph.V, WE)

    def contracted_to_multi_graph(
        self,
        clustering: pw.Table[Clustering],
    ) -> Graph:
        full_clustering = _extended_to_full_clustering(self.V, clustering)
        return _contract(self.E, full_clustering)

    def without_self_loops(self) -> Graph:
        return Graph(self.V, self.E.filter(self.E.u != self.E.v))


@dataclass
class WeightedGraph(Graph):
    r"""
    Basic class representing undirected, unweighted (multi)graph.
    """

    WE: pw.Table[Edge | Weight]

    @staticmethod
    def from_vertices_and_weighted_edges(V, WE):
        return WeightedGraph(V, WE, WE)

    def contracted_to_weighted_simple_graph(
        self,
        clustering: pw.Table[Clustering],
        **reducer_expressions: pw.ReducerExpression,
    ) -> WeightedGraph:
        contracted_graph = self.contracted_to_multi_graph(clustering)
        contracted_graph.WE = contracted_graph.WE.groupby(
            contracted_graph.WE.u, contracted_graph.WE.v
        ).reduce(contracted_graph.WE.u, contracted_graph.WE.v, **reducer_expressions)
        return contracted_graph

    def contracted_to_multi_graph(
        self,
        clustering: pw.Table[Clustering],
    ) -> WeightedGraph:
        full_clustering = _extended_to_full_clustering(self.V, clustering)
        return _contract_weighted(self.WE, full_clustering)

    def without_self_loops(self) -> WeightedGraph:
        new_edges = self.WE.filter(self.WE.u != self.WE.v)
        return WeightedGraph.from_vertices_and_weighted_edges(self.V, new_edges)

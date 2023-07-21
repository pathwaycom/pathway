# Copyright © 2023 Pathway

from __future__ import annotations

from dataclasses import dataclass

import pathway.internals as pw


class Vertex(pw.Schema):
    pass


class Cluster(Vertex, pw.Schema):
    pass


class Clustering(pw.Schema):
    r"""
    Class describing cluster membership relation:
    vertex u belongs to cluster c.
    """
    c: pw.Pointer[Cluster]


class Edge(pw.Schema):
    r"""
    Basic edge class, holds pointers to the endpoint vertices.
    """
    u: pw.Pointer[Vertex]
    v: pw.Pointer[Vertex]


class Weight(pw.Schema):
    r"""
    Basic weight class. To be used as extension of Vertex / Edge
    """
    weight: float


@dataclass
class Graph:
    r"""
    Basic class representing undirected, unweighted (multi)graph.
    """
    V: pw.Table[Vertex]
    E: pw.Table[Edge]

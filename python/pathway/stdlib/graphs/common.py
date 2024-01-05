# Copyright © 2024 Pathway

from __future__ import annotations

import pathway.internals as pw


class Vertex(pw.Schema):
    pass


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


class Cluster(Vertex, pw.Schema):
    pass


class Clustering(pw.Schema):
    r"""
    Class describing cluster membership relation:
    vertex u (id-column) belongs to cluster c.
    """
    c: pw.Pointer[Cluster]

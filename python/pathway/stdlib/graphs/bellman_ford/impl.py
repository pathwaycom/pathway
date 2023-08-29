# Copyright © 2023 Pathway

from __future__ import annotations

import math

import pathway.internals as pw
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame

from ..common import Edge


class Vertex(pw.Schema):
    is_source: bool


class Dist(pw.Schema):
    dist: float


class DistFromSource(pw.Schema):
    dist_from_source: float


def _bellman_ford_step(
    vertices_dist: pw.Table[DistFromSource], edges: pw.Table[Edge | Dist]
) -> pw.Table[DistFromSource]:
    relaxed_edges = edges + edges.select(
        dist_from_source=vertices_dist.ix(edges.u).dist_from_source + edges.dist
    )
    vertices_dist = vertices_dist.update_rows(
        relaxed_edges.groupby(id=relaxed_edges.v).reduce(
            dist_from_source=pw.reducers.min(relaxed_edges.dist_from_source),
        )
    )
    return vertices_dist


@runtime_type_check
@trace_user_frame
def bellman_ford(vertices: pw.Table[Vertex], edges: pw.Table[Edge | Dist]):
    vertices_dist: pw.Table[DistFromSource] = vertices.select(
        dist_from_source=pw.if_else(vertices.is_source, 0.0, math.inf)  # type: ignore
    )

    return pw.iterate(
        lambda iterated, edges: dict(
            iterated=_bellman_ford_step(vertices_dist=iterated, edges=edges)
        ),
        iterated=pw.iterate_universe(vertices_dist),
        edges=edges,
    ).iterated

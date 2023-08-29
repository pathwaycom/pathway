# Copyright © 2023 Pathway

from __future__ import annotations

import pathway.internals as pw
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame

from ..common import Edge


class Result(pw.Schema):
    rank: int


@runtime_type_check
@trace_user_frame
def pagerank(edges: pw.Table[Edge], steps: int = 5) -> pw.Table[Result]:
    in_vertices = edges.groupby(id=edges.v).reduce(degree=0)
    out_vertices = edges.groupby(id=edges.u).reduce(degree=pw.reducers.count())
    degrees = pw.Table.update_rows(in_vertices, out_vertices)
    base = out_vertices.difference(in_vertices).select(flow=0)

    ranks = degrees.select(rank=6_000)

    for step in range(steps):
        outflow = degrees.select(
            flow=pw.if_else(
                degrees.degree == 0, 0, (ranks.rank * 5) // (degrees.degree * 6)
            ),
        )

        inflows = edges.groupby(id=edges.v).reduce(
            flow=pw.reducers.sum(outflow.ix(edges.u).flow)
        )

        inflows = pw.Table.concat(base, inflows)

        ranks = inflows.select(
            rank=inflows.flow + 1_000,
        )
    return ranks

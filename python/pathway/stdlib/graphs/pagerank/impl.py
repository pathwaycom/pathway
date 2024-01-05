# Copyright © 2024 Pathway

from __future__ import annotations

import pathway.internals as pw
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame

from ..common import Edge


class Result(pw.Schema):
    rank: int


@check_arg_types
@trace_user_frame
def pagerank(edges: pw.Table[Edge], steps: int = 5) -> pw.Table[Result]:
    in_vertices: pw.Table = edges.groupby(id=edges.v).reduce(degree=0)
    out_vertices: pw.Table = edges.groupby(id=edges.u).reduce(
        degree=pw.reducers.count()
    )
    degrees: pw.Table = pw.Table.update_rows(in_vertices, out_vertices)
    base: pw.Table = out_vertices.difference(in_vertices).select(rank=1_000)

    ranks: pw.Table = degrees.select(rank=6_000)

    for step in range(steps):
        outflow = degrees.select(
            flow=pw.if_else(
                degrees.degree == 0, 0, (ranks.rank * 5) // (degrees.degree * 6)
            ),
        )

        inflows = edges.groupby(id=edges.v).reduce(
            rank=pw.reducers.sum(outflow.ix(edges.u).flow) + 1_000
        )

        ranks = pw.Table.concat(base, inflows).with_universe_of(degrees)

    return ranks

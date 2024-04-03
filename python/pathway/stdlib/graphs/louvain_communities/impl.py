# Copyright © 2024 Pathway

from __future__ import annotations

import math
from typing import Any

import pathway.internals as pw
from pathway.internals.fingerprints import fingerprint
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame
from pathway.internals.udfs import udf
from pathway.stdlib.graphs.common import Clustering, Edge, Weight
from pathway.stdlib.graphs.graph import WeightedGraph
from pathway.stdlib.utils.filtering import argmax_rows


def _propose_clusters(
    edges: pw.Table[Edge | Weight],
    clustering: pw.Table[Clustering],
) -> pw.Table[Clustering]:
    r"""
    Given a table of weighted edges and a clustering, this function for finds each vertex
    the best adjacent cluster that locally maximizes the Louvain objective function.
    """

    # some variant of the comment below will be in the showcase; then it might be replaced
    # with a link
    """
    To do so, for each vertex and each edge we first find the cluster id
    of the other endpoint. Then, we aggregate gains form each vertex, to each cluster.

    In this function, we work on directed edges. If the graph is undirected,
    the assumption is that for an undirected edge {u,v} there are two edges (u,v) and
    (v,u) in the set of edges.

    According to the Louvain method, having a vertex v in a cluster C and some other
    cluster C', with
    - deg(C) := sum of degrees in C
    - deg(C') := sum of degrees in C'
    - deg(v in C) := sum of weights of self loops of v and the edges from v to C
    - deg(v in C') := sum of weights of self loops of v and the edges from v to C'
    - m := total weight of edges

    The modularity (objective function) of a single cluster is:
    intra/(2m) - (tot/(2m))^2,
    where
    - intra is the total weight of all edges inside of the cluster,
    - tot is the total weight of edges incident to all members of the cluster

    Therefore, the change of the global objective function (not normalized, m times larger than in
    the original statement) resulting from moving v from C to C' is:
    2*deg(v in C') - deg(v)(2 deg(C') + deg(v)) / m
    - 2*deg(v in C \\ {v}) + deg(v)(2 deg(C \\ {v}) + deg(v)) / m

    The second line does not depend on the target cluster C',
    therefore we can simply evaluate
    2*deg(v in C') - deg(v)(2 deg(C') + deg(v)) / m
    for all clusters C' != C adjacent to v. For cluster C the computation is similar,
    however in-code we compute this gain separately, as we need to take into account
    that removing v from C changes the sum of degrees in C.
    """
    # set up degree penalties for all clusters
    placeholder_penalties = clustering.groupby(id=clustering.c).reduce(
        unscaled_penalty=0.0
    )
    # compute cluster penalties (sums of degrees)
    real_penalties = (
        edges.select(edges.weight, cu=clustering.ix(edges.u).c)
        .groupby(id=pw.this.cu)
        .reduce(unscaled_penalty=pw.reducers.sum(pw.this.weight))
    )
    cluster_penalties = placeholder_penalties.update_rows(real_penalties)

    # degrees of vertices are needed to compute  louvain objective function
    vertex_degrees = edges.groupby(id=edges.v).reduce(
        degree=pw.reducers.sum(edges.weight)
    )

    # self loop edges contribute to each cluster, handling them separately makes the code
    # simpler
    self_loop_contribution = clustering.select(contr=0.0).update_rows(
        edges.filter(edges.u == edges.v).with_id(pw.this.v).select(contr=pw.this.weight)
    )

    edges = edges.filter(edges.u != edges.v)

    # build vertex - cluster graph
    # # add no-weight edges from each vertex to its cluster
    # # they change nothing in the objective function, and allow us to
    # # handle clusters with no incoming edges
    placeholder_edges = clustering.select(u=clustering.id, vc=clustering.c, weight=0.0)
    # compute edges vertex-cluster out of the set of input edges
    vertex_cluster_edges = pw.Table.concat_reindex(
        placeholder_edges,
        edges.with_columns(vc=clustering.ix(edges.v).c).without(pw.this.v),
    ).select(*(pw.this))

    # aggregate the gains for adjacent clusters, adjust for the self loops
    # self loops are counted with weight 0.5, as they were created via graph contraction,
    # which counted each loop twice
    aggregated_gain = vertex_cluster_edges.groupby(pw.this.u, pw.this.vc).reduce(
        pw.this.u,
        pw.this.vc,
        gain=pw.reducers.sum(pw.this.weight)
        + self_loop_contribution.ix(pw.this.u).contr / 2,
        total_weight=clustering.ix(pw.this.u).total_weight,
    )

    @udf(deterministic=True)
    def louvain_gain(gain, degree, penalty, total):
        return 2.0 * gain - degree * (2.0 * penalty + degree) / total

    gain_from_moving = aggregated_gain.select(
        pw.this.u,
        pw.this.vc,
        pw.this.total_weight,
        gain=louvain_gain(
            aggregated_gain.gain,
            vertex_degrees.ix(aggregated_gain.u).degree,
            cluster_penalties.ix(aggregated_gain.vc).unscaled_penalty,
            aggregated_gain.total_weight,
        ),
    )

    gain_for_staying = (
        clustering.select(
            u=clustering.id, vc=clustering.c, total_weight=clustering.total_weight
        )
        .with_id_from(pw.this.u, pw.this.vc)
        .select(
            pw.this.u,
            pw.this.vc,
            pw.this.total_weight,
            gain=louvain_gain(
                aggregated_gain.ix(pw.this.id).gain,
                vertex_degrees.ix(pw.this.u).degree,
                cluster_penalties.ix(pw.this.vc).unscaled_penalty
                - vertex_degrees.ix(pw.this.u).degree,
                pw.this.total_weight,
            ),
        )
    )

    ret = gain_from_moving.update_rows(gain_for_staying)
    return (
        argmax_rows(ret, ret.u, what=ret.gain)
        .with_id(pw.this.u)
        .select(c=pw.this.vc, total_weight=pw.this.total_weight)
        .with_universe_of(clustering)
    )


def _one_step(
    G: WeightedGraph, clustering: pw.Table[Clustering], iter
) -> pw.Table[Clustering]:
    r"""
    This function selects a set of vertices that can be moved in parallel,
    while increasing the Louvain objective function.

    First, it calls _propose_clusters to compute a possible new cluster for each
    vertex. Then, it computes an independent set of movements that can be safely
    executed in parallel (i.e., no cluster participates in two movements)

    In some cases, this might be as slow as sequential implementation, however it
    uses parallel movements whenever they are easily detectable.
    """

    """
    Most of the code within this function handles the detection of parallel
    movements that can be safely executed.
    """

    # Select vertices that actually move, attach cluster of a vertex,
    # to determine the edge adjacency in the cluster graph, also on the u endpoint
    proposed_clusters = _propose_clusters(G.WE, clustering)
    candidate_moves = proposed_clusters.filter(
        proposed_clusters.c != clustering.ix(proposed_clusters.id).c
    ).with_columns(
        u=pw.this.id,
        uc=clustering.ix(pw.this.id).c,
        vc=pw.this.c,
        total_weight=pw.this.total_weight,
    )
    """
    find independent set of edges in the cluster graph
    by selecting local maxima over random priority
    """

    @udf(deterministic=True)
    def rand(x) -> int:
        return fingerprint((x, iter), format="i64")

    # sample priorities
    candidate_moves += candidate_moves.select(r=rand(candidate_moves.id))
    # compute maximum priority over all incident edges
    out_priorities = candidate_moves.select(
        candidate_moves.r, c=candidate_moves.uc, total_weight=pw.this.total_weight
    )
    in_priorities = candidate_moves.select(
        candidate_moves.r, c=candidate_moves.vc, total_weight=pw.this.total_weight
    )
    all_priorities = pw.Table.concat_reindex(out_priorities, in_priorities)
    cluster_max_priority = argmax_rows(
        all_priorities, all_priorities.c, what=all_priorities.r
    ).with_id(pw.this.c)

    # take edges e with same priority as the max priorities of clusters
    # containing the endpoints of e

    delta = (
        candidate_moves.filter(
            (candidate_moves.r == cluster_max_priority.ix(candidate_moves.uc).r)
            & (candidate_moves.r == cluster_max_priority.ix(candidate_moves.vc).r)
        )
        .with_id(pw.this.u)
        .select(c=pw.this.vc, total_weight=pw.this.total_weight)
    )

    return clustering.update_rows(delta).with_universe_of(clustering)


@check_arg_types
@trace_user_frame
def _louvain_level(G: WeightedGraph) -> pw.Table[Clustering]:
    r"""
    This function, given a weighted graph, finds a clustering that
    is a local maximum with respect to the objective function as defined
    by Louvain community detection algorithm
    """
    # arbitrary new ID generation;
    # without re-generating we sometimes end up in a situation in which
    # a cluster of id X does not contain a vertex of id X
    clustering = G.V.select(
        c=G.V.pointer_from(G.V.id), total_weight=G.V.apx_value
    ).update_types(c=pw.Pointer[Any])
    return pw.iterate(
        lambda clustering, V, WE: dict(
            clustering=_one_step(
                WeightedGraph.from_vertices_and_weighted_edges(V, WE),
                clustering,
                # number below needs to be replaced by the iteration number
                42,
            )
        ),
        V=G.V,
        WE=G.WE,
        clustering=clustering,
    ).clustering.without(pw.this.total_weight)


def _louvain_level_fixed_iterations(G: WeightedGraph, number_of_iterations):
    # arbitrary new ID generation;
    # without re-generating we sometimes end up in a situation in which
    # a cluster of id X does not contain a vertex of id X
    clustering = G.V.select(c=G.V.pointer_from(G.V.id), total_weight=G.V.apx_value)

    for iter in range(number_of_iterations):
        clustering = _one_step(G, clustering, iter)
    return clustering.without(clustering.total_weight)


def _approximate_total_weight(edges: pw.Table[Edge | Weight], epsilon):
    # compute total weight
    exact = edges.groupby().reduce(m=pw.reducers.sum(edges.weight))
    # return approximate total weight
    return exact.select(
        lower=pw.apply_with_type(
            lambda x: (1 + epsilon) ** math.floor(math.log(x, 1 + epsilon)),
            float,
            exact.m,
        ),
        value=exact.m,
        upper=pw.apply_with_type(
            lambda x: (1 + epsilon) ** (math.floor(math.log(x, 1 + epsilon) + 1)),
            float,
            exact.m,
        ),
    )


class louvain_communities_fixed_iterations:
    hierarchical_clustering: pw.Table
    clustering_levels: pw.Table
    levels: int
    G: WeightedGraph

    def __init__(self, G: WeightedGraph, apx, levels):
        self.G = G
        self.levels = levels
        self._louvain_communities_fixed_iterations(apx, levels)

    def _louvain_communities_fixed_iterations(self, apx, levels):
        initial_clustering = self.G.V.select(c=self.G.V.id, level=0)

        # keeps hierarchical clustering:
        # for each cluster it keeps
        # - its level,
        # - a pointer to a parent cluster
        self.hierarchical_clustering = initial_clustering

        # keeps flattened hierarchical clustering:
        # for each vertex it keeps all ancestors on all levels,
        # that is, there is one pair (ancestor, level) per row
        # the id's are generated from pair (vertex id, level)

        self.clustering_levels = initial_clustering.with_columns(
            v=initial_clustering.id
        ).with_id_from(pw.this.v, pw.this.level)

        total_weight = _approximate_total_weight(self.G.WE, apx)

        self.G.V = self.G.V._gradual_broadcast(
            total_weight, total_weight.lower, total_weight.value, total_weight.upper
        )

        G_ = self.G
        for lvl in range(levels):
            clustering = _louvain_level(G_)
            # update clusterings
            self.hierarchical_clustering = self.hierarchical_clustering.update_rows(
                clustering.with_columns(level=lvl + 1)
            )
            prev_lvl = self.clustering_levels.filter(
                self.clustering_levels.level == lvl
            )
            self.clustering_levels = self.clustering_levels.update_rows(
                prev_lvl.select(
                    prev_lvl.v, clustering.ix(prev_lvl.c).c, level=lvl + 1
                ).with_id_from(pw.this.v, pw.this.level)
            )
            G_ = G_.contracted_to_weighted_simple_graph(
                clustering, weight=pw.reducers.sum(G_.WE.weight)
            )
            G_.V = G_.V._gradual_broadcast(
                total_weight, total_weight.lower, total_weight.value, total_weight.upper
            )


def exact_modularity(
    G: WeightedGraph, C: pw.Table[Clustering], round_digits=16
) -> pw.Table:
    r"""
    This function computes modularity of a given weighted graph G with
    respect to clustering C.

    This implementation is meant to be used for testing / development,
    as computing exact value requires us to know the exact sum of the edge weights,
    which creates long dependency chains, and may be slow.

    This implementation rounds the modularity to round_digits decimal places
    (default is 16), for result res it returns round(res, ndigits = round_digits)
    """
    clusters = C.groupby(id=C.c).reduce()

    cluster_degrees = clusters.with_columns(degree=0.0).update_rows(
        G.WE.with_columns(c=C.ix(G.WE.u).c)
        .groupby(id=pw.this.c)
        .reduce(degree=pw.reducers.sum(pw.this.weight))
    )
    #
    cluster_internal = clusters.with_columns(internal=0.0).update_rows(
        G.WE.with_columns(cu=C.ix(G.WE.u).c, cv=C.ix(G.WE.v).c)
        .filter(pw.this.cu == pw.this.cv)
        .groupby(id=pw.this.cu)
        .reduce(internal=pw.reducers.sum(pw.this.weight))
    )

    total_weight = G.WE.reduce(m=pw.reducers.sum(pw.this.weight))

    def cluster_modularity(internal: float, degree: float, total: float) -> float:
        return (internal * total - degree * degree) / (total * total)

    score = clusters.join(total_weight, id=clusters.id).select(
        modularity=pw.apply(
            cluster_modularity,
            cluster_internal.ix(pw.this.id).internal,
            cluster_degrees.ix(pw.this.id).degree,
            total_weight.m,
        )
    )

    return score.reduce(
        modularity=pw.reducers.sum(score.modularity).num.round(round_digits)
    )

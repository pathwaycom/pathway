# Copyright © 2024 Pathway

"""k-approximate nearest neighbor search (and classification) via LSH.

  Fine-grained variant, where the distances are computed separately for all the relevant (query,data) pairs.
  the smallest-k filtering is done by engine.

  Typical usage example:

    bucketer = generate_euclidean_lsh_bucketer(d, M, L, A)

    and then to get k-NN:

    knns = k_approximate_nearest_neighbors_flat(data, queries, bucketer)

    or to directly classify:

    predictions = knn_classifier_flat(data, labels, queries, bucketer)

  Each query row should contain it's value of k.
  The algorithm should be incremental with respect to k as well.

  See projects/pathway/experimental/mateusz/kNN_lsh.ipynb for experiments.
"""

from __future__ import annotations

import numpy as np

import pathway as pw

from ._lsh import lsh


class DataPoint(pw.Schema):
    data: np.ndarray


class Query(pw.Schema):
    data: np.ndarray
    k: int


def compute_euclidean_dist2(datapoint: np.ndarray, querypoint: np.ndarray) -> float:
    return np.sum((datapoint - querypoint) ** 2).item()


def compute_cosine_dist(datapoint: np.ndarray, querypoint: np.ndarray) -> float:
    return (
        1
        - np.dot(datapoint, querypoint)
        / (np.linalg.norm(datapoint) * np.linalg.norm(querypoint))
    ).item()


def k_approximate_nearest_neighbors_flat(
    data: pw.Table[DataPoint],
    queries: pw.Table[Query],
    bucketer,
    dist_fun=compute_euclidean_dist2,
) -> pw.Table:
    """Finds k-approximate nearest neighbors of points in queries table against points in data table.

    Requires supplying LSH bucketer, see e.g. generate_euclidean_lsh_bucketer.
    dist_fun is a distance function which should be chosen according to the bucketer.
    """
    flat_data = lsh(data, bucketer, origin_id="data_id", include_data=False)
    flat_queries = lsh(queries, bucketer, origin_id="query_id", include_data=False)

    unique = (
        flat_queries.join(
            flat_data,
            flat_queries.bucketing == flat_data.bucketing,
            flat_queries.band == flat_data.band,
        )
        .select(
            flat_queries.query_id,
            flat_data.data_id,
        )
        .groupby(pw.this.query_id, pw.this.data_id)
        .reduce(pw.this.query_id, pw.this.data_id)
    )

    distances = unique + unique.select(
        dist=pw.apply(
            dist_fun, data.ix(unique.data_id).data, queries.ix(unique.query_id).data
        ),
    )

    # TODO this assert could be deduced
    nonempty_queries = distances.groupby(id=distances.query_id).reduce()
    pw.universes.promise_is_subset_of(nonempty_queries, queries)
    queries_restricted = queries.restrict(nonempty_queries)
    ks = nonempty_queries.select(queries_restricted.k, instance=queries_restricted.id)
    topk = pw.indexing.filter_smallest_k(distances.dist, distances.query_id, ks)
    return topk.select(topk.query_id, topk.data_id)


def knn_classifier_flat(
    data: pw.Table[DataPoint],
    labels: pw.Table,
    queries: pw.Table[Query],
    bucketer,
    dist_fun=compute_euclidean_dist2,
) -> pw.Table:
    """Classifies queries against labeled data using approximate k-NN."""
    knns = k_approximate_nearest_neighbors_flat(data, queries, bucketer, dist_fun)
    labeled = knns.select(knns.query_id, label=labels.ix(knns.data_id).label)
    predictions = pw.utils.col.groupby_reduce_majority(labeled.query_id, labeled.label)
    return predictions.select(predicted_label=predictions.majority).with_id(
        predictions.query_id
    )

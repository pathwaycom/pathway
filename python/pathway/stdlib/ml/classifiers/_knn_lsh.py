# Copyright © 2024 Pathway

"""k-approximate nearest neighbor search (and classification) via LSH.

  Coarse variant, where the buckets are treated as a unit of computation: the smallest-k are computed in Python.

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

import fnmatch
import logging
from statistics import mode
from typing import Literal

import jmespath
import jmespath.functions
import numpy as np

# TODO change to `import pathway as pw` when it is not imported as part of stdlib, OR move the whole file to stdlib
import pathway.internals as pw
from pathway.internals.helpers import StableSet
from pathway.stdlib.utils.col import unpack_col

from ._lsh import generate_cosine_lsh_bucketer, generate_euclidean_lsh_bucketer

DistanceTypes = Literal["euclidean", "cosine"]


class DataPoint(pw.Schema):
    data: np.ndarray


def _euclidean_distance(data_table: np.ndarray, query_table: np.ndarray) -> np.ndarray:
    return np.sum((data_table - query_table) ** 2, axis=1).astype(float)


def compute_cosine_dist(data_table: np.ndarray, query_point: np.ndarray) -> np.ndarray:
    return 1 - np.dot(data_table, query_point) / (
        np.linalg.norm(data_table, axis=1) * np.linalg.norm(query_point)
    )


class MetaDataSchema:
    metadata: dict


def knn_lsh_classifier_train(
    data: pw.Table[DataPoint],
    L: int,
    type: DistanceTypes = "euclidean",
    **kwargs,
):
    """
    Build the LSH index over data.
    L the number of repetitions of the LSH scheme.
    Returns a LSH projector of type (queries: Table, k:Any) -> Table
    """
    if type == "euclidean":
        lsh_projection = generate_euclidean_lsh_bucketer(
            kwargs["d"], kwargs["M"], L, kwargs["A"]
        )
        return knn_lsh_generic_classifier_train(
            data,
            lsh_projection,
            _euclidean_distance,
            L,
        )
    elif type == "cosine":
        lsh_projection = generate_cosine_lsh_bucketer(kwargs["d"], kwargs["M"], L)
        return knn_lsh_generic_classifier_train(
            data,
            lsh_projection,
            compute_cosine_dist,
            L,
        )
    else:
        raise ValueError(
            f"Not supported `type` {type} in knn_lsh_classifier_train. "
            "The allowed values are 'euclidean' and 'cosine'."
        )


# support for glob metadata search
def _globmatch_impl(pat_i, pat_n, pattern, p_i, p_n, path):
    """Match pattern to path, recursively expanding **."""
    if pat_i == pat_n:
        return p_i == p_n
    if p_i == p_n:
        return False
    if pattern[pat_i] == "**":
        return _globmatch_impl(
            pat_i, pat_n, pattern, p_i + 1, p_n, path
        ) or _globmatch_impl(pat_i + 1, pat_n, pattern, p_i, p_n, path)
    if fnmatch.fnmatch(path[p_i], pattern[pat_i]):
        return _globmatch_impl(pat_i + 1, pat_n, pattern, p_i + 1, p_n, path)
    return False


def _globmatch(pattern, path):
    """globmatch path to patter, using fnmatch at every level."""
    pattern_parts = pattern.split("/")
    path_parts = path.split("/")
    return _globmatch_impl(
        0, len(pattern_parts), pattern_parts, 0, len(path_parts), path_parts
    )


class CustomFunctions(jmespath.functions.Functions):
    @jmespath.functions.signature({"types": ["string"]}, {"types": ["string"]})
    def _func_globmatch(self, pattern, string):
        # Given a string, check if it matches the globbing pattern
        return _globmatch(pattern, string)


_glob_options = jmespath.Options(custom_functions=CustomFunctions())


def knn_lsh_generic_classifier_train(
    data: pw.Table, lsh_projection, distance_function, L: int
):
    """
    Build the LSH index over data using the a generic lsh_projector and its associated distance.
    L the number of repetitions of the LSH scheme.
    Returns a LSH projector of type (queries: Table, k:Any) -> Table
    """

    def make_band_col_name(i):
        return f"band_{i}"

    band_col_names = [make_band_col_name(i) for i in range(L)]
    data += data.select(buckets=pw.apply(lsh_projection, data.data))
    # Fix "UserWarning: Object in (<table1>.buckets)[8] is of type numpy.ndarray
    # but its number of dimensions is not known." when calling unpack_col with ndarray.
    # pw.cast not working and unpack_col doesnt take pw.apply so I used pw.apply separately.
    buckets_list = data.select(buckets=pw.apply(list, data.buckets))
    data += unpack_col(buckets_list.buckets, *band_col_names)

    if "metadata" not in data._columns:
        data += data.select(metadata=None)

    def lsh_perform_query(
        queries: pw.Table, k: int | None = None, with_distances=False
    ) -> pw.Table:
        queries += queries.select(buckets=pw.apply(lsh_projection, queries.data))
        if k is not None:
            queries += queries.select(k=k)

        # Same Fix "UserWarning: ... above"
        buckets_list = queries.select(buckets=pw.apply(list, queries.buckets))
        queries += unpack_col(buckets_list.buckets, *band_col_names)

        # step 2: for each query, take union of matching databuckets
        result = queries

        for band_i in range(L):
            band = data.groupby(data[make_band_col_name(band_i)]).reduce(
                data[make_band_col_name(band_i)],
                items=pw.reducers.sorted_tuple(data.id),
            )
            result += queries.select(**{f"items_{band_i}": ()}).update_rows(
                queries.join(
                    band,
                    queries[make_band_col_name(band_i)]
                    == band[make_band_col_name(band_i)],
                    id=queries.id,
                ).select(
                    **{f"items_{band_i}": band.items},
                )
            )

        @pw.udf
        def merge_buckets(*tuples: tuple[pw.Pointer, ...]) -> tuple[pw.Pointer, ...]:
            return tuple(StableSet(sum(tuples, ())))

        if "metadata_filter" not in result._columns:
            result = result.with_columns(metadata_filter=None)

        flattened = result.select(
            pw.this.data,
            pw.this.k,
            pw.this.metadata_filter,
            query_id=result.id,
            ids=merge_buckets(*[result[f"items_{i}"] for i in range(L)]),
        ).filter(pw.this.ids != ())

        actually_flattened = flattened.flatten(pw.this.ids, origin_id="origin_id")
        actually_flattened = (
            actually_flattened[["origin_id", "ids"]]
            + data.ix(actually_flattened.ids)[["data", "metadata"]]
        )
        flattened_with_data = flattened.without(pw.this.ids) + (
            actually_flattened.groupby(id=pw.this.origin_id).reduce(
                training_data=pw.reducers.tuple(pw.this.data),
                training_metadata=pw.reducers.tuple(pw.this.metadata),
                ids=pw.reducers.tuple(pw.this.ids),
            )
        ).with_universe_of(flattened)

        # step 3: find knns in unioned buckets

        @pw.udf
        def knns(
            querypoint, training_data, training_metadata, ids, metadata_filter, k
        ) -> list[tuple[pw.Pointer, float]]:
            try:
                candidates = [
                    (
                        id_candidate,
                        data,
                    )
                    for id_candidate, data, metadata in zip(
                        ids, training_data, training_metadata
                    )
                    if metadata_filter is None
                    or jmespath.search(
                        metadata_filter,
                        metadata.value,
                        options=_glob_options,
                    )
                    is True
                ]
            except jmespath.exceptions.JMESPathError:
                logging.exception("Incorrect JMESPath expression for metadata filter")
                candidates = []

            if len(candidates) == 0:
                return []
            ids_filtered, data_candidates_filtered = zip(*candidates)
            data_candidates = np.array(data_candidates_filtered)
            neighs = min(k, len(data_candidates))
            distances = distance_function(data_candidates, querypoint)
            knn_ids = np.argpartition(
                distances,
                neighs - 1,
            )[
                :neighs
            ]  # neighs - 1 in argpartition, because of 0-based indexing
            k_distances = distances[knn_ids]
            final_ids = np.array(ids_filtered)[knn_ids]
            if len(final_ids) == 0:
                return []
            sorted_dists, sorted_ids_by_dist = zip(*sorted(zip(k_distances, final_ids)))
            return list(zip(sorted_ids_by_dist, sorted_dists))

        knn_result = flattened_with_data.select(
            pw.this.query_id,
            knns_ids_with_dists=knns(
                pw.this.data,
                pw.this.training_data,
                pw.this.training_metadata,
                pw.this.ids,
                pw.this.metadata_filter,
                pw.this.k,
            ),
        )
        result = queries.join_left(
            knn_result, queries.id == knn_result.query_id
        ).select(
            knns_ids_with_dists=pw.coalesce(knn_result.knns_ids_with_dists, ()),
            query_id=queries.id,
        )

        if not with_distances:
            result = result.select(
                pw.this.query_id,
                knns_ids=pw.apply(
                    lambda x: tuple(zip(*x))[0] if len(x) > 0 else (),
                    pw.this.knns_ids_with_dists,
                ),
            )
        return result

    return lsh_perform_query


def knn_lsh_euclidean_classifier_train(data: pw.Table, d, M, L, A):
    """
    Build the LSH index over data using the Euclidean distances.
    d is the dimension of the data, L the number of repetition of the LSH scheme,
    M and A are specific to LSH with Euclidean distance, M is the number of random projections
    done to create each bucket and A is the width of each bucket on each projection.
    """
    lsh_projection = generate_euclidean_lsh_bucketer(d, M, L, A)
    return knn_lsh_generic_classifier_train(
        data, lsh_projection, _euclidean_distance, L
    )


def knn_lsh_classify(knn_model, data_labels: pw.Table, queries: pw.Table, k):
    """Classify the queries.
    Use the knn_model to extract the k closest datapoints.
    The queries are then labeled using a majority vote between the labels
    of the retrieved datapoints, using the labels provided in data_labels.
    """
    knns: pw.Table = knn_model(queries, k)

    knns_nonempty = (
        knns.flatten(pw.this.knns_ids)
        .update_types(knns_ids=pw.Pointer)
        .with_columns(data_labels.ix(pw.this.knns_ids).label)
        .groupby(id=pw.this.query_id)
        .reduce(predicted_label=pw.apply(mode, pw.reducers.tuple(pw.this.label)))
        .update_types(predicted_label=data_labels.typehints()["label"])
    )
    knns_empty = knns.with_id(pw.this.query_id).select(predicted_label=None)
    return knns_empty.update_cells(
        knns_nonempty.promise_universe_is_subset_of(knns_empty)
    )

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
from pathway.stdlib.utils.col import groupby_reduce_majority, unpack_col

from ._lsh import generate_cosine_lsh_bucketer, generate_euclidean_lsh_bucketer

DistanceTypes = Literal["euclidean", "cosine"]


class DataPoint(pw.Schema):
    data: np.ndarray


def _euclidean_distance(data_table: np.ndarray, query_table: np.ndarray) -> np.ndarray:
    return np.sum((data_table - query_table) ** 2, axis=1)


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

        def merge_buckets(*tuples: list[tuple]) -> tuple:
            return tuple(StableSet(sum(tuples, ())))

        if "metadata_filter" not in result._columns:
            result += result.select(metadata_filter=None)

        flattened = result.select(
            result.data,
            query_id=result.id,
            ids=pw.apply(merge_buckets, *[result[f"items_{i}"] for i in range(L)]),
            k=result.k,
            metadata_filter=result.metadata_filter,
        ).filter(pw.this.ids != ())

        # step 3: find knns in unioned buckets
        @pw.transformer
        class compute_knns_transformer:
            class training_data(pw.ClassArg):
                data = pw.input_attribute()
                metadata = pw.input_attribute()

            class flattened(pw.ClassArg):
                data = pw.input_attribute()
                query_id = pw.input_attribute()
                ids = pw.input_attribute()
                k = pw.input_attribute()
                metadata_filter = pw.input_attribute()

                @pw.output_attribute
                def knns(self) -> list[tuple[pw.Pointer, float]]:
                    querypoint = self.data
                    for id_candidate in self.ids:
                        try:
                            self.transformer.training_data[id_candidate].data
                            self.transformer.training_data[id_candidate].metadata
                        except Exception:
                            raise
                        except BaseException:
                            # Used to prefetch values
                            pass

                    try:
                        candidates = [
                            (
                                id_candidate,
                                self.transformer.training_data[id_candidate].data,
                            )
                            for id_candidate in self.ids
                            if self.metadata_filter is None
                            or jmespath.search(
                                self.metadata_filter,
                                self.transformer.training_data[
                                    id_candidate
                                ].metadata.value,
                                options=_glob_options,
                            )
                            is True
                        ]
                    except jmespath.exceptions.JMESPathError:
                        logging.exception(
                            "Incorrect JMESPath expression for metadata filter"
                        )
                        candidates = []

                    if len(candidates) == 0:
                        return []
                    ids_filtered, data_candidates_filtered = zip(*candidates)
                    data_candidates = np.array(data_candidates_filtered)
                    neighs = min(self.k, len(data_candidates))
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
                    sorted_dists, sorted_ids_by_dist = zip(
                        *sorted(zip(k_distances, final_ids))
                    )
                    return list(zip(sorted_ids_by_dist, sorted_dists))

        knn_result: pw.Table = compute_knns_transformer(  # type: ignore
            training_data=data, flattened=flattened
        ).flattened.select(flattened.query_id, knns_ids_with_dists=pw.this.knns)

        knn_result_with_empty_results = queries.join_left(
            knn_result, queries.id == knn_result.query_id
        ).select(knn_result.knns_ids_with_dists, query_id=queries.id)

        result = knn_result_with_empty_results.with_columns(
            knns_ids_with_dists=pw.coalesce(pw.this.knns_ids_with_dists, ()),
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
    knns = knn_model(queries, k)

    # resultA = process_knn_no_transformers(data_labels, knns)
    # resultB = process_knn_two_transformers(data_labels, knns)
    result = (
        take_majority_label(labels=data_labels, knn_table=knns)  # type: ignore
        .knn_table.with_id(knns.query_id)
        .update_types(predicted_label=data_labels.typehints()["label"])
    )

    return result


@pw.transformer
class take_majority_label:
    class labels(pw.ClassArg):
        label = pw.input_attribute()

    class knn_table(pw.ClassArg):
        knns_ids = pw.input_attribute()

        @pw.output_attribute
        def predicted_label(self):
            try:
                for x in self.knns_ids:
                    self.transformer.labels[x].label
            except Exception:
                raise
            except BaseException:
                # Used to prefetch values
                pass
            if self.knns_ids != ():
                return mode(self.transformer.labels[x].label for x in self.knns_ids)
            else:
                return None


def process_knn_no_transformers(data_labels, knns):
    labeled = (
        knns.flatten(pw.this.knns_ids, row_id=pw.this.id)
        .join(knns, pw.this.row_id == knns.id)
        .select(knns.query_id, data_labels.ix(pw.this.knns_ids).label)
    )

    # labeled = flatten_column(knns.knns_ids, origin_id=knns.query_id).select(
    #     pw.this.query_id, data_labels.ix(pw.this.knns_ids).label
    # )
    predictions = groupby_reduce_majority(labeled.query_id, labeled.label)

    results = predictions.select(predicted_label=predictions.majority).with_id(
        predictions.query_id
    )
    return results


def process_knn_two_transformers(data_labels, knns):
    labeled = (
        substitute_with_labels(labels=data_labels, knn_table=knns)  # type: ignore
        .knn_table.select(labels=pw.this.knn_labels)
        .with_id(knns.query_id)
    )

    result: pw.Table = (
        compute_mode_empty_to_none(labeled.select(items=labeled.labels))  # type: ignore
        .data.select(predicted_label=pw.this.mode)
        .with_id(labeled.id)
    )
    return result


@pw.transformer
class substitute_with_labels:
    class labels(pw.ClassArg):
        label = pw.input_attribute()

    class knn_table(pw.ClassArg):
        knns_ids = pw.input_attribute()

        @pw.output_attribute
        def knn_labels(self):
            return tuple(self.transformer.labels[x].label for x in self.knns_ids)


@pw.transformer
class compute_mode_empty_to_none:
    class data(pw.ClassArg):
        items = pw.input_attribute()

        @pw.output_attribute
        def mode(self):
            if self.items != ():
                return mode(self.items)
            else:
                return None

# Copyright © 2023 Pathway

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

from statistics import mode
from typing import List, Tuple

import numpy as np

# TODO change to `import pathway as pw` when it is not imported as part of stdlib, OR move the whole file to stdlib
import pathway.internals as pw
from pathway.internals.helpers import StableSet
from pathway.stdlib.utils.col import groupby_reduce_majority, unpack_col

from ._lsh import generate_euclidean_lsh_bucketer


class DataPoint(pw.Schema):
    data: np.ndarray


def _euclidean_distance(data_table: np.ndarray, query_table: np.ndarray):
    return np.sum((data_table - query_table) ** 2, axis=1)


def knn_lsh_classifier_train(data: pw.Table[DataPoint], L, type="euclidean", **kwargs):
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
    raise Exception("Classifier train failure: missing/wrong arguments.")


def knn_lsh_generic_classifier_train(
    data: pw.Table, lsh_projection, distance_function, L
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

    def lsh_perform_query(queries: pw.Table, k):
        queries += queries.select(buckets=pw.apply(lsh_projection, queries.data))

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

        def merge_buckets(*tuples: List[Tuple]) -> Tuple:
            return tuple(StableSet(sum(tuples, ())))

        flattened = result.select(
            result.data,
            query_id=result.id,
            ids=pw.apply(merge_buckets, *[result[f"items_{i}"] for i in range(L)]),
        ).filter(pw.apply_with_type(lambda x: x != (), bool, pw.this.ids))

        # step 3: find knns in unioned buckets
        @pw.transformer
        class compute_knns_transformer:
            class training_data(pw.ClassArg):
                data = pw.input_attribute()

            class flattened(pw.ClassArg):
                data = pw.input_attribute()
                query_id = pw.input_attribute()
                ids = pw.input_attribute()

                @pw.output_attribute
                def knns_ids(self) -> np.ndarray:
                    querypoint = self.data
                    for id_candidate in self.ids:
                        try:
                            self.transformer.training_data[id_candidate].data
                        except BaseException:
                            pass
                    data_candidates = np.array(
                        [
                            self.transformer.training_data[id_candidate].data
                            for id_candidate in self.ids
                        ]
                    )

                    neighs = min(k, len(data_candidates))
                    knn_ids = np.argpartition(
                        distance_function(data_candidates, querypoint), neighs - 1
                    )[
                        :neighs
                    ]  # neighs - 1 in argpartition, because of 0-based indexing
                    ret = np.array(self.ids)[knn_ids]
                    return ret

        knn_result = compute_knns_transformer(  # type: ignore
            training_data=data, flattened=flattened
        ).flattened.select(pw.this.knns_ids, flattened.query_id)

        # return knn_result

        knn_result_with_empty_results = queries.join_left(
            knn_result, queries.id == knn_result.query_id
        ).select(knn_result.knns_ids, query_id=queries.id)

        knn_result_with_empty_results = knn_result_with_empty_results.with_columns(
            knns_ids=pw.coalesce(pw.this.knns_ids, np.array(()))
        ).update_types(knns_ids=knn_result.schema["knns_ids"])
        # return knn_result
        return knn_result_with_empty_results

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


def knn_lsh_classify(knn_model, data_labels, queries, k):
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
        .update_types(predicted_label=data_labels.schema["label"])
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
            except BaseException:
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

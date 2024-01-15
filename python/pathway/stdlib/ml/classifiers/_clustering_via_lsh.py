# Copyright © 2024 Pathway

"""(Pre)clustering via LSH

  See projects/pathway/experimental/mateusz/LSH.ipynb for explanation and experiments.

  Warning: clustering results might be unstable. TODO: stable clustering or hot_apply
"""

from __future__ import annotations

import numpy as np

import pathway as pw

from ._lsh import lsh


class DataPoint(pw.Schema):
    data: np.ndarray


class Label:
    label: int


def np_divide(data: np.ndarray, other: float) -> np.ndarray:
    return data / other


def clustering_via_lsh(
    data: pw.Table[DataPoint], bucketer, k: int
) -> pw.Table[DataPoint | Label]:  # type: ignore
    flat_data = lsh(data, bucketer, origin_id="data_id", include_data=True)

    representatives = (
        flat_data.groupby(flat_data.bucketing, flat_data.band)
        .reduce(
            flat_data.bucketing,
            flat_data.band,
            sum=pw.reducers.sum(flat_data.data),
            count=pw.reducers.count(),
        )
        .select(
            pw.this.bucketing,
            pw.this.band,
            data=pw.apply(
                np_divide, pw.this.sum, pw.this.count
            ),  # TODO: operators for np.ndarray
            weight=pw.this.count,
        )
    )

    def clustering(data: list[np.ndarray], weights: list[float]) -> list[float]:
        from sklearn.cluster import KMeans

        kmeans = KMeans(n_clusters=3, init="k-means++", random_state=0, n_init=10)
        kmeans.fit(data, sample_weight=weights)

        return kmeans.labels_

    labels = pw.utils.col.apply_all_rows(
        representatives.data,
        representatives.weight,
        fun=clustering,
        result_col_name="label",
    )
    representatives += labels
    votes = flat_data.join(
        representatives,
        flat_data.bucketing == representatives.bucketing,
        flat_data.band == representatives.band,
    ).select(
        flat_data.data_id,
        representatives.label,
    )

    result = pw.utils.col.groupby_reduce_majority(votes.data_id, votes.label)
    return result.select(label=result.majority).with_id(result.data_id)

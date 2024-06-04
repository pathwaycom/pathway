# Copyright © 2024 Pathway

from __future__ import annotations

import numpy as np
import pandas as pd

from pathway.debug import table_to_pandas
from pathway.internals import api
from pathway.stdlib.ml.classifiers import knn_lsh_classifier_train, knn_lsh_classify
from pathway.tests.utils import T, assert_table_equality


def test_aknn():
    """Verifies that approximate knn searches return some closeby points."""
    data = pd.DataFrame(
        {
            "data": [
                np.array([9, 9, 9]),
                np.array([10, 10, 10]),
                np.array([12, 12, 12]),
                np.array([-9, -9, -9]),
                np.array([-10, -10, -10]),
                np.array([-12, -12, -12]),
                np.array([1, 1, 1]),
                np.array([3, 3, 3]),
            ]
        }
    )
    data = T(data, format="pandas", unsafe_trusted_ids=True)
    queries = pd.DataFrame(
        {
            "data": [
                np.array([11, 11, 11]),
                np.array([-11, -11, -11]),
                np.array([2, 2, 2]),
                np.array([1000, 1000, 1000]),
            ],
        },
        index=[0, 1, 2, 3],
    )
    queries = T(queries, format="pandas", unsafe_trusted_ids=True)
    lsh_index = knn_lsh_classifier_train(data, L=5, type="euclidean", d=3, M=7, A=10)
    result = lsh_index(queries, k=3)
    result = result.with_id(result.query_id)
    result_pd = table_to_pandas(result)
    result_pd["knns_ids"] = result_pd["knns_ids"].map(lambda ids: set(map(int, ids)))
    assert len(result_pd) == 4
    assert set() < result_pd["knns_ids"].loc[api.unsafe_make_pointer(0)] <= {0, 1, 2}
    assert set() < result_pd["knns_ids"].loc[api.unsafe_make_pointer(1)] <= {3, 4, 5}
    assert set() < result_pd["knns_ids"].loc[api.unsafe_make_pointer(2)] <= {6, 7}
    assert (
        result_pd["knns_ids"].loc[api.unsafe_make_pointer(3)] == set()
    )  # no matches for last querypoint


def test_knn_classifier():
    data = pd.DataFrame(
        {
            "data": [
                [9, 9, 9],
                [10, 10, 10],
                [12, 12, 12],
                [-9, -9, -9],
                [-10, -10, -10],
                [-12, -12, -12],
                [1, 1, 1],
                [3, 3, 3],
            ]
        }
    )
    data = T(data, format="pandas", unsafe_trusted_ids=True)
    labels = pd.DataFrame({"label": [1, 1, 1, -1, -1, -1, 0, 0, 0]})
    labels = T(labels, format="pandas", unsafe_trusted_ids=True)
    queries = pd.DataFrame({"data": [[11, 11, 11], [-11, -11, -11], [2, 2, 2]]})
    queries = T(queries, format="pandas", unsafe_trusted_ids=True)
    lsh_index = knn_lsh_classifier_train(data, L=5, type="euclidean", d=3, M=8, A=10)
    result = knn_lsh_classify(lsh_index, labels, queries, k=3)

    assert_table_equality(
        result,
        T(
            """
       | predicted_label
    0  | 1
    1  | -1
    2  | 0
    """,
            unsafe_trusted_ids=True,
        ).update_types(predicted_label=int | None),
    )

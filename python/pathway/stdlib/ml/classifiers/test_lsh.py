# Copyright © 2024 Pathway

from __future__ import annotations

import numpy as np
import pandas as pd

from pathway import apply, reducers
from pathway.debug import table_to_pandas
from pathway.tests.utils import T, assert_table_equality_wo_index_types

from ._clustering_via_lsh import clustering_via_lsh
from ._lsh import generate_cosine_lsh_bucketer, generate_euclidean_lsh_bucketer, lsh


def test_bucketer_euclidean():
    """Verifies that L buckets were indeed created"""
    L = 7  # number of ORs
    bucketer = generate_euclidean_lsh_bucketer(d=3, M=5, L=L, A=3)

    data_df = pd.DataFrame({"data": [[1, 2, 3], [4, 5, 6]]})
    data = T(data_df, format="pandas", unsafe_trusted_ids=True)
    data += data.select(buckets=apply(bucketer, data.data))
    res_pd = table_to_pandas(data)
    assert len(res_pd["buckets"].iloc[0]) == L


def test_bucketer_cosine():
    """Verifies that L buckets were indeed created"""
    L = 7  # number of ORs
    bucketer = generate_cosine_lsh_bucketer(d=3, M=5, L=L)

    data_df = pd.DataFrame({"data": [[1, 2, 3], [4, 5, 6]]})
    data = T(data_df, format="pandas", unsafe_trusted_ids=True)
    data += data.select(buckets=apply(bucketer, data.data))
    res_pd = table_to_pandas(data)
    assert len(res_pd["buckets"].iloc[0]) == L


def test_lsh():
    """Verifies that close points are mapped together and distant ones - apart."""
    L = 3  # number of ORs
    data_df = pd.DataFrame({"data": [[1, 2, 3], [1.02, 2.01, 3.03], [4, 5, 6]]})
    data = T(data_df, format="pandas", unsafe_trusted_ids=True)

    bucketer = generate_euclidean_lsh_bucketer(d=3, M=5, L=L, A=3)
    flat_data = lsh(data, bucketer, origin_id="data_id")
    result = flat_data.groupby(flat_data.bucketing, flat_data.band).reduce(
        data_ids=reducers.sorted_tuple(apply(int, flat_data.data_id))
    )
    # TODO change app apply_with_type(int, int, ...) to cast(int, ...) once
    # we have cast from Pointer to int
    res_pd = table_to_pandas(result)
    assert np.array_equal(
        np.unique(res_pd["data_ids"]), np.array([(0, 1), (2,)], dtype=object)
    )  # point 0 and 1 are close together, point 2 is further away


def test_lsh_bucketing():
    """Verifies that bucketing is properly indexed."""
    L = 3  # number of ORs
    data_df = pd.DataFrame({"data": [[1, 2, 3], [1.02, 2.01, 3.03], [4, 5, 6]]})
    data = T(data_df, format="pandas", unsafe_trusted_ids=True)

    bucketer = generate_euclidean_lsh_bucketer(d=3, M=5, L=L, A=3)
    flat_data = lsh(data, bucketer, origin_id="data_id")
    result = flat_data.groupby(flat_data.bucketing).reduce(flat_data.bucketing)
    assert_table_equality_wo_index_types(
        result,
        T(
            """
        bucketing
            0
            1
            2
    """,
        ),
    )


def test_clustering_via_lsh():
    data_df = pd.DataFrame(
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
    data = T(data_df, format="pandas", unsafe_trusted_ids=True)
    bucketer = generate_euclidean_lsh_bucketer(d=3, M=7, L=5, A=10)
    result = clustering_via_lsh(data, bucketer, k=3)

    groups = table_to_pandas(result).groupby("label").groups
    assert set(groups.keys()) == {0, 1, 2}
    assert {tuple(sorted(int(x) for x in group)) for group in groups.values()} == {
        (0, 1, 2),
        (3, 4, 5),
        (6, 7),
    }

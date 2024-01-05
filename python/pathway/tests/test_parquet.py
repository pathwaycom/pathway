# Copyright © 2024 Pathway

import pandas as pd

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index


def test_write_parquet(tmp_path):
    dir = tmp_path / "test_parquet"
    dir.mkdir()
    path = dir / "test_write_parquet.parquet"

    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )

    pw.debug.table_to_parquet(tab, path)
    df = pd.read_parquet(path)
    T_parquet = pw.debug.table_from_pandas(df, id_from=None, unsafe_trusted_ids=False)

    assert_table_equality_wo_index(T_parquet, tab)


def test_read_parquet(tmp_path):
    dir = tmp_path / "test_parquet"
    dir.mkdir()
    path = dir / "test_read_parquet.parquet"

    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )

    df = pw.debug.table_to_pandas(tab)
    df = df.reset_index()
    df = df.drop(["index"], axis=1)

    df.to_parquet(path)
    T_parquet = pw.debug.table_from_parquet(
        path, id_from=None, unsafe_trusted_ids=False
    )

    assert_table_equality_wo_index(T_parquet, tab)

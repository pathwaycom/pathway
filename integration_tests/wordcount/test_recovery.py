# Copyright © 2024 Pathway

import pathlib

import pytest

from .base import FS_STORAGE_NAME, S3_STORAGE_NAME, do_test_failure_recovery_static


@pytest.mark.parametrize(
    "n_threads,n_processes", [(1, 1), (2, 1), (4, 1), (1, 4), (1, 2), (2, 2)]
)
@pytest.mark.parametrize("pstorage_type", [S3_STORAGE_NAME, FS_STORAGE_NAME])
@pytest.mark.parametrize(
    "n_backfilling_runs,min_work_time,max_work_time",
    [
        (3, 5.0, 10.0),
        (3, 0.75, 2.25),
        (3, 8.5, 15.5),
        (10, 5.0, 10.0),
        (3, 3.0, 6.0),
        (3, 6.0, 9.0),
    ],
)
def test_integration_failure_recovery(
    n_backfilling_runs,
    n_threads,
    n_processes,
    min_work_time,
    max_work_time,
    pstorage_type,
    tmp_path: pathlib.Path,
    port: int,
):
    do_test_failure_recovery_static(
        n_backfilling_runs=n_backfilling_runs,
        n_threads=n_threads,
        n_processes=n_processes,
        tmp_path=tmp_path,
        min_work_time=min_work_time,
        max_work_time=max_work_time,
        pstorage_type=pstorage_type,
        first_port=port,
    )

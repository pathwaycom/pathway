import pathlib

import pytest

from .base import FS_STORAGE_NAME, S3_STORAGE_NAME, do_test_failure_recovery_static


@pytest.mark.parametrize("n_cpus", [1, 2, 4])
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
    n_cpus,
    min_work_time,
    max_work_time,
    pstorage_type,
    tmp_path: pathlib.Path,
):
    do_test_failure_recovery_static(
        n_backfilling_runs,
        n_cpus,
        tmp_path,
        min_work_time,
        max_work_time,
        pstorage_type,
    )

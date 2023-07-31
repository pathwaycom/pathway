import pathlib

import pytest

from .base import do_test_failure_recovery_static


@pytest.mark.parametrize(
    "n_backfilling_runs,n_cpus,min_work_time,max_work_time",
    [
        (3, 1, 5.0, 10.0),
        (3, 2, 5.0, 10.0),
        (3, 4, 5.0, 10.0),
        (3, 1, 0.75, 2.25),
        (3, 2, 0.75, 2.25),
        (3, 4, 0.75, 2.25),
        (3, 1, 8.5, 15.5),
        (3, 2, 8.5, 15.5),
        (3, 4, 8.5, 15.5),
        (10, 1, 5.0, 10.0),
        (10, 2, 5.0, 10.0),
        (10, 4, 5.0, 10.0),
    ],
)
def test_integration_failure_recovery(
    n_backfilling_runs, n_cpus, min_work_time, max_work_time, tmp_path: pathlib.Path
):
    do_test_failure_recovery_static(
        n_backfilling_runs, n_cpus, tmp_path, min_work_time, max_work_time
    )

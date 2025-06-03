# Copyright © 2024 Pathway

import argparse
import pathlib

import pytest

from .base import (
    AZURE_STORAGE_NAME,
    FS_STORAGE_NAME,
    INPUT_PERSISTENCE_MODE_NAME,
    OPERATOR_PERSISTENCE_MODE_NAME,
    S3_STORAGE_NAME,
    do_test_failure_recovery,
)


@pytest.mark.parametrize("n_threads,n_processes", [(1, 1), (4, 1), (1, 4), (2, 2)])
@pytest.mark.parametrize(
    "persistence_mode", [INPUT_PERSISTENCE_MODE_NAME, OPERATOR_PERSISTENCE_MODE_NAME]
)
@pytest.mark.parametrize(
    "n_backfilling_runs,min_work_time,max_work_time",
    [
        (3, 0.75, 2.25),
        (10, 5.0, 10.0),
        (3, 3.0, 6.0),
        (3, 6.0, 9.0),
        (10, 8.0, 16.0),
        (10, 5.0, 15.0),
        (10, 9.0, 19.0),
    ],
)
def test_integration_failure_recovery_fs(
    n_backfilling_runs,
    n_threads,
    n_processes,
    min_work_time,
    max_work_time,
    persistence_mode,
    tmp_path: pathlib.Path,
    port: int,
):
    do_test_failure_recovery(
        n_backfilling_runs=n_backfilling_runs,
        n_threads=n_threads,
        n_processes=n_processes,
        tmp_path=tmp_path,
        min_work_time=min_work_time,
        max_work_time=max_work_time,
        pstorage_type=FS_STORAGE_NAME,
        persistence_mode=persistence_mode,
        first_port=port,
    )


@pytest.mark.parametrize("n_threads,n_processes", [(1, 1), (4, 1), (1, 4), (2, 2)])
@pytest.mark.parametrize(
    "persistence_mode", [INPUT_PERSISTENCE_MODE_NAME, OPERATOR_PERSISTENCE_MODE_NAME]
)
@pytest.mark.parametrize(
    "n_backfilling_runs,min_work_time,max_work_time",
    [
        (3, 6.0, 9.0),
        (10, 8.0, 16.0),
        (10, 5.0, 15.0),
        (10, 9.0, 19.0),
    ],
)
def test_integration_failure_recovery_s3(
    n_backfilling_runs,
    n_threads,
    n_processes,
    min_work_time,
    max_work_time,
    persistence_mode,
    tmp_path: pathlib.Path,
    port: int,
):
    do_test_failure_recovery(
        n_backfilling_runs=n_backfilling_runs,
        n_threads=n_threads,
        n_processes=n_processes,
        tmp_path=tmp_path,
        min_work_time=min_work_time,
        max_work_time=max_work_time,
        pstorage_type=S3_STORAGE_NAME,
        persistence_mode=persistence_mode,
        first_port=port,
    )


@pytest.mark.parametrize("n_threads,n_processes", [(1, 4), (2, 2)])
@pytest.mark.parametrize(
    "persistence_mode", [INPUT_PERSISTENCE_MODE_NAME, OPERATOR_PERSISTENCE_MODE_NAME]
)
@pytest.mark.parametrize(
    "n_backfilling_runs,min_work_time,max_work_time",
    [
        (10, 8.0, 16.0),
        (10, 5.0, 15.0),
        (10, 9.0, 19.0),
    ],
)
def test_integration_failure_recovery_azure(
    n_backfilling_runs,
    n_threads,
    n_processes,
    min_work_time,
    max_work_time,
    persistence_mode,
    tmp_path: pathlib.Path,
    port: int,
):
    do_test_failure_recovery(
        n_backfilling_runs=n_backfilling_runs,
        n_threads=n_threads,
        n_processes=n_processes,
        tmp_path=tmp_path,
        min_work_time=min_work_time,
        max_work_time=max_work_time,
        pstorage_type=AZURE_STORAGE_NAME,
        persistence_mode=persistence_mode,
        first_port=port,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple persistence test")
    parser.add_argument("--n-backfilling-runs", type=int, default=3)
    parser.add_argument("--n-threads", type=int, default=1, choices=[1, 2, 4])
    parser.add_argument("--n-processes", type=int, default=1, choices=[1, 2, 4])
    parser.add_argument("--min-work-time", type=float, default=5.0)
    parser.add_argument("--max-work-time", type=float, default=15.0)
    parser.add_argument(
        "--pstorage-type",
        type=str,
        choices=["s3", "fs"],
        default="fs",
    )
    parser.add_argument(
        "--persistence_mode",
        type=str,
        choices=[INPUT_PERSISTENCE_MODE_NAME, OPERATOR_PERSISTENCE_MODE_NAME],
        default=INPUT_PERSISTENCE_MODE_NAME,
    )
    args = parser.parse_args()

    do_test_failure_recovery(
        n_backfilling_runs=args.n_backfilling_runs,
        n_threads=args.n_threads,
        n_processes=args.n_processes,
        tmp_path=pathlib.Path("./"),
        min_work_time=args.min_work_time,
        max_work_time=args.max_work_time,
        pstorage_type=args.pstorage_type,
        persistence_mode=args.persistence_mode,
        first_port=5670,
    )

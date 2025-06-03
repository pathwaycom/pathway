#!/usr/bin/env python

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
    STATIC_MODE_NAME,
    STREAMING_MODE_NAME,
    do_test_persistent_wordcount,
)


@pytest.mark.parametrize("n_threads,n_processes", [(1, 1), (4, 1), (1, 4), (2, 2)])
@pytest.mark.parametrize(
    "pstorage_type", [S3_STORAGE_NAME, FS_STORAGE_NAME, AZURE_STORAGE_NAME]
)
@pytest.mark.parametrize(
    "persistence_mode", [INPUT_PERSISTENCE_MODE_NAME, OPERATOR_PERSISTENCE_MODE_NAME]
)
@pytest.mark.parametrize(
    "n_backfilling_runs,mode",
    [
        (3, STREAMING_MODE_NAME),
        (3, STATIC_MODE_NAME),
    ],
)
def test_integration_new_data(
    n_backfilling_runs,
    n_threads,
    n_processes,
    mode,
    pstorage_type,
    persistence_mode,
    tmp_path: pathlib.Path,
    port: int,
):
    do_test_persistent_wordcount(
        n_backfilling_runs=n_backfilling_runs,
        n_threads=n_threads,
        n_processes=n_processes,
        tmp_path=tmp_path,
        mode=mode,
        pstorage_type=pstorage_type,
        persistence_mode=persistence_mode,
        first_port=port,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple persistence test")
    parser.add_argument("--n-backfilling-runs", type=int, default=0)
    parser.add_argument("--n-threads", type=int, default=1, choices=[1, 2, 4])
    parser.add_argument("--n-processes", type=int, default=1, choices=[1, 2, 4])
    parser.add_argument(
        "--mode",
        type=str,
        choices=[STATIC_MODE_NAME, STREAMING_MODE_NAME],
        default=STREAMING_MODE_NAME,
    )
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

    do_test_persistent_wordcount(
        n_backfilling_runs=args.n_backfilling_runs,
        n_threads=args.n_threads,
        n_processes=args.n_processes,
        tmp_path=pathlib.Path("./"),
        mode=args.mode,
        pstorage_type=args.pstorage_type,
        persistence_mode=args.persistence_mode,
        first_port=56700,
    )

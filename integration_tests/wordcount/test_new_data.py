#!/usr/bin/env python

import argparse
import pathlib

import pytest

from .base import STATIC_MODE_NAME, STREAMING_MODE_NAME, do_test_persistent_wordcount


@pytest.mark.parametrize(
    "n_backfilling_runs,n_cpus,mode",
    [
        (3, 1, STREAMING_MODE_NAME),
        (3, 2, STREAMING_MODE_NAME),
        (3, 4, STREAMING_MODE_NAME),
        (3, 1, STATIC_MODE_NAME),
        (3, 2, STATIC_MODE_NAME),
        (3, 4, STATIC_MODE_NAME),
    ],
)
def test_integration_new_data(n_backfilling_runs, n_cpus, mode, tmp_path: pathlib.Path):
    do_test_persistent_wordcount(n_backfilling_runs, n_cpus, tmp_path, mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple persistence test")
    parser.add_argument("--n-backfilling-runs", type=int, default=0)
    parser.add_argument("--n-cpus", type=int, default=1, choices=[1, 2, 4])
    parser.add_argument(
        "--mode",
        type=str,
        choices=[STATIC_MODE_NAME, STREAMING_MODE_NAME],
        default=STREAMING_MODE_NAME,
    )

    args = parser.parse_args()

    do_test_persistent_wordcount(
        args.n_backfilling_runs, args.n_cpus, pathlib.Path("./"), args.mode
    )

# Copyright © 2024 Pathway

import os
import pathlib

import pytest
from click.testing import CliRunner

from pathway import cli


def run_record(
    path, timestamp_file: pathlib.Path, expected_count, rows_to_generate
) -> int:
    # timestamp_file is used to get back information about number of distinct
    # timestamps - as we are not guaranteed that all timestamps are different,
    # during replay we compare with the number obtained during data generation.
    script_path = os.path.join(os.path.dirname(__file__), "replay.py")
    runner = CliRunner()
    result = runner.invoke(
        cli.spawn,
        [
            "--record",
            "--record-path",
            path,
            "python",
            script_path,
            str(expected_count),
            str(rows_to_generate),
            str(timestamp_file),
        ],
    )
    assert result.exit_code == 0

    n_timestamps = int(timestamp_file.read_text())

    return n_timestamps


def run_replay(
    path, mode, expected_count, rows_to_generate=0, continue_after_replay=True
):
    script_path = os.path.join(os.path.dirname(__file__), "replay.py")
    runner = CliRunner()
    result = runner.invoke(
        cli.replay,
        [
            "--record-path",
            path,
            "--mode",
            mode,
            *("--continue" for _ in range(1) if continue_after_replay),
            "python",
            script_path,
            str(expected_count),
            str(rows_to_generate),
        ],
    )
    assert result.exit_code == 0


@pytest.mark.flaky(reruns=2)
def test_record_replay_through_cli(tmp_path: pathlib.Path):
    replay_dir = str(tmp_path / "test_replay")
    timestamp_file = tmp_path / "timestamp"

    # First run to persist data in local storage
    n_timestamps = run_record(replay_dir, timestamp_file, 15, 15)

    run_replay(replay_dir, "batch", 1)

    run_replay(replay_dir, "speedrun", n_timestamps)

    # Generate rows during replay
    # expected number will be ignored, as we are generating rows
    run_replay(replay_dir, "speedrun", 30, rows_to_generate=15)

    # Check that the rows weren't recorded
    run_replay(replay_dir, "speedrun", 15)

    # Without replay (and with empty input connector), there are no rows
    run_record(replay_dir, timestamp_file, 0, 0)

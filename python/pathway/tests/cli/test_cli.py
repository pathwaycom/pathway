import os
import pathlib

import pytest
from click.testing import CliRunner

from pathway import cli


def run_record(path, expected_count, rows_to_generate):
    script_path = os.path.join(os.path.dirname(__file__), "replay.py")
    runner = CliRunner()
    result = runner.invoke(
        cli.spawn,
        [
            "--record",
            "--record_path",
            path,
            "python",
            script_path,
            str(expected_count),
            str(rows_to_generate),
        ],
    )
    assert result.exit_code == 0


def run_replay(
    path, mode, expected_count, rows_to_generate=0, continue_after_replay=True
):
    script_path = os.path.join(os.path.dirname(__file__), "replay.py")
    runner = CliRunner()
    result = runner.invoke(
        cli.replay,
        [
            "--record_path",
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


@pytest.mark.xfail(reason="failing non-deterministically")
def test_record_replay_through_cli(tmp_path: pathlib.Path):
    replay_dir = str(tmp_path / "test_replay")

    # First run to persist data in local storage
    run_record(replay_dir, 15, 15)

    run_replay(replay_dir, "batch", 1)

    run_replay(replay_dir, "speedrun", 15)

    # When we don't continue after replay, we should not generate new rows
    run_replay(replay_dir, "speedrun", 15, continue_after_replay=False)

    # Generate rows during replay
    run_replay(replay_dir, "speedrun", 30, rows_to_generate=15)

    # Check that the rows weren't recorded
    run_replay(replay_dir, "speedrun", 15)

    # Without replay (and with empty input connector), there are no rows
    run_record(replay_dir, 0, 0)

    # Generate rows and record them (but don't replay saved data)
    run_record(replay_dir, 15, 15)

    # Check that the rows were recorded
    run_replay(replay_dir, "speedrun", 30)

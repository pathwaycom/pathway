import os

import git
import pytest
from click.testing import CliRunner
from deltalake import DeltaTable

from pathway import cli

REPOSITORY_URL = "https://github.com/pathway-labs/airbyte-to-deltalake"
TRACKED_REPOSITORY_URL = "https://github.com/pathwaycom/pathway/"
IDENTITY_PROGRAM = os.path.join(os.path.dirname(__file__), "identity.py")


def count_commits_in_pathway_repository(tmp_path):
    repository = git.Repo.clone_from(TRACKED_REPOSITORY_URL, tmp_path / "repo")
    return sum([1 for _ in repository.iter_commits()])


# Retries are needed for an unlikely case when the number of commits in the repository
# changes between the remote run and the `count_commits_in_pathway_repository` method
# execution.
@pytest.mark.flaky(reruns=2)
def test_repository_url_feature(tmp_path):
    output_deltalake_path = tmp_path / "commit-lake"
    os.environ["LOCAL_OUTPUT_PATH"] = os.fspath(output_deltalake_path)

    runner = CliRunner()
    result = runner.invoke(
        cli.spawn,
        [
            "--repository-url",
            REPOSITORY_URL,
            "python",
            "main.py",
        ],
    )
    assert result.exit_code == 0

    delta_table = DeltaTable(output_deltalake_path)
    pd_table_from_delta = delta_table.to_pandas()
    actual_n_commits = pd_table_from_delta.shape[0]
    expected_n_commits = count_commits_in_pathway_repository(tmp_path)

    assert actual_n_commits == expected_n_commits


def invoke_spawn(runner, args):
    return runner.invoke(
        cli.spawn, args + ["python", IDENTITY_PROGRAM, "input.txt", "output.jsonl"]
    )


def test_processes_and_addresses_are_mutually_exclusive(runner):
    result = invoke_spawn(
        runner, ["--processes", "2", "--addresses", "host0:10000,host1:10000"]
    )
    assert result.exit_code != 0
    assert "--processes and --addresses are mutually exclusive" in result.output


def test_process_id_requires_addresses(runner):
    result = invoke_spawn(runner, ["--process-id", "1"])
    assert result.exit_code != 0
    assert "--process-id requires --addresses" in result.output


def test_addresses_requires_process_id(runner):
    result = invoke_spawn(runner, ["--addresses", "host0:10000,host1:10000"])
    assert result.exit_code != 0
    assert "--process-id is required when --addresses is set" in result.output


def test_address_invalid_format_no_colon(runner):
    result = invoke_spawn(runner, ["--addresses", "host010000", "--process-id", "0"])
    assert result.exit_code != 0
    assert "expected host:port format" in result.output


def test_address_invalid_format_non_numeric_port(runner):
    result = invoke_spawn(runner, ["--addresses", "host0:abc", "--process-id", "0"])
    assert result.exit_code != 0
    assert "expected host:port format" in result.output


def test_address_port_zero(runner):
    result = invoke_spawn(runner, ["--addresses", "host0:0", "--process-id", "0"])
    assert result.exit_code != 0
    assert "must be in range" in result.output


def test_address_port_too_large(runner):
    result = invoke_spawn(runner, ["--addresses", "host0:99999", "--process-id", "0"])
    assert result.exit_code != 0
    assert "must be in range" in result.output


def test_addresses_duplicate_entries(runner):
    result = invoke_spawn(
        runner,
        ["--addresses", "host0:10000,host0:10000", "--process-id", "0"],
    )
    assert result.exit_code != 0
    assert "duplicate entries" in result.output


def test_process_id_out_of_range(runner):
    result = invoke_spawn(
        runner,
        ["--addresses", "host0:10000,host1:10000", "--process-id", "5"],
    )
    assert result.exit_code != 0
    assert "--process-id 5 is out of range" in result.output


def test_process_id_negative(runner):
    result = invoke_spawn(
        runner,
        ["--addresses", "host0:10000,host1:10000", "--process-id", "-1"],
    )
    assert result.exit_code != 0
    assert "--process-id -1 is out of range" in result.output


def test_threads_zero(runner):
    result = invoke_spawn(runner, ["--threads", "0"])
    assert result.exit_code != 0
    assert "--threads must be at least 1" in result.output


def test_threads_negative(runner):
    result = invoke_spawn(runner, ["--threads", "-4"])
    assert result.exit_code != 0
    assert "--threads must be at least 1" in result.output


def test_processes_zero(runner):
    result = invoke_spawn(runner, ["--processes", "0"])
    assert result.exit_code != 0
    assert "--processes must be at least 1" in result.output


def test_first_port_overflow(runner):
    result = invoke_spawn(runner, ["--processes", "3", "--first-port", "65534"])
    assert result.exit_code != 0
    assert "exceeds the maximum" in result.output

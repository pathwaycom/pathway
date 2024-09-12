import os

import git
import pytest
from click.testing import CliRunner
from deltalake import DeltaTable

from pathway import cli

REPOSITORY_URL = "https://github.com/pathway-labs/airbyte-to-deltalake"
TRACKED_REPOSITORY_URL = "https://github.com/pathwaycom/pathway/"


def count_commits_in_pathway_repository(tmp_path):
    repository = git.Repo.clone_from(TRACKED_REPOSITORY_URL, tmp_path / "repo")
    return sum([1 for _ in repository.iter_commits()])


# Retries are needed for an unlikely case when the number of commits in the repository
# changes between the remote run and the `count_commits_in_pathway_repository` method
# execution.
@pytest.mark.flaky(reruns=5)
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

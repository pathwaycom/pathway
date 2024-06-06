import os

import pytest

import pathway as pw
from pathway.tests.utils import run_all

CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "credentials.json")
TEST_CONNECTION_PATH = os.path.join(os.path.dirname(__file__), "test-connection.yaml")


@pytest.mark.parametrize("gcp_job_name", [None, "pw-integration-custom-gcp-job-name"])
@pytest.mark.parametrize("env_vars", [None, {"''": "\"''''\"\""}, {"KEY": "VALUE"}])
def test_airbyte_remote_run(gcp_job_name, env_vars, tmp_path):
    table = pw.io.airbyte.read(
        TEST_CONNECTION_PATH,
        ["Users"],
        service_user_credentials_file=CREDENTIALS_PATH,
        mode="static",
        execution_type="remote",
        env_vars=env_vars,
        gcp_job_name=gcp_job_name,
    )
    output_path = tmp_path / "table.jsonl"
    pw.io.jsonlines.write(table, output_path)
    run_all()
    total_lines = 0
    with open(output_path, "r") as f:
        for _ in f:
            total_lines += 1
    assert total_lines == 500

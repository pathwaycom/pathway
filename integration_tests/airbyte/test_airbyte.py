import os
import threading

import pytest
import yaml

import pathway as pw
from pathway.tests.utils import (
    FileLinesNumberChecker,
    run_all,
    wait_result_with_checker,
    write_lines,
)

CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "credentials.json")
TEST_FAKER_CONNECTION_PATH = os.path.join(
    os.path.dirname(__file__), "test-faker-connection.yaml"
)
TEST_FILE_CONNECTION_PATH = os.path.join(
    os.path.dirname(__file__), "test-file-connection.yaml"
)


@pytest.mark.parametrize("gcp_job_name", [None, "pw-integration-custom-gcp-job-name"])
@pytest.mark.parametrize("env_vars", [None, {"''": "\"''''\"\""}, {"KEY": "VALUE"}])
def test_airbyte_remote_run(gcp_job_name, env_vars, tmp_path):
    table = pw.io.airbyte.read(
        TEST_FAKER_CONNECTION_PATH,
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


# https://github.com/airbytehq/airbyte-protocol/issues/124
@pytest.mark.xfail(reason="depends of airbyte-protocol which is broken at the moment")
def test_airbyte_full_refresh_streams(tmp_path):
    input_path = tmp_path / "input.csv"
    connection_path = tmp_path / "connection.yaml"
    with open(TEST_FILE_CONNECTION_PATH, "r") as f:
        config = yaml.safe_load(f)
    config["source"]["config"]["url"] = os.fspath(input_path)
    with open(connection_path, "w") as f:
        yaml.dump(config, f)

    write_lines(input_path, "header\nfoo\nbar\nbaz")
    output_path = tmp_path / "table.jsonl"

    table = pw.io.airbyte.read(
        connection_path,
        streams=["dataset"],
        mode="streaming",
        execution_type="local",
        enforce_method="venv",
        refresh_interval_ms=100,
    )
    pw.io.jsonlines.write(table, output_path)

    def stream_target():
        wait_result_with_checker(FileLinesNumberChecker(output_path, 3), 5, target=None)
        write_lines(input_path, "header\nbaz")

        wait_result_with_checker(FileLinesNumberChecker(output_path, 5), 5, target=None)
        write_lines(input_path, "header\nfoo")

    inputs_thread = threading.Thread(target=stream_target, daemon=True)
    inputs_thread.start()

    wait_result_with_checker(FileLinesNumberChecker(output_path, 7), 15)
    inputs_thread.join()

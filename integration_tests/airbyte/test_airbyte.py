import os
import threading
import uuid

import pytest
import yaml

import pathway as pw
from pathway.tests.utils import (
    FileLinesNumberChecker,
    run_all,
    wait_result_with_checker,
    write_lines,
)

TEST_FAKER_CONNECTION_PATH = os.path.join(
    os.path.dirname(__file__), "test-faker-connection.yaml"
)
TEST_FILE_CONNECTION_PATH = os.path.join(
    os.path.dirname(__file__), "test-file-connection.yaml"
)


@pytest.mark.parametrize("gcp_job_name", [None, "pw-custom"])
@pytest.mark.parametrize("env_vars", [None, {"''": "\"''''\"\""}, {"KEY": "VALUE"}])
def test_airbyte_remote_run(gcp_job_name, env_vars, tmp_path, credentials_dir):
    if gcp_job_name is not None:
        gcp_job_name = f"{gcp_job_name}-{uuid.uuid4().hex}"
    table = pw.io.airbyte.read(
        TEST_FAKER_CONNECTION_PATH,
        ["Users"],
        service_user_credentials_file=str(credentials_dir / "credentials.json"),
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

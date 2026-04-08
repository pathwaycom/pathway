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
from pathway.third_party.airbyte_serverless.executable_runner import (
    AirbyteSourceException,
)

# Minimal GitHub connector config with a fake PAT. Used by two tests: one that pins
# a broken airbyte-cdk version (7.13.0) where the connector crashes at import time
# before making any API call, and one that pins a working version (7.10.0) where the
# connector loads correctly and reaches GitHub's API, failing with 401 Unauthorized.
_GITHUB_FAKE_CONFIG = {
    "source": {
        "docker_image": "airbyte/source-github:latest",
        "config": {
            "credentials": {
                "personal_access_token": "fake_token_for_import_error_reproduction",
            },
            "repositories": ["pathwaycom/pathway"],
            "start_date": "2020-01-01T00:00:00Z",
        },
    }
}

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


@pytest.mark.xfail(reason="https://github.com/airbytehq/airbyte-protocol/issues/136")
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


def test_github_connector_fails_with_broken_cdk(tmp_path):
    """
    Reproduces https://github.com/pathwaycom/pathway/issues/201.

    airbyte-cdk==7.13.0 removed MessageRepresentationAirbyteTracedErrors. The connector
    process crashes at import time — before emitting any JSON — which surfaces as
    AssertionError inside Pathway's connector infrastructure.

    See also: https://github.com/airbytehq/airbyte-python-cdk/issues/946
    """
    config_path = tmp_path / "github.yaml"
    with open(config_path, "w") as f:
        yaml.dump(_GITHUB_FAKE_CONFIG, f)

    with pytest.raises(
        AssertionError,
        match="No message returned by AirbyteSource with action `discover`",
    ):
        pw.io.airbyte.read(
            config_path,
            streams=["commits"],
            mode="static",
            enforce_method="pypi",
            dependency_overrides=[
                "airbyte-source-github==2.1.13",
                "airbyte-cdk==7.13.0",
            ],
        )


def test_github_connector_unauthorized_with_working_cdk(tmp_path):
    """
    Verifies that pinning airbyte-cdk to a version that still has
    MessageRepresentationAirbyteTracedErrors (7.10.0) allows the connector to load
    and reach GitHub's API, where it fails with a 401 Unauthorized error instead of
    crashing at import time.

    See https://github.com/pathwaycom/pathway/issues/201
    """
    config_path = tmp_path / "github.yaml"
    with open(config_path, "w") as f:
        yaml.dump(_GITHUB_FAKE_CONFIG, f)

    with pytest.raises(AirbyteSourceException, match="HTTP Status Code: 401"):
        pw.io.airbyte.read(
            config_path,
            streams=["commits"],
            mode="static",
            enforce_method="pypi",
            dependency_overrides=["airbyte-cdk==7.10.0"],
        )

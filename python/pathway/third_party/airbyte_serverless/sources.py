"""
There are two global options for how the extraction may work: the local one, which is
done by `DockerAirbyteSource` or `VenvAirbyteSource` and the remote one was done by
`RemoteAirbyteSource`.

When executing locally, there are two options: the Docker option and the
venv option. The point of the venv option is the following: most of the airbyte connectors
were implemented in Python. Each of them is a separate library with a well-defined name:
`airbyte-source-<kind of source>`, for example: `airbyte-source-github` or
`airbyte-source-faker`. Of source, such packages can be used for the local execution:
all you need is to perform the `pip install` and then you can communicate with the
installed console tool following the well-defined protocol:
https://docs.airbyte.com/understanding-airbyte/airbyte-protocol#common-interface. To make
the installed packages not to interfere with the developer's environment, virtualenv
is used, hence the name `VenvAirbyteSource`.

The `DockerAirbyteSource` works as follows: we run a subprocess that calls Docker and
provides it with arguments such as the user-provided config, the connector catalog
(you may think of it as a static config containing the descriptions of the available
data streams), the previous state of the connector, environment variables, etc. Then,
the result of the execution is read from stderr and stdout and translated into the
extracted data. This option is heavier: there is a much bigger overhead for the Docker
container to start, rather than for a console tool. However, sometimes it may be needed:
- Not all connectors are implemented in Python, but all of them can be run via Docker.
One example of a connector that wasn't implemented in Python is Postgres.
- The venv option doesn't provide a way to select the version of the connector. If, for some
reason, you need to use the specific option of a Docker image, you can use this option.
Note that there is no mapping between Python package version and the Docker version in the
registry.

The `RemoteAirbyteSource` is a bit different: it uses the fact that any airbyte connector
is a Docker image having an environment variable AIRBYTE_ENTRYPOINT corresponding to the
connector launch. It then runs a Google Cloud Job that starts inside the corresponding
container, but inside the container itself, it delivers and runs
`executable_runner.py` which sets up the config, catalog, and state and then asks
the connector (that can be reached inside by calling the executable at AIRBYTE_ENTRYPOINT)
for the new data entries.

The remote part also has a few optimizations to save money for the user: runs are billed
per vCPU- and memory GiB-seconds, we need the execution to be as fast as possible.

There are a few tricks used to optimize the execution in both cases:
1. The `executable_runner.py` code is delivered into the container as an environment variable.
Therefore, there is no need to download anything online to get it.
2. The list of dependencies is kept minimal. The code needs a pyyaml library for all cases,
however, the google-cloud-secret-manager is optional and thus should only be used
when a config contains evidence that some of the secrets will be replaced.
3. The catalog doesn't change throughout execution, so it gets cached once acquired.
It can be tricky in case of cloud runs where the length of env vars is limited to 32 Kb,
so, in that case, we do our best effort: whenever ZLib compression + base64 is sufficient
to fit the catalog in the limit, caching is applied.
"""

import base64
import os
import re
import json
import shutil
import logging
import time
import shlex
import subprocess
import tempfile
import pathlib
import zlib
from typing import Iterable

import venv
import yaml

from . import airbyte_utils
from .executable_runner import (
    AirbyteSourceException,
    AbstractAirbyteSource,
    ExecutableAirbyteSource,
    ConnectorResultProcessor,
    MAX_GCP_ENV_VAR_LENGTH,
    get_configured_catalog,
)
from google.oauth2.service_account import Credentials as ServiceCredentials
from google.protobuf.duration_pb2 import Duration


# https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.EnvVar
EXECUTABLE_RUNNER_NAME = "executable_runner.py"
MAX_PIP_INSTALL_ATTEMPTS = 3
PIP_INSTALL_RETRY_DELAY = 10.0


class DockerAirbyteSource(ExecutableAirbyteSource):

    def __init__(
        self,
        connector: str,
        config: dict | None = None,
        streams: Iterable[str] | None = None,
        env_vars: dict | None = None,
    ):
        assert shutil.which("docker") is not None, "docker is needed. Please install it"
        self.docker_image = connector
        super().__init__("", config, streams)
        self.temp_dir_for_executable = "/mnt/temp"
        env_vars = env_vars or {}
        prepared_env_vars = " ".join(
            [
                f"-e {shlex.quote(key)}={shlex.quote(value)}"
                for key, value in env_vars.items()
            ]
        )
        self.executable = (
            f"docker run --rm -i --volume {self.temp_dir}:{self.temp_dir_for_executable} "
            f"{prepared_env_vars} {self.docker_image}"
        )

    @property
    def yaml_definition_example(self):
        yaml_definition_example = "\n".join(
            [
                f'executable: "{self.executable}" # GENERATED | string | Command to launch the Airbyte Source',
                "config: TO_REPLACE",
                "streams: # OPTIONAL | string | Comma-separated list of streams to retrieve. "
                "If missing, all streams are retrieved from source.",
            ]
        )
        spec = self.spec
        config_yaml = airbyte_utils.generate_connection_yaml_config_sample(spec)
        yaml_definition_example = yaml_definition_example.replace(
            "TO_REPLACE", config_yaml.replace("\n", "\n  ").strip()
        )
        return re.sub(
            "executable:.*",
            f'docker_image: "{self.docker_image}" # GENERATED | string | A Public Docker Airbyte Source. '
            "Example: `airbyte/source-faker:0.1.4`. (see connectors list at: "
            '"https://hub.docker.com/search?q=airbyte%2Fsource-" )',
            yaml_definition_example,
        )


class VenvAirbyteSource(ExecutableAirbyteSource):

    def __init__(
        self,
        connector: str,
        config: dict | None = None,
        streams: Iterable[str] | None = None,
        env_vars: dict | None = None,
    ):
        super().__init__("", config, streams, env_vars)
        self._venv_directory = tempfile.TemporaryDirectory()
        self._venv_path = pathlib.Path(self._venv_directory.name)
        logging.info(f"Creating an isolated virtual environment for {connector}...")
        venv.create(self._venv_path, with_pip=True)

        logging.info(f"Installing python connector package for {connector}...")
        pip_path = os.fspath(self._venv_path / "bin" / "pip")
        for n_attempt in range(MAX_PIP_INSTALL_ATTEMPTS):
            process = subprocess.run(
                f"{pip_path} install airbyte-{connector}",
                stderr=subprocess.STDOUT,
                shell=True,
            )
            if process.returncode != 0:
                result_stdout = process.stdout.decode("utf-8") if process.stdout else ""
                result_stderr = process.stderr.decode("utf-8") if process.stderr else ""
                logging.error(
                    f"Failed to install airbyte-{connector}:\n"
                    f"stdout:\n{result_stdout}\n"
                    f"stderr:\n{result_stderr}"
                )
                if n_attempt < MAX_PIP_INSTALL_ATTEMPTS - 1:
                    logging.info(
                        f"Retrying pip install in {PIP_INSTALL_RETRY_DELAY} seconds..."
                    )
                    time.sleep(PIP_INSTALL_RETRY_DELAY)
                else:
                    raise RuntimeError(
                        f"Failed to install package airbyte-{connector} into a virtual "
                        "environment. If the problem persists, please check that the "
                        "package exists on PyPI and consider using enforce_method "
                        "setting."
                    )
            logging.info(
                f"The connector package for {connector} had successfully been installed"
            )
        self.executable = self._venv_path / "bin" / connector


class RemoteAirbyteSource(AbstractAirbyteSource):

    def __init__(
        self,
        config: dict,
        streams: Iterable[str],
        job_id: str,
        credentials: ServiceCredentials,
        region: str,
        env_vars: dict | None = None,
    ):
        import google.cloud.run_v2

        self.config = config
        self.streams = streams
        if len(self.yaml_config_b64) > MAX_GCP_ENV_VAR_LENGTH:
            # Not sure, but perhaps we should deliver GZip-ed config to enhance the limit
            raise ValueError(
                f"Config size limit exceeded. "
                f"Consider redicing it to fit the size of {MAX_GCP_ENV_VAR_LENGTH / 4 * 3} bytes."
            )
        self.job_id = job_id
        self.credentials = credentials
        self.region = region
        self.env_vars = env_vars or {}
        self._cached_catalog = None

        self.cloud_run = google.cloud.run_v2.JobsClient(credentials=self.credentials)
        self.create_gcp_job()

    @property
    def configured_catalog(self):
        if self._cached_catalog is None:
            self.extract()
        if self._cached_catalog is not None:
            catalog = json.loads(
                zlib.decompress(base64.b64decode(self._cached_catalog))
            )
            return get_configured_catalog(catalog, self.streams)
        else:
            raise RuntimeError("No catalog after performing extraction iteration")

    def maybe_delete_google_cloud_job(self):
        import google.api_core.exceptions

        try:
            self.cloud_run.delete_job(name=self.job_name).result()
        except google.api_core.exceptions.NotFound:
            pass

    def on_stop(self):
        self.maybe_delete_google_cloud_job()

    def create_gcp_job(self):
        docker_image = self.config["source"]["docker_image"]
        project = self.credentials.project_id
        region = self.region
        env_vars = self.env_vars
        env = []
        if env_vars:
            assert isinstance(
                env_vars, dict
            ), "Given env_vars argument should be a dict"
            env = [{"name": k, "value": v} for k, v in env_vars.items()]

        # Here we deliver the remote runner into the remotely running Docker container
        # We prefer this way over wget/curl for the following reasons:
        # - There is no need to update the remote runner everywhere else, the version is always actual
        # - Not all connector images contain apt-get to install wget or curl
        # - Installing wget/curl and downloading a file on each iteration takes time
        #
        # OTOH can safely afford this approach because the maximum length of an env
        # variable delivered into container is 32 KB, while the runner code is much
        # smaller
        remote_runner_path = os.path.join(
            os.path.dirname(__file__), EXECUTABLE_RUNNER_NAME
        )
        with open(remote_runner_path, "rb") as f:
            remote_runner_encoded = base64.b64encode(f.read()).decode("utf-8")
            env.append(
                {
                    "name": "RUNNER_CODE",
                    "value": remote_runner_encoded,
                }
            )

        location = f"projects/{project}/locations/{region}"
        self.job_name = f"{location}/jobs/{self.job_id}"

        pip_dependencies = ["pyyaml"]
        if "GCP_SECRET" in self.yaml_config:
            pip_dependencies.append("google-cloud-secret-manager")

        container = {
            "image": docker_image,
            "command": ["/bin/sh"],
            "args": [
                "-c",
                " && ".join(
                    [
                        "echo $RUNNER_CODE > runner.txt",
                        "base64 -d < runner.txt > runner.py",
                        f"pip install {' '.join(pip_dependencies)}",
                        "python runner.py",
                    ]
                ),
            ],
            "env": [{"name": "YAML_CONFIG", "value": self.yaml_config_b64}] + env,
            "resources": {
                "limits": {
                    "memory": "512Mi",
                    "cpu": "1",
                }
            },
        }
        timeout = Duration()
        timeout.seconds = 3600
        job_config = {
            "containers": [container],
            "timeout": timeout,
            "max_retries": 0,
        }

        self.maybe_delete_google_cloud_job()
        self.cloud_run.create_job(
            job={"template": {"template": job_config}},  # type: ignore
            job_id=self.job_id,
            parent=location,
        ).result()

    @property
    def yaml_config(self):
        return yaml.dump(self.config, allow_unicode=True)

    @property
    def yaml_config_b64(self):
        return base64.b64encode(self.yaml_config.encode("utf-8")).decode("utf-8")

    @property
    def project(self):
        return self.credentials.project_id

    def extract(self, state=None):
        from google.cloud import logging as gcp_logging

        prepared_state = json.dumps(state)
        if len(prepared_state) > MAX_GCP_ENV_VAR_LENGTH:
            raise ValueError(
                "The state is too large. Please consider using smaller number of streams."
            )

        env_overrides = []
        if state is not None:
            env_overrides.append(
                {
                    "name": "AIRBYTE_STATE",
                    "value": prepared_state,
                }
            )
        if self._cached_catalog is not None:
            env_overrides.append(
                {
                    "name": "CACHED_CATALOG",
                    "value": self._cached_catalog,
                }
            )

        operation = self.cloud_run.run_job(
            {
                "name": self.job_name,
                "overrides": {
                    "container_overrides": [
                        {
                            "name": self.config["source"]["docker_image"],
                            "env": env_overrides,
                        }
                    ]
                },
            }
        )
        execution_id = operation.metadata.name.split("/")[-1]
        execution_url = f"https://console.cloud.google.com/run/jobs/executions/details/{self.region}/{execution_id}/logs?project={self.project}"  # noqa
        logging.info(f"Launched airbyte extraction job. Details at {execution_url}")

        # Wait for execution finish
        operation_result = operation.result()
        if operation_result.succeeded_count != 1:
            raise AirbyteSourceException(
                f"GCP operation failed. Please visit {execution_url} for details."
            )

        logging.info("Execution finished, fetching results...")
        messages = None
        while messages is None:
            logging.info("Waiting for logs to be delivered in full...")
            log_client = gcp_logging.Client(
                project=self.project,
                credentials=self.credentials,
            )
            logs_processor = ConnectorResultProcessor()

            for log_entry in log_client.list_entries(
                filter_=f'labels."run.googleapis.com/execution_name" = {execution_id}',
                page_size=1000,
            ):
                logs_processor.append_chunk(log_entry.payload)

            messages = logs_processor.get_messages()
            self._cached_catalog = logs_processor.get_catalog()
            if messages is None:
                time.sleep(3.0)

        return messages

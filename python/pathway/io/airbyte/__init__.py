import copy
import logging
import os
import random
import time
import uuid
from collections.abc import Sequence
from typing import Any

import requests
import yaml

from pathway.internals.schema import Schema
from pathway.io._utils import _get_unique_name
from pathway.io.python import read as python_connector_read
from pathway.optional_import import optional_imports
from pathway.third_party.airbyte_serverless.executable_runner import (
    AbstractAirbyteSource,
)

METHOD_PYPI = "pypi"
METHOD_DOCKER = "docker"

N_PYPI_REQUESTS = 5
RETRY_BACKOFF = 1.2
JITTER = 0.2


def _pip_package_exists(name: str) -> bool:
    url = f"https://pypi.org/pypi/{name}/json"
    sleep_duraion = 1.0
    for _ in range(N_PYPI_REQUESTS):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            logging.exception("PyPI request failed")
        time.sleep(sleep_duraion)
        sleep_duraion = sleep_duraion * RETRY_BACKOFF + random.random() * JITTER

    return False


def _construct_local_source(
    source_config: dict[str, Any],
    streams: Sequence[str],
    env_vars: dict[str, str] | None = None,
    enforce_method: str | None = None,
) -> AbstractAirbyteSource:
    with optional_imports("airbyte"):
        from pathway.io.airbyte.logic import (
            FULL_REFRESH_SYNC_MODE,
            INCREMENTAL_SYNC_MODE,
        )
        from pathway.third_party.airbyte_serverless.sources import (
            DockerAirbyteSource,
            VenvAirbyteSource,
        )

    connector_name = source_config["docker_image"].removeprefix("airbyte/")
    connector_name, _, _ = connector_name.partition(":")
    source: AbstractAirbyteSource
    if enforce_method != METHOD_DOCKER and (
        _pip_package_exists(f"airbyte-{connector_name}")
        or enforce_method == METHOD_PYPI
    ):
        logging.info(
            f"The connector {connector_name} was implemented in Python, "
            "running it in the isolated virtual environment"
        )
        source = VenvAirbyteSource(
            connector=connector_name,
            config=source_config.get("config"),
            streams=streams,
            env_vars=copy.copy(env_vars),
        )
    else:
        logging.info(f"Running connector {connector_name} as a Docker image")
        source = DockerAirbyteSource(
            connector=source_config["docker_image"],
            config=source_config.get("config"),
            streams=streams,
            env_vars=copy.copy(env_vars),
        )

    # Run airbyte connector locally and check streams
    global_sync_mode = None
    for stream in source.configured_catalog["streams"]:
        name = stream["stream"]["name"]
        sync_mode = stream["sync_mode"]
        if sync_mode != INCREMENTAL_SYNC_MODE and sync_mode != FULL_REFRESH_SYNC_MODE:
            raise ValueError(f"Stream {name} has unknown sync_mode: {sync_mode}")
        global_sync_mode = global_sync_mode or sync_mode
        if global_sync_mode != sync_mode:
            raise ValueError(
                "All streams within the same 'pw.io.airbyte.read' must have "
                "the same 'sync_mode'"
            )

    return source


class _AirbyteRecordSchema(Schema):
    data: dict


def read(
    config_file_path: os.PathLike | str,
    streams: Sequence[str],
    *,
    execution_type: str = "local",
    mode: str = "streaming",
    env_vars: dict[str, str] | None = None,
    service_user_credentials_file: str | None = None,
    gcp_region: str = "europe-west1",
    gcp_job_name: str | None = None,
    enforce_method: str | None = None,
    refresh_interval_ms: int = 60000,
    name: str | None = None,
    **kwargs,
):
    """
    Reads a table with a free tier Airbyte connector that supports the \
`incremental <https://docs.airbyte.com/using-airbyte/core-concepts/sync-modes/incremental-append>`_ \
mode. Please note that reusage Airbyte license is not supported at the moment.

    If the local execution type is selected, Pathway initially attempts to find the
    specified connector on PyPI and install its latest version in a separate virtual
    environment. If the connector isn't written in Python or isn't found on PyPI, it
    will be executed using a Docker image. Keep in mind, you can change this behavior
    using the ``enforce_method`` parameter.

    Please be aware that it is highly recommended to use the ``enforce_method`` parameter
    in production deployments. This is because autodetection can fail if the PyPI service
    is unavailable.

    Args:
        config_file_path: Path to the config file, created with
            `airbyte-serverless <https://github.com/unytics/airbyte_serverless>`_ tool
            or just via Pathway CLI (that uses ``airbyte-serverless`` under the hood). The "source"
            section in this file must be properly configured in advance.
        streams: Airbyte stream names to be read.
        execution_type: denotes how the airbyte connector is run. If ``"local"`` is specified
            the connector is executed on a local machine. If ``"remote"`` is used, the
            connector runs as a Google Cloud Run job.
        mode: denotes how the engine polls the new data from the source. Currently
            ``"streaming"`` and ``"static"`` are supported. If set to ``"streaming"``,
            it will check for updates every ``refresh_interval_ms`` milliseconds. ``"static"``
            mode will only consider the available data and ingest all of it in one
            commit. The default value is ``"streaming"``.
        env_vars: environment variables to be set in the Airbyte connector before its'
            execution.
        service_user_credentials_file: Google API service user json file. You can refer
            the instructions provided in the `developer's user guide
            <https://pathway.com/developers/user-guide/connectors/gdrive-connector/#setting-up-google-drive>`_
            to obtain them. The credentials are required for the ``"remote"`` execution type.
        gcp_region: Google `region <https://cloud.google.com/compute/docs/regions-zones>`_
            for the cloud job.
        gcp_job_name: the name of GCP job if ``"remote"`` execution type is chosen. If
            unspecified, the name is autogenerated.
        refresh_interval_ms: time in milliseconds between new data queries. Applicable
            if mode is set to ``"streaming"``.
        enforce_method: when set to ``"docker"``, Pathway will not try to locate and
            run the latest connector version from PyPI. On the other hand, when set to
            ``"pypi"``, Pathway will prefer the usage of the latest image available on
            PyPI. Use this option when you need to ensure certain behavior on the local
            run.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.

    Returns:

    A table with a column ``data``, containing the
    `pw.Json </developers/api-docs/pathway#pathway.Json>`_ containing the data read from
    the connector. The format of this data corresponds to the one used in the Airbyte.

    Example:

    The simplest way to test this connector is to use
    `The Sample Data (Faker) <https://docs.airbyte.com/integrations/sources/faker>`_ data
    source provided by Airbyte.

    To do that, you can use Pathway CLI command ``airbyte create-source``. You can create
    the ``Faker`` data source as follows:

    .. code-block:: bash
        pathway airbyte create-source simple --image "airbyte/source-faker:latest"

    The config file is located in ``./connections/simple.yaml``. It
    contains the basic parameters of the test data source, such as random seed and the
    number of records to be generated. You don't have to modify any of them to proceed with
    this testing.

    Now, you can just run the read from this configured source. It contains three
    streams: ``users``, ``products``, and ``purchases``. Let's use the stream ``users``,
    which leads us to the following code:

    >>> import pathway as pw
    >>> users_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/simple.yaml",
    ...     streams=["users"],
    ... )

    Let's proceed to a more complex example.

    Suppose that you need to read a stream of commits in a GitHub repository. To do so,
    you can use the \
`Airbyte GitHub connector <https://docs.airbyte.com/integrations/sources/github>`_.

    .. code-block:: bash

        abs create github --source "airbyte/source-github"

    Then, you need to edit the created config file, located at
    `./connections/github.yaml`.

    To get started in the quickest way possible, you can
    remove uncommented ``option_title``, ``access_token``, ``client_id`` and ``client_secret``
    fields in the config while uncommenting the section "Another valid structure for
    credentials". It will require the PAT token, which can be obtained at the
    `Tokens <https://github.com/settings/tokens>`_ page in the GitHub - please note that
    you need to be logged in.

    Then, you also need to set up the repository name in the ``repositories`` field. For
    example, you can specify ``pathwaycom/pathway``. Then you need to remove the unused
    optional fields, and you're ready to go.

    Now, you can simply configure the Pathway connector and run:

    >>> import pathway as pw
    >>> commits_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/github.yaml",
    ...     streams=["commits"],
    ... )

    The result table will contain the JSON payloads with the comprehensive information
    about the commit times. If the ``mode`` is set to ``"streaming"`` (the default), the
    new commits will be appended to this table when they are made.

    In some cases, it is not necessary to poll the changes because the data is given
    in full in the beginning and is not updated afterwards. For instance, in the first
    example we used with the ``users_table`` table, you could also use the static mode of
    the connector:

    >>> users_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/simple.yaml",
    ...     streams=["Users"],
    ...     mode="static",
    ... )

    In the second example, you could use this mode to load the commits data at
    once and then terminate the connector:

    >>> commits_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/github.yaml",
    ...     streams=["commits"],
    ...     mode="static",
    ... )

    While it's not the case with Github connector, which is implemented in Python,
    it's worth noticing that deployment of the code running with the ``"local"`` execution
    type may be challenging because some connectors use Docker under the hood. That may
    lead to a situation where you use Docker to deploy the code which, in turn, uses Docker
    image to run Airbyte's data extraction routines. This problem is widely known as DinD.

    To avoid DinD you may use the ``"remote"`` type of execution. If chosen, it runs the
    Airbyte's data extraction part on the Google Cloud, which also saves CPU and memory at your
    development machine or server. To enable the ``"remote"`` execution type you would need to
    specify the corresponding execution type and to provide a path to the service account
    credentials data file. Consider that the credentials are located in the file
    ``./credentials.json``. Then, running the second example with the ``"remote"`` type of
    execution looks as follows:

    >>> commits_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/github.yaml",
    ...     streams=["commits"],
    ...     mode="static",
    ...     execution_type="remote",
    ...     service_user_credentials_file="./credentials.json",
    ... )

    Please keep in mind that the Google Cloud Runs are
    `billed <https://cloud.google.com/run/pricing>`_ based on the vCPU time and memory time,
    measured in vCPU-seconds and GiB-seconds respectively. Having that said, the usage of
    small values for ``refresh_interval_ms`` is not advised for the remote runs, as they may
    result in more runs and consequently more vCPU and memory time spent, resulting in a
    bigger bill.
    """
    with optional_imports("airbyte"):
        from google.oauth2.service_account import Credentials as ServiceCredentials

        from pathway.io.airbyte.logic import _PathwayAirbyteSubject
        from pathway.third_party.airbyte_serverless.sources import RemoteAirbyteSource

    with open(config_file_path, "r") as f:
        yaml_config = f.read()

    source: AbstractAirbyteSource
    config = yaml.safe_load(yaml_config)
    source_config = config["source"]
    source_config["streams"] = streams
    if execution_type == "local":
        source = _construct_local_source(
            source_config,
            streams,
            env_vars,
            enforce_method,
        )
    elif execution_type == "remote":
        job_id = gcp_job_name or f"airbyte-serverless-{str(uuid.uuid4())}"
        if service_user_credentials_file is None:
            raise ValueError(
                "'service_user_credentials_file' is required for the 'remote' execution."
            )
        credentials = ServiceCredentials.from_service_account_file(
            service_user_credentials_file
        )
        source = RemoteAirbyteSource(
            config=config,
            streams=streams,
            job_id=job_id,
            credentials=credentials,
            region=gcp_region,
            env_vars=copy.copy(env_vars),
        )
    else:
        raise ValueError(
            f"Unknown execution type: {execution_type}. Only 'local' and 'remote' are supported."
        )

    subject = _PathwayAirbyteSubject(
        source=source,
        streams=streams,
        mode=mode,
        refresh_interval_ms=refresh_interval_ms,
    )

    return python_connector_read(
        subject=subject,
        schema=_AirbyteRecordSchema,
        autocommit_duration_ms=max(refresh_interval_ms, 1),
        name="airbyte",
        unique_name=_get_unique_name(name, kwargs),
    )

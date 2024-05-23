import os
from collections.abc import Sequence

import yaml

from pathway.internals.schema import Schema
from pathway.io.python import read as python_connector_read
from pathway.optional_import import optional_imports

INCREMENTAL_SYNC_MODE = "incremental"


class _AirbyteRecordSchema(Schema):
    data: dict


def read(
    config_file_path: os.PathLike | str,
    streams: Sequence[str],
    *,
    mode: str = "streaming",
    refresh_interval_ms: int = 1000,
):
    """
    Reads a table with an Airbyte connector that supports the \
`incremental <https://docs.airbyte.com/using-airbyte/core-concepts/sync-modes/incremental-append>`_ \
mode.

    Args:
        config_file_path: Path to the config file, created with \
`airbyte-serverless <https://github.com/unytics/airbyte_serverless>`_ tool or just via \
Pathway CLI (that uses ``airbyte-serverless`` under the hood). The "source" section in \
this file must be properly configured in advance.
        streams: Airbyte stream names to be read.
        mode: denotes how the engine polls the new data from the source. Currently \
"streaming" and "static" are supported. If set to "streaming", it will check for \
updates every `refresh_interval_ms` milliseconds. "static" \
mode will only consider the available data and ingest all of it in one commit. The \
default value is "streaming".
        refresh_interval_ms: time in milliseconds between new data queries. Applicable \
if mode is set to "streaming".
        autocommit_duration_ms: the maximum time between two commits. Every \
autocommit_duration_ms milliseconds, the updates received by the connector are \
committed and pushed into Pathway's computation graph.

    Returns:

    A table with a column `data`, containing the \
`pw.Json </developers/api-docs/pathway#pathway.Json>`_ containing the data read from \
the connector. The format of this data corresponds to the one used in the Airbyte.

    Example:

    The simplest way to test this connector is to use \
`The Sample Data (Faker) <https://docs.airbyte.com/integrations/sources/faker>`_ data \
source provided by Airbyte.

    To do that, you can use Pathway CLI command `airbyte create-source`. You can create
    the `Faker` data source as follows:

    .. code-block:: bash
        pathway airbyte create-source simple --image "airbyte/source-faker:0.1.4"

    The config file is located in ``./connections/simple.yaml``. It \
contains the basic parameters of the test data source, such as random seed and the \
number of records to be generated. You don't have to modify any of them to proceed with \
this testing.

    Now, you can just run the read from this configured source. It contains three \
streams: `Users`, `Products`, and `Purchases`. Let's use the stream `Users`, which leads to \
us to the following code:

    >>> import pathway as pw
    >>> users_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/simple.yaml",
    ...     streams=["Users"],
    ... )

    Let's proceed to a more complex example.

    Suppose that you need to read a stream of commits in a GitHub repository. To do so,\
you can use the \
`Airbyte GitHub connector <https://docs.airbyte.com/integrations/sources/github>`_.

    .. code-block:: bash

        abs create github --source "airbyte/source-github"

    Then, you need to edit the created config file, located at \
`./connections/github.yaml`.

    To get started in the quickest way possible, you can \
remove uncommented `option_title`, `access_token`, `client_id` and `client_secret` \
fields in the config while uncommenting the section "Another valid structure for \
credentials". It will require the PAT token, which can be obtained at the \
`Tokens <https://github.com/settings/tokens>`_ page in the GitHub - please note that \
you need to be logged in.

    Then, you also need to set up the repository name in the `repositories` field. For \
example, you can specify `pathwaycom/pathway`. Then you need to remove the unused \
optional fields, and you're ready to go.

    Now, you can simply configure the Pathway connector and run:

    >>> import pathway as pw
    >>> commits_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/github.yaml",
    ...     streams=["commits"],
    ... )

    The result table will contain the JSON payloads with the comprehensive information \
about the commit times. If the ``mode`` is set to ``"streaming"`` (the default), the \
new commits will be appended to this table when they are made.

    In some cases, it is not necessary to poll the changes because the data is given \
in full in the beginning and is not updated afterwards. For instance, in the first \
example we used with the ``users_table`` table, you could also use the static mode of \
the connector:

    >>> users_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/simple.yaml",
    ...     streams=["Users"],
    ...     mode="static",
    ... )

    In the second example, you could use this mode to load the commits data at \
once and then terminate the connector:

    >>> import pathway as pw
    >>> commits_table = pw.io.airbyte.read(  # doctest: +SKIP
    ...     "./connections/github.yaml",
    ...     streams=["commits"],
    ...     mode="static",
    ... )
    """
    with optional_imports("airbyte"):
        from pathway.io.airbyte.logic import _PathwayAirbyteSubject
        from pathway.third_party.airbyte_serverless.connections import (
            Source as AirbyteSource,
        )

    with open(config_file_path, "r") as f:
        yaml_config = f.read()

    config = yaml.safe_load(yaml_config)
    source_config = config["source"]
    source_config["streams"] = streams
    source = AirbyteSource(**source_config)

    for stream in source.configured_catalog["streams"]:
        name = stream["stream"]["name"]
        sync_mode = stream["sync_mode"]
        if sync_mode != INCREMENTAL_SYNC_MODE:
            raise ValueError(f"Stream {name} doesn't support 'incremental' sync mode")

    subject = _PathwayAirbyteSubject(
        source=source,
        mode=mode,
        refresh_interval_ms=refresh_interval_ms,
    )

    return python_connector_read(
        subject=subject,
        schema=_AirbyteRecordSchema,
        autocommit_duration_ms=max(refresh_interval_ms, 1),
        name="airbyte",
    )

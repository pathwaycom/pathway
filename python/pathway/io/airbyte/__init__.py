import json
import logging
import os
import time
from collections.abc import Sequence

import yaml
from airbyte_serverless.connections import Source as AirbyteSource
from airbyte_serverless.destinations import BaseDestination as BaseAirbyteDestination

from pathway.internals.schema import Schema
from pathway.io._utils import STATIC_MODE_NAME
from pathway.io.python import ConnectorSubject, read as python_connector_read

MAX_RETRIES = 5
INCREMENTAL_SYNC_MODE = "incremental"

AIRBYTE_STREAM_RECORD_PREFIX = "_airbyte_raw_"
AIRBYTE_DATA_RECORD_FIELD = "_airbyte_data"
AIRBYTE_STATE_RECORD_TYPE = "_airbyte_states"


class _AirbyteRecordSchema(Schema):
    data: dict


class _PathwayAirbyteDestination(BaseAirbyteDestination):
    def __init__(self, on_event, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_event = on_event
        self._state = {}

    def get_state(self):
        return self._state

    def _write(self, record_type, records):
        if record_type.startswith(AIRBYTE_STREAM_RECORD_PREFIX):
            for record in records:
                payload = record.pop(AIRBYTE_DATA_RECORD_FIELD)
                self.on_event(payload)
        elif record_type == AIRBYTE_STATE_RECORD_TYPE:
            # Parse state according to the schema described in the protocol
            # https://docs.airbyte.com/understanding-airbyte/airbyte-protocol#airbytestatemessage
            for record in records:
                full_state = json.loads(record[AIRBYTE_DATA_RECORD_FIELD])
                state_type = full_state.get("type", "LEGACY")
                if state_type == "LEGACY":
                    self._handle_legacy_state(full_state)
                elif state_type == "GLOBAL":
                    self._handle_global_state(full_state)
                elif state_type == "STREAM" or state_type == "PER_STREAM":
                    # two different names in the docs, hence two clauses
                    self._handle_stream_state(full_state)
                else:
                    logging.warning(
                        f"Unknown state type: {state_type}. Ignoring state: {full_state}"
                    )

    def _handle_stream_state(self, full_state):
        stream = full_state.get("stream")
        if stream is None:
            logging.warning("Stream state doesn't contain 'stream' section")
            return
        self._handle_stream_state_inner(stream)

    def _handle_stream_state_inner(self, stream):
        descriptor = stream.get("stream_descriptor")
        if descriptor is None:
            logging.warning(
                "Stream state doesn't contain 'stream.stream_descriptor' section"
            )
            return
        stream_name = descriptor.get("name")
        if stream_name is None:
            logging.warning(
                "Stream state doesn't contain 'stream.stream_descriptor.name' field"
            )
            return
        state_blob = stream.get("stream_state")
        if state_blob is not None:
            self._state.update({stream_name: state_blob})
        else:
            logging.warning("Stream state doesn't contain 'stream.stream_state' field")

    def _handle_legacy_state(self, full_state):
        state_blob = full_state.get("data")
        if state_blob is not None:
            self._state = state_blob
        else:
            logging.warning("Legacy state doesn't contain 'data' field")

    def _handle_global_state(self, full_state):
        stream_states = full_state.get("stream_states")
        if stream_states is None:
            logging.warning("Global state doesn't contain 'stream_states' section")
            return
        for stream_state in stream_states:
            self._handle_stream_state_inner(stream_state)


class _PathwayAirbyteSubject(ConnectorSubject):

    def __init__(
        self,
        source: AirbyteSource,
        mode: str,
        refresh_interval_ms: int,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source = source
        self.mode = mode
        self.refresh_interval = refresh_interval_ms / 1000.0

    def run(self):
        destination = _PathwayAirbyteDestination(
            on_event=lambda payload: self.next_json({"data": payload})
        )
        n_times_failed = 0
        while True:
            time_before_start = time.time()
            try:
                messages = self.source.extract(destination.get_state())
            except Exception:
                logging.exception(
                    "Failed to query airbyte-serverless source, retrying..."
                )
                n_times_failed += 1
                if n_times_failed == MAX_RETRIES:
                    raise
                time_elapsed = time.time() - time_before_start
                time_between_retries = 1.5**n_times_failed
                if time_elapsed < time_between_retries:
                    time.sleep(time_between_retries - time_elapsed)
                continue

            n_times_failed = 0
            destination.load(messages)

            if self.mode == STATIC_MODE_NAME:
                break

            time_elapsed = time.time() - time_before_start
            if time_elapsed < self.refresh_interval:
                time.sleep(self.refresh_interval - time_elapsed)


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
`airbyte-serverless <https://github.com/unytics/airbyte_serverless>`_ tool. The \
"source" section in this file must be properly configured in advance.
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

    To do that, you first need to install the `airbyte-serverless` tool. It can be done \
from pip. Then, you can create the `Faker` data source as follows:

    .. code-block:: bash

        abs create simple --source "airbyte/source-faker:0.1.4"

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

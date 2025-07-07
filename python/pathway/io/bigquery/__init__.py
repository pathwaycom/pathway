import json
import logging
from typing import Any, Iterable

from google.cloud import bigquery
from google.oauth2.service_account import Credentials as ServiceCredentials

from pathway.internals.api import Pointer, Table
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.json import Json
from pathway.io._subscribe import subscribe


class _OutputBuffer:
    MAX_BUFFER_SIZE = 1024

    def __init__(
        self, dataset_name: str, table_name: str, credentials: ServiceCredentials
    ) -> None:
        self._client = bigquery.Client(credentials=credentials)
        self._table_ref = self._client.dataset(dataset_name).table(table_name)
        self._buffer: list[dict] = []

    def on_change(
        self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
    ) -> None:
        row["time"] = time
        row["diff"] = 1 if is_addition else -1
        self._buffer.append(self._convert_to_compatible_format(row))
        if len(self._buffer) == self.MAX_BUFFER_SIZE:
            self._flush_buffer()

    def on_time_end(self, time: int) -> None:
        if self._buffer:
            self._flush_buffer()

    def _convert_to_compatible_format(self, row: dict[str, Any]):
        def maybe_dump_json_value(value: Any):
            if isinstance(value, Json):
                return Json.dumps(value)
            else:
                return value

        return {key: maybe_dump_json_value(value) for key, value in row.items()}

    def _flush_buffer(self) -> None:
        errors = self._client.insert_rows_json(self._table_ref, self._buffer)
        if errors:
            logging.error(
                f"Failed to insert rows into BigQuery table. Errors: {json.dumps(errors)}"
            )
        else:
            self._buffer = []


def write(
    table: Table,
    dataset_name: str,
    table_name: str,
    service_user_credentials_file: str,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table``'s stream of changes into the specified BigQuery table. Please note
    that the schema of the target table must correspond to the schema of the table that is
    being outputted and include two additional fields: an integral field ``time``, denoting the
    ID of the minibatch where the change occurred and an integral field ``diff`` which can be
    either 1 or -1 and which denotes if the entry was inserted to the table or if it was deleted.

    Note that the modification of the row is denoted with a sequence of two operations:
    the deletion operation (``diff = -1``) and the insertion operation (``diff = 1``).

    Args:
        table: The table to output.
        dataset_name: The name of the dataset where the table is located.
        table_name: The name of the table to be written.
        service_user_credentials_file: Google API service user json file. Please
            follow the instructions provided in the `developer's user guide \
<https://pathway.com/developers/user-guide/connect/connectors/gdrive-connector/#setting-up-google-drive>`_ \
to obtain them.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Suppose that there is a Google BigQuery project with a dataset named ``animals`` and
    you want to output the Pathway table ``animal_measurements`` into this dataset's
    table ``measurements``.

    Consider that the credentials are stored in the file ``./credentials.json``. Then,
    you can configure the output as follows:

    >>> pw.io.bigquery.write(  # doctest: +SKIP
    ...     animal_measurements,
    ...     dataset_name="animals",
    ...     table_name="measurements",
    ...     service_user_credentials_file="./credentials.json"
    ... )
    """

    _check_entitlements("bigquery")
    credentials = ServiceCredentials.from_service_account_file(
        service_user_credentials_file
    )
    output_buffer = _OutputBuffer(dataset_name, table_name, credentials)
    subscribe(
        table,
        on_change=output_buffer.on_change,
        on_time_end=output_buffer.on_time_end,
        name=name,
        sort_by=sort_by,
    )

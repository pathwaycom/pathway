# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(table: Table, path: str) -> None:
    """
    Writes ``table``'s stream of changes into `DeltaLake <https://delta.io/>_` data
    storage, in a directory located at ``path``.

    If the storage doesn't exist, it will be created, and the schema of the corresponding
    table will be inferred from the output table. Please note that the output table must
    contain two integer columns ``time`` and ``diff`` corresponding to the computation
    minibatch and the nature of the change: ``1`` for row addition and ``-1`` for row
    deletion.

    Args:
        table: Table to be written.
        filename: Path to the target output DeltaLake storage directory.

    Returns:
        None

    Example:

    Consider a table ``access_log`` that needs to be output to a DeltaLake storage
    located at the folder ``./logs/access-log``. You can do that as follows:

    >>> pw.io.deltalake.write(access_log, "./logs/access-log")  # doctest: +SKIP

    Please note that if there is no filesystem object under this path, the corresponding
    folder will be created. However, if you run this code twice, it appends the new data
    to the storage created on the first run.
    """
    data_storage = api.DataStorage(storage_type="deltalake", path=path)
    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=None,
        value_fields=_format_output_value_fields(table),
    )
    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="deltalake",
        )
    )

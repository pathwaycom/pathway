# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.internals import api, datasink
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(table: Table) -> None:
    """Writes ``table``'s stream of updates to the empty sink.

    Inside this routine, the data is formatted into the empty object, and then doesn't
    get written anywhere.

    Args:
        table: Table to be written.

    Returns:
        None

    Example:

    One (of a very few) examples, where you can probably need this kind of functionality
    if the case when a Pathway program is benchmarked and the IO part needs to be
    simplified as much as possible.

    If the table is ``table``, the null output can be configured in the following way:

    >>> pw.io.null.write(table)  # doctest: +SKIP
    """

    data_storage = api.DataStorage(storage_type="null")
    data_format = api.DataFormat(
        format_type="null",
        key_field_names=None,
        value_fields=[],
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="null",
        )
    )

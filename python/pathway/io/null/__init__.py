# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Iterable

from pathway.internals import api, datasink
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table``'s stream of updates to the empty sink.

    Inside this routine, the data is formatted into the empty object, and then doesn't
    get written anywhere.

    Args:
        table: Table to be written.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

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
            unique_name=name,
            sort_by=sort_by,
        )
    )

# Copyright © 2024 Pathway

from __future__ import annotations

from os import PathLike

import pathway as pw
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def read(
    path: str | PathLike,
    *,
    mode: str = "streaming",
    object_pattern: str = "*",
    with_metadata: bool = False,
    name: str | None = None,
    autocommit_duration_ms: int | None = 1500,
    debug_data=None,
    **kwargs,
) -> Table:
    """Reads a table from a text file or a directory of text files. The resulting table
    will consist of a single column ``data``, and have the number of rows equal to the number
    of lines in the file. Each cell will contain a single line from the file.

    In case the folder is specified, and there are several files placed in the folder,
    their order is determined according to their modification times: the smaller the
    modification time is, the earlier the file will be passed to the engine.

    Args:
        path: Path to the file or to the folder with files or
            `glob <https://en.wikipedia.org/wiki/Glob_(programming)>`_ pattern for the
            objects to be read. The connector will read the contents of all matching files as well
            as recursively read the contents of all matching folders.
        mode: Denotes how the engine polls the new data from the source. Currently
            "streaming" and "static" are supported. If set to "streaming" the engine will wait for
            the updates in the specified directory. It will track file additions, deletions, and
            modifications and reflect these events in the state. For example, if a file was deleted,
            "streaming" mode will also remove rows obtained by reading this file from the table. On
            the other hand, the "static" mode will only consider the available data and ingest all
            of it in one commit. The default value is "streaming".
        object_pattern: Unix shell style pattern for filtering only certain files in the
            directory. Ignored in case a path to a single file is specified. This value will be
            deprecated soon, please use glob pattern in ``path`` instead.
        with_metadata: When set to true, the connector will add an additional column
            named ``_metadata`` to the table. This column will be a JSON field that will contain two
            optional fields - ``created_at`` and ``modified_at``. These fields will have integral
            UNIX timestamps for the creation and modification time respectively. Additionally, the
            column will also have an optional field named ``owner`` that will contain the name of
            the file owner (applicable only for Un). Finally, the column will also contain a field
            named ``path`` that will show the full path to the file from where a row was filled.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
        autocommit_duration_ms: the maximum time between two commits. Every
            ``autocommit_duration_ms`` milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    >>> import pathway as pw
    >>> t = pw.io.plaintext.read("raw_dataset/lines.txt")
    """

    return pw.io.fs.read(
        path,
        format="plaintext",
        mode=mode,
        object_pattern=object_pattern,
        with_metadata=with_metadata,
        name=name,
        autocommit_duration_ms=autocommit_duration_ms,
        debug_data=debug_data,
        _stacklevel=5,
        **kwargs,
    )

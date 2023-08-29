# Copyright © 2023 Pathway

from __future__ import annotations

from os import PathLike
from typing import Optional, Union

import pathway as pw
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@runtime_type_check
@trace_user_frame
def read(
    path: Union[str, PathLike],
    *,
    mode: str = "streaming",
    persistent_id: Optional[str] = None,
    autocommit_duration_ms: Optional[int] = 1500,
    debug_data=None,
) -> Table:
    """Reads a table from a text file or a directory of text files. The resulting table
    will consist of a single column ``data``, and have the number of rows equal to the number
    of lines in the file. Each cell will contain a single line from the file.

    In case the folder is specified, and there are several files placed in the folder,
    their order is determined according to their modification times: the smaller the
    modification time is, the earlier the file will be passed to the engine.

    Args:
        path: Path to a file or to a folder.
        mode: If set to "streaming", the engine will wait for the new input \
files in the directory. Set it to "static", it will only consider the available \
data and ingest all of it in one commit. Default value is "streaming".
        persistent_id: (unstable) An identifier, under which the state of the table \
will be persisted or ``None``, if there is no need to persist the state of this table. \
When a program restarts, it restores the state for all input tables according to what \
was saved for their ``persistent_id``. This way it's possible to configure the start of \
computations from the moment they were terminated last time.
        autocommit_duration_ms: the maximum time between two commits. Every
          autocommit_duration_ms milliseconds, the updates received by the connector are
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
        persistent_id=persistent_id,
        autocommit_duration_ms=autocommit_duration_ms,
        debug_data=debug_data,
    )

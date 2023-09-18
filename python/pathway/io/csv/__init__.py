# Copyright © 2023 Pathway

from __future__ import annotations

from os import PathLike
from typing import Any, Dict, List, Optional, Type, Union

import pathway as pw
from pathway.internals.api import PathwayType
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import CsvParserSettings, check_deprecated_kwargs


@runtime_type_check
@trace_user_frame
def read(
    path: Union[str, PathLike],
    value_columns: Optional[List[str]] = None,
    *,
    schema: Optional[Type[pw.Schema]] = None,
    csv_settings: Optional[CsvParserSettings] = None,
    mode: str = "streaming",
    autocommit_duration_ms: Optional[int] = 1500,
    persistent_id: Optional[str] = None,
    debug_data=None,
    id_columns: Optional[List[str]] = None,
    types: Optional[Dict[str, PathwayType]] = None,
    default_values: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Table:
    """Reads a table from one or several files with delimiter-separated values.

    In case the folder is passed to the engine, the order in which files from
    the directory are processed is determined according to the modification time of
    files within this folder: they will be processed by ascending order of
    the modification time.

    Args:
        path: Path to the file or to the folder with files.
        value_columns: Names of the columns to be extracted from the files. [will be deprecated soon]
        schema: Schema of the resulting table.
        id_columns: In case the table should have a primary key generated according to
            a subset of its columns, the set of columns should be specified in this field.
            Otherwise, the primary key will be generated randomly. [will be deprecated soon]
        csv_settings: Settings for the CSV parser.
        mode: If set to "streaming", the engine will wait for the new input
            files in the directory. Set it to "static", it will only consider the available
            data and ingest all of it in one commit. Default value is "streaming".
        types: Dictionary containing the mapping between the columns and the data
            types (``pw.Type``) of the values of those columns. This parameter is optional, and if not
            provided the default type is ``pw.Type.ANY``. [will be deprecated soon]
        default_values: dictionary containing default values for columns replacing
            blank entries. The default value of the column must be specified explicitly,
            otherwise there will be no default value. [will be deprecated soon]
        autocommit_duration_ms: the maximum time between two commits. Every
          autocommit_duration_ms milliseconds, the updates received by the connector are
          committed and pushed into Pathway's computation graph.
        persistent_id: (unstable) An identifier, under which the state of the table
            will be persisted or ``None``, if there is no need to persist the state of this table.
            When a program restarts, it restores the state for all input tables according to what
            was saved for their ``persistent_id``. This way it's possible to configure the start of
            computations from the moment they were terminated last time.
        debug_data: Static data replacing original one when debug mode is active.

    Returns:
        Table: The table read.

    Example:

    Consider you want to read a dataset, stored in the filesystem in a standard CSV
    format. The dataset contains data about pets and their owners.

    For the sake of demonstration, you can prepare a small dataset by creating a CSV file
    via a unix command line tool:

    .. code-block:: bash

        printf "id,owner,pet\\n1,Alice,dog\\n2,Bob,dog\\n3,Alice,cat\\n4,Bob,dog" > dataset.csv

    In order to read it into Pathway's table, you can first do the import and then
    use the `pw.io.csv.read` method:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.csv.read("dataset.csv", schema=InputSchema, mode="static")

    Then, you can output the table in order to check the correctness of the read:

    >>> pw.debug.compute_and_print(t, include_id=False)  # doctest: +SKIP
    owner pet
    Alice dog
      Bob dog
    Alice cat
      Bob dog


    Now let's try something different. Consider you have site access logs stored in a
    separate folder in several files. For the sake of simplicity, a log entry contains
    an access ID, an IP address and the login of the user.

    A dataset, corresponding to the format described above can be generated, thanks to the
    following set of unix commands:

    .. code-block:: bash

        mkdir logs
        printf "id,ip,login\\n1,127.0.0.1,alice\\n2,8.8.8.8,alice" > logs/part_1.csv
        printf "id,ip,login\\n3,8.8.8.8,bob\\n4,127.0.0.1,alice" > logs/part_2.csv

    Now, let's see how you can use the connector in order to read the content of this
    directory into a table:

    >>> class InputSchema(pw.Schema):
    ...   ip: str
    ...   login: str
    >>> t = pw.io.csv.read("logs/", schema=InputSchema, mode="static")

    The only difference is that you specified the name of the directory instead of the
    file name, as opposed to what you had done in the previous example. It's that simple!

    But what if you are working with a real-time system, which generates logs all the time.
    The logs are being written and after a while they get into the log directory (this is
    also called "logs rotation"). Now, consider that there is a need to fetch the new files
    from this logs directory all the time. Would Pathway handle that? Sure!

    The only difference would be in the usage of `mode` flag. So the code
    snippet will look as follows:

    >>> t = pw.io.csv.read("logs/", schema=InputSchema, mode="streaming")

    With this method, you obtain a table updated dynamically. The changes in the logs would incur
    changes in the Business-Intelligence 'BI'-ready data, namely, in the tables you would like to output. To see
    how these changes are reported by Pathway, have a look at the
    `"Streams of Updates and Snapshots" </developers/documentation/input-and-output-streams/stream-of-updates/>`_
    article.
    """

    check_deprecated_kwargs(
        kwargs,
        ["poll_new_objects"],
    )

    return pw.io.fs.read(
        path,
        schema=schema,
        format="csv",
        mode=mode,
        csv_settings=csv_settings,
        autocommit_duration_ms=autocommit_duration_ms,
        json_field_paths=None,
        persistent_id=persistent_id,
        debug_data=debug_data,
        value_columns=value_columns,
        primary_key=id_columns,
        types=types,
        default_values=default_values,
    )


@runtime_type_check
@trace_user_frame
def write(table: Table, filename: Union[str, PathLike]) -> None:
    """Writes `table`'s stream of updates to a file in delimiter-separated values format.

    Args:
        table: Table to be written.
        filename: Path to the target output file.

    Returns:
        None

    Example:

    In this simple example you can see how table output works.
    First, import Pathway and create a table:

    >>> import pathway as pw
    >>> t = pw.debug.parse_to_table("age owner pet \\n 1 10 Alice dog \\n 2 9 Bob cat \\n 3 8 Alice cat")

    Consider you would want to output the stream of changes of this table. In order to do that
    you simply do:

    >>> pw.io.csv.write(t, "table.csv")

    Now, let's see what you have on the output:

    .. code-block:: bash

        cat table.csv

    .. code-block:: csv

        age,owner,pet,time,diff
        10,"Alice","dog",0,1
        9,"Bob","cat",0,1
        8,"Alice","cat",0,1

    The first three columns clearly represent the data columns you have. The column time
    represents the number of operations minibatch, in which each of the rows was read. In
    this example, since the data is static: you have 0. The diff is another
    element of this stream of updates. In this context, it is 1 because all three rows were read from
    the input. All in all, the extra information in ``time`` and ``diff`` columns - in this case -
    shows us that in the initial minibatch (``time = 0``), you have read three rows and all of
    them were added to the collection (``diff = 1``).
    """

    pw.io.fs.write(
        table,
        filename=filename,
        format="csv",
    )

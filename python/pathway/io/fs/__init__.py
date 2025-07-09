# Copyright © 2024 Pathway

from __future__ import annotations

import warnings
from os import PathLike, fspath
from typing import Any, Iterable, Literal

from pathway.internals import Schema, api, datasink, datasource
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    CsvParserSettings,
    _get_unique_name,
    construct_schema_and_data_format,
    internal_connector_mode,
    internal_read_method,
)

SUPPORTED_OUTPUT_FORMATS: set[str] = {
    "csv",
    "json",
}


@check_arg_types
@trace_user_frame
def read(
    path: str | PathLike,
    format: Literal[
        "csv", "json", "plaintext", "plaintext_by_file", "binary", "only_metadata"
    ],
    *,
    schema: type[Schema] | None = None,
    mode: Literal["streaming", "static"] = "streaming",
    csv_settings: CsvParserSettings | None = None,
    json_field_paths: dict[str, str] | None = None,
    object_pattern: str = "*",
    with_metadata: bool = False,
    name: str | None = None,
    autocommit_duration_ms: int | None = 1500,
    debug_data: Any = None,
    _stacklevel: int = 1,
    **kwargs,
) -> Table:
    """Reads a table from one or several files with the specified format.

    In case the format is ``"plaintext"``, the table will consist of a single column
    ``data`` with each cell containing a single line from the file.

    In case the format is one of ``"plaintext_by_file"`` or ``"binary"`` the table will
    consist of a single column ``data`` with each cell containing contents of the whole file.

    If the format is ``"only_metadata"``, only the metadata column will be read, without
    opening and without reading the contents of the files. The metadata is then available
    in the ``_metadata`` column.

    Args:
        path: Path to the file or to the folder with files or
            `glob <https://en.wikipedia.org/wiki/Glob_(programming)>`_ pattern for the
            objects to be read. The connector will read the contents of all matching files as well
            as recursively read the contents of all matching folders.
        format: Format of data to be read. Currently ``"csv"``, ``"json"``, ``"plaintext"``,
            ``"plaintext_by_file"``, ``"binary"``, and ``"only_metadata"`` formats are
            supported. The difference between ``"plaintext"`` and ``"plaintext_by_file"`` is
            how the input is tokenized: if the ``"plaintext"`` option is chosen, it's split
            by the newlines. Otherwise, the files are split in full and one row will
            correspond to one file. In case the ``"binary"`` format is specified,
            the data is read as raw bytes without UTF-8 parsing. Finally, if ``"only_metadata"``
            is chosen, the connector only scans the filesystem for file additions,
            changes, modifications, and provides them in the metadata column.
        schema: Schema of the resulting table.
        mode: Denotes how the engine polls the new data from the source. Currently
            ``"streaming"`` and ``"static"`` are supported. If set to ``"streaming"`` the engine will wait for
            the updates in the specified directory. It will track file additions, deletions, and
            modifications and reflect these events in the state. For example, if a file was deleted,
            ``"streaming"`` mode will also remove rows obtained by reading this file from the table. On
            the other hand, the ``"static"`` mode will only consider the available data and ingest all
            of it in one commit. The default value is ``"streaming"``.
        csv_settings: Settings for the CSV parser. This parameter is used only in case
            the specified format is ``"csv"``.
        json_field_paths: If the format is ``"json"``, this field allows to map field names
            into path in the read json object. For the field which require such mapping,
            it should be given in the format ``<field_name>: <path to be mapped>``,
            where the path to be mapped needs to be a
            `JSON Pointer (RFC 6901) <https://www.rfc-editor.org/rfc/rfc6901>`_.
        object_pattern: Unix shell style pattern for filtering only certain files in the
            directory. Ignored in case a path to a single file is specified. This value will be
            deprecated soon, please use glob pattern in ``path`` instead.
        with_metadata: When set to true, the connector will add an additional column
            named ``_metadata`` to the table. This JSON field may contain: (1) ``created_at`` - UNIX
            timestamp of file creation; (2) ``modified_at`` - UNIX timestamp of last modification;
            (3) ``seen_at`` is a UNIX timestamp of when they file was found by the engine;
            (4) ``owner`` - Name of the file ``owner`` (only for Unix); (5) ``path`` - Full file path of the
            source row. (6) ``size`` - File size in bytes.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.
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
    use the ``pw.io.fs.read`` method:

    >>> import pathway as pw
    >>> class InputSchema(pw.Schema):
    ...   owner: str
    ...   pet: str
    >>> t = pw.io.fs.read("dataset.csv", format="csv", schema=InputSchema)

    Then, you can output the table in order to check the correctness of the read:

    >>> pw.debug.compute_and_print(t, include_id=False)  # doctest: +SKIP
    owner pet
    Alice dog
      Bob dog
    Alice cat
      Bob dog

    Similarly, we can do the same for JSON format.

    First, we prepare a dataset:

    .. code-block:: bash

        printf "{\\"id\\":1,\\"owner\\":\\"Alice\\",\\"pet\\":\\"dog\\"}
        {\\"id\\":2,\\"owner\\":\\"Bob\\",\\"pet\\":\\"dog\\"}
        {\\"id\\":3,\\"owner\\":\\"Bob\\",\\"pet\\":\\"cat\\"}
        {\\"id\\":4,\\"owner\\":\\"Bob\\",\\"pet\\":\\"cat\\"}" > dataset.jsonlines

    And then, we use the method with the ``"json"`` format:

    >>> t = pw.io.fs.read("dataset.jsonlines", format="json", schema=InputSchema)

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
    >>> t = pw.io.fs.read("logs/", format="csv", schema=InputSchema)

    The only difference is that you specified the name of the directory instead of the
    file name, as opposed to what you had done in the previous example. It's that simple!

    Alternatively, we can do the same for the ``"json"`` variant:

    The dataset creation would look as follows:

    .. code-block:: bash

        mkdir logs
        printf "{\\"id\\":1,\\"ip\\":\\"127.0.0.1\\",\\"login\\":\\"alice\\"}
        {\\"id\\":2,\\"ip\\":\\"8.8.8.8\\",\\"login\\":\\"alice\\"}" > logs/part_1.jsonlines
        printf "{\\"id\\":3,\\"ip\\":\\"8.8.8.8\\",\\"login\\":\\"bob\\"}
        {\\"id\\":4,\\"ip\\":\\"127.0.0.1\\",\\"login\\":\\"alice\\"}" > logs/part_2.jsonlines

    While reading the data from logs folder can be expressed as:

    >>> t = pw.io.fs.read("logs/", format="json", schema=InputSchema, mode="static")

    But what if you are working with a real-time system, which generates logs all the time.
    The logs are being written and after a while they get into the log directory (this is
    also called "logs rotation"). Now, consider that there is a need to fetch the new files
    from this logs directory all the time. Would Pathway handle that? Sure!

    The only difference would be in the usage of ``mode`` field. So the code
    snippet will look as follows:

    >>> t = pw.io.fs.read("logs/", format="csv", schema=InputSchema, mode="streaming")

    Or, for the ``"json"`` format case:

    >>> t = pw.io.fs.read("logs/", format="json", schema=InputSchema, mode="streaming")

    With this method, you obtain a table updated dynamically. The changes in the logs would incur
    changes in the Business-Intelligence 'BI'-ready data, namely, in the tables you would like to output.
    Finally, a simple example for the plaintext format would look as follows:

    >>> t = pw.io.fs.read("raw_dataset/lines.txt", format="plaintext")
    """

    path = fspath(path)

    if object_pattern != "*":
        warnings.warn(
            "'object_pattern' is deprecated and will be removed soon. "
            "Please use a glob pattern in `path` instead",
            DeprecationWarning,
            stacklevel=_stacklevel + 4,
        )

    only_provide_metadata = format == "only_metadata"
    with_metadata = with_metadata or only_provide_metadata
    data_storage = api.DataStorage(
        storage_type="fs",
        csv_parser_settings=csv_settings.api_settings if csv_settings else None,
        path=path,
        mode=internal_connector_mode(mode),
        read_method=internal_read_method(format),
        object_pattern=object_pattern,
        only_provide_metadata=only_provide_metadata,
    )

    schema, data_format = construct_schema_and_data_format(
        format,
        schema=schema,
        with_metadata=with_metadata,
        csv_settings=csv_settings,
        json_field_paths=json_field_paths,
        _stacklevel=_stacklevel + 4,
    )

    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=_get_unique_name(name, kwargs, _stacklevel + 5),
    )

    table = table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=data_source_options,
            schema=schema,
            datasource_name="fs",
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )
    if only_provide_metadata:
        table = table.select(_metadata=table._metadata)

    return table


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    filename: str | PathLike,
    format: Literal["json", "csv"],
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Writes ``table``'s stream of updates to a file in the given format.

    Args:
        table: Table to be written.
        filename: Path to the target output file.
        format: Format to use for data output. Currently, there are two supported
            formats: ``"json"`` and ``"csv"``.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    In this simple example you can see how table output works.
    First, import Pathway and create a table:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown("age owner pet \\n1 10 Alice dog \\n2 9 Bob cat \\n3 8 Alice cat")

    Consider you would want to output the stream of changes of this table in ``"csv"`` format.
    In order to do that you simply do:

    >>> pw.io.fs.write(t, "table.csv", format="csv")

    Now, let's see what you have on the output:

    .. code-block:: bash

        cat table.csv

    .. code-block:: csv

        age,owner,pet,time,diff
        10,"Alice","dog",0,1
        9,"Bob","cat",0,1
        8,"Alice","cat",0,1

    The first three columns clearly represent the data columns you have. The column ``time``
    represents the number of operations minibatch, in which each of the rows was read. In
    this example, since the data is static: you have ``0``. The ``diff`` is another
    element of this stream of updates. In this context, it is ``1`` because all three rows were read from
    the input. All in all, the extra information in ``time`` and ``diff`` columns - in this case -
    shows us that in the initial minibatch (``time = 0``), you have read three rows and all of
    them were added to the collection (``diff = 1``).

    Alternatively, this data can be written in ``"json"`` format:

    >>> pw.io.fs.write(t, "table.jsonlines", format="json")

    Then, we can also check the output file by executing the command:

    .. code-block:: bash

        cat table.jsonlines

    .. code-block:: json

        {"age":10,"owner":"Alice","pet":"dog","diff":1,"time":0}
        {"age":9,"owner":"Bob","pet":"cat","diff":1,"time":0}
        {"age":8,"owner":"Alice","pet":"cat","diff":1,"time":0}

    As one can easily see, the values remain the same, while the format has changed to
    a plain JSON.
    """

    if format not in SUPPORTED_OUTPUT_FORMATS:
        raise ValueError(
            "Unknown format: {}. Only {} are supported".format(
                format, ", ".join(SUPPORTED_OUTPUT_FORMATS)
            )
        )

    data_storage = api.DataStorage(storage_type="fs", path=fspath(filename))
    if format == "csv":
        data_format = api.DataFormat(
            format_type="dsv",
            key_field_names=[],
            value_fields=_format_output_value_fields(table),
            delimiter=",",
        )
    elif format == "json":
        data_format = api.DataFormat(
            format_type="jsonlines",
            key_field_names=[],
            value_fields=_format_output_value_fields(table),
        )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="fs",
            unique_name=name,
            sort_by=sort_by,
        )
    )

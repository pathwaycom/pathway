# Copyright © 2023 Pathway

import json
import threading
from abc import ABC, abstractmethod
from queue import Queue
from typing import Any

from pathway.internals import api, datasource
from pathway.internals.api import PathwayType, Pointer
from pathway.internals.decorators import table_from_datasource
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.schema import Schema
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    RawDataSchema,
    assert_schema_or_value_columns_not_none,
    get_data_format_type,
    read_schema,
)

SUPPORTED_INPUT_FORMATS: set[str] = {
    "json",
    "raw",
}


class ConnectorSubject(ABC):
    """An abstract class allowing to create custom python connectors.

    Custom python connector can be created by extending this class and implementing
    :py:meth:`run` function responsible for filling the buffer with data.
    This function will be started by pathway engine in a separate thread.

    In order to send a message one of the methods
    :py:meth:`next_json`, :py:meth:`next_str`, :py:meth:`next_bytes` can be used.
    """

    _buffer: Queue

    def __init__(self) -> None:
        self._buffer = Queue()

    @abstractmethod
    def run(self) -> None:
        ...

    def on_stop(self) -> None:
        """Called after the end of the :py:meth:`run` function."""
        pass

    def next_json(self, message: dict) -> None:
        """Sends a message.

        Args:
            message: Dict representing json.
        """
        self.next_str(json.dumps(message, ensure_ascii=False))

    def next_str(self, message: str) -> None:
        """Sends a message.

        Args:
            message: json string.
        """
        self.next_bytes(message.encode(encoding="utf-8"))

    def next_bytes(self, message: bytes) -> None:
        """Sends a message.

        Args:
            message: bytes encoded json string.
        """
        self._add(None, message)

    def commit(self) -> None:
        """Sends a commit message."""
        self.next_bytes(b"*COMMIT*")

    def close(self) -> None:
        """Sends a sentinel message.

        Should be called to indicate that no new messages will be sent.
        """
        self.next_bytes(b"*FINISH*")

    def start(self) -> None:
        """Runs a separate thread with function feeding data into buffer.

        Should not be called directly.
        """

        def target():
            try:
                self.run()
            finally:
                self.on_stop()
                self.close()

        threading.Thread(target=target).start()

    def _add(self, key: Pointer | None, message: Any) -> None:
        self._buffer.put((True, key, message))

    def _remove(self, key: Pointer, message: Any) -> None:
        self._buffer.put((False, key, message))

    def _read(self) -> Any:
        """Allows to retrieve data from a buffer.

        Should not be called directly.
        """
        return self._buffer.get()

    def _is_internal(self) -> bool:
        """
        The Python connector is internal in case it is used to implement an internal
        Pathway feature rather than to read the data from an external source.

        We need this distinction, because internal usages don't read user data and
        aren't a part of the external perimeter, which is currently persisted. Therefore
        we need to tell the engine not to require persistent_id from such connectors
        and not to store the snapshot of its inputs.
        """
        return False


@runtime_type_check
@trace_user_frame
def read(
    subject: ConnectorSubject,
    *,
    schema: type[Schema] | None = None,
    format: str = "json",
    autocommit_duration_ms: int = 1500,
    debug_data=None,
    value_columns: list[str] | None = None,
    primary_key: list[str] | None = None,
    types: dict[str, PathwayType] | None = None,
    default_values: dict[str, Any] | None = None,
    persistent_id: str | None = None,
):
    """Reads a table from a ConnectorSubject.

    Args:
        subject: An instance of a :py:class:`~pathway.python.ConnectorSubject`.
        schema: Schema of the resulting table.
        format: Format of the data produced by a subject, "json" or "raw". In case of
            a "raw" format, table with single "data" column will be produced.
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms: the maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph
        value_columns: Columns to extract for a table. [will be deprecated soon]
        primary_key: In case the table should have a primary key generated according to
            a subset of its columns, the set of columns should be specified in this field.
            Otherwise, the primary key will be generated randomly. [will be deprecated soon]
        types: Dictionary containing the mapping between the columns and the data
            types (``pw.Type``) of the values of those columns. This parameter is optional, and if not
            provided the default type is ``pw.Type.ANY``. [will be deprecated soon]
        default_values: dictionary containing default values for columns replacing
            blank entries. The default value of the column must be specified explicitly,
            otherwise there will be no default value. [will be deprecated soon]
        persistent_id: (unstable) An identifier, under which the state of the table \
will be persisted or ``None``, if there is no need to persist the state of this table. \
When a program restarts, it restores the state for all input tables according to what \
was saved for their ``persistent_id``. This way it's possible to configure the start of \
computations from the moment they were terminated last time.

    Returns:
        Table: The table read.
    """

    data_format_type = get_data_format_type(format, SUPPORTED_INPUT_FORMATS)

    if data_format_type == "identity":
        if primary_key:
            raise ValueError("raw format must not be used with primary_key property")
        if value_columns:
            raise ValueError("raw format must not be used with value_columns property")
        schema = RawDataSchema

    assert_schema_or_value_columns_not_none(schema, value_columns, data_format_type)

    schema, api_schema = read_schema(
        schema=schema,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
    )
    data_format = api.DataFormat(
        **api_schema,
        format_type=data_format_type,
    )
    data_storage = api.DataStorage(
        storage_type="python",
        python_subject=api.PythonSubject(
            start=subject.start, read=subject._read, is_internal=subject._is_internal()
        ),
        persistent_id=persistent_id,
    )
    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms
    )
    return table_from_datasource(
        datasource.GenericDataSource(
            datastorage=data_storage,
            dataformat=data_format,
            data_source_options=data_source_options,
            schema=schema,
        ),
        debug_datasource=datasource.debug_datasource(debug_data),
    )

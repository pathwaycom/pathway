# Copyright © 2024 Pathway
import json
import queue
import threading
import time
from abc import ABC, abstractmethod
from queue import Queue
from typing import Any

import pandas as pd
import panel as pn
from IPython.display import display

from pathway.internals import Table, api, datasource
from pathway.internals.api import DataEventType, PathwayType, Pointer, SessionType
from pathway.internals.decorators import table_from_datasource
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    MetadataSchema,
    RawDataSchema,
    assert_schema_or_value_columns_not_none,
    get_data_format_type,
    internal_read_method,
    read_schema,
)

SUPPORTED_INPUT_FORMATS: set[str] = {
    "json",
    "raw",
    "binary",
}


class ConnectorSubject(ABC):
    """An abstract class allowing to create custom python connectors.

    Custom python connector can be created by extending this class and implementing
    :py:meth:`run` function responsible for filling the buffer with data.
    This function will be started by pathway engine in a separate thread.

    In order to send a message one of the methods
    :py:meth:`next_json`, :py:meth:`next_str`, :py:meth:`next_bytes` can be used.

    If the subject won't delete records, set the class property ``deletions_enabled``
    to ``False`` as it may help to improve the performance.

    Example:

    >>> import pathway as pw
    >>> from pathway.io.python import ConnectorSubject
    >>>
    >>> class MySchema(pw.Schema):
    ...     a: int
    ...     b: str
    ...
    >>>
    >>> class MySubject(ConnectorSubject):
    ...     def run(self) -> None:
    ...         for i in range(4):
    ...             self.next_json({"a": i, "b": f"x{i}"})
    ...     @property
    ...     def _deletions_enabled(self) -> bool:
    ...         return False
    ...
    >>>
    >>> s = MySubject()
    >>>
    >>> table = pw.io.python.read(s, schema=MySchema)
    >>> pw.debug.compute_and_print(table, include_id=False)
    a | b
    0 | x0
    1 | x1
    2 | x2
    3 | x3
    """

    _buffer: Queue
    _thread: threading.Thread | None
    _exception: BaseException | None
    _already_used: bool

    def __init__(self) -> None:
        self._buffer = Queue()
        self._thread = None
        self._exception = None
        self._already_used = False

    @abstractmethod
    def run(self) -> None:
        ...

    def on_stop(self) -> None:
        """Called after the end of the :py:meth:`run` function."""
        pass

    def _is_finite(self) -> bool:
        """
        Denotes if the connector teminates after the code inside run() routine
        is completed.
        """
        return True

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
        self._buffer.put((DataEventType.INSERT, None, b"*COMMIT*", None))

    def close(self) -> None:
        """Sends a sentinel message.

        Should be called to indicate that no new messages will be sent.
        """
        self._buffer.put((DataEventType.INSERT, None, b"*FINISH*", None))

    def start(self) -> None:
        """Runs a separate thread with function feeding data into buffer.

        Should not be called directly.
        """

        def target():
            try:
                self.run()
            except BaseException as e:
                self._exception = e
            finally:
                if self._is_finite() or self._exception is not None:
                    self.on_stop()
                    self.close()

        self._thread = threading.Thread(target=target)
        self._thread.start()

    def end(self) -> None:
        """Joins a thread running :py:meth:`run`.

        Should not be called directly.
        """
        assert self._thread is not None
        self._thread.join()
        if self._exception is not None:
            raise self._exception

    def _add(
        self, key: Pointer | None, message: bytes, metadata: bytes | None = None
    ) -> None:
        if self._session_type == SessionType.NATIVE:
            self._buffer.put((DataEventType.INSERT, key, message, metadata))
        elif self._session_type == SessionType.UPSERT:
            if not self._deletions_enabled:
                raise ValueError(
                    f"Trying to upsert a row in {type(self)} but deletions_enabled is set to False."
                )
            self._buffer.put((DataEventType.UPSERT, key, message, metadata))
        else:
            raise NotImplementedError(f"session type {self._session_type} not handled")

    def _remove(
        self, key: Pointer, message: bytes, metadata: bytes | None = None
    ) -> None:
        if not self._deletions_enabled:
            raise ValueError(
                f"Trying to delete a row in {type(self)} but deletions_enabled is set to False."
            )
        self._buffer.put((DataEventType.DELETE, key, message, metadata))

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

    @property
    def _with_metadata(self) -> bool:
        return False

    @property
    def _session_type(self) -> SessionType:
        return SessionType.NATIVE

    @property
    def _deletions_enabled(self) -> bool:
        return True


@check_arg_types
@trace_user_frame
def read(
    subject: ConnectorSubject,
    *,
    schema: type[Schema] | None = None,
    format: str = "json",
    autocommit_duration_ms: int | None = 1500,
    debug_data=None,
    value_columns: list[str] | None = None,
    primary_key: list[str] | None = None,
    types: dict[str, PathwayType] | None = None,
    default_values: dict[str, Any] | None = None,
    persistent_id: str | None = None,
) -> Table:
    """Reads a table from a ConnectorSubject.

    Args:
        subject: An instance of a :py:class:`~pathway.python.ConnectorSubject`.
        schema: Schema of the resulting table.
        format: Format of the data produced by a subject, "json", "raw" or "binary". In case of
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

    if subject._already_used:
        raise ValueError(
            "You can't use the same ConnectorSubject object in more than one Python connector."
            + "If you want to use the same ConnectorSubject twice, create two separate objects of this class."
        )
    subject._already_used = True

    data_format_type = get_data_format_type(format, SUPPORTED_INPUT_FORMATS)

    if data_format_type == "identity":
        if primary_key:
            raise ValueError("raw format must not be used with primary_key property")
        if value_columns:
            raise ValueError("raw format must not be used with value_columns property")
        schema = RawDataSchema
        if subject._with_metadata is True:
            schema |= MetadataSchema
    assert_schema_or_value_columns_not_none(schema, value_columns, data_format_type)

    schema, api_schema = read_schema(
        schema=schema,
        value_columns=value_columns,
        primary_key=primary_key,
        types=types,
        default_values=default_values,
        _stacklevel=5,
    )
    data_format = api.DataFormat(
        **api_schema,
        format_type=data_format_type,
        session_type=subject._session_type,
        parse_utf8=(format != "binary"),
    )
    mode = (
        api.ConnectorMode.STREAMING
        if subject._deletions_enabled
        else api.ConnectorMode.STATIC
    )
    data_storage = api.DataStorage(
        storage_type="python",
        python_subject=api.PythonSubject(
            start=subject.start,
            read=subject._read,
            end=subject.end,
            is_internal=subject._is_internal(),
            deletions_enabled=subject._deletions_enabled,
        ),
        read_method=internal_read_method(format),
        persistent_id=persistent_id,
        mode=mode,
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


class InteractiveCsvPlayer(ConnectorSubject):
    q: queue.Queue

    def __init__(self, csv_file="") -> None:
        super().__init__()
        self.q = queue.Queue()

        self.df = pd.read_csv(csv_file)

        state = pn.widgets.Spinner(value=0, width=0)
        int_slider = pn.widgets.IntSlider(
            name="Row position in csv",
            start=0,
            end=len(self.df),
            step=1,
            value=0,
            disabled=True,
        )
        int_slider.jscallback(
            value="""
        if (int_slider.value < state.value)
        int_slider.value = state.value
        """,
            args={"int_slider": int_slider, "state": state},
        )

        def updatecallback(target, event):
            if event.new > event.old:
                target.value = event.new
                self.q.put_nowait(target.value)

        int_slider.link(state, callbacks={"value": updatecallback})

        self.state = state
        self.int_slider = int_slider
        display(pn.Row(state, int_slider, f"{len(self.df)} rows in csv"))

    def run(self):
        last_streamed_idx = -1
        while True:
            try:
                new_pos = self.q.get()
                for i in range(last_streamed_idx + 1, new_pos):
                    self.next_json(self.df.iloc[i].to_dict())
                last_streamed_idx = new_pos - 1
                if new_pos == len(self.df):
                    break
            except queue.Empty:
                pass
            time.sleep(0.1)
        self.close()

    def on_stop(self) -> None:
        self.int_slider.disabled = True

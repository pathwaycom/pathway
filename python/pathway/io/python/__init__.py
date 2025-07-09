# Copyright © 2024 Pathway
import json
import queue
import threading
import time
import warnings
from abc import ABC, abstractmethod
from queue import Queue
from typing import Any, Iterable, Literal, final

import pandas as pd
import panel as pn
from IPython.display import display

from pathway.internals import Table, api, datasource
from pathway.internals.api import Pointer, PythonConnectorEventType, SessionType
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema
from pathway.internals.table_io import table_from_datasource
from pathway.internals.table_subscription import subscribe
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import (
    MetadataSchema,
    PlaintextDataSchema,
    RawDataSchema,
    _get_unique_name,
    assert_schema_not_none,
    get_data_format_type,
    read_schema,
)

SUPPORTED_INPUT_FORMATS: set[str] = {
    "json",
    "raw",
    "binary",
}

COMMIT_LITERAL = "*COMMIT*"
ENABLE_COMMITS_LITERAL = "*ENABLE_COMMITS*"
DISABLE_COMMITS_LITERAL = "*DISABLE_COMMITS*"
FINISH_LITERAL = "*FINISH*"

PW_SPECIAL_OFFSET_KEY = "_pw_offset"


class ConnectorSubject(ABC):
    """An abstract class allowing to create custom python input connectors. Use with
    :py:func:`~pathway.io.python.read`.

    Custom python connector can be created by extending this class and implementing
    :py:meth:`run` function responsible for filling the buffer with data.
    This function will be started by pathway engine in a separate thread. When
    the :py:meth:`run` function terminates, the connector will be considered finished
    and pathway won't wait for new messages from it.

    In order to send a message :py:meth:`next` method can be used.

    If the subject won't delete records, set the class property ``deletions_enabled``
    to ``False`` as it may help to improve the performance.
    """

    _buffer: Queue
    _thread: threading.Thread | None
    _exception: BaseException | None
    _already_used: bool
    _pw_format: str
    _datasource_name: str

    def __init__(self, datasource_name: str = "python") -> None:
        self._buffer = Queue()
        self._thread = None
        self._exception = None
        self._already_used = False
        self._started = False
        self._datasource_name = datasource_name

    @abstractmethod
    def run(self) -> None: ...

    @final
    def seek(self, state: bytes) -> None:
        """Called by Rust core on start to resume reading from the last stopping point."""
        assert not self._started
        self._seek(state)

    def _seek(self, state: bytes) -> None:
        """Connector-dependent logic to perform seek up to specific state."""
        pass

    def on_persisted_run(self) -> None:
        """
        This method is called by Rust core to notify that the state will be persisted in this run.
        """
        pass

    def on_stop(self) -> None:
        """Called after the end of the :py:meth:`run` function."""
        pass

    def _is_finite(self) -> bool:
        """
        Denotes if the connector teminates after the code inside run() routine
        is completed.
        """
        return True

    def next(self, **kwargs) -> None:
        """Sends a message to the engine.

        The arguments should be compatible with the schema passed to :py:func:`~pathway.io.python.read`.
        Values for all fields should be passed to this method unless they have a default value
        specified in the schema.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>>
        >>> class InputSchema(pw.Schema):
        ...     a: pw.DateTimeNaive
        ...     b: bytes
        ...     c: int
        ...
        >>> class InputSubject(pw.io.python.ConnectorSubject):
        ...     def run(self):
        ...         self.next(a=pd.Timestamp("2021-03-21T18:34:12"), b="abc".encode(), c=3)
        ...         self.next(a=pd.Timestamp("2022-04-01T11:12:12"), b="def".encode(), c=42)
        ...
        >>> t = pw.io.python.read(InputSubject(), schema=InputSchema)
        >>> pw.debug.compute_and_print(t, include_id=False)
        a                   | b      | c
        2021-03-21 18:34:12 | b'abc' | 3
        2022-04-01 11:12:12 | b'def' | 42
        """
        self._add_inner(None, kwargs)

    def next_json(self, message: dict) -> None:
        """Sends a message.

        Args:
            message: Dict representing json.
        """
        if self._pw_format != "json":
            warnings.warn(
                f"Using `next_json` when format is {self._pw_format!r} is deprecated. Please use `next` instead.",
                DeprecationWarning,
            )
        self._add(
            None, json.dumps(message, ensure_ascii=False).encode(encoding="utf-8")
        )

    def next_str(self, message: str) -> None:
        """Sends a message.

        Args:
            message: a message represented as a string.
        """
        if self._pw_format != "raw":
            warnings.warn(
                f"Using `next_str` when format is {self._pw_format!r} is deprecated. Please use `next` instead.",
                DeprecationWarning,
            )
        self._add(None, message.encode(encoding="utf-8"))

    def next_bytes(self, message: bytes) -> None:
        """Sends a message.

        Args:
            message: a message represented as bytes.
        """
        if self._pw_format != "binary":
            warnings.warn(
                f"Using `next_bytes` when format is {self._pw_format!r} is deprecated. Please use `next` instead.",
                DeprecationWarning,
            )
        self._add(None, message)

    def commit(self) -> None:
        """Sends a commit message."""
        self._send_special_message(COMMIT_LITERAL)

    def _enable_commits(self) -> None:
        """Enables autocommits."""
        self._send_special_message(ENABLE_COMMITS_LITERAL)

    def _disable_commits(self) -> None:
        """Disables autocommits. Useful if you want to have a few messages in the same batch."""
        self._send_special_message(DISABLE_COMMITS_LITERAL)

    def _report_offset(self, offset) -> None:
        self._buffer.put(
            (
                PythonConnectorEventType.EXTERNAL_OFFSET,
                None,
                {PW_SPECIAL_OFFSET_KEY: offset},
            )
        )

    def close(self) -> None:
        """Sends a sentinel message.

        Should be called to indicate that no new messages will be sent.
        """
        self._send_special_message(FINISH_LITERAL)

    def _send_special_message(self, msg: str) -> None:
        self._buffer.put((PythonConnectorEventType.INSERT, None, {"_pw_special": msg}))

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

        self._started = True
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
        self._add_inner(key, self._get_values_dict(message, metadata))

    def _get_values_dict(
        self, message: bytes, metadata: bytes | None
    ) -> dict[str, Any]:
        match self._pw_format:
            case "json":
                values = json.loads(message.decode(encoding="utf-8"))
            case "raw":
                values = {"data": message.decode(encoding="utf-8")}
            case _:
                assert self._pw_format == "binary"
                values = {"data": message}
        if metadata is not None:
            values["_metadata"] = json.loads(metadata.decode(encoding="utf-8"))
        return values

    def _add_inner(self, key: Pointer | None, values: dict[str, Any]) -> None:
        if self._session_type == SessionType.NATIVE:
            self._buffer.put((PythonConnectorEventType.INSERT, key, values))
        elif self._session_type == SessionType.UPSERT:
            if not self._deletions_enabled:
                raise ValueError(
                    f"Trying to modify a row in {type(self)} but deletions_enabled is set to False."
                )
            self._buffer.put((PythonConnectorEventType.INSERT, key, values))
        else:
            raise NotImplementedError(f"session type {self._session_type} not handled")

    def _remove(
        self, key: Pointer, message: bytes, metadata: bytes | None = None
    ) -> None:
        self._remove_inner(key, self._get_values_dict(message, metadata))

    def _remove_inner(self, key: Pointer | None, values: dict[str, Any]) -> None:
        if not self._deletions_enabled:
            raise ValueError(
                f"Trying to delete a row in {type(self)} but deletions_enabled is set to False."
            )
        self._buffer.put((PythonConnectorEventType.DELETE, key, values))

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
        we need to tell the engine not to require name from such connectors
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


def _create_python_datasource(
    subject: ConnectorSubject,
    *,
    schema: type[Schema],
    autocommit_duration_ms: int | None = 1500,
    name: str | None = None,
    _stacklevel: int = 1,
    **kwargs,
) -> datasource.GenericDataSource:
    schema, api_schema = read_schema(schema)
    data_format = api.DataFormat(
        **api_schema,
        format_type="transparent",
        session_type=subject._session_type,
    )
    data_storage = api.DataStorage(
        storage_type="python",
        python_subject=api.PythonSubject(
            start=subject.start,
            seek=subject.seek,
            on_persisted_run=subject.on_persisted_run,
            read=subject._read,
            end=subject.end,
            is_internal=subject._is_internal(),
            deletions_enabled=subject._deletions_enabled,
        ),
    )
    data_source_options = datasource.DataSourceOptions(
        commit_duration_ms=autocommit_duration_ms,
        unique_name=_get_unique_name(name, kwargs, stacklevel=_stacklevel + 1),
    )
    return datasource.GenericDataSource(
        datastorage=data_storage,
        dataformat=data_format,
        data_source_options=data_source_options,
        schema=schema,
        datasource_name=subject._datasource_name,
        append_only=not subject._deletions_enabled,
    )


@check_arg_types
@trace_user_frame
def read(
    subject: ConnectorSubject,
    *,
    schema: type[Schema] | None = None,
    format: Literal["json", "raw", "binary"] | None = None,
    autocommit_duration_ms: int | None = 1500,
    debug_data=None,
    name: str | None = None,
    _stacklevel: int = 1,
    **kwargs,
) -> Table:
    """Reads a table from a ConnectorSubject.

    Args:
        subject: An instance of a :py:class:`~pathway.python.ConnectorSubject`.
        schema: Schema of the resulting table.
        format: Deprecated. Pass values of proper types to ``subject``'s ``next`` instead.
            Format of the data produced by a subject, "json", "raw" or "binary". In case of
            a "raw"/"binary" format, table with single "data" column will be produced.
        debug_data: Static data replacing original one when debug mode is active.
        autocommit_duration_ms: the maximum time between two commits. Every
            autocommit_duration_ms milliseconds, the updates received by the connector are
            committed and pushed into Pathway's computation graph
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the connector's progress.

    Returns:
        Table: The table read.

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
    ...             self.next(a=i, b=f"x{i}")
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

    if format is None:
        format = "json"  # previous default
    else:
        warnings.warn(
            "Setting `format` in Python connector is deprecated."
            + " Please just pass values of proper types to the `next` method.",
            DeprecationWarning,
        )

    if subject._already_used:
        raise ValueError(
            "You can't use the same ConnectorSubject object in more than one Python connector."
            + "If you want to use the same ConnectorSubject twice, create two separate objects of this class."
        )
    subject._already_used = True
    subject._pw_format = format

    data_format_type = get_data_format_type(format, SUPPORTED_INPUT_FORMATS)

    if data_format_type == "identity":
        if format == "binary":
            schema = RawDataSchema
        else:
            schema = PlaintextDataSchema
        if subject._with_metadata is True:
            schema |= MetadataSchema

    schema = assert_schema_not_none(schema, data_format_type)
    return table_from_datasource(
        _create_python_datasource(
            subject,
            schema=schema,
            autocommit_duration_ms=autocommit_duration_ms,
            name=name,
            _stacklevel=_stacklevel + 5,
            **kwargs,
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


class ConnectorObserver(ABC):
    """An abstract class for creating custom Python writers.
    At least `on_change` method must be implemented.
    Use with :py:func:`~pathway.io.python.write`.
    """

    @abstractmethod
    def on_change(
        self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
    ) -> None:
        """
        Called on every change in the table. It is called on table entries in order of increasing processing
        time. For entries with the same processing time (the same batch) the method can be called in any
        order. The function must accept:

        Args:
            key: the key of the changed row;
            row: the changed row as a dict mapping from the field name to the value;
            time: the processing time of the modification, also can be referred as minibatch ID of the change;
            is_addition: boolean value, equals to true if the row is inserted into the
                table, false otherwise. Please note that update is basically two operations: the
                deletion of the old value and the insertion of a new value, which happen within a single
                batch;
        """
        ...

    def on_time_end(self, time: int) -> None:
        """
        Called when a processing time is closed.

        Args:
            time: The finished processing time.
        """
        pass

    def on_end(self) -> None:
        """
        Called when the stream of changes ends.
        """
        pass


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    observer: ConnectorObserver,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
):
    """Writes stream of changes from a table to a Python observer.

    Args:
        table: The table to write.
        observer: An instance of a :py:class:`~pathway.io.python.ConnectorObserver`.
        name: A unique name for the writer. If provided, this name will be used in
            logs and monitoring dashboards. Additionally, if persistence is enabled, it
            will be used as the name for the snapshot that stores the writer's progress.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Example:

    >>> from pathway.tests import utils  # NODOCS
    >>> utils.skip_on_multiple_workers()  # NODOCS
    >>> import pathway as pw
    ...
    >>> table = pw.debug.table_from_markdown('''
    ...      | pet  | owner   | age | __time__ | __diff__
    ...    1 | dog  | Alice   | 10  | 0        | 1
    ...    2 | cat  | Alice   | 8   | 2        | 1
    ...    3 | dog  | Bob     | 7   | 4        | 1
    ...    2 | cat  | Alice   | 8   | 6        | -1
    ... ''')
    ...
    >>> class Observer(pw.io.python.ConnectorObserver):
    ...     def on_change(self, key: pw.Pointer, row: dict, time: int, is_addition: bool):
    ...         print(f"{row}, {time}, {is_addition}")
    ...
    ...     def on_end(self):
    ...         print("End of stream.")
    ...
    >>> pw.io.python.write(table, Observer())
    >>> pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    {'pet': 'dog', 'owner': 'Alice', 'age': 10}, 0, True
    {'pet': 'cat', 'owner': 'Alice', 'age': 8}, 2, True
    {'pet': 'dog', 'owner': 'Bob', 'age': 7}, 4, True
    {'pet': 'cat', 'owner': 'Alice', 'age': 8}, 6, False
    End of stream.
    """

    subscribe(
        table,
        skip_persisted_batch=True,
        on_change=observer.on_change,
        on_time_end=observer.on_time_end,
        on_end=observer.on_end,
        name=name,
        sort_by=sort_by,
    )

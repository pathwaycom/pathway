# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import collections
import functools
import inspect
import logging
import re
from abc import ABCMeta, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar

import pathway.internals as pw
import pathway.internals.column as clmn
import pathway.internals.dtype as dt
import pathway.io as io
from pathway.internals import api, datasource, udfs
from pathway.internals.api import Pointer, SessionType
from pathway.internals.schema import Schema, schema_from_types
from pathway.internals.type_interpreter import eval_type


class _AsyncStatus(Enum):
    PENDING = "-PENDING-"
    FAILURE = "-FAILURE-"
    SUCCESS = "-SUCCESS-"


_ASYNC_STATUS_COLUMN = "_async_status"
_AsyncStatusSchema = schema_from_types(**{_ASYNC_STATUS_COLUMN: dt.Future(dt.STR)})
_INSTANCE_COLUMN = "_pw_instance"


@dataclass(frozen=True)
class _Entry:
    key: Pointer
    time: int
    is_addition: bool
    task_id: Pointer


ResultType = dict[str, api.Value] | _AsyncStatus | None


@dataclass
class _Instance:
    pending: collections.deque[_Entry] = field(default_factory=collections.deque)
    finished: dict[_Entry, ResultType] = field(default_factory=dict)
    buffer: list[tuple[Pointer, bool, Pointer, ResultType]] = field(
        default_factory=list
    )
    buffer_time: int | None = None
    correct: bool = True


class _AsyncConnector(io.python.ConnectorSubject):
    _requests: asyncio.Queue
    _apply: Callable
    _loop: asyncio.AbstractEventLoop
    _transformer: _BaseAsyncTransformer
    _tasks: dict[Pointer, asyncio.Task]
    _invoke: Callable[..., Awaitable[dict[str, Any]]]
    _instances: dict[api.Value, _Instance]
    _time_finished: int | None
    _logger: logging.Logger

    def __init__(self, transformer: _BaseAsyncTransformer) -> None:
        super().__init__(datasource_name="async-transformer")
        self._transformer = transformer
        self._event_loop = asyncio.new_event_loop()
        self._logger = logging.getLogger(__name__)
        self.set_options()

    def set_options(
        self,
        capacity: int | None = None,
        timeout: float | None = None,
        retry_strategy: udfs.AsyncRetryStrategy | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
    ):
        self._invoke = udfs.async_options(
            capacity=capacity,
            timeout=timeout,
            retry_strategy=retry_strategy,
            cache_strategy=cache_strategy,
        )(self._transformer.invoke)

    def run(self) -> None:
        self._tasks = {}
        self._transformer.open()
        self._instances = collections.defaultdict(_Instance)
        self._time_finished = None

        async def loop_forever(event_loop: asyncio.AbstractEventLoop):
            self._maybe_create_queue()

            while True:
                request = await self._requests.get()
                if request == "*FINISH*":
                    break
                if isinstance(request, int):  # time end
                    self._on_time_end(request)
                    continue

                (key, values, time, diff) = request
                row = {}
                input_table = self._transformer._input_table
                task_id = values[-1]
                values = values[:-1]
                if input_table is not None:
                    for field_name, field_value in zip(
                        input_table._columns.keys(), values, strict=True
                    ):
                        row[field_name] = field_value
                else:
                    for i, field_value in enumerate(values):
                        row[f"{i}"] = field_value

                assert diff in [-1, 1], "diff should be 1 or -1"
                addition = diff == 1
                instance = row.get(_INSTANCE_COLUMN, key)
                entry = _Entry(
                    key=key, time=time, is_addition=addition, task_id=task_id
                )
                self._instances[instance].pending.append(entry)

                previous_task = self._tasks.get(key, None)

                async def task(
                    key: Pointer,
                    values: dict[str, Any],
                    time: int,
                    addition: bool,
                    task_id: Pointer,
                    previous_task: asyncio.Task | None,
                ):
                    instance = values.pop(_INSTANCE_COLUMN, key)
                    if not addition:
                        if previous_task is not None:
                            await previous_task
                        self._on_task_finished(
                            key,
                            instance,
                            time,
                            is_addition=False,
                            result=None,
                            task_id=task_id,
                        )
                    else:
                        result: dict[str, Any] | _AsyncStatus
                        try:
                            result = await self._invoke(**values)
                            self._check_result_against_schema(result)
                        except Exception:
                            self._logger.error(
                                "Exception in AsyncTransformer:", exc_info=True
                            )
                            result = _AsyncStatus.FAILURE
                        # If there is a task pending for this key,
                        # let's wait for it and discard result to preserve order
                        # for this key (the instance may change)
                        if previous_task is not None:
                            await previous_task
                        self._on_task_finished(
                            key,
                            instance,
                            time,
                            is_addition=True,
                            result=result,
                            task_id=task_id,
                        )

                current_task = event_loop.create_task(
                    task(key, row, time, addition, task_id, previous_task)
                )
                self._tasks[key] = current_task

            await asyncio.gather(*self._tasks.values())

        self._event_loop.run_until_complete(loop_forever(self._event_loop))

    def _on_time_end(self, time: int) -> None:
        self._time_finished = time
        self.commit()
        instances = list(self._instances)
        # it creates a separate list for iteration because _maybe_produce_instance
        # can remove entries from self._instances
        for instance in instances:
            self._maybe_produce_instance(instance)

    def _on_task_finished(
        self,
        key: Pointer,
        instance: api.Value,
        time: int,
        *,
        is_addition: bool,
        result: dict[str, Any] | _AsyncStatus | None,
        task_id: Pointer,
    ) -> None:
        instance_data = self._instances[instance]
        entry = _Entry(key=key, time=time, is_addition=is_addition, task_id=task_id)
        instance_data.finished[entry] = result
        self._maybe_produce_instance(instance)

    def _maybe_produce_instance(self, instance: api.Value) -> None:
        instance_data = self._instances[instance]
        while instance_data.pending:
            entry = instance_data.pending[0]
            if (
                self._time_finished is None
                or entry.time > self._time_finished
                or entry not in instance_data.finished
            ):
                break
            if instance_data.buffer_time != entry.time:
                assert (
                    instance_data.buffer_time is None
                    or instance_data.buffer_time < entry.time
                )
                self._flush_buffer(instance_data)
                instance_data.buffer_time = entry.time
            result = instance_data.finished.pop(entry)
            if result == _AsyncStatus.FAILURE:
                instance_data.correct = False
            instance_data.buffer.append(
                (entry.key, entry.is_addition, entry.task_id, result)
            )
            instance_data.pending.popleft()

        if (
            not instance_data.pending
            or instance_data.pending[0].time != instance_data.buffer_time
        ):  # if (instance, processing_time) pair is finished
            self._flush_buffer(instance_data)
        if not instance_data.pending:
            del self._instances[instance]

    def _flush_buffer(self, instance_data: _Instance) -> None:
        if not instance_data.buffer:
            return
        self.commit()
        self._disable_commits()
        for key, is_addition, task_id, result in instance_data.buffer:
            if is_addition and instance_data.correct:
                assert isinstance(result, dict)
                self._upsert(key, result, task_id)
            elif is_addition:
                self._set_failure(key, task_id)
            else:
                self._remove_by_key(key, task_id)
        self._enable_commits()  # does a commit as well
        instance_data.buffer.clear()

    def _set_failure(self, key: Pointer, task_id: Pointer) -> None:
        # TODO: replace None with api.ERROR
        data = {col: None for col in self._transformer.output_schema.column_names()}
        self._upsert(key, data, task_id, _AsyncStatus.FAILURE)

    def _upsert(
        self, key: Pointer, data: dict, task_id: Pointer, status=_AsyncStatus.SUCCESS
    ) -> None:
        data = {**data, _ASYNC_STATUS_COLUMN: status.value}
        self._add_inner(task_id, data)

    def _remove_by_key(self, key: Pointer, task_id: Pointer) -> None:
        self._remove_inner(task_id, {})

    def _check_result_against_schema(self, result: dict) -> None:
        if result.keys() != self._transformer.output_schema.keys():
            raise ValueError("result of async function does not match output schema")

    def on_stop(self) -> None:
        self._transformer.close()

    def on_subscribe_change(
        self, key: Pointer, row: list[Any], time: int, is_addition: bool
    ) -> None:
        self._put_request((key, row, time, is_addition))

    def on_subscribe_time_end(self, time: int) -> None:
        self._put_request(time)

    def on_subscribe_end(self) -> None:
        self._put_request("*FINISH*")

    def _put_request(self, message) -> None:
        def put_message(message):
            self._maybe_create_queue()
            self._requests.put_nowait(message)

        self._event_loop.call_soon_threadsafe(put_message, message)

    def _maybe_create_queue(self) -> None:
        if not hasattr(self, "_requests"):
            self._requests = asyncio.Queue()

    def _is_internal(self) -> bool:
        return True

    @property
    def _session_type(self) -> SessionType:
        return SessionType.UPSERT


class _BaseAsyncTransformer(metaclass=ABCMeta):
    output_schema: ClassVar[type[pw.Schema]]
    wrapped_output_schema: ClassVar[type[pw.Schema]]
    _connector: _AsyncConnector
    _autocommit_duration_ms: int | None
    _input_table: pw.Table | None

    def __init__(self, autocommit_duration_ms: int | None = 1500) -> None:
        assert self.output_schema is not None
        self._connector = _AsyncConnector(self)
        self._autocommit_duration_ms = autocommit_duration_ms
        self._input_table = None

    def __init_subclass__(
        cls, /, output_schema: type[pw.Schema] | None = None, **kwargs
    ):
        super().__init_subclass__(**kwargs)
        if output_schema is None:
            return
        cls.output_schema = output_schema
        cls.wrapped_output_schema = (
            output_schema.with_types(
                **{
                    key: dt.Future(dt.Optional(orig_dtype))
                    for key, orig_dtype in output_schema._dtypes().items()
                }
            )
            | _AsyncStatusSchema
        )

    def _get_datasource(self) -> datasource.GenericDataSource:
        return io.python._create_python_datasource(
            self._connector,
            schema=self.wrapped_output_schema,
            autocommit_duration_ms=self._autocommit_duration_ms,
        )

    def open(self) -> None:
        """
        Called before actual work. Suitable for one time setup.
        """
        pass

    def close(self) -> None:
        """
        Called once at the end. Proper place for cleanup.
        """
        pass

    @abstractmethod
    async def invoke(self, *args, **kwargs) -> dict[str, Any]:
        """
        Called for every row of input_table. The arguments will correspond to the
        columns in the input table.

        Should return dict of values matching :py:attr:`output_schema`.
        """
        ...


class AsyncTransformer(_BaseAsyncTransformer, metaclass=ABCMeta):
    """
    Allows to perform async transformations on a table.

    :py:meth:`invoke` will be called asynchronously for each row of an input_table.

    Output table can be acccesed via :py:attr:`successful`.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> class OutputSchema(pw.Schema):
    ...    ret: int
    ...
    >>> class AsyncIncrementTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
    ...     async def invoke(self, value) -> dict[str, Any]:
    ...         await asyncio.sleep(0.1)
    ...         return {"ret": value + 1 }
    ...
    >>> input = pw.debug.table_from_markdown('''
    ...   | value
    ... 1 | 42
    ... 2 | 44
    ... ''')
    >>> result = AsyncIncrementTransformer(input_table=input).successful
    >>> pw.debug.compute_and_print(result, include_id=False)
    ret
    43
    45
    """

    _input_table: pw.Table
    _instance_expression: pw.ColumnExpression | api.Value

    def __init__(
        self,
        input_table: pw.Table,
        *,
        instance: pw.ColumnExpression | api.Value = pw.this.id,
        autocommit_duration_ms: int | None = 1500,
    ) -> None:
        super().__init__(autocommit_duration_ms=autocommit_duration_ms)

        # TODO: when AsyncTransformer uses persistence backend for cache
        # just take the settings for persistence config
        # Use DefaultCache for now as the only available option
        self._connector.set_options(cache_strategy=udfs.DefaultCache())

        sig = inspect.signature(self.invoke)
        self._check_signature_matches_schema(sig, input_table.schema)

        input_table = input_table.with_columns(**{_INSTANCE_COLUMN: instance})
        instance_dtype = eval_type(input_table[_INSTANCE_COLUMN])
        if not dt.is_hashable_in_python(instance_dtype):
            raise ValueError(
                f"You can't use a column of type {instance_dtype} as instance in"
                + " AsyncTransformer because it is unhashable."
            )

        self._input_table = input_table

    def _check_signature_matches_schema(
        self, sig: inspect.Signature, schema: type[Schema]
    ) -> None:
        try:
            sig.bind(**schema.columns())
        except TypeError as e:
            msg = str(e)
            if match := re.match("got an unexpected keyword argument '(.+)'", msg):
                column = match[1]
                raise TypeError(
                    f"Input table has a column {column!r} but it is not present"
                    + " on the argument list of the invoke method."
                )
            elif match := re.match("missing a required argument: '(.+)'", msg):
                arg_name = match[1]
                raise TypeError(
                    f"Column {arg_name!r} is present on the argument list of the invoke"
                    + " method but it is not present in the input_table."
                )
            raise e

    def __init_subclass__(cls, /, output_schema: type[pw.Schema], **kwargs):
        super().__init_subclass__(output_schema, **kwargs)

    def with_options(
        self,
        capacity: int | None = None,
        timeout: float | None = None,
        retry_strategy: udfs.AsyncRetryStrategy | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
    ) -> AsyncTransformer:
        """
        Sets async options.

        Args:
            capacity: Maximum number of concurrent operations.
                Defaults to None, indicating no specific limit.
            timeout: Maximum time (in seconds) to wait for the function result.
                Defaults to None, indicating no time limit.
            retry_strategy: Strategy for handling retries in case of failures.
                Defaults to None, meaning no retries.
            cache_strategy: Defines the caching mechanism. If set to None
                and a persistency is enabled, operations will be cached using the
                persistence layer. Defaults to None.
        Returns:
            self
        """
        self._connector.set_options(capacity, timeout, retry_strategy, cache_strategy)
        return self

    @functools.cached_property
    def successful(self) -> pw.Table:
        """
        The resulting table containing only rows that were executed successfully.
        """
        return (
            self.finished.filter(
                pw.this[_ASYNC_STATUS_COLUMN] == _AsyncStatus.SUCCESS.value
            )
            .without(pw.this[_ASYNC_STATUS_COLUMN])
            .update_types(**self.output_schema.typehints())
        )

    @functools.cached_property
    def failed(self) -> pw.Table:
        """
        The resulting table containing only rows that failed during execution.
        If the ``instance`` argument is specified, it also contains rows that were executed
        successfully but at least one element from their instance with less or equal time failed.
        """
        return self.finished.filter(
            pw.this[_ASYNC_STATUS_COLUMN] == _AsyncStatus.FAILURE.value
        ).without(pw.this[_ASYNC_STATUS_COLUMN])

    @functools.cached_property
    def finished(self) -> pw.Table:
        """
        The resulting table containing all rows that finished their execution.
        The column ``_async_status`` contains the state of the row. The rows that
        finished successfully, have their status set to "-SUCCESS-". The rows that failed,
        have their status set to "-FAILURE-".
        If the ``instance`` argument is specified, rows that were executed successfully
        but at least one element from their instance with less or equal time failed,
        have their status set as "-FAILURE-".

        If you want to get only rows that executed successfully, use ``successful`` property instead.
        """
        return self.output_table.await_futures()

    @functools.cached_property
    def output_table(self) -> pw.Table:
        """
        The resulting table containing all rows that started their execution.
        The column ``_async_status`` contains the state of the row. The rows that
        finished successfully, have their status set to "-SUCCESS-". The rows that failed,
        have their status set to "-FAILURE-". The rows that are still being executed,
        have their state set to "-PENDING-".

        It is recommended to use this property for debugging/presentational purposes only.
        For other purposes, ``successful`` property should be preferred. It returns
        a Table containing only rows that were executed successfully.
        """

        input_id_column = self._input_table._id_column
        input_columns = list(self._input_table._columns.values())

        context = clmn.AsyncTransformerContext(
            input_id_column,
            input_columns,
            self.wrapped_output_schema,
            self._connector.on_subscribe_change,
            self._connector.on_subscribe_time_end,
            self._connector.on_subscribe_end,
            self._get_datasource(),
        )
        return self._input_table._async_transformer(context)

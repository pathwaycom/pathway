# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import collections
import functools
import inspect
import json
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar
from warnings import warn

import pathway.internals as pw
import pathway.internals.dtype as dt
import pathway.io as io
from pathway.internals import api, operator, parse_graph, udfs
from pathway.internals.api import Pointer
from pathway.internals.helpers import StableSet
from pathway.internals.operator import Operator
from pathway.internals.schema import Schema, schema_from_types
from pathway.internals.table_subscription import subscribe
from pathway.internals.type_interpreter import eval_type


class _AsyncStatus(Enum):
    PENDING = "-PENDING-"
    FAILURE = "-FAILURE-"
    SUCCESS = "-SUCCESS-"


_ASYNC_STATUS_COLUMN = "_async_status"
_AsyncStatusSchema = schema_from_types(**{_ASYNC_STATUS_COLUMN: str})
_INSTANCE_COLUMN = "_pw_instance"


@dataclass(frozen=True)
class _Entry:
    key: Pointer
    time: int
    is_addition: bool


ResultType = dict[str, api.Value] | _AsyncStatus | None


@dataclass
class _Instance:
    pending: collections.deque[_Entry] = field(default_factory=collections.deque)
    finished: dict[_Entry, ResultType] = field(default_factory=dict)
    buffer: list[tuple[Pointer, bool, ResultType]] = field(default_factory=list)
    buffer_time: int | None = None
    correct: bool = True


class _AsyncConnector(io.python.ConnectorSubject):
    _requests: asyncio.Queue
    _apply: Callable
    _loop: asyncio.AbstractEventLoop
    _transformer: AsyncTransformer
    _state: dict[Pointer, Any]
    _tasks: dict[Pointer, asyncio.Task]
    _invoke: Callable[..., Awaitable[dict[str, Any]]]
    _instances: dict[api.Value, _Instance]
    _time_finished: int | None
    _logger: logging.Logger

    def __init__(self, transformer: AsyncTransformer) -> None:
        super().__init__()
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
        self._state = {}
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

                (key, values, time, addition) = request
                instance = values[_INSTANCE_COLUMN]
                entry = _Entry(key=key, time=time, is_addition=addition)
                self._instances[instance].pending.append(entry)

                previous_task = self._tasks.get(key, None)
                if previous_task is None:
                    self._set_status(key, _AsyncStatus.PENDING)

                async def task(
                    key: Pointer,
                    values: dict[str, Any],
                    time: int,
                    addition: bool,
                    previous_task: asyncio.Task | None,
                ):
                    instance = values.pop(_INSTANCE_COLUMN)
                    if not addition:
                        if previous_task is not None:
                            await previous_task
                        self._on_task_finished(
                            key, instance, time, is_addition=False, result=None
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
                            key, instance, time, is_addition=True, result=result
                        )

                current_task = event_loop.create_task(
                    task(key, values, time, addition, previous_task)
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
    ) -> None:
        instance_data = self._instances[instance]
        entry = _Entry(key=key, time=time, is_addition=is_addition)
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
            instance_data.buffer.append((entry.key, entry.is_addition, result))
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
        self._disable_commits()
        for key, is_addition, result in instance_data.buffer:
            if is_addition and instance_data.correct:
                assert isinstance(result, dict)
                self._upsert(key, result)
            elif is_addition:
                self._set_status(key, _AsyncStatus.FAILURE)
            else:
                self._remove_by_key(key)
        self._enable_commits()  # does a commit as well
        instance_data.buffer.clear()

    def _set_status(self, key: Pointer, status: _AsyncStatus) -> None:
        data = {col: None for col in self._transformer.output_schema.column_names()}
        self._upsert(key, data, status)

    def _upsert(self, key: Pointer, data: dict, status=_AsyncStatus.SUCCESS) -> None:
        data = {**data, _ASYNC_STATUS_COLUMN: status.value}
        payload = json.dumps(data).encode()
        self._remove_by_key(key)
        self._add(key, payload)
        self._state[key] = data

    def _remove_by_key(self, key) -> None:
        if key in self._state:
            payload = json.dumps(self._state[key]).encode()
            self._remove(key, payload)
            del self._state[key]

    def _check_result_against_schema(self, result: dict) -> None:
        if result.keys() != self._transformer.output_schema.keys():
            raise ValueError("result of async function does not match output schema")

    def on_stop(self) -> None:
        self._transformer.close()

    def on_subscribe_change(
        self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
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


class AsyncTransformer(ABC):
    """
    Allows to perform async transformations on a table.

    :py:meth:`invoke` will be called asynchronously for each row of an input_table.

    Output table can be acccesed via :py:attr:`result`.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> class OutputSchema(pw.Schema):
    ...    ret: int
    ...
    >>> class AsyncIncrementTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
    ...     async def invoke(self, value) -> Dict[str, Any]:
    ...         await asyncio.sleep(0.1)
    ...         return {"ret": value + 1 }
    ...
    >>> input = pw.debug.table_from_markdown('''
    ...   | value
    ... 1 | 42
    ... 2 | 44
    ... ''')
    >>> result = AsyncIncrementTransformer(input_table=input).result
    >>> pw.debug.compute_and_print(result, include_id=False)
    ret
    43
    45
    """

    output_schema: ClassVar[type[pw.Schema]]
    _connector: _AsyncConnector
    _input_table: pw.Table
    _instance_expression: pw.ColumnExpression | api.Value

    def __init__(
        self,
        input_table: pw.Table,
        *,
        instance: pw.ColumnExpression | api.Value = pw.this.id,
        autocommit_duration_ms: int | None = 1500,
    ) -> None:
        assert self.output_schema is not None
        self._connector = _AsyncConnector(self)

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
        self._autocommit_duration_ms = autocommit_duration_ms

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
        super().__init_subclass__(**kwargs)
        cls.output_schema = output_schema

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

    @property
    def result(self) -> pw.Table:
        """
        The resulting table containing only rows that were executed successfully.

        Deprecated. Use ``successful`` instead.
        """
        warn(
            'The "result" property of AsyncTransformer is deprecated. Use "successful" instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        return self.successful

    @functools.cached_property
    def successful(self) -> pw.Table:
        """
        The resulting table containing only rows that were executed successfully.
        """
        return (
            self.output_table.filter(
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
        return self.output_table.filter(
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
        return self.output_table.filter(
            pw.this[_ASYNC_STATUS_COLUMN] != _AsyncStatus.PENDING.value
        )

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

        subscribe(
            self._input_table,
            skip_persisted_batch=False,
            on_change=self._connector.on_subscribe_change,
            on_time_end=self._connector.on_subscribe_time_end,
            on_end=self._connector.on_subscribe_end,
        )
        output_node = list(parse_graph.G.global_scope.nodes)[-1]

        schema = self.output_schema.with_types(
            **{
                key: dt.Optional(orig_dtype)
                for key, orig_dtype in self.output_schema._dtypes().items()
            }
        )

        table: pw.Table = io.python.read(
            self._connector,
            schema=schema | _AsyncStatusSchema,
            name="async-transformer",
            autocommit_duration_ms=self._autocommit_duration_ms,
        )
        input_node = table._source.operator

        class AsyncInputHandle(operator.InputHandle):
            @property
            def dependencies(self) -> StableSet[Operator]:
                return StableSet([output_node])

        input_node._inputs = {
            "async_input": AsyncInputHandle(
                operator=input_node,
                name="async_input",
                value=self._input_table,
            )
        }

        return table.promise_universe_is_subset_of(self._input_table)

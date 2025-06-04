# Copyright © 2024 Pathway

from __future__ import annotations

import abc
import functools
from collections.abc import Callable
from typing import Any, overload
from warnings import warn

from pathway.internals import dtype as dt, expression as expr, udfs
from pathway.internals.helpers import with_optional_kwargs
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.shadows import inspect
from pathway.internals.udfs.caches import (
    CacheStrategy,
    DefaultCache,
    DiskCache,
    InMemoryCache,
    with_cache_strategy,
)
from pathway.internals.udfs.executors import (
    AutoExecutor,
    Executor,
    SyncExecutor,
    async_executor,
    async_options,
    auto_executor,
    fully_async_executor,
    sync_executor,
    with_capacity,
    with_timeout,
)
from pathway.internals.udfs.retries import (
    AsyncRetryStrategy,
    ExponentialBackoffRetryStrategy,
    FixedDelayRetryStrategy,
    NoRetryStrategy,
    with_retry_strategy,
)
from pathway.internals.udfs.utils import coerce_async

__all__ = [
    "udf",
    "UDF",
    "auto_executor",
    "async_executor",
    "sync_executor",
    "fully_async_executor",
    "CacheStrategy",
    "DefaultCache",
    "DiskCache",
    "InMemoryCache",
    "AsyncRetryStrategy",
    "ExponentialBackoffRetryStrategy",
    "FixedDelayRetryStrategy",
    "NoRetryStrategy",
    "async_options",
    "coerce_async",
    "with_cache_strategy",
    "with_capacity",
    "with_retry_strategy",
    "with_timeout",
]


class UDF(abc.ABC):
    """
    Base class for Pathway UDF (user-defined functions).

    Please use the wrapper ``udf`` to create UDFs out of Python functions.
    Please subclass this class to define UDFs using Python classes.
    When subclassing this class, please implement the ``__wrapped__`` function.

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ... a | b
    ... 1 | 2
    ... 3 | 4
    ... 5 | 6
    ... '''
    ... )
    >>>
    >>> class VerySophisticatedUDF(pw.UDF):
    ...     exponent: float
    ...     def __init__(self, exponent: float) -> None:
    ...         super().__init__()
    ...         self.exponent = exponent
    ...     def __wrapped__(self, a: int, b: int) -> float:
    ...         intermediate = (a * b) ** self.exponent
    ...         return round(intermediate, 2)
    ...
    >>> func = VerySophisticatedUDF(1.5)
    >>> res = table.select(result=func(table.a, table.b))
    >>> pw.debug.compute_and_print(res, include_id=False)
    result
    2.83
    41.57
    164.32
    """

    __wrapped__: Callable
    func: Callable
    return_type: Any
    deterministic: bool
    propagate_none: bool
    executor: Executor
    cache_strategy: CacheStrategy | None
    max_batch_size: int | None

    def __init__(
        self,
        *,
        return_type: Any = ...,
        deterministic: bool = False,
        propagate_none: bool = False,
        executor: Executor = AutoExecutor(),
        cache_strategy: CacheStrategy | None = None,
        max_batch_size: int | None = None,
    ) -> None:
        """
        Args:
            return_type: The return type of the function.
                Defaults to ``...``, meaning that the return type is inferred from the
                ``self.__wrapped__`` signature.
            deterministic: Whether the provided function is deterministic. In this context,
                it means that the function always returns the same value for the same arguments.
                If it is not deterministic, Pathway will memoize the results until the row deletion.
                If your function is deterministic, you're **strongly encouraged** to set it
                to True as it will improve the performance.
                Defaults to False, meaning that the function is not deterministic
                and its results will be kept.
            propagate_none: If True, the UDF won't be called if at least one of its
                arguments is None. Then None will be returned without calling the function.
                If False, the function is called also on None.
            executor: Defines the executor of the UDF. It determines if the execution is
                synchronous or asynchronous.
                Defaults to ``AutoExecutor()``, meaning that the execution strategy will be
                inferred from the function definition. By default, if the function is a coroutine,
                then it is executed asynchronously. Otherwise it is executed synchronously.
            cache_strategy: Defines the caching mechanism.
                Defaults to None.
            max_batch_size: If set, defines the maximal number of rows that can be passed
                to a UDF at once. Then each argument is a list of values and a UDF has to
                return a list with results with the same length as input lists. The result
                at position `i` has to be the result for input at position `i`.
        """
        self.return_type = return_type
        self.deterministic = deterministic
        self.propagate_none = propagate_none
        self.executor = self._prepare_executor(executor)
        self.cache_strategy = cache_strategy
        if not isinstance(self.executor, SyncExecutor) and max_batch_size is not None:
            raise ValueError(
                "Batching is currently supported only for synchronous UDFs."
            )
        self.max_batch_size = max_batch_size
        self.func = self._wrap_function()

    def _get_config(self) -> dict[str, Any]:
        return {
            "return_type": self.return_type,
            "deterministic": self.deterministic,
            "propagate_none": self.propagate_none,
            "executor": self.executor,
            "cache_strategy": self.cache_strategy,
        }

    def _get_return_type(self) -> Any:
        return_type = self.return_type
        if inspect.isclass(self.__wrapped__):
            sig_return_type: Any = self.__wrapped__
        else:
            try:
                sig_return_type = inspect.signature(self.__wrapped__).return_annotation
            except ValueError:
                sig_return_type = Any

        try:
            wrapped_sig_return_type = dt.wrap(sig_return_type)
        except TypeError:
            wrapped_sig_return_type = None

        if return_type is not ... and (
            wrapped_sig_return_type is None
            or (
                sig_return_type != Any
                and not dt.dtype_issubclass(
                    wrapped_sig_return_type, dt.wrap(return_type)
                )
            )
        ):
            warn(
                f"The value of return_type parameter ({return_type}) is inconsistent with"
                + f" UDF's return type annotation ({sig_return_type}).",
                stacklevel=3,
            )
        if return_type is ...:  # return type only specified in signature
            if self.max_batch_size is None:
                return sig_return_type
            else:
                if not isinstance(wrapped_sig_return_type, dt.List):
                    raise ValueError(
                        f"A batch UDF has to return a list but is annotated as returning {sig_return_type}"
                    )
                return wrapped_sig_return_type.wrapped

        return return_type

    def _wrap_function(self) -> Callable:
        func = self.executor._wrap(self.__wrapped__)
        if self.cache_strategy is not None:
            func = with_cache_strategy(func, self.cache_strategy)
        return func

    def _prepare_executor(self, executor: Executor) -> Executor:
        is_coroutine = inspect.iscoroutinefunction(self.__wrapped__)
        if is_coroutine and isinstance(executor, SyncExecutor):
            raise ValueError("The function is a coroutine. You can't use SyncExecutor.")
        if isinstance(executor, AutoExecutor):
            return async_executor() if is_coroutine else udfs.sync_executor()
        return executor

    def __call__(self, *args, **kwargs) -> expr.ColumnExpression:
        return self.executor._apply_expression_type(
            self.func,
            return_type=self._get_return_type(),
            propagate_none=self.propagate_none,
            deterministic=self.deterministic,
            max_batch_size=self.max_batch_size,
            **self.executor.additional_expression_args(),
            args=args,
            kwargs=kwargs,
        )


class UDFFunction(UDF):
    """Create a Python UDF (user-defined function) out of a callable.

    The output type of the UDF is determined based on its type annotation.

    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ... age | owner | pet
    ...  10 | Alice | dog
    ...   9 |   Bob | dog
    ...     | Alice | cat
    ...   7 |   Bob | dog
    ... '''
    ... )
    >>>
    >>> @pw.udf
    ... def concat(left: str, right: str) -> str:
    ...     return left + "-" + right
    ...
    >>> @pw.udf(propagate_none=True)
    ... def increment(age: int) -> int:
    ...     assert age is not None
    ...     return age + 1
    ...
    >>> res1 = table.select(
    ...     owner_with_pet=concat(table.owner, table.pet), new_age=increment(table.age)
    ... )
    >>> pw.debug.compute_and_print(res1, include_id=False)
    owner_with_pet | new_age
    Alice-cat      |
    Alice-dog      | 11
    Bob-dog        | 8
    Bob-dog        | 10
    >>>
    >>> @pw.udf
    ... async def sleeping_concat(left: str, right: str) -> str:
    ...     await asyncio.sleep(0.1)
    ...     return left + "-" + right
    ...
    >>> res2 = table.select(col=sleeping_concat(table.owner, table.pet))
    >>> pw.debug.compute_and_print(res2, include_id=False)
    col
    Alice-cat
    Alice-dog
    Bob-dog
    Bob-dog
    """

    def __init__(self, func: Callable, **kwargs):
        # this sets __wrapped__
        functools.update_wrapper(self, func)
        super().__init__(**kwargs)


@overload
def udf(
    *,
    return_type: Any = None,
    deterministic: bool = False,
    propagate_none: bool = False,
    executor: Executor = AutoExecutor(),
    cache_strategy: CacheStrategy | None = None,
    max_batch_size: int | None = None,
) -> Callable[[Callable], UDF]: ...


@overload
def udf(
    fun: Callable,
    /,
    *,
    return_type: Any = None,
    deterministic: bool = False,
    propagate_none: bool = False,
    executor: Executor = AutoExecutor(),
    cache_strategy: CacheStrategy | None = None,
    max_batch_size: int | None = None,
) -> UDF: ...


@with_optional_kwargs
@check_arg_types
def udf(
    fun: Callable,
    /,
    *,
    return_type: Any = ...,
    deterministic: bool = False,
    propagate_none: bool = False,
    executor: Executor = AutoExecutor(),
    cache_strategy: CacheStrategy | None = None,
    max_batch_size: int | None = None,
):
    """Create a Python UDF (user-defined function) out of a callable.

    Output column type deduced from type-annotations of a function.
    Can be applied to a regular or asynchronous function.

    Args:
        return_type: The return type of the function. Can be passed here or as a return
            type annotation.
            Defaults to ``...``, meaning that the return type will be inferred from type annotation.
        deterministic: Whether the provided function is deterministic. In this context,
            it means that the function always returns the same value for the same arguments.
            If it is not deterministic, Pathway will memoize the results until the row deletion.
            If your function is deterministic, you're **strongly encouraged** to set it
            to True as it will improve the performance.
            Defaults to False, meaning that the function is not deterministic
            and its results will be kept.
        executor: Defines the executor of the UDF. It determines if the execution is
            synchronous or asynchronous.
            Defaults to AutoExecutor(), meaning that the execution strategy will be
            inferred from the function annotation. By default, if the function is a coroutine,
            then it is executed asynchronously. Otherwise it is executed synchronously.
        cache_strategy: Defines the caching mechanism.
            Defaults to None.
        max_batch_size: If set, defines the maximal number of rows that can be passed
            to a UDF at once. Then each argument is a list of values and a UDF has to
            return a list with results with the same length as input lists. The result
            at position `i` has to be the result for input at position `i`.
    Example:

    >>> import pathway as pw
    >>> import asyncio
    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ... age | owner | pet
    ...  10 | Alice | dog
    ...   9 |   Bob | dog
    ...     | Alice | cat
    ...   7 |   Bob | dog
    ... '''
    ... )
    >>>
    >>> @pw.udf
    ... def concat(left: str, right: str) -> str:
    ...     return left + "-" + right
    ...
    >>> @pw.udf(propagate_none=True)
    ... def increment(age: int) -> int:
    ...     assert age is not None
    ...     return age + 1
    ...
    >>> res1 = table.select(
    ...     owner_with_pet=concat(table.owner, table.pet), new_age=increment(table.age)
    ... )
    >>> pw.debug.compute_and_print(res1, include_id=False)
    owner_with_pet | new_age
    Alice-cat      |
    Alice-dog      | 11
    Bob-dog        | 8
    Bob-dog        | 10
    >>>
    >>> @pw.udf
    ... async def sleeping_concat(left: str, right: str) -> str:
    ...     await asyncio.sleep(0.1)
    ...     return left + "-" + right
    ...
    >>> res2 = table.select(col=sleeping_concat(table.owner, table.pet))
    >>> pw.debug.compute_and_print(res2, include_id=False)
    col
    Alice-cat
    Alice-dog
    Bob-dog
    Bob-dog
    """

    return UDFFunction(
        fun,
        return_type=return_type,
        deterministic=deterministic,
        propagate_none=propagate_none,
        executor=executor,
        cache_strategy=cache_strategy,
        max_batch_size=max_batch_size,
    )

# Copyright © 2024 Pathway

import asyncio
import functools
import inspect
import threading
from collections.abc import Callable
from typing import Any

import pathway as pw


# https://stackoverflow.com/a/75094151
class _RunThread(threading.Thread):
    def __init__(self, coroutine):
        self.coroutine = coroutine
        self.result = None
        super().__init__()

    def run(self):
        self.result = asyncio.run(self.coroutine)


def _run_async(coroutine):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        thread = _RunThread(coroutine)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(coroutine)


def _coerce_sync(func: Callable) -> Callable:
    if asyncio.iscoroutinefunction(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return _run_async(func(*args, **kwargs))

        return wrapper
    else:
        return func


def _check_model_accepts_arg(model_name: str, provider: str, arg: str):
    from litellm import get_supported_openai_params

    supported_params = (
        get_supported_openai_params(model=model_name, custom_llm_provider=provider)
        or []
    )

    return arg in supported_params


def _extract_value(data: Any | pw.Json) -> Any:
    if isinstance(data, pw.Json):
        return data.value
    return data


def _unwrap_udf(func: pw.UDF | Callable) -> Callable:
    """Turn a Pathway UDF function into regular callable function."""
    if isinstance(func, pw.UDF):
        return func.func  # use settings applied to a UDF
    return func


def get_func_arg_names(func):
    sig = inspect.signature(func)
    return [param.name for param in sig.parameters.values()]

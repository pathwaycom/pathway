# Copyright © 2024 Pathway

import asyncio
import functools
import threading
from collections.abc import Callable
from typing import Any

import pathway as pw
from pathway.xpacks.llm import llms


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

    response: list[str] = get_supported_openai_params(
        model=model_name, custom_llm_provider=provider
    )

    return arg in response


def _check_llm_accepts_arg(llm: pw.UDF, arg: str) -> bool:
    try:
        model_name = llm.kwargs["model"]  # type: ignore
    except KeyError:
        return False

    if isinstance(llm, llms.OpenAIChat):
        return _check_model_accepts_arg(model_name, "openai", arg)
    elif isinstance(llm, llms.LiteLLMChat):
        provider = model_name.split("/")[0]
        model = "".join(
            model_name.split("/")[1:]
        )  # handle case: replicate/meta/meta-llama-3-8b
        return _check_model_accepts_arg(model, provider, arg)
    elif isinstance(llm, llms.CohereChat):
        return _check_model_accepts_arg(model_name, "cohere", arg)

    return False


def _check_llm_accepts_logit_bias(llm: pw.UDF) -> bool:
    return _check_llm_accepts_arg(llm, "logit_bias")


def _extract_value(data: Any | pw.Json) -> Any:
    if isinstance(data, pw.Json):
        return data.value
    return data

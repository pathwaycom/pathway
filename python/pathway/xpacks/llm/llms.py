# Copyright © 2024 Pathway
"""
Pathway UDFs for calling LLMs

This module contains UDFs for calling LLMs chat services:
1. wrappers over LLM APIs
2. prompt building tools
"""

import copy
import json
import logging
import uuid
from abc import abstractmethod
from typing import Any, Iterable, Literal

import pathway as pw
from pathway.internals import udfs
from pathway.optional_import import optional_imports

from ._utils import (
    _check_model_accepts_arg,
    _extract_value_inside_dict,
    _prepare_executor,
)

logger = logging.getLogger(__name__)


def _prepare_messages(messages: list[dict] | list[pw.Json] | pw.Json) -> list[dict]:
    if isinstance(messages, pw.Json):
        messages_as_list: Iterable[dict | pw.Json] = messages.as_list()
    else:
        messages_as_list = messages

    messages_decoded: list[dict] = [
        message.as_dict() if isinstance(message, pw.Json) else message
        for message in messages_as_list
    ]
    return messages_decoded


class BaseChat(pw.UDF):
    """Base class for the LLM chat instances.

    Subclasses need to implement ``__wrapped__`` for use in Pathway.

    Constructor arguments are passed to the :py:func:`~pathway.UDF` constructor.
    Refer to :py:func:`~pathway.UDF` for more information.
    """

    kwargs: dict[str, Any]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = {}

    @abstractmethod
    def _accepts_call_arg(self, arg_name: str) -> bool: ...

    @property
    def model(self) -> str | None:
        return self.kwargs.get("model")


def _prep_message_log(messages: list[dict], verbose: bool) -> str:
    """
    Prepare logs from OpenAI messages.

    If `verbose` is `True`, shorten OpenAI chat logs by redacting images.
    Otherwise, truncate the logs.

    Returns:
        str: Prepared log.
    """
    if verbose:
        log_messages = copy.deepcopy(messages)
        for message in log_messages:
            msg_content = message["content"]
            if isinstance(msg_content, list):
                for c_msg in msg_content:
                    if c_msg.get("type", "text") == "image_url":
                        img_bytes = c_msg["image_url"]["url"]
                        c_msg["image_url"]["url"] = (
                            img_bytes[: img_bytes.index(",") + 25]
                            + "...REDACTED_B64_IMAGE"
                        )
        logs = str(log_messages)
    else:
        str_msg = str(messages)
        logs = str_msg[:100] + "..."
    return logs


class OpenAIChat(BaseChat):
    """Pathway wrapper for OpenAI Chat services.

    The ``capacity``, ``retry_strategy``, ``cache_strategy`` and ``base_url`` need to be specified during object
    construction, and API key must be provided to the constructor with the ``api_key`` argument
    or set in the ``OPENAI_API_KEY`` environment variable. All other arguments can be overridden during application.

    Args:
        capacity: Maximum number of concurrent operations allowed.
            Defaults to None, indicating no specific limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None, meaning no retries.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.
        model: ID of the model to use. See the
            `model endpoint compatibility <https://platform.openai.com/docs/models/model-endpoint-compatibility>`_
            table for details on which models work with the Chat API.
        api_key: API key to be used for API calls to OpenAI. It must be either provided in the
            constructor or set in the ``OPENAI_API_KEY`` environment variable.
        frequency_penalty: Number between -2.0 and 2.0. Positive values penalize new tokens based on their
            existing frequency in the text so far, decreasing the model's likelihood to
            repeat the same line verbatim.

            `See more information about frequency and presence penalties.
            <https://platform.openai.com/docs/guides/text-generation/parameter-details>`_
        function_call: Deprecated in favor of `tool_choice`.

            Controls which (if any) function is called by the model. `none` means the model
            will not call a function and instead generates a message. `auto` means the model
            can pick between generating a message or calling a function. Specifying a
            particular function via `{"name": "my_function"}` forces the model to call that
            function.

            `none` is the default when no functions are present. `auto` is the default if
            functions are present.
        functions: Deprecated in favor of `tools`.

            A list of functions the model may generate JSON inputs for.
        logit_bias: Modify the likelihood of specified tokens appearing in the completion.

            Accepts a JSON object that maps tokens (specified by their token ID in the
            tokenizer) to an associated bias value from -100 to 100. Mathematically, the
            bias is added to the logits generated by the model prior to sampling. The exact
            effect will vary per model, but values between -1 and 1 should decrease or
            increase likelihood of selection; values like -100 or 100 should result in a ban
            or exclusive selection of the relevant token.
        logprobs: Whether to return log probabilities of the output tokens or not. If true,
            returns the log probabilities of each output token returned in the `content` of
            `message`. This option is currently not available on the `gpt-4-vision-preview`
            model.
        max_tokens: The maximum number of [tokens](/tokenizer) that can be generated in the chat
            completion.

            The total length of input tokens and generated tokens is limited by the model's
            context length.
            `Example Python code <https://cookbook.openai.com/examples/how_to_count_tokens_with_tiktoken>`_
            for counting tokens.
        n: How many chat completion choices to generate for each input message. Note that
            you will be charged based on the number of generated tokens across all of the
            choices. Keep `n` as `1` to minimize costs.
        presence_penalty: Number between -2.0 and 2.0. Positive values penalize new tokens based on
            whether they appear in the text so far, increasing the model's likelihood to
            talk about new topics.

            `See more information about frequency and presence penalties.
            <https://platform.openai.com/docs/guides/text-generation/parameter-details>`_
        response_format: An object specifying the format that the model must output. Compatible with
            `gpt-4-1106-preview` and `gpt-3.5-turbo-1106`.

            Setting to `{ "type": "json_object" }` enables JSON mode, which guarantees the
            message the model generates is valid JSON.

            **Important:** when using JSON mode, you **must** also instruct the model to
            produce JSON yourself via a system or user message. Without this, the model may
            generate an unending stream of whitespace until the generation reaches the token
            limit, resulting in a long-running and seemingly "stuck" request. Also note that
            the message content may be partially cut off if `finish_reason="length"`, which
            indicates the generation exceeded `max_tokens` or the conversation exceeded the
            max context length.
        seed: This feature is in Beta. If specified, our system will make a best effort to
            sample deterministically, such that repeated requests with the same `seed` and
            parameters should return the same result. Determinism is not guaranteed, and you
            should refer to the `system_fingerprint` response parameter to monitor changes
            in the backend.
        stop: Up to 4 sequences where the API will stop generating further tokens.
        stream: If set, partial message deltas will be sent, like in ChatGPT. Tokens will be
            sent as data-only
            `server-sent events
            <https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#Event_stream_format>`_
            as they become available, with the stream terminated by a `data: [DONE]`
            message.
            `Example Python code <https://cookbook.openai.com/examples/how_to_stream_completions>`_.
        temperature: What sampling temperature to use, between 0 and 2. Higher values like 0.8 will
            make the output more random, while lower values like 0.2 will make it more
            focused and deterministic.

            We generally recommend altering this or `top_p` but not both.
        tool_choice: Controls which (if any) function is called by the model. `none` means the model
            will not call a function and instead generates a message. `auto` means the model
            can pick between generating a message or calling a function. Specifying a
            particular function via
            `{"type: "function", "function": {"name": "my_function"}}` forces the model to
            call that function.

            `none` is the default when no functions are present. `auto` is the default if
            functions are present.
        tools: A list of tools the model may call. Currently, only functions are supported as a
            tool. Use this to provide a list of functions the model may generate JSON inputs
            for.
        top_logprobs: An integer between 0 and 5 specifying the number of most likely tokens to return
            at each token position, each with an associated log probability. `logprobs` must
            be set to `true` if this parameter is used.
        top_p: An alternative to sampling with temperature, called nucleus sampling, where the
            model considers the results of the tokens with top_p probability mass. So 0.1
            means only the tokens comprising the top 10% probability mass are considered.

            We generally recommend altering this or `temperature` but not both.
        user: A unique identifier representing your end-user, which can help OpenAI to monitor
            and detect abuse.
            `Learn more <https://platform.openai.com/docs/guides/safety-best-practices/end-user-ids>`_.
        extra_headers: Send extra headers
        extra_query: Add additional query parameters to the request
        extra_body: Add additional JSON properties to the request
        timeout: Override the client-level default timeout for this request, in seconds


    Any arguments can be provided either to the constructor or in the UDF call.
    To specify the `model` in the UDF call, set it to None in the constructor.

    Example:

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import llms
    >>> from pathway.udfs import ExponentialBackoffRetryStrategy
    >>> chat = llms.OpenAIChat(model=None, retry_strategy=ExponentialBackoffRetryStrategy(max_retries=6))
    >>> t = pw.debug.table_from_markdown('''
    ... txt     | model
    ... Wazzup? | gpt-3.5-turbo
    ... ''')
    >>> r = t.select(ret=chat(llms.prompt_chat_single_qa(t.txt), model=t.model))
    >>> r
    <pathway.Table schema={'ret': str | None}>
    """

    def __init__(
        self,
        capacity: int | None = None,
        retry_strategy: udfs.AsyncRetryStrategy | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        model: str | None = "gpt-3.5-turbo",
        *,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
        **openai_kwargs,
    ):
        with optional_imports("xpack-llm"):
            import openai  # noqa:F401
        executor = _prepare_executor(
            async_mode=async_mode, capacity=capacity, retry_strategy=retry_strategy
        )
        super().__init__(
            executor=executor,
            cache_strategy=cache_strategy,
        )
        self.kwargs.update(openai_kwargs)
        api_key = self.kwargs.pop("api_key", None)
        base_url = self.kwargs.pop("base_url", None)

        self.client = openai.AsyncOpenAI(
            api_key=api_key, base_url=base_url, max_retries=0
        )

        if model is not None:
            self.kwargs["model"] = model

    async def __wrapped__(self, messages: list[dict] | pw.Json, **kwargs) -> str | None:
        messages_decoded = _prepare_messages(messages)
        kwargs = {**self.kwargs, **kwargs}
        kwargs = _extract_value_inside_dict(kwargs)
        verbose = kwargs.pop("verbose", False)

        msg_id = str(uuid.uuid4())[-8:]

        event = {
            "_type": "openai_chat_request",
            "kwargs": copy.deepcopy(kwargs),
            "id": msg_id,
            "messages": _prep_message_log(messages_decoded, verbose),
        }
        logger.info(json.dumps(event, ensure_ascii=False))

        ret = await self.client.chat.completions.create(messages=messages_decoded, **kwargs)  # type: ignore
        response: str | None = ret.choices[0].message.content

        if response is not None:
            event = {
                "_type": "openai_chat_response",
                "response": (
                    response if verbose else response[: min(50, len(response))] + "..."
                ),
                "id": msg_id,
            }
            logger.info(json.dumps(event, ensure_ascii=False))
        return response

    def __call__(self, messages: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """Sends messages to OpenAI Chat and returns response.

        Args:
            messages (ColumnExpression[list[dict] | pw.Json]): Column with messages to send
                to OpenAIChat
            **kwargs: override for defaults set in the constructor
        """
        return super().__call__(messages, **kwargs)

    def _accepts_call_arg(self, arg_name: str) -> bool:
        """Check whether the LLM accepts the argument during the inference.
        If ``model`` is not set, return ``False``.

        Args:
            arg_name: Argument name to be checked.
        """

        if self.model is None:
            return False
        return _check_model_accepts_arg(self.model, "openai", arg_name)


class LiteLLMChat(BaseChat):
    """Pathway wrapper for LiteLLM Chat services.

    Model has to be specified either in constructor call or in each application, no default
    is provided. The capacity, retry_strategy and cache_strategy need to be specified during object
    construction. All other arguments can be overridden during application.

    Args:
        capacity: Maximum number of concurrent operations allowed.
            Defaults to None, indicating no specific limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None, meaning no retries.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.
        model: ID of the model to use. Check the
            `providers supported by LiteLLM <https://docs.litellm.ai/docs/providers>`_
            for details on which models work with the LiteLLM API.
        api_base: API endpoint to be used for the call.
        api_version: API version to be used for the call. Only for Azure models.
        num_retries: The number of retries if the API call fails.
        context_window_fallback_dict: Mapping of fallback models to be used in case of context window error
        fallbacks: List of fallback models to be used if the initial call fails
        metadata: Additional data to be logged when the call is made.

    For more information on provider specific arguments check the
    `LiteLLM documentation <https://docs.litellm.ai/docs/completion/input>`_.

    Any arguments can be provided either to the constructor or in the UDF call.
    To specify the `model` in the UDF call, set it to None in the constructor.

    Example:

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import llms
    >>> from pathway.udfs import ExponentialBackoffRetryStrategy
    >>> chat = llms.LiteLLMChat(model=None, retry_strategy=ExponentialBackoffRetryStrategy(max_retries=6))
    >>> t = pw.debug.table_from_markdown('''
    ... txt     | model
    ... Wazzup? | gpt-3.5-turbo
    ... ''')
    >>> r = t.select(ret=chat(llms.prompt_chat_single_qa(t.txt), model=t.model))
    >>> r
    <pathway.Table schema={'ret': str | None}>
    """

    def __init__(
        self,
        capacity: int | None = None,
        retry_strategy: udfs.AsyncRetryStrategy | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        model: str | None = None,
        *,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
        **litellm_kwargs,
    ):
        with optional_imports("xpack-llm"):
            import litellm  # noqa:F401
        executor = _prepare_executor(
            async_mode=async_mode, capacity=capacity, retry_strategy=retry_strategy
        )
        super().__init__(
            executor=executor,
            cache_strategy=cache_strategy,
        )
        self.kwargs.update(litellm_kwargs)
        if model is not None:
            self.kwargs["model"] = model

    def __wrapped__(self, messages: list[dict] | pw.Json, **kwargs) -> str | None:
        import litellm

        messages_decoded = _prepare_messages(messages)

        kwargs = {**self.kwargs, **kwargs}
        kwargs = _extract_value_inside_dict(kwargs)

        verbose = kwargs.pop("verbose", False)

        event = {
            "_type": "litellm_chat_request",
            "kwargs": copy.deepcopy(kwargs),
            "messages": _prep_message_log(messages_decoded, verbose),
        }
        logger.info(json.dumps(event, ensure_ascii=False))

        ret = litellm.completion(
            messages=messages_decoded, **kwargs
        )  # temporarily disable async due to behavior difference while using `json` mode with Ollama
        response = ret.choices[0]["message"]["content"]

        event = {
            "_type": "litellm_chat_response",
            "response": response if verbose else response[:50] + "...",
        }
        logger.info(json.dumps(event, ensure_ascii=False))

        return response

    def __call__(self, messages: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """Sends messages to the LLM and returns response.

        Args:
            messages (ColumnExpression[list[dict] | pw.Json]): Column with messages to send
                to the LLM
            **kwargs: override for defaults set in the constructor
        """
        return super().__call__(messages, **kwargs)

    def _accepts_call_arg(self, arg_name: str) -> bool:
        """Check whether the LLM accepts the argument during the inference.
        If ``model`` is not set, return ``False``.

        Args:
            arg_name: Argument name to be checked.
        """

        if self.model is None:
            return False

        if "/" in self.model:
            provider = self.model.split("/")[0]
            model = "".join(
                self.model.split("/")[1:]
            )  # handle case: replicate/meta/meta-llama-3-8b
        else:
            model = self.model
            provider = None
        return _check_model_accepts_arg(model, provider, arg_name)


class HFPipelineChat(BaseChat):
    """
    Pathway wrapper for HuggingFace Pipeline.

    Args:
        model: ID of the model to be used
        call_kwargs: kwargs that will be passed to each call of HuggingFace Pipeline.
            These can be overridden during each application. For possible arguments check
            `the HuggingFace documentation
            <https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TextGenerationPipeline.__call__>`_.
        device: defines which device will be used to run the Pipeline
        batch_size: maximum size of a single batch to be sent to the chat. Bigger
            batches may reduce the time needed for generating the answer, especially on GPU.
        pipeline_kwargs: kwargs accepted during initialization of HuggingFace Pipeline.
            For possible arguments check
            `the HuggingFace documentation <https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.pipeline>`_.

    `call_kwargs` can be overridden during application, all other arguments need
    to be specified during class construction.

    Example:

    >>> import pathway as pw  # doctest: +SKIP
    >>> from pathway.xpacks.llm import llms  # doctest: +SKIP
    >>> chat = llms.HFPipelineChat(model="gpt2")  # doctest: +SKIP
    >>> t = pw.debug.table_from_markdown('''
    ... txt
    ... Wazzup?
    ... ''')  # doctest: +SKIP
    >>> r = t.select(ret=chat(t.txt))  # doctest: +SKIP
    >>> r  # doctest: +SKIP
    <pathway.Table schema={'ret': str | None}>
    """  # noqa: E501

    def __init__(
        self,
        model: str | None = "gpt2",
        call_kwargs: dict = {},
        device: str = "cpu",
        batch_size: int = 32,
        **pipeline_kwargs,
    ):
        with optional_imports("xpack-llm-local"):
            import transformers

        super().__init__(max_batch_size=batch_size)
        self.pipeline = transformers.pipeline(
            model=model, device=device, **pipeline_kwargs
        )
        self.tokenizer = self.pipeline.tokenizer
        self.kwargs = {"batch_size": batch_size, **call_kwargs}

    def __wrapped__(
        self, messages_list: list[list[dict] | pw.Json | str], **kwargs
    ) -> list[str | None]:

        def decode_messages(messages: list[dict] | pw.Json | str) -> list[dict] | str:
            if isinstance(messages, str):
                return messages
            else:
                return _prepare_messages(messages)

        messages_decoded_list = [
            decode_messages(messages) for messages in messages_list
        ]
        kwargs = _extract_value_inside_dict(kwargs)
        constant_kwargs = {}
        per_row_kwargs = {}

        if kwargs:
            for key, values in kwargs.items():
                v = values[0]
                if all(value == v for value in values):
                    constant_kwargs[key] = v
                else:
                    per_row_kwargs[key] = values

        def decode_output(output) -> str | None:
            result = output[0]["generated_text"]
            if isinstance(result, list):
                return result[-1]["content"]
            else:
                return result

        # if kwargs are not the same for every message we cannot batch them
        # huggingface does not allow batching if tokenizer or tokenizer.pad_token_id is None
        if (
            per_row_kwargs
            or self.pipeline.tokenizer is None
            or self.pipeline.tokenizer.pad_token_id is None
        ):

            def infer_single(messages, kwargs) -> str | None:
                kwargs = {**self.kwargs, **constant_kwargs, **kwargs}
                output = self.pipeline(messages, **kwargs)
                return decode_output(output)

            list_of_per_row_kwargs = [
                dict(zip(per_row_kwargs, values))
                for values in zip(*per_row_kwargs.values())
            ]

            if list_of_per_row_kwargs:
                result_list = [
                    infer_single(messages, kwargs)
                    for messages, kwargs in zip(
                        messages_decoded_list, list_of_per_row_kwargs
                    )
                ]
            else:
                result_list = [
                    infer_single(messages, {}) for messages in messages_decoded_list
                ]

            return result_list

        else:
            kwargs = {**self.kwargs, **constant_kwargs}
            output_list = self.pipeline(messages_decoded_list, **kwargs)

            if output_list is None:
                return [None] * len(messages_list)

            result_list = [decode_output(output) for output in output_list]
            return result_list

    def crop_to_max_length(
        self, input_string: pw.ColumnExpression, max_prompt_length: int = 500
    ) -> pw.ColumnExpression:
        @pw.udf
        def wrapped(input_string: str) -> str:
            tokens = self.tokenizer.tokenize(input_string)
            if len(tokens) > max_prompt_length:
                tokens = tokens[:max_prompt_length]
            return self.tokenizer.convert_tokens_to_string(tokens)

        return wrapped(input_string)

    def __call__(self, messages: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """Sends messages to the LLM and returns response.

        Args:
            messages (ColumnExpression[list[dict] | pw.Json]): Column with messages to send
                to the LLM
            **kwargs: override for defaults set in the constructor
        """
        return super().__call__(messages, **kwargs)

    def _accepts_call_arg(self, arg_name: str) -> bool:
        """Check whether the LLM accepts the argument during the inference.
        If ``model`` is not set, return ``False``.

        Note that some OpenAI generation params have different names in ``transformers.pipelines``,
        for example, ``logit_bias`` can be set with ``sequence_bias``.

        Args:
            arg_name: Argument name to be checked.
        """

        if self.model is None:
            return False

        return _check_model_accepts_arg(self.model, "huggingface", arg_name)


class CohereChat(BaseChat):
    """Pathway wrapper for Cohere Chat services.
    Returns answer and cited docs as tuple[str, list[dict]]. Cited docs is empty list if
    there are no citations.

    Model defaults to `command`.

    The capacity, retry_strategy and cache_strategy need to be specified during object
    construction. All other arguments can be overridden during application.

    Args:
        capacity: Maximum number of concurrent operations allowed.
            Defaults to None, indicating no specific limit.
        retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None, meaning no retries.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.
        model: name of the model to use. Check the
            `available models <https://docs.cohere.com/docs/command-beta>`_
            for details.


    >>> import pathway as pw
    >>> from pathway.xpacks.llm import llms
    >>> from pathway.udfs import ExponentialBackoffRetryStrategy
    >>> chat = llms.CohereChat(retry_strategy=ExponentialBackoffRetryStrategy(max_retries=6))
    >>> t = pw.debug.table_from_markdown('''
    ... txt
    ... Wazzup?
    ... ''')
    >>> r = t.select(ret=chat(llms.prompt_chat_single_qa(t.txt)))
    >>> parsed_table = r.select(response=pw.this.ret[0], citations=pw.this.ret[1])
    >>> parsed_table
    <pathway.Table schema={'response': <class 'str'>, 'citations': list[pathway.internals.json.Json]}>
    >>> docs = [{"text": "Pathway is a high-throughput, low-latency data processing framework that handles live data & streaming for you."},
    ... {"text": "RAG stands for Retrieval Augmented Generation."}]
    >>> t_with_docs = t.select(*pw.this, docs=docs)
    >>> r = t_with_docs.select(ret=chat(llms.prompt_chat_single_qa("what is rag?"), documents=pw.this.docs))
    >>> parsed_table = r.select(response=pw.this.ret[0], citations=pw.this.ret[1])
    >>> parsed_table
    <pathway.Table schema={'response': <class 'str'>, 'citations': list[pathway.internals.json.Json]}>
    """  # noqa: E501

    def __init__(
        self,
        capacity: int | None = None,
        retry_strategy: udfs.AsyncRetryStrategy | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        model: str | None = "command",
        **cohere_kwargs,
    ):
        with optional_imports("xpack-llm"):
            import cohere  # noqa:F401

        executor = udfs.async_executor(
            capacity=capacity,
            retry_strategy=retry_strategy,
        )
        super().__init__(
            executor=executor,
            cache_strategy=cache_strategy,
        )
        self.kwargs.update(cohere_kwargs)
        if model is not None:
            self.kwargs["model"] = model

    def __wrapped__(
        self,
        messages: list[dict] | pw.Json,
        documents: list[dict] | pw.Json | tuple | None = None,
        **kwargs,
    ) -> tuple[str, list[dict]]:
        import cohere

        messages_decoded = _prepare_messages(messages)

        chat_history = messages_decoded[:-1]
        message = messages_decoded[-1]["content"]

        if isinstance(documents, pw.Json):
            docs: list[dict[str, str]] | None = documents.as_list()
        elif isinstance(documents, tuple):
            docs = [doc.value if isinstance(doc, pw.Json) else doc for doc in documents]
        else:
            docs = documents

        kwargs = {**self.kwargs, **kwargs}
        kwargs = _extract_value_inside_dict(kwargs)
        api_key = kwargs.pop("api_key", None)

        client = cohere.Client(api_key=api_key)
        ret = client.chat(
            message=message, chat_history=chat_history, documents=docs, **kwargs  # type: ignore
        )
        response: str = ret.text
        cited_documents: list = ret.citations or []
        cited_documents = list(map(lambda citation: citation.dict(), cited_documents))

        event = {
            "_type": "cohere_chat_response",
            "response": response,
        }
        logger.info(json.dumps(event, ensure_ascii=False))

        return (response, cited_documents)

    def __call__(
        self,
        messages: pw.ColumnExpression,
        documents: pw.ColumnExpression | None = None,
        **kwargs,
    ) -> pw.ColumnExpression:
        """Sends messages to Cohere Chat and returns response.

        Args:
            messages (ColumnExpression[list[dict] | pw.Json]): Column with messages to send.
                to the LLM
            documents (ColumnExpression[list[dict] | pw.Json | tuple] | None): Column with context
                documents to be sent to Cohere Chat. This argument is optional.
            **kwargs: override for defaults set in the constructor.
        """
        return super().__call__(messages, documents, **kwargs)

    def _accepts_call_arg(self, arg_name: str) -> bool:
        """Check whether the LLM accepts the argument during the inference.
        If ``model`` is not set, return ``False``.

        Args:
            arg_name: Argument name to be checked.
        """

        if self.model is None:
            return False

        return _check_model_accepts_arg(self.model, "cohere", arg_name)


@pw.udf
def prompt_chat_single_qa(question: str) -> pw.Json:
    """
    Create chat prompt messages for single question answering. A string with a question
    is converted into one-element list with a dictionary with keys `role` and `content`.

    Args:
        question (ColumnExpression[str]): a column with questions to be transformed into prompts

    Example:

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import llms
    >>> t = pw.debug.table_from_markdown('''
    ... txt
    ... Wazzup?
    ... ''')
    >>> r = t.select(prompt=llms.prompt_chat_single_qa(t.txt))
    >>> pw.debug.compute_and_print(r, include_id=False)
    prompt
    [{"role": "user", "content": "Wazzup?"}]
    """
    return pw.Json([dict(role="user", content=question)])

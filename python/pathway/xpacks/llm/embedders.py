# Copyright © 2024 Pathway
"""
Pathway embedder UDFs.
"""

import litellm as litellm_mod
import openai as openai_mod

import pathway as pw
from pathway.internals import asynchronous


class OpenAIEmbedder(pw.UDFAsync):
    """Pathway wrapper for OpenAI Embedding services.

    The capacity, retry_strategy and cache_strategy need to be specified during object
    construction. All other arguments can be overridden during application.

    Parameters:
    - capacity: Maximum number of concurrent operations allowed.
        Defaults to None, indicating no specific limit.
    - retry_strategy: Strategy for handling retries in case of failures.
        Defaults to None.
    - cache_strategy: Defines the caching mechanism. If set to None and a persistency
        is enabled, operations will be cached using the persistence layer.
        Defaults to None.
    - model: ID of the model to use. You can use the
        [List models](https://platform.openai.com/docs/api-reference/models/list) API to
        see all of your available models, or see
        [Model overview](https://platform.openai.com/docs/models/overview) for
        descriptions of them.
    - encoding_format: The format to return the embeddings in. Can be either `float` or
        [`base64`](https://pypi.org/project/pybase64/).
    - user: A unique identifier representing your end-user, which can help OpenAI to monitor
        and detect abuse.
        [Learn more](https://platform.openai.com/docs/guides/safety-best-practices/end-user-ids).
    - extra_headers: Send extra headers
    - extra_query: Add additional query parameters to the request
    - extra_body: Add additional JSON properties to the request
    - timeout: Timeout for requests, in seconds

    Any arguments can be provided either to the constructor or in the UDF call.
    To specify the `model` in the UDF call, set it to None.

    Examples:
    >>> import pathway as pw
    >>> from pathway.xpacks.llm import embedders
    >>> embedder = embedders.OpenAIEmbedder(model="text-embedding-ada-002")
    >>> t = pw.debug.table_from_markdown('''
    ... txt
    ... Text
    ... ''')
    >>> t.select(ret=embedder(pw.this.txt))
    <pathway.Table schema={'ret': list[float]}>

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import embedders
    >>> embedder = embedders.OpenAIEmbedder()
    >>> t = pw.debug.table_from_markdown('''
    ... txt  | model
    ... Text | text-embedding-ada-002
    ... ''')
    >>> t.select(ret=embedder(pw.this.txt, model=pw.this.model))
    <pathway.Table schema={'ret': list[float]}>
    """

    def __init__(
        self,
        *,
        capacity: int | None = None,
        retry_strategy: asynchronous.AsyncRetryStrategy | None = None,
        cache_strategy: asynchronous.CacheStrategy | None = None,
        model: str | None = "text-embedding-ada-002",
        **openai_kwargs,
    ):
        super().__init__(
            capacity=capacity,
            retry_strategy=retry_strategy,
            cache_strategy=cache_strategy,
        )
        self.kwargs = dict(openai_kwargs)
        if model is not None:
            self.kwargs["model"] = model

    async def __wrapped__(self, input, **kwargs) -> list[float]:
        """Embed the documents

        Parameters:
            - input: mandatory, the string to embed.
            - **kwargs: optional parameters, if unset defaults from the constructor
              will be taken.
        """
        kwargs = {**self.kwargs, **kwargs}
        api_key = kwargs.pop("api_key", None)
        client = openai_mod.AsyncOpenAI(api_key=api_key)
        ret = await client.embeddings.create(input=[input or "."], **kwargs)
        return ret.data[0].embedding


class LiteLLMEmbedder(pw.UDFAsync):
    """Pathway wrapper for `litellm.embedding`.

    Model has to be specified either in constructor call or in each application, no default
    is provided. The capacity, retry_strategy and cache_strategy need to be specified
    during object construction. All other arguments can be overridden during application.

    Parameters:
    - capacity: Maximum number of concurrent operations allowed.
        Defaults to None, indicating no specific limit.
    - retry_strategy: Strategy for handling retries in case of failures.
        Defaults to None.
    - cache_strategy: Defines the caching mechanism. If set to None and a persistency
        is enabled, operations will be cached using the persistence layer.
        Defaults to None.
    - model: The embedding model to use.
    - timeout: The timeout value for the API call, default 10 mins
    - litellm_call_id: The call ID for litellm logging.
    - litellm_logging_obj: The litellm logging object.
    - logger_fn: The logger function.
    - api_base: Optional. The base URL for the API.
    - api_version: Optional. The version of the API.
    - api_key: Optional. The API key to use.
    - api_type: Optional. The type of the API.
    - custom_llm_provider: The custom llm provider.

    Any arguments can be provided either to the constructor or in the UDF call.
    To specify the `model` in the UDF call, set it to None.

    Examples:
    >>> import pathway as pw
    >>> from pathway.xpacks.llm import embedders
    >>> embedder = embedders.LiteLLMEmbedder(model="text-embedding-ada-002")
    >>> t = pw.debug.table_from_markdown('''
    ... txt
    ... Text
    ... ''')
    >>> t.select(ret=embedder(pw.this.txt))
    <pathway.Table schema={'ret': list[float]}>

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import embedders
    >>> embedder = embedders.LiteLLMEmbedder()
    >>> t = pw.debug.table_from_markdown('''
    ... txt  | model
    ... Text | text-embedding-ada-002
    ... ''')
    >>> t.select(ret=embedder(pw.this.txt, model=pw.this.model))
    <pathway.Table schema={'ret': list[float]}>
    """

    def __init__(
        self,
        *,
        capacity: int | None = None,
        retry_strategy: asynchronous.AsyncRetryStrategy | None = None,
        cache_strategy: asynchronous.CacheStrategy | None = None,
        model: str | None = None,
        **llmlite_kwargs,
    ):
        super().__init__(
            capacity=capacity,
            retry_strategy=retry_strategy,
            cache_strategy=cache_strategy,
        )
        self.kwargs = dict(llmlite_kwargs)
        if model is not None:
            self.kwargs["model"] = model

    async def __wrapped__(self, input, **kwargs) -> list[float]:
        """Embed the documents

        Parameters:
            - input: mandatory, the string to embed.
            - **kwargs: optional parameters, if unset defaults from the constructor
              will be taken.
        """
        kwargs = {**self.kwargs, **kwargs}
        ret = await litellm_mod.aembedding(input=[input or "."], **kwargs)
        return ret.data[0]["embedding"]


class SentenceTransformerEmbedder(pw.UDFSync):
    """
    Pathway wrapper for Sentence-Transformers embedder.

    Args:
        model: model name or path
        call_kwargs: kwargs that will be passed to each call of encode.
            These can be overridden during each application. For possible arguments check
            [the Sentence-Transformers documentation](https://www.sbert.net/docs/package_reference/SentenceTransformer.html#sentence_transformers.SentenceTransformer.encode).
        device: defines which device will be used to run the Pipeline
        sentencetransformer_kwargs: kwargs accepted during initialization of SentenceTransformers.
            For possible arguments check
            [the Sentence-Transformers documentation](https://www.sbert.net/docs/package_reference/SentenceTransformer.html#sentence_transformers.SentenceTransformer)
    """  # noqa: E501

    def __init__(
        self,
        model: str,
        call_kwargs: dict = {},
        device: str = "cpu",
        **sentencetransformer_kwargs,
    ):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ValueError(
                "Please install sentence_transformers: `pip install sentence_transformers`"
            )

        super().__init__()
        self.model = SentenceTransformer(
            model_name_or_path=model, device=device, **sentencetransformer_kwargs
        )
        self.kwargs = call_kwargs

    def __wrapped__(self, text: str, **kwargs) -> list[float]:
        """
        Embed the text

        Args:
            - input: mandatory, the string to embed.
            - **kwargs: optional parameters for `encode` method. If unset defaults from the constructor
              will be taken. For possible arguments check
              [the Sentence-Transformers documentation](https://www.sbert.net/docs/package_reference/SentenceTransformer.html#sentence_transformers.SentenceTransformer.encode).
        """  # noqa: E501
        kwargs = {**self.kwargs, **kwargs}
        return self.model.encode(text, **kwargs).tolist()

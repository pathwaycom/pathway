# Copyright © 2024 Pathway

"""
A library of text spliiters - routines which slit a long text into smaller chunks.
"""
import abc
import unicodedata
from typing import TYPE_CHECKING

import pathway as pw
from pathway.optional_import import optional_imports


def _normalize_unicode(text: str):
    """
    Get rid of ligatures
    """
    return unicodedata.normalize("NFKC", text)


class BaseSplitter(pw.UDF):
    """
    Abstract base class for splitters that split a long text into smaller chunks.
    """

    kwargs: dict = {}

    def __call__(self, text: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """
        Split a given text into smaller chunks. Preserves metadata and propagates it to all chunks
        that are created from the same input string.

        Args:
            text: input column containing text to be split
            **kwargs: override for defaults settings from the constructor

        Returns:
            pw.ColumnExpression: A column of pairs: (chunk ``text``, ``metadata``).
                Metadata are propagated to all chunks created from the same input string.
                If no metadata is provided, an empty dictionary is used.
        """
        return super().__call__(text, **kwargs)

    def __wrapped__(
        self, inputs: str | tuple[str, dict | pw.Json], **kwargs
    ) -> list[tuple[str, dict]]:

        if isinstance(inputs, tuple):
            if len(inputs) != 2:
                raise ValueError(
                    f"Expected a tuple of length 2, got {len(inputs)} elements"
                )

            text, metadata = inputs
            if not isinstance(text, str):
                raise ValueError(
                    f"Expected `text` to be of type `str`, got {type(text)}"
                )

            if isinstance(metadata, pw.Json):
                metadata = metadata.as_dict()
            elif isinstance(metadata, dict):
                pass
            else:
                raise ValueError(
                    f"Expected `metadata` to be of type `dict` or `pw.Json`, got {type(metadata)}"
                )

        elif isinstance(inputs, str):
            text, metadata = inputs, {}

        else:
            raise ValueError(
                f"Expected `inputs` to be of type `str` or a tuple of `str` and `dict`, got {type(inputs)}"
            )

        return self.chunk(text, metadata, **kwargs)

    @abc.abstractmethod
    def chunk(self, text: str, metadata: dict = {}, **kwargs) -> list[tuple[str, dict]]:
        pass


SEPARATORS = ["\n\n", "\n", " ", ""]


# wrapper around Langchain splitter
class RecursiveSplitter(BaseSplitter):
    """
    Splitter that splits a long text into smaller chunks based on a set of separators.
    Chunking is performed recursively using first separator in the list and then second
    separator in the list and so on, until the text is split into chunks of length smaller than ``chunk_size``.
    Length of the chunks is measured by the number of characters in the text if none of
    ``encoding_name``, ``model_name`` or ``hf_tokenizer`` is provided. Otherwise, the length of the
    chunks is measured by the number of tokens that particular tokenizer would output.

    Under the hood it is a wrapper around ``langchain_text_splitters.RecursiveTextSplitter`` (MIT license).

    Args:
        chunk_size: maximum size of a chunk in characters/tokens.
        chunk_overlap: number of characters/tokens to overlap between chunks.
        separators: list of strings to split the text on.
        is_separator_regex: whether the separators are regular expressions.
        encoding_name: name of the encoding from ``tiktoken``.
            For the list of available encodings please refer to tiktoken documentation:
            https://cookbook.openai.com/examples/how_to_count_tokens_with_tiktoken
        model_name: name of the model from ``tiktoken``. See the link above for more details.
        hf_tokenizer: Huggingface tokenizer to use for tokenization.
    """

    if TYPE_CHECKING:
        from transformers import PreTrainedTokenizerBase

    def __init__(
        self,
        chunk_size: int = 500,
        chunk_overlap: int = 0,
        separators: list[str] = SEPARATORS,
        is_separator_regex: bool = False,
        encoding_name: str | None = None,
        model_name: str | None = None,
        hf_tokenizer: "PreTrainedTokenizerBase | None" = None,
    ):
        super().__init__()

        with optional_imports("xpack-llm"):
            from langchain_text_splitters import (
                RecursiveCharacterTextSplitter,
                TextSplitter,
            )

        self.kwargs = dict(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=separators,
            is_separator_regex=is_separator_regex,
        )

        self._splitter: TextSplitter

        if encoding_name is not None:
            self._splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                encoding_name=encoding_name, **self.kwargs
            )
        elif model_name is not None:
            self._splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                model_name=model_name, **self.kwargs
            )
        elif hf_tokenizer is not None:
            self._splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
                tokenizer=hf_tokenizer, **self.kwargs
            )
        else:
            self._splitter = RecursiveCharacterTextSplitter(**self.kwargs)

    def chunk(self, text: str, metadata: dict = {}, **kwargs) -> list[tuple[str, dict]]:
        chunked = self._splitter.split_text(text)
        return [(chunk, metadata) for chunk in chunked]


class NullSplitter(BaseSplitter):
    """A splitter which returns its argument as one long text ith null metadata.

    Args:
        txt: text to be split

    Returns:
        list of pairs: chunk text and metadata.

    The null splitter always return a list of length one containing the full text and empty metadata.
    """

    def chunk(self, text: str, metadata: dict = {}, **kwargs) -> list[tuple[str, dict]]:
        return [(text, metadata)]


class TokenCountSplitter(BaseSplitter):
    """
    Splits a given string or a list of strings into chunks based on token
    count.

    This splitter tokenizes the input texts and splits them into smaller parts ("chunks")
    ensuring that each chunk has a token count between `min_tokens` and
    `max_tokens`. It also attempts to break chunks at sensible points such as
    punctuation marks.
    Splitter expects input to be a Pathway column of strings OR pairs of strings and dict metadata.

    All default arguments may be overridden in the UDF call

    Args:
        min_tokens: minimum tokens in a chunk of text.
        max_tokens: maximum size of a chunk in tokens.
        encoding_name: name of the encoding from `tiktoken`.
            For a list of available encodings please refer to the tiktoken documentation:
            https://cookbook.openai.com/examples/how_to_count_tokens_with_tiktoken

    Example:

    >>> from pathway.xpacks.llm.splitters import TokenCountSplitter
    >>> import pathway as pw
    >>> t  = pw.debug.table_from_markdown(
    ...     '''| text
    ... 1| cooltext'''
    ... )
    >>> splitter = TokenCountSplitter(min_tokens=1, max_tokens=1)
    >>> t += t.select(chunks = splitter(pw.this.text))
    >>> pw.debug.compute_and_print(t, include_id=False)
    text     | chunks
    cooltext | (('cool', pw.Json({})), ('text', pw.Json({})))
    """

    CHARS_PER_TOKEN = 3
    PUNCTUATION = [".", "?", "!", "\n"]

    def __init__(
        self,
        min_tokens: int = 50,
        max_tokens: int = 500,
        encoding_name: str = "cl100k_base",
    ):
        with optional_imports("xpack-llm"):
            import tiktoken  # noqa:F401

        super().__init__()
        self.kwargs = dict(
            min_tokens=min_tokens, max_tokens=max_tokens, encoding_name=encoding_name
        )

    def chunk(self, text: str, metadata: dict = {}, **kwargs) -> list[tuple[str, dict]]:
        """
        Split a given string into smaller chunks. Preserves metadata and propagates it to all chunks
        that are created from the same input string.

        Args:
            inputs: input text to be split
            metadata: metadata associated with the input text
            **kwargs: override for defaults set in the constructor

        Returns:
            list[tuple[str, dict]]: List of pairs: (chunk ``text``, ``metadata``).
                Metadata are propagated to all chunks created from the same input string.
                If no metadata is provided an empty dictionary is used.
        """

        import tiktoken

        kwargs = {**self.kwargs, **kwargs}
        tokenizer = tiktoken.get_encoding(kwargs.pop("encoding_name"))
        max_tokens = kwargs.pop("max_tokens")
        min_tokens = kwargs.pop("min_tokens")
        if kwargs:
            raise ValueError(f"Unknown arguments: {', '.join(kwargs.keys())}")

        text = _normalize_unicode(text)
        tokens = tokenizer.encode_ordinary(text)

        output: list[tuple[str, dict]] = []
        i = 0
        while i < len(tokens):
            chunk_tokens = tokens[i : i + max_tokens]
            chunk = tokenizer.decode(chunk_tokens)
            last_punctuation = max(
                [chunk.rfind(p) for p in self.PUNCTUATION], default=-1
            )
            if (
                last_punctuation != -1
                and last_punctuation > self.CHARS_PER_TOKEN * min_tokens
            ):
                chunk = chunk[: last_punctuation + 1]
            i += len(tokenizer.encode_ordinary(chunk))
            output.append((chunk, metadata))

        return output

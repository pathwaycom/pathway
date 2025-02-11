# Copyright © 2024 Pathway

"""
A library of text spliiters - routines which slit a long text into smaller chunks.
"""
import abc
import unicodedata

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

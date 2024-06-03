# Copyright © 2024 Pathway

"""
A library of text spliiters - routines which slit a long text into smaller chunks.
"""
import unicodedata

import pathway as pw
from pathway.optional_import import optional_imports


@pw.udf
def null_splitter(txt: str) -> list[tuple[str, dict]]:
    """A splitter which returns its argument as one long text ith null metadata.

    Args:
        txt: text to be split

    Returns:
        list of pairs: chunk text and metadata.

    The null splitter always return a list of length one containing the full text and empty metadata.
    """
    return [(txt, {})]


def _normalize_unicode(text: str):
    """
    Get rid of ligatures
    """
    return unicodedata.normalize("NFKC", text)


class TokenCountSplitter(pw.UDF):
    """
    Splits a given string or a list of strings into chunks based on token
    count.

    This splitter tokenizes the input texts and splits them into smaller parts ("chunks")
    ensuring that each chunk has a token count between `min_tokens` and
    `max_tokens`. It also attempts to break chunks at sensible points such as
    punctuation marks.

    All arguments set default which may be overridden in the UDF call

    Args:
        min_tokens: minimum tokens in a chunk of text.
        max_tokens: maximum size of a chunk in tokens.
        encoding_name: name of the encoding from `tiktoken`.

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

    def __wrapped__(self, txt: str, **kwargs) -> list[tuple[str, dict]]:
        import tiktoken

        kwargs = {**self.kwargs, **kwargs}

        tokenizer = tiktoken.get_encoding(kwargs.pop("encoding_name"))
        max_tokens = kwargs.pop("max_tokens")
        min_tokens = kwargs.pop("min_tokens")

        if kwargs:
            raise ValueError(f"Unknown arguments: {', '.join(kwargs.keys())}")

        text = _normalize_unicode(txt)
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
            output.append((chunk, {}))
        return output

    def __call__(self, text: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """Split given strings into smaller chunks.

        Args:
            - messages (ColumnExpression[str]): Column with texts to be split
            - **kwargs: override for defaults set in the constructor
        """
        return super().__call__(text, **kwargs)

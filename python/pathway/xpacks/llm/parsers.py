# Copyright © 2024 Pathway

"""
A library for document parsers: functions that take raw bytes and return a list of text
chunks along with their metadata.
"""

from collections.abc import Callable
from io import BytesIO
from typing import Any

import pathway as pw


class ParseUtf8(pw.UDFSync):
    def __init__(
        self,
    ):
        pass

    def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        docs: list[tuple[str, dict]] = [(contents.decode("utf-8"), {})]
        return docs


# Based on:
# https://github.com/langchain-ai/langchain/blob/master/libs/langchain/langchain/document_loaders/unstructured.py#L134
# MIT licensed
class ParseUnstructured(pw.UDFSync):
    """
    Parse document using https://unstructured.io/.

    All arguments can be overridden during UDF application.

    Args:
        - mode: single, elements or paged.
          When single, each document is parsed as one long text string.
          When elements, each document is split into unstructured's elements.
          When paged, each pages's text is separately extracted.
        - post_processors: list of callables that will be applied ot all extracted texts.
        - **unstructured_kwargs: extra kwargs to be passed to unstructured.io's `partition` function
    """

    def __init__(
        self,
        mode: str = "single",
        post_processors: list[Callable] | None = None,
        **unstructured_kwargs: Any,
    ):
        # lazy load to prevent unstructured from being a dependency on whole pathway
        try:
            import unstructured.partition.auto  # noqa
        except ImportError:
            raise ValueError(
                "Please install unstructured with all documents support: `pip install unstructured[all-docs]`"
            )

        _valid_modes = {"single", "elements", "paged"}
        if mode not in _valid_modes:
            raise ValueError(
                f"Got {mode} for `mode`, but should be one of `{_valid_modes}`"
            )
        self.kwargs = dict(
            mode=mode,
            post_processors=post_processors or [],
            unstructured_kwargs=unstructured_kwargs,
        )

    def __wrapped__(self, contents: bytes, **kwargs) -> list[tuple[str, dict]]:
        """
        Parse the given document:

        Args:
            - contents: document contents
            - **kwargs: override for defaults set in the constructor

        Returns:
            a list of pairs: text chunk and metadata
        """
        import unstructured.partition.auto

        kwargs = {**self.kwargs, **kwargs}

        elements = unstructured.partition.auto.partition(
            file=BytesIO(contents), **kwargs.pop("unstructured_kwargs")
        )

        post_processors = kwargs.pop("post_processors")
        for element in elements:
            for post_processor in post_processors:
                element.apply(post_processor)

        metadata = {}

        mode = kwargs.pop("mode")

        if kwargs:
            raise ValueError(f"Unknown arguments: {', '.join(kwargs.keys())}")

        if mode == "elements":
            docs: list[tuple[str, dict]] = list()
            for element in elements:
                # NOTE(MthwRobinson) - the attribute check is for backward compatibility
                # with unstructured<0.4.9. The metadata attributed was added in 0.4.9.
                if hasattr(element, "metadata"):
                    metadata.update(element.metadata.to_dict())
                if hasattr(element, "category"):
                    metadata["category"] = element.category
                docs.append((str(element), metadata))
        elif mode == "paged":
            text_dict: dict[int, str] = {}
            meta_dict: dict[int, dict] = {}

            for idx, element in enumerate(elements):
                if hasattr(element, "metadata"):
                    metadata.update(element.metadata.to_dict())
                page_number = metadata.get("page_number", 1)

                # Check if this page_number already exists in docs_dict
                if page_number not in text_dict:
                    # If not, create new entry with initial text and metadata
                    text_dict[page_number] = str(element) + "\n\n"
                    meta_dict[page_number] = metadata
                else:
                    # If exists, append to text and update the metadata
                    text_dict[page_number] += str(element) + "\n\n"
                    meta_dict[page_number].update(metadata)

            # Convert the dict to a list of dicts representing documents
            docs = [(text_dict[key], meta_dict[key]) for key in text_dict.keys()]
        elif mode == "single":
            text = "\n\n".join([str(el) for el in elements])
            docs = [(text, metadata)]
        else:
            raise ValueError(f"mode of {mode} not supported.")
        return docs

# Copyright © 2026 Pathway

"""
A library for document parsers: functions that take raw bytes and return a list of text
chunks along with their metadata.
"""
from __future__ import annotations

import asyncio
import inspect
import io
import logging
import re
import time
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
from functools import partial
from io import BytesIO
from typing import TYPE_CHECKING, Iterable, Iterator, Literal, TypeAlias, get_args

import numpy as np
from pydantic import BaseModel

import pathway as pw
from pathway.internals import udfs
from pathway.internals.config import _check_entitlements
from pathway.io._utils import DurationLike, as_duration_seconds
from pathway.optional_import import optional_imports
from pathway.xpacks.llm import llms, prompts
from pathway.xpacks.llm._utils import _build_async_twelvelabs_client, _prepare_executor
from pathway.xpacks.llm.constants import DEFAULT_VISION_MODEL

if TYPE_CHECKING:
    with optional_imports("xpack-llm-docs"):
        from paddleocr import PaddleOCR, PPStructureV3
        from PIL import Image
        from unstructured.documents.elements import Element
        from unstructured.file_utils.filetype import FileType

logger = logging.getLogger(__name__)


# TODO: fix it
def default_vision_llm() -> pw.UDF:
    return llms.OpenAIChat(
        model=DEFAULT_VISION_MODEL,
        cache_strategy=udfs.DefaultCache(),
        retry_strategy=udfs.ExponentialBackoffRetryStrategy(max_retries=4),
        verbose=True,
    )


class Utf8Parser(pw.UDF):
    """
    Decode text encoded as UTF-8. If the text is type ``str``, return it without any modification.
    """

    async def __wrapped__(self, contents: bytes | str) -> list[tuple[str, dict]]:
        if isinstance(contents, str):
            return [(contents, {})]
        else:
            docs: list[tuple[str, dict]] = [(contents.decode("utf-8"), {})]
            return docs

    def __call__(self, contents: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """
        Parse the given document.

        Args:
            contents: document contents

        Returns:
            A column with a list of pairs for each query. Each pair is a text chunk and
            associated metadata. The metadata is an empty dictionary.
        """
        return super().__call__(contents, **kwargs)


class ParseUtf8(Utf8Parser):

    def __init__(self, *args, **kwargs):
        warnings.warn("This class is deprecated, use `Utf8Parser` instead.")
        super().__init__(*args, **kwargs)


ChunkingMode: TypeAlias = Literal["single", "elements", "paged", "basic", "by_title"]


# Based on:
# https://github.com/langchain-ai/langchain/blob/master/libs/langchain/langchain/document_loaders/unstructured.py#L134
# MIT licensed
class UnstructuredParser(pw.UDF):
    """
    Parse document using `https://unstructured.io/ <https://unstructured.io/>`_.

    All arguments can be overridden during UDF application.

    Args:
        chunking_mode: Mode used to chunk the document.
            When ``"basic"`` it uses default Unstructured's chunking strategy.
            When ``"by_title"``, same as ``"basic"`` but it chunks the document preserving section boundaries.
            When ``"single"``, each document is parsed as one long text string.
            When ``"elements"``, each document is split into Unstructured's elements.
            When ``"paged"``, each page's text is separately extracted.
            Defaults to ``"single"``.
        post_processors: list of callables that will be applied to all extracted texts.
        partition_kwargs: extra kwargs to be passed to unstructured.io's ``partition`` function
        chunking_kwargs: extra kwargs to be passed to unstructured.io's
            ``chunk_elements`` or ``chunk_by_title`` function
    """

    def __init__(
        self,
        chunking_mode: ChunkingMode = "single",
        partition_kwargs: dict = {},
        post_processors: list[Callable] | None = None,
        chunking_kwargs: dict = {},
        cache_strategy: udfs.CacheStrategy | None = None,
    ):
        with optional_imports("xpack-llm-docs"):
            import unstructured.partition.auto  # noqa:F401

        super().__init__(cache_strategy=cache_strategy)

        self._validate_chunking_mode(chunking_mode)

        self.chunking_mode = chunking_mode
        self.partition_kwargs = partition_kwargs
        self.post_processors = post_processors or []
        self.chunking_kwargs = chunking_kwargs

    @staticmethod
    def _validate_chunking_mode(chunking_mode: ChunkingMode):
        if chunking_mode not in get_args(ChunkingMode):
            raise ValueError(
                f"Got {chunking_mode} for `chunking_mode`, but should be one of `{ChunkingMode}`"
            )

    def _combine_metadata(self, left: dict, right: dict) -> dict:
        """
        Combines two metadata dictionaries into one. Used when elements are
        grouped together for chunking.

        This method merges two dictionaries containing metadata information.
        It concatenates the "links" lists from both dictionaries, removes duplicates
        from the "languages" lists, and updates the result dictionary with the remaining
        key-value pairs from both dictionaries. Additionally, it removes the keys
        "coordinates", "parent_id", and "category_depth" from the result as these are
        element specific.

        Args:
            left (dict): The first metadata dictionary.
            right (dict): The second metadata dictionary.

        Returns:
            dict: A dictionary containing the combined metadata.
        """
        result = {}
        links = left.pop("links", []) + right.pop("links", [])
        languages = list(set(left.pop("languages", []) + right.pop("languages", [])))
        result.update(left)
        result.update(right)
        result["links"] = links
        result["languages"] = languages
        result.pop("coordinates", None)
        result.pop("parent_id", None)
        result.pop("category_depth", None)
        result.pop("category", None)
        return result

    @staticmethod
    def _extract_element_meta(element: Element) -> tuple[str, dict]:
        if hasattr(element, "metadata"):
            metadata = element.metadata.to_dict()
        else:
            metadata = {}
        if hasattr(element, "category"):
            metadata["category"] = element.category
        return str(element), metadata

    def _chunk(
        self,
        elements: Iterable[Element],
        chunking_mode: ChunkingMode | None,
        chunking_kwargs: dict,
    ) -> list[tuple[str, dict]]:

        with optional_imports("xpack-llm-docs"):
            from unstructured.chunking.basic import chunk_elements
            from unstructured.chunking.title import chunk_by_title

        docs: list[tuple[str, dict]]

        chunking_mode = chunking_mode or self.chunking_mode

        if chunking_mode == "basic":
            chunked_elements = chunk_elements(elements, **chunking_kwargs)
            docs = [self._extract_element_meta(el) for el in chunked_elements]

        elif chunking_mode == "by_title":
            chunked_elements = chunk_by_title(elements, **chunking_kwargs)
            docs = [self._extract_element_meta(el) for el in chunked_elements]

        elif chunking_mode == "elements":
            docs = [self._extract_element_meta(el) for el in elements]

        elif chunking_mode == "paged":
            text_dict: dict[int, str] = defaultdict(str)
            meta_dict: dict[int, dict] = defaultdict(dict)

            for element in elements:
                el, metadata = self._extract_element_meta(element)
                page_number = metadata.get("page_number", 1)

                # Append text and update metadata for the given page_number
                text_dict[page_number] += el + "\n\n"
                meta_dict[page_number] = self._combine_metadata(
                    meta_dict[page_number], metadata
                )

            # Convert the dict to a list of dicts representing documents
            docs = [
                (text_dict[key], meta_dict[key])
                for key in sorted(list(text_dict.keys()))
            ]

        elif chunking_mode == "single":
            metadata = {}
            for element in elements:
                if hasattr(element, "metadata"):
                    metadata = self._combine_metadata(
                        metadata, element.metadata.to_dict()
                    )
            text = "\n\n".join([str(el) for el in elements])
            docs = [(text, metadata)]

        return docs

    async def __wrapped__(
        self,
        contents: bytes,
        chunking_mode: ChunkingMode | None = None,
        partition_kwargs: dict = {},
        post_processors: list[Callable] | None = None,
        chunking_kwargs: dict = {},
    ) -> list[tuple[str, dict]]:
        """
        Parse the given document:

        Args:
            contents: document contents
            partition_kwargs: extra kwargs to be passed to unstructured.io's ``partition`` function
            post_processors: list of callables that will be applied to all extracted texts.
            chunking_kwargs: extra kwargs to be passed to unstructured.io's ``chunk_elements``
                or ``chunk_by_title`` function

        Returns:
            list[tuple[str, dict]]: list of pairs: text chunk and metadata
                The metadata is obtained from Unstructured, you can check possible values
                in the `Unstructed documentation <https://unstructured-io.github.io/unstructured/metadata.html>`
                Note that when ``chunking_mode`` is set to ``"single"`` or ``"paged"`` some of these fields are
                removed if they are specific to a single element, e.g. ``"category_depth"``.
        """
        with optional_imports("xpack-llm-docs"):
            import unstructured.partition.auto
            from unstructured.documents.elements import Text
            from unstructured.partition.common import UnsupportedFileFormatError

        class FileFormatOrDependencyError(UnsupportedFileFormatError):
            pw_message: str = (
                "Unsupported file format. This error may indicate libmagic (magic) dependency is missing. "
                "Please install it via `apt-get install libmagic1` or `brew install libmagic` (MacOS)."
            )

            def __init__(self, *args, **kwargs):
                super().__init__(self.pw_message, *args, **kwargs)

        partition_kwargs = self.partition_kwargs | dict(partition_kwargs)
        try:
            elements = unstructured.partition.auto.partition(
                file=BytesIO(contents), **partition_kwargs
            )
        except UnsupportedFileFormatError as e:
            raise FileFormatOrDependencyError(*e.args) from e

        post_processors = post_processors or self.post_processors
        for element in elements:
            if isinstance(element, Text):
                for post_processor in post_processors:
                    element.apply(post_processor)

        chunking_kwargs = self.chunking_kwargs | dict(chunking_kwargs)
        docs = self._chunk(elements, chunking_mode, chunking_kwargs)

        return docs

    def __call__(
        self,
        contents: pw.ColumnExpression,
        chunking_mode: pw.ColumnExpression | ChunkingMode | None = None,
        partition_kwargs: pw.ColumnExpression | dict = {},
        post_processors: pw.ColumnExpression | list[Callable] | None = None,
        chunking_kwargs: pw.ColumnExpression | dict = {},
    ) -> pw.ColumnExpression:
        """
        Parse the given document. Providing ``chunking_mode``, ``partition_kwargs``, ``post_processors`` or
        ``chunking_kwargs`` is used for overriding values set during initialization.

        Args:
            contents: document contents
            chunking_mode: Mode used to chunk the document.
            partition_kwargs: extra kwargs to be passed to unstructured.io's ``partition`` function
            post_processors: list of callables that will be applied to all extracted texts.
            chunking_kwargs: extra kwargs to be passed to unstructured.io's ``chunk_elements``
            or ``chunk_by_title`` function

        Returns:
            A column with a list of pairs for each query. Each pair is a text chunk and
            associated metadata.
            The metadata is obtained from Unstructured, you can check possible values
            in the `Unstructed documentation <https://unstructured-io.github.io/unstructured/metadata.html>`
            Note that when ``chunking_mode`` is set to ``"single"`` or ``"paged"`` some of these fields are
            removed if they are specific to a single element, e.g. ``category_depth``.
        """
        return super().__call__(
            contents, chunking_mode, partition_kwargs, post_processors, chunking_kwargs
        )


class ParseUnstructured(UnstructuredParser):

    def __init__(self, *args, **kwargs):
        warnings.warn("This class is deprecated, use `UnstructuredParser` instead.")
        super().__init__(*args, **kwargs)


# uses https://github.com/DS4SD/docling
# MIT licensed
class DoclingParser(pw.UDF):
    """
    Parse PDFs using ``docling`` library.
    This class is a wrapper around the ``DocumentConverter`` from ``docling`` library with some extra
    functionality to also parse images from the PDFs using vision LLMs.

    Args:
        image_parsing_strategy (Literal["llm"] | None): Strategy for parsing images.
            If set to ``"llm"``, images will be replaced with descriptions generated by the vision LLM.
            In that case you have to provide a vision LLM in the ``multimodal_llm`` argument.
            Defaults to None.
        table_parsing_strategy (Literal["docling", "llm"]): Strategy for parsing tables.
            If set to ``"docling"``, tables will be parsed using ``docling`` library.
            If set to ``"llm"``, tables will be replaced with descriptions generated by the vision LLM.
            This description will contain table data parsed with LLM.
            Defaults to ``"docling"``.
        multimodal_llm (llms.OpenAIChat | llms.LiteLLMChat | None): LLM for parsing the image.
            Provided LLM should support image inputs in the same API format as OpenAI does.
            Required if ``parse_images`` is set to ``True``.
        cache_strategy (udfs.CacheStrategy | None): Defines the caching mechanism.
        pdf_pipeline_options (dict): Additional options for the ``DocumentConverter`` from ``docling``.
            These options will be passed to the ``PdfPipelineOptions`` object and will override the defaults
            that are dynamically created based on other arguments set in this constructor.
            See original code for reference:
            https://github.com/DS4SD/docling/blob/main/docling/datamodel/pipeline_options.py#L288
            Keep in mind that you can also change lower-level configurations like TableStructureOptions; e.g.:
            ``pdf_pipeline_options={"table_structure_options": {"mode": "accurate"}}``.
        chunk (bool): Whether to chunk parsed document into smaller structurally coherent parts.
            Under the hood it will use modified ``HybridChunker`` from ``docling`` library.
            Modification has been made to properly handle additional functionality of this class
            that allows to parse images and tables using vision LLMs. It also modifies how tables
            are transformed into text (instead of creating triplets of row, column and value we
            directly transform table into its markdown format).
            All images and tables are split into separate chunks. Each of them contains a caption.
            Chunks that have similar metadata will be merged together.
            As of now, this chunker is not sensitive for length of the chunks (neither if measured
            in characters or tokens). It will chunk the document based only on the structure.
            If set to ``False`` the entire document will be returned as a single chunk.
            Defaults to ``True``.
    """

    if TYPE_CHECKING:
        with optional_imports("xpack-llm-docs"):
            from docling_core.transforms.chunker.hierarchical_chunker import BaseChunk
            from docling_core.types.doc.document import (
                DoclingDocument,
                PictureItem,
                TableItem,
                TextItem,
            )
            from docling_core.types.io import DocumentStream

    def __init__(
        self,
        image_parsing_strategy: Literal["llm"] | None = None,
        table_parsing_strategy: Literal["docling", "llm"] | None = "docling",
        multimodal_llm: llms.OpenAIChat | llms.LiteLLMChat | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        pdf_pipeline_options: dict = {},
        chunk: bool = True,
        *,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
    ):
        with optional_imports("xpack-llm-docs"):
            from docling.datamodel.pipeline_options import (
                PdfPipelineOptions,
                TableStructureOptions,
            )
            from docling.document_converter import (
                DocumentConverter,
                InputFormat,
                PdfFormatOption,
            )

        from pathway.xpacks.llm import _parser_utils

        self.chunker: _parser_utils._HybridChunker | None = None
        self.multimodal_llm: llms.OpenAIChat | llms.LiteLLMChat | None = None

        if image_parsing_strategy == "llm" or table_parsing_strategy == "llm":
            if multimodal_llm is None:
                warnings.warn(
                    "`image_parsing_strategy` is set to `llm`, but `multimodal_llm` is "
                    f"not specified in DoclingParser, defaulting to `{DEFAULT_VISION_MODEL}`."
                )
                self.multimodal_llm = llms.OpenAIChat(
                    model=DEFAULT_VISION_MODEL,
                    cache_strategy=udfs.DefaultCache(),
                    retry_strategy=udfs.ExponentialBackoffRetryStrategy(max_retries=4),
                    verbose=True,
                )
            else:
                self.multimodal_llm = multimodal_llm
        else:
            self.multimodal_llm = None

        self.image_parsing_strategy = image_parsing_strategy
        self.table_parsing_strategy = table_parsing_strategy

        default_table_structure_options = {
            # whether to perform cell matching; small impact on speed, noticeable impact on quality
            "do_cell_matching": True,
            # mode of table parsing; either `fast` or `accurate`; big impact on speed
            # small impact on quality (might be useful for complex tables though)
            "mode": "fast",
        }
        default_pipeline_options = {
            # whether to parse tables or not; big impact on speed
            "do_table_structure": (
                True if table_parsing_strategy == "docling" else False
            ),
            # whether to make images of pages; small impact on speed
            "generate_page_images": True if self.multimodal_llm is not None else False,
            # whether to make images of pictures; small impact on speed
            "generate_picture_images": (
                True if self.multimodal_llm is not None else False
            ),
            "generate_table_images": (
                True if self.multimodal_llm is not None else False
            ),
            # whether to perform OCR; big impact on speed
            "do_ocr": False,
            # scale of images; small impact on speed
            "images_scale": 2,
        }

        pipeline_options = PdfPipelineOptions(
            **(default_pipeline_options | pdf_pipeline_options)
        )
        pipeline_options.table_structure_options = TableStructureOptions(
            **(
                default_table_structure_options
                | pdf_pipeline_options.get("table_structure_options", {})
            )
        )

        # actual docling converter
        self.converter: DocumentConverter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            },
            # TODO: Add more file types
        )

        # modified Docling's chunker
        if chunk:
            self.chunker = _parser_utils._HybridChunker(merge_peers=True)

        executor = _prepare_executor(async_mode=async_mode)
        super().__init__(cache_strategy=cache_strategy, executor=executor)

    def format_chunk(self, chunk: BaseChunk) -> tuple[str, dict]:
        text = ""
        metadata = chunk.meta.export_json_dict()
        headings = metadata.pop("headings", [])
        caption = "\n".join(metadata.pop("captions", []))

        # prepend each heading (with appropriate number of #s) to the text
        if len(headings) > 0:
            text += "HEADINGS:\n"
            for level, heading in enumerate(headings, start=1):
                text += "#" * level + " " + heading + "\n"

        text += "CONTENT:\n" + chunk.text
        # add caption if present
        if caption:
            text += "\n\nCAPTION:\n" + caption

        # move page numbers from the chunk's metadata to be higher in the stack
        metadata["pages"] = sorted(
            {
                prov_item["page_no"]
                for item in metadata["doc_items"]
                for prov_item in item["prov"]
            }
        )

        # no need to keep these in metadata
        metadata.pop("version")
        metadata.pop("schema_name")
        metadata.pop("origin")

        return (text, metadata)

    def format_document(self, doc: DoclingDocument) -> tuple[str, dict]:
        from docling_core.types.doc.document import PictureItem, TableItem, TextItem
        from docling_core.types.doc.labels import DocItemLabel

        text = ""

        for item, level in doc.iterate_items():

            if isinstance(item, TextItem):
                label: DocItemLabel = item.label

                if label in [
                    DocItemLabel.CAPTION,
                    DocItemLabel.PAGE_FOOTER,
                    DocItemLabel.FOOTNOTE,
                ]:
                    continue

                if label == DocItemLabel.TITLE:
                    text += "# "
                if label == DocItemLabel.SECTION_HEADER:
                    text += "#" * (level + 1) + " "

                text += item.text

            elif isinstance(item, TableItem):
                table_df = item.export_to_dataframe()
                text += table_df.to_markdown(index=False)
                captions = [c.text for c in [r.resolve(doc) for r in item.captions]]
                if len(captions):
                    text += "\n\n" + "\n\n".join(captions)

            elif isinstance(item, PictureItem):
                captions = [cap.resolve(doc).text for cap in item.captions]
                if len(captions):
                    text += "\n\n".join(captions)

            else:
                continue

            text += "\n\n"

        return (text, {})

    async def parse_visual_data(
        self,
        b64_imgs: list[str] | str,
        prompt: str = prompts.DEFAULT_IMAGE_PARSE_PROMPT,
    ) -> list[str]:
        """
        Perform OCR using the vision LLM on the given images.
        In this context image could be an actual picture (e.g. of a corgi) or an image of a table.
        Image must be encoded using base 64 format (with the prefix "data:image/jpeg;base64,").

        Args:
            b64_imgs (list[str] | str): List of base64 encoded images.
            prompt (str): The prompt used by the language model for parsing.
        """

        from pathway.xpacks.llm import _parser_utils

        if not self.multimodal_llm:
            raise ValueError(
                "Image parsing is not enabled, cannot replace images with descriptions."
            )

        if isinstance(b64_imgs, str):
            b64_imgs = [b64_imgs]

        # remove the prefix from the base64 encoded images as these are added later on
        b64_imgs = [i.replace("data:image/png;base64,", "") for i in b64_imgs]

        image_descriptions, _ = await _parser_utils._parse_b64_images(
            b64_imgs,
            self.multimodal_llm,
            prompt,
            run_mode="parallel",
            parse_details=False,
            detail_parse_schema=None,
            parse_fn=_parser_utils.parse_image,
            parse_image_details_fn=None,
        )

        return image_descriptions

    async def parse(self, contents: bytes) -> list[tuple[str, dict]]:

        with optional_imports("xpack-llm-docs"):
            from docling_core.transforms.chunker.hierarchical_chunker import BaseChunk
            from docling_core.types.doc.document import PictureItem, TableItem
            from docling_core.types.doc.labels import DocItemLabel
            from docling_core.types.io import DocumentStream

        stream = DocumentStream(name="document.pdf", stream=BytesIO(contents))

        # parse document
        parsing_result = self.converter.convert(stream)
        doc = parsing_result.document

        # analyze images and/or tables with multimodal llm
        if self.multimodal_llm:
            if self.image_parsing_strategy == "llm":
                picture_items: list[PictureItem] = []
                b64_picture_imgs = []
                # collect all docling items that are pictures and their base64 encoded images
                for item, _ in doc.iterate_items():
                    if isinstance(item, PictureItem):
                        if item.image is not None:
                            b64_picture_imgs.append(str(item.image.uri))
                            picture_items.append(item)
                # parse all images in one batch
                picture_descriptions = await self.parse_visual_data(
                    b64_picture_imgs, prompts.DEFAULT_IMAGE_PARSE_PROMPT
                )
                # modify document's items with the parsed descriptions (saved as additional captions)
                for item, description in zip(picture_items, picture_descriptions):
                    new_text_item = doc.add_text(
                        DocItemLabel.CAPTION, description, parent=item
                    )
                    item.captions.append(new_text_item.get_ref())

            if self.table_parsing_strategy == "llm":
                table_items: list[TableItem] = []
                b64_table_imgs = []
                for item, _ in doc.iterate_items():
                    if isinstance(item, TableItem):
                        if item.image is not None:
                            b64_table_imgs.append(str(item.image.uri))
                            table_items.append(item)
                table_descriptions = await self.parse_visual_data(
                    b64_table_imgs, prompts.DEFAULT_TABLE_PARSE_PROMPT
                )
                for item, description in zip(table_items, table_descriptions):
                    new_text_item = doc.add_text(
                        DocItemLabel.CAPTION, description, parent=item
                    )
                    item.captions.append(new_text_item.get_ref())

        # chunk the document
        chunks: list[tuple[str, dict]] = []
        if self.chunker:

            docling_chunks: Iterator[BaseChunk] = self.chunker.chunk(doc)
            for chunk in docling_chunks:
                md_text, meta = self.format_chunk(chunk)
                chunks.append((md_text, meta))

        # if chunking is disabled then format the document into markdown and treat it as one, large chunk
        else:
            md_text, meta = self.format_document(doc)
            chunks.append((md_text, meta))

        return chunks

    async def __wrapped__(self, contents: bytes, **kwargs) -> list[tuple[str, dict]]:
        return await self.parse(contents)


class ImageParser(pw.UDF):
    """
    A class to parse images using vision LLMs.

    Args:
        llm (pw.UDF): LLM for parsing the image. Provided LLM should support image inputs. If not
            provided, an OpenAI LLM will be used.
        parse_prompt: The prompt used by the language model for parsing.
        detail_parse_schema: A schema for detailed parsing, if applicable.
            Providing a Pydantic schema will call the LLM second time to parse necessary information,
            leaving it as None will skip this step.
        downsize_horizontal_width: Width to which images are downsized if necessary.
            Default is 1920.
        include_schema_in_text: If the parsed schema should be included in the ``text`` description.
            May help with search and retrieval. Defaults to ``False``. Only usable if ``detail_parse_schema``
            is provided.
        max_image_size: Maximum allowed size of the images in bytes. Default is 15 MB.
        run_mode: Mode of execution,
            either ``"sequential"`` or ``"parallel"``. Default is ``"parallel"``.
            ``"parallel"`` mode is suggested for speed, but if timeouts or memory usage in local LLMs are concern,
            ``"sequential"`` may be better.
        retry_strategy: Retrying strategy for the LLM calls. Defining a retrying strategy with
            propriety LLMs is strongly suggested.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid :py:class:`~pathway.udfs.CacheStrategy` should be provided.
            Defaults to None.
    """

    parse_fn: Callable
    parse_image_details_fn: Callable | None = None

    def __init__(
        self,
        llm: pw.UDF | None = None,
        parse_prompt: str = prompts.DEFAULT_IMAGE_PARSE_PROMPT,
        detail_parse_schema: type[BaseModel] | None = None,
        include_schema_in_text: bool = False,
        downsize_horizontal_width: int = 1280,
        max_image_size: int = 15 * 1024 * 1024,
        run_mode: Literal["sequential", "parallel"] = "parallel",
        retry_strategy: (
            udfs.AsyncRetryStrategy | None
        ) = udfs.ExponentialBackoffRetryStrategy(max_retries=6),
        cache_strategy: udfs.CacheStrategy | None = None,
        *,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
    ):
        with optional_imports("xpack-llm-docs"):
            from PIL import Image  # noqa:F401
        with optional_imports("xpack-llm"):
            import openai

        from pathway.xpacks.llm import _parser_utils

        executor = _prepare_executor(async_mode=async_mode)

        super().__init__(cache_strategy=cache_strategy, executor=executor)
        if llm is None:
            self.llm = default_vision_llm()
        else:
            self.llm = llm
        self.parse_prompt = parse_prompt
        self.detail_parse_schema = detail_parse_schema
        self.parse_details = self.detail_parse_schema is not None

        if not self.parse_details and include_schema_in_text:
            raise ValueError(
                "`include_schema_in_text` is set to `True` but no `detail_parse_schema` provided. "
                "Please provide a `detail_parse_schema` or set `include_schema_in_text` to `False`."
            )
        self.include_schema_in_text = include_schema_in_text

        self.downsize_horizontal_width = downsize_horizontal_width
        self.max_image_size = max_image_size
        self.run_mode = run_mode
        self.retry_strategy = retry_strategy

        parser_executor: udfs.Executor = udfs.async_executor(
            retry_strategy=retry_strategy
        )

        self.parse_fn = parser_executor._wrap(_parser_utils.parse_image)

        if self.parse_details:
            _schema_parser_executor: udfs.Executor = udfs.async_executor(
                capacity=None, retry_strategy=retry_strategy
            )

            llm_args: dict = llm.kwargs  # type: ignore

            allowed_client_args = inspect.signature(
                openai.AsyncOpenAI.__init__
            ).parameters.keys()

            parse_image_details_fn = partial(
                _parser_utils.parse_image_details,
                model=llm_args["model"],
                openai_client_args={
                    key: llm_args[key]
                    for key in llm_args.keys()
                    if key in allowed_client_args
                },
            )

            self.parse_image_details_fn = _schema_parser_executor._wrap(
                parse_image_details_fn
            )

    async def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        """Parse image bytes with GPT-v model."""

        from PIL import Image

        from pathway.xpacks.llm import _parser_utils

        images: list[Image.Image] = [Image.open(BytesIO(contents))]

        logger.info("`ImageParser` applying `maybe_downscale`.")

        images = [
            _parser_utils.maybe_downscale(
                img, self.max_image_size, self.downsize_horizontal_width
            )
            for img in images
        ]

        parsed_content, parsed_details = await _parser_utils.parse_images(
            images,
            self.llm,
            self.parse_prompt,
            run_mode=self.run_mode,
            parse_details=self.parse_details,
            detail_parse_schema=self.detail_parse_schema,
            parse_fn=self.parse_fn,
            parse_image_details_fn=self.parse_image_details_fn,
        )

        logger.info(
            f"ImageParser completed parsing, total number of images: {len(parsed_content)}"
        )

        docs = [
            (
                (
                    i
                    if not self.include_schema_in_text
                    else (i + "\n" + parsed_details[idx].model_dump_json())
                ),
                {
                    **(parsed_details[idx].model_dump() if self.parse_details else {}),
                },
            )
            for idx, i in enumerate(parsed_content)
        ]

        return docs


class SlideParser(pw.UDF):
    """
    A class to parse PPTX and PDF slides using vision LLMs.

    Use of this class requires Pathway Live Data Framework Scale account.
    Get your license `here <https://pathway.com/framework/get-license>`_ to gain access.

    Args:
        llm: LLM for parsing the image. Provided LLM should support image inputs. If not
            provided, an OpenAI LLM will be used.
        parse_prompt: The prompt used by the language model for parsing.
        detail_parse_schema: A schema for detailed parsing, if applicable.
            Providing a Pydantic schema will call the LLM second time to parse necessary information,
            leaving it as None will skip this step.
        include_schema_in_text: If the parsed schema should be included in the ``text`` description.
            May help with search and retrieval. Defaults to ``False``.
            Only usable if ``detail_parse_schema`` is provided.
        intermediate_image_format: Intermediate image format used when converting PDFs to images.
            Defaults to ``"jpg"`` for speed and memory use.
        image_size (tuple[int, int], optional): The target size of the images. Default is (1280, 720).
            Note that setting higher resolution will increase the cost and latency.
            Since vision LLMs will resize the given image into certain resolution, setting high resolutions
            may not help with the accuracy.
        run_mode: Mode of execution,
            either ``"sequential"`` or ``"parallel"``. Default is ``"parallel"``.
            ``"parallel"`` mode is suggested for speed, but if timeouts or memory usage in local LLMs are concern,
            ``"sequential"`` may be better.
        retry_strategy: Retrying strategy for the LLM calls. Defining a retrying strategy with
            propriety LLMs is strongly suggested.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid :py:class:`~pathway.udfs.CacheStrategy` should be provided.
            Defaults to None.
    """

    parse_fn: Callable
    parse_image_details_fn: Callable | None = None

    def __init__(
        self,
        llm: pw.UDF | None = None,
        parse_prompt: str = prompts.DEFAULT_IMAGE_PARSE_PROMPT,
        detail_parse_schema: type[BaseModel] | None = None,
        include_schema_in_text: bool = False,
        intermediate_image_format: str = "jpg",
        image_size: tuple[int, int] = (1280, 720),
        run_mode: Literal["sequential", "parallel"] = "parallel",
        retry_strategy: (
            udfs.AsyncRetryStrategy | None
        ) = udfs.ExponentialBackoffRetryStrategy(max_retries=6),
        cache_strategy: udfs.CacheStrategy | None = None,
        *,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
    ):
        _check_entitlements("advanced-parser")

        with optional_imports("xpack-llm-docs"):
            from pdf2image import convert_from_bytes  # noqa:F401
            from unstructured.file_utils.filetype import (  # noqa:F401
                FileType,
                detect_filetype,
            )
        with optional_imports("xpack-llm"):
            import openai

        from pathway.xpacks.llm import _parser_utils

        executor = _prepare_executor(async_mode=async_mode)

        super().__init__(cache_strategy=cache_strategy, executor=executor)
        if llm is None:
            self.llm = default_vision_llm()
        else:
            self.llm = llm
        self.parse_prompt = parse_prompt
        self.detail_parse_schema = detail_parse_schema
        self.intermediate_image_format = intermediate_image_format
        self.image_size = image_size
        self.parse_details = self.detail_parse_schema is not None

        if not self.parse_details and include_schema_in_text:
            raise ValueError(
                "`include_schema_in_text` is set to `True` but no `detail_parse_schema` provided. "
                "Please provide a `detail_parse_schema` or set `include_schema_in_text` to `False`."
            )

        self.include_schema_in_text = include_schema_in_text
        self.run_mode = run_mode
        self.retry_strategy = retry_strategy

        _parser_executor: udfs.Executor = udfs.async_executor(
            capacity=None, retry_strategy=retry_strategy
        )

        self.parse_fn = _parser_executor._wrap(_parser_utils.parse_image)

        if self.parse_details:
            _schema_parser_executor: udfs.Executor = udfs.async_executor(
                capacity=None, retry_strategy=retry_strategy
            )

            llm_args: dict = llm.kwargs  # type: ignore

            allowed_client_args = inspect.signature(
                openai.AsyncOpenAI.__init__
            ).parameters.keys()

            parse_image_details_fn = partial(
                _parser_utils.parse_image_details,
                model=llm_args["model"],
                openai_client_args={
                    key: llm_args[key]
                    for key in llm_args.keys()
                    if key in allowed_client_args
                },
            )

            self.parse_image_details_fn = _schema_parser_executor._wrap(
                parse_image_details_fn
            )

    async def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        """Parse slides with GPT-v model by converting to images."""

        from pdf2image import convert_from_bytes
        from unstructured.file_utils.filetype import FileType, detect_filetype

        from pathway.xpacks.llm import _parser_utils

        byte_file = io.BytesIO(contents)
        filetype = detect_filetype(
            file=byte_file,
        )

        if filetype == FileType.PPTX:
            logger.info("`SlideParser` converting PPTX to PDF from byte object.")
            contents = _parser_utils._convert_pptx_to_pdf(contents)

        logger.info("`SlideParser` converting PDFs to images from byte object.")

        try:
            images = convert_from_bytes(
                contents, fmt=self.intermediate_image_format, size=self.image_size
            )
        except Exception:
            logger.info(
                f"Failed to extract images in `{self.intermediate_image_format}` format, "
                "trying with the default format."
            )
            images = convert_from_bytes(contents, size=self.image_size)

        b64_images = [_parser_utils.img_to_b64(image) for image in images]

        parsed_content, parsed_details = await _parser_utils._parse_b64_images(
            b64_images,
            self.llm,
            self.parse_prompt,
            run_mode=self.run_mode,
            parse_details=self.parse_details,
            detail_parse_schema=self.detail_parse_schema,
            parse_fn=self.parse_fn,
            parse_image_details_fn=self.parse_image_details_fn,
        )

        logger.info(
            f"ImageParser completed parsing, total number of images: {len(parsed_content)}"
        )

        page_count = len(images)

        docs = [
            (
                (
                    i
                    if not self.include_schema_in_text
                    else (i + "\n" + parsed_details[idx].model_dump_json())
                ),
                {
                    "b64_image": b64_images[idx],
                    "image_page": idx,
                    "tot_pages": page_count,
                    **(parsed_details[idx].model_dump() if self.parse_details else {}),
                },
            )
            for idx, i in enumerate(parsed_content)
        ]

        return docs


class PypdfParser(pw.UDF):
    """
    Parse PDF document using ``pypdf`` library.
    Optionally, applies additional text cleanups for readability.

    Args:
        apply_text_cleanup: Apply text cleanup for line breaks and repeated spaces.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid :py:class:`~pathway.udfs.CacheStrategy` should be provided.
            Defaults to None.
    """

    def __init__(
        self,
        apply_text_cleanup: bool = True,
        cache_strategy: udfs.CacheStrategy | None = None,
    ):
        with optional_imports("xpack-llm-docs"):
            from pypdf import PdfReader  # noqa:F401

        super().__init__(cache_strategy=cache_strategy)
        self.apply_text_cleanup = apply_text_cleanup

    def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        from pypdf import PdfReader

        pdf = PdfReader(stream=BytesIO(contents))

        docs: list[tuple[str, dict]] = []
        file_metadata: dict = {}

        logger.info(
            f"PypdfParser starting to parse a document of length: {len(pdf.pages)}"
        )

        for page in pdf.pages:
            text: str = page.extract_text()

            if self.apply_text_cleanup:
                text = self._clean_text(text)

            page_metadata: dict = file_metadata | {"page_number": page.page_number}

            docs.append((text, page_metadata))

        logger.info(
            f"PypdfParser completed parsing, total number of pages: {len(pdf.pages)}"
        )

        return docs

    def _clean_text(self, text: str):
        text_wo_lines = self._clean_text_lines(text)
        simplified_text = self._remove_empty_space(text_wo_lines)
        formatted_text = self._replace_newline_with_space_if_lower(simplified_text)
        return formatted_text

    def _clean_text_lines(self, text: str) -> str:
        return re.sub(
            r"(?<=\n)\s*([A-Z][^ ]*|[\d][^ ]*)", lambda m: m.group(1), text
        ).replace("\n ", "\n")

    def _remove_empty_space(self, text: str) -> str:
        return text.replace("   ", " ")

    def _replace_newline_with_space_if_lower(self, text: str) -> str:
        """Remove unnecessary line breaks."""

        def replace_newline(match: re.Match):
            if match.group(1).islower():
                return " " + match.group(1)
            return "\n" + match.group(1)

        modified_text = re.sub(r"\n(\w)", replace_newline, text)
        return modified_text


class _PaddleParser(ABC):
    """
    Abstract wrapper for Paddle pipeline, that extracts text from OCR results.
    """

    pipeline: PaddleOCR | PPStructureV3

    def __init__(self, pipeline: PaddleOCR | PPStructureV3):
        self.pipeline = pipeline

    def parse(self, image: np.ndarray) -> str:
        ocr_result = self.pipeline.predict(image)
        return self.extract_text(ocr_result)

    @abstractmethod
    def extract_text(self, ocr_result: list) -> str:
        pass

    @staticmethod
    def create_for(pipeline: PaddleOCR | PPStructureV3) -> _PaddleParser:
        with optional_imports("xpack-llm-docs"):
            from paddleocr import PaddleOCR, PPStructureV3

        match pipeline:
            case PPStructureV3():
                return _PaddlePPStructureV3Parser(pipeline)
            case PaddleOCR():
                return _PaddleOCRParser(pipeline)
            case _:
                raise NotImplementedError(
                    f"Extractor for {type(pipeline)} is not implemented."
                )


class _PaddlePPStructureV3Parser(_PaddleParser):
    def extract_text(self, ocr_result: list) -> str:
        pages = []

        for res in ocr_result:
            try:
                pages.append(res.markdown)
            except AttributeError:
                logger.error("Failed to extract text from OCR result.")
                continue

        result = self.pipeline.concatenate_markdown_pages(pages)

        return result


class _PaddleOCRParser(_PaddleParser):
    def extract_text(self, ocr_result: list) -> str:
        result = ""
        for res in ocr_result:
            try:
                text = res["rec_texts"]
                result += " ".join(text) + "\n\n"
            except KeyError:
                logger.error("Failed to extract text from OCR result.")
                continue
        return result


class PaddleOCRParser(pw.UDF):
    """
    A class to parse images, PDFs and PPTX slides using PaddleOCR.

    PaddleOCRParser requires ``paddlepaddle`` to be installed, with the version depending on your hardware.
    If you want to run the OCR on CPU, install it with ``pip install paddlepaddle>=3.2.0``.
    For GPU support, follow the instructions on the
    `official site <https://www.paddlepaddle.org.cn/en/install/quick>`_.

    Args:
        pipeline: A Paddle pipeline object. Currently PaddleOCR and PPStructureV3 are supported.
            If not provided, a default PPStructureV3 pipeline will be used.
            Use PPStructureV3 for better accuracy on documents with complex layouts. PaddleOCR can be used for
            simpler documents, extracting only text but may be faster.
        concatenate_pages: Whether to concatenate multi-paged documents into a single output. Defaults to False.
        intermediate_image_format: Intermediate image format used when converting PDFs to images.
            Defaults to ``"jpg"`` for speed and memory use.
        max_image_size: Maximum allowed size of the images in bytes. Default is 15 MB.
        downsize_horizontal_width: Width to which images are downsized if necessary.
            Default is 1920.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid :py:class:`~pathway.udfs.CacheStrategy` should be provided.
            Defaults to None.
        async_mode: Mode of execution for the UDF, either ``"batch_async"`` or ``"fully_async"``.
            Default is ``"batch_async"``.
    """

    parser: _PaddleParser
    intermediate_image_format: str
    max_image_size: int
    downsize_horizontal_width: int

    def __init__(
        self,
        pipeline: PaddleOCR | PPStructureV3 | None = None,
        *,
        concatenate_pages: bool = False,
        intermediate_image_format: str = "jpg",
        max_image_size: int = 15 * 1024 * 1024,
        downsize_horizontal_width: int = 1920,
        cache_strategy: udfs.CacheStrategy | None = None,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
    ):
        super().__init__(
            executor=_prepare_executor(async_mode=async_mode),
            cache_strategy=cache_strategy,
        )

        with optional_imports("xpack-llm-docs"):
            import paddleocr  # noqa:F401
            from pdf2image import convert_from_bytes  # noqa:F401
            from PIL import Image  # noqa:F401
            from unstructured.file_utils.filetype import (  # noqa:F401
                FileType,
                detect_filetype,
            )

        try:
            import paddle  # noqa:F401
        except ImportError as e:
            raise ImportError(
                "PaddleOCRParser requires `paddlepaddle` to be installed, "
                "with the version depending on your hardware. "
                "If you want to run the OCR on CPU, install it with `pip install paddlepaddle>=3.2.0`. "
                "For GPU support, follow the instructions at https://www.paddlepaddle.org.cn/en/install/quick."
            ) from e

        self.intermediate_image_format = intermediate_image_format
        self.max_image_size = max_image_size
        self.downsize_horizontal_width = downsize_horizontal_width
        self.concatenate_pages = concatenate_pages

        if pipeline is None:
            pipeline = self._default_pipeline()

        self.parser = _PaddleParser.create_for(pipeline)

    def _default_pipeline(self) -> PPStructureV3:
        with optional_imports("xpack-llm-docs"):
            from paddleocr import PPStructureV3
        return PPStructureV3(
            use_table_recognition=False,
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            use_seal_recognition=False,
            use_formula_recognition=False,
            use_chart_recognition=False,
            use_region_detection=False,
        )

    def _normalize_input(
        self,
        contents: bytes,
    ) -> tuple[list[Image.Image], FileType | None]:
        from pdf2image import convert_from_bytes
        from PIL import Image
        from unstructured.file_utils.filetype import FileType, detect_filetype

        from pathway.xpacks.llm import _parser_utils

        byte_file = io.BytesIO(contents)
        filetype = detect_filetype(file=byte_file)

        match filetype:
            case FileType.PPT | FileType.PPTX:
                contents = _parser_utils._convert_pptx_to_pdf(contents)
                images = convert_from_bytes(
                    contents, fmt=self.intermediate_image_format
                )
            case FileType.PDF:
                images = convert_from_bytes(
                    contents, fmt=self.intermediate_image_format
                )
            case _ as filetype:
                try:
                    images = [Image.open(io.BytesIO(contents)).convert("RGB")]
                except Exception as e:
                    logger.error(f"Failed to parse provided file. Reason: {e}")
                    return [], None

        images = [
            _parser_utils.maybe_downscale(
                img,
                max_image_size=self.max_image_size,
                downsize_horizontal_width=self.downsize_horizontal_width,
            )
            for img in images
        ]

        return images, filetype

    async def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        from unstructured.file_utils.filetype import FileType

        images, original_filetype = self._normalize_input(contents)

        def metadata(page_number: int) -> dict:
            if original_filetype in [FileType.PPT, FileType.PPTX, FileType.PDF]:
                return {"page_number": page_number}
            return {}

        docs = []

        for i, image in enumerate(images):
            try:
                img_np = np.array(image)
                text = self.parser.parse(img_np)
                docs.append((text, metadata(i)))
            except Exception as e:
                logger.error(f"Failed to process an image. Reason: {e}")
                continue

        if self.concatenate_pages and len(docs) > 1:
            concatenated_text = "\n\n".join([doc[0] for doc in docs])
            docs = [(concatenated_text, {"page_number": 0})]

        return docs


class AudioParser(pw.UDF):
    """
    Parse audio using OpenAI's Whisper API.

    Args:
        model: Whisper model to use (default: "whisper-1").
        api_key: OpenAI API key.
        base_url: OpenAI Base URL.
        capacity: Maximum number of concurrent operations.
        retry_strategy: Retrying strategy for the LLM calls. Defining a retrying strategy with
            propriety LLMs is strongly suggested.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid :py:class:`~pathway.udfs.CacheStrategy` should be provided.
            Defaults to None.
        async_mode: Mode of execution for the UDF, either ``"batch_async"`` or ``"fully_async"``.
            Default is ``"batch_async"``.
        **kwargs: Additional arguments for ``audio.transcriptions.create``.
    """

    def __init__(
        self,
        audio_format: Literal[
            "mp3", "mp4", "mpeg", "mpga", "m4a", "wav", "webm"
        ] = "mp3",
        model: str = "whisper-1",
        api_key: str | None = None,
        base_url: str | None = None,
        capacity: int | None = None,
        retry_strategy: udfs.AsyncRetryStrategy | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
        **kwargs,
    ):
        with optional_imports("xpack-llm"):
            import openai

        executor = _prepare_executor(
            async_mode, capacity=capacity, retry_strategy=retry_strategy
        )
        super().__init__(executor=executor, cache_strategy=cache_strategy)

        self.client = openai.AsyncOpenAI(
            api_key=api_key, base_url=base_url, max_retries=0
        )
        self.model = model
        self.audio_format = audio_format
        self.kwargs = kwargs

    async def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        transcript = await self.client.audio.transcriptions.create(
            model=self.model,
            file=(f"audio.{self.audio_format}", contents),
            **self.kwargs,
        )
        text = getattr(transcript, "text", str(transcript))
        return [(text, {})]


DEFAULT_PEGASUS_MODEL = "pegasus1.5"
DEFAULT_PEGASUS_PROMPT = (
    "Describe this video in detail. Summarize what happens, who and what appears, "
    "the setting, any spoken or on-screen text, and the overall topic. "
    "Write the description so it can be used to answer questions about the video."
)
# Pegasus input limits (https://docs.twelvelabs.io/docs/concepts/models/pegasus):
# duration 4 s - 2 h, file size <= 2 GB, resolution 360x360 - 5184x2160.
_PEGASUS_MAX_VIDEO_BYTES = 2 * 1024**3


class TwelveLabsVideoParser(pw.UDF):
    """Parse videos into text using the TwelveLabs Pegasus model.

    The parser uploads the incoming video bytes to TwelveLabs as an asset, waits
    for the asset to be ready, and then asks Pegasus to produce a textual
    description of the video using ``prompt``. The returned text is suitable for
    chunking, embedding and indexing by the standard Pathway RAG components.

    Pegasus accepts videos between 4 seconds and 2 hours long, up to 2 GB in
    size, with resolution between 360x360 and 5184x2160, in any FFmpeg-supported
    container. Videos larger than 2 GB are rejected by the parser before the
    upload; the other limits are enforced by the TwelveLabs API.

    By default the uploaded asset is deleted once the analysis finishes (even if
    the analysis fails), so repeated runs do not flood the TwelveLabs asset list.
    Set ``delete_assets=False`` to keep the assets around for reuse or
    inspection; in that case the emitted ``twelvelabs_asset_id`` metadata refers
    to a live, retrievable asset.

    Parsing a video takes minutes and costs money, so in production it is
    recommended to persist the results with
    ``cache_strategy=pw.udfs.DiskCache()`` — otherwise every restart of the
    pipeline re-parses all videos.

    Args:
        prompt: Instruction sent to Pegasus describing what to extract from the
            video. Defaults to a generic, RAG-oriented description prompt.
        model: Pegasus model name. Defaults to ``"pegasus1.5"``.
        api_key: TwelveLabs API key. If ``None``, the SDK reads it from the
            ``TWELVELABS_API_KEY`` environment variable.
        max_tokens: Maximum number of tokens Pegasus may generate. Defaults to 2048.
        temperature: Sampling temperature for Pegasus. Defaults to ``None`` (SDK default).
        video_format: Container format of the incoming videos, used as the
            filename extension of the uploaded asset. Any FFmpeg-supported
            format (e.g. ``"mp4"``, ``"webm"``, ``"mov"``). Defaults to ``"mp4"``.
        asset_poll_interval: Time between asset-readiness checks, given as a number
            of seconds or a ``datetime.timedelta`` / ``pw.Duration``. Defaults to 5.
        asset_timeout: Maximum time to wait for an uploaded asset to become ready
            before raising, given as a number of seconds or a ``datetime.timedelta``
            / ``pw.Duration``. Defaults to 600 seconds.
        delete_assets: If ``True`` (the default), the uploaded asset is deleted
            after the analysis completes, so repeated runs do not accumulate
            assets in your TwelveLabs account. When ``True``, the emitted
            ``twelvelabs_asset_id`` metadata is omitted because the asset no
            longer exists. Set to ``False`` to keep assets (e.g. for reuse or
            debugging), in which case the id is included in the metadata.
        capacity: Maximum number of videos processed concurrently. Defaults to
            ``None`` (no specific limit).
        retry_strategy: Strategy for handling retries in case of failures.
            Retries are applied per video, before ``on_error`` is consulted.
            Note that a retry re-runs the whole parse, including the video
            upload. Defaults to
            :py:class:`~pathway.udfs.ExponentialBackoffRetryStrategy`.
        on_error: What to do when parsing a video fails after all retries:
            ``"raise"`` (the default) propagates the error and fails the
            pipeline, ``"skip"`` logs the error and produces no chunks for the
            failed video, letting the rest of the pipeline continue. Use
            ``"skip"`` in production pipelines where a single malformed video
            must not halt processing.
        cache_strategy: Pathway caching strategy. To enable caching, pass a valid
            :py:class:`~pathway.udfs.CacheStrategy`. Defaults to ``None``.
        async_mode: Mode of execution for the UDF, either ``"batch_async"`` or
            ``"fully_async"``. In the default ``"batch_async"`` mode a minibatch
            waits for all of its videos to finish; ``"fully_async"`` lets the
            rest of the pipeline proceed while videos are being parsed, which
            suits the minutes-long parse times better.

    Example:

    >>> import pathway as pw  # doctest: +SKIP
    >>> from pathway.xpacks.llm.parsers import TwelveLabsVideoParser  # doctest: +SKIP
    >>> parser = TwelveLabsVideoParser(  # doctest: +SKIP
    ...     cache_strategy=pw.udfs.DiskCache(),  # don't re-parse videos on restarts
    ...     on_error="skip",  # a single broken video must not halt the pipeline
    ... )
    """

    def __init__(
        self,
        prompt: str = DEFAULT_PEGASUS_PROMPT,
        model: str = DEFAULT_PEGASUS_MODEL,
        api_key: str | None = None,
        max_tokens: int = 2048,
        temperature: float | None = None,
        video_format: str = "mp4",
        asset_poll_interval: DurationLike = 5.0,
        asset_timeout: DurationLike = 600.0,
        delete_assets: bool = True,
        capacity: int | None = None,
        retry_strategy: (
            udfs.AsyncRetryStrategy | None
        ) = udfs.ExponentialBackoffRetryStrategy(max_retries=4),
        on_error: Literal["raise", "skip"] = "raise",
        cache_strategy: udfs.CacheStrategy | None = None,
        async_mode: Literal["batch_async", "fully_async"] = "batch_async",
    ):
        _check_entitlements("advanced-parser")
        # Retries are applied inside `__wrapped__` (see there) rather than at the
        # executor level, so that `on_error="skip"` kicks in only after the
        # retries are exhausted.
        executor = _prepare_executor(async_mode, capacity=capacity)
        super().__init__(executor=executor, cache_strategy=cache_strategy)
        self.prompt = prompt
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.video_format = video_format
        self.asset_poll_interval = as_duration_seconds(
            asset_poll_interval, "asset_poll_interval"
        )
        self.asset_timeout = as_duration_seconds(
            asset_timeout, "asset_timeout", allow_zero=False
        )
        self.delete_assets = delete_assets
        self.retry_strategy = retry_strategy or udfs.NoRetryStrategy()
        self.on_error = on_error
        self._api_key = api_key
        self._aclient = None

    @property
    def aclient(self):
        if self._aclient is None:
            self._aclient = _build_async_twelvelabs_client(self._api_key)
        return self._aclient

    async def _upload_asset(self, contents: bytes) -> str:
        """Upload video bytes and return the asset id once it is ready."""

        filename = f"video.{self.video_format}"
        asset = await self.aclient.assets.create(
            method="direct", file=(filename, contents), filename=filename
        )
        deadline = time.monotonic() + self.asset_timeout
        while asset.status not in ("ready", "failed"):
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"TwelveLabs asset {asset.id} was not ready after "
                    f"{self.asset_timeout}s (last status: {asset.status})."
                )
            await asyncio.sleep(self.asset_poll_interval)
            asset = await self.aclient.assets.retrieve(asset.id)
        if asset.status == "failed":
            raise RuntimeError(f"TwelveLabs asset {asset.id} failed to process.")
        return asset.id

    async def __wrapped__(self, contents: bytes, **kwargs) -> list[tuple[str, dict]]:
        try:
            if len(contents) > _PEGASUS_MAX_VIDEO_BYTES:
                raise ValueError(
                    f"Video is {len(contents)} bytes; Pegasus accepts videos up "
                    f"to {_PEGASUS_MAX_VIDEO_BYTES} bytes (2 GB)."
                )
            return await self.retry_strategy.invoke(self._parse, contents)
        except Exception:
            if self.on_error == "skip":
                logger.error(
                    "Failed to parse a video with TwelveLabs; skipping it.",
                    exc_info=True,
                )
                return []
            raise

    async def _parse(self, contents: bytes) -> list[tuple[str, dict]]:
        from twelvelabs.types.video_context import VideoContext_AssetId

        asset_id = await self._upload_asset(contents)
        try:
            logger.info("Analyzing TwelveLabs asset %s with Pegasus...", asset_id)
            analyze_kwargs: dict = dict(
                model_name=self.model,
                video=VideoContext_AssetId(asset_id=asset_id),
                prompt=self.prompt,
                max_tokens=self.max_tokens,
            )
            if self.temperature is not None:
                analyze_kwargs["temperature"] = self.temperature
            response = await self.aclient.analyze(**analyze_kwargs)
            text = response.data or ""
            if not text:
                logger.warning(
                    "Pegasus returned no text for TwelveLabs asset %s; "
                    "the video will produce an empty document.",
                    asset_id,
                )
        finally:
            if self.delete_assets:
                # Remove the per-run asset so repeated runs do not flood the
                # TwelveLabs asset list. Best-effort: a cleanup failure must not
                # mask the analysis result (or an analysis error above).
                try:
                    await self.aclient.assets.delete(asset_id)
                except Exception:  # noqa: BLE001
                    logger.warning("Failed to delete TwelveLabs asset %s.", asset_id)
        # When the asset has been deleted the id no longer resolves, so only
        # surface it in the metadata when the asset is kept around.
        metadata = {} if self.delete_assets else {"twelvelabs_asset_id": asset_id}
        return [(text, metadata)]

    def __call__(self, contents: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """Parse the video document.

        Args:
            contents: Column with the raw bytes of each video.

        Returns:
            A column with a list of ``(text, metadata)`` pairs for each video.
            When ``delete_assets=False`` the metadata records the TwelveLabs
            ``twelvelabs_asset_id`` used for the analysis; with the default
            ``delete_assets=True`` the asset is removed afterwards and the id is
            omitted (it would no longer resolve).
        """
        return super().__call__(contents, **kwargs)

# Copyright © 2024 Pathway

"""
A library for document parsers: functions that take raw bytes and return a list of text
chunks along with their metadata.
"""
from __future__ import annotations

import inspect
import io
import logging
import re
import warnings
from collections import defaultdict
from collections.abc import Callable
from functools import partial
from io import BytesIO
from typing import TYPE_CHECKING, Iterable, Iterator, Literal, TypeAlias, get_args

from PIL import Image
from pydantic import BaseModel

import pathway as pw
from pathway.internals import udfs
from pathway.internals.config import _check_entitlements
from pathway.optional_import import optional_imports
from pathway.xpacks.llm import llms, prompts
from pathway.xpacks.llm._utils import _prepare_executor
from pathway.xpacks.llm.constants import DEFAULT_VISION_MODEL

if TYPE_CHECKING:
    with optional_imports("xpack-llm-docs"):
        from unstructured.documents.elements import Element

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
    Decode text encoded as UTF-8. If the text is type `str`, return it without any modification.
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
    Parse PDFs using `docling` library.
    This class is a wrapper around the `DocumentConverter` from `docling` library with some extra
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
            If set to False the entire document will be returned as a single chunk.
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
                if item.label in [
                    DocItemLabel.CAPTION,
                    DocItemLabel.PAGE_FOOTER,
                    DocItemLabel.FOOTNOTE,
                ]:
                    continue

                if item.label == DocItemLabel.TITLE:
                    text += "# "
                if item.label == DocItemLabel.SECTION_HEADER:  # type: ignore
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
            a valid :py:class:``~pathway.udfs.CacheStrategy`` should be provided.
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

    Use of this class requires Pathway Scale account.
    Get your license `here <https://pathway.com/get-license>`_ to gain access.

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
            a valid :py:class:``~pathway.udfs.CacheStrategy`` should be provided.
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
            a valid :py:class:``~pathway.udfs.CacheStrategy`` should be provided.
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

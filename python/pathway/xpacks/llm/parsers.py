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
from collections.abc import Callable
from functools import partial
from io import BytesIO
from typing import Any, Literal

from PIL import Image
from pydantic import BaseModel

import pathway as pw
from pathway.internals import udfs
from pathway.internals.config import _check_entitlements
from pathway.optional_import import optional_imports
from pathway.xpacks.llm import _parser_utils, llms, prompts
from pathway.xpacks.llm.constants import DEFAULT_VISION_MODEL

logger = logging.getLogger(__name__)


# TODO: fix it
DEFAULT_VISION_LLM = llms.OpenAIChat(
    model=DEFAULT_VISION_MODEL,
    cache_strategy=udfs.DefaultCache(),
    retry_strategy=udfs.ExponentialBackoffRetryStrategy(max_retries=4),
    verbose=True,
)


class Utf8Parser(pw.UDF):
    """
    Decode text encoded as UTF-8.
    """

    async def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
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


# Based on:
# https://github.com/langchain-ai/langchain/blob/master/libs/langchain/langchain/document_loaders/unstructured.py#L134
# MIT licensed
class UnstructuredParser(pw.UDF):
    """
    Parse document using `https://unstructured.io/ <https://unstructured.io/>`_.

    All arguments can be overridden during UDF application.

    Args:
        mode: single, elements or paged.
          When single, each document is parsed as one long text string.
          When elements, each document is split into unstructured's elements.
          When paged, each pages's text is separately extracted.
        post_processors: list of callables that will be applied to all extracted texts.
        **unstructured_kwargs: extra kwargs to be passed to unstructured.io's `partition` function
    """

    def __init__(
        self,
        mode: Literal["single", "elements", "paged"] = "single",
        post_processors: list[Callable] | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        **unstructured_kwargs: Any,
    ):
        with optional_imports("xpack-llm-docs"):
            import unstructured.partition.auto  # noqa:F401

        super().__init__(cache_strategy=cache_strategy)
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

    # `links` and `languages` in metadata are lists, so their content should be added.
    # We don't want return `coordinates`, `parent_id` and `category_depth` - these are
    # element specific (i.e. they can differ for elements on the same page)
    def _combine_metadata(self, left: dict, right: dict) -> dict:
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
        return result

    async def __wrapped__(self, contents: bytes, **kwargs) -> list[tuple[str, dict]]:
        """
        Parse the given document:

        Args:
            contents: document contents
            **kwargs: override for defaults set in the constructor

        Returns:
            a list of pairs: text chunk and metadata
            The metadata is obtained from Unstructured, you can check possible values
            in the `Unstructed documentation <https://unstructured-io.github.io/unstructured/metadata.html>`
            Note that when `mode` is set to `single` or `paged` some of these fields are
            removed if they are specific to a single element, e.g. `category_depth`.
        """
        import unstructured.partition.auto
        from unstructured.documents.elements import Text

        kwargs = {**self.kwargs, **kwargs}

        elements = unstructured.partition.auto.partition(
            file=BytesIO(contents), **kwargs.pop("unstructured_kwargs")
        )

        post_processors = kwargs.pop("post_processors")
        for element in elements:
            assert isinstance(element, Text), "unsupported element type"
            for post_processor in post_processors:
                element.apply(post_processor)

        mode = kwargs.pop("mode")

        if kwargs:
            raise ValueError(f"Unknown arguments: {', '.join(kwargs.keys())}")

        if mode == "elements":
            docs: list[tuple[str, dict]] = list()
            for element in elements:
                # NOTE(MthwRobinson) - the attribute check is for backward compatibility
                # with unstructured<0.4.9. The metadata attributed was added in 0.4.9.
                if hasattr(element, "metadata"):
                    metadata = element.metadata.to_dict()
                else:
                    metadata = {}
                if hasattr(element, "category"):
                    metadata["category"] = element.category
                docs.append((str(element), metadata))
        elif mode == "paged":
            text_dict: dict[int, str] = {}
            meta_dict: dict[int, dict] = {}

            for idx, element in enumerate(elements):
                if hasattr(element, "metadata"):
                    metadata = element.metadata.to_dict()
                else:
                    metadata = {}
                page_number = metadata.get("page_number", 1)

                # Check if this page_number already exists in docs_dict
                if page_number not in text_dict:
                    # If not, create new entry with initial text and metadata
                    text_dict[page_number] = str(element) + "\n\n"
                    meta_dict[page_number] = metadata
                else:
                    # If exists, append to text and update the metadata
                    text_dict[page_number] += str(element) + "\n\n"
                    meta_dict[page_number] = self._combine_metadata(
                        meta_dict[page_number], metadata
                    )

            # Convert the dict to a list of dicts representing documents
            docs = [(text_dict[key], meta_dict[key]) for key in text_dict.keys()]
        elif mode == "single":
            metadata = {}
            for element in elements:
                if hasattr(element, "metadata"):
                    metadata = self._combine_metadata(
                        metadata, element.metadata.to_dict()
                    )
            text = "\n\n".join([str(el) for el in elements])
            docs = [(text, metadata)]
        else:
            raise ValueError(f"mode of {mode} not supported.")
        return docs

    def __call__(self, contents: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """
        Parse the given document.

        Args:
            contents: document contents
            **kwargs: override for defaults set in the constructor

        Returns:
            A column with a list of pairs for each query. Each pair is a text chunk and
            associated metadata.
            The metadata is obtained from Unstructured, you can check possible values
            in the `Unstructed documentation <https://unstructured-io.github.io/unstructured/metadata.html>`
            Note that when `mode` is set to `single` or `paged` some of these fields are
            removed if they are specific to a single element, e.g. `category_depth`.
        """
        return super().__call__(contents, **kwargs)


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
        parse_images (bool): whether to parse the detected images from the PDF. Detected images will be
            cropped and described by the vision LLM and embedded in the markdown output.
            If set to `True`, `multimodal_llm` should be provided.
            If set to `False`, images will be replaced with placeholders in the markdown output.
        multimodal_llm (llms.OpenAIChat | llms.LiteLLMChat | None): LLM for parsing the image.
            Provided LLM should support image inputs in the same API format as OpenAI does.
            Required if `parse_images` is set to `True`.
        cache_strategy (udfs.CacheStrategy | None): Defines the caching mechanism.
        pdf_pipeline_options (dict): Additional options for the `DocumentConverter` from `docling`.

    """

    def __init__(
        self,
        parse_images: bool = False,
        multimodal_llm: llms.OpenAIChat | llms.LiteLLMChat | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
        pdf_pipeline_options: dict = {},
    ):
        with optional_imports("xpack-llm-docs"):
            from docling.datamodel.pipeline_options import PdfPipelineOptions
            from docling.document_converter import DocumentConverter, PdfFormatOption

        self.multimodal_llm: llms.OpenAIChat | llms.LiteLLMChat | None
        self.parse_images = parse_images

        if parse_images:
            if multimodal_llm is None:
                warnings.warn(
                    "`parse_images` is set to `True`, but `multimodal_llm` is "
                    f"not specified in DoclingParser, defaulting to `{DEFAULT_VISION_MODEL}`."
                )
                multimodal_llm = llms.OpenAIChat(
                    model=DEFAULT_VISION_MODEL,
                    cache_strategy=udfs.DefaultCache(),
                    retry_strategy=udfs.ExponentialBackoffRetryStrategy(max_retries=4),
                    verbose=True,
                )
            self.image_mode = "embedded"  # will make docling export document to markdown with base64-embedded images
            self.multimodal_llm = multimodal_llm
        else:
            self.multimodal_llm = None
            self.image_mode = "placeholder"  # will make docling export document to markdown with image placeholders

        default_pipeline_options = {
            "do_table_structure": True,
            "generate_picture_images": True if parse_images else False,
            "generate_page_images": False,
            "do_ocr": True,
            "images_scale": 2,
        }

        pipeline_options = PdfPipelineOptions(
            **(default_pipeline_options | pdf_pipeline_options)
        )
        pipeline_options.table_structure_options.do_cell_matching = False

        # actual docling converter
        self.converter: DocumentConverter = DocumentConverter(
            format_options={"pdf": PdfFormatOption(pipeline_options=pipeline_options)},
            # TODO: Add more file types
        )
        super().__init__(cache_strategy=cache_strategy)

    async def replace_images_with_descriptions(self, md_doc: str) -> str:
        if not self.multimodal_llm:
            raise ValueError(
                "Image parsing is not enabled, cannot replace images with descriptions."
            )

        image_pattern = re.compile(r"!\[Image\]\(data:image/[^;]+;base64,([^)\s]+)\)")
        base64_strings = list(set(image_pattern.findall(md_doc)))

        image_descriptions, _ = await _parser_utils._parse_b64_images(
            base64_strings,
            self.multimodal_llm,
            prompts.DEFAULT_IMAGE_PARSE_PROMPT,
            run_mode="parallel",
            parse_details=False,
            detail_parse_schema=None,
            parse_fn=_parser_utils.parse_image,
            parse_image_details_fn=None,
        )
        # wrap descriptions in newlines for better readability
        # TODO: consider special token for image descriptions for better chunking
        image_descriptions = [f"\n{desc}\n" for desc in image_descriptions]

        base64_to_desc = dict(zip(base64_strings, image_descriptions))

        def replace_image(match):
            base64_str = match.group(1)
            return base64_to_desc.get(base64_str, "[Image removed]")

        updated_markdown = image_pattern.sub(replace_image, md_doc)
        return updated_markdown

    async def parse(self, contents: bytes) -> list[tuple[str, dict]]:

        with optional_imports("xpack-llm-docs"):
            from docling_core.types.io import DocumentStream

        stream = DocumentStream(name="document.pdf", stream=BytesIO(contents))

        # parse document
        parsing_result = self.converter.convert(stream)
        doc = parsing_result.document

        # get markdown from parsed document
        doc_as_md = doc.export_to_markdown(image_mode=self.image_mode)

        # replace all images with their descriptions from the vision LLM
        if self.parse_images:
            doc_as_md = await self.replace_images_with_descriptions(doc_as_md)

        return [(doc_as_md, {})]

    async def __wrapped__(self, contents: bytes, **kwargs) -> list[tuple[str, dict]]:
        return await self.parse(contents)


class ImageParser(pw.UDF):
    """
    A class to parse images using vision LLMs.

    Args:
        llm (pw.UDF): LLM for parsing the image. Provided LLM should support image inputs.
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
        llm: pw.UDF = DEFAULT_VISION_LLM,
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
    ):
        with optional_imports("xpack-llm"):
            import openai

        super().__init__(cache_strategy=cache_strategy)
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
        llm: LLM for parsing the image. Provided LLM should support image inputs.
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
        llm: pw.UDF = DEFAULT_VISION_LLM,
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

        super().__init__(cache_strategy=cache_strategy)
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

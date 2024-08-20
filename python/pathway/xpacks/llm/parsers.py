# Copyright © 2024 Pathway

"""
A library for document parsers: functions that take raw bytes and return a list of text
chunks along with their metadata.
"""
from __future__ import annotations

import asyncio
import inspect
import io
import logging
import os
import subprocess
import tempfile
from collections.abc import Callable
from functools import partial
from io import BytesIO
from typing import TYPE_CHECKING, Any, Literal

from PIL import Image
from pydantic import BaseModel

import pathway as pw
from pathway.internals import udfs
from pathway.internals.config import _check_entitlements
from pathway.optional_import import optional_imports
from pathway.xpacks.llm import llms, prompts
from pathway.xpacks.llm._parser_utils import (
    img_to_b64,
    maybe_downscale,
    parse,
    parse_image_details,
)
from pathway.xpacks.llm.constants import DEFAULT_VISION_MODEL

if TYPE_CHECKING:
    from openparse.processing import IngestionPipeline

logger = logging.getLogger(__name__)


DEFAULT_VISION_LLM = llms.OpenAIChat(
    model=DEFAULT_VISION_MODEL,
    cache_strategy=udfs.DiskCache(),
    retry_strategy=udfs.ExponentialBackoffRetryStrategy(max_retries=4),
    verbose=True,
)


class ParseUtf8(pw.UDF):
    """
    Decode text encoded as UTF-8.
    """

    def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        docs: list[tuple[str, dict]] = [(contents.decode("utf-8"), {})]
        return docs

    def __call__(self, contents: pw.ColumnExpression, **kwargs) -> pw.ColumnExpression:
        """
        Parse the given document.

        Args:
            - contents: document contents

        Returns:
            A column with a list of pairs for each query. Each pair is a text chunk and
            associated metadata. The metadata is an empty dictionary.
        """
        return super().__call__(contents, **kwargs)


# Based on:
# https://github.com/langchain-ai/langchain/blob/master/libs/langchain/langchain/document_loaders/unstructured.py#L134
# MIT licensed
class ParseUnstructured(pw.UDF):
    """
    Parse document using `https://unstructured.io/ <https://unstructured.io/>`_.

    All arguments can be overridden during UDF application.

    Args:
        - mode: single, elements or paged.
          When single, each document is parsed as one long text string.
          When elements, each document is split into unstructured's elements.
          When paged, each pages's text is separately extracted.
        - post_processors: list of callables that will be applied to all extracted texts.
        - **unstructured_kwargs: extra kwargs to be passed to unstructured.io's `partition` function
    """

    def __init__(
        self,
        mode: str = "single",
        post_processors: list[Callable] | None = None,
        **unstructured_kwargs: Any,
    ):
        with optional_imports("xpack-llm-docs"):
            import unstructured.partition.auto  # noqa:F401

        super().__init__()
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

    def __wrapped__(self, contents: bytes, **kwargs) -> list[tuple[str, dict]]:
        """
        Parse the given document:

        Args:
            - contents: document contents
            - **kwargs: override for defaults set in the constructor

        Returns:
            a list of pairs: text chunk and metadata
            The metadata is obtained from Unstructured, you can check possible values
            in the `Unstructed documentation <https://unstructured-io.github.io/unstructured/metadata.html>`
            Note that when `mode` is set to `single` or `paged` some of these fields are
            removed if they are specific to a single element, e.g. `category_depth`.
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
            - contents: document contents
            - **kwargs: override for defaults set in the constructor

        Returns:
            A column with a list of pairs for each query. Each pair is a text chunk and
            associated metadata.
            The metadata is obtained from Unstructured, you can check possible values
            in the `Unstructed documentation <https://unstructured-io.github.io/unstructured/metadata.html>`
            Note that when `mode` is set to `single` or `paged` some of these fields are
            removed if they are specific to a single element, e.g. `category_depth`.
        """
        return super().__call__(contents, **kwargs)


class OpenParse(pw.UDF):
    """
    Parse PDFs using `open-parse library <https://github.com/Filimoa/open-parse>`_.

    When used in the
    `VectorStoreServer <https://pathway.com/developers/api-docs/pathway-xpacks-llm/
    vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer>`_,
    splitter can be set to ``None`` as OpenParse already chunks the documents.

    Args:
        - table_args: dictionary containing the table parser arguments. Needs to have key ``parsing_algorithm``,
            with the value being one of ``"llm"``, ``"unitable"``, ``"pymupdf"``, ``"table-transformers"``.
            ``"llm"`` parameter can be specified to modify the vision LLM used for parsing.
            Will default to ``OpenAI`` ``gpt-4o``, with markdown table parsing prompt.
            Default config requires ``OPENAI_API_KEY`` environment variable to be set.
            For information on other parsing algorithms and supported arguments check
            `the OpenParse documentation <https://filimoa.github.io/open-parse/processing/parsing-tables/overview/>`_.
        - image_args: dictionary containing the image parser arguments.
            Needs to have the following keys ``parsing_algorithm``, ``llm``, ``prompt``.
            Currently, only supported ``parsing_algorithm`` is ``"llm"``.
            ``"llm"`` parameter can be specified to modify the vision LLM used for parsing.
            Will default to ``OpenAI`` ``gpt-4o``, with markdown image parsing prompt.
            Default config requires ``OPENAI_API_KEY`` environment variable to be set.
        - parse_images: whether to parse the images from the PDF. Detected images will be
            indexed by their description from the parsing algorithm.
            Note that images are parsed with separate OCR model, parsing may take a while.
        - processing_pipeline: ``openparse.processing.IngestionPipeline`` that will post process
            the extracted elements. Can be set to Pathway defined ``CustomIngestionPipeline``
            by setting to ``"pathway_pdf_default"``,
            ``SamePageIngestionPipeline`` by setting to ``"merge_same_page"``,
            or any of the pipelines under the ``openparse.processing``.
            Defaults to ``CustomIngestionPipeline``.
        - cache_strategy: Defines the caching mechanism. To enable caching,
            a valid :py:class:``~pathway.udfs.CacheStrategy`` should be provided.
            Defaults to None.

    Example:

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import llms, parsers, prompts
    >>> chat = llms.OpenAIChat(model="gpt-4o")
    >>> table_args = {
    ...    "parsing_algorithm": "llm",
    ...    "llm": chat,
    ...    "prompt": prompts.DEFAULT_MD_TABLE_PARSE_PROMPT,
    ... }
    >>> image_args = {
    ...     "parsing_algorithm": "llm",
    ...     "llm": chat,
    ...     "prompt": prompts.DEFAULT_IMAGE_PARSE_PROMPT,
    ... }
    >>> parser = parsers.OpenParse(table_args=table_args, image_args=image_args)
    """

    def __init__(
        self,
        table_args: dict | None = None,
        image_args: dict | None = None,
        parse_images: bool = False,
        processing_pipeline: IngestionPipeline | str | None = None,
        cache_strategy: udfs.CacheStrategy | None = None,
    ):
        with optional_imports("xpack-llm-docs"):
            import openparse  # noqa:F401
            from pypdf import PdfReader  # noqa:F401

            from ._openparse_utils import (
                CustomDocumentParser,
                CustomIngestionPipeline,
                SamePageIngestionPipeline,
            )

        super().__init__(cache_strategy=cache_strategy)

        if table_args is None:
            table_args = {
                "parsing_algorithm": "llm",
                "llm": DEFAULT_VISION_LLM,
                "prompt": prompts.DEFAULT_MD_TABLE_PARSE_PROMPT,
            }

        if parse_images:
            if image_args is None:
                logger.warn(
                    "`parse_images` is set to `True`, but `image_args` is not specified, defaulting to `gpt-4o`."
                )
                image_args = {
                    "parsing_algorithm": "llm",
                    "llm": DEFAULT_VISION_LLM,
                    "prompt": prompts.DEFAULT_IMAGE_PARSE_PROMPT,
                }
            else:
                if image_args["parsing_algorithm"] != "llm":
                    raise ValueError(
                        "Image parsing is only supported with LLMs.",
                        "Either change the `parsing_algorithm` to `llm` or set the `parse_images` to `False`.",
                        f"Given args: {image_args}",
                    )
        else:
            logger.warn(
                "`parse_images` is set to `False`, but `image_args` is specified, skipping image parsing."
            )
            image_args = None

        if processing_pipeline is None:
            processing_pipeline = CustomIngestionPipeline()
        elif isinstance(processing_pipeline, str):
            if processing_pipeline == "pathway_pdf_default":
                processing_pipeline = CustomIngestionPipeline()
            elif processing_pipeline == "merge_same_page":
                processing_pipeline = SamePageIngestionPipeline()

        self.doc_parser = CustomDocumentParser(
            table_args=table_args,
            image_args=image_args,
            processing_pipeline=processing_pipeline,
        )

    def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        import openparse
        from pypdf import PdfReader

        reader = PdfReader(stream=BytesIO(contents))
        doc = openparse.Pdf(file=reader)

        parsed_content = self.doc_parser.parse(doc)
        nodes = list(parsed_content.nodes)

        logger.info(
            f"OpenParser completed parsing, total number of nodes: {len(nodes)}"
        )

        metadata: dict = {}
        docs = [(node.model_dump()["text"], metadata) for node in nodes]

        return docs

    def __call__(self, contents: pw.ColumnExpression) -> pw.ColumnExpression:
        """
        Parse the given PDFs.

        Args:
            - contents (ColumnExpression[bytes]): A column with PDFs to be parsed, passed as bytes.

        Returns:
            A column with a list of pairs for each query. Each pair is a text chunk and
            metadata, which in case of `OpenParse` is an empty dictionary.
        """
        return super().__call__(contents)


class ImageParser(pw.UDF):
    """
    A class to parse images using vision LLMs.

    Args:
        - llm (pw.UDF): LLM for parsing the image. Provided LLM should support image inputs.
        - parse_prompt: The prompt used by the language model for parsing.
        - detail_parse_schema: A schema for detailed parsing, if applicable.
            Providing a Pydantic schema will call the LLM second time to parse necessary information,
            leaving it as None will skip this step.
        - downsize_horizontal_width: Width to which images are downsized if necessary.
            Default is 1920.
        - include_schema_in_text: If the parsed schema should be included in the ``text`` description.
            May help with search and retrieval. Defaults to ``False``. Only usable if ``detail_parse_schema``
            is provided.
        - max_image_size: Maximum allowed size of the images in bytes. Default is 15 MB.
        - run_mode: Mode of execution,
            either ``"sequential"`` or ``"parallel"``. Default is ``"parallel"``.
            ``"parallel"`` mode is suggested for speed, but if timeouts or memory usage in local LLMs are concern,
            ``"sequential"`` may be better.
        - retry_strategy: Retrying strategy for the LLM calls. Defining a retrying strategy with
            propriety LLMs is strongly suggested.
        - cache_strategy: Defines the caching mechanism. To enable caching,
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

        self.parse_fn = parser_executor._wrap(parse)

        if self.parse_details:
            _schema_parser_executor: udfs.Executor = udfs.async_executor(
                capacity=None, retry_strategy=retry_strategy
            )

            llm_args: dict = llm.kwargs  # type: ignore

            allowed_client_args = inspect.signature(
                openai.AsyncOpenAI.__init__
            ).parameters.keys()

            parse_image_details_fn = partial(
                parse_image_details,
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

    def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        """Parse image bytes with GPT-v model."""

        images: list[Image.Image] = [Image.open(BytesIO(contents))]

        logger.info("`ImageParser` applying `maybe_downscale`.")

        images = [
            maybe_downscale(img, self.max_image_size, self.downsize_horizontal_width)
            for img in images
        ]

        parsed_content, parsed_details = parse_images(
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


def _convert_pptx_to_pdf(contents: bytes) -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as pptx_temp:
        pptx_temp.write(contents)
        pptx_temp_path = pptx_temp.name

    pdf_temp_path = pptx_temp_path.replace(".pptx", ".pdf").split(os.path.sep)[-1]

    try:
        result = subprocess.run(
            ["soffice", "--headless", "--convert-to", "pdf", pptx_temp_path],
            check=True,
            capture_output=True,
            text=True,
        )

        logger.info(f"`_convert_pptx_to_pdf` result: {str(result)}")

        with open(pdf_temp_path, "rb") as pdf_temp:
            pdf_contents = pdf_temp.read()

    except FileNotFoundError:
        raise Exception(
            "`LibreOffice` is not installed or `soffice` command is not found. Please install LibreOffice."
        )

    finally:
        os.remove(pptx_temp_path)
        if os.path.exists(pdf_temp_path):
            os.remove(pdf_temp_path)

    return pdf_contents


class SlideParser(pw.UDF):
    """
    A class to parse PPTX and PDF slides using vision LLMs.

    Use of this class requires Pathway Scale account.
    Get your license `here <https://pathway.com/get-license>`_ to gain access.

    Args:
        - llm: LLM for parsing the image. Provided LLM should support image inputs.
        - parse_prompt: The prompt used by the language model for parsing.
        - detail_parse_schema: A schema for detailed parsing, if applicable.
            Providing a Pydantic schema will call the LLM second time to parse necessary information,
            leaving it as None will skip this step.
        - include_schema_in_text: If the parsed schema should be included in the ``text`` description.
            May help with search and retrieval. Defaults to ``False``.
            Only usable if ``detail_parse_schema`` is provided.
        - intermediate_image_format: Intermediate image format used when converting PDFs to images.
            Defaults to ``"jpg"`` for speed and memory use.
        - image_size (tuple[int, int], optional): The target size of the images. Default is (1280, 720).
            Note that setting higher resolution will increase the cost and latency.
            Since vision LLMs will resize the given image into certain resolution, setting high resolutions
            may not help with the accuracy.
        - run_mode: Mode of execution,
            either ``"sequential"`` or ``"parallel"``. Default is ``"parallel"``.
            ``"parallel"`` mode is suggested for speed, but if timeouts or memory usage in local LLMs are concern,
            ``"sequential"`` may be better.
        - retry_strategy: Retrying strategy for the LLM calls. Defining a retrying strategy with
            propriety LLMs is strongly suggested.
        - cache_strategy: Defines the caching mechanism. To enable caching,
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

        self.parse_fn = _parser_executor._wrap(parse)

        if self.parse_details:
            _schema_parser_executor: udfs.Executor = udfs.async_executor(
                capacity=None, retry_strategy=retry_strategy
            )

            llm_args: dict = llm.kwargs  # type: ignore

            allowed_client_args = inspect.signature(
                openai.AsyncOpenAI.__init__
            ).parameters.keys()

            parse_image_details_fn = partial(
                parse_image_details,
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

    def __wrapped__(self, contents: bytes) -> list[tuple[str, dict]]:
        """Parse slides with GPT-v model by converting to images."""

        from pdf2image import convert_from_bytes
        from unstructured.file_utils.filetype import FileType, detect_filetype

        byte_file = io.BytesIO(contents)
        filetype = detect_filetype(
            file=byte_file,
        )

        if filetype == FileType.PPTX:
            logger.info("`SlideParser` converting PPTX to PDF from byte object.")
            contents = _convert_pptx_to_pdf(contents)

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

        b64_images = [img_to_b64(image) for image in images]

        parsed_content, parsed_details = asyncio.run(
            _parse_b64_images(
                b64_images,
                self.llm,
                self.parse_prompt,
                run_mode=self.run_mode,
                parse_details=self.parse_details,
                detail_parse_schema=self.detail_parse_schema,
                parse_fn=self.parse_fn,
                parse_image_details_fn=self.parse_image_details_fn,
            )
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


def parse_images(
    images: list[Image.Image],
    llm: pw.UDF,
    parse_prompt: str,
    *,
    run_mode: Literal["sequential", "parallel"] = "parallel",
    parse_details: bool = False,
    detail_parse_schema: type[BaseModel] | None = None,
    parse_fn: Callable,
    parse_image_details_fn: Callable | None,
) -> tuple[list[str], list[BaseModel]]:
    """
    Parse images and optional Pydantic model with a multi-modal LLM.
    `parse_prompt` will be only used for the regular parsing.

    Args:
        - images: Image list to be parsed. Images are expected to be `PIL.Image.Image`.
        - llm: LLM model to be used for parsing. Needs to support image input.
        - parse_details: Whether to make second LLM call to parse specific Pydantic
            model from the image.
        - run_mode: Mode of execution,
            either ``"sequential"`` or ``"parallel"``. Default is ``"parallel"``.
            ``"parallel"`` mode is suggested for speed, but if timeouts or memory usage in local LLMs are concern,
            ``"sequential"`` may be better.
        - parse_details: Whether a schema should be parsed.
        - detail_parse_schema: Pydantic model for schema to be parsed.
        - parse_fn: Awaitable image parsing function.
        - parse_image_details_fn: Awaitable image schema parsing function.

    """
    logger.info("`parse_images` converting images to base64.")

    b64_images = [img_to_b64(image) for image in images]

    return asyncio.run(
        _parse_b64_images(
            b64_images,
            llm,
            parse_prompt,
            run_mode=run_mode,
            parse_details=parse_details,
            detail_parse_schema=detail_parse_schema,
            parse_fn=parse_fn,
            parse_image_details_fn=parse_image_details_fn,
        )
    )


async def _parse_b64_images(
    b64_images: list[str],
    llm: pw.UDF,
    parse_prompt: str,
    *,
    run_mode: Literal["sequential", "parallel"],
    parse_details: bool,
    detail_parse_schema: type[BaseModel] | None,
    parse_fn: Callable,
    parse_image_details_fn: Callable | None,
) -> tuple[list[str], list[BaseModel]]:
    tot_pages = len(b64_images)

    if parse_details:
        assert detail_parse_schema is not None and issubclass(
            detail_parse_schema, BaseModel
        ), "`detail_parse_schema` must be valid Pydantic Model class when `parse_details` is True"

    logger.info(f"`parse_images` parsing descriptions for {tot_pages} images.")

    parsed_details: list[BaseModel] = []

    if run_mode == "sequential":
        parsed_content = []

        for img in b64_images:
            parsed_txt = await parse_fn(img, llm, parse_prompt)
            parsed_content.append(parsed_txt)

        if parse_details:
            assert parse_image_details_fn is not None
            parsed_details = []
            for img in b64_images:
                parsed_detail = await parse_image_details_fn(
                    img,
                    parse_schema=detail_parse_schema,
                )
                parsed_details.append(parsed_detail)

    else:
        parse_tasks = [parse_fn(img, llm, parse_prompt) for img in b64_images]

        if parse_details:
            assert parse_image_details_fn is not None
            detail_tasks = [
                parse_image_details_fn(
                    img,
                    parse_schema=detail_parse_schema,
                )
                for img in b64_images
            ]
        else:
            detail_tasks = []

        results = await asyncio.gather(*parse_tasks, *detail_tasks)

        parsed_content = results[: len(b64_images)]
        parsed_details = results[len(b64_images) :]

    return parsed_content, parsed_details

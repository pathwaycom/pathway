# Copyright © 2024 Pathway

"""
A library for document parsers: functions that take raw bytes and return a list of text
chunks along with their metadata.
"""

import asyncio
import base64
import concurrent.futures
import io
import logging
from typing import List, Literal, Union

import PIL
from openparse import DocumentParser, consts, tables, text
from openparse.pdf import Pdf
from openparse.schemas import ParsedDocument, TableElement
from openparse.tables.parse import (
    Bbox,
    PyMuPDFArgs,
    TableTransformersArgs,
    UnitableArgs,
    _ingest_with_pymupdf,
    _ingest_with_table_transformers,
    _ingest_with_unitable,
)
from openparse.tables.utils import adjust_bbox_with_padding, crop_img_with_padding
from pydantic import BaseModel, ConfigDict, Field

from pathway.internals import udfs
from pathway.xpacks.llm._utils import _coerce_sync
from pathway.xpacks.llm.llms import OpenAIChat

DEFAULT_TABLE_PARSE_PROMPT = """Explain the given table in JSON format in detail.
Do not skip over details or units/metrics.
Make sure column and row names are understandable.
If it is not a table, return 'No table.' ."""


logger = logging.getLogger(__name__)

chat = OpenAIChat(
    model="gpt-4o",
    cache_strategy=udfs.DiskCache(),
    retry_strategy=udfs.ExponentialBackoffRetryStrategy(max_retries=4),
)


def llm_parse_table(
    image, model="gpt-4o", prompt=DEFAULT_TABLE_PARSE_PROMPT, **kwargs
) -> str:

    content = [
        {"type": "text", "text": prompt},
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{image}"},
        },
    ]

    messages = [
        {
            "role": "user",
            "content": content,
        }
    ]

    logger.info(f"Parsing table, model: {model}\nmessages: {str(content)[:350]}...")

    response = _coerce_sync(chat.__wrapped__)(model=model, messages=messages, **kwargs)

    return response


async def a_llm_parse_table(image, model="gpt-4o", prompt=DEFAULT_TABLE_PARSE_PROMPT):
    loop = asyncio.get_event_loop()
    task = loop.run_in_executor(None, llm_parse_table, image, model, prompt)
    result = await task
    return result


class LLMArgs(BaseModel):
    parsing_algorithm: Literal["llm"] = Field(default="llm")
    min_table_confidence: float = Field(default=0.9, ge=0.0, le=1.0)
    llm_model: str = Field(default="gpt-4o")
    prompt: str = Field(default=DEFAULT_TABLE_PARSE_PROMPT)

    model_config = ConfigDict(extra="forbid")  # type: ignore


def _table_args_dict_to_model(args_dict: dict):
    if args_dict["parsing_algorithm"] == "table-transformers":
        return tables.TableTransformersArgs(**args_dict)
    elif args_dict["parsing_algorithm"] == "pymupdf":
        return tables.PyMuPDFArgs(**args_dict)
    elif args_dict["parsing_algorithm"] == "unitable":
        return tables.UnitableArgs(**args_dict)
    elif args_dict["parsing_algorithm"] == "llm":
        return LLMArgs(**args_dict)
    else:
        raise ValueError(
            f"Unsupported parsing_algorithm: {args_dict['parsing_algorithm']}"
        )


def img_to_b64(img: PIL.Image) -> str:
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)

    img_bytes = buffer.read()

    return base64.b64encode(img_bytes).decode("utf-8")


def _ingest_with_llm(
    doc: Pdf,
    args: LLMArgs,
    verbose: bool = False,
) -> List[TableElement]:
    try:
        from openparse.tables.table_transformers.ml import find_table_bboxes
        from openparse.tables.utils import doc_to_imgs

    except ImportError as e:
        raise ImportError(
            "Table detection and extraction requires the `torch`, `torchvision` and `transformers` libraries to be installed.",  # noqa: E501
            e,
        )
    pdoc = doc.to_pymupdf_doc()
    pdf_as_imgs = doc_to_imgs(pdoc)

    pages_with_tables = {}
    for page_num, img in enumerate(pdf_as_imgs):
        pages_with_tables[page_num] = find_table_bboxes(img, args.min_table_confidence)

    tables = []
    image_ls = []
    for page_num, table_bboxes in pages_with_tables.items():
        page = pdoc[page_num]
        for table_bbox in table_bboxes:
            padding_pct = 0.05
            padded_bbox = adjust_bbox_with_padding(
                bbox=table_bbox.bbox,
                page_width=page.rect.width,
                page_height=page.rect.height,
                padding_pct=padding_pct,
            )
            table_img = crop_img_with_padding(pdf_as_imgs[page_num], padded_bbox)

            img = img_to_b64(table_img)

            image_ls.append(img)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        task_results = list(
            executor.map(
                lambda img: llm_parse_table(img, args.llm_model, args.prompt),
                image_ls,
            )
        )

    for table_str in task_results:
        fy0 = page.rect.height - padded_bbox[3]
        fy1 = page.rect.height - padded_bbox[1]

        table_elem = TableElement(
            bbox=Bbox(
                page=page_num,
                x0=padded_bbox[0],
                y0=fy0,
                x1=padded_bbox[2],
                y1=fy1,
                page_width=page.rect.width,
                page_height=page.rect.height,
            ),
            text=table_str,
        )

        tables.append(table_elem)

    return tables


def ingest(
    doc: Pdf,
    parsing_args: Union[
        TableTransformersArgs, PyMuPDFArgs, UnitableArgs, LLMArgs, None
    ] = None,
    verbose: bool = False,
) -> List[TableElement]:
    if isinstance(parsing_args, TableTransformersArgs):
        return _ingest_with_table_transformers(doc, parsing_args, verbose)
    elif isinstance(parsing_args, PyMuPDFArgs):
        return _ingest_with_pymupdf(doc, parsing_args, verbose)
    elif isinstance(parsing_args, UnitableArgs):
        return _ingest_with_unitable(doc, parsing_args, verbose)
    elif isinstance(parsing_args, LLMArgs):
        return _ingest_with_llm(doc, parsing_args, verbose)
    else:
        raise ValueError("Unsupported parsing_algorithm.")


class CustomDocumentParser(DocumentParser):
    def parse(
        self,
        doc,
    ) -> ParsedDocument:
        """
        Parse a given document with the multi modal LLM.

        Uses pymupdf to parse the document, then runs the LLM on the table images.

        Args:
            doc: Document to be parsed.
        """

        text_engine = "pymupdf"
        text_elems = text.ingest(doc, parsing_method=text_engine)
        text_nodes = self._elems_to_nodes(text_elems)

        table_nodes = []
        table_args_obj = None
        if self.table_args:
            table_args_obj = _table_args_dict_to_model(self.table_args)
            table_elems = ingest(doc, table_args_obj, verbose=self._verbose)
            table_nodes = self._elems_to_nodes(table_elems)

        nodes = text_nodes + table_nodes
        nodes = self.processing_pipeline.run(nodes)

        parsed_doc = ParsedDocument(
            nodes=nodes,
            filename="Path(file).name",
            num_pages=doc.num_pages,
            coordinate_system=consts.COORDINATE_SYSTEM,
            table_parsing_kwargs=(
                table_args_obj.model_dump() if table_args_obj else None
            ),
            creation_date=doc.file_metadata.get("creation_date"),
            last_modified_date=doc.file_metadata.get("last_modified_date"),
            last_accessed_date=doc.file_metadata.get("last_accessed_date"),
            file_size=doc.file_metadata.get("file_size"),
        )
        return parsed_doc

# Copyright © 2024 Pathway

import asyncio
from typing import Any, List, Literal, Union

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

from pathway.xpacks.llm._parser_utils import img_to_b64, parse
from pathway.xpacks.llm._utils import _run_async
from pathway.xpacks.llm.prompts import DEFAULT_MD_TABLE_PARSE_PROMPT

# TODO: This should be added somewhere in documentation so that users know that
# they can change them


class LLMArgs(BaseModel):
    parsing_algorithm: Literal["llm"] = Field(default="llm")
    min_table_confidence: float = Field(default=0.9, ge=0.0, le=1.0)

    llm: Any = Field(default=None)
    llm_model: str | None = Field(default=None)
    prompt: str = Field(default=DEFAULT_MD_TABLE_PARSE_PROMPT)

    model_config = ConfigDict(extra="forbid")


def _table_args_dict_to_model(args_dict: dict) -> BaseModel:
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

    async def parse_image_list(image_list: list[str], prompt: str):
        return await asyncio.gather(
            *[
                parse(
                    img,
                    args.llm,
                    prompt,
                    model=args.llm_model,
                )
                for img in image_list
            ]
        )

    parse_results = _run_async(parse_image_list(image_ls, args.prompt))
    # may extend or add second call for non-table images

    for table_str in parse_results:
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

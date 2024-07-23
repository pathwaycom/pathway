# Copyright © 2024 Pathway

import asyncio
import logging
from typing import Any, Literal

import openparse
from openparse import DocumentParser, consts, tables, text
from openparse._types import NOT_GIVEN, NotGiven
from openparse.doc_parser import PyMuPDFArgsDict, TableTransformersArgsDict
from openparse.pdf import Pdf
from openparse.processing import (
    CombineNodesSpatially,
    IngestionPipeline,
    ProcessingStep,
)
from openparse.processing.basic_transforms import (
    CombineBullets,
    CombineHeadingsWithClosestText,
    RemoveFullPageStubs,
    RemoveMetadataElements,
    RemoveNodesBelowNTokens,
    RemoveRepeatedElements,
    RemoveTextInsideTables,
)
from openparse.schemas import Bbox, Node, ParsedDocument, TableElement
from openparse.tables.parse import (
    PyMuPDFArgs,
    TableTransformersArgs,
    UnitableArgs,
    _ingest_with_pymupdf,
    _ingest_with_table_transformers,
    _ingest_with_unitable,
)
from openparse.tables.utils import adjust_bbox_with_padding, crop_img_with_padding
from pydantic import BaseModel, ConfigDict, Field

from pathway.optional_import import optional_imports
from pathway.xpacks.llm._parser_utils import img_to_b64, parse
from pathway.xpacks.llm._utils import _run_async
from pathway.xpacks.llm.prompts import DEFAULT_MD_TABLE_PARSE_PROMPT

logger = logging.getLogger(__name__)

# TODO: This should be added somewhere in documentation so that users know that
# they can change them


class LLMArgs(BaseModel):
    parsing_algorithm: Literal["llm"] = Field(default="llm")
    min_table_confidence: float = Field(default=0.7, ge=0.0, le=1.0)

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


class CustomIngestionPipeline(IngestionPipeline):
    """
    A simple PDF processing pipeline that combines close elements, combines the headers
    with the text body, and removes weirdly formatted/small elements.

    Implementation slightly deviates from the original ``BasicIngestionPipeline``.

    https://github.com/Filimoa/open-parse/blob/main/src/openparse/processing/ingest.py
    """

    def __init__(self):
        self.transformations = [
            RemoveTextInsideTables(),
            # increased from original to fix large images from being removed @berke
            RemoveFullPageStubs(max_area_pct=0.75),
            # mostly aimed at combining bullets and weird formatting
            CombineNodesSpatially(
                x_error_margin=10, y_error_margin=4, criteria="both_small"
            ),
            CombineHeadingsWithClosestText(),
            CombineBullets(),
            CombineNodesSpatially(
                x_error_margin=0, y_error_margin=10, criteria="both_small"
            ),
            RemoveMetadataElements(),
            CombineNodesSpatially(criteria="either_stub"),
            RemoveRepeatedElements(threshold=2),
            # tried everything to combine, remove stubs that are still left
            # reduced this from the original @berke
            RemoveNodesBelowNTokens(min_tokens=10),
            # combines bullets split across pages
            # (previously page metadata would have prevented this)
            CombineBullets(),
        ]


class PageChunker(ProcessingStep):
    def process(self, nodes: list[Node]) -> list[Node]:
        """
        Process a list of Nodes and return list of Nodes where elements are grouped by
        the pages.
        """
        return self._merge_nodes_by_page(nodes)

    def _merge_nodes_by_page(self, nodes: list[Node]) -> list[Node]:
        """Merge nodes based on their pages."""
        elements_by_page = {}

        for node in nodes:
            for element in node.elements:
                page = element.page
                if page not in elements_by_page:
                    elements_by_page[page] = [element]
                else:
                    elements_by_page[page].append(element)

        merged_nodes = []
        for elems in elements_by_page.values():
            new_node = Node(elements=elems)
            merged_nodes.append(new_node)

        return merged_nodes


class SamePageIngestionPipeline(IngestionPipeline):
    """Simple ingestion pipeline that combines the elements from the same pages."""

    def __init__(self, additional_transformations: list[ProcessingStep] = []):
        self.transformations = [PageChunker()] + additional_transformations


async def parse_image_list(
    image_list: list[str], llm, prompt: str, llm_model: str | None
):
    return await asyncio.gather(
        *[
            parse(
                img,
                llm,
                prompt,
                model=llm_model,
            )
            for img in image_list
        ]
    )


def _ingest_with_llm(
    doc: Pdf,
    args: LLMArgs,
    verbose: bool = False,
) -> list[TableElement]:
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

    logger.info("OpenParse extracting tables.")

    image_ls: list[str] = []
    bbox_ls: list[Bbox] = []

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

            fy0 = page.rect.height - padded_bbox[3]
            fy1 = page.rect.height - padded_bbox[1]

            bbox = Bbox(
                page=page_num,
                x0=padded_bbox[0],
                y0=fy0,
                x1=padded_bbox[2],
                y1=fy1,
                page_width=page.rect.width,
                page_height=page.rect.height,
            )

            image_ls.append(img)
            bbox_ls.append(bbox)
    logger.info(f"OpenParse extracted {len(image_ls)} tables. Starting parsing...")

    parse_results = _run_async(
        parse_image_list(image_ls, args.llm, args.prompt, args.llm_model)
    )

    element_list = []

    for bbox, table_str in list(zip(bbox_ls, parse_results)):
        table_elem = TableElement(
            bbox=bbox,
            text=table_str,
        )

        element_list.append(table_elem)

    return element_list


def _ingest_images_with_llm(
    doc: Pdf,
    args: LLMArgs,
    verbose: bool = False,
) -> list[TableElement]:
    with optional_imports("xpack-llm-local"):
        from openparse.tables.utils import doc_to_imgs
        from surya.detection import batch_text_detection
        from surya.layout import batch_layout_detection
        from surya.model.detection.segformer import load_model, load_processor
        from surya.settings import settings

    pdoc = doc.to_pymupdf_doc()

    pdf_as_imgs = doc_to_imgs(pdoc)

    logger.info("OpenParse image extractor model loading...")

    model = load_model(checkpoint=settings.LAYOUT_MODEL_CHECKPOINT)
    processor = load_processor(checkpoint=settings.LAYOUT_MODEL_CHECKPOINT)
    det_model = load_model()
    det_processor = load_processor()

    logger.info("OpenParse image extractor model loaded successfully.")

    logger.info("Image extractor detecting layout.")
    line_predictions = batch_text_detection(pdf_as_imgs, det_model, det_processor)
    layout_predictions = batch_layout_detection(
        pdf_as_imgs, model, processor, line_predictions
    )
    logger.info("Image extractor detected layout.")

    image_ls: list[str] = []
    bbox_ls: list[Bbox] = []

    for page_num, layout in enumerate(layout_predictions):
        page = pdoc[page_num]
        for element in layout.bboxes:
            if element.label == "Figure":
                figure_image = pdf_as_imgs[page_num].crop(element.bbox)
                img = img_to_b64(figure_image)

                padding_pct = 0.05
                padded_bbox = adjust_bbox_with_padding(
                    bbox=element.bbox,
                    page_width=page.rect.width,
                    page_height=page.rect.height,
                    padding_pct=padding_pct,
                )

                fy0 = page.rect.height - padded_bbox[3]
                fy1 = page.rect.height - padded_bbox[1]

                bbox = Bbox(
                    page=page_num,
                    x0=padded_bbox[0],
                    y0=fy0,
                    x1=padded_bbox[2],
                    y1=fy1,
                    page_width=page.rect.width,
                    page_height=page.rect.height,
                )

                image_ls.append(img)
                bbox_ls.append(bbox)

    logger.info(f"Extracted {len(image_ls)} images. Starting parsing...")

    parse_results = _run_async(
        parse_image_list(image_ls, args.llm, args.prompt, args.llm_model)
    )

    logger.info("Completed parsing the images.")

    element_list = []

    for bbox, table_str in list(zip(bbox_ls, parse_results)):
        table_elem = TableElement(
            bbox=bbox,
            text=table_str,
        )

        element_list.append(table_elem)

    return element_list


def ingest(
    doc: Pdf,
    parsing_args: (
        TableTransformersArgs | PyMuPDFArgs | UnitableArgs | LLMArgs | None
    ) = None,
    verbose: bool = False,
) -> list[TableElement]:
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


# modified from https://github.com/Filimoa/open-parse/blob/main/src/openparse/doc_parser.py
class CustomDocumentParser(DocumentParser):
    def __init__(
        self,
        *,
        processing_pipeline: IngestionPipeline | NotGiven | None = NOT_GIVEN,
        table_args: TableTransformersArgsDict | PyMuPDFArgsDict | NotGiven = NOT_GIVEN,
        image_args: TableTransformersArgsDict | PyMuPDFArgsDict | NotGiven = NOT_GIVEN,
    ):
        super().__init__(processing_pipeline=processing_pipeline, table_args=table_args)
        self.image_args = image_args

    def parse(
        self,
        doc: openparse.Pdf,
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

        image_nodes = []
        if self.image_args:
            image_args_obj = _table_args_dict_to_model(self.image_args)
            assert isinstance(
                image_args_obj, LLMArgs
            ), "Image extractor expects `LLMArgs` for parsing arguments."
            image_elems = _ingest_images_with_llm(doc, image_args_obj)
            image_nodes = self._elems_to_nodes(image_elems)

        table_nodes = []
        table_args_obj = None
        if self.table_args:
            table_args_obj = _table_args_dict_to_model(self.table_args)
            table_elems = ingest(doc, table_args_obj, verbose=self._verbose)
            table_nodes = self._elems_to_nodes(table_elems)

        logger.info(
            f"""OpenParse parsed PDF. Number of parsed nodes:
            text: {len(text_nodes)}, table: {len(table_nodes)}, image: {len(image_nodes)}""",
        )
        nodes = text_nodes + table_nodes + image_nodes
        logger.info("`CustomDocumentParser` applying processing_pipeline.")
        nodes = self.processing_pipeline.run(nodes)
        logger.info(f"Number of nodes after processing pipeline: {len(nodes)}")

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

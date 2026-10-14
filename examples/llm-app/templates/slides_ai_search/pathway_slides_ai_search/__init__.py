# Copyright © 2026 Pathway

import base64
import logging
import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, create_model

import pathway as pw
from pathway.xpacks.llm.question_answering import DeckRetriever

CUSTOM_FIELDS = {"option": Literal}


def get_model_from_file(file_path: str | os.PathLike[str]) -> type[BaseModel]:
    """
    Return Pydantic schema from a YAML file.

    Replaces types of `CUSTOM_FIELDS` with the definitions, other types are evaluated
    as primitive Python type.

    Args:
        - file_path: Path of the YAML file.
    """

    with open(file_path, "r") as file:
        schema = yaml.safe_load(file)

    return get_model_from_dict(schema)


def get_model_from_dict(schema: dict[str, Any]) -> type[BaseModel]:
    fields: dict[str, Any] = {}

    if schema.keys() == {"fields"} and "type" not in schema["fields"]:
        schema = schema["fields"]

    for field_name, field_info in schema.items():
        field_type: object
        match field_info.pop("type"):
            case "option":
                field_type = Literal[tuple(field_info.pop("values"))]
            case f_type:
                field_type = f_type

        fields[field_name] = (field_type, Field(**field_info))

    return create_model("ParsePydanticSchema", **fields)


def get_model(source: str | os.PathLike[str] | dict[str, Any]) -> type[BaseModel]:
    if isinstance(source, dict):
        return get_model_from_dict(source)
    else:
        return get_model_from_file(source)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,  # instructor library overrides `basicConfig`, this fixes logging
)

STORAGE_FOLDER = Path("storage")
IMAGE_DUMP_FOLDER = STORAGE_FOLDER / "pw_dump_images"
FILE_DUMP_FOLDER = STORAGE_FOLDER / "pw_dump_files"


def encode_str(original_string: str) -> str:
    return base64.urlsafe_b64encode(original_string.encode("utf-8")).decode("us-ascii")


def add_slide_id(text: str, metadata: dict) -> tuple[str, dict]:
    encoded_name = encode_str(metadata["path"])

    page = metadata["image_page"]
    page_count = metadata["tot_pages"]

    slide_id = f"{encoded_name}_{page}_{page_count}.png"

    logging.info(f"`add_slide_id` for {slide_id}...")

    metadata["slide_id"] = slide_id

    return (text, metadata)


class DeckRetrieverWithFileSave(DeckRetriever):
    def dump_img_callback(self, key, row, time, is_addition):
        # save images parsed by the Pathway Live Data Framework
        metadata = row["data"]
        slide_id = metadata["slide_id"].value
        file_path = IMAGE_DUMP_FOLDER / slide_id
        if is_addition:
            data = base64.b64decode(metadata["b64_image"].value)
            file_path.write_bytes(data)
        else:
            try:
                file_path.unlink()
                logging.info("Removed %s", file_path)
            except Exception as e:
                logging.info("Error removing %s: %s", file_path, e)

    def dump_file_callback(self, key, row, time, is_addition):
        # save parsed files
        file_name = row["path"].value.rpartition("/")[-1]  # XXX
        file_path = FILE_DUMP_FOLDER / file_name
        if is_addition:
            file_path.write_bytes(row["data"])
        else:
            try:
                file_path.unlink()
                logging.info("Removed %s", file_path)
            except Exception as e:
                logging.info("Error removing %s: %s", file_path, e)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        IMAGE_DUMP_FOLDER.mkdir(parents=True, exist_ok=True)
        FILE_DUMP_FOLDER.mkdir(parents=True, exist_ok=True)

        chunked_docs = self.indexer.chunked_docs
        t = chunked_docs.select(
            data=pw.this.metadata,
        )
        pw.io.subscribe(t, on_change=self.dump_img_callback)

        docs = self.indexer.input_docs
        t = docs.select(data=docs.text, path=docs.metadata["path"])
        pw.io.subscribe(t, on_change=self.dump_file_callback)

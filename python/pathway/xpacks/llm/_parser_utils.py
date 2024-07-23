# Copyright © 2024 Pathway

import base64
import io
import logging

import PIL.Image
from pydantic import BaseModel

import pathway as pw
from pathway.internals.udfs import coerce_async
from pathway.optional_import import optional_imports
from pathway.xpacks.llm.constants import DEFAULT_VISION_MODEL

logger = logging.getLogger(__name__)


def img_to_b64(img: PIL.Image.Image) -> str:
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    img_bytes = buffer.getbuffer()
    return base64.b64encode(img_bytes).decode("utf-8")


def maybe_downscale(
    img: PIL.Image.Image, max_image_size: int, downsize_horizontal_width: int
) -> PIL.Image.Image:
    """Downscale an image if it exceeds `max_image_size` limit, while maintaining the aspect ratio.

    Args:
        - img: The image to be downscaled.
        - max_image_size: The maximum allowable size of the image in bytes.
        - downsize_horizontal_width: The target width for the downscaled image if resizing is needed.
    """
    img_size = img.size[0] * img.size[1] * 3

    if img_size > max_image_size:
        logging.info(
            f"Image size exceeds the limit. Size: `{img_size/(1024 * 1024)}MBs`. Resizing."
        )
        ratio = img.size[1] / img.size[0]  # keep the ratio
        img = img.resize(
            (downsize_horizontal_width, int(downsize_horizontal_width * ratio))
        )

    return img


async def parse(
    b_64_img,
    llm: pw.UDF,
    prompt: str,
    model: str | None = None,
    **kwargs,
) -> str:
    """
    Parse base64 image with the LLM. `model` will be set to llm's default if not provided.
    If llm's `model` is also not set, ``OpenAI`` ``gpt-4o`` will be used.

    Args:
        - b_64_img: Image in base64 format to be parsed. See `img_to_b64` for the conversion utility.
        - llm: LLM instance to be called with image.
        - prompt: Instructions for image parsing.
        - model: Optional LLM model name. Defaults to ``OpenAI`` ``gpt-4o``,
            if neither `model` nor `llm.model` is set.
        - kwargs: Additional arguments to be sent to the LLM inference.
            Refer to the specific provider's API for available options.
            Examples include `temperature`, `max_tokens`, etc.
    """
    model = model or llm.kwargs.get("model") or DEFAULT_VISION_MODEL  # type:ignore

    content = [
        {"type": "text", "text": prompt},
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b_64_img}"},
        },
    ]

    messages = [
        {
            "role": "user",
            "content": content,
        }
    ]

    logger.info(f"Parsing table, model: {model}\nmessages: {str(content)[:350]}...")

    response = await coerce_async(llm.func)(model=model, messages=messages, **kwargs)

    logger.info(f"Parsed table, model: {model}\nmessages: {str(response)}...")

    return response


async def parse_image_details(
    b_64_img,
    parse_schema: type[BaseModel],
    model: str = DEFAULT_VISION_MODEL,
    openai_client_args: dict = {},
    **kwargs,
) -> BaseModel:
    """Parse Pydantic schema from given base64 image."""
    with optional_imports("xpack-llm"):
        import instructor
        import openai

    client = instructor.from_openai(openai.AsyncOpenAI(**openai_client_args))

    content = [
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b_64_img}"},
        },
    ]

    messages = [
        {
            "role": "user",
            "content": content,
        }
    ]

    logger.info(
        f"Parsing slide details, schema: {parse_schema}, model: {model}\nmessages: {str(content)[:350]}..."
    )

    user_info = await client.chat.completions.create(
        model=model,
        response_model=parse_schema,
        messages=messages,
        **kwargs,
    )

    return user_info

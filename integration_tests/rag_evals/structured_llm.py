# type: ignore
# this is not pretty but mypy complains about ObjectWrapper
# and gives some other false positives
import copy
import json
import logging
import uuid

import openai
from pydantic import BaseModel

import pathway as pw
from pathway.xpacks.llm.llms import OpenAIChat, _prep_message_log


class OpenAIStructuredChat(OpenAIChat):

    async def __wrapped__(
        self,
        messages: list[dict] | pw.Json,
        response_format: type[BaseModel] | pw.PyObjectWrapper,
        **kwargs,
    ) -> pw.Json:  # pw.PyObjectWrapper[BaseModel]
        if isinstance(response_format, pw.PyObjectWrapper):
            response_format = response_format.value

        if isinstance(messages, pw.Json):
            messages_decoded: list[openai.ChatCompletionMessageParam] = messages.value  # type: ignore
        else:
            messages_decoded = messages

        kwargs = {**self.kwargs, **kwargs}

        verbose: bool = kwargs.pop("verbose", False)
        api_key = kwargs.pop("api_key", None)
        base_url = kwargs.pop("base_url", None)

        msg_id = str(uuid.uuid4())[-8:]

        event = {
            "_type": "structured_openai_chat_request",
            "kwargs": copy.deepcopy(kwargs),
            "id": msg_id,
            "messages": _prep_message_log(messages_decoded, verbose),
        }
        logging.info(json.dumps(event))

        client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)
        ret = await client.beta.chat.completions.parse(
            messages=messages,
            response_format=response_format,
            **kwargs,
        )

        response: BaseModel | None = ret.choices[0].message.parsed
        assert isinstance(response, response_format)

        _response_str: str = response.model_dump_json()

        event = {
            "_type": "structured_openai_chat_response",
            "response": _response_str if verbose else _response_str[:50],
            "id": msg_id,
        }
        logging.info(json.dumps(event))

        return pw.Json(response.model_dump())

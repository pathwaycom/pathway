import json

import httpx
import requests

from pathway.xpacks.llm.document_store import DocumentStoreClient
from pathway.xpacks.llm.question_answering import RAGClient, send_post_request


async def a_send_post_request(
    url: str, data: dict, headers: dict = {}, timeout: int | None = None
):
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()


# Use DocumentStoreClient from pathway instead of local implementation
VectorStoreClient = DocumentStoreClient


class RagConnector:
    """Rag connector for evals. Returns context docs in `answer_question`."""

    def __init__(self, base_url: str):
        self.base_url = base_url

        self.index_client = DocumentStoreClient(
            url=base_url,
        )

    def answer_question(
        self,
        prompt,
        filter=None,
        model=None,
        return_context_docs: bool | None = True,
    ) -> dict:
        api_url = f"{self.base_url}/v2/answer"
        payload = {
            "prompt": prompt,
        }

        if filter:
            payload["filters"] = filter

        if model:
            payload["model"] = model

        if return_context_docs is not None:
            payload["return_context_docs"] = return_context_docs

        response = send_post_request(api_url, payload)

        return response

    def list_documents(self, filter=None, keys=["path"]):
        api_url = f"{self.base_url}/v2/list_documents"
        payload = {}

        if filter:
            payload["metadata_filter"] = filter

        response = send_post_request(api_url, payload)
        result = [{k: v for k, v in dc.items() if k in keys} for dc in response]
        return result


# TODO: switch to this, replace asyncio.to_thread in evaluator
class ARAGClient(RAGClient):
    async def a_list_documents(
        self, filters: str | None = None, keys: list[str] = ["path"]
    ):
        """
        List indexed documents from the vector store with optional filtering.

        Args:
            - filters: Optional metadata filter for the documents.
            - keys: List of metadata keys to be included in the response.
                Defaults to ``["path"]``. Setting to ``None`` will retrieve all available metadata.
        """
        api_url = f"{self.url}/v2/list_documents"
        payload = {}

        if filters:
            payload["metadata_filter"] = filters

        response: list[dict] = await a_send_post_request(
            api_url, payload, self.additional_headers
        )

        if response:
            if keys:
                result = [{k: v for k, v in dc.items() if k in keys} for dc in response]
            else:
                result = response
        else:
            result = []
        return result

    async def a_answer(
        self,
        prompt: str,
        filters: str | None = None,
        model: str | None = None,
        return_context_docs: bool | None = True,
    ):
        """
        Return RAG answer based on a given prompt and optional filter.

        Args:
            - prompt: Question to be asked.
            - filters: Optional metadata filter for the documents. Defaults to ``None``, which
                means there will be no filter.
            - model: Optional LLM model. If ``None``, app default will be used by the server.
        """
        api_url = f"{self.url}/v2/answer"
        payload = {
            "prompt": prompt,
        }

        if filters:
            payload["filters"] = filters

        if model:
            payload["model"] = model

        if return_context_docs is not None:
            payload["return_context_docs"] = return_context_docs  # type: ignore

        response = await a_send_post_request(api_url, payload, self.additional_headers)
        return response

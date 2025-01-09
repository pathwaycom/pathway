import json

import httpx
import requests

OPENAI_API_KEY = "sk-................................................"  # placeholder, passes the rag app


def send_post_request(
    url: str, data: dict, headers: dict = {}, timeout: int | None = None
):
    response = requests.post(url, json=data, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


async def a_send_post_request(
    url: str, data: dict, headers: dict = {}, timeout: int | None = None
):
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()


def prep_payload(payload):
    payload["openai_api_key"] = OPENAI_API_KEY
    return payload


class VectorStoreClient:
    """
    A client you can use to query VectorStoreServer.

    Please provide either the `url`, or `host` and `port`.

    Args:
        host: host on which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer>`_ listens
        port: port on which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer>`_ listens
        url: url at which `VectorStoreServer </developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer>`_ listens
        timeout: timeout for the post requests in seconds
    """  # noqa

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        url: str | None = None,
        timeout: int | None = 30,
        additional_headers: dict | None = None,
    ):
        err = "Either (`host` and `port`) or `url` must be provided, but not both."
        if url is not None:
            if host or port:
                raise ValueError(err)
            self.url = url
        else:
            if host is None:
                raise ValueError(err)
            port = port or 80
            self.url = f"http://{host}:{port}"

        self.timeout = timeout
        self.additional_headers = additional_headers or {}

    def query(
        self,
        query: str,
        k: int = 3,
        metadata_filter: str | None = None,
        filepath_globpattern: str | None = None,
    ) -> list[dict]:
        """
        Perform a query to the vector store and fetch results.

        Args:
            query:
            k: number of documents to be returned
            metadata_filter: optional string representing the metadata filtering query
                in the JMESPath format. The search will happen only for documents
                satisfying this filtering.
            filepath_globpattern: optional glob pattern specifying which documents
                will be searched for this query.
        """

        data = {"query": query, "k": k}
        if metadata_filter is not None:
            data["metadata_filter"] = metadata_filter
        if filepath_globpattern is not None:
            data["filepath_globpattern"] = filepath_globpattern
        url = self.url + "/v1/retrieve"
        response = requests.post(
            url,
            data=json.dumps(data),
            headers=self._get_request_headers(),
            timeout=self.timeout,
        )

        responses = response.json()
        return sorted(responses, key=lambda x: x["dist"])

    # Make an alias
    __call__ = query

    def get_vectorstore_statistics(self):
        """Fetch basic statistics about the vector store."""

        url = self.url + "/v1/statistics"
        response = requests.post(
            url,
            json={},
            headers=self._get_request_headers(),
            timeout=self.timeout,
        )
        responses = response.json()
        return responses

    def get_input_files(
        self,
        metadata_filter: str | None = None,
        filepath_globpattern: str | None = None,
    ):
        """
        Fetch information on documents in the the vector store.

        Args:
            metadata_filter: optional string representing the metadata filtering query
                in the JMESPath format. The search will happen only for documents
                satisfying this filtering.
            filepath_globpattern: optional glob pattern specifying which documents
                will be searched for this query.
        """
        url = self.url + "/v1/inputs"
        response = requests.post(
            url,
            json={
                "metadata_filter": metadata_filter,
                "filepath_globpattern": filepath_globpattern,
            },
            headers=self._get_request_headers(),
            timeout=self.timeout,
        )
        responses = response.json()
        return responses

    def _get_request_headers(self):
        request_headers = {"Content-Type": "application/json"}
        request_headers.update(self.additional_headers)
        return request_headers


class RagConnector:
    """Rag connector for evals. Returns context docs in `pw_ai_answer_question`."""

    def __init__(self, base_url: str):
        self.base_url = base_url

        self.index_client = VectorStoreClient(
            url=base_url,
        )

    def pw_ai_answer_question(
        self,
        prompt,
        filter=None,
        response_type="short",
        model=None,
    ) -> dict:
        api_url = f"{self.base_url}/v1/pw_ai_answer"
        payload = {
            "prompt": prompt,
            "response_type": response_type,
            # "return_context_docs": return_context_docs,  # TODO: later
        }

        if filter:
            payload["filters"] = filter

        if model:
            payload["model"] = model

        result: dict = {}

        response = send_post_request(api_url, prep_payload(payload))

        result["response"] = response

        context_docs = self.index_client.query(prompt, metadata_filter=filter, k=6)

        result["context_docs"] = context_docs

        return result

    def pw_list_documents(self, filter=None, keys=["path"]):
        api_url = f"{self.base_url}/v1/pw_list_documents"
        payload = {}

        if filter:
            payload["metadata_filter"] = filter

        response = send_post_request(api_url, prep_payload(payload))
        result = [{k: v for k, v in dc.items() if k in keys} for dc in response]
        return result


class RAGClient:
    """
    Connector for interacting with the Pathway RAG applications.
    Either (`host` and `port`) or `url` must be set.

    Args:
        - host: The host of the RAG service.
        - port: The port of the RAG service.
        - url: The URL of the RAG service.
        - timeout: Timeout for requests in seconds. Defaults to 90.
        - additional_headers: Additional headers for the requests.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        url: str | None = None,
        timeout: int | None = 90,
        additional_headers: dict | None = None,
    ):
        err = "Either (`host` and `port`) or `url` must be provided, but not both."
        if url is not None:
            if host is not None or port is not None:
                raise ValueError(err)
            self.url = url
        else:
            if host is None:
                raise ValueError(err)
            port = port or 80

            protocol = "https" if port == 443 else "http"
            self.url = f"{protocol}://{host}:{port}"

        self.timeout = timeout
        self.additional_headers = additional_headers or {}

        self.index_client = VectorStoreClient(
            url=self.url,
            timeout=self.timeout,
            additional_headers=self.additional_headers,
        )

    def retrieve(
        self,
        query: str,
        k: int = 3,
        metadata_filter: str | None = None,
        filepath_globpattern: str | None = None,
    ):
        """
        Retrieve closest documents from the vector store based on a query.

        Args:
            - query: The query string.
            - k: The number of results to retrieve.
            - metadata_filter: Optional metadata filter for the documents. Defaults to `None`, which
                means there will be no filter.
            - filepath_globpattern: Glob pattern for file paths.
        """
        return self.index_client.query(
            query=query,
            k=k,
            metadata_filter=metadata_filter,
            filepath_globpattern=filepath_globpattern,
        )

    def statistics(
        self,
    ):
        """
        Retrieve stats from the vector store.
        """
        return self.index_client.get_vectorstore_statistics()

    def pw_ai_answer(
        self,
        prompt: str,
        filters: str | None = None,
        model: str | None = None,
    ):
        """
        Return RAG answer based on a given prompt and optional filter.

        Args:
            - prompt: Question to be asked.
            - filters: Optional metadata filter for the documents. Defaults to ``None``, which
                means there will be no filter.
            - model: Optional LLM model. If ``None``, app default will be used by the server.
        """
        api_url = f"{self.url}/v1/pw_ai_answer"
        payload = {
            "prompt": prompt,
        }

        if filters:
            payload["filters"] = filters

        if model:
            payload["model"] = model

        response = send_post_request(api_url, payload, self.additional_headers)
        return response

    def pw_ai_summary(
        self,
        text_list: list[str],
        model: str | None = None,
    ):
        """
        Summarize a list of texts.

        Args:
            - text_list: List of texts to summarize.
            - model: Optional LLM model. If ``None``, app default will be used by the server.
        """
        api_url = f"{self.url}/v1/pw_ai_summary"
        payload: dict = {
            "text_list": text_list,
        }

        if model:
            payload["model"] = model

        response = send_post_request(api_url, payload, self.additional_headers)
        return response

    def pw_list_documents(self, filters: str | None = None, keys: list[str] = ["path"]):
        """
        List indexed documents from the vector store with optional filtering.

        Args:
            - filters: Optional metadata filter for the documents.
            - keys: List of metadata keys to be included in the response.
                Defaults to ``["path"]``. Setting to ``None`` will retrieve all available metadata.
        """
        api_url = f"{self.url}/v1/pw_list_documents"
        payload = {}

        if filters:
            payload["metadata_filter"] = filters

        response: list[dict] = send_post_request(
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


# TODO: switch to this, replace asyncio.to_thread in evaluator
class ARAGClient(RAGClient):
    async def a_pw_list_documents(
        self, filters: str | None = None, keys: list[str] = ["path"]
    ):
        """
        List indexed documents from the vector store with optional filtering.

        Args:
            - filters: Optional metadata filter for the documents.
            - keys: List of metadata keys to be included in the response.
                Defaults to ``["path"]``. Setting to ``None`` will retrieve all available metadata.
        """
        api_url = f"{self.url}/v1/pw_list_documents"
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

    async def pw_ai_answer(
        self,
        prompt: str,
        filters: str | None = None,
        model: str | None = None,
    ):
        """
        Return RAG answer based on a given prompt and optional filter.

        Args:
            - prompt: Question to be asked.
            - filters: Optional metadata filter for the documents. Defaults to ``None``, which
                means there will be no filter.
            - model: Optional LLM model. If ``None``, app default will be used by the server.
        """
        api_url = f"{self.url}/v1/pw_ai_answer"
        payload = {
            "prompt": prompt,
        }

        if filters:
            payload["filters"] = filters

        if model:
            payload["model"] = model

        response = await a_send_post_request(api_url, payload, self.additional_headers)
        return response

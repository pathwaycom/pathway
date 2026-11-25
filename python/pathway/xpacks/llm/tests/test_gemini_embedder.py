# Copyright © 2026 Pathway

import json
from importlib.metadata import version
from types import SimpleNamespace

import numpy as np
import pytest
from google import genai
from google.genai import types

import pathway as pw
from pathway.xpacks.llm.embedders import GeminiEmbedder


@pytest.fixture
def gemini_http(monkeypatch):
    if version("google-genai").startswith("2."):
        import httpx
    else:
        import httpx2 as httpx

    for name in ("GOOGLE_API_KEY", "GEMINI_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    state = SimpleNamespace(requests=[], responses=[], clients=[])
    original_client = genai.Client

    def respond(request):
        state.requests.append(request)
        status, payload = (
            state.responses.pop(0)
            if state.responses
            else (200, {"embeddings": [{"values": [0.1, -0.2, 0.3]}]})
        )
        return httpx.Response(status, json=payload)

    def create_client(**kwargs):
        options = types.HttpOptions.model_validate(kwargs.pop("http_options", {}))
        options = options.model_copy(
            update={
                "httpx_client": httpx.Client(transport=httpx.MockTransport(respond)),
            }
        )
        client = original_client(http_options=options, **kwargs)
        state.clients.append(client)
        return client

    monkeypatch.setattr(genai, "Client", create_client)
    yield state
    for client in state.clients:
        client.close()


def test_gemini_embedder_preserves_model_parameters(gemini_http):
    embedder = GeminiEmbedder(
        model="models/custom-model",
        api_key="test-key",
        task_type="retrieval_document",
        title="Document title",
        output_dimensionality=3,
    )

    result = embedder.__wrapped__("Document text")

    np.testing.assert_array_equal(result, np.array([0.1, -0.2, 0.3]))
    request = gemini_http.requests[0]
    assert request.url.path == "/v1beta/models/custom-model:batchEmbedContents"
    assert request.headers["x-goog-api-key"] == "test-key"
    body = json.loads(request.content)["requests"][0]
    assert body["model"] == "models/custom-model"
    assert body["content"]["parts"] == [{"text": "Document text"}]
    assert {
        key: body[key] for key in ("taskType", "title", "outputDimensionality")
    } == {
        "taskType": "retrieval_document",
        "title": "Document title",
        "outputDimensionality": 3,
    }


def test_gemini_embedder_call_overrides(gemini_http):
    embedder = GeminiEmbedder(
        model="models/constructor-model", api_key="constructor-key", title="Old title"
    )
    embedder.__wrapped__(
        "Text",
        model="models/row-model",
        api_key="row-key",
        title="New title",
    )
    request = gemini_http.requests[0]
    assert request.url.path == "/v1beta/models/row-model:batchEmbedContents"
    assert request.headers["x-goog-api-key"] == "row-key"
    assert json.loads(request.content)["requests"][0]["title"] == "New title"
    assert embedder.kwargs["title"] == "Old title"


def test_gemini_embedder_clients_keep_keys_separate(gemini_http):
    first = GeminiEmbedder(model="custom-model", api_key="first-key")
    second = GeminiEmbedder(model="custom-model", api_key="second-key")

    first.__wrapped__("First")
    second.__wrapped__("Second")
    first.__wrapped__("Third")

    assert [r.headers["x-goog-api-key"] for r in gemini_http.requests] == [
        "first-key",
        "second-key",
        "first-key",
    ]
    assert len(gemini_http.clients) == 2


def test_gemini_embedder_environment_key(gemini_http, monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY", "environment-key")
    embedder = GeminiEmbedder(model="custom-model")
    embedder.__wrapped__("Text")
    assert gemini_http.requests[0].headers["x-goog-api-key"] == "environment-key"


def test_gemini_embedder_legacy_timeout(gemini_http):
    request_options = {"timeout": 1.25}
    http_options = types.HttpOptions(headers={"x-custom-header": "custom-value"})
    embedder = GeminiEmbedder(
        model="custom-model",
        api_key="test-key",
        request_options=request_options,
        http_options=http_options,
    )
    embedder.__wrapped__("Text")
    request = gemini_http.requests[0]
    assert request.extensions["timeout"]["read"] == 1.25
    assert request.headers["x-custom-header"] == "custom-value"
    assert request_options == {"timeout": 1.25}
    assert http_options.timeout is None


def test_gemini_embedder_legacy_retry(gemini_http):
    from google.api_core.retry import Retry

    gemini_http.responses.append(
        (503, {"error": {"code": 503, "status": "UNAVAILABLE", "message": "Busy"}})
    )
    embedder = GeminiEmbedder(
        model="custom-model",
        api_key="test-key",
        request_options={"retry": Retry(initial=0, maximum=0, timeout=2)},
    )
    np.testing.assert_array_equal(
        embedder.__wrapped__("Text"), np.array([0.1, -0.2, 0.3])
    )
    assert len(gemini_http.requests) == 2


def test_gemini_embedder_explicitly_disabled_sdk_retry(gemini_http):
    gemini_http.responses.append(
        (503, {"error": {"code": 503, "status": "UNAVAILABLE", "message": "Busy"}})
    )
    embedder = GeminiEmbedder(
        model="custom-model", api_key="test-key", request_options={"retry": None}
    )
    with pytest.raises(genai.errors.ServerError):
        embedder.__wrapped__("Text")
    assert len(gemini_http.requests) == 1


@pytest.mark.asyncio
async def test_gemini_embedder_pathway_retry_and_disk_cache(
    gemini_http, monkeypatch, tmp_path
):
    monkeypatch.setenv("PATHWAY_PERSISTENT_STORAGE", str(tmp_path))
    gemini_http.responses.append(
        (503, {"error": {"code": 503, "status": "UNAVAILABLE", "message": "Busy"}})
    )
    embedder = GeminiEmbedder(
        model="custom-model",
        api_key="test-key",
        request_options={"retry": None},
        retry_strategy=pw.udfs.FixedDelayRetryStrategy(max_retries=1, delay_ms=1),
        cache_strategy=pw.udfs.DiskCache(),
    )
    for _ in range(2):
        np.testing.assert_array_equal(
            await embedder.func("Text"), np.array([0.1, -0.2, 0.3])
        )
    assert len(gemini_http.requests) == 2


def test_gemini_embedder_keeps_supplied_client_open(gemini_http):
    client = genai.Client(vertexai=False, api_key="client-key")
    embedder = GeminiEmbedder(model="custom-model", client=client)
    embedder.__wrapped__("First")
    embedder.__wrapped__("Second")
    assert len(gemini_http.requests) == 2
    assert all(
        r.headers["x-goog-api-key"] == "client-key" for r in gemini_http.requests
    )


@pytest.mark.parametrize("payload", [{}, {"embeddings": []}, {"embeddings": [{}]}])
def test_gemini_embedder_rejects_missing_embedding(gemini_http, payload):
    gemini_http.responses.append((200, payload))
    embedder = GeminiEmbedder(model="custom-model", api_key="test-key")
    with pytest.raises(ValueError, match="returned no embedding"):
        embedder.__wrapped__("Text")


def test_gemini_embedder_propagates_api_error(gemini_http):
    gemini_http.responses.append(
        (400, {"error": {"code": 400, "status": "INVALID_ARGUMENT", "message": "Bad"}})
    )
    embedder = GeminiEmbedder(model="custom-model", api_key="test-key")
    with pytest.raises(genai.errors.ClientError):
        embedder.__wrapped__("Text")
    assert len(gemini_http.requests) == 1


def test_gemini_embedder_per_row_model_in_graph(gemini_http):
    embedder = GeminiEmbedder(model=None, api_key="test-key")
    table = pw.debug.table_from_markdown(
        """
        text | model
        First | models/first-model
        Second | models/second-model
        """
    )
    result = table.select(vector=embedder(table.text, model=table.model))
    values = pw.debug.table_to_pandas(result)["vector"]
    assert len(values) == 2
    for vector in values:
        np.testing.assert_array_equal(vector, np.array([0.1, -0.2, 0.3]))
    assert {r.url.path for r in gemini_http.requests} == {
        "/v1beta/models/first-model:batchEmbedContents",
        "/v1beta/models/second-model:batchEmbedContents",
    }

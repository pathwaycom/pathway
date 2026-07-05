# Copyright © 2026 Pathway

"""Tests for the TwelveLabs video-RAG components.

The no-network tests stub the TwelveLabs SDK and run without any credentials.
The live test is skipped unless ``TWELVELABS_API_KEY`` is set in the environment.

Run with::

    pytest python/pathway/xpacks/llm/tests/test_twelvelabs.py
"""

import os

import numpy as np
import pytest

from pathway.xpacks.llm.embedders import DEFAULT_MARENGO_MODEL, MarengoEmbedder
from pathway.xpacks.llm.parsers import DEFAULT_PEGASUS_MODEL, TwelveLabsVideoParser


class _FakeSegment:
    def __init__(self, vector):
        self.float_ = vector


class _FakeTextEmbedding:
    def __init__(self, vector):
        self.segments = [_FakeSegment(vector)]


class _FakeEmbeddingResponse:
    def __init__(self, vector):
        self.text_embedding = _FakeTextEmbedding(vector)


class _FakeEmbed:
    def __init__(self, vector):
        self._vector = vector
        self.calls = []

    def create(self, *, model_name, text):
        self.calls.append((model_name, text))
        return _FakeEmbeddingResponse(self._vector)


class _FakeAsset:
    def __init__(self, id, status):
        self.id = id
        self.status = status


class _FakeAssets:
    def __init__(self):
        self.uploaded = None
        self.deleted = []

    def create(self, *, method, file, filename):
        self.uploaded = (method, filename)
        return _FakeAsset("asset-123", "ready")

    def retrieve(self, asset_id):
        return _FakeAsset(asset_id, "ready")

    def delete(self, asset_id):
        self.deleted.append(asset_id)


class _FakeAnalyzeResponse:
    def __init__(self, data):
        self.data = data


class _FakeClient:
    def __init__(self, *, vector=None, analyze_text="a description"):
        self.embed = _FakeEmbed(vector or [0.0] * 512)
        self.assets = _FakeAssets()
        self._analyze_text = analyze_text
        self.analyze_calls = []

    def analyze(self, **kwargs):
        self.analyze_calls.append(kwargs)
        return _FakeAnalyzeResponse(self._analyze_text)


# --- No-network unit tests -------------------------------------------------


class _FakeAsyncEmbed:
    def __init__(self, vector):
        self._vector = vector
        self.calls = []

    async def create(self, *, model_name, text):
        self.calls.append((model_name, text))
        return _FakeEmbeddingResponse(self._vector)


class _FakeAsyncClient:
    def __init__(self, *, vector=None):
        self.embed = _FakeAsyncEmbed(vector or [0.0] * 512)


def test_embedder_returns_vector_array():
    embedder = MarengoEmbedder()
    embedder._client = _FakeClient(vector=list(range(512)))

    out = embedder._embed_one("a red car")

    assert isinstance(out, np.ndarray)
    assert out.shape == (512,)
    assert out.dtype == np.float32
    assert embedder._client.embed.calls == [(DEFAULT_MARENGO_MODEL, "a red car")]


def test_embedder_defaults():
    embedder = MarengoEmbedder()
    assert embedder.model == DEFAULT_MARENGO_MODEL
    # Marengo embeds a single text per request.
    assert embedder.max_batch_size == 1


def test_embedder_wrapped_is_async_and_concurrent():
    # The hot path runs on the async client and returns one array per input.
    import asyncio

    embedder = MarengoEmbedder()
    embedder._aclient = _FakeAsyncClient(vector=list(range(512)))

    out = asyncio.run(embedder.__wrapped__(["a red car", "a blue boat"]))

    assert len(out) == 2
    for arr in out:
        assert isinstance(arr, np.ndarray)
        assert arr.shape == (512,)
        assert arr.dtype == np.float32
    assert embedder._aclient.embed.calls == [
        (DEFAULT_MARENGO_MODEL, "a red car"),
        (DEFAULT_MARENGO_MODEL, "a blue boat"),
    ]


def test_embedding_dimension_probe_returns_512():
    # `BaseEmbedder.get_embedding_dimension` probes `__wrapped__` with a single
    # string (not a list); the index factory relies on this returning the true
    # vector size, so `__wrapped__` must handle a bare string input.
    embedder = MarengoEmbedder()
    embedder._client = _FakeClient(vector=[0.0] * 512)
    assert embedder.get_embedding_dimension() == 512


def test_video_parser_uploads_then_analyzes_and_deletes_asset():
    # Default: delete_assets=True -> asset is removed and id omitted from metadata.
    parser = TwelveLabsVideoParser(prompt="What happens?")
    parser._client = _FakeClient(analyze_text="A red car drives on a highway.")

    out = parser.__wrapped__(b"fake-video-bytes")

    assert out == [("A red car drives on a highway.", {})]
    # Asset was uploaded via the direct method...
    assert parser._client.assets.uploaded is not None
    assert parser._client.assets.uploaded[0] == "direct"
    # ...deleted afterwards so runs don't flood the asset list...
    assert parser._client.assets.deleted == ["asset-123"]
    # ...and Pegasus was called with the right model, prompt and asset.
    (call,) = parser._client.analyze_calls
    assert call["model_name"] == DEFAULT_PEGASUS_MODEL
    assert call["prompt"] == "What happens?"
    assert call["video"].asset_id == "asset-123"


def test_video_parser_keeps_asset_when_disabled():
    parser = TwelveLabsVideoParser(delete_assets=False)
    parser._client = _FakeClient(analyze_text="desc")

    out = parser.__wrapped__(b"bytes")

    assert out == [("desc", {"twelvelabs_asset_id": "asset-123"})]
    assert parser._client.assets.deleted == []


def test_video_parser_failed_asset_raises():
    parser = TwelveLabsVideoParser()
    client = _FakeClient()
    client.assets.create = lambda **kw: _FakeAsset("a", "failed")
    parser._client = client

    with pytest.raises(RuntimeError):
        parser.__wrapped__(b"bytes")
    # Failure happens during upload (before analyze); nothing was deleted.
    assert client.assets.deleted == []


def test_video_parser_deletes_asset_even_when_analyze_raises():
    parser = TwelveLabsVideoParser()
    client = _FakeClient()

    def _boom(**kwargs):
        raise RuntimeError("pegasus exploded")

    client.analyze = _boom
    parser._client = client

    with pytest.raises(RuntimeError):
        parser.__wrapped__(b"bytes")
    # try/finally still cleaned up the uploaded asset.
    assert client.assets.deleted == ["asset-123"]


# --- Live smoke test (requires TWELVELABS_API_KEY) -------------------------


@pytest.mark.skipif(
    not os.environ.get("TWELVELABS_API_KEY"),
    reason="TWELVELABS_API_KEY not set; skipping live TwelveLabs call",
)
def test_marengo_live_embedding_is_512_dim():
    embedder = MarengoEmbedder()
    vector = embedder._embed_one("a red car driving on a highway")
    assert vector.shape == (512,)

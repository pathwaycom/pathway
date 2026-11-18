# Copyright © 2026 Pathway

"""Tests for the TwelveLabs video-RAG components.

The no-network tests stub the TwelveLabs SDK and run without any credentials.
The live tests are skipped unless ``TWELVELABS_API_KEY`` is set in the
environment (the live parser test additionally needs ``ffmpeg`` on PATH to
generate a short test video).

Run with::

    pytest python/pathway/xpacks/llm/tests/test_twelvelabs.py
"""

import asyncio
import logging
import os
import shutil
import subprocess

import numpy as np
import pytest

import pathway.xpacks.llm.parsers as parsers_module
from pathway import udfs
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


class _FakeAsset:
    def __init__(self, id, status):
        self.id = id
        self.status = status


class _FakeAsyncAssets:
    def __init__(self):
        self.uploaded = None
        self.deleted = []

    async def create(self, *, method, file, filename):
        self.uploaded = (method, filename)
        return _FakeAsset("asset-123", "ready")

    async def retrieve(self, asset_id):
        return _FakeAsset(asset_id, "ready")

    async def delete(self, asset_id):
        self.deleted.append(asset_id)


class _FakeAnalyzeResponse:
    def __init__(self, data):
        self.data = data


class _FakeAsyncEmbed:
    def __init__(self, vector):
        self._vector = vector
        self.calls = []

    async def create(self, *, model_name, text):
        self.calls.append((model_name, text))
        return _FakeEmbeddingResponse(self._vector)


class _FakeAsyncClient:
    def __init__(self, *, vector=None, analyze_text="a description"):
        self.embed = _FakeAsyncEmbed(vector or [0.0] * 512)
        self.assets = _FakeAsyncAssets()
        self._analyze_text = analyze_text
        self.analyze_calls = []

    async def analyze(self, **kwargs):
        self.analyze_calls.append(kwargs)
        return _FakeAnalyzeResponse(self._analyze_text)


# --- No-network unit tests: embedder ----------------------------------------


def test_embedder_defaults():
    embedder = MarengoEmbedder()
    assert embedder.model == DEFAULT_MARENGO_MODEL
    # Marengo embeds a single text per request.
    assert embedder.max_batch_size == 1


def test_embedder_wrapped_is_async_and_concurrent():
    # The hot path runs on the async client and returns one array per input.
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


def test_embedding_dimension_known_without_api_call():
    # Index factories ask for the dimension at graph-build time; by default it
    # must come from the known model dimension, without touching the network
    # (no client is configured here, so an API call would fail loudly).
    embedder = MarengoEmbedder()
    assert embedder.get_embedding_dimension() == 512


def test_embedding_dimension_probes_api_when_unknown(monkeypatch):
    # With embedding_dimension=None the dimension is probed with a real
    # (here: stubbed) single-text request.
    fake_client = _FakeAsyncClient(vector=[0.0] * 512)
    monkeypatch.setattr(
        "pathway.xpacks.llm.embedders._build_async_twelvelabs_client",
        lambda api_key: fake_client,
    )
    embedder = MarengoEmbedder(embedding_dimension=None)
    assert embedder.get_embedding_dimension() == 512
    assert fake_client.embed.calls == [(DEFAULT_MARENGO_MODEL, ".")]


# --- No-network unit tests: parser ------------------------------------------


def test_video_parser_hot_path_is_async():
    # Parsing takes minutes per video (upload + polling + Pegasus), so the hot
    # path must be a coroutine running on an async executor — a blocking
    # implementation would serialize videos and hold engine threads.
    parser = TwelveLabsVideoParser()
    assert asyncio.iscoroutinefunction(parser.__wrapped__)


def test_video_parser_uploads_then_analyzes_and_deletes_asset():
    # Default: delete_assets=True -> asset is removed and id omitted from metadata.
    parser = TwelveLabsVideoParser(prompt="What happens?")
    parser._aclient = _FakeAsyncClient(analyze_text="A red car drives on a highway.")

    out = asyncio.run(parser.__wrapped__(b"fake-video-bytes"))

    assert out == [("A red car drives on a highway.", {})]
    # Asset was uploaded via the direct method...
    assert parser._aclient.assets.uploaded is not None
    assert parser._aclient.assets.uploaded[0] == "direct"
    # ...deleted afterwards so runs don't flood the asset list...
    assert parser._aclient.assets.deleted == ["asset-123"]
    # ...and Pegasus was called with the right model, prompt and asset.
    (call,) = parser._aclient.analyze_calls
    assert call["model_name"] == DEFAULT_PEGASUS_MODEL
    assert call["prompt"] == "What happens?"
    assert call["video"].asset_id == "asset-123"


def test_video_parser_uploads_with_configured_format():
    parser = TwelveLabsVideoParser(video_format="webm")
    parser._aclient = _FakeAsyncClient()

    asyncio.run(parser.__wrapped__(b"bytes"))

    assert parser._aclient.assets.uploaded == ("direct", "video.webm")


def test_video_parser_keeps_asset_when_disabled():
    parser = TwelveLabsVideoParser(delete_assets=False)
    parser._aclient = _FakeAsyncClient(analyze_text="desc")

    out = asyncio.run(parser.__wrapped__(b"bytes"))

    assert out == [("desc", {"twelvelabs_asset_id": "asset-123"})]
    assert parser._aclient.assets.deleted == []


def test_video_parser_rejects_oversized_video_before_upload(monkeypatch):
    # Pegasus accepts videos up to 2 GB; an oversized video must be rejected
    # locally, before its bytes are sent over the network.
    monkeypatch.setattr(parsers_module, "_PEGASUS_MAX_VIDEO_BYTES", 8)
    parser = TwelveLabsVideoParser()
    client = _FakeAsyncClient()
    parser._aclient = client

    with pytest.raises(ValueError, match="2 GB"):
        asyncio.run(parser.__wrapped__(b"way-more-than-8-bytes"))
    assert client.assets.uploaded is None


def test_video_parser_failed_asset_raises():
    parser = TwelveLabsVideoParser()
    client = _FakeAsyncClient()

    async def _create_failed(**kwargs):
        return _FakeAsset("a", "failed")

    client.assets.create = _create_failed
    parser._aclient = client

    with pytest.raises(RuntimeError):
        asyncio.run(parser.__wrapped__(b"bytes"))
    # Failure happens during upload (before analyze); nothing was deleted.
    assert client.assets.deleted == []


def test_video_parser_deletes_asset_even_when_analyze_raises():
    parser = TwelveLabsVideoParser(retry_strategy=None)
    client = _FakeAsyncClient()

    async def _boom(**kwargs):
        raise RuntimeError("pegasus exploded")

    client.analyze = _boom
    parser._aclient = client

    with pytest.raises(RuntimeError):
        asyncio.run(parser.__wrapped__(b"bytes"))
    # try/finally still cleaned up the uploaded asset.
    assert client.assets.deleted == ["asset-123"]


def test_video_parser_retries_transient_failures():
    parser = TwelveLabsVideoParser(
        retry_strategy=udfs.ExponentialBackoffRetryStrategy(
            max_retries=2, initial_delay=1, jitter_ms=1
        )
    )
    client = _FakeAsyncClient(analyze_text="desc")
    attempts = {"n": 0}
    successful_analyze = client.analyze

    async def flaky_analyze(**kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("transient API error")
        return await successful_analyze(**kwargs)

    client.analyze = flaky_analyze
    parser._aclient = client

    out = asyncio.run(parser.__wrapped__(b"bytes"))

    assert out == [("desc", {})]
    assert attempts["n"] == 2
    # A retry re-runs the whole parse, so the asset was uploaded (and cleaned
    # up) once per attempt.
    assert client.assets.deleted == ["asset-123", "asset-123"]


def test_video_parser_on_error_skip_drops_failed_video():
    # In production a single malformed video must not halt the pipeline:
    # after retries are exhausted, on_error="skip" produces no chunks.
    parser = TwelveLabsVideoParser(retry_strategy=None, on_error="skip")
    client = _FakeAsyncClient()

    async def _boom(**kwargs):
        raise RuntimeError("pegasus exploded")

    client.analyze = _boom
    parser._aclient = client

    out = asyncio.run(parser.__wrapped__(b"bytes"))

    assert out == []
    # The uploaded asset was still cleaned up.
    assert client.assets.deleted == ["asset-123"]


def test_video_parser_warns_when_pegasus_returns_no_text(caplog):
    parser = TwelveLabsVideoParser()
    parser._aclient = _FakeAsyncClient(analyze_text=None)

    with caplog.at_level(logging.WARNING):
        out = asyncio.run(parser.__wrapped__(b"bytes"))

    assert out == [("", {})]
    assert any("returned no text" in record.message for record in caplog.records)


# --- Live smoke tests (require TWELVELABS_API_KEY) --------------------------


@pytest.mark.skipif(
    not os.environ.get("TWELVELABS_API_KEY"),
    reason="TWELVELABS_API_KEY not set; skipping live TwelveLabs call",
)
def test_marengo_live_embedding_is_512_dim():
    # Probe the real API for the dimension and check it matches the default
    # the embedder reports without a network call.
    embedder = MarengoEmbedder(embedding_dimension=None)
    assert embedder.get_embedding_dimension() == 512
    assert MarengoEmbedder().get_embedding_dimension() == 512


@pytest.mark.skipif(
    not os.environ.get("TWELVELABS_API_KEY") or shutil.which("ffmpeg") is None,
    reason="TWELVELABS_API_KEY not set or ffmpeg unavailable; "
    "skipping live Pegasus call",
)
def test_pegasus_live_video_parse_returns_description(tmp_path):
    # Generate a 5-second test-pattern video satisfying the Pegasus input
    # limits (>= 4 s, >= 360x360) and run it through the full parse cycle:
    # upload, readiness polling, analysis, asset cleanup.
    video_path = tmp_path / "video.mp4"
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=5:size=480x360:rate=10",
            "-pix_fmt",
            "yuv420p",
            str(video_path),
        ],
        check=True,
        capture_output=True,
    )

    parser = TwelveLabsVideoParser()
    out = asyncio.run(parser.__wrapped__(video_path.read_bytes()))

    assert len(out) == 1
    text, metadata = out[0]
    assert isinstance(text, str)
    assert text.strip()
    assert metadata == {}

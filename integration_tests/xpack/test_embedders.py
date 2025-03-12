import pytest

from pathway.internals.udfs.utils import _coerce_sync
from pathway.xpacks.llm import embedders

SHORT_TEXT = "A"
LONG_TEXT = "B" * 50_000


@pytest.mark.parametrize(
    "text", [SHORT_TEXT, LONG_TEXT], ids=["short_text", "long_text"]
)
@pytest.mark.parametrize(
    "model", [None, "text-embedding-ada-002", "text-embedding-3-small"]
)
@pytest.mark.parametrize("strategy", ["start", "end"])
def test_openai_embedder(text: str, model: str, strategy: str):
    if model is None:
        embedder = embedders.OpenAIEmbedder(truncation_keep_strategy=strategy)
    else:
        embedder = embedders.OpenAIEmbedder(
            model=model, truncation_keep_strategy=strategy  # type: ignore
        )

    sync_embedder = _coerce_sync(embedder.func)

    embedding = sync_embedder(text)

    assert len(embedding) > 1500


@pytest.mark.parametrize("model", ["text-embedding-ada-002", "text-embedding-3-small"])
def test_openai_embedder_fails_no_truncation(model: str):
    truncation_keep_strategy = None
    embedder = embedders.OpenAIEmbedder(
        model=model, truncation_keep_strategy=truncation_keep_strategy
    )

    sync_embedder = _coerce_sync(embedder.func)

    with pytest.raises(Exception) as exc:
        sync_embedder(LONG_TEXT)

    assert "maximum context length" in str(exc)

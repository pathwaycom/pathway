import pytest

import pathway as pw
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
        embedder = embedders.OpenAIEmbedder(
            truncation_keep_strategy=strategy,
            retry_strategy=pw.udfs.ExponentialBackoffRetryStrategy(),
        )
    else:
        embedder = embedders.OpenAIEmbedder(
            model=model,
            truncation_keep_strategy=strategy,  # type: ignore
            retry_strategy=pw.udfs.ExponentialBackoffRetryStrategy(),
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


def test_sentence_transformer_embedder():
    table = pw.debug.table_from_rows(
        schema=pw.schema_from_types(text=str), rows=[("aaa",), ("bbb",)]
    )

    embedder = embedders.SentenceTransformerEmbedder(model="intfloat/e5-large-v2")

    table = table.select(embedding=embedder(pw.this.text))

    result = pw.debug.table_to_pandas(table).to_dict("records")

    assert len(result) == 2
    assert isinstance(result[0]["embedding"][0], float)
    assert len(result[0]["embedding"]) == 1024
    assert isinstance(result[1]["embedding"][0], float)
    assert len(result[1]["embedding"]) == 1024


def test_sentence_transformer_embedder_with_common_parameter():
    table = pw.debug.table_from_rows(
        schema=pw.schema_from_types(text=str), rows=[("aaa",), ("bbb",)]
    )

    embedder = embedders.SentenceTransformerEmbedder(model="intfloat/e5-large-v2")

    table = table.select(embedding=embedder(pw.this.text, normalize_embeddings=True))

    result = pw.debug.table_to_pandas(table).to_dict("records")

    assert len(result) == 2
    assert isinstance(result[0]["embedding"][0], float)
    assert len(result[0]["embedding"]) == 1024
    assert abs(sum([x * x for x in result[0]["embedding"]]) - 1.0) < 0.001
    assert isinstance(result[1]["embedding"][0], float)
    assert len(result[1]["embedding"]) == 1024
    assert abs(sum([x * x for x in result[1]["embedding"]]) - 1.0) < 0.001


def test_sentence_transformer_embedder_with_different_parameter():
    table = pw.debug.table_from_rows(
        schema=pw.schema_from_types(text=str, normalize_embeddings=bool),
        rows=[("aaa", True), ("bbb", False)],
    )

    embedder = embedders.SentenceTransformerEmbedder(model="intfloat/e5-large-v2")

    table = table.select(
        embedding=embedder(
            pw.this.text, normalize_embeddings=pw.this.normalize_embeddings
        )
    )

    result = pw.debug.table_to_pandas(table).to_dict("records")

    assert len(result) == 2
    assert isinstance(result[0]["embedding"][0], float)
    assert len(result[0]["embedding"]) == 1024
    assert abs(sum([x * x for x in result[0]["embedding"]]) - 1.0) < 0.001
    assert isinstance(result[1]["embedding"][0], float)
    assert len(result[1]["embedding"]) == 1024


def test_sentence_transformer_get_embedding_dimension():
    embedder = embedders.SentenceTransformerEmbedder(model="intfloat/e5-large-v2")
    embedding_dimension = embedder.get_embedding_dimension()
    assert embedding_dimension == 1024

    embedding_dimension = embedder.get_embedding_dimension(normalize_embeddings=True)
    assert embedding_dimension == 1024


def test_litellm_embedder():
    table = pw.debug.table_from_rows(
        schema=pw.schema_from_types(text=str), rows=[("aaa",), ("bbb",)]
    )

    embedder = embedders.LiteLLMEmbedder(
        model="text-embedding-3-small",
        retry_strategy=pw.udfs.ExponentialBackoffRetryStrategy(),
    )

    table = table.select(embedding=embedder(pw.this.text))

    result = pw.debug.table_to_pandas(table).to_dict("records")

    assert len(result) == 2
    assert isinstance(result[0]["embedding"][0], float)
    assert len(result[0]["embedding"]) > 1500
    assert isinstance(result[1]["embedding"][0], float)
    assert len(result[1]["embedding"]) > 1500

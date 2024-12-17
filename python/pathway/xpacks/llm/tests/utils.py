import pathway as pw
from pathway.xpacks.llm.question_answering import BaseRAGQuestionAnswerer
from pathway.xpacks.llm.vector_store import VectorStoreServer

from .mocks import FakeChatModel, fake_embeddings_model

DEFAULT_PATHWAY_HOST: str = "127.0.0.1"


def build_vector_store(embedder: pw.UDF | None = None) -> VectorStoreServer:
    """Build vector store instance from an optional embedder, with a single demo doc."""

    if embedder is None:
        embedder = fake_embeddings_model

    docs = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=bytes, _metadata=dict),
        rows=[
            (
                "test".encode("utf-8"),
                {"path": "test_module.py"},
            )
        ],
    )

    vector_server = VectorStoreServer(
        docs,
        embedder=embedder,
    )

    return vector_server


def create_rag_app(**kwargs) -> BaseRAGQuestionAnswerer:
    """Create RAG app with fake embedder and LLM."""
    chat = FakeChatModel()

    vector_server = build_vector_store(fake_embeddings_model)

    rag_app = BaseRAGQuestionAnswerer(
        llm=chat,
        indexer=vector_server,
        default_llm_name="gpt-4o-mini",
        **kwargs,
    )
    return rag_app


def create_build_rag_app(
    port: int, host: str = DEFAULT_PATHWAY_HOST, **kwargs
) -> BaseRAGQuestionAnswerer:
    """Create and build RAG app with fake embedder and LLM.
    Builds the server with optional host and the given port.

    Host and the port will not be occupied until the app is run.
    """

    rag_app = create_rag_app(**kwargs)

    rag_app.build_server(host=host, port=port)

    return rag_app

import os
import time
from multiprocessing import Process
from typing import List

import pytest
import requests
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.node_parser import TextSplitter

import pathway as pw
from pathway.tests.utils import xfail_on_multiple_threads
from pathway.xpacks.llm.vector_store import VectorStoreServer

PATHWAY_HOST = "127.0.0.1"

ENV_PORT = os.environ.get("LLAMA_READER_PORT")
PATHWAY_PORT = 7769

if ENV_PORT:
    PATHWAY_PORT = int(ENV_PORT)


EXAMPLE_TEXT_FILE = "example_text.md"


@pytest.mark.xfail
def get_data_sources():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    example_text_path = os.path.join(test_dir, EXAMPLE_TEXT_FILE)

    data_sources = []
    data_sources.append(
        pw.io.fs.read(
            example_text_path,
            format="binary",
            mode="streaming",
            with_metadata=True,
        )
    )
    return data_sources


def mock_get_text_embedding(text: str) -> List[float]:
    """Mock get text embedding."""
    print("Embedder embedding:", text)
    if text == "Hello world.":
        return [1, 0, 0, 0, 0]
    elif text == "This is a test.":
        return [0, 1, 0, 0, 0]
    elif text == "This is another test.":
        return [0, 0, 1, 0, 0]
    elif text == "This is a test v2.":
        return [0, 0, 0, 1, 0]
    elif text == "This is a test v3.":
        return [0, 0, 0, 0, 1]
    elif text == "This is bar test.":
        return [0, 0, 1, 0, 0]
    elif text == "Hello world backup.":
        return [0, 0, 0, 0, 1]
    else:
        return [0, 0, 0, 0, 0]


class NewlineTextSplitter(TextSplitter):
    def split_text(self, text: str) -> List[str]:
        return text.split(",")


class FakeEmbedding(BaseEmbedding):
    def _get_text_embedding(self, text: str) -> List[float]:
        return mock_get_text_embedding(text)

    def _get_query_embedding(self, query: str) -> List[float]:
        return mock_get_text_embedding(query)

    async def _aget_query_embedding(self, query: str) -> List[float]:
        return mock_get_text_embedding(query)


def pathway_server(port):
    data_sources = get_data_sources()

    embed_model = FakeEmbedding()

    custom_transformations = [
        NewlineTextSplitter(),
        embed_model,
    ]

    processing_pipeline = VectorStoreServer.from_llamaindex_components(
        *data_sources,
        transformations=custom_transformations,  # type: ignore
    )

    thread = processing_pipeline.run_server(
        host=PATHWAY_HOST,
        port=port,
        threaded=True,
        with_cache=False,
    )
    thread.join()


@pytest.mark.flaky(reruns=4, reruns_delay=8)
@xfail_on_multiple_threads
def test_llama_retriever():
    port = PATHWAY_PORT
    p = Process(target=pathway_server, args=[port])
    p.start()

    time.sleep(15)

    from llama_index.retrievers.pathway import PathwayRetriever

    retriever = PathwayRetriever(host=PATHWAY_HOST, port=port)

    MAX_ATTEMPTS = 8
    attempts = 0
    results = []
    while attempts < MAX_ATTEMPTS:
        try:
            results = retriever.retrieve(str_or_query_bundle="Hello world.")
        except requests.exceptions.RequestException:
            pass
        else:
            break
        time.sleep(1)
        attempts += 1

    assert len(results) == 1
    assert results[0].text == "Hello world."
    assert results[0].score == 1.0

    p.terminate()


@pytest.mark.flaky(reruns=4, reruns_delay=8)
@xfail_on_multiple_threads
def test_llama_reader():
    port = PATHWAY_PORT + 1
    p = Process(target=pathway_server, args=[port])
    p.start()

    time.sleep(20)

    from llama_index.readers.pathway import PathwayReader

    pr = PathwayReader(host=PATHWAY_HOST, port=port)

    MAX_ATTEMPTS = 8
    attempts = 0
    results = []
    while attempts < MAX_ATTEMPTS:
        try:
            results = pr.load_data("Hello world.", k=1)
        except requests.exceptions.RequestException:
            pass
        else:
            break
        time.sleep(1)
        attempts += 1

    assert len(results) == 1

    first_result = results[0]
    assert first_result.text == "Hello world."
    assert EXAMPLE_TEXT_FILE in first_result.metadata["path"]

    attempts = 0
    results = []
    while attempts < MAX_ATTEMPTS:
        try:
            results = pr.load_data("This is a test.", k=1)
        except requests.exceptions.RequestException:
            pass
        else:
            break
        time.sleep(1)
        attempts += 1

    time.sleep(1)
    first_result = results[0]
    assert first_result.text == "This is a test."
    assert EXAMPLE_TEXT_FILE in first_result.metadata["path"]

    p.terminate()

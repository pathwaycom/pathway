import os

from dotenv import load_dotenv

import pathway as pw
from pathway.stdlib.indexing.nearest_neighbors import BruteForceKnnFactory
from pathway.xpacks.llm import llms
from pathway.xpacks.llm.document_store import DocumentStore
from pathway.xpacks.llm.embedders import OpenAIEmbedder
from pathway.xpacks.llm.parsers import UnstructuredParser
from pathway.xpacks.llm.splitters import TokenCountSplitter

load_dotenv()


class DocumentSchema(pw.Schema):
    data: list[tuple[str, pw.Json]]


class QuerySchema(pw.Schema):
    messages: str


# Alternative class: DocumentStore.RetrieveQuerySchema
class RetrieveQuerySchema(pw.Schema):
    query: str
    k: int
    metadata_filter: str | None
    filepath_globpattern: str | None


# Alternative class: DocumentStore.QueryResultSchema
class QueryResultSchema(pw.Schema):
    result: pw.Json


class RetrievedDocsSchema(pw.Schema):
    docs: pw.Json


class PromptsSchema(pw.Schema):
    prompts: str


class ResultsSchema(pw.Schema):
    results: str


def get_documents() -> DocumentSchema:
    documents = pw.io.fs.read("./data/", format="binary", with_metadata=True)
    return documents


def get_documentstore(documents: DocumentSchema) -> DocumentStore:
    text_splitter = TokenCountSplitter(
        min_tokens=100, max_tokens=500, encoding_name="cl100k_base"
    )
    embedder = OpenAIEmbedder(api_key=os.environ["OPENAI_API_KEY"])
    retriever_factory = BruteForceKnnFactory(
        embedder=embedder,
    )
    parser = UnstructuredParser(
        chunking_mode="by_title",
        chunking_kwargs={
            "max_characters": 3000,
            "new_after_n_chars": 2000,
        },
    )
    store = DocumentStore(
        docs=documents,
        retriever_factory=retriever_factory,
        parser=parser,
        splitter=text_splitter,
    )
    return store


def prepare_queries(raw_queries: QuerySchema) -> RetrieveQuerySchema:
    queries = raw_queries.select(
        query=pw.this.messages,
        k=1,
        metadata_filter=None,
        filepath_globpattern=None,
    )
    return queries


def build_prompts(
    queries: RetrieveQuerySchema, documents: RetrievedDocsSchema
) -> PromptsSchema:
    queries_context = queries + documents

    def get_context(documents):
        content_list = []
        for doc in documents:
            content_list.append(str(doc["text"]))
        return " ".join(content_list)

    @pw.udf
    def build_prompts_udf(documents, query) -> str:
        context = get_context(documents)
        prompt = (
            f"Given the following documents : \n {context} \nanswer this query: {query}"
        )
        return prompt

    prompts = queries_context + queries_context.select(
        prompts=build_prompts_udf(pw.this.docs, pw.this.query)
    )
    return prompts


def get_responses(prompts: PromptsSchema) -> ResultsSchema:
    model = llms.OpenAIChat(
        model="gpt-4o-mini",
        api_key=os.environ[
            "OPENAI_API_KEY"
        ],  # Read OpenAI API key from environmental variables
    )

    response = prompts.select(
        *pw.this.without(pw.this.query, pw.this.prompts, pw.this.docs),
        result=model(
            llms.prompt_chat_single_qa(pw.this.prompts),
        ),
    )
    return response


def get_webserver():
    webserver = pw.io.http.PathwayWebserver(host="0.0.0.0", port=8011)
    queries, writer = pw.io.http.rest_connector(
        webserver=webserver,
        schema=QuerySchema,
        autocommit_duration_ms=50,
        delete_completed_queries=False,
    )
    return queries, writer


def pipeline():
    documents: DocumentSchema = get_documents()
    document_store: DocumentStore = get_documentstore(documents)

    queries, writer = get_webserver()
    queries: RetrieveQuerySchema = prepare_queries(queries)

    retrieved_documents: QueryResultSchema = document_store.retrieve_query(queries)
    retrieved_documents: RetrievedDocsSchema = retrieved_documents.select(
        docs=pw.this.result
    )

    prompts = build_prompts(queries, retrieved_documents)

    responses = prompts.select(result=pw.this.prompts)
    responses = get_responses(prompts)

    writer(responses)

    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


pipeline()

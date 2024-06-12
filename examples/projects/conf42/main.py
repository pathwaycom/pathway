from dotenv import load_dotenv

import pathway as pw
from pathway.xpacks.llm import embedders, llms, prompts
from pathway.xpacks.llm.parsers import ParseUnstructured
from pathway.xpacks.llm.splitters import TokenCountSplitter
from pathway.xpacks.llm.vector_store import VectorStoreServer

# To use advanced features with Pathway Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Community, comment out the line below.
pw.set_license_key("demo-license-key-with-telemetry")

load_dotenv()


class PWAIQuerySchema(pw.Schema):
    query: str
    user: str


def _unwrap_udf(func):
    if isinstance(func, pw.UDF):
        return func.__wrapped__
    return func


@pw.udf
def prep_rag_prompt(prompt: str, docs: list[pw.Json]) -> str:
    docs = docs.value
    docs = [{"text": doc["text"], "path": doc["metadata"]["path"]} for doc in docs]
    prompt_func = _unwrap_udf(prompts.prompt_short_qa)
    return prompt_func(prompt, docs)


# Read the documents
documents = pw.io.fs.read("./documents/", format="binary", with_metadata=True)

# Define the model
embedder = embedders.OpenAIEmbedder(model="text-embedding-ada-002")
chat = llms.OpenAIChat(model="gpt-3.5-turbo", temperature=0.05)
text_splitter = TokenCountSplitter(max_tokens=400)

# Initialize the vector store
vector_server = VectorStoreServer(
    documents,
    embedder=embedder,
    splitter=text_splitter,
    parser=ParseUnstructured(),
)

# Connect the webserver for the queries
webserver = pw.io.http.PathwayWebserver(host="0.0.0.0", port=8000)
queries, writer = pw.io.http.rest_connector(
    webserver=webserver,
    schema=PWAIQuerySchema,
    autocommit_duration_ms=50,
    delete_completed_queries=True,
)

# Query the Vector Index
results = queries + vector_server.retrieve_query(
    queries.select(
        query=pw.this.query,
        k=1,
        metadata_filter=pw.cast(str | None, None),
        filepath_globpattern=pw.cast(str | None, None),
    )
).select(
    docs=pw.this.result,
)

# Generate the prompt
results += results.select(rag_prompt=prep_rag_prompt(pw.this.query, pw.this.docs))
# Query the LLM with the prompt
results += results.select(
    result=chat(
        llms.prompt_chat_single_qa(pw.this.rag_prompt),
    )
)
# Send back the answer
writer(results)

# Run the pipeline
pw.run()

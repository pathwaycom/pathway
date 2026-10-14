"""
Microservice for a context-aware alerting ChatGPT assistant.

This demo is very similar to the `alert` example, the only difference is the data source (Google Drive)
For the demo, alerts are sent to Slack (you need to provide `slack_alert_channel_id` and `slack_alert_token`),
you can either put these env variables in .env file under llm-app directory,
or create env variables in the terminal (i.e. export in bash).

The program then starts a REST API endpoint serving queries about Google Docs stored in a
Google Drive folder.

We can create notifications by asking from Streamlit or sending query to API stating we want to be notified.
One example would be `Tell me and alert about the start date of the campaign for Magic Cola`

How Does It Work?
First, Pathway Live Data Framework connects to Google Drive, extracts all documents, splits them into chunks,
turns them into vectors using OpenAI embedding service, and stores them in a nearest neighbor index.

Each query text is first turned into a vector, then relevant document chunks are found
using the nearest neighbor index. A prompt is built from the relevant chunk
and sent to the OpenAI GPT3.5 chat service for processing and answering.

After an initial answer is provided, Pathway Live Data Framework monitors changes to documents and selectively
re-triggers potentially affected queries. If the new answer is significantly different from
the previously presented one, a new notification is created.

Please check the README.md in this directory for how-to-run instructions.
"""

import asyncio
import os

import dotenv

import pathway as pw
from pathway.stdlib.ml.index import KNNIndex
from pathway.xpacks.llm.embedders import OpenAIEmbedder
from pathway.xpacks.llm.llms import OpenAIChat, prompt_chat_single_qa
from pathway.xpacks.llm.parsers import UnstructuredParser
from pathway.xpacks.llm.splitters import TokenCountSplitter

# To use advanced features with Pathway Live Data Framework Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Live Data Framework Community, comment out the line below.
pw.set_license_key("demo-license-key-with-telemetry")

dotenv.load_dotenv()


class DocumentInputSchema(pw.Schema):
    doc: str


class QueryInputSchema(pw.Schema):
    query: str
    user: str


# Helper Functions
@pw.udf
def build_prompt(documents, query):
    docs_str = "\n".join(
        [f"Doc-({idx}) -> {doc}" for idx, doc in enumerate(documents[::-1])]
    )
    prompt = f"""Given a set of documents, answer user query. If answer is not in docs, say it cant be inferred.

Docs: {docs_str}
Query: '{query}'
Final Response:"""
    return prompt


@pw.udf
def build_prompt_check_for_alert_request_and_extract_query(query: str) -> str:
    prompt = f"""Evaluate the user's query and identify if there is a request for notifications on answer alterations:
    User Query: '{query}'

    Respond with 'Yes' if there is a request for alerts, and 'No' if not,
    followed by the query without the alerting request part.

    Examples:
    "Tell me about windows in Pathway Live Data Framework"
    => "No. Tell me about windows in Pathway Live Data Framework"
    "Tell me and alert about windows in Pathway Live Data Framework"
    => "Yes. Tell me about windows in Pathway Live Data Framework"
    """
    return prompt


@pw.udf
def split_answer(answer: str) -> tuple[bool, str]:
    alert_enabled = "yes" in answer[:3].lower()
    true_query = answer[3:].strip(' ."')
    return alert_enabled, true_query


def build_prompt_compare_answers(new: str, old: str) -> str:
    prompt = f"""
    Are the two following responses deviating?
    Answer with Yes or No.

    First response: "{old}"

    Second response: "{new}"
    """
    return prompt


def make_query_id(user, query) -> str:
    return str(hash(query + user))


@pw.udf
def construct_notification_message(query: str, response: str) -> str:
    return f'New response for question "{query}":\n{response}'


@pw.udf
def construct_message(response, alert_flag, metainfo=None):
    if alert_flag:
        if metainfo:
            response += "\n" + str(metainfo)
        return response + "\n\n🔔 Activated"
    return response


def decision_to_bool(decision: str) -> bool:
    return "yes" in decision.lower()


def run(
    *,
    object_id=os.environ.get("FILE_OR_DIRECTORY_ID", ""),
    api_key: str = os.environ.get("OPENAI_API_KEY", ""),
    host: str = os.environ.get("PATHWAY_REST_CONNECTOR_HOST", "0.0.0.0"),
    port: int = int(os.environ.get("PATHWAY_REST_CONNECTOR_PORT", "8080")),
    embedder_locator: str = "text-embedding-ada-002",
    embedding_dimension: int = 1536,
    model_locator: str = "gpt-3.5-turbo",
    max_tokens: int = 400,
    temperature: float = 0.0,
    slack_alert_channel_id=os.environ.get("SLACK_ALERT_CHANNEL_ID", ""),
    slack_alert_token=os.environ.get("SLACK_ALERT_TOKEN", ""),
    service_user_credentials_file=os.environ.get(
        "GOOGLE_CREDS", "templates/drive_alert/secrets.json"
    ),
    **kwargs,
):
    # Part I: Build index
    embedder = OpenAIEmbedder(
        api_key=api_key,
        model=embedder_locator,
        retry_strategy=pw.asynchronous.FixedDelayRetryStrategy(),
        cache_strategy=pw.asynchronous.DefaultCache(),
    )

    # We start building the computational graph. Each pathway variable represents a
    # dynamically changing table.

    # The files table contains contents of documents in Google Drive.
    # Pathway Live Data Framework automatically tracks changes to files and propagates these changes through
    # following computations.
    # Other Pathway Live Data Framework connectors can be used as well - notably:
    # - pw.io.fs.read to load and track changes to the local drive and
    # - pw.io.s3.read to use an S3-compatible storage
    files = pw.io.gdrive.read(
        object_id=object_id,
        service_user_credentials_file=service_user_credentials_file,
        refresh_interval=30,  # interval between fetch operations in seconds, lower this for more responsiveness
    )
    parser = UnstructuredParser()
    documents = files.select(texts=parser(pw.this.data))
    documents = documents.flatten(pw.this.texts)
    documents = documents.select(texts=pw.this.texts[0])

    splitter = TokenCountSplitter()
    documents = documents.select(
        chunks=splitter(pw.this.texts, min_tokens=40, max_tokens=120)
    )
    documents = documents.flatten(pw.this.chunks)
    documents = documents.select(chunk=pw.this.chunks[0])

    enriched_documents = documents + documents.select(data=embedder(pw.this.chunk))

    # The index is updated each time a file changes.
    index = KNNIndex(
        enriched_documents.data, enriched_documents, n_dimensions=embedding_dimension
    )

    # Part II: receive queries, detect intent and prepare cleaned query

    # The rest_connector returns a table of all queries under processing
    query, response_writer = pw.io.http.rest_connector(
        host=host,
        port=port,
        schema=QueryInputSchema,
        autocommit_duration_ms=50,
        delete_completed_queries=False,
    )

    model = OpenAIChat(
        api_key=api_key,
        model=model_locator,
        temperature=temperature,
        max_tokens=max_tokens,
        retry_strategy=pw.asynchronous.FixedDelayRetryStrategy(),
        cache_strategy=pw.asynchronous.DefaultCache(),
    )

    # Pre-process the queries:
    # - detect alerting intent
    # - then embed the query for nearest neighbor retrieval
    query += query.select(
        prompt=build_prompt_check_for_alert_request_and_extract_query(query.query)
    )
    query += query.select(
        tupled=split_answer(
            model(
                prompt_chat_single_qa(pw.this.prompt),
                max_tokens=100,
            )
        ),
    )
    query = query.select(
        pw.this.user,
        alert_enabled=pw.this.tupled[0],
        query=pw.this.tupled[1],
    )

    query += query.select(
        data=embedder(pw.this.query),
        query_id=pw.apply(make_query_id, pw.this.user, pw.this.query),
    )

    # Part III: respond to queries

    # The context is a dynamic table: Pathway Live Data Framework updates it each time:
    # - a new query arrives
    # - a source document is changed significantly enough to change the set of
    #   nearest neighbors
    query_context = query + index.get_nearest_items(query.data, k=3).select(
        documents_list=pw.this.chunk
    ).with_universe_of(query)

    # then we answer the queries using retrieved documents
    prompt = query_context.select(
        pw.this.query_id,
        pw.this.query,
        pw.this.alert_enabled,
        prompt=build_prompt(pw.this.documents_list, pw.this.query),
    )

    responses = prompt.select(
        pw.this.query_id,
        pw.this.query,
        pw.this.alert_enabled,
        response=model(
            prompt_chat_single_qa(pw.this.prompt),
        ),
    )

    output = responses.select(
        result=construct_message(pw.this.response, pw.this.alert_enabled)
    )

    # and send the answers back to the asking users
    response_writer(output)

    # Part IV: send alerts about responses which changed significantly.

    # However, for the queries with alerts the processing continues
    # whenever the set of documents retrieved for a query changes,
    # the table of responses is updated.
    responses = responses.filter(pw.this.alert_enabled)

    def acceptor(new: str, old: str) -> bool:
        if new == old:
            return False

        # TODO: clean after udfs can be used as common functions
        prompt = [dict(role="system", content=build_prompt_compare_answers(new, old))]
        decision = asyncio.run(model.__wrapped__(prompt, max_tokens=20))
        return decision_to_bool(decision)

    # Each update is compared with the previous one for deduplication
    deduplicated_responses = pw.stateful.deduplicate(
        responses,
        col=responses.response,
        acceptor=acceptor,
        instance=responses.query_id,
    )

    # Significant alerts are sent to the user
    alerts = deduplicated_responses.select(
        message=construct_notification_message(pw.this.query, pw.this.response)
    )
    pw.io.slack.send_alerts(alerts.message, slack_alert_channel_id, slack_alert_token)

    # Finally, we execute the computation graph
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


if __name__ == "__main__":
    run()

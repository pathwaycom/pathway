<p align="center" class="flex items-center gap-1 justify-center flex-wrap">
    <img src="../../assets/gcp-logo.svg?raw=true" alt="GCP Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/gcp-deploy">Deploy with GCP</a> |
    <img src="../../assets/aws-fargate-logo.svg?raw=true" alt="AWS Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/aws-fargate-deploy">Deploy with AWS</a> |
    <img src="../../assets/azure-logo.svg?raw=true" alt="Azure Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/azure-aci-deploy">Deploy with Azure</a> |
    <img src="../../assets/render.png?raw=true" alt="Render Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/render-deploy"> Deploy with Render </a>
</p>

# Pathway Live Data Framework RAG app with always up-to-date knowledge

This demo shows how to create a real-time RAG application using [Pathway Live Data Framework](https://github.com/pathwaycom/pathway) that provides always up-to-date knowledge to your LLM without the need for a separate ETL.

You will see a running example of how to get started with the Pathway Live Data Framework vector store that eliminates the need for ETL pipelines which are needed in the regular VectorDBs. 
This significantly reduces the developer's workload.

This demo allows you to:

- Create a Document store with real-time document indexing from Google Drive, Microsoft 365 SharePoint, or a local directory;
- Connect an OpenAI LLM model of choice to your knowledge base;
- Get quality, accurate, and precise responses to your questions;
- Ask questions about folders, files or all your documents easily, with the help of filtering options;
- Use LLMs over API to summarize texts;
- Get an executive outlook for a question on different files to easily access available knowledge in your documents;


Note: This app relies on [Document Store](https://pathway.com/developers/api-docs/pathway-xpacks-llm/document_store) to learn more, you can check out [this blog post](https://pathway.com/developers/user-guide/llm-xpack/docs-indexing).

## Table of contents
- [Summary of available endpoints](#Summary-of-available-endpoints)
- [How it works](#How-it-works)
- [Customizing the pipeline](#Customizing-the-pipeline)
- [How to run the project](#How-to-run-the-project)
- [Using the app](#Query-the-documents)


## Summary of available endpoints

This example spawns a lightweight webserver using Pathway Live Data Framework’s [`QASummaryRestServer`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/servers#pathway.xpacks.llm.servers.QASummaryRestServer) that accepts queries on five possible endpoints, divided into two categories: document indexing and RAG with LLM.

### Document Indexing capabilities
- `/v1/retrieve` to perform similarity search;
- `/v1/statistics` to get the basic stats about the indexer's health;
- `/v2/list_documents` to retrieve the metadata of all files currently processed by the indexer.

### LLM and RAG capabilities
- `/v2/answer` to ask questions about your documents, or directly talk with your LLM;
- `/v2/summarize` to summarize a list of texts;

See the [using the app section](###Using-the-app) to learn how to use the provided endpoints.

## How it works

1. **Data Ingestion**  
We define one or more sources in `app.yaml` (local directories, Google Drive, Microsoft SharePoint, etc.).  
- The provided demo references a local folder `data/` by default.  
- The code can poll these sources at configured intervals, so when new documents appear or existing ones change, they are automatically parsed and re-indexed in real-time.

2. **Parsing & Splitting**  
Using [Docling](https://www.docling.ai/) (through Pathway Live Data Framework’s [`DoclingParser`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.DoclingParser)) and [TokenCountSplitter](https://pathway.com/developers/api-docs/pathway-xpacks-llm/splitters#pathway.xpacks.llm.splitters.TokenCountSplitter), documents are parsed and chunked into smaller parts.

3. **Embedding**  
Via [`OpenAIEmbedder`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/embedders#pathway.xpacks.llm.embedders.OpenAIEmbedder), the chunks get turned into embeddings. You can substitute your own embedder if you wish.

4. **Indexing**  
Using [`USearchKnnFactory`](https://pathway.com/developers/api-docs/indexing#pathway.stdlib.indexing.UsearchKnnFactory), the embeddings are stored in a vector index. This is all streaming as well, so new embeddings are added or updated automatically.

5. **Serving**  
- We create a [`SummaryQuestionAnswerer`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/question_answering#pathway.xpacks.llm.question_answering.SummaryQuestionAnswerer) (as specified in `app.py`), which can handle both question-answering and summarization requests.  
- A web server, [`QASummaryRestServer`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/servers#pathway.xpacks.llm.servers.QASummaryRestServer), exposes multiple endpoints for retrieval, Q&A, summarization, and more.

Because Pathway Live Data Framework is fully incremental, any changes to your source files immediately flow through parsing, embedding, indexing, and ultimately get reflected in the answers from the LLM. The user can then query the created index with simple HTTP requests to the endpoints mentioned above.

## Pipeline Organization

This folder contains several objects:
- `app.py`, the application code using Pathway Live Data Framework and written in Python;
- `app.yaml`, the file containing configuration of the pipeline, like LLM models, sources or server address;
- `requirements.txt`, the dependencies for the pipeline. It can be passed to `pip install -r requirements.txt` to install everything that is needed to launch the pipeline locally;
- `Dockerfile`, the Docker configuration for running the pipeline in the container;
- `.env`, a short environment variables configuration file where the OpenAI key must be stored;
- `data/`, a folder with exemplary files that can be used for the test runs.
- `ui/`, a simple ui written in Streamlit for asking questions.

## Pathway Live Data Framework tooling
### Prompts and helpers

Pathway Live Data Framework allows you to define custom prompts in addition to the ones provided in [`pathway.xpacks.llm`](https://pathway.com/developers/user-guide/llm-xpack/overview).

You can also use user-defined functions using the [`@pw.udf`](https://pathway.com/developers/api-docs/pathway#pathway.udf) decorator to define custom functions that will run on streaming data.

### RAG

Pathway Live Data Framework provides all the tools to create a RAG application and query it: a [Pathway Live Data Framework Document store](https://pathway.com/developers/api-docs/pathway-xpacks-llm/document_store#pathway.xpacks.llm.document_store.DocumentStore) and a web server (defined with the [REST connector](https://pathway.com/developers/api-docs/pathway-io/http#pathway.io.http.rest_connector)).
For the sake of the demo, we kept the app simple, consisting of the main components you would find in a regular RAG application. It can be further enhanced with query writing methods, re-ranking layer and custom splitting steps.

Don't hesitate to take a look at our [documentation](https://pathway.com/developers/user-guide/introduction/welcome) to learn how Pathway Live Data Framework works.


## OpenAI API Key Configuration

Default LLM provider in this template is OpenAI, so, unless you change the configuration, you need to provide OpenAI API key. Please configure your key in a `.env` file by providing it as follows: `OPENAI_API_KEY=sk-*******`. You can refer to the stub file `.env` in this repository, where you will need to paste your key instead of `sk-*******`.

## Customizing the pipeline

The code can be modified by changing the `app.yaml` configuration file. To read more about YAML files used in Pathway Live Data Framework templates, read [our guide](https://pathway.com/developers/templates/configure-yaml).

In the `app.yaml` file we define:
- input connectors
- LLM
- embedder
- index
and any of these can be replaced or, if no longer needed, removed. For components that can be used check 
Pathway Live Data Framework [LLM xpack](https://pathway.com/developers/user-guide/llm-xpack/overview), or you can implement your own.
 
You can also check our other templates - [Question Answering RAG](https://github.com/pathwaycom/pathway/tree/main/examples/llm-app/templates/question_answering_rag),
[Multimodal RAG](https://github.com/pathwaycom/pathway/tree/main/examples/llm-app/templates/multimodal_rag) or
[Private RAG](https://github.com/pathwaycom/pathway/tree/main/examples/llm-app/templates/private_rag). As all of these only differ
in the YAML configuration file, you can also use them as an inspiration for your custom pipeline.

Here some examples of what can be modified.

### LLM Model

You can choose any of the models offered by Open AI, like GPT-5, GPT-4.1, or GPT-4o.
You can find the whole list on their [models page](https://platform.openai.com/docs/models).

You simply need to change the `model` to the one you want to use, e.g., to use GPT-5:
```yaml
$llm: !pw.xpacks.llm.llms.OpenAIChat
  model: "gpt-5"
  retry_strategy: !pw.udfs.ExponentialBackoffRetryStrategy
    max_retries: 6
  cache_strategy: !pw.udfs.DefaultCache
  capacity: 8
```

The default model is `gpt-4.1-mini`.

You can also use different provider, by using different class from [Pathway Live Data Framework LLM xpack](https://pathway.com/developers/user-guide/llm-xpack/overview),
e.g. here is configuration for locally run Mistral model.

```yaml
$llm: !pw.xpacks.llm.llms.LiteLLMChat
  model: "ollama/mistral"
  retry_strategy: !pw.udfs.ExponentialBackoffRetryStrategy
    max_retries: 6
  cache_strategy: !pw.udfs.DiskCache
  temperature: 0
  top_p: 1
  api_base: "http://localhost:11434"
```

### Index of your choice

The [`DocumentStore`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/document_store#pathway.xpacks.llm.document_store.DocumentStore) makes building and customizing a document indexing pipeline straightforward. It processes documents and enables querying the closest documents to a given query based on your chosen indexing strategy. Here's how you can use a hybrid indexing approach, combining vector-based and text-based retrieval: 

Example: Hybrid Indexing with `USearchKNN` and `TantivyBM25`

The following example demonstrates how to configure and use the [HybridIndex](https://pathway.com/developers/api-docs/indexing#pathway.stdlib.indexing.HybridIndex) that combines:

- **`USearchKNN`**: A vector-based index leveraging embeddings for semantic similarity search.
- **[`TantivyBM25`](https://pathway.com/developers/api-docs/indexing#pathway.stdlib.indexing.TantivyBM25)**: A text-based index using BM25 for keyword matching.


```yaml
$knn_index: !pw.indexing.USearchKnnFactory
  reserved_space: 1000
  embedder: $embedder
  metric: !pw.indexing.USearchKnnMetricKind.COS
  dimensions: 1536

$bm25_index: !pw.indexing.TantivyBM25Factory

$hybrid_index_factory: !pw.indexing.HybridIndexFactory
  retriever_factories:
    - $knn_index
    - $bm25_index

$document_store: !pw.xpacks.llm.document_store.DocumentStore
  docs: $sources
  parser: $parser
  splitter: $splitter
  retriever_factory: $hybrid_index_factory
```
Choose the indexing strategy that fits your requirements with `DocumentStore`

### Webserver

You can configure the host and the port of the webserver.
Here is the default configuration:
```yaml
host: "0.0.0.0"
port: 8000
```

### Cache

You can configure whether you want to enable cache or persistence, to avoid repeated API accesses, and where the cache is stored.
Default values:
```yaml
persistence_mode: !pw.PersistenceMode.UDF_CACHING
persistence_backend: !pw.persistence.Backend.filesystem
  path: ".Cache"
```

### Data sources

You can configure the data sources by changing `$sources` in `app.yaml`.
You can add as many data sources as you want. You can have several sources of the same kind, for instance, several local sources from different folders.
The sections below describe how to configure local, Google Drive and Sharepoint source, and you can check the examples of YAML configuration in our [user guide](https://pathway.com/developers/templates/yaml-snippets/data-sources-examples/). While these are not described in this Section, you can also use any input [connector](https://pathway.com/developers/user-guide/connecting-to-data/connectors) from Pathway Live Data Framework package.

By default, the app uses a local data source to read documents from the `data` folder.

#### Local Data Source

The local data source is configured by using map with tag `!pw.io.fs.read`. Then set `path` to denote the path to a folder with files to be indexed.

#### Google Drive Data Source

The Google Drive data source is enabled by using map with tag `!pw.io.gdrive.read`. The map must contain two main parameters:
- `object_id`, containing the ID of the folder that needs to be indexed. It can be found from the URL in the web interface, where it's the last part of the address. For example, the publicly available demo folder in Google Drive has the URL `https://drive.google.com/drive/folders/1cULDv2OaViJBmOfG5WB0oWcgayNrGtVs`. The last part of this address is `1cULDv2OaViJBmOfG5WB0oWcgayNrGtVs` and this is the `object_id` you would need to specify.
- `service_user_credentials_file`, containing the path to the credentials files for the Google [service account](https://cloud.google.com/iam/docs/service-account-overview). To get more details on setting up the service account and getting credentials, you can also refer to [this tutorial](https://pathway.com/developers/user-guide/connectors/gdrive-connector#setting-up-google-drive).

Besides, to speed up the indexing process you may want to specify the `refresh_interval` parameter, denoted by an integer number of seconds. It corresponds to the frequency between two sequential folder scans. If unset, it defaults to 30 seconds.

For the full list of the available parameters, please refer to the Google Drive connector [documentation](https://pathway.com/developers/api-docs/pathway-io/gdrive#pathway.io.gdrive.read).

#### SharePoint Data Source

This data source requires Scale or Enterprise [license key](https://pathway.com/pricing) - you can obtain free Scale key on [Pathway website](https://pathway.com/get-license).

To use it, set the map tag to be `!pw.xpacks.connectors.sharepoint.read`, and then provide values of `url`, `tenant`, `client_id`, `cert_path`, `thumbprint` and `root_path`. To read about the meaning of these arguments, check the Sharepoint connector [documentation](https://pathway.com/developers/api-docs/pathway-xpacks-sharepoint#pathway.xpacks.connectors.sharepoint.read).

## How to run the project

Clone the Pathway Live Data Framework repository from GitHub to access the LLM app templates. This repository contains all the files you’ll need.

```bash
git clone https://github.com/pathwaycom/pathway.git
```

### Locally
If you are on Windows, please refer to [running with docker](#With-Docker) section below.

To run locally, change your directory in the terminal to this folder. Then, run the app with `python`.

```bash
cd pathway/examples/llm-app/templates/question_answering_rag

python app.py
```

Please note that the local run requires the dependencies to be installed. It can be done with a simple pip command:
`pip install -r requirements.txt`

### With Docker

Build the Docker with:

```bash
docker compose build
```

And, run with:

```bash
docker compose up
```

This will start the pipeline and the ui for asking questions.

### Query the documents
You will see the logs for parsing & embedding documents in the Docker image logs. 
Give it a few minutes to finish up on embeddings, you will see `0 entries (x minibatch(es)) have been...` message.
If there are no more updates, this means the app is ready for use!

To test it, let's query the stats:
```bash
curl -X 'POST'   'http://localhost:8000/v1/statistics'   -H 'accept: */*'   -H 'Content-Type: application/json'
```

For more information on available endpoints by default, see [above](#Summary-of-available-endpoints).

We provide some example `curl` queries to start with.

The general structure is sending a request to `http://{HOST}:{PORT}/{ENDPOINT}`.

Where HOST is the `host` variable you specify in your app configuration. PORT is the `port` number you are running your app on, and ENDPOINT is the specific extension for endpoints. They are specified in the application code, and they are listed with the versioning as `/v1/...`.

Note that, if you are using the Pathway Live Data Framework hosted version, you should send requests to `https://...` rather than `http://...` and emit the `:{PORT}` part of the URL.

You need to add two headers, `-H 'accept: */*'   -H 'Content-Type: application/json'`.

Finally, for endpoints that expect data in the query, you can pass it with `-d '{key: value}'` format.

#### Listing inputs
Get the list of available inputs and associated metadata.

```bash
curl -X 'POST'   'http://localhost:8000/v2/list_documents'   -H 'accept: */*'   -H 'Content-Type: application/json'
```

#### Searching in your documents

Search API gives you the ability to search in available inputs and get up-to-date knowledge.
`query` is the search query you want to execute.

`k` (optional) is an integer, the number of documents to be retrieved. Documents in this case means small chunks that are stored in your vector store.

`metadata_filter` (optional) String to filter results with Jmespath query.

`filepath_globpattern` (optional) String to filter results with globbing pattern. For example `"*"` would result in no filter, `"*.docx"` would result in only `docx` files being retrieved.


```bash
curl -X 'POST' \
  'http://0.0.0.0:8000/v1/retrieve' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "query": "Which articles of General Data Protection Regulation are relevant for clinical trials?",
  "k": 6
}'
```

#### Asking questions to LLM (With and without RAG)

- Note: The local version of this app does not require `openai_api_key` parameter in the payload of the query. Embedder and LLM will use the API key in the `.env` file. However, Pathway Live Data Framework hosted public demo available on the [website](https://pathway.com/solutions/ai-pipelines) requires a valid `openai_api_key` to execute the query.

- Note: All of the RAG endpoints use the `model` provided in the config by default, however, you can specify another model with the `model` parameter in the payload to use a different one for generating the response.

For question answering without any context, simply omit `filters` key in the payload and send the following request.

```bash
curl -X 'POST' \
  'http://0.0.0.0:8000/v2/answer' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "prompt": "What is the start date of the contract?"
}'
```

Question answering with the knowledge from a specific file, based on the path.
```bash
curl -X 'POST' \
  'http://0.0.0.0:8000/v2/answer' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "prompt": "What is the start date of the contract?",
  "filters": "globmatch(`"IdeanomicsInc_20160330_10-K_EX-10.26_9512211_EX-10.26_Content License Agreement.pdf"`, path)"
}'
```

Alternatively, with the knowledge from files that have the word `Ide` in their paths.
```bash
curl -X 'POST' \
  'http://0.0.0.0:8000/v2/answer' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "prompt": "What is the start date of the contract?",
  "filters": "contains(path, `"Ide"`)"
}'
```

You can also retrieve the context documents that were used to answer the question,
```bash
curl -X 'POST' \
  'http://0.0.0.0:8000/v2/answer' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "prompt": "What is the start date of the contract?",
  "return_context_docs": true
}'
```

- Note: You can limit the knowledge to a folder or, to only Word documents by using ```"contains(path, `docx`)"```
- Note: You could also use a few filters separated with `||` (`or` clause) or with `&&` (`and` clause).

You can further modify behavior in the payload by defining keys and values in `-d '{key: value}'`.

If you wish to use another model, specify in the payload as `"model": "gpt-4"`.

For more detailed responses add `"response_type": "long"` to payload.

#### Summarization
To summarize a list of texts, use the following `curl` command.

```bash
curl -X 'POST' \
  'http://0.0.0.0:8000/v2/summarize' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "text_list": [
    "I love apples.",
    "I love oranges."
  ]
}'
```

Specifying the GPT model with `"model": "gpt-4"` is also possible.

This endpoint also supports setting different models in the query by default.

To execute similar curl queries as above, you can visit [ai-pipelines page](https://pathway.com/solutions/ai-pipelines/) and try out the queries from the Swagger UI.


#### Adding Files to Index

First, you can try adding your files and seeing changes in the index. To test index updates, simply add more files to the `data` folder.

If you are using Google Drive or other sources, simply upload your files there.

### Using the UI
This pipeline includes a simple ui written in Streamlit. After you run the pipeline with `docker compose up`, you can access the UI at `http://localhost:8501`. This UI uses the `/v2/answer` endpoint to answer your questions.

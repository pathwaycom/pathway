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

# Fully private RAG with Pathway Live Data Framework

## **Overview**

Retrieval-Augmented Generation (RAG) is a powerful method for answering questions using a private knowledge database. Ensuring data security is essential, especially for sensitive information like trade secrets, confidential IP, GDPR-protected data, and internal documents. This showcase demonstrates setting up a private RAG pipeline with adaptive retrieval using Pathway Live Data Framework, Mistral, and Ollama. The provided code deploys this adaptive RAG technique with Pathway Live Data Framework, ensuring no API access or data leaves the local machine.

The app utilizes modules under `pathway.xpacks.llm`. The `BaseRAGQuestionAnswerer` class is the foundation for building RAG applications with the Pathway Live Data Framework vector store and xpack components, enabling a quick start with RAG applications.

This example uses `AdaptiveRAGQuestionAnswerer`, an extension of `BaseRAGQuestionAnswerer` with adaptive retrieval. For more on building and deploying RAG applications with Pathway Live Data Framework, including containerization, refer to the demo on question answering.

The application responds to requests at the `/v2/answer` endpoint. The `answer_query` function takes the `pw_ai_queries` table as input, containing prompts and other arguments from the post request. This table's data is used to call the adaptive retrieval logic.

The `AdaptiveRAGQuestionAnswerer` implementation under `pathway.xpacks.llm.question_answering` builds a RAG app with the Pathway Live Data Framework vector store and components. It supports two question answering strategies, short (concise) and long (detailed) responses, set during the post request. It allows LLM agnosticity, giving the freedom to choose between proprietary or open-source LLMs. It adapts the number of chunks used as a context, starting with `n_starting_documents` chunks and increasing until an answer is found.

To learn more about building & deploying RAG applications with Pathway Live Data Framework, including containerization, refer to [demo question answering](../question_answering_rag/README.md).


## Table of contents

This includes the technical details to the steps to create a REST Endpoint to run the application via Docker and modify it for your use-cases.

- [Overview](#Overview)
- [Architecture](#Architecture)
- [Deploying and using a local LLM](#Deploying-and-using-a-local-LLM)
- [Running the app](#Running-the-app)
- [Querying the app/pipeline](#Querying-the-app/pipeline)
- [Modifying the code](#Modifying-the-code)
- [Conclusion](#Conclusion)


## Architecture

![Architecture](https://i.imgur.com/9TJEoUd.png)

The architecture consists of two connected technology bricks, which will run as services on your machine:
- Pathway Live Data Framework brings support for real-time data synchronization pipelines out of the box, and the possibility of secure private document handling with enterprise connectors for synchronizing Sharepoint and Google Drive incrementally. The Pathway Live Data Framework service you'll run performs live document indexing pipeline, and will use Pathway Live Data Framework’s built-in vector store.
- The language model you use will be a Mistral 7B, which you will locally deploy as an Ollama service. This model was chosen for its performance and compact size.

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

This template is prepared to run by default locally. However, the pipeline is LLM model agnostic, so you can change them to use other locally deployed model, or even
use LLM model available through API calls. For discussion on models used in this template check [the dedicated Section](#deploying-and-using-a-local-LLM).

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
- `object_id`, containing the ID of the folder that needs to be indexed. It can be found from the URL in the web interface, where it's the last part of the address. For example, the publicly available demo folder in Google Drive has the URL `https://drive.google.com/drive/folders/1cULDv2OaViJBmOfG5WB0oWcgayNrGtVs`. Consequently, the last part of this address is `1cULDv2OaViJBmOfG5WB0oWcgayNrGtVs`, hence this is the `object_id` you would need to specify.
- `service_user_credentials_file`, containing the path to the credentials files for the Google [service account](https://cloud.google.com/iam/docs/service-account-overview). To get more details on setting up the service account and getting credentials, you can also refer to [this tutorial](https://pathway.com/developers/user-guide/connectors/gdrive-connector#setting-up-google-drive).

Besides, to speed up the indexing process you may want to specify the `refresh_interval` parameter, denoted by an integer number of seconds. It corresponds to the frequency between two sequential folder scans. If unset, it defaults to 30 seconds.

For the full list of the available parameters, please refer to the Google Drive connector [documentation](https://pathway.com/developers/api-docs/pathway-io/gdrive#pathway.io.gdrive.read).

#### SharePoint Data Source

This data source requires Scale or Enterprise [license key](https://pathway.com/pricing) - you can obtain free Scale key on [Pathway website](https://pathway.com/get-license).

To use it, set the map tag to be `!pw.xpacks.connectors.sharepoint.read`, and then provide values of `url`, `tenant`, `client_id`, `cert_path`, `thumbprint` and `root_path`. To read about the meaning of these arguments, check the Sharepoint connector [documentation](https://pathway.com/developers/api-docs/pathway-xpacks-sharepoint#pathway.xpacks.connectors.sharepoint.read).


## Deploying and using a local LLM

### Embedding Model Selection

You will use `pathway.xpacks.llm.embedders` module to load open-source embedding models from the HuggingFace model library. For this showcase, pick the `avsolatorio/GIST-small-Embedding-v0` model which has a dimension of 384 as it is compact and performed well in our tests. 

```yaml 
$embedding_model: "avsolatorio/GIST-small-Embedding-v0"

$embedder: !pw.xpacks.llm.embedders.SentenceTransformerEmbedder
  model: $embedding_model
  call_kwargs: 
    show_progress_bar: False
```

If you would like to use a higher-dimensional model, here are some possible alternatives you could use instead:

- mixedbread-ai/mxbai-embed-large-v1
- avsolatorio/GIST-Embedding-v0

For other possible choices, take a look at the [MTEB Leaderboard](https://huggingface.co/spaces/mteb/leaderboard) managed by HuggingFace.


### Local LLM Deployment

Due to its size and performance it is best to run the `Mistral 7B` LLM. Here you would deploy it as a service running on GPU, using `Ollama`.

To run local LLM, you can refer to these steps:
- Download Ollama from [ollama.com/download](https://ollama.com/download)
- In your terminal, run `ollama serve`
- In another terminal, run `ollama run mistral`

You can now test it with the following request:

```bash
curl -X POST http://localhost:11434/api/generate -d '{
  "model": "mistral",
  "prompt":"Here is a story about llamas eating grass"
 }'
```


### LLM Initialization

Now you will initialize the LLM instance that will call the local model.

```yaml
$llm_model: "ollama/mistral"

$llm: !pw.xpacks.llm.llms.LiteLLMChat
  model: $llm_model
  retry_strategy: !pw.udfs.ExponentialBackoffRetryStrategy
    max_retries: 6
  cache_strategy: !pw.udfs.DefaultCache {}
  temperature: 0
  api_base: "http://localhost:11434"  
  # api_base: "http://host.docker.internal:11434" # use this when you are running the app in the Docker on Mac or Windows
  async_mode: "fully_async"
```

## Running the app

First, make sure your local LLM is up and running. By default, the pipeline tries to access the LLM at `http://localhost:11434`. You can change that by setting `api_base` value in the app.yaml file.

### With Docker
In order to let the pipeline get updated with each change in local files, you need to mount the folder onto the docker. The following commands show how to do that.

#### Linux
```bash
# Build the image in this folder
docker build -t privaterag .

# Run the image, mount the `data` folder into image 
docker run --net host -v ./data:/app/data privaterag
```

#### Mac or Windows
In the `app.yaml` change `api_base` to be `http://host.docker.internal:11434`. Then run:

```bash
# Build the image in this folder
docker build -t privaterag .

# Run the image, mount the `data` folder into image 
docker run -v ./data:/app/data -p 8000:8000 privaterag
```

### Locally
To run locally you need to install the Pathway Live Data Framework app with LLM dependencies using:
```bash
pip install pathway[all]
```

Then change your directory in the terminal to this folder and run the app:
```bash
python app.py
```

## Querying the app/pipeline

Finally, query the application with;

```bash
curl -X 'POST'   'http://0.0.0.0:8000/v2/answer'   -H 'accept: */*'   -H 'Content-Type: application/json'   -d '{
  "prompt": "What is the start date of the contract?" 
}'
```
> `{"response": "December 21, 2015 [6]"}`


## Conclusion:

Now you have a fully private RAG set up with Pathway Live Data Framework and Ollama. All your data remains safe on your system. Moreover, the set-up is optimized for speed, thanks to how Ollama runs the LLM, and how Pathway Live Data Framework’s adaptive retrieval mechanism reduces token consumption while preserving the accuracy of the RAG.

This is a full production-ready set-up which includes reading your data sources, parsing the data, and serving the endpoint.
This private RAG setup can be run entirely locally with open-source LLMs, making it ideal for organizations with sensitive data and explainable AI needs.

## Quick Links:

- [Pathway Live Data Framework Developer Documentation](https://pathway.com/developers/user-guide/introduction/welcome)
- [Pathway Live Data Framework App Templates](https://pathway.com/developers/templates)
- [Discord Community of Pathway](https://discord.gg/pathway)
- [Pathway Live Data Framework Issue Tracker](https://github.com/pathwaycom/pathway/issues)
- [End-to-end dynamic RAG pipeline with Pathway Live Data Framework](https://github.com/pathwaycom/pathway/tree/main/examples/llm-app/templates/question_answering_rag)
- [Using Pathway Live Data Framework as a vector store with Langchain](https://python.langchain.com/v0.2/docs/integrations/vectorstores/pathway/) 
- [Using Pathway Live Data Framework as a retriever with LlamaIndex](https://docs.llamaindex.ai/en/stable/examples/retrievers/pathway_retriever/) 

Make sure to drop a “Star” to our repositories if you found this resource helpful!

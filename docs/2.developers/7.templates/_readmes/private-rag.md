<p align="center">
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/private-rag/../../../assets/gcp-logo.svg" alt="GCP Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/gcp-deploy">Deploy with GCP</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/private-rag/../../../assets/aws-fargate-logo.svg" alt="AWS Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/aws-fargate-deploy">Deploy with AWS</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/private-rag/../../../assets/azure-logo.svg" alt="Azure Logo" height="30">
    <a href="/developers/user-guide/deployment/azure-aci-deploy">Deploy with Azure</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/private-rag/../../../assets/render.png" alt="Render Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/render-deploy"> Deploy with Render </a>
</p>

# Fully private RAG with Pathway

## **Overview**

Retrieval-Augmented Generation (RAG) is a powerful method for answering questions using a private knowledge database. Ensuring data security is essential, especially for sensitive information like trade secrets, confidential IP, GDPR-protected data, and internal documents. This showcase demonstrates setting up a private RAG pipeline with adaptive retrieval using Pathway, Mistral, and Ollama. The provided code deploys this adaptive RAG technique with Pathway, ensuring no API access or data leaves the local machine.

The app utilizes modules under `pathway.xpacks.llm`. The `BaseRAGQuestionAnswerer` class is the foundation for building RAG applications with the Pathway vector store and xpack components, enabling a quick start with RAG applications.

This example uses `AdaptiveRAGQuestionAnswerer`, an extension of `BaseRAGQuestionAnswerer` with adaptive retrieval. For more on building and deploying RAG applications with Pathway, including containerization, refer to the demo on question answering.

The application responds to requests at the `/v1/pw_ai_answer` endpoint. The `pw_ai_query` function takes the `pw_ai_queries` table as input, containing prompts and other arguments from the post request. This table's data is used to call the adaptive retrieval logic.

The `AdaptiveRAGQuestionAnswerer` implementation under `pathway.xpacks.llm.question_answering` builds a RAG app with the Pathway vector store and components. It supports two question answering strategies, short (concise) and long (detailed) responses, set during the post request. It allows LLM agnosticity, giving the freedom to choose between proprietary or open-source LLMs. It adapts the number of chunks used as a context, starting with `n_starting_documents` chunks and increasing until an answer is found.

To learn more about building & deploying RAG applications with Pathway, including containerization, refer to [demo question answering](https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/private-rag/../demo-question-answering/README.md).


## Table of contents

This includes the technical details to the steps to create a REST Endpoint to run the application via Docker and modify it for your use-cases.

- [Overview](#overview)
- [Architecture](#architecture)
- [Deploying and using a local LLM](#deploying-and-using-a-local-llm)
- [Running the app](#running-the-app)
- [Querying the app/pipeline](#querying-the-app/pipeline)
- [Modifying the code](#modifying-the-code)
- [Conclusion](#conclusion)


## Architecture

![Architecture](https://i.imgur.com/9TJEoUd.png?raw=true)

The architecture consists of two connected technology bricks, which will run as services on your machine:
- Pathway brings support for real-time data synchronization pipelines out of the box, and the possibility of secure private document handling with enterprise connectors for synchronizing Sharepoint and Google Drive incrementally. The Pathway service you'll run performs live document indexing pipeline, and will use Pathway’s built-in vector store.
- The language model you use will be a Mistral 7B, which you will locally deploy as an Ollama service. This model was chosen for its performance and compact size.

## Customizing the pipeline

The code can be modified by changing the `app.yaml` configuration file. To read more about YAML files used in Pathway templates, read [our guide](https://pathway.com/developers/user-guide/llm-xpack/yaml-templates).

In the `app.yaml` file we define:
- input connectors
- LLM
- embedder
- index
and any of these can be replaced or, if no longer needed, removed. For components that can be used check 
Pathway [LLM xpack](https://pathway.com/developers/user-guide/llm-xpack/overview), or you can implement your own.
 
You can also check our other templates - [demo-question-answering](https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/demo-question-answering), 
[Multimodal RAG](https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/gpt_4o_multimodal_rag) or 
[Private RAG](https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/private-rag). As all of these only differ 
in the YAML configuration file, you can also use them as an inspiration for your custom pipeline.

Here some examples of what can be modified.

### LLM Model

This template is prepared to run by default locally. However, the pipeline is LLM model agnostic, so you can change them to use other locally deployed model, or even
use LLM model available through API calls. For discussion on models used in this template check [the dedicated Section](#deploying-and-using-a-local-llm).

### Webserver

You can configure the host and the port of the webserver.
Here is the default configuration:
```yaml
host: "0.0.0.0"
port: 8000
```

### Cache

You can configure whether you want to enable cache, to avoid repeated API accesses, and where the cache is stored.
Default values:
```yaml
with_cache: True
cache_backend: !pw.persistence.Backend.filesystem
  path: ".Cache"
```

### Data sources

You can configure the data sources by changing `$sources` in `app.yaml`.
You can add as many data sources as you want. You can have several sources of the same kind, for instance, several local sources from different folders.
The sections below describe how to configure local, Google Drive and Sharepoint source, but you can use any input [connector](https://pathway.com/developers/user-guide/connecting-to-data/connectors) from Pathway package.

By default, the app uses a local data source to read documents from the `data` folder.

#### Local Data Source

The local data source is configured by using map with tag `!pw.io.fs.read`. Then set `path` to denote the path to a folder with files to be indexed.

#### Google Drive Data Source

The Google Drive data source is enabled by using map with tag `!pw.io.gdrive.read`. The map must contain two main parameters:
- `object_id`, containing the ID of the folder that needs to be indexed. It can be found from the URL in the web interface, where it's the last part of the address. For example, the publicly available demo folder in Google Drive has the URL `https://drive.google.com/drive/folders/1cULDv2OaViJBmOfG5WB0oWcgayNrGtVs`. Consequently, the last part of this address is `1cULDv2OaViJBmOfG5WB0oWcgayNrGtVs`, hence this is the `object_id` you would need to specify.
- `service_user_credentials_file`, containing the path to the credentials files for the Google [service account](https://cloud.google.com/iam/docs/service-account-overview). To get more details on setting up the service account and getting credentials, you can also refer to [this tutorial](https://pathway.com/developers/user-guide/connectors/gdrive-connector/#setting-up-google-drive).

Besides, to speed up the indexing process you may want to specify the `refresh_interval` parameter, denoted by an integer number of seconds. It corresponds to the frequency between two sequential folder scans. If unset, it defaults to 30 seconds.

For the full list of the available parameters, please refer to the Google Drive connector [documentation](https://pathway.com/developers/api-docs/pathway-io/gdrive#pathway.io.gdrive.read).

#### SharePoint Data Source

This data source requires Scale or Enterprise [license key](https://pathway.com/pricing) - you can obtain free Scale key on [Pathway website](https://pathway.com/get-license).

To use it, set the map tag to be `!pw.xpacks.connectors.sharepoint.read`, and then provide values of `url`, `tenant`, `client_id`, `cert_path`, `thumbprint` and `root_path`. To read about the meaning of these arguments, check the Sharepoint connector [documentation](https://pathway.com/developers/api-docs/pathway-xpacks-sharepoint/#pathway.xpacks.connectors.sharepoint.read).


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
  cache_strategy: !pw.udfs.DiskCache
  temperature: 0
  top_p: 1
  format: "json"  # only available in Ollama local deploy, not usable in Mistral API
  api_base: "http://localhost:11434"
```

## Running the app

First, make sure your local LLM is up and running. By default, the pipeline tries to access the LLM at `http://localhost:11434`. You can change that by setting `api_base` value in the app.yaml file.

### With Docker
In order to let the pipeline get updated with each change in local files, you need to mount the folder onto the docker. The following commands show how to do that.

```bash
# Build the image in this folder
docker build -t privaterag .

# Run the image, mount the `data` folder into image 
# -e is used to pass value of LLM_API_BASE environmental variable
docker run -v ./data:/app/data -e LLM_API_BASE -p 8000:8000 privaterag
```

### Locally
To run locally you need to install the Pathway app with LLM dependencies using:
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
curl -X 'POST'   'http://0.0.0.0:8000/v1/pw_ai_answer'   -H 'accept: */*'   -H 'Content-Type: application/json'   -d '{
  "prompt": "What is the start date of the contract?" 
}'
```
> `December 21, 2015 [6]`


## Conclusion:

Now you have a fully private RAG set up with Pathway and Ollama. All your data remains safe on your system. Moreover, the set-up is optimized for speed, thanks to how Ollama runs the LLM, and how Pathway’s adaptive retrieval mechanism reduces token consumption while preserving the accuracy of the RAG.

This is a full production-ready set-up which includes reading your data sources, parsing the data, and serving the endpoint.
This private RAG setup can be run entirely locally with open-source LLMs, making it ideal for organizations with sensitive data and explainable AI needs.

## Quick Links:

- [Pathway Developer Documentation](https://pathway.com/developers/user-guide/introduction/welcome)
- [Pathway App Templates](https://pathway.com/developers/templates)
- [Discord Community of Pathway](https://discord.gg/pathway)
- [Pathway Issue Tracker](https://github.com/pathwaycom/pathway/issues)
- [End-to-end dynamic RAG pipeline with Pathway](https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/demo-question-answering)
- [Using Pathway as a vector store with Langchain](https://python.langchain.com/v0.2/docs/integrations/vectorstores/pathway/) 
- [Using Pathway as a retriever with LlamaIndex](https://docs.llamaindex.ai/en/stable/examples/retrievers/pathway_retriever/) 

Make sure to drop a “Star” to our repositories if you found this resource helpful!

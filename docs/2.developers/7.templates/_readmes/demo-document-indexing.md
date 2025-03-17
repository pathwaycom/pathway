<p align="center">
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/demo-document-indexing/../../../assets/gcp-logo.svg" alt="GCP Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/gcp-deploy">Deploy with GCP</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/demo-document-indexing/../../../assets/aws-fargate-logo.svg" alt="AWS Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/aws-fargate-deploy">Deploy with AWS</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/demo-document-indexing/../../../assets/azure-logo.svg" alt="Azure Logo" height="30">
    <a href="/developers/user-guide/deployment/azure-aci-deploy">Deploy with Azure</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/demo-document-indexing/../../../assets/render.png" alt="Render Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/render-deploy"> Deploy with Render </a>
</p>

# Realtime Document Indexing: Vector Store with always-up-to-date knowledge

This is a basic service for a real-time document indexing pipeline powered by [Pathway](https://github.com/pathwaycom/pathway).

The capabilities of the pipeline include:
    
- Real-time document indexing from Microsoft 365 SharePoint, Google Drive, or a local directory;
- Similarity search by user query;
- Filtering by the metadata according to the condition given in [JMESPath format](https://jmespath.org/);
- Basic stats on the indexer's health.

## Summary of the Pipeline

This example spawns a lightweight webserver that accepts queries on three possible endpoints:
- `/v1/retrieve` to perform similarity search;
- `/v1/statistics` to get the basic stats about the indexer's health;
- `/v1/inputs` to retrieve the metadata of all files currently processed by the indexer.

Please refer to the Open API doc on Hosted Pipelines [website](https://pathway.com/solutions/ai-pipelines) for the format of the requests to the endpoints.

## How It Works

This pipeline uses several Pathway connectors to read the data from the local drive, Google Drive, and Microsoft SharePoint sources. It allows you to poll the changes with low latency and to do the modifications tracking. So, if something changes in the tracked files, the corresponding change is reflected in the internal collections. The contents are read into a single Pathway Table as binary objects. 

After that, those binary objects are parsed with [unstructured](https://unstructured.io/) library and split into chunks. With the usage of OpenAI API, the pipeline embeds the obtained chunks.

Finally, the embeddings are indexed with the capabilities of Pathway's machine-learning library. The user can then query the created index with simple HTTP requests to the endpoints mentioned above.

## Pipeline Organization

This folder contains several objects:
- `main.py`, the pipeline code using Pathway and written in Python;
- `app.yaml`, the file containing configuration of the pipeline, like embedding model, sources, or the server address;
- `requirements.txt`, the textfile denoting the pip dependencies for running this pipeline. It can be passed to `pip install -r ...` to install everything that is needed to launch the pipeline locally;
- `Dockerfile`, the Docker configuration for running the pipeline in the container;
- `docker-compose.yml`, the docker-compose configuration for running the pipeline along with the chat UI;
- `.env`, a short environment variables configuration file where the OpenAI key must be stored;
- `files-for-indexing/`, a folder with exemplary files that can be used for the test runs.

## Customizing the pipeline

The code can be modified by changing the `app.yaml` configuration file. To read more about YAML files used in Pathway templates, read [our guide](https://pathway.com/developers/user-guide/llm-xpack/yaml-templates).

In the `app.yaml` file we define:
- input connectors
- embedder
- index
and any of these can be replaced or, if no longer needed, removed. For components that can be used check 
Pathway [LLM xpack](https://pathway.com/developers/user-guide/llm-xpack/overview), or you can implement your own.

Here some examples of what can be modified.

### Embedding Model

By default this template uses locally run model `mixedbread-ai/mxbai-embed-large-v1`. If you wish, you can replace this with any other model, by changing
`$embedder` in `app.yaml`. For example, to use OpenAI embedder, set:
```yaml
$embedder: !pw.xpacks.llm.embedders.OpenAIEmbedder
  model: "text-embedding-ada-002"
  cache_strategy: !pw.udfs.DiskCache
```

If you choose to use a provider, that requires API key, remember to set appropriate environmental values (you can also set them in `.env` file).

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

## Running the Example

### Locally

This example can be run locally by executing `python main.py` in this directory. It has several command-line arguments:
- `--host` denoting the host, where the server will run. The default setting is `0.0.0.0`;
- `--port` denoting the port, where the server will accept requests. The default setting is `8000`;
- `--sources-config` pointing to a data source configuration file, `sources_configuration.yaml` by default. You can customize it to change the folders indexed by the vector store. The free version supports `local` and `gdrive` hosted files, while the commercial one also supports `sharepoint` hosted folders. By default, the `local` option indexes files from the `files-for-indexing/` folder that is prefilled with exemplary documents.

Please note that the local run requires the dependencies to be installed. It can be done with a simple pip command:

```bash
pip install -r requirements.txt
```

### With Docker

First, create or fill the `.env` file in this folder (`/demo-document-indexing`) with your OpenAI key `OPENAI_API_KEY=sk-*******`.

To run jointly the vector indexing pipeline and a simple UI please execute:

```bash
docker compose up --build
```

Then, the UI will run at http://127.0.0.1:8501 by default. You can access it by following this URL in your web browser.

The `docker-compose.yml` file declares a [volume bind mount](https://docs.docker.com/reference/cli/docker/container/run/#volume) that makes changes to files under `files-for-indexing/` made on your host computer visible inside the docker container. If the index does not react to file changes, please check that the bind mount works 
by running `docker compose exec pathway_vector_indexer ls -l /app/files-for-indexing/` and verifying that all files are visible.

Alternatively, you can launch just the indexing pipeline as a single Docker container:

```bash
docker build -t vector_indexer .
docker run -v `pwd`/files-for-indexing:/app/files-for-indexing -p 8000:8000 vector_indexer
```

The volume overlay is important - without it, docker will not see changes to files under the `files-for-indexing` folder.

## Querying the Example

Since the pipeline spawns an HTTP server and interacts with the user using the REST protocol, you can test it locally by sending requests with `curl`. To get the target address for the request, you can refer to the routes in the first section: `v1/retrieve`, `v1/statistics`, and `v1/inputs` and the Open API docs available at the Hosted Pipelines webpage.

Let's assume that the server runs with the default settings and therefore uses port 8000. That being said, you can query the similarity search endpoint with the following command:

```bash
curl -X 'GET' \
  'http://localhost:8000/v1/retrieve?query=Pathway%20data%20processing%20framework&k=2' \
  -H 'accept: */*'
```

Above, there are two CGI parameters to be customized: the `query`, which should contain the [urlencoded](https://en.wikipedia.org/wiki/Percent-encoding) search terms, and an integer `k` denoting the number of documents you want to retrieve.

Apart from the querying part, there is a health-check endpoint that shows the number of documents currently indexed and the most recent indexing time. It can be accessed, in a similar way, with the following command:

```bash
curl -X 'GET' \
  'http://localhost:8000/v1/statistics' \
  -H 'accept: */*'
```

Finally, there is a way to see the metadata of all documents that are currently indexed. To do that, you need to query the `v1/inputs` endpoint. The request command then looks as follows:

```bash
curl -X 'GET' \
  'http://localhost:8000/v1/inputs' \
  -H 'accept: */*'
```

Please make sure that you've changed the port in the requests if you did that when launching the main script.

## Adding Files to Index

To test index updates, simply add more files to the `files-for-indexing` folder if the local data source is used. 
If you are using Google Drive, simply upload your files in the folder configured in the `sources_configuration.yaml` file.

Then you can use the similarity search and stats endpoints, provided below.

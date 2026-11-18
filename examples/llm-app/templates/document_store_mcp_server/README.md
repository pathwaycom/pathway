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

# MCP Server with Realtime Document Indexing

This is a template for exposing a real-time document indexing pipeline powered by [Pathway Live Data Framework](https://github.com/pathwaycom/pathway) as an Model Context Protocol (MCP) server. 

The [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) is designed to standardize the way applications interact with large language models (LLMs). It serves as a bridge, much like a universal connector, enabling seamless integration between AI models and various data sources and tools. This protocol facilitates the creation of sophisticated AI workflows and agents, enhancing the capabilities of LLMs by connecting them with real-world data and functionalities.

The capabilities of the pipeline include:
    
- Real-time document indexing from Microsoft 365 SharePoint, Google Drive, or a local directory;
- Similarity search by user query;
- Filtering by the metadata according to the condition given in [JMESPath format](https://jmespath.org/);
- The documents are available from a standardized MCP server.

## Summary of the Pipeline

This example spawns an MCP server that has three tools:
- `retrieve_query` to perform similarity search on the indexed documents,
- `statistics_query` to get the basic stats about the indexer's health,
- `inputs_query` to retrieve the metadata of all files currently processed by the indexer.

You can get specification of those tools by querying the `list_tools` on the MCP server.

## How It Works

This pipeline uses several Pathway Live Data Framework connectors to read the data from the local drive, Google Drive, or Microsoft SharePoint sources. It allows you to poll the changes with low latency and to do the modifications tracking. So, if something changes in the tracked files, the corresponding change is reflected in the internal collections. The contents are read into a single Pathway Live Data Framework Table as binary objects. 

After that, those binary objects are parsed with the [Docling](https://www.docling.ai/) library and split into chunks. With the usage of the [SentenceTransformer](https://www.sbert.net/) embedder, the pipeline embeds the obtained chunks.

Finally, the embeddings are indexed with the capabilities of Pathway Live Data Framework's machine-learning library. The user can then query the created index by connecting to the MCP server using an MCP client.

## Pipeline Organization

This folder contains several objects:
- `app.py`, the pipeline code using Pathway Live Data Framework and written in Python;
- `app.yaml`, the file containing configuration of the pipeline, like embedding model, sources, or the server address;
- `requirements.txt`, the textfile denoting the pip dependencies for running this pipeline. It can be passed to `pip install -r requirements.txt` to install everything that is needed to launch the pipeline locally;
- `Dockerfile`, the Docker configuration for running the pipeline in the container;
- `docker-compose.yml`, the docker-compose configuration for running the pipeline along with the chat UI;
- `files-for-indexing/`, a folder with exemplary files that can be used for the test runs.

## Customizing the pipeline

The code can be modified by changing the `app.yaml` configuration file. To read more about YAML files used in Pathway Live Data Framework templates, read [our guide](https://pathway.com/developers/templates/configure-yaml).

In the `app.yaml` file we define:
- input connectors
- embedder
- index
and any of these can be replaced or, if no longer needed, removed. For components that can be used check 
The Pathway Live Data Framework [LLM xpack](https://pathway.com/developers/user-guide/llm-xpack/overview), or you can implement your own.

Here some examples of what can be modified.

### Embedding Model

By default this template uses locally run model `mixedbread-ai/mxbai-embed-large-v1`. If you wish, you can replace this with any other model, by changing
`$embedder` in `app.yaml`. For example, to use OpenAI embedder, set:
```yaml
$embedder: !pw.xpacks.llm.embedders.OpenAIEmbedder
  model: "text-embedding-3-small"
  cache_strategy: !pw.udfs.DefaultCache {}
  retry_strategy: !pw.udfs.ExponentialBackoffRetryStrategy {}
```

If you choose to use a provider, that requires API key, remember to set appropriate environmental values (you can also set them in the `.env` file) - e.g. for using OpenAI embedders, set the `OPENAI_API_KEY` variable.

### Webserver

You can configure the name, the host and the port of the MCP server.
Here is the default configuration:
```yaml
mcp_http: !pw.xpacks.llm.mcp_server.PathwayMcp
  name: "Streamable MCP Server"
  transport: "streamable-http"
  host: "localhost"
  port: 8068
  serve:
    - $document_store
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

By default, the app uses a local data source to read documents from the `files-from-indexing` folder.

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

## Running the Template

### Pathway Live Data Framework License Key
The Pathway Live Data Framework MCP Server requires a Pathway Live Data Framework license key, so before you run the template, you need to set the license key. This template is available for free via [Pathway Live Data Framework Scale](https://pathway.com/features), for which you can get the license key [here](https://pathway.com/user/license). Once you have your license key, create a `.env` file, in which set `PATHWAY_LICENSE_KEY` to your license key - see `.env.example` for an example of `.env` file.

### Locally

This template can be run locally by executing `python app.py` in this directory. Please note that the local run requires the Pathway Live Data Framework library and other dependencies to be installed. It can be done with a pip command:

```bash
pip install pathway[all]
pip install -r requirements.txt
```

### With Docker`.

To run jointly the MCP server with real-time document indexint, please execute:

```bash
docker compose up --build
```

The `docker-compose.yml` file declares a [volume bind mount](https://docs.docker.com/reference/cli/docker/container/run/#volume) that makes changes to files under `files-for-indexing/` made on your host computer visible inside the docker container. If the index does not react to file changes, please check that the bind mount works 
by running `docker compose exec pathway_vector_indexer ls -l /app/files-for-indexing/` and verifying that all files are visible.


## Querying the Template with an MCP client

To test your examples, you need an MCP client which will connect to your MCP server. You can use the fastmcp package to define a client as follows:

```python
import asyncio
from fastmcp import Client

# Change the URL if you change the default values in the app.yaml
PATHWAY_MCP_URL = "http://localhost:8068/mcp/"

client = Client(PATHWAY_MCP_URL)


async def main():
    async with client:
        tools = await client.list_tools()
        print(tools)

    async with client:
        result = await client.call_tool(
            name="retrieve_query",
            arguments={"query": "How to create a webserver in Pathway Live Data Framework?", "k": 3},
        )
        print(result)


asyncio.run(main())

```

You can list the different tools available in the MCP server using the `list_tools` of the client. To access a given tool, you can use the method call_tool, with the name and arguments parameters. The arguments should be a dict of the different values: in this case, the `retrieve_query` tool has two required arguments: `query` and `k`.

## Using MCP server in Claude Desktop
To use MCP server created by this template in Claude Desktop, follow the [guide in Pathway Live Data Framework's documentation](https://pathway.com/developers/user-guide/llm-xpack/pathway-mcp-claude-desktop). 

## Adding Files to Index

To test index updates, simply add more files to the `files-for-indexing` folder if the local data source is used. 
If you are using Google Drive, simply upload your files in the folder configured in the `sources_configuration.yaml` file.

Then you can use the similarity search and stats endpoints, provided below.

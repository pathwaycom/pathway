<p align="center">
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/slides_ai_search/../../../assets/gcp-logo.svg" alt="GCP Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/gcp-deploy">Deploy with GCP</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/slides_ai_search/../../../assets/aws-fargate-logo.svg" alt="AWS Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/aws-fargate-deploy">Deploy with AWS</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/slides_ai_search/../../../assets/azure-logo.svg" alt="Azure Logo" height="30">
    <a href="/developers/user-guide/deployment/azure-aci-deploy">Deploy with Azure</a> |
    <img src="https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/slides_ai_search/../../../assets/render.png" alt="Render Logo" height="30">
    <a href="https://pathway.com/developers/user-guide/deployment/render-deploy"> Deploy with Render </a>
</p>

# **Slides AI Search App**

## **Overview**

This app template will help you build a multi-modal search service using `GPT-4o` with Metadata Extraction and Vector Index. It uses [Pathway](https://github.com/pathwaycom/llm-app) for indexing and retrieving slides from PowerPoint and PDF presentations.

How is this different?

* Build highly accurate RAG pipelines powered by indexes that are updated in real-time.
* All of the steps, including parsing, embedding and indexing happen locally on your machine (local or cloud). 
* Pathway uses vision language models to understand and index your presentations and PDFs, automatically updating as changes are made.
* Get started with a minimalistic and production-ready approach.

Boost productivity with accurate search across your PowerPoints, PDFs, and Slides all within your work environment. Try out the [demo](https://sales-rag-chat.demo.pathway.com/#search-your-slide-decks) here.


## Quickstart

Check the `.env.example`, create a new `.env` file and fill in the template. 
For a quick start, you need to only change the following fields:
- `PATHWAY_LICENSE_KEY`
- `OPENAI_API_KEY`

This app template is available for free via [Pathway Scale](https://pathway.com/features). Get your [license key here](https://pathway.com/user/license) and fill in the `PATHWAY_LICENSE_KEY` here in the `.env` file.

To learn more about configuring the input sources, how to overcome OpenAI limits and other information, check out the [configuration section below](#prerequisitesconfiguration).
> **Note:** Pathway API is only used for logging basic statistics, everything happens and stays in your computer, except the OpenAI API calls. No personal or private data will be sent to Pathway servers. Handling of the data, processing, parsing and indexing are done locally.

## How it Helps

**1) Improved Efficiency:**

* **Save Efforts:** You no longer need to manually sift through countless presentations.
* **Faster Information Retrieval:** Instantly find specific information with a few keywords or descriptive prompts, saving you time when preparing for presentations or reviewing past projects.

**2) Enhanced Organization**

* **Automated Categorization:** You can organize your slide library by topic, project, or other criteria. Configure the schema file to customize the parsed fields.

**3) Enhanced Reliability**

* **Automatic Updates:** Hybrid indexes update automatically whenever a new slide is added or removed, ensuring your information is always current and accurate.

**4) Automated Slide Parsing:** 

* Process PPTX and PDF slide decks with vision language models to extract the content. (The default setup loads PDF's).

**5) Flexible Data Sources:** 

* Compatible with local directories, SharePoint, Google Drive, and other [Pathway connectors](https://pathway.com/developers/user-guide/connect/pathway-connectors), ensuring a wide range of application scenarios can be supported.

By automating the extraction and retrieval of slide information, this app addresses the critical pain point of managing and utilizing extensive slide decks efficiently, enhancing productivity and information accuracy for sales teams.


## Architecture:

The architecture of the Slides AI Search App is designed to connect various local or cloud repositories, transforming and indexing slides for efficient querying. It supports integration with closed and open-source LLMs for enhanced search capabilities.

![Architecture](https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/slides_ai_search/ai-slides-diagram.svg?raw=true)

This demo consists of three parts:
* `app.py`: Pathway app that handles parsing, indexing and backend.
* `nginx`: File server that hosts images to be consumed by the UI.
* `UI`: A Streamlit UI for interacting with the app.


## How it works:

### **Data Ingestion**

1. **Data Sources**:
    * The application reads slide files (PPTX and PDF) from a specified directory. The directory is set to `./data/`in the `app.py` file.
    * In the default app setup, the connected folder is a local file folder. You can add more folders and file sources, such as [Google Drive](https://pathway.com/developers/user-guide/connectors/gdrive-connector/#google-drive-connector) or [Sharepoint](https://pathway.com/developers/user-guide/connecting-to-data/connectors/#tutorials), by changing configuration in `app.yaml`.
    * More inputs can be added by configuring the `sources` list in the `app.yaml`.


### **Slide Parsing and Indexing**


1. **Parsing**:
    * The [`SlideParser`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.SlideParser) from Pathway is used to parse the slides. The parser is configured to parse a text description and schema that is defined in the `app.yaml`.
    * Our example schema includes fields such as `category`, `tags`, `title`, `main_color`, `language`, and `has_images`. This can be modified for specific use cases.
    * Note that, UI is configured to make use of two extracted fields `category` and `language`, these need to be kept for the UI to work. However, the app can still be used without the UI with different schemas or no parsed schema.
2. **Embedding**:
    * Parsed slide content is embedded with the OpenAI's `text-embedding-ada-002` embedder.
    * The embeddings are then stored in Pathway's vector store using the `SlidesVectorStoreServer`.
3. **Metadata Handling**:
    * Images and files are dumped into local directories (`storage/pw_dump_images` and `storage/pw_dump_files`).
    * Each slide gets a unique ID. This helps with opening files and images from the UI.
    

### **Query Handling**

1. **Retrieval Augmented Generation (RAG)**:
    * The `DeckRetriever` class builds the backend, handling all steps of the application from parsing files to serving the endpoints. Refer to the [API docs](https://pathway.com/developers/api-docs/pathway-xpacks-llm/question_answering#pathway.xpacks.llm.question_answering.DeckRetriever) for more information.

## Pipeline Organization

This folder contains several components necessary for setting up and running the Sales Slide RAG application:


1. **app.py**:
    * The main application that sets up the slide search functionality. It initializes the OpenAI vision-language model, slide parser, vector store, and initializes the DeckRetriever for handling queries.
2. **app.yaml**:
    * Defines data sources, OpenAI vision-language model configuration, and other key settings.
    * Defines the schema for parsing the slides and including fields such as `category`, `tags`, `title`, `main_color`, `language`, and `has_images`. These fields will be appended to the `metadata`, if you prefer to also add them to `text` field, set `include_schema_in_text` of `SlideParser` to `True`.
    * **Note:** If you intend the use the default UI, `category` and the `language` fields in the schema are needed for the filtering options in the UI. The UI will not function properly without them.

3. **.env**:
    * Config file for the environment variables, such as the OpenAI API key and Pathway key.

## **Prerequisites/Configuration**

### **Environment Setup**

1. **OpenAI API Key**: 
    * Get an API key from the [OpenAI’s API Key Management page](https://platform.openai.com/account/api-keys). Keep this API key secure.
    * Configure your key in the `.env` file.
    * You can refer to the stub file `.env.example` in this repository.
    * Note: This is only needed in OpenAI LLMs and embedders. It is also possible to use other multi-modal, local LLMs and embedders.

    **OpenAI API Usage**:
    * This app relies on `gpt-4o` model for image parsing. OpenAI currently limits the usage to paid users only. It is possible to use any other model (including local models) with the modules under the `pathway.xpacks`.
    * If you are experiencing API throttle, you can set the `capacity` parameter of the LLM instance `llms.OpenAIChat` to be lower. This parameter defines the number of parallel requests. Or, it is possible to disable parallel requests and only parse sequentially by changing the `run_mode` in the `SlideParser` to `run_mode="sequential"` instead of the `"parallel"`.
    * Update: The newly released `gpt-4o-mini` model has a similar image understanding performance at lower costs, it is also another good option.

2. **Pathway’s License Key**: 
    * This app template is available for free via [Pathway Scale](https://pathway.com/features).
    * Get your [license key here](https://pathway.com/user/license).
    * **Note:** Pathway API is only used for logging basic statistics, everything happens and stays in your computer except the OpenAI API calls. No personal or private data will be sent to Pathway servers.

### **Configuring the Inputs**

By default, the app takes the files under the `./data/` folder as input. Inputs can be set by adding more entries to the `sources` list under the `app.yaml`.

It is possible to configure the app to use any kind of input, `Google Drive`, `Microsoft 365 SharePoint`, or a `local directory` to name a few.
You can also use other kind of data sources using the [connectors](https://pathway.com/developers/user-guide/connecting-to-data/connectors) provided by Pathway.

Pathway polls the changes with low latency. So, if something changes in the tracked files, the corresponding change is reflected in real-time, and search results are updated accordingly.
To learn more about the data sources, you can check out [demo question answering](https://github.com/pathwaycom/llm-app/blob/main/examples/pipelines/slides_ai_search/../demo-question-answering/README.md#data-sources)

## How to run the project on your machine

First, clone the Pathway LLM App Repository

```bash
git clone https://github.com/pathwaycom/llm-app.git
```

Make sure you are in the right directory:
```bash
cd examples/pipelines/slides_ai_search
```

> Note: If your OpenAI API usage is throttled, you may want to change the `run_mode` in the `SlideParser` to `run_mode="sequential"` instead of the `"parallel"`.

## Deploying and running with Docker 

Build the Docker with:

```bash
docker compose build
```

And, run with:

```bash
docker compose up
```

This will start all three components of the demo. This deployment method is recommended for production use.

## Using the app

After Docker is running, you will see a stream of logs of your files being parsed.

### Accessing the UI

On your browser, visit [`http://localhost:8501`](http://localhost:8501/) to access the UI.

Here, you will see a search bar, some filters, and information about the indexed documents on the left side.


### Sending requests to the server

#### With CURL

UI is not a necessary component, especially for developers. If you are interested in building your own app, check out the following ways to use the app:

First, let's check the indexed files:
```bash
curl -X 'POST'   'http://0.0.0.0:8000/v1/pw_list_documents'   -H 'accept: */*'   -H 'Content-Type: application/json'
```

This will return a list of metadata from the indexed files.

Now, let's search through our slides:


```bash
curl -X 'POST'   'http://0.0.0.0:8000/v1/pw_ai_answer'   -H 'accept: */*'   -H 'Content-Type: application/json'   -d '{
  "prompt": "diagrams that contain value propositions" 
}'
```

This will search through our files, and return parsed slides with the `text`, `slide_id` and other `metadata` (also including the parsed schema).

#### With the Pathway RAG Client

Import RAGClient with:

```python
from pathway.xpacks.llm.question_answering import RAGClient
```

Initialize the client:

```python
# conn = RAGClient(url=f"http://{PATHWAY_HOST}:{PATHWAY_PORT}")

# with the default config
conn = RAGClient(url=f"http://localhost:8000")
```

List the indexed files:
```python
conn.pw_list_documents()
```
> `[{'path': 'data/slide.pdf'}, ...`

Query the app:

```python
conn.pw_ai_answer("introduction slide")
```
> `[{'dist': 0.47761982679367065, 'metadata': ...`


## Advanced variant: Run locally without Open AI, using a local LLM and Embedder

To run the app fully locally, without needing any API access, we use [vLLM](https://github.com/vllm-project/vllm) and open source embedder from the HuggingFace.

### Running the local LLM
We use Phi 3 Vision for its relatively small size and good performance. It is possible to use any other VLM.

1. **Download and Install vLLM:**

See the [installation page](https://docs.vllm.ai/en/latest/getting_started/installation.html).

2. **Run the model:**

The following command will run the Phi 3 vision model and mimic the OpenAI API.

```bash
python -m vllm.entrypoints.openai.api_server --model microsoft/Phi-3-vision-128k-instruct --trust-remote-code --dtype=half --image-input-type pixel_values --image-token-id=32044 --image-input-shape=1,3,1008,1344 --image-feature-size=1921 --max-model-len=42500 --gpu-memory-utilization 0.9  --swap-space 16 --max-num-seqs 65
```

Check if the model is available with:

```bash
curl http://localhost:8000/v1/completions \ns \
    -H "Content-Type: application/json" \
    -d '{
        "model": "microsoft/Phi-3-vision-128k-instruct",
        "prompt": "San Francisco is a",
        "max_tokens": 7,
        "temperature": 0
    }'
```

### Set the LLM Instance in the configuration file

```yaml
llm: !pw.xpack.llm.llms.OpenAIChat
  model: "microsoft/Phi-3-vision-128k-instruct"
  temperature: 0.0
  capacity: 1
  base_url: "http://localhost:8000/v1"
  api_key: "ignore the key, not needed"
  cache_strategy: !DiskCache
  retry_strategy: !ExponentialBackoffRetryStrategy
    max_retries: 3
```

This will use your local Phi 3 vision model as the LLM for parsing the slides.

### Set an open-source embedder model for embeddings

Here, we can check [MTEB Leaderboard](https://huggingface.co/spaces/mteb/leaderboard) to find a good-performing embedder model. 
From performance/computational-cost standpoint, `avsolatorio/GIST-Embedding-v0`, `avsolatorio/GIST-small-Embedding-v0`, `mixedbread-ai/mxbai-embed-large-v1`, `Alibaba-NLP/gte-large-en-v1.5` are some of the better models.

Here, we go with the `avsolatorio/GIST-small-Embedding-v0`. 
Note that, larger models may take longer to process the inputs.

We replace the `embedder` with the following embedding model in `app.yaml`:

```yaml
$embedding_model: "avsolatorio/GIST-small-Embedding-v0"

embedder: !pw.xpack.llms.embedders.SentenceTransformerEmbedder
  model: $embedding_model
  call_kwargs: 
    show_progress_bar: false
```

## Advanced variant: Running without Docker
Running the whole demo without Docker is a bit tricky as there are three components. 

1. **Download and Install LibreOffice:**
    * Download LibreOffice from the [LibreOffice website](https://www.libreoffice.org/download/download-libreoffice).
    * Follow the installation instructions specific to your operating system. \

2. **Verify LibreOffice Installation:**
    * Download LibreOffice from the LibreOffice website.
    * Open a terminal or command prompt and run the following command:
    * You should see the LibreOffice version information, indicating LibreOffice is installed correctly.

        **Purpose:** LibreOffice helps with converting PPTX files into PDFs, which is essential for the document processing workflow in the Slides AI Search App.

If you are on Windows, please refer to the [running with Docker](#running-with-docker) section below. 

To run the Pathway app without the UI, 

```bash
python app.py
```

## Not sure how to get started? 

Let's discuss how we can help you build a powerful, customized RAG application. [Reach us here to talk or request a demo!](https://pathway.com?modal=GetStarted)


## FAQ

> I am getting OpenAI API throttle limit errors.
- You can change `run_mode` in `SlideParser` to `run_mode="sequential"`. This will parse images one by one, however, this will significantly slow down the parsing stage.

> UI shows that my file is being indexed, but I don't have that file in the inputs.
- App mounts `storage` folder from the Docker to the local file system. This helps the file server serve the content. This folder is not cleaned between the runs, files from the previous runs will be staying here. You can remove the folder after closing the app to get rid of these.

> Can I use other vision LMs or LLMs?
- Yes, you can configure the `OpenAIChat` to reach local LLMs or swap it with any other LLM wrappers (such as `LiteLLMChat`) to use other models. Make sure your model of choice supports vision inputs.

> Can I persist the cache between the runs?
- Yes, you can uncomment the `- ./Cache:/app/Cache` under the `app:/volumes:` section inside the `docker-compose.yml` to allow caching between the runs. You will see that requests are not repeated in the next runs.

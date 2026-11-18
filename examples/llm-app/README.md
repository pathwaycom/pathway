<div align="center">

# Pathway Live Data Framework AI Pipelines


![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)
![macOS](https://img.shields.io/badge/mac%20os-000000?style=for-the-badge&logo=apple&logoColor=white)
[![chat on Discord](https://img.shields.io/badge/Discord-5865F2?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/pathway)
[![follow on X](  https://img.shields.io/badge/X-000000?style=for-the-badge&logo=x&logoColor=white)](https://x.com/intent/follow?screen_name=pathway_com)
</div>

The Pathway Live Data Framework's **AI Pipelines** allow you to quickly put in production AI applications that offer **high-accuracy RAG and AI enterprise search at scale** using the most **up-to-date knowledge** available in your data sources. It provides you ready-to-deploy **LLM (Large Language Model) App Templates**. You can test them on your own machine and deploy on-cloud (GCP, AWS, Azure, Render,...) or on-premises.

The apps connect and sync (all new data additions, deletions, updates) with data sources on your **file system, Google Drive, Sharepoint, S3, Kafka, PostgreSQL, real-time data APIs**. They come with no infrastructure dependencies that would need a separate setup. They include **built-in data indexing** enabling vector search, hybrid search, and full-text search - all done in-memory, with cache.


## Application Templates

The application templates provided in this repo scale up to **millions of pages of documents**. Some of them are optimized for simplicity, some are optimized for amazing accuracy. Pick the one that suits you best. You can use it out of the box, or change some steps of the pipeline - for example, if you would like to add a new data source, or change a Vector Index into a Hybrid Index, it's just a one-line change. 

| Application (template)                                                                           | Description                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`Question-Answering RAG App`](templates/question_answering_rag/)    | Basic end-to-end RAG app. A question-answering pipeline that uses the GPT model of choice to provide answers to queries to your documents (PDF, DOCX,...) on a live connected data source (files, Google Drive, Sharepoint,...). You can also try out a [demo REST endpoint](https://pathway.com/solutions/rag-pipelines#try-it-out).              |
| [`Live Document Indexing (Vector Store / Retriever)`](templates/document_indexing/)     | A real-time document indexing pipeline for RAG that acts as a vector store service. It performs live indexing on your documents (PDF, DOCX,...) from a connected data source (files, Google Drive, Sharepoint,...). It can be used with any frontend, or integrated as a retriever backend for a [Langchain](https://pathway.com/blog/langchain-integration) or [Llamaindex](https://pathway.com/blog/llamaindex-pathway) application. You can also try out a [demo REST endpoint](https://pathway.com/solutions/ai-contract-management#try-it-out).         |
| [`Multimodal RAG pipeline with GPT4o`](templates/multimodal_rag/) | Multimodal RAG using GPT-4o in the parsing stage to index PDFs and other documents from a connected data source files, Google Drive, Sharepoint,...). It is perfect for extracting information from unstructured financial documents in your folders (including charts and tables), updating results as documents change or new ones arrive.|
| [`Unstructured-to-SQL pipeline + SQL question-answering`](templates/unstructured_to_sql_on_the_fly/) | A RAG example which connects to unstructured financial data sources (financial report PDFs), structures the data into SQL, and loads it into a PostgreSQL table. It also answers natural language user queries to these financial documents by translating them into SQL using an LLM and executing the query on the PostgreSQL table. |
| [`Adaptive RAG App`](templates/adaptive_rag/) | A RAG application using Adaptive RAG, a technique developed by Pathway to reduce token cost in RAG up to 4x while maintaining accuracy. |
| [`Private RAG App with Mistral and Ollama`](templates/private_rag/) |  A fully private (local) version of the `question_answering_rag` RAG pipeline using Pathway Live Data Framework, Mistral, and Ollama. |
| [`Slides AI Search App`](templates/slides_ai_search/)                                        | An indexing pipeline for retrieving slides. It performs multi-modal of PowerPoint and PDF and maintains live index of your slides."|
| [`Video RAG with TwelveLabs`](templates/video_rag_twelvelabs/) | A RAG pipeline over **video**. It uses [TwelveLabs](https://twelvelabs.io) Pegasus to turn videos into rich text descriptions and Marengo multimodal embeddings to index them, so you can ask questions about your videos on a live connected data source (files, Google Drive,...). |


## How do these AI Pipelines work?

The apps can be run as **Docker containers**, and expose an **HTTP API** to connect the frontend. To allow quick testing and demos, some app templates also include an optional Streamlit UI which connects to this API. 

The apps rely on the [Pathway Live Data Framework](https://github.com/pathwaycom/pathway) for data source synchronization and for serving API requests (Pathway Live Data Framework is a standalone Python library with a Rust engine built into it). They bring you a **simple and unified application logic** for back-end, embedding, retrieval, LLM tech stack. There is no need to integrate and maintain separate modules for your Gen AI app: ~Vector Database (e.g. Pinecone/Weaviate/Qdrant) + Cache (e.g. Redis) + API Framework (e.g. Fast API)~. Pathway Live Data Framework's default choice of **built-in vector index** is based on the lightning-fast [usearch](https://github.com/unum-cloud/usearch) library, and **hybrid full-text indexes** make use of [Tantivy](https://github.com/quickwit-oss/tantivy) library. Everything works out of the box.

## Getting started

Each of the [App templates](templates/) in this repo contains a README.md with instructions on how to run it.

You can also find [more ready-to-run code templates](https://pathway.com/developers/templates/) on the Pathway website.


## Some visual highlights

Effortlessly extract and organize table and chart data from PDFs, docs, and more with multimodal RAG - in real-time:

![Effortlessly extract and organize table and chart data from PDFs, docs, and more with multimodal RAG - in real-time](https://github.com/pathwaycom/pathway/blob/main/examples/llm-app/templates/multimodal_rag/gpt4o_with_pathway_comparison.gif)

(Check out [`Multimodal RAG pipeline with GPT4o`](templates/multimodal_rag/) to see the whole pipeline in the works. You may also check out the [`Unstructured-to-SQL pipeline`](templates/unstructured_to_sql_on_the_fly/) for a minimal example that works with non-multimodal models as well.)


Automated real-time knowledge mining and alerting:

![Automated real-time knowledge mining and alerting](templates/drive_alert/drive_alert_demo.gif)

(Check out the [`Alerting when answers change on Google Drive`](https://github.com/pathwaycom/pathway/tree/main/examples/llm-app/templates/drive_alert) app example.)


###  Do-it-Yourself Videos

▶️ [An introduction to building LLM apps with Pathway Live Data Framework](https://www.youtube.com/watch?v=kcrJSk00duw) - by [Jan Chorowski](https://scholar.google.com/citations?user=Yc94070AAAAJ)

▶️ [Let's build a real-world LLM app in 11 minutes](https://www.youtube.com/watch?v=k1XGo7ts4tI) - by [Pau Labarta Bajo](https://substack.com/@paulabartabajo)


## Troubleshooting

To provide feedback or report a bug, please [raise an issue on our issue tracker](https://github.com/pathwaycom/pathway/issues).

## Contributing

Anyone who wishes to contribute to this project, whether documentation, features, bug fixes, code cleanup, testing, or code reviews, is very much encouraged to do so. If this is your first contribution to a GitHub project, here is a [Get Started Guide](https://docs.github.com/en/get-started/quickstart/contributing-to-projects). 

If you'd like to make a contribution that needs some more work, just raise your hand on the [Pathway Discord server](https://discord.com/invite/pathway) (#get-help) and let us know what you are planning!

## Supported and maintained by

<p align="center">
  <a href="https://github.com/pathwaycom/"><img src="https://pathway.com/logo-light.svg" alt="Pathway"/></a>
</p>
<p align="center">
  <a href="https://pathway.com/solutions/llm-app">
    <img src="https://img.shields.io/badge/See%20Pathway's%20offering%20for%20AI%20applications-0000FF" alt="See Pathway's offering for AI applications"/>
  </a>
</p>

# ---
# title: "Data Indexing"
# description: ''
# aside: true
# author: 'pathway'
# article:
#   date: '2023-12-15'
#   thumbnail: ''
#   tags: ['showcase', 'llm', 'data-pipeline']
# keywords: ['LLM', 'RAG', 'GPT', 'OpenAI', 'Google Docs', 'KNN', 'Vector store', 'langchain', 'llama-index', 'vectordb', 'vectore store langchain', 'retriever', 'unstructured']
# notebook_export_path: notebooks/showcases/live_vector_indexing_pipeline.ipynb
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Always Up-to-date Data Indexing pipeline
#
# This showcase shows how to use Pathway to deploy a live data indexing pipeline, which can be queried like a typical vector store. However, under the hood, Pathway updates the index on each data change, always giving up-to-date answers.
# <!-- canva link: https://www.canva.com/design/DAF1cxQW5Vg/LcFdDrPApBrgwM5kyirY6w/edit  -->
# ::article-img
# ---
# src: '/assets/content/showcases/vectorstore/vectorstore_doc.png'
# alt: 'Pathway data indexing pipeline'
# class: 'mx-auto'
# zoomable: true
# ---
# ::
#
# Pathway Vectorstore enables building a document index on top of you documents without the
# complexity of ETL pipelines, managing different containers for storing, embedding, and serving.
# It allows for easy to manage, always up-to-date, LLM pipelines accesible using a RESTful API
# and with integrations to popular LLM toolkits such as Langchain and LlamaIndex.
#
#
# In this article, we will use a simple document processing pipeline that:
# 1. Monitors several data sources (files, S3 folders, cloud storages) for data changes.
# 2. Parses, splits and embeds the documents.
# 3. Builds a vector index for the data.
#
# However, If you prefer not to create the pipeline from the ground up and would like to check out the functionality,
# take a look at our [`managed pipelines`](https://cloud.pathway.com/docindex) in action.
#
# We will connect to the index using a `VectorStore` client, which allows retrieval of semantically similar documents.

# %% [markdown]
# ## Prerequisites
#
# Install the `pathway` package. You can also install the `unstructured` package to use the most powerful `unstructured.io`-based parser.
#
# Then download sample data.

# %%
# _MD_SHOW_!pip install pathway litellm
# # !pip install unstructured[all-docs]
# _MD_SHOW_!mkdir -p sample_documents
# _MD_SHOW_![ -f sample_documents/repo_readme.md ] || wget 'https://gist.githubusercontent.com/janchorowski/dd22a293f3d99d1b726eedc7d46d2fc0/raw/pathway_readme.md' -O 'sample_documents/repo_readme.md'

# _MD_COMMENT_START_
if 1:  # group to prevent isort messing up
    import json
    import os

    from common.shadows import fs

    os.environ["OPENAI_API_KEY"] = json.loads(
        fs.open("vault://kv.v2:deployments@/legal_rag_demo").read()
    )["OPENAI_KEY"]
# _MD_COMMENT_END_

# %%
import logging
import sys
import time

logging.basicConfig(stream=sys.stderr, level=logging.WARN, force=True)

# %% [markdown]
# ## Building the data pipeline
#
# First, make sure you have an API key with an LLM provider such as OpenAI.

# %%
import getpass
import os

if "OPENAI_API_KEY" not in os.environ:
    os.environ["OPENAI_API_KEY"] = getpass.getpass("OpenAI API Key:")

# %% [markdown]
# We will now assemble the data vectorization pipeline, using a simple `UTF8` file parser, a  character splitter and an embedder from the [Pathway LLM xpack](/developers/user-guide/llm-xpack/overview).
#
# First, we define the data sources. We use the files-based one for simplicity, but any supported `pathway` [connector](/developers/api-docs/pathway-io/), such as [s3](/developers/api-docs/pathway-io/s3/) or [Google Drive](/developers/api-docs/pathway-io/gdrive#pathway.io.gdrive.read) will also work.
#
# Then, we define the embedder and splitter.
#
# Last, we assemble the data pipeline. We will start it running in a background thread to be able to query it immediately from the demonstration. Please note that in a production deployment, the server will run in another process, possibly on another machine. For the quick-start, we keep the server and client as different threads of the same Python process.

# %%
import pathway as pw

# This creates a connector that tracks files in a given directory.
data_sources = []
data_sources.append(
    pw.io.fs.read(
        "./sample_documents",
        format="binary",
        mode="streaming",
        with_metadata=True,
    )
)

# This creates a connector that tracks files in Google Drive.
# Please follow the instructions at /developers/user-guide/connect/connectors/gdrive-connector/ to get credentials.
# data_sources.append(
#     pw.io.gdrive.read(object_id="17H4YpBOAKQzEJ93xmC2z170l0bP2npMy", service_user_credentials_file="credentials.json", with_metadata=True))

# %%
# We now build the VectorStore pipeline

from pathway.xpacks.llm.embedders import OpenAIEmbedder
from pathway.xpacks.llm.splitters import TokenCountSplitter
from pathway.xpacks.llm.vector_store import VectorStoreClient, VectorStoreServer

PATHWAY_PORT = 8765

# Choose document transformers
text_splitter = TokenCountSplitter()
embedder = OpenAIEmbedder(api_key=os.environ["OPENAI_API_KEY"])

# The `PathwayVectorServer` is a wrapper over `pathway.xpacks.llm.vector_store` to accept LangChain transformers.
# Fell free to fork it to develop bespoke document processing pipelines.
vector_server = VectorStoreServer(
    *data_sources,
    embedder=embedder,
    splitter=text_splitter,
)
# _MD_SHOW_vector_server.run_server(host="127.0.0.1", port=PATHWAY_PORT, threaded=True, with_cache=False)
# _MD_SHOW_time.sleep(30)  # Workaround for Colab - messages from threads are not visible unless a cell is running

# %% [markdown]
# We now instantiate and configure the client

# %%
client = VectorStoreClient(
    host="127.0.0.1",
    port=PATHWAY_PORT,
)

# %% [markdown]
# And we can start asking queries

# %%
query = "What is Pathway?"
# _MD_SHOW_docs = client(query)
# _MD_SHOW_docs


# %% [markdown]
# **Your turn!** Now make a change to the source documents or make a fresh one and retry the query!

# %% [markdown]
# ## Integrations
#
# ### Langchain
#
# You can use a Pathway Vector Store in LangChain pipelines with `PathwayVectorClient`
# and configure a `VectorStoreServer` using LangChain components. For more information see [our article](/developers/showcases/langchain-integration) or [LangChain documentation](https://python.langchain.com/v0.1/docs/integrations/vectorstores/pathway/).
#

# %%
# _MD_SHOW_!pip install langchain
# _MD_SHOW_!pip install langchain-openai
# _MD_SHOW_!pip install langchain-community

# %% [markdown]
# ```python
# from langchain_community.vectorstores import PathwayVectorClient
#
# # PathwayVectorClient implements regular VectorStore API of LangChain
# client = PathwayVectorClient(host="127.0.0.1", port=PATHWAY_PORT)
# docs = client.similarity_search("What is Pathway?")
# ```

# %%
# Here we show how to configure a server that uses LangChain document processing components

# _MD_SHOW_from langchain_openai import OpenAIEmbeddings
# _MD_SHOW_from langchain.text_splitter import CharacterTextSplitter

# Choose proper LangChain document transformers
# _MD_SHOW_text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
# _MD_SHOW_embeddings_model = OpenAIEmbeddings(openai_api_key=os.environ["OPENAI_API_KEY"])

# Use VectorStoreServer.from_langchain_components to create a vector server using LangChain
# document processors
# _MD_SHOW_vector_server = VectorStoreServer.from_langchain_components(
# _MD_SHOW_    *data_sources,
# _MD_SHOW_    embedder=embeddings_model,
# _MD_SHOW_    splitter=text_splitter,
# _MD_SHOW_)
# _MD_SHOW_vector_server.run_server(host="127.0.0.1", port=PATHWAY_PORT+1, threaded=True, with_cache=False)
# _MD_SHOW_time.sleep(30)  # colab workaround

# %%
# You can connect to the Pathway+LangChain server using any client - Pathway's, Langchain's or LlamaIndex's!
# _MD_SHOW_client = VectorStoreClient(
# _MD_SHOW_    host="127.0.0.1",
# _MD_SHOW_    port=PATHWAY_PORT+1,
# _MD_SHOW_)

# _MD_SHOW_client.query("pathway")

# %% [markdown]
# ### LlamaIndex
#
# Pathway is fully integrated with LlamaIndex! We show below how to instantiate a Llama-Index
# retriever that queries the Pathway VectorStoreServer
# and how to configure a server using LlamaIndex components.
#
# For more information see `Pathway Retriever`
# [cookbook](https://docs.llamaindex.ai/en/stable/examples/retrievers/pathway_retriever.html).
# %%
# _MD_SHOW_!pip install llama-index llama-index-retrievers-pathway llama-index-embeddings-openai

# %%
# You can connect to the PathwayVectorStore using a llama-index compatible retriever
# _MD_SHOW_from llama_index.retrievers.pathway import PathwayRetriever

# PathwayRetriever implements the Retriever interface
# _MD_SHOW_pr = PathwayRetriever(host="127.0.0.1", port=PATHWAY_PORT)
# _MD_SHOW_pr.retrieve(str_or_query_bundle="What is Pathway?")

# %%
# Here we show how to configure a server that uses LlamaIndex document processing components

# _MD_SHOW_from llama_index.embeddings.openai import OpenAIEmbedding
# _MD_SHOW_from llama_index.core.node_parser import TokenTextSplitter

# Choose proper LlamaIndex document transformers
# _MD_SHOW_embed_model = OpenAIEmbedding(embed_batch_size=10)

# _MD_SHOW_transformations_example = [
# _MD_SHOW_    TokenTextSplitter(
# _MD_SHOW_        chunk_size=150,
# _MD_SHOW_        chunk_overlap=10,
# _MD_SHOW_        separator=" ",
# _MD_SHOW_    ),
# _MD_SHOW_    embed_model,
# _MD_SHOW_]

# Use VectorStoreServer.from_llamaindex_components to create a vector server using LlamaIndex
# document processors
# _MD_SHOW_vector_server = VectorStoreServer.from_llamaindex_components(
# _MD_SHOW_    *data_sources,
# _MD_SHOW_    transformations=transformations_example,
# _MD_SHOW_)
# _MD_SHOW_vector_server.run_server(host="127.0.0.1", port=PATHWAY_PORT+2, threaded=True, with_cache=False)
# _MD_SHOW_time.sleep(30)  # colab workaround

# %%
# You can connect to the Pathway+LlamaIndex server using any client - Pathway's, Langchain's or LlamaIndex's!
# _MD_SHOW_client = VectorStoreClient(
# _MD_SHOW_    host="127.0.0.1",
# _MD_SHOW_    port=PATHWAY_PORT+2,
# _MD_SHOW_)

# _MD_SHOW_client.query("pathway")

# %% [markdown]
# ## Advanced topics
#
# ### Getting information on indexed files

# %% [markdown]
# [`PathwayVectorClient.get_vectorstore_statistics()`](/developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreClient.get_vectorstore_statistics) gives essential statistics on the state of the vector store, like the number of indexed files and the timestamp of the last updated one. You can use it in your chains to tell the user how fresh your knowledge base is.

# %%
# _MD_SHOW_client.get_vectorstore_statistics()

# %% [markdown]
# You can also use [`PathwayVectorClient.get_input_files()`](/developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreClient.get_input_files) to get the list of indexed files along with the associated metadata.

# %%
# _MD_SHOW_client.get_input_files()

# %% [markdown]
# ### Filtering based on file metadata
#
# We support document filtering using [jmespath](https://jmespath.org/) expressions, for instance:

# %%
# take into account only sources modified later than unix timestamp
# _MD_SHOW_docs = client(query, metadata_filter="modified_at >= `1702672093`")

# take into account only sources modified later than unix timestamp
# _MD_SHOW_docs = client(query, metadata_filter="owner == `james`")

# take into account only sources with path containing 'repo_readme'
# _MD_SHOW_docs = client(query, metadata_filter="contains(path, 'repo_readme')")

# and of two conditions
# _MD_SHOW_docs = client(query, metadata_filter="owner == `james` && modified_at >= `1702672093`")

# or of two conditions
# _MD_SHOW_docs = client(query, metadata_filter="owner == `james` || modified_at >= `1702672093`")

# %% [markdown]
# ### Configuring the parser
#
# The vectorization pipeline supports pluggable parsers. If not provided, defaults to `UTF-8` parser. You can find available parsers [here](https://github.com/pathwaycom/pathway/blob/main/python/pathway/xpacks/llm/parsers.py).
# An example parser that can read PDFs, Word documents and other formats is provided with `parsers.ParseUnstructured`:

# %%
# # !pip install unstructured[all-docs]  # if you will need to parse complex documents

# %% [markdown]
# ```python
# from pathway.xpacks.llm import parsers
#
# vector_server = VectorStoreServer(
#     *data_sources,
#     parser=parsers.ParseUnstructured(),
#     embedder=embeddings_model,
#     splitter=text_splitter,
# )
# ```

# %% [markdown]
# ### Configuring the cache
#
# The Pathway vectorizing pipeline comes with an embeddings cache:
# ```python
# vector_server.run_server(..., with_cache=True)
# ```
#
# The default cache configuration is the locally hosted disk cache, stored in the `./Cache` directory. However, it can be customized by explicitly specifying the caching backend chosen among several persistent backend [options](/developers/api-docs/persistence-api#pathway.persistence.Backend).


# %% [markdown]
# ### Running in production
#
# A production deployment will typically run the server in a separate process. We recommend running the Pathway data indexing pipeline in a container-based deployment environment like Docker or Kubernetes. For more info, see [Pathway's deployment guide](/developers/user-guide/deployment/docker-deployment/).
#
# ::shoutout-banner
# ---
# href: "https://discord.gg/pathway"
# icon: "ic:baseline-discord"
# ---
# #title
# Discuss tricks & tips for RAG
# #description
# Join our Discord community and dive into discussions on tricks and tips for mastering Retrieval Augmented Generation
# ::

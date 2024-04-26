# ---
# title: 'Private RAG with Adaptive Retrieval using Mistral, Ollama and Pathway'
# description: 'Adaptive RAG with fully local models'
# aside: true
# article:
#     thumbnail: '/assets/content/blog/local-adaptive-rag/local_adaptive.png'
#     tags: ['showcase', 'llm']
#     date: '2024-04-23'
#     related: ['/developers/showcases/adaptive-rag', '/developers/showcases/llamaindex-pathway']
# notebook_export_path: notebooks/showcases/mistral_adaptive_rag_question_answering.ipynb
# author: 'berke'
# keywords: ['LLM', 'RAG', 'Adaptive RAG', 'prompt engineering', 'explainability', 'mistral', 'ollama', 'private rag', 'local rag', 'ollama rag']
# ---

# # Private RAG with Adaptive Retrieval using Mistral, Ollama and Pathway

# Retrieval Augmented Generation (RAG) is a powerful way to answer questions based on your own, private, knowledge database.
# However, data security is key: sensitive information like R&D secrets, GDPR-protected data, and internal documents cannot be entrusted to third parties.
#
# Most of the existing RAG solutions rely on LLM APIs that send your data, at least a part of it, to the LLM provider.
# For example, if your RAG solution uses ChatGPT, you will have to send your data to OpenAI through OpenAI API.
#
# Fortunately, there is a solution to keep your data private: deploying a local LLM.
# By deploying everything locally, your data remains secure within own infrastructure.
# It eliminates the risk of sensitive information ever leaving your control.
#
# While this seems quite an engineering feat, don't worry Pathway provides you everything you need to make this as easy as possible.
#
# In this showcase, you will learn how to set up a private RAG pipeline with adaptive retrieval using Pathway, Mistral, and Ollama.
# The pipeline answers questions from the Stanford Question Answering Dataset [(SQUAD)](https://rajpurkar.github.io/SQuAD-explorer/) using a selection of Wikipedia pages from the same dataset as the context, split into paragraphs.
# **This RAG pipeline runs without any API access or any data leaving the local machine with Pathway**.
#
# ![Reference architecture](/assets/content/blog/local-adaptive-rag/local_adaptive.png)
#
# Our RAG app will use a Mistral 7B, locally deployed using Ollama. Mistral 7B is chosen for its performance and efficient size.
# The pipeline uses Pathway vector store & live document indexing pipeline with an open-source embedding model from the HuggingFace.
#
# Pathway brings support for real-time data synchronization pipelines out of the box, and possibility of secure private document handling with enterprise connectors for synchronizing Sharepoint and Google Drive incrementally.
# Here, Pathway's built-in vector index is used.
#
# If you are not familiar with the Pathway, refer to [overview of Pathway's LLM xpack](https://pathway.com/developers/user-guide/llm-xpack/overview) and [its documentation](https://pathway.com/developers/api-docs/pathway-xpacks-llm/llms).
# This article is an extension of our previous [Adaptive RAG showcase](/developers/showcases/adaptive-rag) on how to improve RAG accuracy while maintaining cost and time efficiency.
# The main difference is that showcase was using OpenAI LLM and `text-embedding-ada-002 embedder` while in the following the pipeline will use locally-hosted LLMs and embedders.
#
# You will explore how to use Pathway to:
# - load and index documents
# - connect to our local LLM
# - prompt our LLM with relevant context, and adaptively add more documents as needed
# - combine everything, and orchestrate the RAG pipeline.


# ## What is Private RAG and Why Do You Need It?
#
# Most of the RAG applications require you to send your documents & data to propriety APIs. This is a concern for most organizations as data privacy with sensitive documents becomes an issue. Generally, you need to send your documents during the indexing and LLM question-answering stages.
#
# To tackle this, you can use a **private RAG: locally deployed LLMs and embedders in your RAG pipeline**.
# You don't need to go to proprietary APIs with the help of Ollama, HuggingFace and Pathway.
# Everything is staying local on your machine.
#
# ### Why use Local LLMs?
#
# There are several advantages to using local LLMs (Large Language Models) over cloud-based APIs:
# - **Data Privacy**: Local LLMs keep your sensitive data on your machines.
# - **Accuracy**: Cloud-based LLM accuracy can sometimes regress, whereas you can fine-tune local models for better performance.
# - **Customization**: Local LLMs allow for fine-tuning to achieve specific behaviors or domain adaptation.
#
# ### Mistral and Ollama for privacy
#
# **Mistral 7B** is publicly available LLM model release by [Mistral AI](https://mistral.ai/).
# Its (relative) small size of 7.3B parameters and impressive performances make Mistral 7B a perfect candidate for a local deployment.
#
# The pipeline relies on **Ollama** to deploy the Mistral 7B model.
# [Ollama](https://ollama.com/) is a tool to create, manage, and run LLM models.
# Ollama is used to download and configure locally the Mistral 7B model.
#
# ## Pathway Adaptive RAG
#
# Standard RAG process first retrieves a fixed number of documents to answer the question and then build a personalized prompt using the documents to allow the LLM to answer the question with a relevant context.
# The number of retrieved document is a tradeoff: a large number of documents increases the ability of the LLM to provide a correct answer but also increases LLM costs.
#
# Pathway Adaptive RAG improves this tradeoff by adapting the number of retrieved documents depending on the difficulty of the question.
# First, a small number of context documents is retrieved and if the LLM refuses to answer, repeat the question with a larger prompt.
# This process is repeated until the LLM got a answer.
#
# To learn more about how we do it, and the observed benefits, please see the original work [here](/developers/showcases/adaptive-rag).


# ## Private RAG with Pathway Adaptive RAG
#
# This section provides a step-by-step guide on how to set up Private RAG with Adaptive Retrieval using Pathway, a framework for building LLM applications. The guide covers:
# 1. **Installation**: Installing Pathway and required libraries.
# 2. **Data Loading**: Loading documents for which answer retrieval will be performed.
# 3. **Embedding Model Selection**: Choosing an open-source embedding model from Hugging Face.
# 4. **Local LLM Deployment**: Instructions on deploying a local LLM using Ollama, a lightweight container runtime.
# 5. **LLM Initialization**: Setting up the LLM instance to interact with the local model.
# 6. **Vector Document Index Creation**: Building an index for efficient document retrieval using the embedding model.
# 7. **Adaptive RAG Table Creation**: Defining the parameters for the Adaptive RAG strategy.
# 8. **Pipeline Execution**: Running the Private RAG pipeline with your data.
#
# ### 1. Installation
#
# You install Pathway into a Python 3.10+ Linux runtime with a simple pip command:


# %%capture --no-display
# !pip install -U --prefer-binary pathway
# !pip install "litellm>=1.35"


# ### 2. Data Loading
# Download `adaptive-rag-contexts.jsonl` with ~1000 contexts from the SQUAD dataset


# !wget -q -nc https://public-pathway-releases.s3.eu-central-1.amazonaws.com/data/adaptive-rag-contexts.jsonl


import pandas as pd
import pathway as pw
from pathway.stdlib.indexing import VectorDocumentIndex
from pathway.xpacks.llm import embedders
from pathway.xpacks.llm.llms import LiteLLMChat
from pathway.xpacks.llm.question_answering import (
    answer_with_geometric_rag_strategy_from_index,
)


# ### 3. Embedding Model Selection
#
# For the embeddings, we provide a few selected models that can be used to replicate the work.
# In case you have access to limited computation, and want to use an embedder over the API, we also provide a snippet on how to use Mistral embeddings below.


# +
# embedder = LiteLLMEmbedder(
#     capacity = 5,
#     retry_strategy = pw.udfs.ExponentialBackoffRetryStrategy(max_retries=4, initial_delay=1200),
#     model = "mistral/mistral-embed",
#     api_key=<mistral_api_key>,
# )
# -


# Here are a few embedding models that have performed well in our tests
# These models were selected from the [MTEB Leaderboard](https://huggingface.co/spaces/mteb/leaderboard).
#
# We use `pathway.xpacks.llm.embedders` module to load open-source embedding models from the HuggingFace.


# +
# large_model = "mixedbread-ai/mxbai-embed-large-v1"
# medium_model = "avsolatorio/GIST-Embedding-v0"
small_model = "avsolatorio/GIST-small-Embedding-v0"

embedder = embedders.SentenceTransformerEmbedder(
    small_model, call_kwargs={"show_progress_bar": False}
)  # disable verbose logs
embedding_dimension: int = len(
    embedder.__wrapped__(".")
)  # call the model once to get the embedding_dim
print("Embedding dimension:", embedding_dimension)


# +
# Load documents in which answers will be searched
class InputSchema(pw.Schema):
    doc: str


documents = pw.io.fs.read(
    "adaptive-rag-contexts.jsonl",
    format="json",
    schema=InputSchema,
    json_field_paths={"doc": "/context"},
    mode="static",
)

# +
# check if documents are correctly loaded
# documents
# -

# Create a table with example questions
df = pd.DataFrame(
    {
        "query": [
            "When it is burned what does hydrogen make?",
            "What was undertaken in 2010 to determine where dogs originated from?",
            # "What did Arnold's journey into politics look like?",
        ]
    }
)
query = pw.debug.table_from_pandas(df)


# #### 4. Local LLM Deployement
# Due to its size, performances, and ease of use, we decided to run the `Mistral 7B` on `Ollama`
#
# In order to run local LLM, refer to these steps:
# - Download Ollama from [ollama.com/download](https://ollama.com/download)
# - In your terminal, run `ollama serve`
# - In another terminal, run `ollama run mistral`
#
# You can now test it with the following:
#
# ```bash
# curl -X POST http://localhost:11434/api/generate -d '{
#   "model": "mistral",
#   "prompt":"Here is a story about llamas eating grass"
#  }'
# ```


# ### 5. LLM Initialization
#
# Initialize the LLM instance that will call our local model.

# +
# we specifically instruct LLM to return json. in this format, LLM follows the instructions more strictly
# this is not needed in gpt-3.5-turbo and mistral-large, but necessary in mistral-7b

model = LiteLLMChat(
    model="ollama/mistral",
    temperature=0,
    top_p=1,
    api_base="http://localhost:11434",  # local deployment
    format="json",  # only available in Ollama local deploy, not usable in Mistral API
)
# -


# ### 6. Vector Document Index Creation
# Create the index with documents and embedding model


index = VectorDocumentIndex(
    documents.doc, documents, embedder, n_dimensions=embedding_dimension
)


# ### 7. Adaptive RAG Table Creation
# Create the adaptive rag table with created index, LLM, embedder, documents, and hyperparameters:


result = query.select(
    question=query.query,
    result=answer_with_geometric_rag_strategy_from_index(
        query.query,
        index,
        documents.doc,
        model,
        n_starting_documents=2,
        factor=2,
        max_iterations=4,
        strict_prompt=True,  # needed for open source models, instructs LLM to give JSON output strictly
    ),
)


# ### 8. Pipeline Execution
# You can run the pipeline and print the results table with `pw.debug.compute_and_print`:


# _MD_COMMENT_START_
print(
    """Hydrogen makes water when burned [2].

Extensive genetic studies were conducted during the 2010s which indicated that dogs diverged from an extinct wolf-like canid in Eurasia around 40,000 years ago."""
)
# _MD_COMMENT_END_
# _MD_SHOW_pw.debug.compute_and_print(result)


# # Going to Production

# Now you have an fully private adaptive RAG. Not only the adaptive retrieval improve the accuracy of the RAG, but by using a local LLM model, all your data remain safe on your system.
# You can go further by building and deploying your RAG application in production with Pathway, including serving the endpoints. 
#
# To learn more, check out our [question answering demo](https://github.com/pathwaycom/llm-app/tree/main/examples/pipelines/demo-question-answering) for brief introduction on how to use Pathway to build a full RAG application.

# # Key Takeaways

# While [Adaptive RAG](https://pathway.com/developers/showcases/adaptive-rag) provides a good balance between cost-efficiency and accuracy, and makes improvements over the naive RAG, it was relying -as most of the RAG solutions- on third parties API.
# In this showcase, you have learned how to do design a **fully local and private adaptive RAG** setup, including local embedder and LLM.
# Using Pathway, only small modifications to the original prompts and parameters were needed to provide privacy to the RAG pipeline.
#
# To ensure stricter adherence to instructions, especially with smaller open-source LLMs, we employed the `json` mode for LLM outputs. This approach enhances control over the model's response, leading to more predictable behavior.
#
# **This private RAG setup can be run entirely locally with open-source LLMs, making it ideal for organizations with sensitive data or who have already deployed local LLMs.**

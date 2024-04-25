# ---
# title: 'Private RAG with Adaptive Retrieval using Mistral, Ollama and Pathway'
# description: 'Adaptive RAG with fully local models'
# aside: true
# article:
#     thumbnail: '/assets/content/blog/local-adaptive-rag/local_adaptive.png'
#     tags: ['showcase', 'llm']
#     date: '2024-04-23'
#     related: false
# notebook_export_path: notebooks/showcases/mistral_adaptive_rag_question_answering.ipynb
# author: 'berke'
# keywords: ['LLM', 'RAG', 'Adaptive RAG', 'prompt engineering', 'explainability', 'mistral', 'ollama', 'private rag', 'local rag', 'ollama rag']
# ---

# # Private RAG with Adaptive Retrieval using Mistral, Ollama and Pathway

# In our previous [Adaptive RAG](https://pathway.com/developers/showcases/adaptive-rag) showcase, we demonstrated how to improve RAG accuracy while maintaining cost and time efficiency. We achieved this using OpenAI LLM and `text-embedding-ada-002` embedder. This showcase replicates the same technique with locally hosted LLMs and embedders.
#
# Pathway brings support for real-time data synchronization pipelines out of the box, and possibility of secure private document handling with enterprise connectors for synchronizing Sharepoint and Google Drive incrementally. Here, we use Pathway's built-in vector index.
#
# In this showcase, we explore how to set up a private RAG pipeline with adaptive retrieval using Pathway, Mistral, and Ollama.
# Our pipeline answers questions from the Stanford Question Answering Dataset [(SQUAD)](https://rajpurkar.github.io/SQuAD-explorer/) using a selection of Wikipedia pages from the same dataset as the context, split into paragraphs.
#
# We use a locally deployed Mistral 7B, chosen for its performance and efficient size.
#
# ![Reference architecture](/assets/content/blog/local-adaptive-rag/local_adaptive.png)
#
# Many organizations are concerned with data privacy while building LLM or RAG applications using proprietary models over the API. When we released the Adaptive RAG showcase, we got multiple requests for redoing it in a fully private setup, this is the key motivation behind this showcase. Here, we explore how to build a RAG application without any API access or any data leaving the local machine with Pathway.
#
#
# We set up our vector store & live document indexing pipeline using Pathway with an open-source embedding model from the HuggingFace.
#
# If you are not familiar with the Pathway, refer to [overview of Pathway's LLM xpack](https://pathway.com/developers/user-guide/llm-xpack/overview) and [its documentation](https://pathway.com/developers/api-docs/pathway-xpacks-llm/llms).
#
# We explore how to use Pathway to;
# - load and index documents
# - connect to our local LLM
# - prompt our LLM with relevant context, and adaptively add more documents as needed
# - combine everything, and orchestrate the RAG pipeline


# ## Introduction
#
# Retrieval Augmented Generation (RAG) allows Large Language Models (LLMs) to answer questions based on knowledge not present in the original training set. At [Pathway](https://pathway.com/) we use RAG to build [document intelligence solutions](/solutions/rag-pipelines) that answer questions based on private document collections, such as a repository of legal contracts. We are constantly working on improving the accuracy and explainability of our models while keeping the costs low. In this blog post, we share a trick that helped us reach those goals.
#
# A typical RAG Question Answering procedure works in two steps. First, the question is analyzed and a number of relevant documents are retrieved from a database, typically using a similarity search inside a vector space created by a neural embedding model. Second, retrieved documents are pasted, along with the original question, into a prompt which is sent to the LLM. Thus, the LLM answers the question within a relevant context.
#
# Practical implementations of the RAG procedure need to specify the number of documents put into the prompt. A large number of documents increases the ability of the LLM to provide a correct answer but also increases LLM costs, which typically grow linearly with the length of the provided prompt. The prompt size also influences model explainability: retrieved context documents explain and justify the answer of the LLM and the fewer context documents are needed, the easier it is to verify and trust model outputs.
#
# Thus the context size, given by the number of considered documents in a RAG setup, must be chosen to balance costs, desired answer quality, and explainability. However, can we do better than using the same context size regardless of the question to be answered? Intuitively, not all questions are equally hard and some can be answered using a small number of supporting documents, while some may require the LLM to consult a larger prompt. We can confirm this by running a question-answering experiment.
#
# ## Adaptive RAG
# We can use the model’s refusal to answer questions as a form of model introspection which enables an adaptive RAG question-answering strategy:
#
# ::card
# #title
# Adaptive Rag Idea
# #description
# Ask the LLM with a small number of context documents. If it refuses to answer, repeat the question with a larger prompt.
# ::
#
# This RAG scheme adapts to the hardness of the question and the quality of the retrieved supporting documents using the feedback from the LLM - for most documents a single LLM call with a small prompt is sufficient, and there is no need for an auxiliary LLM calls to e.g. guess an initial supporting document count for a question. However, a fraction of questions will require re-asking or rere-asking the LLM.
#
# To solve it, we can use a geometric series to expand the prompt with retrieved documents.
#
# To learn more about how we do it, and the observed benefits, please see the original work [here.](https://pathway.com/developers/showcases/adaptive-rag)
#
# ## What is (local) private RAG, do I need it?
#
# Most of the RAG applications require you to send your documents & data to propriety APIs. This is a concern for most organizations as data privacy with sensitive documents becomes an issue. Generally, you need to send your documents during the indexing and LLM question-answering stages.
#
# To tackle this, you can use locally deployed LLMs and embedders in your RAG pipeline. We eliminate the need to go to proprietary APIs with the help of Ollama, HuggingFace and Pathway.
# Everything is staying local on your machine.
#
# ### Why use local LLMs?
#
# There are several reasons why you may want to use local or open-source LLMs over propriety ones.
# - Building RAG applications require documents to be sent to the LLM, this may be an issue in some organizations with sensitive data.
# - We have observed that some LLMs that are served over the API are regressing in terms of accuracy, with local models, this won't be an issue.
# - It is possible to fine-tune local LLMs to behave in certain ways or achieve domain adaptation.


# ## Using Adaptive RAG locally with Pathway
#
# In the cell below, we install Pathway into a Python 3.10+ Linux runtime.


# %%capture --no-display
# !pip install -U --prefer-binary pathway
# !pip install "litellm>=1.35"


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


# #### Deploying and using a local LLM
# Due to its popularity and ease of use, we decided to run the `Mistral 7B` on `Ollama`
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


# Create the index with documents and embedding model


index = VectorDocumentIndex(
    documents.doc, documents, embedder, n_dimensions=embedding_dimension
)


# Create the adaptive rag table with created index, LLM, embedder, documents, and hyperparameters


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


# Run the pipeline


# _MD_COMMENT_START_
print(
    """Hydrogen makes water when burned [2].

Extensive genetic studies were conducted during the 2010s which indicated that dogs diverged from an extinct wolf-like canid in Eurasia around 40,000 years ago."""
)
# _MD_COMMENT_END_
# _MD_SHOW_pw.debug.compute_and_print(result)


# # Going to production

# This showcase demonstrates how you can improve the core RAG logic with adaptive retrieval. Pathway also enables you to easily build & deploy your RAG application in production, including serving the endpoints. 
#
# To learn more, check out our [question answering demo](https://github.com/pathwaycom/llm-app/tree/main/examples/pipelines/demo-question-answering) for brief introduction on how to use Pathway to build a full RAG application.

# # Conclusion

# In the prior showcase, we have shown that adaptive RAG provides a good balance between cost-efficiency and accuracy, and makes improvements over the naive RAG.
# In this showcase, we demonstrate how to replicate the [Adaptive RAG](https://pathway.com/developers/showcases/adaptive-rag) strategy with a fully local setup, including local embedder and LLM. To achieve this, we had to make some modifications to the original prompts, and the LLM inference parameters.
#
# To ensure stricter adherence to instructions, especially with smaller open-source LLMs, we employed the `json` mode for LLM outputs. This approach enhances control over the model's response, leading to more predictable behavior.
#
#
# Notably, this setup can be run entirely locally with open-source LLMs, making it ideal for organizations with sensitive data or who have already deployed local LLMs.

# ---
# title: 'Langchain and Pathway: RAG Apps with always-up-to-date knowledge'
# description: ''
# article:
#     date: '2024-05-18'
#     thumbnail: '/assets/content/showcases/vectorstore/Langchain-Pathway.png'
#     tags: ['showcase', 'llm']
# author: 'szymon'
# notebook_export_path: notebooks/showcases/langchain-integration.ipynb
# keywords: ['LLM', 'RAG', 'GPT', 'OpenAI', 'LangChain', 'notebook']
# ---

# # Langchain and Pathway: RAG Apps with always-up-to-date knowledge
#
# <!-- Canva link for thumbnail: https://www.canva.com/design/DAGFNCj-Np8/P3FKop5Gv4r9MDCrgecDPQ/edit -->
#
# You can now use Pathway in your RAG applications which enables always up-to-date knowledge from your documents to LLMs with Langchaing integration.
#
# Pathway is now available on [Langchain](https://python.langchain.com/docs/integrations/vectorstores/pathway/), a framework for developing applications powered by large language models (LLMs).
# You can now query Pathway and access up-to-date documents for your RAG applications from LangChain using [PathwayVectorClient](https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.pathway.PathwayVectorClient.html).
#
# With this new integration, you will be able to use Pathway Vector Store natively in LangChain. In this guide, you will have a quick dive into Pathway + LangChain to learn how to create a simple, yet powerful RAG solution.

# ## Prerequisites
#
# To work with LangChain you need to install `langchain` package, as it is not a dependence of Pathway. In the example in this guide you will also use `OpenAIEmbeddings` class for which you need `langchain_openai` package.

# _MD_SHOW_!pip install langchain
# _MD_SHOW_!pip install langchain_community
# _MD_SHOW_!pip install langchain_openai
# _MD_COMMENT_START_
if 1:  # group to prevent isort messing up
    import json
    import os

    from common.shadows import fs

    os.environ["OPENAI_API_KEY"] = json.loads(
        fs.open("vault://kv.v2:deployments@/legal_rag_demo").read()
    )["OPENAI_KEY"]
# _MD_COMMENT_END_

# ## Using LangChain components in Pathway Vector Store
# When using Pathway [`VectorStoreServer`](/developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer), you can use LangChain embedder and splitter for processing documents. To do that, use [`from_langchain_components`](/developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreServer.from_langchain_components) class method.
#
# To start, you need to create a folder Pathway will listen to. Feel free to skip this if you already have a folder on which you want to build your RAG application. You can also use Google Drive, Sharepoint, or any other source from [pathway-io](/developers/api-docs/pathway-io).

# !mkdir -p 'data/'

# To run this example you also need to set OpenAI API key, or change the embedder.

# +
import os
import getpass

# Set OpenAI API Key
if "OPENAI_API_KEY" in os.environ:
    api_key = os.environ["OPENAI_API_KEY"]
else:
    api_key = getpass.getpass("OpenAI API Key:")
# -

# To run the server use Pathway filesystem connector to read files from the `data` folder.

# +
import pathway as pw
from pathway.xpacks.llm.vector_store import VectorStoreServer
from langchain_openai import OpenAIEmbeddings
from langchain.text_splitter import CharacterTextSplitter

data = pw.io.fs.read(
    "./data",
    format="binary",
    mode="streaming",
    with_metadata=True,
)
# -

# And then pass them to the server, which will split them using `CharacterTextSplitter` and embed them using `OpenAIEmbeddings`, both from LangChain.

# +
embeddings = OpenAIEmbeddings(api_key=api_key)
splitter = CharacterTextSplitter()

host = "127.0.0.1"
port = 8666

server = VectorStoreServer.from_langchain_components(
    data, embedder=embeddings, splitter=splitter
)
# _MD_SHOW_server.run_server(host, port=port, with_cache=True, cache_backend=pw.persistence.Backend.filesystem("./Cache"), threaded=True)
# -

# The server is now running and ready for querying with a [`VectorStoreServer`](/developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreClient) ot with a `PathwayVectorClient` from `langchain-community` described in the next Section.

# ## Using Pathway as a Vector Store in LangChain pipelines
#
# Once you have a `VectorStoreServer` running you can access it from LangChain pipeline by using [PathwayVectorClient](https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.pathway.PathwayVectorClient.html).
#
# To do that you need to provide either the `url` or `host` and `port` of the running `VectorStoreServer`. In the code example below, you will connect to the `VectorStoreServer` defined in the previous Section, so make sure it's running before making queries. Alternatively, you can also use a publicly available [demo pipeline](https://pathway.com/solutions/rag-pipelines#try-it-out) to test your client. Its REST API you can access at `https://demo-document-indexing.pathway.stream`. This demo ingests documents from [Google Drive](https://drive.google.com/drive/u/0/folders/1cULDv2OaViJBmOfG5WB0oWcgayNrGtVs) and [Sharepoint](https://navalgo.sharepoint.com/sites/ConnectorSandbox/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FConnectorSandbox%2FShared%20Documents%2FIndexerSandbox&p=true&ga=1).

# +
from langchain_community.vectorstores import PathwayVectorClient

client = PathwayVectorClient(host=host, port=port)
# -

query = "What is Pathway?"
# _MD_SHOW_docs = client.similarity_search(query)
# _MD_SHOW_print(docs)

# As you can see, the LLM cannot respond clearly as it lacks current knowledge, but this is where Pathway shines. Add new data to the folder Pathway is listening to, then ask our agent again to see how it responds.
# To do that, you can download the repo readme of Pathway into our data folder:

# !wget 'https://raw.githubusercontent.com/pathwaycom/pathway/main/README.md' -O 'data/pathway_readme.md' -q -nc

# Try again to query with the new data:

# +
# _MD_SHOW_docs = client.similarity_search(query)
# _MD_SHOW_print(docs)
# -

# ### RAG pipeline in LangChain

# The next step is to write a chain in LangChain. The next example implements a simple RAG, that given a question, retrieves documents from Pathway Vector Store. These are then used as a context for the given question in a prompt sent to the OpenAI chat.

# +
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI

retriever = client.as_retriever()

template = """
You are smart assistant that helps users with their documents on Google Drive and Sharepoint.
Given a context, respond to the user question.
CONTEXT:
{context}
QUESTION: {question}
YOUR ANSWER:"""

prompt = ChatPromptTemplate.from_template(template)
llm = ChatOpenAI()
chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)
# -

# Now you have a RAG chain written in LangChain that uses Pathway as its Vector Store. Test it by asking some question.

# +
# _MD_SHOW_chain.invoke("What is Pathway?")
# -

# ### Vector Store statistics
#
# Just like [`VectorStoreClient`](/developers/api-docs/pathway-xpacks-llm/vectorstore#pathway.xpacks.llm.vector_store.VectorStoreClient) from the Pathway LLM xpack, `PathwayVectorClient` gives you two methods for getting information about indexed documents.
#
# The first one is [`get_vectorstore_statistics`](https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.pathway.PathwayVectorClient.html#langchain_community.vectorstores.pathway.PathwayVectorClient.get_vectorstore_statistics) and gives essential statistics on the state of the vector store, like the number of indexed files and the timestamp of the last updated one. The second one is [`get_input_files`](https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.pathway.PathwayVectorClient.html#langchain_community.vectorstores.pathway.PathwayVectorClient.get_input_files), which gets the list of indexed files along with the associated metadata.

# +
# _MD_SHOW_print(client.get_vectorstore_statistics())
# _MD_SHOW_print(client.get_input_files())

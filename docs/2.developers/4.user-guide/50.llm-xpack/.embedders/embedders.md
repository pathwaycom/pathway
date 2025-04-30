---
title: 'Embedders'
description: 'Embedders available through the Pathway xpack'
date: '2025-02-04'
thumbnail: ''
tags: ['tutorial', 'embedder']
keywords: ['LLM', 'GPT', 'OpenAI', 'Gemini', 'LiteLLM', 'Embedder']
---

# Embedders

When storing a document in a vector store, you compute the embedding vector for the text and store the vector with a reference to the original document. You can then compute the embedding of a query and find the embedded documents closest to the query.

The following embedding wrappers are available through the Pathway xpack:

- [`OpenAIEmbedder`](#openaiembedder) - Embed text with any of OpenAI's embedding models
- [`LiteLLMEmbedder`](#litellmembedder) - Embed text with any model available through LiteLLM
- [`SentenceTransformersEmbedder`](#sentencetransformerembedder) - Embed text with any model available through SentenceTransformer (aka. SBERT) maintained by Hugging Face
- [`GeminiEmbedder`](#gemeniembedder) - Embed text with any of Google's available embedding models

## OpenAIEmbedder
The default model for [`OpenAIEmbedder`](/developers/api-docs/pathway-xpacks-llm/embedders/#pathway.xpacks.llm.embedders.OpenAIEmbedder) is `text-embedding-3-small`.

::if{path="/llm-xpack/"}
```python
import os
import pathway as pw
from pathway.xpacks.llm.parsers import UnstructuredParser
from pathway.xpacks.llm.embedders import OpenAIEmbedder

files = pw.io.fs.read(
    os.environ.get("DATA_DIR"),
    mode="streaming",
    format="binary",
    autocommit_duration_ms=50,
)

# Parse the documents in the specified directory
parser = UnstructuredParser(chunking_mode="paged")
documents = files.select(elements=parser(pw.this.data))
documents = documents.flatten(pw.this.elements)  # flatten list into multiple rows
documents = documents.select(text=pw.this.elements[0], metadata=pw.this.elements[1])

# Embed each page of the document
embedder = OpenAIEmbedder(api_key=os.environ["OPENAI_API_KEY"])
embeddings = documents.select(embedding=embedder(pw.this.text))
```
::
::if{path="/templates/"}
```yaml
embedder: !pw.xpacks.llm.embedders.OpenAIEmbedder
  model: "text-embedding-3-small"
```
::

## LiteLLMEmbedder
The model for [`LiteLLMEmbedder`](/developers/api-docs/pathway-xpacks-llm/embedders/#pathway.xpacks.llm.embedders.LiteLLMEmbedder) has to be specified during initialization. No default is provided.

::if{path="/llm-xpack/"}
```python
from pathway.xpacks.llm import embedders

embedder = embedders.LiteLLMEmbedder(
    model="text-embedding-3-small", api_key=API_KEY
)
# Create a table with one column for the text to embed
t = pw.debug.table_from_markdown(
    """
text_column
Here is some text
"""
)
res = t.select(ret=embedder(pw.this.text_column))
```
::
::if{path="/templates/"}
```yaml
embedder: !pw.xpacks.llm.embedders.LiteLLMEmbedder
  model: "text-embedding-3-small"
```
::

## SentenceTransformerEmbedder
This [`SentenceTransformerEmbedder`](/developers/api-docs/pathway-xpacks-llm/embedders/#pathway.xpacks.llm.embedders.SentenceTransformerEmbedder) embedder allows you to use the models from the Hugging Face Sentence Transformer models.

The model is specified during initialization. Here is a list of [`available models`](https://www.sbert.net/docs/sentence_transformer/pretrained_models.html).

::if{path="/llm-xpack/"}
```python
import pathway as pw
from pathway.xpacks.llm import embedders

embedder = embedders.SentenceTransformerEmbedder(model="intfloat/e5-large-v2")

# Create a table with text to embed
t = pw.debug.table_from_markdown('''
txt
Some text to embed
''')

# Extract the embedded text
t.select(ret=embedder(pw.this.txt))
```
::
::if{path="/templates/"}
```yaml
embedder: !pw.xpacks.llm.embedders.SentenceTransformerEmbedder
  model: "intfloat/e5-large-v2"
```
::

## GeminiEmbedder
[`GeminiEmbedder`](/developers/api-docs/pathway-xpacks-llm/embedders/#pathway.xpacks.llm.embedders.GeminiEmbedder) is the embedder for Google's Gemini Embedding Services. Available models can be found [`here`](https://ai.google.dev/gemini-api/docs/models/gemini#text-embedding-and-embedding).

::if{path="/llm-xpack/"}
```python
import pathway as pw
from pathway.xpacks.llm import embedders

embedder = embedders.GeminiEmbedder(model="models/text-embedding-004")

# Create a table with a column for the text to embed
t = pw.debug.table_from_markdown('''
txt
Some text to embed
''')

t.select(ret=embedder(pw.this.txt))
```
::
::if{path="/templates/"}
```yaml
embedder: !pw.xpacks.llm.embedders.GeminiEmbedder
  model: "models/text-embedding-004"
```
::

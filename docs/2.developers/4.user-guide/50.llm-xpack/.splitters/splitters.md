---
title: 'Chunking text'
description: 'Splitters available through the Pathway xpack'
date: '2025-02-04'
thumbnail: ''
tags: ['splitters', 'chunking']
keywords: ['parsers', 'chunking']
---

# Chunking

Embedding entire documents as a single vector often leads to poor retrieval performance. This happens because the model is forced to compress all the document's information into a single representation, making it difficult to capture granular details. As a result, important context may be lost, and retrieval effectiveness decreases.

There a several strategies how to best chunk a document. A simple approach might involve slicing the text every n characters. However, this can split sentences or phrases awkwardly, resulting in incomplete or distorted chunks. Additionally, token counts vary (a token might be a character, word, or punctuation), making it hard to manage consistent chunk sizes with character-based splitting.

A better method is to chunk the text by tokens, ensuring each chunk makes sense and aligns with sentence or paragraph boundaries. Token-based chunking is typically done at logical breakpoints, such as periods, commas, or newlines.

## TokenCountSplitter
Pathway offers a [`TokenCountSplitter`](/developers/api-docs/pathway-xpacks-llm/splitters#pathway.xpacks.llm.splitters.TokenCountSplitter) for token-based chunking. Here's how to use it:

::if{path="/llm-xpack/"}
```python
from pathway.xpacks.llm.splitters import TokenCountSplitter

text_splitter = TokenCountSplitter(
    min_tokens=100,
    max_tokens=500,
    encoding_name="cl100k_base"
)
```
::
::if{path="/ai-pipelines/"}
```yaml
splitter: pw.xpacks.llm.splitters.TokenCountSplitter
  min_tokes: 100
  max_tokens: 500
  encoding_name: "cl100k_base"
```
::

This configuration creates chunks of 100–500 tokens using the `cl100k_base` tokenizer, compatible with OpenAI's embedding models.

For more on token encodings, refer to [OpenAI's tiktoken guide](https://cookbook.openai.com/examples/how_to_count_tokens_with_tiktoken#encodings).


## RecursiveSplitter

Another kind of splitter that you can use to chunk your documents in [`RecursiveSplitter`](/developers/api-docs/pathway-xpacks-llm/splitters#pathway.xpacks.llm.splitters.RecursiveSplitter). 
It functions similarly to `TokenCountSplitter` in that it measures chunk length based on the number of tokens required to encode the text. 
However, the way it determines split points differs.
`RecursiveSplitter` processes a document by iterating through a list of ordered `separators` (configurable in the constructor), starting with the most granular and moving to the least. For example, it may first attempt to split using `\n\n` and, if necessary, fall back to splitting at periods (`.`).
The splitter continues this process until all chunks are smaller than `chunk_size`.
Additionally, you can introduce overlapping chunks by setting the `chunk_overlap` parameter. This is particularly useful if you want to capture different contexts in your chunks. However, keep in mind that enabling overlap increases the total number of chunks retrieved, which could impact performance.

::if{path="/llm-xpack/"}
```python
splitter = RecursiveSplitter(
    chunk_size=400,
    chunk_overlap=200,
    separators=["\n#", "\n##", "\n\n", "\n"],  # separators for markdown documents
    model_name="gpt-4o-mini",
)
```
::
::if{path="/ai-pipelines/"}
```yaml
splitter: pw.xpacks.llm.splitters.RecursiveSplitter
  chunk_size: 400
  chunk_overlap: 200
  separators:
    - "\n#
    - "\n##"
    - "\n\n"
    - "\n"
  model_name: "gpt-4o-mini"
```
::

---
title: 'Rerankers'
description: 'Rerankers available through the Pathway xpack'
date: '2025-02-04'
thumbnail: ''
tags: ['tutorial', 'reranker']
keywords: ['LLM', 'Reranker']
---

# Rerankers 
Rerankers can be used to have the model rank the relevance of documents against a query or text.

In RAG systems, initial sparse retrieval is often based on cosine similarity. This will likely result in some documents not being relevant to the query. This happens because retrieval is typically based on vector representations that condense a passage's meaning into a single embedding, which can overlook important nuances. While this approach is fast, it can also be inaccurate. To improve the quality of retrieved documents, it is common to refine the initial set and select only the most relevant ones using rerankers.
Rerankers help the model reassess and prioritize documents based on their relevance to the query. This is usually done by presenting the model with `(query, document)` pairs and evaluating whether the given `document` contributes to answering the `query`.

Pathway xpack provides the following rerankers:
- [`LLMReranker`](#llmreranker) - Have an LLM rerank the documents
- [`CrossEncoderReranker`](#crossencoderreranker) - Rerank with a CrossEncoder from SBERT / SentenceTransformers)
- [`EncoderReranker`](#encoderreranker): Rerank with SentenceTransformers EncoderRerank (measure similarity)

## LLMReranker
The [`LLMReranker`](/developers/api-docs/pathway-xpacks-llm/rerankers#pathway.xpacks.llm.rerankers.LLMReranker) asks the provided LLM to evaluate the relevance of a query against the provided documents on a scale 1-5.

```python
from pathway.xpacks.llm import rerankers
from pathway.xpacks.llm import llms
import pandas as pd

docs = [
    {"text": "John drinks coffee"},
    {"text": "Someone drinks tea"},
    {"text": "Nobody drinks coca-cola"},
]

query = "What does John drink?"

df = pd.DataFrame({"docs": docs, "prompt": query})

chat = llms.OpenAIChat(model="gpt-4o-mini", api_key=API_KEY, response_format="{'type': 'json_object'}")
reranker = rerankers.LLMReranker(llm=chat)

input = pw.debug.table_from_pandas(df)
res = input.select(rank=reranker(pw.this.docs["text"], pw.this.prompt))
```

## CrossEncoderReranker
The [`CrossEncoderReranker`](/developers/api-docs/pathway-xpacks-llm/rerankers#pathway.xpacks.llm.rerankers.CrossEncoderReranker) works on text-pairs and computes a score 0..1 (or the logits if the activation function is not passed). The score determines how relevant the document is to the query.
More information can be found [`here`](https://www.sbert.net/docs/cross_encoder/pretrained_models.html).


```python
from pathway.xpacks.llm import rerankers
import pandas as pd
import torch

docs = [
    {"text": "John drinks coffee"},
    {"text": "Someone drinks tea"},
    {"text": "Nobody drinks coca-cola"},
]

query = "What does John drink?"

df = pd.DataFrame({"docs": docs, "prompt": query})

reranker = rerankers.CrossEncoderReranker(
    model_name="cross-encoder/ms-marco-MiniLM-L-6-v2",
    default_activation_function=torch.nn.Sigmoid(), # Make outputs between 0..1
)

input = pw.debug.table_from_pandas(df)
res = input.select(
    rank=reranker(pw.this.docs["text"], pw.this.prompt), text=pw.this.docs["text"]
)
pw.debug.compute_and_print(res)
```

## EncoderReranker
The [`EncoderReranker`](/developers/api-docs/pathway-xpacks-llm/rerankers#pathway.xpacks.llm.rerankers.EncoderReranker) computes the relevance of the query to the supplied documents using the [`SentenceTransformer encoders`](https://www.sbert.net/docs/sentence_transformer/pretrained_models.html).

```python
from pathway.xpacks.llm import rerankers
import pandas as pd

docs = [
    {"text": "John drinks coffee"},
    {"text": "Someone drinks tea"},
    {"text": "Nobody drinks coca-cola"},
]

query = "What does John drink?"

df = pd.DataFrame({"docs": docs, "prompt": query})

reranker = rerankers.EncoderReranker(
    model_name="all-mpnet-base-v2",
)

input = pw.debug.table_from_pandas(df)
res = input.select(
    rank=reranker(pw.this.docs["text"], pw.this.prompt), text=pw.this.docs["text"]
)
```

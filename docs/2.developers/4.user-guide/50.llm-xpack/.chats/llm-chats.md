---
title: 'LLM Chats'
description: 'LLM Wrappers available through Pathway xpack'
date: '2025-01-30'
thumbnail: ''
tags: ['tutorial', 'LLM', 'LLM Wrappers', 'LLM Chats']
keywords: ['LLM', 'GPT', 'OpenAI', 'Gemini', 'LiteLLM', 'Wrapper']
---

# LLM Chats

Out of the box, the LLM xpack provides wrappers for text generation and embedding LLMs. For text generation, you can use native wrappers for the OpenAI chat model and HuggingFace models running locally. Many other popular models, including Azure OpenAI, HuggingFace (when using their API) or Gemini can be used with the [`LiteLLM`](/developers/user-guide/llm-xpack/llm-chats#litellm) wrapper. To check the full list of providers supported by LiteLLM check [LiteLLM documentation](https://docs.litellm.ai/docs/providers). 
::if{path="/llm-xpack/"}
Currently, Pathway provides wrappers for the following LLMs:
- [OpenAI](/developers/user-guide/llm-xpack/llm-chats#openaichat)
- [LiteLLM](/developers/user-guide/llm-xpack/llm-chats#litellm)
- [Hugging Face Pipeline](/developers/user-guide/llm-xpack/llm-chats#hugging-face-pipeline)
- [Cohere](/developers/user-guide/llm-xpack/llm-chats#cohere)
::
::if{path="/templates/"}
Currently, Pathway provides wrappers for the following LLMs:
- [OpenAI](/developers/user-guide/llm-xpack/llm-chats#openaichat)
- [LiteLLM](/developers/user-guide/llm-xpack/llm-chats#litellm)
- [Hugging Face Pipeline](/developers/user-guide/llm-xpack/llm-chats#hugging-face-pipeline)
::


::if{path="/llm-xpack/"}
To use a wrapper, first create an instance of the wrapper, which you can then apply to a column containing prompts.

We create a Pathway table to be used in the examples below:
```python
import pathway as pw
queries = pw.debug.table_from_markdown(
    """
questions | max_tokens
How many 'r' there are in 'strawberry'? | 400
""",
    split_on_whitespace=False,
)
```
::

::if{path="/llm-xpack/"}
## UDFs

Each wrapper is a [UDF](/developers/api-docs/pathway#pathway.UDF) (User Defined Function), which allows users to define their own functions to interact with Pathway objects. A UDF, in general, is any function that takes some input, processes it, and returns an output. In the context of the Pathway library, UDFs enable seamless integration of custom logic, such as invoking LLMs for specific tasks.

In particular a UDF can serve as a wrapper for LLM calls, allowing users to pass prompts or other inputs to a model and retrieve the corresponding outputs. This design makes it easy to interact with Pathway tables and columns while incorporating the power of LLMs.
::

## OpenAIChat

 For OpenAI, you create a wrapper using the [`OpenAIChat` class](/developers/api-docs/pathway-xpacks-llm/llms#pathway.xpacks.llm.llms.OpenAIChat).

::if{path="/llm-xpack/"}
```python
from pathway.xpacks.llm import llms

model = llms.OpenAIChat(
    model="gpt-4o-mini",
    api_key=os.environ["OPENAI_API_KEY"], # Read OpenAI API key from environmental variables
)
# Send queries from column `question` in table `queries` to OpenAI
responses = queries.select(result=model(llms.prompt_chat_single_qa(pw.this.questions)))
# Run the computations (including sending requests to OpenAI) and print the output table
pw.debug.compute_and_print(responses)
```
::
::if{path="/templates/"}
```yaml
chat: !pw.xpacks.llm.llms.OpenAIChat
  model: "gpt-4o-mini
  api_key: $OPENAI_API_KEY
```
::


::if{path="/llm-xpack/"}
### Message format
`OpenAIChat` expects messages to be in the format required by [OpenAI API](https://platform.openai.com/docs/api-reference/chat/create) - that is a list of dictionaries, where each dictionary is one message in the conversation so far. For asking a single question, you can use [`pw.xpacks.llm.llm.prompt_chat_single_qa`](/developers/api-docs/pathway-xpacks-llm/llms#pathway.xpacks.llm.llms.prompt_chat_single_qa) to wrap a string so that it matches the format expected by OpenAI API. Our example above presents that use case.

If you, however, want to have more control over messages sent to OpenAI, you can prepare the messages yourself.

```python
messages = pw.debug.table_from_rows(
    pw.schema_from_types(questions=list[dict]),
    rows=[
        (
            [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "How many 'r' there are in 'strawberry'?"},
            ],
        )
    ],
)
responses = messages.select(result=model(pw.this.questions))
pw.debug.compute_and_print(responses)
```

### Model parameters
OpenAI API takes a number of parameters, including `model` and `api_key` used in the code stubs above. `OpenAIChat` allows you to set their default value during the initialization of the class, but you can also override them during application.

```python
model = llms.OpenAIChat(
    model="gpt-4o-mini",
    api_key=os.environ["OPENAI_API_KEY"], # Read OpenAI API key from environmental variables
    max_tokens=200, # Set default value of max_tokens to be 200
)
# As max_tokens is not set, value 200 will be used
responses = queries.select(result=model(llms.prompt_chat_single_qa(pw.this.questions)))
# Now value of max_tokens is taken from column `max_tokens`, overriding default value set when initializing OpenAIChat
responses = queries.select(result=model(llms.prompt_chat_single_qa(pw.this.questions), max_tokens(pw.this.max_tokens)))
pw.debug.compute_and_print(responses)
```
::

## LiteLLM
Pathway has a wrapper for LiteLLM - [`LiteLLMChat`](/developers/api-docs/pathway-xpacks-llm/llms#pathway.xpacks.llm.llms.LiteLLMChat). For example, to use Gemini with LiteLLM, create an instance of `LiteLLMChat` and then apply it to the column with messages to be sent over API.

::if{path="/llm-xpack/"}
```python
from pathway.xpacks.llm import llms

model = llms.LiteLLMChat(
    model="gemini/gemini-pro", # Choose the model you want
    api_key=os.environ["GEMINI_API_KEY"], # Read GEMINI API key from environmental variables
)
# Ask Gemini questions from `prompt` column 
responses = queries.select(result=model(llms.prompt_chat_single_qa(pw.this.questions)))
pw.debug.compute_and_print(responses)
```
::
::if{path="/templates/"}
```yaml
llm: !pw.xpacks.llm.llms.LiteLLMChat
  model: "gemini/gemini-pro", # Choose the model you want
``` 
::

With the wrapper for LiteLLM, Pathway allows you to use many popular LLMs.

## Hugging Face pipeline
For models from Hugging Face that you want to run locally, Pathway gives a separate wrapper called `HFPipelineChat` (for calling HuggingFace through API, use LiteLLM wrapper). When an instance of this wrapper is created, it initializes a HuggingFace `pipeline`, so any [argument to the `pipeline`](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.pipeline) - including the name of the model - must be set during the initialization of `HFPipelineChat`. Any parameters to `pipeline.__call__` can be as before set during initialization or overridden during application.

::if{path="/llm-xpack/"}
```python
from pathway.xpacks.llm import llms

model = llms.HFPipelineChat(
    model="gpt2", # Choose the model you want
)
responses = queries.select(result=model(pw.this.questions))
pw.debug.compute_and_print(responses)
```
::
::if{path="/templates/"}
```yaml
llm: !pw.xpacks.llm.llms.HFPipelineChat
  model: "TinyLlama/TinyLlama-1.1B-Chat-v1.0", # Choose the model you want
``` 
::

Note that format of questions used in Hugging Face pipeline depends on the model. Some models, like [`gpt2`](https://huggingface.co/openai-community/gpt2), expect a prompt string, whereas conversation models also accept messages as a list of dicts. The model's prompt template will be used if a conversation with a list of dicts is passed.
::if{path="/templates/"}
Note that Pathway AI pipelines expect conversation models, so models like `gpt2` cannot be used. 
::
For more information, see [pipeline docs](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TextGenerationPipeline.__call__.text_inputs). 

::if{path="/llm-xpack/"}
For example for model [`TinyLlama/TinyLlama-1.1B-Chat-v1.0`](https://huggingface.co/TinyLlama/TinyLlama-1.1B-Chat-v1.0), you can use it with:

```python
from pathway.xpacks.llm import llms

model = llms.HFPipelineChat(
    model="TinyLlama/TinyLlama-1.1B-Chat-v1.0",
)
responses = queries.select(result=model(llms.prompt_chat_single_qa(pw.this.questions)))
pw.debug.compute_and_print(responses)
```
::

::if{path="/llm-xpack/"}
## Cohere
Pathway has a wrapper for the [`Cohere Chat Services`](https://docs.cohere.com/docs/command-beta). The wrapper allows for augmenting the query with documents. The result contains cited documents along with the response.

```python
from pathway.xpacks.llm import llms

model = llms.CohereChat()
queries_with_docs = pw.debug.table_from_rows(
    schema=pw.schema_from_types(questions=str, docs=list[dict]),
    rows=[
        (
            "What is RAG?",
            [
                {
                    "text": "Pathway is a high-throughput, low-latency data processing framework that handles live data & streaming for you."
                },
                {"text": "RAG stands for Retrieval Augmented Generation."},
            ],
        )
    ],
)

r = queries_with_docs.select(
    ret=model(llms.prompt_chat_single_qa(pw.this.questions), documents=pw.this.docs)
)
parsed_table = r.select(response=pw.this.ret[0], citations=pw.this.ret[1])
pw.debug.compute_and_print(parsed_table)
```
::

## Wrappers are asynchronous
Wrapper for OpenAI and LiteLLM, both for chat and embedding, are asynchronous, and Pathway allows you to set three parameters to set their behavior. These are:
- `capacity`, which sets the number of concurrent operations allowed,
- `retry_strategy`, which sets the strategy for handling retries in case of failures,
- `cache_strategy`, which defines the cache mechanism.

These three parameters need to be set during the initialization of the wrapper. You can read more about them in the [UDFs guide](/developers/user-guide/data-transformation/user-defined-functions#asyncexecutor).

::if{path="/llm-xpack/"}
```python
model = llms.OpenAIChat(
    # maximum concurrent operations is 10
    capacity=10,
    # in case of failure, retry 5 times, each time waiting twice as long before retrying
    retry_strategy=pw.udfs.ExponentialBackoffRetryStrategy(max_retries=5, initial_delay=1000, backoff_factor=2),
    # if PATHWAY_PERSISTENT_STORAGE is set, then it is used to cache the calls
    cache_strategy=pw.udfs.DefaultCache(),
    # select the model
    model="gpt-4o-mini",
    # read OpenAI API key from environmental variables
    api_key=os.environ["OPENAI_API_KEY"],
)
responses = queries.select(result=model(prompt_chat_single_qa(pw.this.questions)))
pw.debug.compute_and_print(responses)
```
::
::if{path="/templates/"}
```yaml
chat: !pw.xpacks.llm.llms.OpenAIChat
  model: "gpt-4o-mini
  capacity: 10
  retry_strategy: !pw.udfs.ExponentialBackoffRetryStrategy
    max_retries: 5
    initial_delay: 1000
    backoff_factor: 2
```
::

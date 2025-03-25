# Retrieval-Augmented Generation (RAG) Pipeline with Pathway

This project demonstrates how to build a Retrieval-Augmented Generation (RAG) pipeline using Pathway.
You can learn more about this [here](https://pathway.com/developers/user-guide/llm-xpack/create-your-own-rag).

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [RAG architecture](#rag-architecture)
4. [Pathway HTTP Server](#pathway-http-server)
5. [Setup and Installation](#setup-and-installation)
6. [Usage](#usage)
7. [Conclusions](#conclusions)

## Introduction

The RAG pipeline is designed to enhance the accuracy and relevance of responses to user queries by leveraging a combination of document retrieval and generative language models. This approach ensures that the generated answers are not only coherent but also grounded in the most relevant information available.

## Prerequisites

Before you start, ensure you have the necessary package installed. You can install it using the following command:

```bash
pip install pathway[xpack-llm] python-dotenv
```

## RAG architecture

Here's how the RAG structure works:
- **Document Indexing**: The process begins with a collection of documents that are indexed and stored in a searchable format. Indexing involves analyzing the content of each document to identify key terms and phrases, which are then organized for efficient retrieval.
- **User Query**: A user inputs a query, which could be a question or a request for information. This query serves as the starting point for the RAG process.
- **Document Retrieval**: The retrieval system takes the user's query and searches through the indexed documents to find the most relevant pieces of information. This step uses advanced algorithms to quickly identify and retrieve documents that are likely to contain the answer to the query.
- **Context Building**: The retrieved documents are then used to build a context. This context includes the relevant information extracted from the documents, which will be used to generate a response.
- **Prompt Construction**: A prompt is constructed by combining the user's query with the context built from the retrieved documents. This prompt serves as the input for the generative language model.
- **Answer Generation**: The generative language model processes the prompt and generates a coherent and accurate response. This model is trained to understand and produce human-like text, ensuring that the response is informative and contextually appropriate.
- **Final Output**: The generated response is presented to the user as the final output. This response is designed to be comprehensive and easy to understand, providing the user with the information they sought.

By integrating retrieval and generation, RAG ensures that the responses are not only accurate but also contextually relevant, making it a robust solution for complex information retrieval tasks.

## Pathway HTTP server
This project comes with a light HTTP server to send queries and retrieve answers.
By default, the host is `0.0.0.0` and the port is `8011`, you can configure it by editing this line:

```python
webserver = pw.io.http.PathwayWebserver(host="0.0.0.0", port=8011)
```


## Setup and Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/pathwaycom/pathway.git
   cd ./examples/projects/question-answering-rag/
   ```

2. **Install Dependencies**:
   ```bash
   pip install pathway[xpack-llm] python-dotenv
   ```

3. **Environment Variables**:
   Create a `.env` file in the project root and add your OpenAI API key:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   ```

## Usage

1. **Run the Pipeline**:
   ```bash
   python main.py
   ```

2. **Submit a Query**:
   Use `curl` to send a query to the web server:
   ```bash
   curl --data '{ "messages": "What is the value of X?"}' http://localhost:8011
   ```

3. **Receive the Response**:
   The server will process the query and return a generated response based on the retrieved documents.

## Conclusions
This project provides a simple RAG pipeline using Pathway.
The RAG is live by default, updating the index whenever the documentation changes.

You can find more ready-to-run pipelines in our [templates section](/developers/templates?tab=ai-pipelines).

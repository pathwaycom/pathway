# AG2 Multi-Agent Conversations with Pathway Real-Time RAG

This example demonstrates how to combine [AG2](https://ag2.ai/) (formerly AutoGen)
multi-agent conversations with [Pathway](https://pathway.com/)'s real-time RAG pipeline.

AG2 is a multi-agent conversation framework with 500K+ monthly PyPI downloads,
4,300+ GitHub stars, and 400+ contributors.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Setup and Installation](#setup-and-installation)
5. [Usage](#usage)
6. [How It Works](#how-it-works)
7. [Conclusions](#conclusions)

## Introduction

This project combines two powerful frameworks:
- **Pathway** continuously indexes documents in real-time using its streaming engine, serving them through a VectorStoreServer REST API
- **AG2** orchestrates multiple AI agents (Researcher + Analyst) that query Pathway's index as a tool during their conversation

The key advantage: Pathway re-indexes documents automatically whenever they change, so AG2 agents always query the **latest** version of the knowledge base — no manual re-indexing required.

## Prerequisites

- Python >= 3.10
- OpenAI API key

## Architecture

```
Documents (live folder) --> Pathway VectorStoreServer (real-time indexing)
                                         |
                                    REST API /v1/retrieve
                                         |
User Query --> AG2 UserProxy --> GroupChat [Researcher + Analyst]
                                         |
                               search_documents tool --> HTTP POST --> Pathway
                                         |
                               Grounded, real-time answers with citations
```

## Setup and Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/pathwaycom/pathway.git
   cd pathway/examples/projects/ag2-multiagent-rag/
   ```

2. **Install Dependencies**:
   ```bash
   pip install -U pathway "ag2[openai]>=0.11.4,<1.0" requests python-dotenv
   ```

3. **Environment Variables**:
   Create a `.env` file in the project root and add your OpenAI API key:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   ```

4. **Add Documents**:
   Place your documents (TXT, MD, PDF) in the `./data/` directory. A sample document is included for testing.

## Usage

1. **Run the Pipeline**:
   ```bash
   python main.py
   ```

2. The script will:
   - Start a Pathway VectorStoreServer that indexes documents in `./data/`
   - Launch AG2 agents that query the server for information
   - Print the multi-agent conversation with grounded answers

3. **Add documents while running** — Pathway re-indexes automatically.

## How It Works

- **Pathway** reads documents from `./data/`, chunks them with `TokenCountSplitter`, embeds them with OpenAI embeddings, and serves the index via HTTP
- **AG2 Researcher agent** queries Pathway's `/v1/retrieve` endpoint via the `search_documents` tool to retrieve relevant chunks
- **AG2 Analyst agent** synthesizes retrieved information into a comprehensive answer
- The agents communicate via AG2's `GroupChat`, coordinated by a `GroupChatManager`

## Conclusions

This example shows how Pathway's real-time document indexing complements AG2's multi-agent orchestration. The combination is especially useful for scenarios where documents change frequently and agents need access to the latest information — such as live knowledge bases, continuously updated reports, or streaming data pipelines.

You can find more ready-to-run pipelines in our [templates section](/developers/templates?tab=ai-pipelines).

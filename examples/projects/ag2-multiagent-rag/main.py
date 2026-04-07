# Copyright © 2026 Pathway

"""
AG2 Multi-Agent Conversations with Pathway Real-Time RAG
=========================================================

Demonstrates AG2 (formerly AutoGen) multi-agent conversations using
Pathway's real-time VectorStoreServer as the knowledge retrieval backend.

Pathway continuously indexes documents and serves them via a REST API.
AG2 agents query this API as a tool during multi-agent conversations.

Requirements:
    pip install -U pathway "ag2[openai]>=0.11.4,<1.0" requests python-dotenv

Environment variables:
    OPENAI_API_KEY - OpenAI API key (used by both Pathway and AG2)

Usage:
    1. Place documents in ./data/ folder
    2. Run: python main.py
"""

import json
import os
import sys
import threading
import time

import requests
from autogen import (
    AssistantAgent,
    GroupChat,
    GroupChatManager,
    LLMConfig,
    UserProxyAgent,
)
from dotenv import load_dotenv

import pathway as pw
from pathway.xpacks.llm.embedders import OpenAIEmbedder
from pathway.xpacks.llm.splitters import TokenCountSplitter
from pathway.xpacks.llm.vector_store import VectorStoreServer

# To use advanced features with Pathway Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Community, comment out the line below.
pw.set_license_key("demo-license-key-with-telemetry")

load_dotenv()

PATHWAY_HOST = "127.0.0.1"
PATHWAY_PORT = 8765
DATA_DIR = "./data"


def start_pathway_server():
    """Start Pathway VectorStoreServer in a background thread."""
    documents = pw.io.fs.read(
        DATA_DIR,
        format="binary",
        mode="streaming",
        with_metadata=True,
    )

    embedder = OpenAIEmbedder(model="text-embedding-3-small")
    splitter = TokenCountSplitter(max_tokens=400)

    server = VectorStoreServer(
        documents,
        embedder=embedder,
        splitter=splitter,
    )

    server.run_server(
        host=PATHWAY_HOST,
        port=PATHWAY_PORT,
        threaded=False,
        with_cache=True,
    )


def query_pathway_server(query: str, k: int = 5) -> str:
    """Query the Pathway VectorStoreServer via HTTP.

    The /v1/retrieve endpoint returns a JSON list of objects:
    [{"text": "...", "metadata": {...}, "dist": float}, ...]
    sorted by dist (lower = more similar).
    """
    url = f"http://{PATHWAY_HOST}:{PATHWAY_PORT}/v1/retrieve"
    payload = {"query": query, "k": k}

    try:
        response = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        results = response.json()

        if not results:
            return "No relevant documents found."

        formatted = []
        for i, result in enumerate(results, 1):
            text = result.get("text", "")
            metadata = result.get("metadata", {})
            source = metadata.get("path", "Unknown source")
            dist = result.get("dist", "N/A")
            formatted.append(f"[{i}] Source: {source} (distance: {dist})\n{text}")

        return "\n\n---\n\n".join(formatted)

    except requests.exceptions.ConnectionError:
        return "Error: Pathway server is not running or not ready yet."
    except Exception as e:
        return f"Error querying Pathway: {e}"


def main():
    """Run AG2 multi-agent RAG with Pathway real-time indexing."""

    # Validate environment
    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is not set.")
        print("Set it in your shell or create a .env file.")
        sys.exit(1)

    # Validate data directory
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created {DATA_DIR}/ directory.")
        print("Please add documents (TXT, MD, PDF) and re-run.")
        sys.exit(1)

    if not any(f for f in os.listdir(DATA_DIR) if not f.startswith(".")):
        print(f"No documents found in {DATA_DIR}/")
        print("Please add documents (TXT, MD, PDF) and re-run.")
        sys.exit(1)

    # Start Pathway server in background thread
    print("Starting Pathway VectorStoreServer...")
    server_thread = threading.Thread(target=start_pathway_server, daemon=True)
    server_thread.start()

    # Wait for server to be ready
    print("Waiting for Pathway server to initialize...")
    for attempt in range(60):
        try:
            resp = requests.post(
                f"http://{PATHWAY_HOST}:{PATHWAY_PORT}/v1/statistics",
                json={},
                timeout=10,
            )
            if resp.status_code == 200:
                stats = resp.json()
                print(
                    f"Pathway server is ready! Indexed files: {stats.get('file_count', 'N/A')}"
                )
                break
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            time.sleep(2)
    else:
        print("Warning: Pathway server may not be fully ready.")

    # AG2 LLM Configuration
    llm_config = LLMConfig(
        {
            "model": "gpt-4o-mini",
            "api_key": os.environ["OPENAI_API_KEY"],
            "api_type": "openai",
        }
    )

    # Create AG2 Agents
    researcher = AssistantAgent(
        name="researcher",
        system_message=(
            "You are a research agent. When asked a question, use the "
            "search_documents tool to retrieve relevant information from "
            "the knowledge base. Present your findings clearly with source "
            "references. If initial results are insufficient, try different "
            "search queries."
        ),
        llm_config=llm_config,
    )

    analyst = AssistantAgent(
        name="analyst",
        system_message=(
            "You are an analyst. Based on the researcher's findings, "
            "synthesize the information into a comprehensive, well-structured "
            "answer. Always reference the source documents. If information is "
            "insufficient, ask the researcher to search with different terms. "
            "End with TERMINATE when the answer is complete."
        ),
        llm_config=llm_config,
    )

    user_proxy = UserProxyAgent(
        name="user_proxy",
        human_input_mode="NEVER",
        max_consecutive_auto_reply=10,
        code_execution_config=False,
        is_termination_msg=lambda x: x.get("content", "")
        and "TERMINATE" in x.get("content", ""),
    )

    # Register Pathway search as AG2 tool
    @user_proxy.register_for_execution()
    @researcher.register_for_llm(
        description=(
            "Search the document knowledge base powered by Pathway. "
            "Returns relevant document chunks with source citations. "
            "The knowledge base is continuously updated in real-time. "
            "Use specific, targeted search queries for best results."
        )
    )
    def search_documents(query: str, top_k: int = 5) -> str:
        """Search Pathway VectorStoreServer for relevant document chunks.

        Args:
            query: The search query string.
            top_k: Number of results to return (default: 5).

        Returns:
            Formatted string with retrieved document chunks and sources.
        """
        return query_pathway_server(query, k=top_k)

    # Set up GroupChat
    group_chat = GroupChat(
        agents=[user_proxy, researcher, analyst],
        messages=[],
        max_round=12,
    )

    manager = GroupChatManager(
        groupchat=group_chat,
        llm_config=llm_config,
    )

    # Run the conversation
    query = (
        "What are the key topics and insights described in the documents? "
        "Provide a comprehensive summary with citations."
    )

    print(f"\n{'=' * 60}")
    print("AG2 Multi-Agent RAG with Pathway Real-Time Indexing")
    print(f"{'=' * 60}")
    print(f"Query: {query}\n")

    user_proxy.run(manager, message=query).process()

    print(f"\n{'=' * 60}")
    print("Conversation complete.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

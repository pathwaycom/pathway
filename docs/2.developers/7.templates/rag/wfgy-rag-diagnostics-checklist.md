# Streaming RAG diagnostics with the WFGY 16-problem map

This example shows how to add a lightweight diagnostic layer on top of an existing Pathway RAG template, using the **WFGY ProblemMap** as a checklist of common failure modes.

Instead of reading raw logs query by query, you will:

- log a small set of signals for each RAG request
- map those signals to one or more **problem codes** from the WFGY 16-problem map
- use these codes as a shared language when debugging or reviewing incidents

The WFGY ProblemMap is an MIT-licensed, open source “failure atlas” for LLM and RAG pipelines.  
You can browse the full map in the upstream repository:

- [WFGY ProblemMap README](https://github.com/onestardao/WFGY/blob/main/ProblemMap/README.md)

This page focuses on how to connect that map to Pathway in a minimal, implementation-agnostic way.

---

## What you will build

You will not build a new RAG app from scratch.  
Instead, you will extend an existing template with:

- a small diagnostics record per query
- a `diagnose_wfgy` helper that tags each run with candidate problem codes such as `No.1`, `No.5`, `No.9`
- a place to inspect these tags (logs, metrics, or a debug view)

The actual RAG logic (indexing, retrieval, LLM calls) stays in the existing Pathway template.

---

## When to use this example

This diagnostic layer is useful when:

- you already run a Pathway RAG app in development or production
- some answers look wrong, but it is unclear whether the root cause is retrieval, embeddings, orchestration, or the model
- you want a repeatable checklist for your team, not ad-hoc debugging

You can attach this checklist to any template that exposes a query response and access to basic retrieval metrics, for example:

- the question-answering RAG template
- the adaptive RAG template
- your own RAG application built with `pathway.xpacks.llm`

---

## The 16-problem map at a glance

The full WFGY map contains 16 failure modes.  
Here is a short subset to orient yourself; see the upstream README for full details and fix ideas.

| Code | Name                                | Short description                                              |
|------|-------------------------------------|----------------------------------------------------------------|
| No.1 | hallucination & chunk drift         | retrieval misses or returns off-topic chunks, model fills gaps |
| No.2 | interpretation collapse             | context is relevant, but question type is misunderstood        |
| No.5 | semantic vs embedding mismatch      | cosine scores look good while semantic match is poor           |
| No.7 | memory breaks across sessions       | follow-up questions lose state between turns                   |
| No.8 | debugging is a black box            | logs lack the signals needed to understand failures            |
| No.9 | entropy collapse                    | answers become very short or repetitive despite rich context   |
| No.13| multi-agent chaos                   | multiple components overwrite each other’s state or indices    |
| No.14–16 | bootstrap / deploy issues       | things work locally but fail after deployment or re-indexing   |

The goal of this example is not to reproduce the entire map inside Pathway, but to make it easy to tag each query with a small set of candidate codes.

---

## Minimal diagnostics record per query

You can implement diagnostics with any logging or metrics system.  
Each query run should produce one record containing at least:

- `query_id` – unique identifier
- `question_text` – optionally truncated
- `retrieved_docs` – number of retrieved items
- `avg_similarity` – average similarity score
- `max_similarity` – maximum similarity score
- `context_chars` – total characters in concatenated context
- `answer_length_chars` – length of the answer
- `answer_status` – for example: `ok`, `timeout`, `error`, `empty`
- `index_name` – or collection identifier

Existing templates may already expose some of these values; in that case you only need to add the missing ones.

---

## A small `diagnose_wfgy` helper

The following helper is framework-agnostic.  
It takes a single diagnostics record and returns a list of suggested WFGY problem codes based on simple heuristics.

You can call it from any part of your Pathway app that has access to the per-query metrics.

```python
from typing import Dict, List


def diagnose_wfgy(run: Dict) -> List[str]:
    """
    Inspect a single query run and return a list of WFGY Problem Map codes.
    `run` is a dict with the fields listed in the previous section.
    """
    codes: List[str] = []

    retrieved = run.get("retrieved_docs", 0)
    avg_sim = run.get("avg_similarity", 0.0)
    max_sim = run.get("max_similarity", 0.0)
    answer_len = run.get("answer_length_chars", 0)
    context_len = run.get("context_chars", 0)
    status = run.get("answer_status", "ok")

    # No.1 – hallucination & chunk drift
    if retrieved == 0 and answer_len > 0 and status == "ok":
        codes.append("No.1")

    # No.5 – semantic vs embedding mismatch
    if retrieved > 0 and max_sim > 0.85 and avg_sim < 0.4:
        codes.append("No.5")

    # No.9 – entropy collapse
    if context_len > 0 and answer_len < 64 and status == "ok":
        codes.append("No.9")

    # No.14–16 – bootstrap / deploy issues
    if status != "ok" and retrieved == 0:
        codes.append("No.14–16")

    return codes
````

How you integrate this into Pathway is up to you. Two common options are:

* attach the `wfgy_codes` list to existing logs for each query
* stream the codes into a dedicated debug view for your team

A typical enriched record might look like:

```json
{
  "query_id": "2026-02-20-00123",
  "retrieved_docs": 0,
  "avg_similarity": 0.05,
  "answer_length_chars": 420,
  "answer_status": "ok",
  "wfgy_codes": ["No.1", "No.14–16"]
}
```

This immediately suggests “hallucination on top of a broken index or deployment path”.

---

## Using the full WFGY checklist outside Pathway

For more detailed diagnostics, you can also use the full WFGY text packs in any external LLM environment:

1. Load the [WFGY ProblemMap](https://github.com/onestardao/WFGY/blob/main/ProblemMap/README.md) and, optionally, the [TXT OS text pack](https://github.com/onestardao/WFGY/blob/main/OS/TXTOS.txt) into an LLM of your choice.
2. Paste one or more failing traces or the structured diagnostics record.
3. Ask the model to identify the most likely Problem Map codes and to explain its reasoning.

This keeps your Pathway deployment lean while still giving you access to the complete failure atlas when needed.

---

## Summary

* Pathway provides building blocks for high-performance RAG pipelines.
* The WFGY ProblemMap adds a compact taxonomy of 16 recurring failure modes.
* By logging a few extra metrics per query and applying simple heuristics, you can tag each run with candidate problem codes.
* These tags make it easier to debug incidents, prioritise fixes, and onboard new team members to your RAG infrastructure.

<p align="center" class="flex items-center gap-1 justify-center flex-wrap">
    <img src="../../assets/gcp-logo.svg?raw=true" alt="GCP Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/gcp-deploy">Deploy with GCP</a> |
    <img src="../../assets/aws-fargate-logo.svg?raw=true" alt="AWS Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/aws-fargate-deploy">Deploy with AWS</a> |
    <img src="../../assets/azure-logo.svg?raw=true" alt="Azure Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/azure-aci-deploy">Deploy with Azure</a> |
    <img src="../../assets/render.png?raw=true" alt="Render Logo" height="20" width="20">
    <a href="https://pathway.com/developers/user-guide/deployment/render-deploy"> Deploy with Render </a>
</p>

# Video RAG with Pathway Live Data Framework and TwelveLabs

## Overview

This app template shows how to build a RAG application **over video** using the
Pathway Live Data Framework together with [TwelveLabs](https://twelvelabs.io).

Videos are notoriously hard to put into a RAG pipeline: most stacks first
transcribe the audio and throw away everything that happens on screen. This
template indexes the *whole* video instead, using two TwelveLabs models:

- **[Pegasus](https://docs.twelvelabs.io/docs/concepts/models/pegasus)** — a
  video-understanding model that turns each video into a rich text description
  (what happens, who and what appears, the setting, on-screen and spoken text,
  the overall topic). Pathway Live Data Framework indexes that text exactly like it would index a PDF.
- **[Marengo](https://docs.twelvelabs.io/docs/concepts/models/marengo)** — a
  multimodal embedding model that produces 512-dimensional vectors in a shared
  space for text, image, audio and video. It is used here as the retriever
  embedder.

Because the template uses the standard Pathway Live Data Framework `DocumentStore` and
`BaseRAGQuestionAnswerer`, everything else works out of the box: live sync with
your data source, the in-memory vector index, caching, and the HTTP API. Drop a
new video into the connected folder and it becomes queryable automatically.

## Architecture

```
video files ─▶ pw.io.fs.read (bytes)
            ─▶ TwelveLabsVideoParser  (Pegasus: video → text)
            ─▶ TokenCountSplitter      (chunking)
            ─▶ DocumentStore + UsearchKnnFactory
                 with MarengoEmbedder  (text → 512-dim vectors)
            ─▶ BaseRAGQuestionAnswerer  (OpenAI LLM over retrieved context)
            ─▶ REST API on :8000
```

The TwelveLabs components are part of the Pathway Live Data Framework LLM xpack:

- [`TwelveLabsVideoParser`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.TwelveLabsVideoParser)
  — a Pathway Live Data Framework parser that uploads the video bytes as a TwelveLabs asset and asks
  Pegasus to describe it.
- [`MarengoEmbedder`](https://pathway.com/developers/api-docs/pathway-xpacks-llm/embedders#pathway.xpacks.llm.embedders.MarengoEmbedder)
  — a Pathway Live Data Framework embedder that calls the Marengo embedding endpoint.

Both are wired in entirely through [`app.yaml`](app.yaml), so you can swap models,
prompts, the data source, or the LLM without touching any Python.

## Customizing the pipeline

- **Change what is extracted from the video.** Set the `prompt` field of
  `$parser` in `app.yaml`, e.g. `"Describe this video, focusing on the products
  that appear and any prices shown."`
- **Change the data source.** Replace the `!pw.io.fs.read` source with the Google
  Drive, SharePoint, or S3 connector (a commented Google Drive example is
  included in `app.yaml`).
- **Change the answering LLM.** Edit the `$llm` block — any
  [Pathway Live Data Framework LLM wrapper](https://pathway.com/developers/api-docs/pathway-xpacks-llm/llms)
  works.

## Running the app

### Prerequisites

- A TwelveLabs API key. Get a free one at [twelvelabs.io](https://twelvelabs.io) —
  there is a generous free tier.
- An OpenAI API key for the question-answering LLM.
- A Pathway Live Data Framework license key, required by the `TwelveLabsVideoParser`. Get a free one
  at [pathway.com/features](https://pathway.com/features).

Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
# edit .env and set TWELVELABS_API_KEY, OPENAI_API_KEY and PATHWAY_LICENSE_KEY
```

Put one or more videos (e.g. `.mp4`, `.mov`) into the `data/` directory.

### With Docker

```bash
docker build -t video-rag-twelvelabs .
docker run -v $(pwd)/data:/app/data --env-file .env -p 8000:8000 video-rag-twelvelabs
```

### Locally

```bash
pip install -r requirements.txt
python app.py
```

### Querying

Once the server is up, ask questions about your videos:

```bash
curl -X POST http://localhost:8000/v1/pw_ai_answer \
  -H "Content-Type: application/json" \
  -d '{"prompt": "What products are shown in the videos?"}'
```

## Tests

The TwelveLabs components are tested as part of the Pathway Live Data Framework core test suite, in
[`python/pathway/xpacks/llm/tests/test_twelvelabs.py`](https://github.com/pathwaycom/pathway/blob/main/python/pathway/xpacks/llm/tests/test_twelvelabs.py).

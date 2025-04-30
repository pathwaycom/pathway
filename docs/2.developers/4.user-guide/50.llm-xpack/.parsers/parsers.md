---
title: "Parsers"
description: "Article about Pathway's parsers."
date: '2024-06-04'
tags: ['tutorial', 'LLM']
keywords: ['LLM', 'unstructured', 'docling', 'parsers', 'ocr']
---


# Parsers

Parsers play a crucial role in the Retrieval-Augmented Generation (RAG) pipeline by transforming raw, unstructured data into structured formats that can be effectively indexed, retrieved, and processed by language models.
In a RAG system, data often comes from diverse sources such as documents, web pages, APIs, and databases, each with its own structure and format.
Parsers help extract relevant content, normalize it into a consistent structure, and enhance the retrieval process by making information more accessible and usable.

This article explores the different types of parsers that can be used in our pipelines, highlighting their specific functions and how they differ from one another.
Understanding these parsers is key to optimizing data ingestion, improving retrieval accuracy, and ultimately enhancing the quality of generated responses.

Here is a table listing all available parsers and some details about them:

| Name               | Data | Description |
|--------------------|------|-------------|
| Utf8Parser         | Text | Decodes text encoded in UTF-8. |
| UnstructuredParser | Text + tables | Leverages Unstructured library to parse various document types. |
| DoclingParser      | PDF + tables + images | Utilizes docling library to extract structured content from PDFs, including images. |
| PypdfParser        | PDF  | Uses pypdf library to extract text from PDFs with optional text cleanup. |
| ImageParser        | Image| Transforms images into textual descriptions and extracts structured information. |
| SlideParser        | Slide| Extracts information from PPTX and PDF slide decks using vision-based LLMs. |



## Utf8Parser

[`Utf8Parser`](/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.Utf8Parser) is a simple parser designed to decode text encoded in UTF-8. It ensures that raw byte-encoded content is converted into a readable string format for further processing in a RAG pipeline.


## UnstructuredParser

[`UnstructuredParser`](/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.UnstructuredParser) leverages the parsing capabilities of [Unstructured](https://unstructured.io/). It supports various document types, including PDFs, HTML, Word documents, and [more](https://docs.unstructured.io/open-source/introduction/supported-file-types), making it a robust out-of-the-box solution for most use cases. Additionally, it offers good performance in terms of speed.

However, there are some [limitations](https://docs.unstructured.io/open-source/introduction/overview#limits) associated with the open-source library, such as reduced performance in document and table extraction and reliance on older, less sophisticated vision transformer models. Moreover, Unstructured does not support image extraction.


### Chunking modes

Many parsers include chunking functionality, allowing them to use a document's structure to split content into smaller, semantically consistent chunks.
Pathway's `UnstructuredParser` supports five chunking modes:

- `basic` - Uses Unstructured's [basic chunking strategy](https://github.com/Unstructured-IO/unstructured/blob/238f985ddaaa04952ac089c6e94e592de2bda9b6/unstructured/chunking/basic.py#L24), which splits text into chunks shorter than the specified `max_characters` length (set via the `chunking_kwargs` argument). It also supports a soft threshold for chunk length using `new_after_n_chars`.
- `by_title` - Uses Unstructured's [chunk-by-title strategy](https://github.com/Unstructured-IO/unstructured/blob/238f985ddaaa04952ac089c6e94e592de2bda9b6/unstructured/chunking/title.py#L23) strategy, similar to basic chunking but with additional constraints to split chunks at section or page breaks, resulting in more structured chunks. Like basic chunking, it can be configured via `chunking_kwargs`.
- `elements` -  Breaks down a document into homogeneous Unstructured elements such as `Title`, `NarrativeText`, `Footer`, `ListItem` etc. Not recommended for PDFs or other complex data sources. Best suited for simple input data where individual elements need to be separated.
- `paged` - Collects all elements found on a single page into one chunk. Useful for documents where content is well-separated across pages.
- `single` - Aggregates all Unstructured elements into a single large chunk. Use this mode when applying other chunking strategies available in Pathway <!-- TODO: Link Pathway-defined chunking here --> or when using a custom chunking approach.

::if{path="/llm-xpack/"}
Example of usage:

```python
from pathway.xpacks.llm.parsers import UnstructuredParser
parser = UnstructuredParser(
    chunking_mode="by_title",
    chunking_kwargs={
        "max_characters": 3000,       # hard limit on number of characters in each chunk
        "new_after_n_chars": 2000,    # soft limit on number of characters in each chunk
    },
)

result = data_sources.with_columns(parsed=parser(data_sources.data))
```
::

::if{path="/templates/"}
Example of YAML configuration
```yaml
$parser: !pw.xpacks.llm.parsers.UnstructuredParser
  chunking_mode: "by_title"
  chunking_kwargs:
    max_characters: 3000        # hard limit on number of characters in each chunk
    new_after_n_chars: 2000     # soft limit on number of characters in each chunk
```
::

Unstructured chunking is character-based rather than token-based, meaning you do not have precise control over the maximum number of tokens each chunk will occupy in the context window.


## DoclingParser

[`DoclingParser`](/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.DoclingParser) is a PDF parser that utilizes the [docling](https://github.com/DS4SD/docling) library to extract structured content from PDFs. It extends docling's DocumentConverter with additional functionality to parse images from PDFs using vision-enabled language models. This allows for a more comprehensive extraction of content, including tables and embedded images.

It is recommended to use this parser when extracting **text**, **tables**, and **images** from PDFs.

DoclingParser offers structure-aware chunking functionality. It separates tables and images into distinct chunks, merges all list items into a single chunk, and ensures each chunk is wrapped with markdown headings at the top and appropriate captions at the bottom when available (e.g., for tables and images).

### Table parsing

There are two main approaches for parsing tables: (1) using Docling engine or (2) parsing using multimodal LLM. The first one will run Docling OCR on the top of table that is in the pdf and transform it into markdown format. The second one will transform the table into an image and send it to multimodal LLM and ask for parsing it. As of now we only support LLMs having same API interface as OpenAI.

In order to choose between these two you must set `table_parsing_strategy` to either `llm` or `docling`.
If you don't want to parse tables simply set this argument to `None`.


### Image parsing

If `image_parsing_strategy="llm"`, the parser detects images within the document, processes them with a multimodal LLM (such as OpenAI's GPT-4o), and embeds its descriptions in the Markdown output. If disabled, images are replaced with placeholders.

::if{path="/llm-xpack/"}
Example:

```python
from pathway.xpacks.llm.parsers import DoclingParser
from pathway.xpacks.llm.llms import OpenAIChat

multimodal_llm = OpenAIChat(model="gpt-4o-mini")

parser = DoclingParser(
    image_parsing_strategy="llm",
    multimodal_llm=multimodal_llm,
    pdf_pipeline_options={  # use it to override our default options for parsing pdfs with docling
        "do_formula_enrichment": True,
        "image_scale": 1.5,
    }
)
```
::

::if{path="/templates/"}
Example of YAML configuration
```yaml
$multimodal_llm: !pw.xpacks.llm.llms.OpenAIChat
  model: "gpt-4o-mini"

$parser: !pw.xpacks.llm.parsers.DoclingParser
  parse_images: True
  multimodal_llm: $multimodal_llm
  pdf_pipeline_options:
    do_formula_enrichment: True
    image_scale: 1.5
```
::

See [PdfPipelineOptions](https://github.com/DS4SD/docling/blob/6875913e34abacb8d71b5d31543adbf7b5bd5e92/docling/datamodel/pipeline_options.py#L217) for reference of possible configuration, like OCR options, picture classification, code OCR, scientific formula enrichment, etc.


<!-- ### Other references -->

<!-- Want to learn more? Have a look on our special [blog post](/blog/docling-parser) about DoclingParser. -->

## PypdfParser

[`PypdfParser`](/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.PypdfParser) is a lightweight PDF parser that utilizes the [pypdf](https://pypdf.readthedocs.io/en/stable/) library to extract text from PDF documents. It also includes an optional text cleanup feature to enhance readability by removing unnecessary line breaks and spaces.

Keep in mind that it might not be adequate for table extraction. No image extraction is supported.

## ImageParser

This parser can be used to transform image (e.g. in `.png` or `.jpg` format) into a textual description made by multimodal LLM. On top of that it could be used to extract structured information from the image via predefined schema.

::if{path="/llm-xpack/"}
### Example

Image that you have an application for detecting breed of dogs from the picture. You also want to know the color and surrounding of the dog.

Let's put an image of corgi into `./dogs` directory:
```
wget https://media.os.fressnapf.com/cms/2020/07/ratgeber_hund_rasse_portraits_welsh-corgi-pembroke_1200x527.jpg?t=seoimgsqr_527 -P ./dogs
```

Now, lets build some simple Pathway pipeline that would try to parse the image and extract structured information defined in pydantic schema.

```python
from pydantic import BaseModel
from pathway.xpacks.llm.llms import OpenAIChat
from pathway.xpacks.llm.parsers import ImageParser

data_sources = pw.io.fs.read(
    "./dogs",
    format="binary",
    mode="static",
)

chat = OpenAIChat(model="gpt-4o-mini")

# schema defining information we want to extract from the image
class DogDetails(BaseModel):
    breed: str
    surroundings: str
    color: str

prompt = "Please provide a description of the image."

parser = ImageParser(
    llm=chat,
    parse_prompt=prompt,
    detail_parse_schema=DogDetails,
)

result = data_sources.select(parsed=parser(data_sources.data))
```

The result (after writing to json) will be:

```
{
    "parsed": [
        [
            "The image shows a happy Corgi dog running in a grassy area. The Corgi has a reddish-brown and white coat, a fluffy tail, and its tongue is out, giving it a cheerful expression. Its ears are perked up, and it appears to be wearing a red collar. The background is slightly blurred, emphasizing the dog in motion.",
            {
                "breed": "Pembroke Welsh Corgi",
                "surroundings": "outdoors in a grassy area",
                "color": "tan and white"
            }
        ]
    ],
}

```

Under the hood, there are two requests to the `llm` model - the first one generates the basic description using provided `prompt`, while the second one uses [`instructor`](https://github.com/instructor-ai/instructor) to extract information from the image and organize it into a provided `detail_parse_schema`.

The second step is optional - if you don't specify `detail_parse_schema` parameter, instructor won't call LLM.
::


## SlideParser

[`SlideParser`](/developers/api-docs/pathway-xpacks-llm/parsers#pathway.xpacks.llm.parsers.SlideParser) is a powerful parser designed to extract information from PowerPoint (PPTX) and PDF slide decks using vision-based LLMs. 
It converts slides into images before processing them by vision LLM that tries to describe the content of a slide.

As in case of ImageParser you can also extract information specified in pydantic schema.



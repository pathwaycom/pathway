# Web Scraping with Pathway

This project demonstrates how to create a real-time web scraper using Pathway, a powerful data processing framework. The implementation fetches and processes news articles from websites, making it possible to continuously monitor and analyze the web content.

## Overview

This project consists of two main Python files:

- `scraping_python.py`: Contains the core web scraping functionality using the `newspaper4k` and `news-please` libraries
- `scraping_pathway.py`: Implements a Pathway connector that integrates the scraper with Pathway's data processing pipeline

## Features

- Dynamically fetch articles from news websites
- Extract article content and metadata
- Configurable refresh intervals
- Output to JSON Lines format

## Requirements

- Pathway
- newspaper4k
- news-please

## Installation

```bash
pip install -r requirements.txt
```

## How It Works

### scraping_python.py

This provides the core scraping functionality:

1. **Article Discovery**: Uses `newspaper4k` to build a site map and discover article URLs
2. **Content Extraction**: Uses `news-please` to fetch and parse article content
3. **Data Processing**: Cleans and normalizes article data

The main function `scrape_articles()` is a generator that yields article data with the configurable refresh intervals.

### scraping_pathway.py

This file integrates the scraper with Pathway:

1. **Connector**: Implements `NewsScraperSubject` that inherits from Pathway's `ConnectorSubject`
2. **Data Schema**: Defines a schema for article data with URL as primary key
3. **Table**: Each article is stored as a row in the table
4. **Pipeline**: Sets up a data pipeline that:
    - Reads data from websites
    - Outputs articles to a JSONL file


## Running the Scraper

### Docker

Build the image:
`docker build . -t scraper`

Run the container:
`docker run -t scraper`

Optionally, you can mount a volume to save the scraped articles data (jsonl file in this case):
`docker run -v $(pwd):/app scraper`

### Local
Run the scraper with:

```bash
python scraping_pathway.py
```

### Configuration Options

In `scraping_pathway.py`, you can configure:

- `website_urls`: List of websites to scrape
- `refresh_interval`: Time between scraping cycles (in seconds)

## Example Output

The scraper produces a JSONL file containing the scraped articles with content and optional metadata.

## Use Cases

- News monitoring and analysis
- Content aggregation
- Trend detection
- Topic extraction (when combined with LLMs)

## Important Notes

- Web scraping may face limitations due to anti-bot measures
- Some sites may implement rate limiting or IP blocking
- Consider using proxies for production use
- Not all websites are easily parsable with automated tools

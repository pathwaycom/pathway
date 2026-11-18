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

# Pathway Live Data Framework + PostgreSQL + LLM: app for querying financial reports with live document structuring pipeline

The aim of this pipeline is to extract and structure the data out of unstructured data (PDFs, queries)
on the fly.

This example consists of two separate parts that can be used independently.
1 - Pipeline 1: Proactive data pipeline that is always live and tracking file changes,
    it reads documents, structures them and writes results to PostgreSQL.
2 - Pipeline 2: Query answering pipeline that reads user queries, and answers them by
    generating SQL queries that are run on the data stored in PostgreSQL.


Specifically, Pipeline 1 reads in a collection of financial PDF documents from a local directory
(that can be synchronized with a Dropbox account), tokenizes each document using the tiktoken encoding,
then extracts, using the OpenAI API, the wanted fields.
The values are stored in a Pathway Live Data Framework table which is then output to a PostgreSQL instance.

Pipeline 2 then starts a REST API endpoint serving queries about programming in Pathway Live Data Framework.

Each query text is converted into a SQL query using the OpenAI API.

Architecture diagram and description are at
https://pathway.com/developers/templates/rag/unstructured-to-structured


⚠️ This project requires a running PostgreSQL instance.

🔵 The extracted fields from the PDFs documents are the following:
- company_symbol: str
- year: int
- quarter: str
- revenue_md: float
- eps: float
- net_income_md: float

⚠️ The revenue and net income are expressed in millions of dollars, the eps is in dollars.

🔵 The script uses a prompt to instruct the Language Model and generate SQL queries that adhere to the specified format.
The allowed queries follow a particular pattern:
1. The SELECT clause should specify columns or standard aggregator operators (SUM, COUNT, MIN, MAX, AVG).
2. The WHERE clause should include conditions using standard binary operators (<, >, =, etc.),
    with support for AND and OR logic.
3. To prevent 'psycopg2.errors.GroupingError', relevant columns from the WHERE clause are included
    in the GROUP BY clause.
4. For readability, if no aggregator is used, the company_symbol, year,
    and quarter are included in addition to the wanted columns.

Example:
"What is the net income of all companies?" should return:
Response:
'SELECT company_symbol, net_income_md, quarter, net_income_md FROM table;'


## Project architecture:
```
.
├── postgresql/
│   └── init-db.sql
├── ui/
│   └── server.py
├── __init__.py
├── app.py
└── docker-compose.yml
```

## Running the Pipeline

### Setup environment:

Set your env variables in the .env file placed in this directory.

```bash
OPENAI_API_KEY=sk-...
PATHWAY_PERSISTENT_STORAGE= # Set this variable if you want to use caching
```

### With Docker

To run jointly the Unstructured to SQL on the fly pipeline and a simple UI please execute:

```bash
docker compose up --build
```

Then, the UI will run at http://127.0.0.1:8501 by default. You can access it by following this URL in your web browser.

The `docker-compose.yml` file declares a [volume bind mount](https://docs.docker.com/reference/cli/docker/container/run/#volume) that makes changes to files under `data/` made on your host computer visible inside the docker container. The files in `data/quarterly_earnings` are indexed by the pipeline - you can paste new files there and they will impact the computations.

### Manually

Alternatively, you can run each service separately. To run PostgreSQL use Docker. To run it, run:
`docker compose up -d postgres`.

To install the dependencies, run:
`pip install -r requirements.txt`
Then run:
`python app.py`

This will run the pipeline, which you can access through REST API at `localhost:8080`. For example, you can send questions using curl:
```
curl --data '{
  "user": "user",
  "query": "What is the maximum quarterly revenue achieved by Apple?"
}' http://localhost:8080/ | jq
```

You can also run the Streamlit interface:
`streamlit run ui/server.py`


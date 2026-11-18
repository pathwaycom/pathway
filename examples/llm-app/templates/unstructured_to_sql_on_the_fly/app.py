"""
Microservice for accounting assistant.

The aim of this project is to extract and structure the data out of unstructured data (PDFs, queries)
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


Please check the README.md in this directory for how-to-run instructions.
"""

import json
import logging
import os

import dotenv
import psycopg
import tiktoken

import pathway as pw
from pathway.stdlib.utils.col import unpack_col
from pathway.xpacks.llm.llms import OpenAIChat, prompt_chat_single_qa
from pathway.xpacks.llm.parsers import UnstructuredParser

# To use advanced features with Pathway Live Data Framework Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Live Data Framework Community, comment out the line below.
pw.set_license_key("demo-license-key-with-telemetry")

dotenv.load_dotenv()


class FinancialStatementSchema(pw.Schema):
    company_symbol: str
    year: int
    quarter: str
    revenue_md: float
    eps: float
    net_income_md: float


class NLQuerySchema(pw.Schema):
    query: str
    user: str


@pw.udf
def build_prompt_structure(
    texts: list[str],
    max_tokens: int = 8000,
    encoding_name: str = "cl100k_base",
):
    """
    Insert instructions for the LLM here.
    max_tokens for the context. If gpt-3.5-turbo-16k is used, set it to 16k.
    """
    docs_str = " ".join(texts)
    encoding = tiktoken.get_encoding(encoding_name)
    prompt_prefix = "Given the following quarterly earnings release : \n"
    prompt_suffix = (
        f" \nfill in this schema for the quarter in question {FinancialStatementSchema.typehints()}\n"
        + """while respecting the instructions:
            - amounts should be in millions of dollars.
            - Parse quarterly data and ignore yearly records if present.
            - Your answer should be parseable by json. i.e. json.loads(response) doesn't throw any errors."""
    )

    prefix_tokens = len(list(encoding.encode_ordinary(prompt_prefix)))
    suffix_tokens = len(list(encoding.encode_ordinary(prompt_suffix)))

    # Calculate available tokens for docs_str
    available_tokens = max_tokens - (prefix_tokens + suffix_tokens)

    # Tokenize docs_str and truncate if needed
    doc_tokens = list(encoding.encode_ordinary(docs_str))
    if len(doc_tokens) > available_tokens:
        logging.warning("Document is too large for one query.")
        docs_str = encoding.decode(doc_tokens[:available_tokens])

    prompt = prompt_prefix + docs_str + prompt_suffix
    return prompt


@pw.udf
def build_prompt_query(postresql_table: str, query: str) -> str:
    prompt = f"""Transform the given query '{query}' into a specific SQL SELECT statement format.
    For invalid queries, return the string 'None'. The result should be executable in PostgreSQL.

    The query should include the following components:
    The SELECT clause should specify one or more columns from the table {postresql_table}.
    You can use column names or standard aggregator operators such as SUM, COUNT, MIN, MAX, or AVG
    to retrieve data from the columns.
    The WHERE clause should include conditions that use standard binary operators (e.g., <, >, =) to filter the data.
    You can use AND and OR operators to combine multiple conditions.
    If any columns from the WHERE clause are used in the conditions, please ensure that those columns are included
    in the GROUP BY clause to prevent the 'psycopg2.errors.GroupingError.'
    You may use logical reasoning to decide which columns should be part of the GROUP BY clause.

    The columns are from {postresql_table} table whose schema is:
    company_symbol (str)
    year (int)
    quarter (str)
    revenue_md (float)
    eps (float)
    net_income_md (float)

    Quarter values are Q1, Q2, Q3 or Q4.
    The company_symbol is the stock name: for example AAPL for Apple and GOOG for Google.

    If no aggregator are used, please always add the company_symbol, year,
    and quarter in addition of the wanted columns:
    "What is the net income of all companies?" should return:
    'SELECT company_symbol, net_income_md, quarter, net_income_md FROM {postresql_table};'

    Please ensure that the generated SQL query follows this structure and constraints.
    For example, a valid query might look like:
    'SELECT company_symbol, SUM(net_income_md) FROM {postresql_table}
    WHERE year = 2022 AND eps > 1.0 GROUP BY company_symbol;'


    Make sure the query adheres to the specified format, and that it includes GROUP BY clause
    if you aggregate results
    and do not include any other SQL commands or clauses besides the SELECT statement.
    Thank you!"""
    return prompt


@pw.udf
def parse_str_to_list(response: str) -> list:
    dct = json.loads(response)
    return [dct[k] for k in sorted(dct)]


def structure_on_the_fly(
    documents: pw.Table,
    api_key: str,
    model_locator: str,
    max_tokens: int,
    temperature: float,
):
    prompt = documents.select(prompt=build_prompt_structure(pw.this.texts))

    model = OpenAIChat(
        api_key=api_key,
        model=model_locator,
        temperature=temperature,
        max_tokens=max_tokens,
        retry_strategy=pw.udfs.ExponentialBackoffRetryStrategy(),
        cache_strategy=pw.udfs.DefaultCache(),
    )

    responses = prompt.select(
        result=model(prompt_chat_single_qa(pw.this.prompt)),
    )

    responses = responses.select(values=parse_str_to_list(pw.this.result))
    result = unpack_col(responses.values, *sorted(FinancialStatementSchema.keys()))
    result = result.with_columns(
        eps=pw.apply(float, pw.this.eps),
        net_income_md=pw.apply(float, pw.this.net_income_md),
        revenue_md=pw.apply(float, pw.this.revenue_md),
    )
    return result


def unstructured_query(
    postgreSQL_settings,
    postgreSQL_table,
    api_key: str,
    model_locator: str,
    max_tokens: int,
    temperature: float,
    host: str,
    port: int,
):
    query, response_writer = pw.io.http.rest_connector(
        host=host,
        port=port,
        schema=NLQuerySchema,
        autocommit_duration_ms=50,
        delete_completed_queries=True,
    )

    query += query.select(prompt=build_prompt_query(postgreSQL_table, pw.this.query))

    model = OpenAIChat(
        api_key=api_key,
        model=model_locator,
        temperature=temperature,
        max_tokens=max_tokens,
        retry_strategy=pw.udfs.ExponentialBackoffRetryStrategy(),
        cache_strategy=pw.udfs.DefaultCache(),
    )

    query += query.select(
        sql_query=model(prompt_chat_single_qa(pw.this.prompt)),
    )

    # Connecting to the document database for queries
    connection_string = psycopg.conninfo.make_conninfo(**postgreSQL_settings)
    conn = psycopg.connect(connection_string)
    cursor = conn.cursor()

    @pw.udf
    def execute_sql_query(sql_query):
        cursor.execute(sql_query)
        answer = cursor.fetchall()
        # answer = answer[0][0]
        conn.commit()
        return answer

    query = query.select(
        pw.this.query,
        pw.this.sql_query,
        result=execute_sql_query(
            pw.this.sql_query,
        ),
    )
    answers = query.select(result=pw.make_tuple(pw.this.sql_query, pw.this.result))
    response_writer(answers)


@pw.udf
def strip_metadata(docs: list[tuple[str, dict]]) -> list[str]:
    return [doc[0] for doc in docs]


def run(
    *,
    data_dir: str = os.environ.get("PATHWAY_DATA_DIR", "./data/quarterly_earnings"),
    api_key: str = os.environ.get("OPENAI_API_KEY", ""),
    host: str = os.environ.get("PATHWAY_REST_CONNECTOR_HOST", "0.0.0.0"),
    port: int = int(os.environ.get("PATHWAY_REST_CONNECTOR_PORT", "8080")),
    model_locator: str = "gpt-3.5-turbo",  # "gpt-4",  # gpt-3.5-turbo-16k
    max_tokens: int = 120,
    temperature: float = 0.0,
    postresql_host: str = os.environ.get("POSTGRESQL_HOST", "localhost"),
    postresql_port: str = os.environ.get("POSTGRESQL_PORT", "5432"),
    postresql_db: str = os.environ.get("POSTGRESQL_DB", "STRUCTUREDDB"),
    postresql_user: str = os.environ.get("POSTGRESQL_USER", "user"),
    postresql_password: str = os.environ.get("POSTGRESQL_PASSWORD", "password"),
    postresql_table: str = os.environ.get("POSTGRESQL_TABLE", "quarterly_earnings"),
    **kwargs,
):
    #
    # # Pipeline 1 - parsing documents into a PostgreSql table
    #
    postgreSQL_settings = {
        "host": postresql_host,
        "port": postresql_port,
        "dbname": postresql_db,
        "user": postresql_user,
        "password": postresql_password,
    }

    files = pw.io.fs.read(
        data_dir,
        format="binary",
    )
    parser = UnstructuredParser()
    unstructured_documents = files.select(texts=parser(pw.this.data)).select(
        texts=strip_metadata(pw.this.texts)
    )
    structured_table = structure_on_the_fly(
        unstructured_documents, api_key, model_locator, max_tokens, temperature
    )
    pw.io.postgres.write(structured_table, postgreSQL_settings, postresql_table)
    pw.io.csv.write(structured_table, "./data/quarterly_earnings.csv")

    #
    # # Pipeline 2 - query answering using PostgreSql
    #
    unstructured_query(
        postgreSQL_settings,
        postresql_table,
        api_key,
        model_locator,
        max_tokens,
        temperature,
        host,
        port,
    )

    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


if __name__ == "__main__":
    run()

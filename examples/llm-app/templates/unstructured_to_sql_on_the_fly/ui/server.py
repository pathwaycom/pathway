import os

import pandas as pd
import requests
import streamlit as st

api_host = os.environ.get("PATHWAY_HOST", "localhost")
api_port = os.environ.get("PATHWAY_PORT", 8000)

with st.sidebar:
    st.markdown(
        "## How to query your data\n"
        "Enter your question about financial data\n"
        "Example: What is the average quarterly net income achieved by all companies in the fourth quarter?"
    )
    st.markdown("---")
    st.markdown("# Units")
    st.markdown(
        "⚠️ The revenue and net income are expressed in millions of dollars,"
        " the eps is in dollars per share."
    )
    st.markdown("---")
    st.markdown("# About")
    st.markdown(
        "Financial LLM app to synthesize on the fly the data in your financial documents."
        "It uses Pathway Live Data Framework's [LLM features](https://github.com/pathwaycom/pathway) "
        "to build and maintain a real-time database and synthesize your documents "
        " using LLM(Large Language Model) and store the data in PostgreSQL.\n"
    )
    st.markdown(
        """[View the source code on GitHub](
https://github.com/pathwaycom/pathway/blob/main/examples/llm-app/templates/unstructured_to_sql_on_the_fly/app.py)"""
    )


# Streamlit UI elements
st.title("📈 Financial summary with LLM App")

question = st.text_input(
    "Search for something",
    placeholder="What summary are looking for?",
)


def json_to_table(json_value):
    result = ""
    for row in json_value:
        for col_value in row:
            result = result + str(col_value) + "\t"
        result = result + "\n"
    return result


# Handle Discounts API request if data source is selected and a question is provided
if question:
    url = f"http://{api_host}:{api_port}/"
    try:
        data = {"user": "user", "query": question}
        response = requests.post(url, json=data)

        if response.status_code == 200:
            response_JSON = response.json()
            sql_query = response_JSON[0]
            answer = response_JSON[1]
            dataframe = pd.DataFrame.from_records(answer)
            st.write("### Answer")
            st.write("Resulting SQL query:")
            st.write(sql_query)
            st.write("SQL Answer:")
            st.dataframe(dataframe)
        else:
            st.error(
                f"Failed to send data to Finance API. Status code: {response.status_code}"
            )
    except Exception as e:
        st.write("### Parsing error:")
        st.write("Couldn't parse the entry.")
        st.write(str(e))

import os
from enum import auto

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from querying import register_table, run_with_querying
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pathway as pw


def send_table_to_web(port: int, table: pw.Table, alias: str):
    # Stream the table so you can later get the data via requests
    register_table(table, alias)

    run_with_querying(
        pathway_kwargs=dict(monitoring_level=pw.MonitoringLevel.NONE),
        uvicorn_kwargs=dict(host="0.0.0.0", port=port),
    )


class DataModes(auto):
    STATIC = 0
    REPLAY = 1
    LIVE = 2
    name = ["STATIC", "REPLAY", "LIVE"]


if __name__ == "__main__":
    # Change here the mode you want your data in
    mode = DataModes.STATIC
    load_dotenv()
    port = int(os.environ.get("PORT"))

    st.title("Option Greeks")
    st.markdown(
        "This app will compute the Option Greeks. Each option is identified by `instrument_id`."
    )
    st.markdown(
        f"You work in a {DataModes.name[mode]} mode in this case. This can be changed in `app.py`, for now."
    )

    url = f"http://localhost:{port}/get_table?alias=table_greeks"

    # requests.get() might not work, check this link for more details.
    # https://stackoverflow.com/questions/23013220/max-retries-exceeded-with-url-in-requests

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    table_request = session.get(url)

    print(table_request.status_code)

    if table_request.status_code == 200:
        response_json = table_request.json()
        df = pd.DataFrame.from_records(response_json).set_index("instrument_id")
        df["ts_recv"] = pd.to_datetime(df["ts_recv"] / 1e9, unit="s")
        st.write("Computed and got Greeks!")
        st.dataframe(df)
    else:
        st.error(
            f"Failed to send data. Failed with status code {table_request.status_code}"
        )

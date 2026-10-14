import os

import requests
import streamlit as st
from dotenv import load_dotenv

api_host = "localhost"
api_port = 8080

load_dotenv()
api_host = os.environ.get("PATHWAY_REST_CONNECTOR_HOST", "127.0.0.1")
api_port = int(os.environ.get("PATHWAY_REST_CONNECTOR_PORT", 8080))

with st.sidebar:
    st.markdown("## How to query your data\n")
    st.markdown(
        """Enter your question, optionally
ask to be alerted.\n"""
    )
    st.markdown(
        "Example: 'When does the magic cola campaign start? Alert me if the start date changes'",
    )
    st.markdown(
        """[View the source code on GitHub](
https://github.com/pathwaycom/pathway/blob/main/examples/llm-app/templates/drive_alert/app.py)"""
    )
    st.markdown("## Current Alerts:\n")


# Streamlit UI elements
st.title("Google Drive notifications with LLM")

prompt = st.text_input("How can I help you today?")
# prompt = st.chat_input("How can I help you today?")
# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


# React to user input
if prompt:
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    for message in st.session_state.messages:
        if message["role"] == "user":
            st.sidebar.text(f"📩 {message['content']}")

    url = f"http://{api_host}:{api_port}/"
    data = {"query": prompt, "user": "user"}

    response = requests.post(url, json=data)

    if response.status_code == 200:
        response = response.json()
        with st.chat_message("assistant"):
            st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})
    else:
        st.error(f"Failed to send data. Status code: {response.status_code}")

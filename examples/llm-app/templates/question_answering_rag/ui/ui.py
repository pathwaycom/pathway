# Copyright © 2026 Pathway

import logging
import os

import streamlit as st
from dotenv import load_dotenv

from pathway.xpacks.llm.document_store import IndexingStatus
from pathway.xpacks.llm.question_answering import RAGClient

load_dotenv()

PATHWAY_HOST = os.environ.get("PATHWAY_HOST", "app")
PATHWAY_PORT = os.environ.get("PATHWAY_PORT", 8000)

st.set_page_config(
    page_title="Pathway Live Data Framework RAG App", page_icon="favicon.ico"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)

logger = logging.getLogger("streamlit")
logger.setLevel(logging.INFO)

conn = RAGClient(url=f"http://{PATHWAY_HOST}:{PATHWAY_PORT}")

note = """
<H4><b>Ask a question"""
st.markdown(note, unsafe_allow_html=True)

st.markdown(
    """
<style>
div[data-baseweb="base-input"]{
}
input[class]{
font-size:150%;
color: black;}
button[data-testid="baseButton-primary"], button[data-testid="baseButton-secondary"]{
    border: none;
    display: flex;
    background-color: #E7E7E7;
    color: #454545;
    transition: color 0.3s;
}
button[data-testid="baseButton-primary"]:hover{
    color: #1C1CF0;
    background-color: rgba(28,28,240,0.3);
}
button[data-testid="baseButton-secondary"]:hover{
    color: #DC280B;
    background-color: rgba(220,40,11,0.3);
}
div[data-testid="stHorizontalBlock"]:has(button[data-testid="baseButton-primary"]){
    display: flex;
    flex-direction: column;
    z-index: 0;
    width: 3rem;

    transform: translateY(-500px) translateX(672px);
}
</style>
""",
    unsafe_allow_html=True,
)


question = st.text_input(label="", placeholder="Ask your question?")


def get_indexed_files(metadata_list: list[dict], opt_key: str) -> list:
    """Get all available options in a specific metadata key."""
    only_indexed_files = [
        file
        for file in metadata_list
        if file["_indexing_status"] == IndexingStatus.INDEXED
    ]
    options = set(map(lambda x: x[opt_key], only_indexed_files))
    return list(options)


def get_ingested_files(metadata_list: list[dict], opt_key: str) -> list:
    """Get all available options in a specific metadata key."""
    not_indexed_files = [
        file
        for file in metadata_list
        if file["_indexing_status"] == IndexingStatus.INGESTED
    ]
    options = set(map(lambda x: x[opt_key], not_indexed_files))
    return list(options)


logger.info("Requesting list_documents...")
document_meta_list = conn.list_documents(keys=[])
logger.info("Received response list_documents")

st.session_state["document_meta_list"] = document_meta_list

indexed_files = get_indexed_files(st.session_state["document_meta_list"], "path")
ingested_files = get_ingested_files(st.session_state["document_meta_list"], "path")


logo_htm = """
<div>
    <figure style="display: flex; align-items: center; margin: 0;">
        <img style="max-width:300px" src="app/static/pathway-logo-black.png" alt="Pathway Logo">
    </figure>
</div>
"""

with st.sidebar:
    st.markdown(logo_htm, unsafe_allow_html=True)

    st.info(
        body="See the source code [here](https://github.com/pathwaycom/pathway/tree/main/examples/llm-app/templates/question_answering_rag).",  # noqa: E501
        icon=":material/code:",
    )

    indexed_file_names = [i.split("/")[-1] for i in indexed_files]
    ingested_file_names = [i.split("/")[-1] for i in ingested_files]

    markdown_table = "| Indexed files |\n| --- |\n"
    for file_name in indexed_file_names:
        markdown_table += f"| {file_name} |\n"

    if len(ingested_file_names) > 0:
        markdown_table += "| Files being processed |\n| --- |\n"
        for file_name in ingested_file_names:
            markdown_table += f"| {file_name} |\n"

    st.markdown(markdown_table, unsafe_allow_html=True)

    st.button("⟳ Refresh", use_container_width=True)

css = """
<style>
.slider-container {
    margin-top: 20px; /* Add some space between the main image and the slider */
}

.slider-item {
    float: left;
    margin: 10px;
    width: 120px; /* Adjust the width to your liking */
    // height: 50px; /* Adjust the height to your liking */
    border: 1px solid #ccc;
    border-radius: 5px;
    cursor: pointer;
}

.slider-item img {
    width: 100%;
    height: 100%;
    object-fit: cover;
    border-radius: 5px;
}

.slider-wrapper {
    display: flex;
    justify-content: center;
    flex-wrap: wrap;
}

.slider-item {
    margin: 10px;
}

</style>"""


st.markdown(css, unsafe_allow_html=True)


if question:
    logger.info(
        {
            "_type": "search_request_event",
            "query": question,
        }
    )

    with st.spinner("Retrieving response..."):
        api_response = conn.answer(question, return_context_docs=True)
        response = api_response["response"]
        context_docs = api_response["context_docs"]

    logger.info(
        {
            "_type": "search_response_event",
            "query": question,
            "response": type(response),
        }
    )

    logger.info(type(response))

    st.markdown(f"**Answering question:** {question}")
    st.markdown(f"""{response}""")
    with st.expander(label="Context documents"):
        st.markdown("Documents sent to LLM as context:\n")
        for i, doc in enumerate(context_docs):
            st.markdown(
                f"{i+1}. Path: {doc['metadata']['path']}\n ```\n{doc['text']}\n```"
            )

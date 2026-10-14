# Copyright © 2026 Pathway

import logging
import os
import urllib.parse
from itertools import cycle
from pathlib import PurePosixPath

import requests
import streamlit as st
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from pathway.xpacks.llm.question_answering import RAGClient

load_dotenv()

PATHWAY_HOST = os.environ.get("PATHWAY_HOST", "app")
PATHWAY_PORT = os.environ.get("PATHWAY_PORT", 8000)

st.set_page_config(page_title="Find the right slide", page_icon="favicon.ico")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)

logger = logging.getLogger("streamlit")
logger.setLevel(logging.INFO)

conn = RAGClient(url=f"http://{PATHWAY_HOST}:{PATHWAY_PORT}")

file_server_base_url = os.environ.get("FILE_SERVER_URL", "http://localhost:8080/")

file_server_image_base_url = f"{file_server_base_url}images"

file_server_pdf_base_url = f"{file_server_base_url}documents"
internal_file_server_pdf_base_url = "http://nginx:8080/"

note = """
<H4><b>Search your slide decks"""
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


question = st.text_input(label="", placeholder="What are you searching for?")


def get_options_list(metadata_list: list[dict], opt_key: str) -> list:
    """Get all available options in a specific metadata key."""
    options = set(map(lambda x: x[opt_key], metadata_list))
    return list(options)


def parse_slide_id_components(slide_id: str) -> tuple[str, int, int]:
    stem = PurePosixPath(slide_id).stem
    (name_page, _, page_count) = stem.rpartition("_")
    (name, _, page) = name_page.rpartition("_")
    return (name, int(page), int(page_count))


def create_slide_url(name: str, page: int, page_count: int) -> str:
    return f"{file_server_image_base_url}/{name}_{page}_{page_count}.png"


def get_image_serve_url(metadata: dict) -> str:
    name, page, page_count = parse_slide_id_components(metadata["slide_id"])
    return create_slide_url(name, page, page_count)


def get_adjacent_image_urls(metadata: dict) -> list[str]:
    logger.info(
        {"_type": "create_adjacent_image_urls", "slide_id": metadata["slide_id"]}
    )

    name, page, page_count = parse_slide_id_components(metadata["slide_id"])

    ret_images = []
    for page in range(page - 2, page + 3):
        if page < 0 or page >= page_count:
            continue
        ret_images.append(create_slide_url(name, page, page_count))
    return ret_images


st.session_state["available_categories"] = None
st.session_state["available_languages"] = None

logger.info("Requesting list_documents...")
document_meta_list = conn.list_documents(keys=[])
logger.info("Received response list_documents")

st.session_state["document_meta_list"] = document_meta_list


available_categories = get_options_list(document_meta_list, "category")
st.session_state["available_categories"] = available_categories

available_languages = get_options_list(document_meta_list, "language")
st.session_state["available_languages"] = available_languages


available_files = get_options_list(st.session_state["document_meta_list"], "path")


def get_slide_link(file_name, page_num=None) -> str:
    filename_encoded = urllib.parse.quote(file_name)
    image_url = f"{file_server_pdf_base_url}/{filename_encoded}"
    if page_num is not None:
        image_url += f"#page={page_num}"
    return image_url


def get_all_index_files() -> list[str]:
    logger.info("request get_all_index_files")
    response = requests.get(internal_file_server_pdf_base_url + "/")
    logger.info("response get_all_index_files")

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        file_links = [a["href"] for a in soup.find_all("a", href=True)]

        file_links = [link for link in file_links if not link.endswith("/")]
    else:
        file_links = []
    return file_links


with st.sidebar:
    st.info(
        """This demo app only allows `PDF` and `PPTX` documents.
        For other file types, convert to `PDF` or contact **Pathway**."""
    )
    st.info(
        body="See the source code [here](https://github.com/pathwaycom/pathway/tree/main/examples/llm-app/templates/slides_ai_search).",  # noqa: E501
        icon=":material/code:",
    )

    file_names = [i.split("/")[-1] for i in available_files]
    links = [get_slide_link(i) for i in file_names]

    markdown_table = "| Slides Ready for Search |\n| --- |\n"
    for file_name, link in zip(file_names, links):
        markdown_table += f"| [{file_name}]({link}) |\n"
    st.markdown(markdown_table, unsafe_allow_html=True)

    all_drive_files = get_all_index_files()
    all_drive_files = [urllib.parse.unquote(i) for i in all_drive_files]
    all_drive_files = [
        i for i in all_drive_files if i.endswith(".pdf") or i.endswith(".pptx")
    ]
    logger.info(f"All source files: {all_drive_files}\nIndexed files: {file_names}")
    currently_processing_files = set(all_drive_files) - set(file_names)

    st.markdown("\n\n", unsafe_allow_html=True)

    if currently_processing_files:
        markdown_table = "| Indexing in Progress |\n| --- |\n"

        links = [get_slide_link(i) for i in currently_processing_files]
        for file_name, link in zip(currently_processing_files, links):
            markdown_table += f"| [{file_name}]({link}) |\n"
        st.markdown(markdown_table, unsafe_allow_html=True)
    else:
        st.markdown("## All files indexed.")

    st.button("⟳ Refresh", use_container_width=True)


category_options = st.session_state["available_categories"]

lang_options = st.session_state["available_languages"]

cols = cycle(st.columns(2))

with next(cols):
    cat_options = st.multiselect(
        "Filtered Categories",
        category_options or [],
        [],
        key="cat_selection",
        label_visibility="hidden",
        placeholder="Filtered Categories",
    )

with next(cols):
    language_options = st.multiselect(
        "Languages",
        lang_options or [],
        [],
        key="lang_selection",
        label_visibility="hidden",
        placeholder="Filtered Languages",
    )

with st.sidebar:
    cat_prefix = "cat_"

    logger.info("All category options: %s", category_options)

    selected_categories = category_options if len(cat_options) == 0 else cat_options

    logger.info("Selected categories: %s", selected_categories)

    lang_prefix = "lang_"

    selected_languages = (
        lang_options if len(language_options) == 0 else language_options
    )

    st.session_state.category_filter = selected_categories
    st.session_state.language_filter = selected_languages


def get_category_filter(category: str) -> str:
    return f"contains(`{str(category)}`, category)"


# TODO: merge these
def get_language_filter(lang: str) -> str:
    return f"contains(`{str(lang)}`, language)"


def combine_filters(*args: str | None) -> str:
    """Construct single jmespath filter with `&&` from number of filters."""
    return " && ".join([arg for arg in args if arg is not None])


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


def get_ext_img_with_href(url, target_url, *args) -> str:
    width: int = 600
    margin = 20

    def get_img_html(dc):
        return f"""
        <div class="slider-item">
                    <img src="{dc['url']}" width="90" />
                </div>"""

    slider_images = "\n".join([get_img_html(dc) for dc in args])

    html_code = f"""
        <a href="{target_url}">
        <img style="display: block; margin: {margin}px auto {margin}px auto" loading="lazy" src="{url}" width="{width}" />
        </a>

        <div class="slider-container">
            <!-- Slider wrapper -->
            <div class="slider-wrapper">
                <!-- Slider items -->
                    {slider_images}
                <!-- Add more slider items here -->
            </div>
        </div>
        """  # noqa: E501
    return html_code


def log_rate_answer(event, idx, kwargs):
    logger.info({"_type": "rate_event", "rating": event, "rank": idx, **kwargs})


if question:
    select_cat = st.session_state.category_filter
    select_lang = st.session_state.language_filter

    filter_ls = [get_category_filter(select_cat), get_language_filter(select_lang)]

    combined_query_filter = combine_filters(*filter_ls)

    logger.info(
        {
            "_type": "search_request_event",
            "filter": combined_query_filter,
            "query": question,
        }
    )

    response = conn.answer(question, filters=combined_query_filter)

    logger.info(
        {
            "_type": "search_response_event",
            "filter": combined_query_filter,
            "query": question,
            "response": type(response),
        }
    )

    if response:
        logger.info(type(response[0]))

        text_responses = [r["text"] for r in response]

        image_metadatas = [r["metadata"] for r in response]

        for m in image_metadatas:
            logger.info("Retrieved metadatas: %s || %s", m["language"], m["category"])

        st.markdown(f"**Searched for:** {question}")

        for idx, cur_metadata in enumerate(image_metadatas):
            file_name = cur_metadata["path"].split("/")[-1]

            select_page = cur_metadata["image_page"] + 1

            adjacent_urls = get_adjacent_image_urls(cur_metadata)

            args = [{"url": i} for i in adjacent_urls]

            image_html = get_ext_img_with_href(
                get_image_serve_url(cur_metadata),
                get_slide_link(file_name, select_page),
                *args,
            )

            image_url = get_slide_link(file_name, select_page)

            slide_id = cur_metadata["slide_id"]

            st.markdown(f"Page `{select_page}` of [`{file_name}`]({image_url})")

            st.markdown(image_html, unsafe_allow_html=True)

            log_args = (
                idx,
                {
                    "slide_id": slide_id,
                    "filter": combined_query_filter,
                    "query": question,
                    "file_name": file_name,
                    "selected_cat": select_cat,
                    "selected_lang": select_lang,
                },
            )
            col1, col2, col3 = st.columns([12, 1, 1])

    else:
        st.markdown(
            f"""No results were found for search query: `{question}`
            and filter criteria: `{combined_query_filter}`"""
        )

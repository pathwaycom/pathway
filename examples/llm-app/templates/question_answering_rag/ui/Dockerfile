ARG PATHWAY_SRC_IMAGE=pathwaycom/pathway:latest

FROM ${PATHWAY_SRC_IMAGE}

ENV PYTHONUNBUFFERED=1

WORKDIR /ui

COPY requirements.txt .
RUN pip install -U --no-cache-dir -r requirements.txt

COPY . .

CMD exec streamlit run ui.py --server.port ${UI_PORT}

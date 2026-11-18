ARG PATHWAY_SRC_IMAGE=pathwaycom/pathway:latest

FROM ${PATHWAY_SRC_IMAGE}

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    poppler-utils \
    libreoffice \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .
RUN pip install -U --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "app.py" ]

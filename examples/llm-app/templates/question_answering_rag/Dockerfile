ARG PATHWAY_SRC_IMAGE=pathwaycom/pathway:latest

FROM ${PATHWAY_SRC_IMAGE}

WORKDIR /app

RUN apt-get update \
    && apt-get install -y python3-opencv tesseract-ocr-eng poppler-utils libreoffice \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

COPY requirements.txt .
RUN pip install -U --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "app.py"]

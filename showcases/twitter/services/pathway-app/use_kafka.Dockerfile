FROM python:3.10
ARG PATHWAY_INDEX_URL
RUN status_code=$(curl --write-out %{http_code} --silent --output /dev/null $PATHWAY_INDEX_URL) &&\
    if [ "${status_code}" -gt "399" ]; then \
    echo "⛔ Wrong Pathway package url"; \
    echo "Please register at https://pathway.com/developers/documentation/introduction/installation-and-first-steps"; \
    echo "and paste the link under PATHWAY_INDEX_URL in settings.env file."; \
    exit 1; \
    fi
WORKDIR /pathway_app
COPY ./requirements.txt /pathway_app/requirements.txt
RUN pip install --no-cache-dir --upgrade --extra-index-url $PATHWAY_INDEX_URL -r /pathway_app/requirements.txt
COPY ./app /pathway_app/app
COPY ./geolocator_cache.pkl.gz /pathway_app
CMD ["python", "app/main.py"]

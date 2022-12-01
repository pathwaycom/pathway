FROM python:3.10
WORKDIR /pathway_app
COPY ./requirements.txt /pathway_app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /pathway_app/requirements.txt
COPY ./app /pathway_app/app
COPY ./geolocator_cache.pkl.gz /pathway_app
CMD ["python", "app/main.py", "--dataset-path", "/shared-volume", "--poll-new-objects", "true"]

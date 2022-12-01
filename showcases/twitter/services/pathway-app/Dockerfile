FROM python:3.10
WORKDIR /pathway_app
COPY ./requirements.txt /pathway_app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /pathway_app/requirements.txt
COPY ./app /pathway_app/app
COPY ./filter_vmas.txt.gz /pathway_app/
COPY ./geolocator_cache.pkl.gz /pathway_app
RUN gunzip /pathway_app/filter_vmas.txt.gz
CMD ["python", "app/main.py", "--dataset-path", "filter_vmas.txt"]
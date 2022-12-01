FROM python:3.10
WORKDIR /dataset_replay
COPY ./requirements.txt /dataset_replay/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /dataset_replay/requirements.txt
COPY ./filter_vmas.txt.gz /dataset_replay/
RUN gunzip /dataset_replay/filter_vmas.txt.gz
COPY ./app /dataset_replay/app
CMD ["python", "app/main.py", "--dataset-path", "filter_vmas.txt", "--speed", "50", "--batch-size", "1000"]

FROM python:3.10

RUN pip install kafka-python
COPY ./producer-src/create-stream.py create-stream.py
COPY ./producer-src/dataset.csv dataset.csv

CMD ["python", "-u", "create-stream.py"]
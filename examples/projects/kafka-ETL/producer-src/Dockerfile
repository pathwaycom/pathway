FROM python:3.10

RUN pip install kafka-python
COPY ./producer-src/create-stream.py create-stream.py

CMD ["python", "create-stream.py"]

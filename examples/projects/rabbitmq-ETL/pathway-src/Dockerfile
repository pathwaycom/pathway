FROM python:3.10

RUN pip install -U pathway
COPY ./pathway-src/etl.py etl.py

CMD ["python", "etl.py"]

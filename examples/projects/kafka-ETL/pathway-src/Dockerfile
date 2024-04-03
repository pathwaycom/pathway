FROM python:3.10

RUN pip install -U pathway
COPY ./pathway-src/etl.py etl.py
COPY ./pathway-src/read-results.py read-results.py

CMD ["python", "etl.py"]

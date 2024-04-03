FROM python:3.10

RUN pip install -U pathway
COPY ./pathway-src/process-stream.py process-stream.py

CMD ["python", "-u", "process-stream.py"]
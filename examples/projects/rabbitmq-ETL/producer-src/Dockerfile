FROM python:3.10

ENV PYTHONUNBUFFERED=1
RUN pip install rstream
COPY ./producer-src/create-streams.py create-streams.py

CMD ["python", "create-streams.py"]

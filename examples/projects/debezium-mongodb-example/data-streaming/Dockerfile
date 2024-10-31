FROM --platform=linux/x86_64 python:3.10

RUN pip install -U pymongo
RUN pip install -U pathway
COPY ./data-streaming/streamer.py streamer.py

CMD ["python", "-u", "streamer.py"]

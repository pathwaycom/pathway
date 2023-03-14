FROM --platform=linux/x86_64 python:3.10

RUN pip install --extra-index-url https://packages.pathway.com/966431ef6ba pathway
RUN pip install kafka-python
COPY ./pathway-src/sum.py sum.py

CMD ["python", "-u", "sum.py"]
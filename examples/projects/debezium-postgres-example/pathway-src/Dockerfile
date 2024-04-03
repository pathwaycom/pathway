FROM --platform=linux/x86_64 python:3.10

RUN pip install -U pathway
RUN pip install kafka-python
COPY ./pathway-src/sum.py sum.py

CMD ["python", "-u", "sum.py"]
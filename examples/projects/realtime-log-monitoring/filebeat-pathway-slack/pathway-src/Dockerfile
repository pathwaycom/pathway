FROM --platform=linux/x86_64 python:3.10

RUN pip install -U pathway
RUN pip install requests
RUN pip install python-dateutil
COPY ./pathway-src/alerts.py alerts.py

CMD ["python", "-u", "alerts.py"]
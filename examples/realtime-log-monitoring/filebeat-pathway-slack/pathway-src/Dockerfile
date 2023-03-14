FROM --platform=linux/x86_64 python:3.10

RUN pip install --extra-index-url https://packages.pathway.com/966431ef6ba pathway
RUN pip install requests
RUN pip install python-dateutil
COPY ./pathway-src/alerts.py alerts.py

CMD ["python", "-u", "alerts.py"]
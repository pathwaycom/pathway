FROM python:3.10

RUN pip install --extra-index-url https://packages.pathway.com/... pathway
RUN pip install requests
COPY ./pathway-src/alerts.py alerts.py

CMD ["python", "-u", "alerts.py"]
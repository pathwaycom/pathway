FROM python:3.10

RUN pip install --extra-index-url https://packages.pathway.com/966431ef6b789140248947273e541114f6679aa3436359bb5d67064b5693047a90ec699d6a pathway
RUN pip install kafka-python
COPY ./pathway-src/sum.py sum.py

CMD ["python", "-u", "sum.py"]
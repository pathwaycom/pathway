FROM --platform=linux/x86_64 python:3.10

RUN pip install --no-cache --prefer-binary --pre -U --extra-index-url https://packages.pathway.com/966431ef6ba pathway
COPY ./pathway-src/sum.py sum.py

CMD ["python", "-u", "sum.py"]

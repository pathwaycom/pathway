FROM python:3.10

COPY ./github-config.yaml github-config.yaml
COPY ./main.py main.py
COPY ./requirements.txt requirements.txt

RUN apt update
RUN apt install -y default-jre
RUN pip install --no-cache --prefer-binary --pre -U --extra-index-url https://packages.pathway.com/966431ef6ba -r requirements.txt

CMD ["python", "main.py"]

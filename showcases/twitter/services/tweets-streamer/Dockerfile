FROM python:3.10
WORKDIR /tweets_stream
COPY ./requirements.txt /tweets_stream/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /tweets_stream/requirements.txt
COPY ./app /tweets_stream/app
CMD ["python", "app/main.py", "--max-batch-size", "20", "--max-batch-lifetime-sec", "60", "--tweets-limit", "1000", "--rule", "France"]

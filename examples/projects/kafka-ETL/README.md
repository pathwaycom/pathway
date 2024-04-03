# ETL with Kafka in/Kafka out

In this project, you build an ETL pipeline with Pathway with Kafka in and Kafka out.
Two data sources broadcast date times with different time zones to different Kafka topics.
Pathway connects to those topics, extracts (E) the data, transforms (T) the date times
to timestamps, and loads (L) the data to a third Kafka topic.

The project is organized as follows:
 - a streaming platform: Kafka/ZooKeeper
 - a Python container creating the data streams and sending them to Kafka
 - a Python container doing the ETL using Pathway, sending the results back to Kafka

The containers are managed via docker-compose.

## Launching the program

You can launch the project using `docker compose up -d` or using the Makefile (`make`).

You can access the logs of the containers using the commands in the Makefile.

### Reading the results

The example is now running, and the date times are transformed and loaded into
the `unified_timestamps` topic of the Kafka instance.
You can access those results with your favorite way to access Kafka.

You can read the results and output them as a CSV file by using the `read-results.py`
provided in the container running Pathway.
Do as follows:
 - connect to the container with `make connect` (or `docker compose exec -it pathway bash`)
 - launch the script with `python read-results.py`
Pathway reads the incoming data and outputs it in the `results.csv` file.
New data will be automatically added.
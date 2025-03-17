# Realtime monitoring of logs

The purpose of this project is to do an end-to-end application with Pathway to monitors logs (such as nginx logs).
It connects Filebeat/Logstash to Pathway (via Kafka) and send the alerts to ElasticSearch.

The project is organized in six docker containers:
 - Filebeat: this container contains the logs, and Filebeat is launch to monitors them and then the updates to Logstash.
 - Logstash: Logstash forwards the logs from Filebeat to Kafka.
 - Kafka and Zookeeper: these containers are working as a gateway between Filebeat and Pathway.
 - Pathway: the logs are received from Kafka, processed and send to ElasticSearch.
 - ElasticSearch: the alerts processed by Pathway are stored in this container.
 
The logs are processed as follows (in `./pathway-src/alerts.py`):
 1. The timestamps and messages (logs) are extracted from the JSON messages created by Filebeat.
 2. The timestamps are converted to real timestamps (instead of a ISO8601 format).
 3. We only keep the messages of the last X seconds (X=1s by default). Here the current time is the timestamp of the last log.
 4. If there are more than Y messages (Y=5 by default), we output alert=True.

To install the project, you only need to clone the repository.

How to launch the project:
 1. `make` in the root repo. It will start all the four containers
 2. `make connect` to connect to the Filebeat container.
 3. `./generate_input_stream.sh` to launch the stream generation from the Filebeat container.

The updates should be received directly in ElasticSearch.
To access the logs in ElasticSearch, you can do `curl localhost:9200/alerts/_search?pretty`.
Note that ElasticSearch takes a few seconds before being available: you should wait ~15s before accessing it and generating the logs.

You can use `make connect-pathway` to connect to the pathway container.
By adding a `pw.io.csv.write(log_table, "./logs.csv")` you can see the logs by typing `cat logs.csv` from the pathway container.

To stop, use `make stop`.
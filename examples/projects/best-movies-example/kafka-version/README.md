# Best-rated movies example - Kafka version

In this project, you build an end-to-end application with Pathway to compute the K-best-rated items in a movie dataset.

The project is organized as followed:
 - a streaming platform: Kafka/ZooKeeper
 - a Python container creating the stream from a static CSV file and sending it to Kafka
 - a Python container computing the K-best-rated items using Pathway

The different containers are managed via docker-compose.

A toy dataset is provided in the sources, formatted in the same way as the [MovieLens25M dataset](https://grouplens.org/datasets/movielens/25m/):
we encourage you to test it on the entire dataset.

## Launching the program

You can launch the project using `docker compose up -d` or using the make file.

To access the Pathway's container and check the results:
 - connect to the container with `make connect` (or `docker compose exec -it pathway bash`)
 - print the file `cat best_ratings.csv`

You can access the logs of the different containers using the commands in the Makefile
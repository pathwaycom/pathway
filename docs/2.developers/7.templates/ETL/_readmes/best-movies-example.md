# Best-rated movies examples

The purpose of this project is two-fold:
 1. doing an end-to-end application with Pathway to compute the K-best-rated items in a movie dataset,
 2. switching from Kafka to Redpanda

The aim is to show that Pathway can be used the same way independently of the choice between Kafka and Redpanda.

The project is organized as followed:
 - a streaming platform, Kafka/ZooKeeper or Redpanda depending on the project
 - a Python container creating the stream from a static CSV file and sending it to Kafka/Redpanda
 - a Python container computing the K-best-rated items using Pathway

The different containers are managed via docker-compose.

A toy dataset is provided in the sources, formatted in the same way as the [MovieLens25M dataset](https://grouplens.org/datasets/movielens/25m/):
we encourage you to test it on the entire dataset.

A Makefile is provided for each project.
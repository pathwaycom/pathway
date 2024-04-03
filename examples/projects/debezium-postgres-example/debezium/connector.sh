#!/bin/bash

curl -H 'Content-Type: application/json' debezium:8083/connectors --data '
{
  "name": "values-connector",  
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector", 
    "plugin.name": "pgoutput",
    "database.hostname": "postgres", 
    "database.port": "5432", 
    "database.user": "user", 
    "database.password": "password", 
    "database.dbname" : "values_db", 
    "database.server.name": "postgres", 
    "table.include.list": "public.values",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.inventory"
  }
}'
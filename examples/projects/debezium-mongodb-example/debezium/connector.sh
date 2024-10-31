#!/bin/bash

while true; do
  http_code=$(curl -o /dev/null -w "%{http_code}" -H 'Content-Type: application/json' debezium:8083/connectors --data '{
    "name": "values-connector",  
    "config": {
      "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
      "mongodb.hosts": "rs0/mongodb:27017",
      "mongodb.name": "my_mongo_db",
      "database.include.list": "test_database",
      "database.history.kafka.bootstrap.servers": "kafka:9092",
      "database.history.kafka.topic": "dbhistory.mongo"
    }
  }')
  if [ "$http_code" -eq 201 ]; then
    echo "Debezium connector has been created successfully"
    break
  else
    echo "Retrying Debezium connection creation in 1 second..."
    sleep 1
  fi
done

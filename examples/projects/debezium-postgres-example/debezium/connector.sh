#!/bin/bash

while true; do
  http_code=$(curl -o /dev/null -w "%{http_code}" -H 'Content-Type: application/json' debezium:8083/connectors --data '{
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
  }')
  if [ "$http_code" -eq 201 ]; then
    echo "Debezium connector has been created successfully"
    break
  else
    echo "Retrying Debezium connection creation in 1 second..."
    sleep 1
  fi
done

import time

import pathway as pw

# Debezium settings
input_rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
}
output_postgres_settings = {
    "host": "postgres",
    "port": "5432",
    "dbname": "values_db",
    "user": "user",
    "password": "password",
}


print("Imports OK!")
time.sleep(10)
print("Starting Pathway:")


# We use the Kafka connector to listen to the "connector_example" topic
t = pw.debezium.read(
    input_rdkafka_settings,
    topic_name="postgres.public.values",
    value_columns=["value"],
    autocommit_duration_ms=100,
)
t = t.debug("t")
print(t)

# # We compute the sum (this part is independent of the connectors).
t = t.select(value=pw.apply_with_type(int, int, t.value))
t = t.reduce(sum=pw.reducers.sum(t.value))

# print(t)

# We use the Kafka connector to send the resulting output stream containing the sum
pw.csv.write(t, "essai.csv")
pw.postgres.write(t, output_postgres_settings, "sum_table")

# We launch the computation.
pw.run()

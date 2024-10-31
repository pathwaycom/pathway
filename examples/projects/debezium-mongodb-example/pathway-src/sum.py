# Copyright © 2024 Pathway

import pathway as pw

# To use advanced features with Pathway Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Community, comment out the line below.
pw.set_license_key("demo-license-key-with-telemetry")

# Kafka settings
input_rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}


class InputSchema(pw.Schema):
    value: int


if __name__ == "__main__":
    # The Kafka connector is used to listen to the "my_mongo_db.test_database.values" topic.
    t = pw.io.debezium.read(
        input_rdkafka_settings,
        topic_name="my_mongo_db.test_database.values",
        schema=InputSchema,
        autocommit_duration_ms=100,
    )

    # The sum is computed (this part is independent of the connectors).
    t = t.reduce(sum=pw.reducers.sum(t.value))

    # The MongoDB and regular filesystem connectors are used to send the
    # resulting output stream containing the sum to the database and to
    # the filesystem.
    pw.io.mongodb.write(
        t,
        connection_string="mongodb://mongodb:27017/",
        database="my_mongo_db",
        collection="sum_values",
    )
    pw.io.csv.write(t, "output_stream.csv")

    # The computation is launched.
    pw.run()

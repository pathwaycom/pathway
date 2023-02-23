import os

import pathway as pw

# import kafka clusters setting from upstash
rdkafka_settings = {
    "bootstrap.servers": "talented-cow-10356-eu1-kafka.upstash.io:9092",
    "security.protocol": "sasl_ssl",
    "sasl.mechanism": "SCRAM-SHA-256",
    "group.id": "$GROUP_NAME",
    "session.timeout.ms": "6000",
    "sasl.username": os.environ["UPSTASH_KAFKA_USER"],
    "sasl.password": os.environ["UPSTASH_KAFKA_PASS"],
}

# use kafka connector to read the kafka stream
t = pw.kafka.read(
    rdkafka_settings,
    topic_names=["linear-regression"],
    value_columns=["x", "y"],
    format="json",
    autocommit_duration_ms=1000,
    types={"x": pw.Type.INT, "y": pw.Type.FLOAT},
)

# write the input data to a CSV file for future reference
pw.csv.write(t, "regression_input.csv")

# expand your table to include x2 and x*y
t = t.select(
    *pw.this,
    x_square=t.x * t.x,
    x_y=t.x * t.y,
)

# produce table with sums and count of data points
statistics_table = t.reduce(
    count=pw.reducers.count(),
    sum_x=pw.reducers.sum(t.x),
    sum_y=pw.reducers.sum(t.y),
    sum_x_y=pw.reducers.sum(t.x_y),
    sum_x_square=pw.reducers.sum(t.x_square),
)

# compute estimation of a and b // perform linear regression
def compute_a(sum_x, sum_y, sum_x_square, sum_x_y, count):
    d = count * sum_x_square - sum_x * sum_x
    if d == 0:
        return 0
    else:
        return (sum_y * sum_x_square - sum_x * sum_x_y) / d


def compute_b(sum_x, sum_y, sum_x_square, sum_x_y, count):
    d = count * sum_x_square - sum_x * sum_x
    if d == 0:
        return 0
    else:
        return (count * sum_x_y - sum_x * sum_y) / d


# apply linear regression to input table
results_table = statistics_table.select(
    a=pw.apply(compute_a, **statistics_table),
    b=pw.apply(compute_b, **statistics_table),
)

# write results out to csv
pw.csv.write(results_table, "regression_output_stream.csv")

# run the pipeline
pw.run()

# Copyright © 2026 Pathway

import time

import pathway as pw

# To use advanced features with Pathway Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Community, comment out the line below.
pw.set_license_key("82687D-05BBFA-2CCBE1-F021E4-8FAAE4-V3")

MSSQL_CONNECTION = (
    "Server=tcp:localhost,1433;"
    "Database=sensordb;"
    "User Id=sa;"
    "Password=YourStrong!Passw0rd;"
    "TrustServerCertificate=true"
)


class SensorSchema(pw.Schema):
    sensor_name: str
    temperature: float
    humidity: float


print("Imports OK!")
time.sleep(10)
print("Starting Pathway:")

# Read sensor data from MSSQL
readings = pw.io.mssql.read(
    connection_string=MSSQL_CONNECTION,
    table_name="sensor_readings",
    schema=SensorSchema,
    autocommit_duration_ms=1000,
)

# Compute average temperature and humidity per sensor
stats = readings.groupby(readings.sensor_name).reduce(
    sensor_name=pw.this.sensor_name,
    avg_temp=pw.reducers.avg(pw.this.temperature),
    avg_humidity=pw.reducers.avg(pw.this.humidity),
    count=pw.reducers.count(),
)

# Write aggregated results back to MSSQL
pw.io.mssql.write(
    stats,
    connection_string=MSSQL_CONNECTION,
    table_name="sensor_stats",
    init_mode="create_if_not_exists",
    output_table_type="snapshot",
    primary_key=[stats.sensor_name],
)

# Also write a stream-of-changes log
pw.io.csv.write(stats, "output.csv")

# Launch the computation
pw.run()

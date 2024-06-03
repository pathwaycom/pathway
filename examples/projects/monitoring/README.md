# Pathway Monitoring using OpenTelemetry Collector and Grafana Cloud

This example contains the necessary configuration files to set up an OpenTelemetry Collector instance using Docker Compose. It also includes an Grafana dashboard JSON file for visualizing the collected monitoring data.

## Contents

- [config.yaml](./config.yaml): OpenTelemetry Collector configuration file.
- [docker-compose.yaml](./docker-compose.yaml): Docker Compose file to set up the OpenTelemetry Collector.
- [grafana-dashboard.json](./grafana-dashboard.json): Example Grafana dashboard JSON file to import.
- [monitoring_demo.py](./monitoring_demo.py): Example Pathway pipeline to test monitoring.

## Getting Started

### Prerequisites

Ensure you have Docker and Docker Compose installed on your machine. For installation instructions, visit the [Docker website](https://www.docker.com/get-started/).

To send data to Grafana Cloud, you will need a free Grafana account. You can create one by visiting [Grafana Cloud](https://grafana.com/).

Make sure Pathway is installed in version 0.11.2 or higher:

```python
pip install -U pathway
```

### Configuration

Before starting the OpenTelemetry Collector, fill in the Loki, Prometheus, and Tempo credentials in the `docker-compose.yaml` file.

Credentials can be obtained by logging into [Grafana](https://grafana.com/). You will be redirected to the **My Account** page, where you can manage your tokens for specific Grafana services.

### Running the OpenTelemetry Collector

To start the OpenTelemetry Collector instance, run the following command:

```sh
OTLP_GRPC_PORT=<OTLP_GRPC_PORT> docker-compose up
```

If you wish to use the default port (4317), you can omit the OTLP_GRPC_PORT environment variable.

### Running pathway

Once the OpenTelemetry Collector instance is running, you can start your Pathway pipeline.

```python
import pathway as pw

pw.set_license_key(key="YOUR-KEY")
pw.set_monitoring_config(server_endpoint="http://localhost:<OTLP_GRPC_PORT>")

# your pipeline here...

pw.run()
```

You can also adjust the license key and monitoring endpoint in the [example script](./monitoring_demo.py) and run it with:

```bash
python monitoring_demo.py
```


## Grafana Dashboard

To visualize the collected telemetry data, you can import the provided Grafana dashboard JSON file. Follow these steps:

1. Open Grafana Cloud in your web browser.
2. Navigate to the Dashboards section.
3. Click on Import.
4. Upload the grafana-dashboard.json file.
5. Click Import to load the example dashboard.

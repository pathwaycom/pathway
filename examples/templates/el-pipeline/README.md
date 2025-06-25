# Pathway EL Pipeline

Pathway's EL pipeline allows you to move data from different data sources (Extract) to the sinks of your choice (Load). 
By customizing a single YAML file, you can configure and run the pipeline without modifying any Python code.
You can learn more about the pipeline in our [article](https://pathway.com/developers/templates/etl/el-pipeline/).


## Project Structure
This projects contains two files:
- `app.py`, the application code using Pathway and written in Python;
- `app.yaml`, the file containing the configuration of the pipelines such as the data sources and data sinks.

## Prerequisites
- Python 3.8 or greater
- Git
- Docker (optional, for containerizer execution)

## Installation
Clone the Pathway GitHub repository:
```
git clone https://github.com/pathwaycom/pathway.git
```
Go to the EL pipeline repository:
```
cd examples/templates/el-pipeline/
```

## Configuration
Configure the pipeline by editing the `app.yaml` file.
This file uses a declarative YAML format to define data sources, sinks, and other settings.
You can learn more about YAML configuration in Pathway [here](https://pathway.com/developers/templates/configure-yaml).

### Declaring a Data Source
Define a data source using Pathway input connectors. For example, to set up a file system input connector:
```yaml
$source: !pw.io.fs.read
  path: ./input_data/
  format: binary
  name: input_connector
```

### Declaring a Data Sink
Similarly, define a data sink using Pathway output connectors. For example, to output data as a CSV file:
```yaml
output: !pw.io.csv.write
  table: $source
  filename: ./output.csv
  name: output_connector
```

### Pathway Connectors
Pathway supports a wide range of connectors for various data sources and sinks.
Refer to the [Pathway documentation](https://pathway.com/developers/user-guide/connect/pathway-connectors/) for a complete list of connectors.

### Schema
Define schemas to structure your data. Use `pw.schema_from_types` to specify column types:
```yaml
$schema: !pw.schema_from_types
  colA: str
  colB: int
  colC: float
```
Apply the schema to a connector:
```yaml
$source: !pw.io.csv.read
  path: ./input_data/
  schema: $schema
  name: input_connector
```

### Persistence
Configure persistence to preserve the state of computations and recover from failures:
```yaml
persistence_config: !pw.persistence.Config
  backend: !pw.persistence.Backend.filesystem
    path: ./persistence_storage/
```

## Example Configuration: From JSON to CSV

```yaml
# This YAML configuration file is used to set up and configure the EL pipeline.
# It defines the data sources, the data sinks, and the persistence configuration.

# Structure of the data sources using Pathway schema.
$schema: !pw.schema_from_types
  colA: str
  colB: int
  colC: float

# Define the data source using the schema.
$source: !pw.io.fs.read
  path: ./input_data/
  format: json
  schema: $schema
  name: input_connector

# Output the data in the data sink of your choice.
output: !pw.io.csv.write
  table: $source
  filename: ./output.csv
  name: output_connector

# Uncomment to use persistence on the file system.
# persistence_config: !pw.persistence.Config
#   backend: !pw.persistence.Backend.filesystem
#     path: ./persistence_storage/
```

## Default Configuration: Kafka to PostgreSQL Example
The project comes with a default configuration set up to move data from Kafka to PostgreSQL.

### Configuration Details
The default configuration is defined in the `app.yaml` file and includes the following components:
#### Kafka Configuration
Sets up a Kafka input connector to read messages from a specified Kafka topic.

```yaml
$InputStreamSchema: !pw.schema_from_types
  date: str
  message: str

$rdkafka_settings:
  "bootstrap.servers": $KAFKA_HOSTNAME
  "security.protocol": "plaintext"
  "group.id": $KAFKA_GROUP_ID
  "session.timeout.ms": "6000"
  "auto.offset.reset": "earliest"
  
$kafka_source: !pw.io.kafka.read
  rdkafka_settings: $rdkafka_settings
  topic: $KAFKA_TOPIC
  format: "json"
  schema: $InputStreamSchema
  autocommit_duration_ms: 100
  name: input_kafka_connector
```

#### PostgreSQL Sink Configuration
Configures a PostgreSQL output connector to write the data from Kafka into a PostgreSQL table.
```yaml

$postgres_settings:
    "host": $DB_HOSTNAME
    "port": $DB_PORT
    "dbname": $DB_NAME
    "user": $DB_USER
    "password": $DB_PASSWORD

$table_name: "messages_table"

output: !pw.io.postgres.write
  table: $kafka_source
  postgres_settings: $postgres_settings
  table_name: $table_name
  name: output_postgres_connector
```

#### Persistence Configuration
Ensures the state of the computation is preserved and can be recovered in case of failures.
```yaml
persistence_config: !pw.persistence.Config
  backend: !pw.persistence.Backend.filesystem
    path: ./persistence_storage/
```

#### Environment Variables
To run this example, you need to set the following environment variables:
- `KAFKA_HOSTNAME`: The hostname of your Kafka server.
- `KAFKA_GROUP_ID`: The group ID for the Kafka consumer.
- `KAFKA_TOPIC`: The Kafka topic to read messages from.
- `DB_HOSTNAME`: The hostname of your PostgreSQL database.
- `DB_PORT`: The port of your PostgreSQL database.
- `DB_NAME`: The name of the PostgreSQL database.
- `DB_USER`: The username for the PostgreSQL database.
- `DB_PASSWORD`: The password for the PostgreSQL database.


## Running the Project
Run the pipeline directly using Python:
```
python app.py
```
Or use Docker for containerized execution.

## Monitoring and Logging
- Accessing Logs
- Monitoring Pipeline Performance

## Troubleshooting
- Ensure all environment variables are correctly set.
- Verify that all paths and credentials in the YAML configuration are accurate.
- Check logs for detailed error messages and debugging information.


## Documentation and Support<a id="resources"></a>
The entire documentation of Pathway is available at [pathway.com/developers/](https://pathway.com/developers/user-guide/introduction/welcome), including the [API Docs](https://pathway.com/developers/api-docs/pathway).

If you have any question, don't hesitate to [open an issue on GitHub](https://github.com/pathwaycom/pathway/issues), join us on [Discord](https://discord.com/invite/pathway), or send us an email at [contact@pathway.com](mailto:contact@pathway.com).

## Contribution Guidelines
If you develop a library or connector which you would like to integrate with this repo, we suggest releasing it first as a separate repo on a MIT/Apache 2.0 license. 

For all concerns regarding core Pathway functionalities, Issues are encouraged. For further information, don't hesitate to engage with Pathway's [Discord community](https://discord.gg/pathway).

## License
Pathway is distributed on a [BSL 1.1 License](https://github.com/pathwaycom/pathway/blob/main/LICENSE.txt) which allows for unlimited non-commercial use, as well as use of the Pathway package [for most commercial purposes](https://pathway.com/license/), free of charge. Code in this repository automatically converts to Open Source (Apache 2.0 License) after 4 years. Some [public repos](https://github.com/pathwaycom) which are complementary to this one (examples, libraries, connectors, etc.) are licensed as Open Source, under the MIT license.

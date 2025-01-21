import time
import uuid

import psycopg2
import requests
from pymongo import MongoClient

import pathway as pw
from pathway.internals import dtype

POSTGRES_DB_HOST = "postgres"
POSTGRES_DB_PORT = 5432
POSTGRES_DB_USER = "user"
POSTGRES_DB_PASSWORD = "password"
POSTGRES_DB_NAME = "tests"
POSTGRES_SETTINGS = {
    "host": POSTGRES_DB_HOST,
    "port": str(POSTGRES_DB_PORT),
    "dbname": POSTGRES_DB_NAME,
    "user": POSTGRES_DB_USER,
    "password": POSTGRES_DB_PASSWORD,
}

MONGODB_HOST_WITH_PORT = "mongodb:27017"
MONGODB_CONNECTION_STRING = f"mongodb://{MONGODB_HOST_WITH_PORT}/?replicaSet=rs0"
MONGODB_BASE_NAME = "tests"

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_SETTINGS = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}

DEBEZIUM_CONNECTOR_URL = "http://debezium:8083/connectors"


class PostgresContext:

    def __init__(self):
        self.connection = psycopg2.connect(
            host=POSTGRES_DB_HOST,
            port=POSTGRES_DB_PORT,
            database=POSTGRES_DB_NAME,
            user=POSTGRES_DB_USER,
            password=POSTGRES_DB_PASSWORD,
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def insert_row(
        self, table_name: str, values: dict[str, int | bool | str | float]
    ) -> None:
        field_names = []
        field_values = []
        for key, value in values.items():
            field_names.append(key)
            if isinstance(value, str):
                field_values.append(f"'{value}'")
            elif value is True:
                field_values.append("'t'")
            elif value is False:
                field_values.append("'f'")
            else:
                field_values.append(str(value))
        condition = f'INSERT INTO {table_name} ({",".join(field_names)}) VALUES ({",".join(field_values)})'
        print(f"Inserting a row: {condition}")
        self.cursor.execute(condition)

    def create_table(self, schema: type[pw.Schema], *, used_for_output: bool) -> str:
        table_name = self.random_table_name()

        primary_key_found = False
        fields = []
        for field_name, field_schema in schema.columns().items():
            parts = [field_name]
            field_type = field_schema.dtype
            if field_type == dtype.STR:
                parts.append("TEXT")
            elif field_type == dtype.INT:
                parts.append("BIGINT")
            elif field_type == dtype.FLOAT:
                parts.append("DOUBLE PRECISION")
            elif field_type == dtype.BOOL:
                parts.append("BOOLEAN")
            else:
                raise RuntimeError(f"This test doesn't support field type {field_type}")
            if field_schema.primary_key:
                assert (
                    not primary_key_found
                ), "This test only supports simple primary keys"
                primary_key_found = True
                parts.append("PRIMARY KEY NOT NULL")
            fields.append(" ".join(parts))

        if used_for_output:
            fields.append("time BIGINT NOT NULL")
            fields.append("diff BIGINT NOT NULL")

        self.cursor.execute(
            f'CREATE TABLE IF NOT EXISTS {table_name} ({",".join(fields)})'
        )

        return table_name

    def get_table_contents(
        self,
        table_name: str,
        column_names: list[str],
        sort_by: str | tuple | None = None,
    ) -> list[dict[str, str | int | bool | float]]:
        select_query = f'SELECT {",".join(column_names)} FROM {table_name};'
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        result = []
        for row in rows:
            row_map = {}
            for name, value in zip(column_names, row):
                row_map[name] = value
            result.append(row_map)
        if sort_by is not None:
            if isinstance(sort_by, tuple):
                result.sort(key=lambda item: tuple(item[key] for key in sort_by))
            else:
                result.sort(key=lambda item: item[sort_by])
        return result

    def random_table_name(self) -> str:
        return f'postgres_{str(uuid.uuid4()).replace("-", "")}'


class MongoDBContext:
    client: MongoClient

    def __init__(self):
        self.client = MongoClient(MONGODB_CONNECTION_STRING)

    def generate_collection_name(self) -> str:
        table_name = f'mongodb_{str(uuid.uuid4()).replace("-", "")}'
        return table_name

    def get_collection(
        self, collection_name: str, field_names: list[str]
    ) -> list[dict[str, str | int | bool | float]]:
        db = self.client[MONGODB_BASE_NAME]
        collection = db[collection_name]
        data = collection.find()
        result = []
        for document in data:
            entry = {}
            for field_name in field_names:
                entry[field_name] = document[field_name]
            result.append(entry)
        return result

    def insert_document(
        self, collection_name: str, document: dict[str, int | bool | str | float]
    ) -> None:
        db = self.client[MONGODB_BASE_NAME]
        collection = db[collection_name]
        collection.insert_one(document)


class DebeziumContext:

    def _register_connector(self, payload: dict, result_on_ok: str) -> str:
        for _ in range(300):
            try:
                r = requests.post(DEBEZIUM_CONNECTOR_URL, timeout=60, json=payload)
                is_ok = r.status_code // 100 == 2
            except Exception as e:
                print(f"Debezium is not ready to register connector yet: {e}")
                time.sleep(1.0)
                continue
            if is_ok:
                return result_on_ok
            else:
                print(
                    f"Debezium is not ready to register connector yet. Code: {r.status_code}. Text: {r.text}"
                )
                time.sleep(1.0)
        raise RuntimeError("Failed to register Debezium connector")

    def register_mongodb(self) -> str:
        connector_id = str(uuid.uuid4()).replace("-", "")
        payload = {
            "name": f"values-connector-{connector_id}",
            "config": {
                "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
                "mongodb.hosts": f"rs0/{MONGODB_HOST_WITH_PORT}",
                "mongodb.name": f"{connector_id}",
                "database.include.list": MONGODB_BASE_NAME,
                "database.history.kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "database.history.kafka.topic": "dbhistory.mongo",
            },
        }
        return self._register_connector(payload, f"{connector_id}.{MONGODB_BASE_NAME}.")

    def register_postgres(self, table_name: str) -> str:
        connector_id = str(uuid.uuid4()).replace("-", "")
        payload = {
            "name": f"values-connector-{connector_id}",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "plugin.name": "pgoutput",
                "database.hostname": POSTGRES_DB_HOST,
                "database.port": str(POSTGRES_DB_PORT),
                "database.user": str(POSTGRES_DB_USER),
                "database.password": str(POSTGRES_DB_PASSWORD),
                "database.dbname": str(POSTGRES_DB_NAME),
                "database.server.name": connector_id,
                "table.include.list": f"public.{table_name}",
                "database.history.kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            },
        }
        return self._register_connector(payload, f"{connector_id}.public.{table_name}")

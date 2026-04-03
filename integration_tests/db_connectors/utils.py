import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Union, get_args, get_origin

import boto3
import mysql.connector
import numpy as np
import pandas as pd
import psycopg2
import requests
from pymongo import MongoClient

import pathway as pw
from pathway.internals import api, dtype

# from pgvector.psycopg2 import register_vector # FIXME enable once pgvector can be added to env


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

POSTGRES_WITH_TLS_DB_HOST = "postgres-tls"
POSTGRES_WITH_TLS_SETTINGS = {
    "host": POSTGRES_WITH_TLS_DB_HOST,
    "port": str(POSTGRES_DB_PORT),
    "dbname": POSTGRES_DB_NAME,
    "user": POSTGRES_DB_USER,
    "password": POSTGRES_DB_PASSWORD,
}

PGVECTOR_DB_HOST = "pgvector"
PGVECTOR_SETTINGS = {
    "host": PGVECTOR_DB_HOST,
    "port": str(POSTGRES_DB_PORT),
    "dbname": POSTGRES_DB_NAME,
    "user": POSTGRES_DB_USER,
    "password": POSTGRES_DB_PASSWORD,
}

QUEST_DB_HOST = "questdb"
QUEST_DB_WIRE_PORT = 8812
QUEST_DB_LINE_PORT = 9000
QUEST_DB_NAME = "qdb"
QUEST_DB_USER = "admin"
QUEST_DB_PASSWORD = "quest"

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

MYSQL_DB_HOST = "mysql"
MYSQL_DB_PORT = 3306
MYSQL_DB_NAME = "testdb"
MYSQL_DB_USER = "testuser"
MYSQL_DB_PASSWORD = "testpass"
MYSQL_CONNECTION_STRING = (
    f"mysql://{MYSQL_DB_USER}:{MYSQL_DB_PASSWORD}"
    + f"@{MYSQL_DB_HOST}:{MYSQL_DB_PORT}/{MYSQL_DB_NAME}"
)

MSSQL_DB_HOST = "mssql"
MSSQL_DB_PORT = 1433
MSSQL_DB_NAME = "testdb"
MSSQL_DB_USER = "sa"
MSSQL_DB_PASSWORD = "YourStrong!Passw0rd"
MSSQL_CONNECTION_STRING = (
    f"Server=tcp:{MSSQL_DB_HOST},{MSSQL_DB_PORT};"
    f"Database={MSSQL_DB_NAME};"
    f"User Id={MSSQL_DB_USER};"
    f"Password={MSSQL_DB_PASSWORD};"
    "TrustServerCertificate=true"
)


def is_mysql_reachable():
    try:
        mysql.connector.connect(
            host=MYSQL_DB_HOST,
            port=MYSQL_DB_PORT,
            database=MYSQL_DB_NAME,
            user=MYSQL_DB_USER,
            password=MYSQL_DB_PASSWORD,
            autocommit=True,
        )
    except mysql.connector.errors.InterfaceError:
        return False

    return True


@dataclass(frozen=True)
class ColumnProperties:
    type_name: str
    is_nullable: bool


class SimpleObject:
    def __init__(self, a):
        self.a = a

    def __eq__(self, other):
        return self.a == other.a


class WireProtocolSupporterContext:

    def __init__(
        self, *, host: str, port: int, database: str, user: str, password: str
    ):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def get_table_schema(
        self, table_name: str, schema: str = "public"
    ) -> dict[str, ColumnProperties]:
        query = """
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position;
        """
        self.cursor.execute(query, (table_name, schema))
        rows = self.cursor.fetchall()

        schema_props = {}
        for column_name, type_name, is_nullable in rows:
            assert is_nullable in ("YES", "NO")
            schema_props[column_name] = ColumnProperties(
                type_name.lower(), is_nullable == "YES"
            )
        return schema_props

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

    def create_table(self, schema: type[pw.Schema], *, add_special_fields: bool) -> str:
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
            elif isinstance(field_type, dtype.Array) and "_vector" in field_name:
                # hack to create an array with a specific type
                parts.append("VECTOR")
            elif isinstance(field_type, dtype.Array) and "_halfvec" in field_name:
                # hack to create an array with a specific type
                parts.append("HALFVEC")
            else:
                raise RuntimeError(f"This test doesn't support field type {field_type}")
            if field_schema.primary_key:
                assert (
                    not primary_key_found
                ), "This test only supports simple primary keys"
                primary_key_found = True
                parts.append("PRIMARY KEY NOT NULL")
            fields.append(" ".join(parts))

        if add_special_fields:
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
        def convert_value(value):
            if isinstance(value, memoryview):
                return bytes(value)
            if isinstance(value, list):
                return [convert_value(v) for v in value]
            return value

        select_query = f'SELECT {",".join(column_names)} FROM {table_name};'
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        result = []
        for row in rows:
            row_map = {}
            for name, value in zip(column_names, row):
                row_map[name] = convert_value(value)
            result.append(row_map)
        if sort_by is not None:
            if isinstance(sort_by, tuple):
                result.sort(key=lambda item: tuple(item[key] for key in sort_by))
            else:
                result.sort(key=lambda item: item[sort_by])
        return result

    def execute_sql(self, query: str):
        self.cursor.execute(query)

    def random_table_name(self) -> str:
        return f'wire_{str(uuid.uuid4()).replace("-", "")}'

    @contextmanager
    def publication(self, table_name: str):
        pub_name = f"{table_name}_pub"
        create_sql = f"CREATE PUBLICATION {pub_name} FOR TABLE {table_name};"
        drop_sql = f"DROP PUBLICATION IF EXISTS {pub_name};"

        try:
            self.execute_sql(create_sql)
            yield pub_name
        finally:
            try:
                self.execute_sql(drop_sql)
            except Exception as e:
                logging.warning(f"Warning: Failed to drop publication {pub_name}: {e}")


class PostgresContext(WireProtocolSupporterContext):

    def __init__(self):
        super().__init__(
            host=POSTGRES_DB_HOST,
            port=POSTGRES_DB_PORT,
            database=POSTGRES_DB_NAME,
            user=POSTGRES_DB_USER,
            password=POSTGRES_DB_PASSWORD,
        )


class PostgresWithTlsContext(WireProtocolSupporterContext):

    def __init__(self):
        super().__init__(
            host=POSTGRES_WITH_TLS_DB_HOST,
            port=POSTGRES_DB_PORT,
            database=POSTGRES_DB_NAME,
            user=POSTGRES_DB_USER,
            password=POSTGRES_DB_PASSWORD,
        )


class PgvectorContext(WireProtocolSupporterContext):

    def __init__(self):
        super().__init__(
            host=PGVECTOR_DB_HOST,
            port=POSTGRES_DB_PORT,
            database=POSTGRES_DB_NAME,
            user=POSTGRES_DB_USER,
            password=POSTGRES_DB_PASSWORD,
        )
        self.cursor.execute("CREATE EXTENSION vector")
        # register_vector(self.connection) # FIXME


class QuestDBContext(WireProtocolSupporterContext):

    def __init__(self):
        super().__init__(
            host=QUEST_DB_HOST,
            port=QUEST_DB_WIRE_PORT,
            database=QUEST_DB_NAME,
            user=QUEST_DB_USER,
            password=QUEST_DB_PASSWORD,
        )


MILVUS_VECTOR_DIM = 3


def _make_milvus_client(uri: str):
    """Create a MilvusClient for the given URI.

    For local ``.db`` files, works around a pymilvus 2.6.x bug where the
    Unix-domain-socket address is not forwarded to the gRPC handler.  See
    ``pathway.io.milvus._make_client`` for the full explanation.
    """
    from pymilvus import MilvusClient

    if uri.endswith(".db"):
        try:
            from milvus_lite.server_manager import server_manager_instance

            uds_uri = server_manager_instance.start_and_get_uri(uri)
            if uds_uri is None:
                raise RuntimeError(
                    f"milvus-lite failed to start a local server for: {uri}"
                )
            return MilvusClient(uds_uri)
        except ImportError:
            pass
    return MilvusClient(uri)


class MilvusContext:
    def __init__(self, uri: str) -> None:
        from pymilvus import DataType

        self.uri = uri
        self._DataType = DataType
        self.client = _make_milvus_client(uri)

    def create_collection(
        self, collection_name: str, *, dimension: int = MILVUS_VECTOR_DIM
    ) -> None:
        schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", self._DataType.INT64, is_primary=True)
        schema.add_field("vector", self._DataType.FLOAT_VECTOR, dim=dimension)
        index_params = self.client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE", index_type="FLAT")
        self.client.create_collection(
            collection_name, schema=schema, index_params=index_params
        )

    def create_scalar_collection(
        self, collection_name: str, value_type, **value_kwargs
    ) -> None:
        """Create a collection with id (INT64 PK), value (<value_type>), vec (FLOAT_VECTOR).

        ``value_type`` is a Milvus ``DataType``. Extra keyword arguments are
        forwarded to ``add_field`` for the value field (e.g. ``max_length`` for
        ``VARCHAR``).  The ``vec`` field is always added so that the collection
        can be indexed.
        """
        schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", self._DataType.INT64, is_primary=True)
        schema.add_field("value", value_type, **value_kwargs)
        schema.add_field("vec", self._DataType.FLOAT_VECTOR, dim=MILVUS_VECTOR_DIM)
        index_params = self.client.prepare_index_params()
        index_params.add_index("vec", metric_type="COSINE", index_type="FLAT")
        self.client.create_collection(
            collection_name, schema=schema, index_params=index_params
        )

    def query_all(self, collection_name: str, output_fields: list[str]) -> list[dict]:
        # Empty filter requires a limit in Milvus 2.6+; use id >= 0 to fetch
        # all rows without a limit cap (test IDs are always positive integers).
        # query() returns HybridExtraList in pymilvus 2.6.x; list() unwraps it.
        return list(
            self.client.query(
                collection_name, filter="id >= 0", output_fields=output_fields
            )
        )

    def generate_collection_name(self) -> str:
        return f"milvus_{uuid.uuid4().hex[:12]}"

    def close(self) -> None:
        self.client.close()


class MongoDBContext:
    client: MongoClient

    def __init__(self):
        self.client = MongoClient(MONGODB_CONNECTION_STRING)

    def generate_collection_name(self) -> str:
        table_name = f'mongodb_{str(uuid.uuid4()).replace("-", "")}'
        return table_name

    def collection_exists(self, collection_name: str) -> bool:
        db = self.client[MONGODB_BASE_NAME]
        return collection_name in db.list_collection_names()

    def get_full_collection(self, collection_name):
        if not self.collection_exists(collection_name):
            return []
        db = self.client[MONGODB_BASE_NAME]
        collection = db[collection_name]
        return [i for i in collection.find({}, {"_id": 0})]  # cast to list

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

    def replace_document(
        self,
        collection_name: str,
        filter: dict,
        replacement: dict[str, int | bool | str | float],
    ) -> None:
        db = self.client[MONGODB_BASE_NAME]
        collection = db[collection_name]
        collection.replace_one(filter, replacement)

    def delete_document(self, collection_name: str, filter: dict) -> None:
        db = self.client[MONGODB_BASE_NAME]
        collection = db[collection_name]
        collection.delete_one(filter)


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


class DynamoDBContext:

    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb", region_name="us-west-2")

    def get_table_contents(self, table_name: str) -> list[dict]:
        table = self.dynamodb.Table(table_name)
        response = table.scan()
        data = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            data.extend(response["Items"])

        return data

    def generate_table_name(self) -> str:
        return "table" + str(uuid.uuid4())


class MySQLContext:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=MYSQL_DB_HOST,
            port=MYSQL_DB_PORT,
            database=MYSQL_DB_NAME,
            user=MYSQL_DB_USER,
            password=MYSQL_DB_PASSWORD,
            autocommit=True,
        )
        self.cursor = self.connection.cursor()

    def get_table_schema(self, table_name: str) -> dict[str, ColumnProperties]:
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position;
        """
        self.cursor.execute(query, (table_name, self.connection.database))
        rows = self.cursor.fetchall()

        schema_props = {}
        for column_name, type_name, is_nullable in rows:
            schema_props[column_name] = ColumnProperties(
                type_name.lower(), is_nullable.upper() == "YES"
            )
        return schema_props

    def insert_row(
        self, table_name: str, values: dict[str, Union[int, bool, str, float]]
    ) -> None:
        field_names = list(values.keys())
        placeholders = ", ".join(["%s"] * len(values))
        query = f"INSERT INTO {table_name} ({','.join(field_names)}) VALUES ({placeholders})"
        print(f"Inserting a row: {query}")
        self.cursor.execute(query, tuple(values.values()))

    def create_table(self, schema: type[pw.Schema], *, add_special_fields: bool) -> str:
        table_name = self.random_table_name()

        primary_key_found = False
        fields = []
        for field_name, field_schema in schema.columns().items():
            parts = [f"`{field_name}`"]
            field_type = field_schema.dtype
            if field_type == dtype.STR:
                parts.append("VARCHAR(255)")
            elif field_type == dtype.INT:
                parts.append("BIGINT")
            elif field_type == dtype.FLOAT:
                parts.append("DOUBLE")
            elif field_type == dtype.BOOL:
                parts.append("BOOLEAN")
            else:
                raise RuntimeError(f"Unsupported field type {field_type}")
            if field_schema.primary_key:
                if primary_key_found:
                    raise AssertionError("Only single primary key supported")
                primary_key_found = True
                parts.append("PRIMARY KEY NOT NULL")
            fields.append(" ".join(parts))

        if add_special_fields:
            fields.append("`time` BIGINT NOT NULL")
            fields.append("`diff` BIGINT NOT NULL")

        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(fields)})"
        self.cursor.execute(create_sql)
        return table_name

    def get_table_contents(
        self,
        table_name: str,
        column_names: list[str],
        sort_by: Union[str, tuple, None] = None,
    ) -> list[dict[str, Union[str, int, bool, float]]]:
        select_query = f"SELECT {','.join(column_names)} FROM {table_name};"
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        result = []
        for row in rows:
            row_map = dict(zip(column_names, row))
            result.append(row_map)
        if sort_by is not None:
            if isinstance(sort_by, tuple):
                result.sort(key=lambda item: tuple(item[key] for key in sort_by))
            else:
                result.sort(key=lambda item: item[sort_by])
        return result

    def random_table_name(self) -> str:
        return f"mysql_{uuid.uuid4().hex}"


class MssqlContext:
    def __init__(self):
        import pymssql

        self.connection = pymssql.connect(
            server=MSSQL_DB_HOST,
            port=MSSQL_DB_PORT,
            user=MSSQL_DB_USER,
            password=MSSQL_DB_PASSWORD,
            database=MSSQL_DB_NAME,
            autocommit=True,
            tds_version="7.3",
        )
        self.cursor = self.connection.cursor()

    def random_table_name(self) -> str:
        return f"mssql_{uuid.uuid4().hex}"

    def execute_sql(self, query: str):
        self.cursor.execute(query)

    def enable_cdc(
        self, table_name: str, max_retries: int = 5, retry_delay: float = 1.0
    ) -> None:
        """Enable CDC on a table, retrying on deadlock (SQL Server error 1205)."""
        import pymssql

        for attempt in range(max_retries):
            try:
                self.cursor.execute(
                    f"EXEC sys.sp_cdc_enable_table "
                    f"@source_schema=N'dbo', "
                    f"@source_name=N'{table_name}', "
                    f"@role_name=NULL"
                )
                return
            except pymssql.exceptions.OperationalError as e:
                if attempt < max_retries - 1 and "1205" in str(e):
                    time.sleep(retry_delay)
                else:
                    raise

    def insert_row(
        self, table_name: str, values: dict[str, Union[int, bool, str, float, bytes]]
    ) -> None:
        field_names = []
        value_exprs = []
        params = []
        for k, v in values.items():
            field_names.append(k)
            if isinstance(v, (bytes, bytearray)):
                # pymssql encodes empty bytes b"" as '' (varchar) instead of
                # 0x (binary), causing SQL Server error 257.  Embed all bytes
                # values as SQL binary literals to avoid the issue entirely.
                value_exprs.append(f"0x{v.hex()}")
            else:
                value_exprs.append("%s")
                params.append(v)
        query = f"INSERT INTO {table_name} ({','.join(field_names)}) VALUES ({','.join(value_exprs)})"
        self.cursor.execute(query, tuple(params))

    def create_table(self, schema: type[pw.Schema], *, add_special_fields: bool) -> str:
        table_name = self.random_table_name()

        primary_key_found = False
        fields = []
        for field_name, field_schema in schema.columns().items():
            parts = [f"[{field_name}]"]
            field_type = field_schema.dtype
            if field_type == dtype.STR:
                parts.append("NVARCHAR(MAX)")
            elif field_type == dtype.INT:
                parts.append("BIGINT")
            elif field_type == dtype.FLOAT:
                parts.append("FLOAT")
            elif field_type == dtype.BOOL:
                parts.append("BIT")
            else:
                raise RuntimeError(f"Unsupported field type {field_type}")
            if field_schema.primary_key:
                if primary_key_found:
                    raise AssertionError("Only single primary key supported")
                primary_key_found = True
                parts.append("PRIMARY KEY NOT NULL")
            fields.append(" ".join(parts))

        if add_special_fields:
            fields.append("[time] BIGINT NOT NULL")
            fields.append("[diff] BIGINT NOT NULL")

        create_sql = (
            f"IF OBJECT_ID(N'{table_name}', N'U') IS NULL "
            f"CREATE TABLE {table_name} ({','.join(fields)})"
        )
        self.cursor.execute(create_sql)
        return table_name

    def get_table_contents(
        self,
        table_name: str,
        column_names: list[str],
        sort_by: Union[str, tuple, None] = None,
    ) -> list[dict[str, Union[str, int, bool, float]]]:
        select_query = f"SELECT {','.join(column_names)} FROM {table_name};"
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        result = []
        for row in rows:
            row_map = dict(zip(column_names, row))
            result.append(row_map)
        if sort_by is not None:
            if isinstance(sort_by, tuple):
                result.sort(key=lambda item: tuple(item[key] for key in sort_by))
            else:
                result.sort(key=lambda item: item[sort_by])
        return result

    def get_table_schema(self, table_name: str) -> dict[str, ColumnProperties]:
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'dbo'
            ORDER BY ordinal_position;
        """
        self.cursor.execute(query, (table_name,))
        rows = self.cursor.fetchall()

        schema_props = {}
        for column_name, type_name, is_nullable in rows:
            schema_props[column_name] = ColumnProperties(
                type_name.lower(), is_nullable.upper() == "YES"
            )
        return schema_props


class EntryCountChecker:

    def __init__(
        self,
        n_expected_entries: int,
        db_context: DynamoDBContext | WireProtocolSupporterContext,
        **get_table_contents_kwargs,
    ):
        self.n_expected_entries = n_expected_entries
        self.db_context = db_context
        self.get_table_contents_kwargs = get_table_contents_kwargs

    def __call__(self) -> bool:
        try:
            table_contents = self.db_context.get_table_contents(
                **self.get_table_contents_kwargs
            )
        except Exception:
            return False
        return len(table_contents) == self.n_expected_entries


def _compare_input_and_output(
    ItemType: type,
    input_rows: list[dict],
    output_rows: list[dict],
    timestamp_precision: int = 1000,
    timezone_supported: bool = True,
):
    def normalize_input(value):
        if isinstance(value, pw.Pointer):
            return str(value)
        if isinstance(value, pw.Json):
            return value.value
        if isinstance(value, pd.Timestamp):
            result = value.to_pydatetime()
            if not timezone_supported:
                result = result.replace(tzinfo=None)
            return result
        if isinstance(value, pd.Timedelta):
            return value.value // timestamp_precision
        if isinstance(value, np.ndarray):
            return normalize_input(value.tolist())
        if hasattr(value, "_create_with_serializer"):
            return value.value
        if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict)):
            return [normalize_input(v) for v in value]
        return value

    def normalize_output(value, ItemType: type):
        if hasattr(ItemType, "_create_with_serializer"):
            value = api.deserialize(value)
            assert isinstance(
                value, pw.PyObjectWrapper
            ), f"expecting PyObjectWrapper, got {type(value)}"
            return value.value

        actual_type = get_args(ItemType) or (ItemType,)
        if pw.Json in actual_type and isinstance(value, str):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass  # plain string is a valid JSON string value
        # JSON-parse string values for list/tuple types (e.g. MSSQL stores them as JSON strings)
        if isinstance(value, str) and get_origin(ItemType) in (list, tuple):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass
        # Parse ndarray JSON representation {"shape":[...],"elements":[...]} back to nested lists
        if isinstance(value, str) and get_origin(ItemType) is np.ndarray:
            try:
                parsed = json.loads(value)
                if (
                    isinstance(parsed, dict)
                    and "shape" in parsed
                    and "elements" in parsed
                ):
                    arr = np.array(parsed["elements"]).reshape(parsed["shape"])
                    return arr.tolist()
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict)):
            args = get_args(ItemType)
            nested_arg = None
            for arg in args:
                if arg is not None:
                    nested_arg = arg
                    break
            return [normalize_output(v, nested_arg) for v in value]  # type: ignore
        return value

    output_rows.sort(key=lambda item: item["pkey"])

    for input_row in input_rows:
        input_row["item"] = normalize_input(input_row["item"])

    for output_row in output_rows:
        output_row["item"] = normalize_output(output_row["item"], ItemType)
        output_row.pop("time", None)
        output_row.pop("diff", None)

    print("input rows", input_rows)
    print("output rows", output_rows)
    assert output_rows == input_rows


def _get_expected_python_type(ItemType: type) -> type | tuple:
    import types as builtin_types

    origin = get_origin(ItemType)

    if origin is Union or (
        hasattr(builtin_types, "UnionType") and origin is builtin_types.UnionType
    ):
        args = get_args(ItemType)
        non_none_args = [a for a in args if a is not type(None)]
        inner = _get_expected_python_type(non_none_args[0])
        if isinstance(inner, tuple):
            return inner + (type(None),)
        return (inner, type(None))

    if origin is not None:
        return origin

    if ItemType is pw.Duration:
        return pd.Timedelta
    if ItemType in (pw.DateTimeNaive, pw.DateTimeUtc):
        return pd.Timestamp

    return ItemType


def _make_type_check_observer(
    ItemType: type,
) -> tuple[pw.io.python.ConnectorObserver, list[str]]:
    type_errors: list[str] = []
    expected_type = _get_expected_python_type(ItemType)

    class TypeCheckObserver(pw.io.python.ConnectorObserver):
        def on_change(self, key, row, time, is_addition):
            if is_addition:
                value = row["item"]
                if not isinstance(value, expected_type):
                    # tuple is acceptable when the schema type is list
                    if isinstance(value, tuple) and (
                        expected_type is list
                        or (isinstance(expected_type, tuple) and list in expected_type)
                    ):
                        return
                    type_errors.append(
                        f"item value {value!r} has type {type(value)}, "
                        f"expected {expected_type}"
                    )

        def on_end(self):
            pass

    return TypeCheckObserver(), type_errors


def _create_ndarray_table(ItemType: type, input_rows: list[dict]) -> pw.Table:
    class InputSchemaWithPkey(pw.Schema):
        pkey: int
        item: Any

    return pw.debug.table_from_rows(
        InputSchemaWithPkey,
        [tuple(row.values()) for row in input_rows],
    ).update_types(item=ItemType)

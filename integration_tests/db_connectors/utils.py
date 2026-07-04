import fcntl
import json
import logging
import os
import random
import tempfile
import time
import types as builtin_types
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Union, cast, get_args, get_origin

import boto3
import mysql.connector
import numpy as np
import pandas as pd
import psycopg2
import pymssql
import requests
from pymongo import MongoClient
from pymongo.operations import SearchIndexModel

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

# NeonDB is exposed through the official ``neondatabase/neon_local`` proxy
# container (service name ``neon`` in docker-compose). The proxy speaks the
# plain PostgreSQL wire protocol on 5432 and forwards to an ephemeral branch of
# a real Neon cloud project. Every connection parameter is read from the
# environment so nothing is hardcoded; the defaults are the neon_local proxy's
# well-known values, so the suite runs against the docker-compose ``neon``
# service with no configuration:
#   NEON_HOST     - proxy host                       (default: ``neon``)
#   NEON_PORT     - proxy port                        (default: ``5432``)
#   NEON_USER     - proxy database user               (default: ``neon``)
#   NEON_PASSWORD - proxy database password           (default: ``npg``)
#   NEON_DATABASE - database name                     (default: ``neondb``)
# Overriding them lets the suite point at a neon_local container published on
# localhost, or any other reachable proxy, outside CI.
NEON_DB_HOST = os.environ.get("NEON_HOST", "neon")
NEON_DB_PORT = int(os.environ.get("NEON_PORT", "5432"))
NEON_DB_USER = os.environ.get("NEON_USER", "neon")
NEON_DB_PASSWORD = os.environ.get("NEON_PASSWORD", "npg")
NEON_DB_NAME = os.environ.get("NEON_DATABASE", "neondb")
NEON_SETTINGS = {
    "host": NEON_DB_HOST,
    "port": str(NEON_DB_PORT),
    "dbname": NEON_DB_NAME,
    "user": NEON_DB_USER,
    "password": NEON_DB_PASSWORD,
}

QUEST_DB_HOST = "questdb"
QUEST_DB_WIRE_PORT = 8812
QUEST_DB_LINE_PORT = 9000
QUEST_DB_NAME = "qdb"
QUEST_DB_USER = "admin"
QUEST_DB_PASSWORD = "quest"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_NATIVE_PORT = 9000
CLICKHOUSE_HTTP_PORT = 8123
CLICKHOUSE_DB_NAME = "default"
# Recent clickhouse-server images restrict the built-in ``default`` user to
# localhost, so the compose service provisions this network-accessible user via
# the CLICKHOUSE_USER / CLICKHOUSE_PASSWORD environment variables.
CLICKHOUSE_USER = "pathway"
CLICKHOUSE_PASSWORD = "pathway"
CLICKHOUSE_CONNECTION_STRING = (
    f"tcp://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:"
    f"{CLICKHOUSE_NATIVE_PORT}/{CLICKHOUSE_DB_NAME}"
)

MONGODB_HOST_WITH_PORT = "mongodb:27017"
MONGODB_CONNECTION_STRING = f"mongodb://{MONGODB_HOST_WITH_PORT}/?replicaSet=rs0"
MONGODB_BASE_NAME = "tests"

# Atlas Local runs as its own compose service (the ``mongodb/mongodb-atlas-local``
# image), separate from the plain ``mongo`` service above: it is the only MongoDB
# image that ships the ``mongot`` search process, so ``$vectorSearch`` works in CI
# without a cloud Atlas account. ``directConnection=true`` talks straight to the
# single node instead of going through replica-set/SRV topology discovery.
MONGODB_ATLAS_HOST_WITH_PORT = "mongodb-atlas:27017"
MONGODB_ATLAS_CONNECTION_STRING = (
    f"mongodb://{MONGODB_ATLAS_HOST_WITH_PORT}/?directConnection=true"
)
MONGODB_ATLAS_BASE_NAME = "pathway_atlas_tests"
MONGODB_ATLAS_VECTOR_DIM = 736

ELASTICSEARCH_HOST = "elasticsearch"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_URL = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"

# Defaults match the compose service name; override via env vars to point the
# tests at a locally port-proxied Weaviate (see the module docstring).
WEAVIATE_HOST = os.environ.get("WEAVIATE_HOST", "weaviate")
WEAVIATE_HTTP_PORT = int(os.environ.get("WEAVIATE_PORT", "8080"))
WEAVIATE_GRPC_PORT = int(os.environ.get("WEAVIATE_GRPC_PORT", "50051"))
WEAVIATE_VECTOR_DIM = 3


def elasticsearch_now_ms() -> int:
    # The Elasticsearch reader requires a numeric timestamp column and watermarks
    # by it; tests build per-document timestamps off this epoch-millisecond clock.
    return int(time.time() * 1000)


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
# A second, otherwise-identical MySQL booted with `--local-infile=ON`. The
# `mysql` host keeps the image default (`local_infile=OFF`), so the connector's
# startup probe selects the multi-row INSERT fallback there and the LOAD DATA
# LOCAL INFILE fast path here. Tests parametrize over both to cover both write
# strategies. See the service definition in docker-compose-integration.yml.
MYSQL_LOCAL_INFILE_DB_HOST = "mysql-local-infile"
MYSQL_DB_PORT = 3306
MYSQL_DB_NAME = "testdb"
MYSQL_DB_USER = "testuser"
MYSQL_DB_PASSWORD = "testpass"
MYSQL_DB_ROOT_PASSWORD = "rootpass"


def mysql_connection_string(host: str = MYSQL_DB_HOST) -> str:
    return (
        f"mysql://{MYSQL_DB_USER}:{MYSQL_DB_PASSWORD}"
        + f"@{host}:{MYSQL_DB_PORT}/{MYSQL_DB_NAME}"
    )


MYSQL_CONNECTION_STRING = mysql_connection_string()
MYSQL_LOCAL_INFILE_CONNECTION_STRING = mysql_connection_string(
    MYSQL_LOCAL_INFILE_DB_HOST
)

# ---------------------------------------------------------------------------
# Cross-process reader/writer lock for the shared binary log.
#
# ``test_mysql_read_persistence_errors_when_binlog_purged`` runs
# ``PURGE BINARY LOGS``, which is a *server-global* operation: it deletes
# binary-log files for the whole instance, not just for one table. Any other
# streaming (binlog-tailing) test that happens to be resuming from a position
# inside a purged file then fails with ``ERROR 1236 (Could not find first log
# file name in binary log index file)`` and its worker crashes.
#
# The tests are marked ``xdist_group("mysql")`` to serialize them onto one
# worker, but that mark is only honored by ``--dist loadgroup`` — under the
# ``worksteal``/``load`` schedulers used in practice it is a no-op, so the
# tests really do run concurrently across workers. This readers/writer lock
# enforces the required isolation regardless of the scheduler: every
# binlog-tailing test holds it *shared* (they may all run at once), while the
# purge test holds it *exclusive* (it then runs strictly alone). The lock lives
# in a file shared by every xdist worker on the host.
#
# A gate file makes acquisition fair: a waiting writer takes the gate
# exclusively, which parks new readers behind it so a steady stream of readers
# cannot starve the purge test indefinitely.
_BINLOG_RWLOCK_PATH = os.path.join(tempfile.gettempdir(), "pw_mysql_binlog.rwlock")
_BINLOG_GATE_PATH = os.path.join(tempfile.gettempdir(), "pw_mysql_binlog.gate")


@contextmanager
def binlog_reader_access():
    """Shared access for a test that tails the MySQL binary log.

    Any number of readers may hold this simultaneously; it only blocks while
    the destructive purge test holds :func:`binlog_purge_access`.
    """
    gate_fd = os.open(_BINLOG_GATE_PATH, os.O_CREAT | os.O_RDWR, 0o666)
    lock_fd = os.open(_BINLOG_RWLOCK_PATH, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        # Pass through the gate (blocks only while a writer holds it), grab the
        # shared lock, then release the gate so other readers can follow.
        fcntl.flock(gate_fd, fcntl.LOCK_SH)
        fcntl.flock(lock_fd, fcntl.LOCK_SH)
        fcntl.flock(gate_fd, fcntl.LOCK_UN)
        yield
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
        os.close(gate_fd)


@contextmanager
def binlog_purge_access():
    """Exclusive access for the test that purges the binary log.

    Holding the gate exclusively parks new readers; acquiring the exclusive
    lock then waits for all in-flight readers to finish, so the purge runs with
    no binlog-tailing test active.
    """
    gate_fd = os.open(_BINLOG_GATE_PATH, os.O_CREAT | os.O_RDWR, 0o666)
    lock_fd = os.open(_BINLOG_RWLOCK_PATH, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(gate_fd, fcntl.LOCK_EX)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        fcntl.flock(gate_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
        os.close(gate_fd)


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


# ---------------------------------------------------------------------------
# Cross-process concurrency caps for the database-backed test suites.
#
# Every suite here points 48 xdist workers at a *single* database container.
# The per-suite `xdist_group(...)` marks are meant to land a suite on one
# worker, but that mark is a no-op under the `worksteal`/`load` schedulers used
# in practice (same caveat as the MySQL binlog lock below), so in reality
# dozens of workers hammer one server at once. A few servers don't degrade
# gracefully under that:
#
#   * SQL Server's CDC subsystem: every `sp_cdc_enable_table` fights the capture
#     agent for system-table locks and the agent has to scan the log for each
#     CDC-enabled table. Past a point the agent falls minutes behind and a whole
#     batch of unrelated MSSQL tests trips its checker timeout at once (~20
#     correlated failures while the server is pegged).
#   * PostgreSQL logical replication: every streaming test opens a walsender on
#     a temporary slot; under enough concurrent slots/connections the server
#     occasionally terminates a freshly started walsender, which the reader now
#     surfaces as an error (see `PsqlReader::get_next_records`) and the test
#     fails.
#
# Bound how many tests of a given suite touch its server concurrently with a
# counting semaphore: `N` lock files, and a test must grab any free one before
# it talks to the server and releases it on teardown. The cap keeps real
# parallelism (so the suite stays fast) while removing the overload that turns
# into correlated failures. Lock files live in a host-shared tmp dir so the
# bound holds across every xdist worker process. This treats the root cause
# (too much concurrency against one server) rather than masking it with reruns.
@contextmanager
def db_concurrency_slot(name: str, max_concurrency: int, poll_interval: float = 0.1):
    """Block until one of ``max_concurrency`` ``name`` slots is free, hold it
    for the duration of the ``with`` body, then release it."""
    paths = [
        os.path.join(tempfile.gettempdir(), f"pw_{name}_slot_{i}.lock")
        for i in range(max_concurrency)
    ]
    fds = [os.open(path, os.O_CREAT | os.O_RDWR, 0o666) for path in paths]
    acquired: int | None = None
    try:
        while acquired is None:
            for fd in fds:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = fd
                    break
                except OSError:
                    continue
            if acquired is None:
                time.sleep(poll_interval)
        yield
    finally:
        if acquired is not None:
            fcntl.flock(acquired, fcntl.LOCK_UN)
        for fd in fds:
            os.close(fd)


# SQL Server's CDC subsystem is the most contention-sensitive, so it gets the
# tightest bound; PostgreSQL handles concurrent replication better, so its cap
# is looser (it only needs to keep walsender churn below the level that trips
# server-side terminations).
MSSQL_MAX_CONCURRENCY = 8
# Tightened: under maximum concurrency PostgreSQL occasionally terminates a
# freshly-started walsender right after START_REPLICATION (the temporary slot
# can't be resumed, so the reader errors). A tighter bound lowers how often that
# happens. NOTE: this only reduces the frequency — it can't fully eliminate the
# server-side behavior; a fully robust fix would need a resumable (non-temporary)
# slot in the reader.
POSTGRES_MAX_CONCURRENCY = 6
# ClickHouse handles concurrent inserts well, so this is a generous bound — just
# enough to keep the single container off the failure mode where a freshly
# inserted block, acked over the native protocol, doesn't become visible to the
# follow-up HTTP SELECT within the checker window under maximum concurrency.
CLICKHOUSE_MAX_CONCURRENCY = 12
# MongoDB streaming tests each open a change-stream (oplog tailing) cursor; this
# is light insurance against the single-node replica set being overloaded. The
# real flakiness fix is in the parsing test itself: it no longer inserts new
# rows until the reader's initial snapshot has landed, so the change-stream
# catch-up (which only settles on two empty getMores) can't be kept spinning by
# concurrent inserts during reader startup.
MONGODB_MAX_CONCURRENCY = 12
# MySQL streaming tests tail the binary log; bound how many do so at once so the
# server isn't saturated while a resume test races the binlog position.
MYSQL_MAX_CONCURRENCY = 12
# Every ElasticSearch test creates its own index(es) on a single ES node with a
# small (512 MB) heap. Under the full suite (dozens of xdist workers, the host
# CPU saturated by every other connector's engine work) the node falls behind:
# index creation backs up the master's pending-task queue and freshly created
# primary shards stay INITIALIZING. A write that arrives before its primary is
# active fails with ``503 ... primary shard is not active`` once the bulk's
# one-minute wait elapses, which the engine surfaces as a hard error. Capping how
# many ES tests hit the node at once keeps that backlog short so primaries
# activate promptly (verified: at a starved node uncapped concurrency 503s while
# a cap of 8 lands every write). ``create_index`` additionally waits for the
# primary to be active before returning, as belt-and-suspenders for the race.
ELASTICSEARCH_MAX_CONCURRENCY = 8


@contextmanager
def mssql_concurrency_slot():
    with db_concurrency_slot("mssql", MSSQL_MAX_CONCURRENCY):
        yield


@contextmanager
def postgres_concurrency_slot():
    with db_concurrency_slot("postgres", POSTGRES_MAX_CONCURRENCY):
        yield


@contextmanager
def clickhouse_concurrency_slot():
    with db_concurrency_slot("clickhouse", CLICKHOUSE_MAX_CONCURRENCY):
        yield


@contextmanager
def mongodb_concurrency_slot():
    with db_concurrency_slot("mongodb", MONGODB_MAX_CONCURRENCY):
        yield


@contextmanager
def mysql_concurrency_slot():
    with db_concurrency_slot("mysql", MYSQL_MAX_CONCURRENCY):
        yield


@contextmanager
def elasticsearch_concurrency_slot():
    with db_concurrency_slot("elasticsearch", ELASTICSEARCH_MAX_CONCURRENCY):
        yield


def _connect_to_mysql(host: str = MYSQL_DB_HOST, timeout_sec: float = 120.0):
    """Open a MySQL connection, waiting for the server to become reachable.

    The official ``mysql`` Docker image briefly runs a socket-only temporary
    server (``--skip-networking``) while it creates the user and database during
    initialization. A TCP client can therefore be refused — or hit a transient
    "access denied" because the user does not exist yet — for a short window
    even after the container's healthcheck has flipped. Rather than gate the
    whole suite behind a one-shot reachability probe, wait for the real server
    with bounded backoff (the other connectors rely on docker-compose's
    ``service_healthy`` for the same guarantee).
    """
    deadline = time.monotonic() + timeout_sec
    delay = 0.5
    while True:
        try:
            return mysql.connector.connect(
                host=host,
                port=MYSQL_DB_PORT,
                database=MYSQL_DB_NAME,
                user=MYSQL_DB_USER,
                password=MYSQL_DB_PASSWORD,
                autocommit=True,
            )
        except mysql.connector.Error:
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 2.0)


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

    def execute_sql_with_retry(self, query: str, max_retries: int = 6) -> None:
        """``execute_sql`` with retry on PostgreSQL catalog concurrency
        errors.

        ``REASSIGN OWNED BY`` / ``DROP OWNED BY`` / ``DROP USER`` /
        ``DROP DATABASE`` mutate ``pg_authid``/``pg_database``
        system tables. When two test workers call these concurrently
        — even for *different* role names — PostgreSQL occasionally
        fails one of them with ``tuple concurrently updated`` (or
        ``deadlock detected``). The error is transient: re-running
        the same command immediately succeeds. This helper keeps
        per-test cleanup deterministic without serializing the whole
        suite.
        """
        TRANSIENT = (
            "tuple concurrently updated",
            "deadlock detected",
            "could not serialize",
        )
        for attempt in range(max_retries):
            try:
                self.cursor.execute(query)
                return
            except psycopg2.Error as e:
                msg = str(e)
                if not any(s in msg for s in TRANSIENT) or attempt + 1 == max_retries:
                    raise
                # The cursor may be in an aborted state after the
                # failed transaction. Recreate it before retrying.
                try:
                    self.cursor = self.connection.cursor()
                except Exception:
                    pass
                time.sleep(0.1 * (1.5**attempt) + random.uniform(0, 0.05))

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

    @contextmanager
    def temporary_table(self, ddl_fmt: str | None = None):
        """Yield a randomly-generated table name and drop the table
        unconditionally afterwards. If ``ddl_fmt`` is provided, it is
        executed first with a single ``{t}`` placeholder replaced by
        the generated name, e.g.
        ``"CREATE TABLE {t} (k BIGINT, v TEXT)"``. Pass ``ddl_fmt=None``
        to skip the CREATE — useful when the test lets Pathway create
        the table via ``init_mode="replace"`` /
        ``"create_if_not_exists"`` and only needs the cleanup.

        Replaces the ``table_name = random_table_name() / execute_sql
        CREATE / try ... finally DROP TABLE IF EXISTS`` boilerplate."""
        table_name = self.random_table_name()
        if ddl_fmt is not None:
            self.execute_sql(ddl_fmt.format(t=table_name))
        try:
            yield table_name
        finally:
            try:
                self.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                logging.warning(
                    f"Warning: Failed to drop temporary table {table_name}: {e}"
                )


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


class NeonContext(WireProtocolSupporterContext):

    def __init__(self, timeout_sec: float = 120.0):
        # The proxy provisions its ephemeral branch from a real Neon project, so
        # it cannot work without these. Fail loudly and immediately on a missing
        # credential rather than letting the connect loop below time out with an
        # opaque error — a misconfigured CI must never pass by accident.
        missing = [
            name
            for name in ("NEON_API_KEY", "NEON_PROJECT_ID")
            if not os.environ.get(name)
        ]
        if missing:
            raise RuntimeError(
                "NeonDB integration tests require "
                + " and ".join(missing)
                + " to be set: the neon_local proxy uses them to provision an "
                "ephemeral branch of a real Neon project."
            )

        # The ``neon_local`` proxy provisions a fresh ephemeral branch when its
        # container starts; the upstream compute can take several seconds to
        # become routable, and the proxy may briefly refuse connections in that
        # window. The compose dependency is therefore ``service_started`` (not
        # ``service_healthy``), so wait for the endpoint here with bounded
        # backoff — the same approach ``_connect_to_mysql`` uses for the MySQL
        # init window.
        deadline = time.monotonic() + timeout_sec
        delay = 0.5
        while True:
            try:
                super().__init__(
                    host=NEON_DB_HOST,
                    port=NEON_DB_PORT,
                    database=NEON_DB_NAME,
                    user=NEON_DB_USER,
                    password=NEON_DB_PASSWORD,
                )
                self._tracked_tables: set[str] = set()
                return
            except psycopg2.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 5.0)

    def __enter__(self) -> "NeonContext":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # Drop this context's tables and close the connection on exit, whatever
        # the verdict — see ``cleanup``.
        self.cleanup()

    def random_table_name(self) -> str:
        # Track every name handed out so ``cleanup`` can drop it even if the
        # test that used it left it behind (e.g. an assertion failed before a
        # manual DROP, or the test creates the table with no DROP at all).
        name = super().random_table_name()
        self._tracked_tables.add(name)
        return name

    @staticmethod
    def _quote_ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def cleanup(self) -> None:
        """Drop the tables this context created, then close the connection.
        Called from the ``neon`` fixture teardown, so it runs whatever the test
        verdict — pass, fail, or error. Only this instance's own tables (tracked
        through ``random_table_name``) are touched, so the suite stays
        parallel-safe and nothing leaks between tests in a run. End-of-run
        leftovers need no handling here: the proxy deletes the ephemeral Neon
        branch on shutdown (``DELETE_BRANCH=true``).
        """
        for name in self._tracked_tables:
            try:
                self.cursor.execute(
                    f"DROP TABLE IF EXISTS {self._quote_ident(name)} CASCADE"
                )
            except Exception as e:
                logging.warning(f"NeonDB cleanup: failed to drop table {name}: {e}")
        self._tracked_tables.clear()

        try:
            self.connection.close()
        except Exception:
            pass


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

    def __init__(self, timeout_sec: float = 60.0):
        # QuestDB opens its PG-wire port before it can actually answer queries,
        # so a bare connect against a just-started server can fail (or connect
        # to a server that then rejects the first query). Poll connect +
        # `SELECT 1` until it responds — this is the readiness check the
        # `flaky` reruns used to stand in for ("No way to check that DB is
        # ready to accept queries").
        deadline = time.monotonic() + timeout_sec
        delay = 0.25
        while True:
            try:
                super().__init__(
                    host=QUEST_DB_HOST,
                    port=QUEST_DB_WIRE_PORT,
                    database=QUEST_DB_NAME,
                    user=QUEST_DB_USER,
                    password=QUEST_DB_PASSWORD,
                )
                self.cursor.execute("SELECT 1")
                self.cursor.fetchall()
                return
            except Exception:
                try:
                    self.connection.close()
                except Exception:
                    pass
                if time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 2.0)


class ClickHouseContext:
    """Test helper talking to ClickHouse over its HTTP interface.

    The Pathway connector writes through the native protocol (port 9000); this
    context reads results back over HTTP (port 8123) so the tests need no extra
    native-protocol client dependency. ``connection_string`` is the value to be
    passed to ``pw.io.clickhouse.write``.
    """

    def __init__(self):
        self.http_url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/"
        self.connection_string = CLICKHOUSE_CONNECTION_STRING

    def _query(self, sql: str, *, settings: dict | None = None) -> str:
        params = {"database": CLICKHOUSE_DB_NAME}
        if settings is not None:
            params.update(settings)
        auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
        response = requests.post(
            self.http_url,
            params=params,
            data=sql.encode("utf-8"),
            auth=auth,
            timeout=30,
        )
        response.raise_for_status()
        return response.text

    def execute_sql(self, query: str) -> None:
        self._query(query)

    def drop_table(self, table_name: str) -> None:
        self.execute_sql(f"DROP TABLE IF EXISTS {table_name}")

    def random_table_name(self) -> str:
        return f"ch_{uuid.uuid4().hex}"

    @contextmanager
    def temp_table(self):
        """Yield a randomly-generated table name and drop the table afterwards,
        whether the body succeeds or raises.

        Replaces the ``table_name = random_table_name() ... try/finally
        drop_table`` boilerplate::

            with clickhouse.temp_table() as table_name:
                ...
        """
        table_name = self.random_table_name()
        try:
            yield table_name
        finally:
            try:
                self.drop_table(table_name)
            except Exception as e:
                logging.warning(f"Failed to drop temporary table {table_name}: {e}")

    def get_table_contents(
        self,
        table_name: str,
        column_names: list[str],
        sort_by: str | tuple | None = None,
        final_: bool = False,
    ) -> list[dict]:
        columns = ",".join(column_names)
        # ``FINAL`` forces ClickHouse to deduplicate a ReplacingMergeTree at query
        # time, returning the current snapshot (latest version per key, deleted
        # keys excluded) regardless of whether a background merge has run yet.
        final_clause = " FINAL" if final_ else ""
        sql = f"SELECT {columns} FROM {table_name}{final_clause} FORMAT JSON"
        # Return 64-bit integers as JSON numbers rather than quoted strings so
        # callers get native Python ints without post-processing.
        text = self._query(sql, settings={"output_format_json_quote_64bit_integers": 0})
        data = json.loads(text)["data"]
        # Each row is already keyed by the output column name (the alias for
        # expressions such as ``hex(col) AS col_hex``), so the rows are returned
        # as-is.
        result = [dict(row) for row in data]
        if sort_by is not None:
            if isinstance(sort_by, tuple):
                result.sort(key=lambda item: tuple(item[key] for key in sort_by))
            else:
                result.sort(key=lambda item: item[sort_by])
        return result


MILVUS_VECTOR_DIM = 3


def _is_milvus_transient_connect_error(e: Exception) -> bool:
    """Whether ``e`` is milvus-lite's embedded-server connection race.

    milvus-lite reports its local server as started as soon as the server
    process is alive, *before* the server's local socket actually accepts
    connections. The first client therefore races the socket coming up, and a
    client that loses the race fails with a ``server unavailable`` /
    ``connect failed`` error. Under heavy test parallelism the socket can take
    a few seconds to appear, so this is retried in
    :func:`_connect_milvus_client_with_retry`.
    """
    text = str(e)
    return (
        "Fail connecting to server" in text
        or "server unavailable" in text
        or "failed to connect to all addresses" in text
        or "No such file or directory" in text
    )


def _connect_milvus_client_with_retry(uri: str, *, timeout: float = 30.0):
    """Create a ``MilvusClient`` for ``uri``, retrying while a freshly started
    milvus-lite embedded server brings its local socket up (see
    :func:`_is_milvus_transient_connect_error`). Non-transient errors are raised
    immediately."""
    from pymilvus import MilvusClient

    deadline = time.monotonic() + timeout
    while True:
        try:
            return MilvusClient(uri)
        except Exception as e:
            if (
                not _is_milvus_transient_connect_error(e)
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.25)


def _make_milvus_client(uri: str):
    """Create a MilvusClient for the given URI.

    For local ``.db`` files, works around a pymilvus 2.6.x bug where the
    Unix-domain-socket address is not forwarded to the gRPC handler.  See
    ``pathway.io.milvus._make_client`` for the full explanation.
    """
    if uri.endswith(".db"):
        try:
            from milvus_lite.server_manager import server_manager_instance

            uds_uri = server_manager_instance.start_and_get_uri(uri)
            if uds_uri is None:
                raise RuntimeError(
                    f"milvus-lite failed to start a local server for: {uri}"
                )
            return _connect_milvus_client_with_retry(uds_uri)
        except ImportError:
            pass
    return _connect_milvus_client_with_retry(uri)


def _is_milvus_transient_init_error(e: Exception) -> bool:
    """Whether ``e`` is milvus-lite's embedded-server startup race.

    The local milvus-lite server occasionally serves a ``create_collection``
    before its storage layer finishes initializing, failing with
    ``Assert "init_flag_ == true" => Mmap manager has not been init`` (segcore
    error, code 2000). It's an internal milvus-lite race, not a Pathway issue,
    and a freshly started server doesn't hit it — see
    :meth:`MilvusContext._create_with_retry`.
    """
    text = str(e)
    return "Mmap manager has not been init" in text or "init_flag_" in text


class MilvusContext:
    def __init__(self, uri: str) -> None:
        from pymilvus import DataType

        self.uri = uri
        self._DataType = DataType
        self._retry_counter = 0
        self.client = _make_milvus_client(uri)

    def _restart_on_fresh_server(self) -> None:
        """Drop the current milvus-lite client and start a brand-new embedded
        server on a fresh ``.db`` path, so its storage layer initializes from
        scratch. Used to recover from the startup race (the already-running
        server that lost the race can't be re-initialized in place, so a new
        one keyed on a new path is the way out)."""
        try:
            self.client.close()
        except Exception:
            pass
        self._retry_counter += 1
        suffix = f"_retry{self._retry_counter}"
        self.uri = (
            self.uri[:-3] + suffix + ".db"
            if self.uri.endswith(".db")
            else self.uri + suffix
        )
        self.client = _make_milvus_client(self.uri)

    def create_collection_with_retry(self, build_and_create) -> None:
        """Public entry point for tests that build a collection inline (their
        own schema/index) rather than via :meth:`create_collection` /
        :meth:`create_scalar_collection`. ``build_and_create`` takes no
        arguments and must read ``self.client`` afresh each call (so a server
        restart is picked up) — i.e. build the schema/index off
        ``<ctx>.client`` and call ``<ctx>.client.create_collection(...)``. It is
        retried through the same milvus-lite init-race recovery."""
        self._create_with_retry(build_and_create)

    def _create_with_retry(self, build_and_create, max_retries: int = 5) -> None:
        """Run ``build_and_create`` (which builds a schema off ``self.client``
        and calls ``create_collection``), retrying on milvus-lite's init race by
        restarting the embedded server on a fresh path. The schema is rebuilt
        each attempt because it is bound to the (now replaced) client."""
        for attempt in range(max_retries):
            try:
                build_and_create()
                return
            except Exception as e:
                if attempt < max_retries - 1 and _is_milvus_transient_init_error(e):
                    logging.warning(
                        f"milvus-lite init race on attempt {attempt + 1}/"
                        f"{max_retries}; restarting embedded server: {e}"
                    )
                    self._restart_on_fresh_server()
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise

    def create_collection(
        self, collection_name: str, *, dimension: int = MILVUS_VECTOR_DIM
    ) -> None:
        def build_and_create() -> None:
            schema = self.client.create_schema(
                auto_id=False, enable_dynamic_field=False
            )
            schema.add_field("id", self._DataType.INT64, is_primary=True)
            schema.add_field("vector", self._DataType.FLOAT_VECTOR, dim=dimension)
            index_params = self.client.prepare_index_params()
            index_params.add_index("vector", metric_type="COSINE", index_type="FLAT")
            self.client.create_collection(
                collection_name, schema=schema, index_params=index_params
            )

        self._create_with_retry(build_and_create)

    def create_scalar_collection(
        self, collection_name: str, value_type, **value_kwargs
    ) -> None:
        """Create a collection with id (INT64 PK), value (<value_type>), vec (FLOAT_VECTOR).

        ``value_type`` is a Milvus ``DataType``. Extra keyword arguments are
        forwarded to ``add_field`` for the value field (e.g. ``max_length`` for
        ``VARCHAR``).  The ``vec`` field is always added so that the collection
        can be indexed.
        """

        def build_and_create() -> None:
            schema = self.client.create_schema(
                auto_id=False, enable_dynamic_field=False
            )
            schema.add_field("id", self._DataType.INT64, is_primary=True)
            schema.add_field("value", value_type, **value_kwargs)
            schema.add_field("vec", self._DataType.FLOAT_VECTOR, dim=MILVUS_VECTOR_DIM)
            index_params = self.client.prepare_index_params()
            index_params.add_index("vec", metric_type="COSINE", index_type="FLAT")
            self.client.create_collection(
                collection_name, schema=schema, index_params=index_params
            )

        self._create_with_retry(build_and_create)

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


# Host/port of the Chroma server. The defaults match the ``chroma`` service in
# docker-compose; both are env-overridable so the suite can also run against a
# Chroma container published on localhost without editing this file.
CHROMA_HOST = os.environ.get("CHROMA_HOST", "chroma")
CHROMA_PORT = int(os.environ.get("CHROMA_PORT", "8000"))


class ChromaContext:
    """Test helper talking to a Chroma server over its HTTP API.

    ``host``/``port`` are what the tests pass to ``pw.io.chroma.write``. The
    constructor polls the server's heartbeat with bounded backoff before
    returning: the container's TCP healthcheck flips as soon as the socket is
    open, which can be a moment before the API actually answers, so this is the
    same readiness pattern :class:`QuestDBContext` uses.
    """

    def __init__(
        self,
        host: str = CHROMA_HOST,
        port: int = CHROMA_PORT,
        timeout_sec: float = 60.0,
    ) -> None:
        import chromadb

        self.host = host
        self.port = port
        deadline = time.monotonic() + timeout_sec
        delay = 0.25
        while True:
            try:
                self.client = chromadb.HttpClient(host=host, port=port)
                self.client.heartbeat()
                return
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 2.0)

    def generate_collection_name(self) -> str:
        return f"chroma_{uuid.uuid4().hex[:12]}"

    def create_collection(self, collection_name: str) -> None:
        self.client.create_collection(collection_name)

    def delete_collection(self, collection_name: str) -> None:
        try:
            self.client.delete_collection(collection_name)
        except Exception as e:
            logging.warning(f"Failed to drop Chroma collection {collection_name}: {e}")

    def count(self, collection_name: str) -> int:
        return self.client.get_collection(collection_name).count()

    def query_all(
        self,
        collection_name: str,
        include: tuple[str, ...] = ("embeddings", "documents", "metadatas"),
    ) -> list[dict]:
        """Return every record as a list of dicts keyed ``id``/``embedding``/
        ``document``/``metadata``, sorted by id.

        Embeddings come back from Chroma as ``numpy.ndarray`` (float32); they are
        converted to plain ``list[float]`` so callers can compare against the
        values they wrote.
        """
        collection = self.client.get_collection(collection_name)
        # ``include`` is a Chroma Literal union and ``GetResult`` is a TypedDict
        # whose optional fields are typed ``... | None``; cast to a plain dict so
        # the by-index access below type-checks.
        result: dict = cast(dict, collection.get(include=cast(Any, list(include))))
        rows = []
        for index, id_ in enumerate(result["ids"]):
            row: dict[str, Any] = {"id": id_}
            if result.get("embeddings") is not None:
                row["embedding"] = [float(x) for x in result["embeddings"][index]]
            if result.get("documents") is not None:
                row["document"] = result["documents"][index]
            if result.get("metadatas") is not None:
                row["metadata"] = result["metadatas"][index]
            rows.append(row)
        rows.sort(key=lambda r: r["id"])
        return rows

    def close(self) -> None:
        # chromadb.HttpClient holds no socket of its own to close; clearing the
        # process-wide client cache keeps repeated fixtures from accumulating
        # identity-cached clients across the run.
        try:
            self.client.clear_system_cache()
        except Exception:
            pass


QDRANT_VECTOR_DIM = 3


class QdrantContext:
    def __init__(self, grpc_url: str, rest_url: str) -> None:
        from qdrant_client import QdrantClient

        # The native connector talks to Qdrant over gRPC, so the URL handed to
        # ``pw.io.qdrant.write`` points at the gRPC port. The verification client
        # below is the Python client, which uses the REST port.
        #
        # An explicit timeout because the client's default (httpx's 5 s) is far
        # too tight for a shared container under the full parallel suite: when
        # the host is saturated by every other connector's engine work, a
        # ``create_collection`` (WAL fsync + collection registry updates) can
        # take tens of seconds even though it succeeds — with the default the
        # whole test dies on ``ReadTimeout`` instead of just waiting it out.
        self.url = grpc_url
        self.client = QdrantClient(url=rest_url, timeout=120)
        # Names handed out by ``generate_collection_name`` so the fixture can
        # drop them on teardown. A Qdrant node keeps every collection's RocksDB
        # files open, so leaking collections across the (parallel) suite would
        # eventually exhaust the container's file descriptors and fail
        # create_collection — delete each test's collections instead.
        self._collections: list[str] = []

    def create_collection(
        self, collection_name: str, *, dimension: int = QDRANT_VECTOR_DIM, distance=None
    ) -> None:
        """Create a collection with the given vector dimension and distance.

        Defaults to Euclidean distance so that stored vectors are returned
        verbatim — Qdrant normalizes vectors when the collection uses Cosine
        distance, which would break exact vector comparisons in the tests.
        """
        from qdrant_client.models import Distance, VectorParams

        self.client.create_collection(
            collection_name,
            vectors_config=VectorParams(
                size=dimension, distance=distance or Distance.EUCLID
            ),
        )

    def query_all(
        self, collection_name: str, *, with_vectors: bool = True
    ) -> list[dict]:
        """Return every point as its payload, with the vector added under
        ``"vector"``.

        The Qdrant point id is the connector-generated UUID derived from the
        Pathway row key, so it is not asserted on directly; the row's own
        identifier columns live in the payload.
        """
        points, _ = self.client.scroll(
            collection_name,
            limit=100000,
            with_payload=True,
            with_vectors=with_vectors,
        )
        result = []
        for point in points:
            row: dict = dict(point.payload or {})
            if with_vectors and point.vector is not None:
                row["vector"] = list(point.vector)
            result.append(row)
        return result

    def count(self, collection_name: str) -> int:
        return self.client.count(collection_name, exact=True).count

    def generate_collection_name(self) -> str:
        name = f"qdrant_{uuid.uuid4().hex[:12]}"
        self._collections.append(name)
        return name

    def close(self) -> None:
        # Drop every collection the test touched (whether created directly or
        # auto-created by the connector) before closing, so collections do not
        # accumulate on the shared Qdrant container.
        for collection_name in self._collections:
            try:
                self.client.delete_collection(collection_name)
            except Exception:
                pass
        self.client.close()


class WeaviateContext:
    """Minimal Weaviate helper for the output-connector tests.

    Connects to a running Weaviate over its REST + gRPC endpoints. Each test
    creates a uniquely named collection (Weaviate class names must start with a
    capital letter) configured with the ``none`` vectorizer so the vectors the
    connector sends are stored verbatim rather than recomputed server-side. The
    vector dimension is not pinned at creation time — Weaviate fixes it from the
    first object written — so :meth:`create_collection` ignores the ``dimension``
    argument it accepts for parity with the other vector-store contexts.
    """

    def __init__(
        self,
        host: str = WEAVIATE_HOST,
        http_port: int = WEAVIATE_HTTP_PORT,
        grpc_port: int = WEAVIATE_GRPC_PORT,
    ) -> None:
        self.host = host
        self.http_port = http_port
        self.grpc_port = grpc_port
        self._created_collections: list[str] = []
        self.client = self._connect()

    def _connect(self):
        import weaviate

        return weaviate.connect_to_custom(
            http_host=self.host,
            http_port=self.http_port,
            http_secure=False,
            grpc_host=self.host,
            grpc_port=self.grpc_port,
            grpc_secure=False,
        )

    def create_collection(
        self, collection_name: str, *, dimension: int = WEAVIATE_VECTOR_DIM
    ) -> None:
        from weaviate.classes.config import Configure

        # No explicit properties: the primary key is encoded in the object UUID
        # (Weaviate reserves the "id" property name), and any other property the
        # connector writes is added by Weaviate's auto-schema on first insert.
        self._created_collections.append(collection_name)
        self.client.collections.create(
            collection_name,
            vectorizer_config=Configure.Vectorizer.none(),
        )

    def query_all(
        self, collection_name: str, *, include_vector: bool = True
    ) -> list[dict]:
        """Return every object as a dict of its properties plus its ``"uuid"``.

        The primary key is not stored as a property — it is encoded in the object
        UUID — so the UUID (as a string) is included for identity checks. When
        ``include_vector`` is set, the vector is included under ``"vector"``.
        """
        collection = self.client.collections.get(collection_name)
        results = []
        for obj in collection.iterator(include_vector=include_vector):
            row = dict(obj.properties)
            row["uuid"] = str(obj.uuid)
            if include_vector:
                row["vector"] = list(obj.vector["default"])
            results.append(row)
        return results

    def count(self, collection_name: str) -> int:
        collection = self.client.collections.get(collection_name)
        return collection.aggregate.over_all(total_count=True).total_count

    def property_types(self, collection_name: str) -> dict[str, str]:
        """Return the Weaviate-inferred data type (as a string) of each property."""
        config = self.client.collections.get(collection_name).config.get()
        return {prop.name: prop.data_type.value for prop in config.properties}

    def generate_collection_name(self) -> str:
        return f"Pwtest{uuid.uuid4().hex[:12]}"

    def cleanup(self) -> None:
        for collection_name in self._created_collections:
            try:
                self.client.collections.delete(collection_name)
            except Exception:
                pass
        self._created_collections = []
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
        # Read with the default ("local") read concern, NOT "majority". The tests
        # gate on this via default-read-concern polls (e.g. `count_documents`), and
        # the connector writes with the default write concern (w:1), so the data is
        # already committed on the primary. A "majority" read here can lag behind
        # those just-written rows under load — the single-node replica set's
        # majority-commit point advances slightly after the write is locally
        # visible — producing a spurious "row missing" assertion even though the
        # data is present (it would appear a few hundred ms later). Reading the
        # primary with the default concern returns the same committed state the
        # poll observed.
        db = self.client[MONGODB_BASE_NAME]
        return list(db[collection_name].find({}, {"_id": 0}))

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


class AtlasContext:
    """Helper for the ``mongodb-atlas`` compose service (MongoDB Atlas Local).

    Wraps a ``pymongo`` client pointed at the in-network ``mongodb-atlas``
    service. That service runs the ``mongodb/mongodb-atlas-local`` image, the
    only MongoDB image that ships the ``mongot`` search process, so the
    Atlas-specific ``$vectorSearch`` aggregation works against it directly —
    no cloud Atlas account, and no per-test ``docker run`` from inside the
    tests. The vector-search index is created through the regular driver
    (``createSearchIndexes``), which is why these helpers use ``pymongo``
    rather than any connector code.
    """

    INDEX_READY_TIMEOUT_S = 180

    def __init__(self):
        self.connection_string = MONGODB_ATLAS_CONNECTION_STRING
        self.client: MongoClient = MongoClient(MONGODB_ATLAS_CONNECTION_STRING)

    def collection_name(self) -> str:
        return f"vec_{uuid.uuid4().hex}"

    def collection(self, name: str):
        return self.client[MONGODB_ATLAS_BASE_NAME][name]

    def documents(self, name: str) -> list[dict]:
        return list(self.collection(name).find({}))

    def create_vector_index(
        self,
        collection: str,
        *,
        path: str = "embedding",
        dimensions: int = MONGODB_ATLAS_VECTOR_DIM,
        similarity: str = "cosine",
        index_name: str = "pw_vector_index",
    ) -> str:
        """Create an Atlas Vector Search index and block until it is queryable.

        This is the only Atlas-specific step. It mirrors what a user would run
        once after pointing ``pw.io.mongodb.write`` at their Atlas collection.
        """
        model = SearchIndexModel(
            definition={
                "fields": [
                    {
                        "type": "vector",
                        "path": path,
                        "numDimensions": dimensions,
                        "similarity": similarity,
                    }
                ]
            },
            name=index_name,
            type="vectorSearch",
        )
        self.collection(collection).create_search_index(model)
        self._wait_index_queryable(collection, index_name)
        return index_name

    def _wait_index_queryable(self, collection: str, index_name: str) -> None:
        deadline = time.monotonic() + self.INDEX_READY_TIMEOUT_S
        while time.monotonic() < deadline:
            for idx in self.collection(collection).list_search_indexes():
                if idx.get("name") == index_name and idx.get("queryable"):
                    return
            time.sleep(2)
        raise TimeoutError(
            f"vector search index {index_name!r} did not become queryable "
            f"within {self.INDEX_READY_TIMEOUT_S}s"
        )

    def vector_search(
        self,
        collection: str,
        query_vector,
        *,
        index_name: str = "pw_vector_index",
        path: str = "embedding",
        limit: int = 3,
        num_candidates: int = 100,
    ) -> list[dict]:
        pipeline = [
            {
                "$vectorSearch": {
                    "index": index_name,
                    "path": path,
                    "queryVector": [float(x) for x in query_vector],
                    "numCandidates": num_candidates,
                    "limit": limit,
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "doc_id": 1,
                    "score": {"$meta": "vectorSearchScore"},
                }
            },
        ]
        return list(self.collection(collection).aggregate(pipeline))


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


class ElasticsearchContext:
    """Minimal Elasticsearch helper for the input-connector tests.

    Talks to the cluster over its REST API with ``requests`` so the tests do not
    depend on the Python ``elasticsearch`` client. Documents are indexed with
    ``refresh="wait_for"`` so they are immediately visible to the connector's
    search queries.

    Every request goes through :meth:`_request`, which retries on transient
    connection / read-timeout errors. A single Elasticsearch node shared by many
    parallel xdist workers (each also running a Pathway engine that hammers it)
    can momentarily take longer than a naive timeout to answer; the retry makes
    the bookkeeping resilient so it never turns cluster back-pressure into a
    spurious test failure (the same reason :func:`_connect_to_mysql` waits for
    MySQL to become reachable).
    """

    # Generous per-attempt timeout plus retries: under heavy parallel load a
    # request can legitimately take a while, and an occasional dropped
    # connection should be retried rather than failing the test.
    _REQUEST_TIMEOUT = 120
    _MAX_ATTEMPTS = 8

    def __init__(self, base_url: str = ELASTICSEARCH_URL):
        self.base_url = base_url.rstrip("/")
        self._created_indices: list[str] = []
        self._session = requests.Session()

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self._REQUEST_TIMEOUT)
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(self._MAX_ATTEMPTS):
            try:
                return self._session.request(method, url, **kwargs)
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                time.sleep(min(0.5 * (2**attempt), 8.0))
        assert last_exc is not None
        raise last_exc

    def generate_index_name(self) -> str:
        return f'es_{str(uuid.uuid4()).replace("-", "")}'

    def create_index(
        self,
        index_name: str,
        properties: dict[str, dict[str, str]],
        *,
        dynamic: bool = True,
    ) -> str:
        """Create an index with an explicit mapping.

        The timestamp and id columns must be given sortable mappings (``long``
        and ``keyword`` respectively) so the connector can ``range``-filter and
        sort on them. Pass ``dynamic=False`` to keep all other fields in
        ``_source`` (so the connector still reads them) while never indexing
        them — this avoids dynamic-mapping conflicts when a single field holds
        values of different JSON shapes across documents (e.g. a ``Json`` column
        that is sometimes an object, sometimes an array, sometimes a scalar).
        """
        mappings: dict[str, Any] = {"properties": properties}
        if not dynamic:
            mappings["dynamic"] = False
        # Track the index up-front so cleanup runs even if a retried create
        # actually succeeded server-side before a read timeout surfaced.
        self._created_indices.append(index_name)
        # `wait_for_active_shards=1` makes the create wait for the primary shard
        # to be active before returning (see `_wait_for_index_active` for why).
        response = self._request(
            "PUT",
            f"/{index_name}",
            json={"mappings": mappings},
            params={"wait_for_active_shards": 1},
        )
        # A create that timed out after succeeding, then retried, comes back as
        # "already exists" — treat that as success.
        if response.status_code == 400 and (
            "resource_already_exists_exception" in response.text
        ):
            self._wait_for_index_active(index_name)
            return index_name
        response.raise_for_status()
        self._wait_for_index_active(index_name)
        return index_name

    def _wait_for_index_active(self, index_name: str, timeout: float = 120.0) -> None:
        """Block until the index's primary shard is active.

        A successful ``PUT /<index>`` only means the index exists in cluster
        state; its primary shard may still be ``INITIALIZING``. Writing to it
        before the primary is active fails the bulk request with
        ``503 ... primary shard is not active`` — which the connector surfaces as
        an engine error after exhausting its retries. Under a busy single-node
        cluster (the whole db-connectors suite hammering one ES container) shard
        allocation lags far enough behind index creation that the write loses
        this race. ``wait_for_active_shards`` on create covers the common case,
        but its own timeout can elapse under load, so poll cluster health here
        with a generous deadline as the actual guarantee.

        On a single node the default replica stays ``UNASSIGNED`` (health never
        reaches ``green``), so ``yellow`` is the success state — it means exactly
        "primary active".
        """
        deadline = time.monotonic() + timeout
        while True:
            response = self._request(
                "GET",
                f"/_cluster/health/{index_name}",
                params={"wait_for_status": "yellow", "timeout": "10s"},
            )
            if response.status_code == 200:
                body = response.json()
                if not body.get("timed_out", True) and body.get("status") in (
                    "yellow",
                    "green",
                ):
                    return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Elasticsearch index {index_name!r} primary shard did not "
                    f"become active within {timeout}s (last health: {response.text})"
                )

    def refresh(self, index_name: str) -> None:
        self._request("POST", f"/{index_name}/_refresh")

    def get_all_sources(self, index_name: str) -> list[dict[str, Any]]:
        """Return the ``_source`` of every document in the index."""
        self.refresh(index_name)
        response = self._request(
            "GET",
            f"/{index_name}/_search",
            json={"size": 10000, "query": {"match_all": {}}},
        )
        response.raise_for_status()
        return [hit["_source"] for hit in response.json()["hits"]["hits"]]

    def index_document(
        self, index_name: str, document: dict[str, Any], *, refresh: bool = True
    ) -> None:
        params = {"refresh": "wait_for"} if refresh else {}
        response = self._request(
            "POST", f"/{index_name}/_doc", json=document, params=params
        )
        response.raise_for_status()

    def index_documents(
        self,
        index_name: str,
        documents: list[dict[str, Any]],
        *,
        refresh: bool = True,
        id_field: str | None = None,
    ) -> None:
        """Bulk-index documents.

        When ``id_field`` is given, each document's ``_id`` is set from that
        field so a retried bulk (after a transient error) upserts rather than
        duplicating — keeping document counts exact under parallel load.
        """
        if not documents:
            return
        lines = []
        for document in documents:
            if id_field is not None:
                action = {"index": {"_id": str(document[id_field])}}
            else:
                action = {"index": {}}
            lines.append(json.dumps(action))
            lines.append(json.dumps(document))
        body = "\n".join(lines) + "\n"
        params = {"refresh": "wait_for"} if refresh else {}
        response = self._request(
            "POST",
            f"/{index_name}/_bulk",
            data=body,
            params=params,
            headers={"Content-Type": "application/x-ndjson"},
        )
        response.raise_for_status()
        assert not response.json().get("errors"), response.text

    def document_count(self, index_name: str) -> int:
        self.refresh(index_name)
        response = self._request("GET", f"/{index_name}/_count")
        response.raise_for_status()
        return response.json()["count"]

    def max_content_length(self) -> int:
        """The cluster's smallest ``http.max_content_length`` (in bytes).

        Elasticsearch rejects any HTTP body larger than this with
        ``413 Request Entity Too Large``. Read live from the node info so a test
        sizing a batch against the limit stays correct even if the cluster is
        configured with a non-default value.
        """
        response = self._request(
            "GET", "/_nodes/http", params={"filter_path": "nodes.*.http"}
        )
        response.raise_for_status()
        nodes = response.json()["nodes"]
        return min(
            node["http"]["max_content_length_in_bytes"] for node in nodes.values()
        )

    def delete_index(self, index_name: str) -> None:
        self._request("DELETE", f"/{index_name}", params={"ignore_unavailable": "true"})

    def cleanup(self) -> None:
        for index_name in self._created_indices:
            self.delete_index(index_name)
        self._created_indices.clear()
        self._session.close()


class MySQLContext:
    def __init__(self, host: str = MYSQL_DB_HOST):
        self.host = host
        self.connection_string = mysql_connection_string(host)
        self.connection = _connect_to_mysql(host)
        self.cursor = self.connection.cursor()

    def close(self) -> None:
        """Close the underlying connection.

        Each ``MySQLContext`` holds one server connection for its whole
        lifetime. Left to garbage collection, dozens of them linger across an
        ``xdist`` run and crowd out the small per-writer pools, which is exactly
        what manifests as a flaky ``ERROR 1040 (Too many connections)``. Closing
        promptly — on fixture teardown and when a helper thread finishes — keeps
        the live connection count bounded and deterministic.
        """
        try:
            self.connection.close()
        except Exception:
            pass

    def get_table_schema(self, table_name: str) -> dict[str, ColumnProperties]:
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position;
        """
        self.cursor.execute(query, (table_name, self.connection.database))
        rows = self.cursor.fetchall()

        schema_props: dict[str, ColumnProperties] = {}
        for column_name, type_name, is_nullable in rows:
            schema_props[str(column_name)] = ColumnProperties(
                str(type_name).lower(), str(is_nullable).upper() == "YES"
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

    def grant_replication_privileges(self) -> None:
        """Grant the privileges the binary-log reader needs to ``testuser``.

        ``COM_BINLOG_DUMP`` requires ``REPLICATION SLAVE`` and
        ``SHOW MASTER STATUS`` / ``SHOW BINARY LOGS`` require
        ``REPLICATION CLIENT`` — both are server-global privileges that the
        ``MYSQL_USER`` created by the container does not get by default. Grant
        them through a fresh root connection (idempotent, so it is safe to call
        from every streaming test).
        """
        root_connection = mysql.connector.connect(
            host=self.host,
            port=MYSQL_DB_PORT,
            user="root",
            password=MYSQL_DB_ROOT_PASSWORD,
            autocommit=True,
        )
        try:
            cursor = root_connection.cursor()
            cursor.execute(
                f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* "
                f"TO '{MYSQL_DB_USER}'@'%'"
            )
            cursor.execute("FLUSH PRIVILEGES")
        finally:
            root_connection.close()

    def execute_as_root(self, query: str) -> list:
        """Run a statement through a fresh root connection and return any rows.

        Used for binary-log administration (``FLUSH``/``PURGE BINARY LOGS``,
        ``SHOW BINARY LOGS``) that requires privileges ``testuser`` does not
        hold. Non-result statements simply return an empty list.
        """
        root_connection = mysql.connector.connect(
            host=self.host,
            port=MYSQL_DB_PORT,
            user="root",
            password=MYSQL_DB_ROOT_PASSWORD,
            autocommit=True,
        )
        try:
            cursor = root_connection.cursor()
            cursor.execute(query)
            try:
                return cursor.fetchall()
            except mysql.connector.Error:
                return []
        finally:
            root_connection.close()

    def binlog_available(self) -> bool:
        """Whether the server is configured for row-based binary logging, the
        prerequisite for the streaming (CDC) reader."""
        try:
            self.cursor.execute("SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format")
            row = self.cursor.fetchone()
        except Exception:
            return False
        if row is None:
            return False
        log_bin, binlog_format = row
        return int(log_bin) == 1 and str(binlog_format).upper() == "ROW"


class MssqlContext:
    def __init__(self):
        import pymssql

        self.connection = pymssql.connect(
            server=MSSQL_DB_HOST,
            port=str(MSSQL_DB_PORT),
            user=MSSQL_DB_USER,
            password=MSSQL_DB_PASSWORD,
            database=MSSQL_DB_NAME,
            autocommit=True,
            tds_version="7.3",
        )
        self.cursor = self.connection.cursor()
        # Tables and CDC capture instances we've registered.  ``cleanup()``
        # tears them all down on fixture teardown, so a crashed/skipped test
        # cannot leave orphan capture instances behind — orphans inflate
        # ``cdc.lsn_time_mapping`` and the SQL Agent's per-instance scan, which
        # is what made later runs of CDC tests time out waiting for the agent
        # to catch up.
        self._tracked_tables: set[str] = set()
        self._tracked_cdc: set[str] = set()

    def random_table_name(self) -> str:
        name = f"mssql_{uuid.uuid4().hex}"
        # Track every name handed out: every existing caller of this method
        # uses the returned name in a CREATE TABLE on this same context.
        self._tracked_tables.add(name)
        return name

    def execute_sql(
        self,
        query: str,
        params: tuple | None = None,
        drain_status_rows: bool = False,
        max_retries: int = 6,
        retry_delay: float = 0.5,
    ) -> None:
        """Execute a single statement, retrying on deadlock victim (1205).

        Under heavy xdist parallelism every statement that touches CDC system
        metadata (sp_cdc_* procs, DDL on CDC-enabled objects, queries against
        cdc.* tables) competes with the capture agent for system-table locks
        and SQL Server occasionally elects it as the deadlock victim.  Heavy
        DDL on master.sys.databases (CREATE/ALTER/DROP DATABASE) and on
        sys.objects/sys.schemas (CREATE/DROP TABLE/SCHEMA) is similarly
        vulnerable.  The recommended response per Microsoft is to rerun the
        transaction, which is what we do here.

        A fresh cursor is opened per attempt — pymssql leaves the cursor in a
        state where the next statement reports "Statement(s) could not be
        prepared" after a 1205.  On success ``self.cursor`` is updated to the
        cursor that ran the statement, so callers can immediately
        ``self.cursor.fetchone()`` / ``self.cursor.fetchall()`` to read result
        rows from a SELECT.

        ``drain_status_rows=True`` discards every rowset produced by the
        statement (use for sp_cdc_* procs that emit "(N rows affected)"
        rowsets pymssql leaves pending — the next statement on the same
        connection would otherwise fail with "Statement(s) could not be
        prepared").  Do not pass this for SELECT — it would discard the data.

        Non-1205 exceptions propagate immediately — this is for transient
        contention, not for swallowing real errors.
        """
        last_exc: Exception | None = None
        for attempt in range(max_retries):
            cur = self.connection.cursor()
            try:
                if params is None:
                    cur.execute(query)
                else:
                    cur.execute(query, params)
                if drain_status_rows:
                    while cur.nextset():
                        pass
                self.cursor = cur
                return
            except pymssql.exceptions.OperationalError as e:
                if "1205" in str(e) and attempt < max_retries - 1:
                    last_exc = e
                    logging.warning(
                        f"deadlock victim on attempt {attempt + 1}/"
                        f"{max_retries}, retrying: {e}"
                    )
                    # Jitter is essential here, not cosmetic: two workers
                    # deadlocked against each other (or against the capture
                    # agent) and retrying on the same deterministic schedule
                    # keep colliding and re-deadlocking. A random component
                    # desynchronizes their retries so one wins.
                    time.sleep(
                        retry_delay * (1.25**attempt) + random.uniform(0, retry_delay)
                    )
                    continue
                raise
        if last_exc is not None:
            raise last_exc

    def wait_for_capture_count(
        self, table_name: str, target_count: int, timeout_sec: float = 90.0
    ) -> None:
        """Block until ``cdc.dbo_<table_name>_CT`` contains at least
        ``target_count`` rows.

        SQL Server's CDC capture agent processes transactions asynchronously
        from the log, in LSN order.  Under heavy concurrent write load
        (multiple xdist workers writing to multiple CDC-enabled tables in the
        same database) the agent can be many seconds behind — a freshly
        committed insert may not appear in its capture instance's change table
        for a while.

        Weaker waits (e.g. polling ``fn_cdc_get_min_lsn`` for non-NULL, which
        only proves the *capture instance* exists, or polling
        ``MAX(__$start_lsn)`` for any non-NULL value, which trips on the very
        first row regardless of how many we expect) accept the partial state
        and let the test continue with stale CDC contents — the persisted
        offset and the data the resume path replays end up disagreeing.

        Counting against an expected target (one row per insert, one per
        delete, two per update with ``all update old``) is the only check that
        proves *our* writes are captured before the next test step.
        """
        capture_instance = f"dbo_{table_name}"
        query = f"SELECT COUNT(*) FROM cdc.{capture_instance}_CT"
        deadline = time.time() + timeout_sec
        last_count = -1
        while time.time() < deadline:
            self.execute_sql(query)
            row = self.cursor.fetchone()
            count = int(str(row[0])) if row is not None and row[0] is not None else 0
            if count >= target_count:
                return
            last_count = count
            time.sleep(0.25)
        raise AssertionError(
            f"CDC capture agent did not produce {target_count} rows for "
            f"'{capture_instance}' within {timeout_sec}s — last seen: {last_count}"
        )

    def ensure_database_cdc(self) -> None:
        """Idempotently enable change data capture at the database level.

        Table-level ``sp_cdc_enable_table`` (see ``enable_cdc``) fails with
        error 22901 unless the database itself is CDC-enabled first.  The
        docker ``mssql-init`` job runs ``sp_cdc_enable_db`` once, but relying
        on a single external one-shot step makes every CDC test flaky: if that
        step is skipped or silently fails (``sqlcmd`` does not fail on SQL
        errors without ``-b``) the whole suite collapses with 22901 in setup.
        Re-establishing the precondition from inside the fixture makes the CDC
        tests self-sufficient regardless of how the database was provisioned.

        Guarded by ``is_cdc_enabled`` so the steady state is a cheap no-op, and
        tolerant of the concurrent-enable race under xdist: if another worker
        enables CDC between our check and our ``EXEC``, ``sp_cdc_enable_db``
        rejects the second call — we re-confirm the desired end state and only
        re-raise if it is genuinely not enabled.
        """
        self.execute_sql(
            "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()"
        )
        row = self.cursor.fetchone()
        if row is not None and row[0]:
            return
        try:
            self.execute_sql("EXEC sys.sp_cdc_enable_db", drain_status_rows=True)
        except pymssql.exceptions.OperationalError as e:
            self.execute_sql(
                "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()"
            )
            row = self.cursor.fetchone()
            if not (row is not None and row[0]):
                raise
            logging.warning(f"sp_cdc_enable_db raced with another worker: {e}")

    def enable_cdc(
        self,
        table_name: str,
        max_retries: int = 10,
        retry_delay: float = 1.0,
        captured_column_list: str | None = None,
        capture_instance: str | None = None,
    ) -> None:
        """Enable CDC on a table, retrying on transient SQL Server failures.

        ``captured_column_list`` restricts capture to a subset of the table's
        columns (``@captured_column_list``); ``capture_instance`` names the
        capture instance (``@capture_instance``) so a single table can carry
        more than one. Both default to SQL Server's behavior (all columns, a
        single ``dbo_<table>`` instance).

        Every caller must go through here rather than issuing
        ``sp_cdc_enable_table`` by hand: under heavy xdist parallelism the proc
        deadlocks on CDC system-table locks (error 22832/22834 wrapping a 1205
        victim), and the retry below — crucially with random jitter so two
        deadlocking workers don't keep retrying in lockstep — is what keeps the
        CDC tests from flaking. A bare ``execute_sql`` retries without jitter
        and loses the deadlock race against the capture agent.
        """
        extra = ""
        if capture_instance is not None:
            extra += f", @capture_instance=N'{capture_instance}'"
        if captured_column_list is not None:
            extra += f", @captured_column_list=N'{captured_column_list}'"
        for attempt in range(max_retries):
            try:
                self.cursor.execute(
                    f"EXEC sys.sp_cdc_enable_table "
                    f"@source_schema=N'dbo', "
                    f"@source_name=N'{table_name}', "
                    f"@role_name=NULL"
                    f"{extra}"
                )
                # Drain any status rows the proc emitted.
                while self.cursor.nextset():
                    pass
                self._tracked_cdc.add(table_name)
                return
            except pymssql.exceptions.OperationalError as e:
                message = str(e)
                # Two distinct transient failures, both of which SQL Server
                # asks the caller to resubmit:
                #  - 1205: the proc was picked as a deadlock victim on CDC
                #    system-table locks;
                #  - 2714 on 'create table msdb.dbo.cdc_jobs' (wrapped in
                #    22832/22836): the server bootstraps msdb.dbo.cdc_jobs
                #    lazily on its first-ever sp_cdc_add_job, and two workers
                #    enabling CDC concurrently right after server start both
                #    attempt the create — the loser fails, and on resubmit
                #    finds the table in place and succeeds.
                transient = (
                    "1205" in message
                    or "There is already an object named 'cdc_jobs'" in message
                )
                if attempt < max_retries - 1 and transient:
                    delay = retry_delay * (1.25**attempt) + random.uniform(
                        0, retry_delay
                    )
                    logging.warning(
                        f"transient CDC enable failure on {table_name} "
                        f"(attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {delay:.1f}s: {e}"
                    )
                    # Recreate the cursor — pymssql leaves it in a bad state after
                    # a deadlock even with autocommit=True.
                    self.cursor = self.connection.cursor()
                    time.sleep(delay)
                else:
                    logging.error(
                        f"CDC enable failed on {table_name} after "
                        f"{attempt + 1} attempt(s), giving up: {e}"
                    )
                    raise

    def disable_cdc(
        self, table_name: str, max_retries: int = 6, retry_delay: float = 0.5
    ) -> None:
        """Disable every capture instance on ``table_name``.  Idempotent — a
        second call (or a call against a never-enabled table) is a no-op.
        Retries on deadlock victim (1205): under concurrent CDC test load,
        ``sp_cdc_disable_table`` competes with the capture agent for system
        metadata locks and is occasionally chosen as the victim.
        """
        if table_name not in self._tracked_cdc:
            return
        for attempt in range(max_retries):
            cur = self.connection.cursor()
            try:
                cur.execute(
                    "EXEC sys.sp_cdc_disable_table "
                    "@source_schema=%s, @source_name=%s, @capture_instance=%s",
                    ("dbo", table_name, "all"),
                )
                # ``sp_cdc_disable_table`` emits status row(s).  pymssql leaves
                # them pending until the next statement, where they surface as
                # "Statement(s) could not be prepared" — drain them now so the
                # subsequent DROP TABLE actually runs.
                while cur.nextset():
                    pass
                self._tracked_cdc.discard(table_name)
                return
            except pymssql.exceptions.OperationalError as e:
                if attempt < max_retries - 1 and "1205" in str(e):
                    time.sleep(retry_delay * (1.25**attempt))
                    continue
                logging.warning(f"sp_cdc_disable_table failed on {table_name}: {e}")
                break
            except Exception as e:
                logging.warning(f"sp_cdc_disable_table failed on {table_name}: {e}")
                break
        self._tracked_cdc.discard(table_name)

    def drop_table(self, table_name: str, schema_name: str = "dbo") -> None:
        """Disable CDC on ``schema_name.table_name`` (if any) and drop it.

        Retries on deadlock victim (1205) — concurrent CDC-enabled tests fight
        each other on system-table locks (sys.objects etc.), and the system
        lock contention occasionally elects the DROP as the deadlock victim.
        Idempotent: a second call (or a call against a non-existent table) is
        a no-op.
        """
        if schema_name == "dbo":
            self.disable_cdc(table_name)
        self._drop_with_retry(self.connection, table_name, schema_name)
        self._tracked_tables.discard(table_name)

    @contextmanager
    def cdc_table(self, columns_sql: str):
        """Context-manager guard for CDC-enabled test tables.

        Creates a fresh table with the given column definition, enables CDC on
        it, yields the table name, and on exit (success or exception) disables
        CDC and drops the table — so a failing or interrupted test cannot
        leave behind a capture instance for the SQL Agent to keep scanning.
        Equivalent to writing the create / ``enable_cdc`` / ``drop_table``
        sequence by hand, but the cleanup is guaranteed.
        """
        table_name = self.random_table_name()
        self.execute_sql(f"CREATE TABLE {table_name} ({columns_sql})")
        try:
            self.enable_cdc(table_name)
            yield table_name
        finally:
            self.drop_table(table_name)

    def cleanup(self) -> None:
        """Disable every CDC instance and drop every table this context
        registered, then close the connection.  Called by the ``mssql``
        fixture on teardown — runs even if the test errored out.

        Drops use the test's own connection (a fresh per-cleanup connection
        was tried but produced thousands of TDS handshakes per suite run that
        SQL Server occasionally rejected).  Each DROP is paired with a
        verify-and-retry against ``sys.tables``: ``sp_cdc_disable_table``
        sometimes returns before SQL Server has fully released the source
        table's schema lock, and a DROP issued in that window completes
        without error but leaves the table.  Re-polling for tens of ms
        catches that.
        """
        for t in list(self._tracked_cdc):
            self.disable_cdc(t)
        for t in list(self._tracked_tables):
            self._drop_with_retry(self.connection, t)
            self._tracked_tables.discard(t)
        try:
            self.connection.close()
        except Exception:
            pass

    @staticmethod
    def _drop_with_retry(
        connection, table_name: str, schema_name: str = "dbo", attempts: int = 8
    ) -> None:
        """DROP TABLE with deadlock-victim retry.

        SQL Server occasionally aborts the DROP with error 1205 ("deadlock
        victim") when many CDC-enabled test tables are dropped concurrently —
        the system-table locks acquired by the DROP fight with the locks the
        capture agent holds on the same metadata.  pymssql also leaves the
        cursor in a state where the next statement reports "Statement(s) could
        not be prepared", so we recreate the cursor between attempts.
        """
        for i in range(attempts):
            cur = connection.cursor()
            try:
                cur.execute(f"DROP TABLE IF EXISTS [{schema_name}].[{table_name}]")
            except Exception as e:
                logging.warning(
                    f"cleanup: DROP TABLE attempt {i + 1} failed on "
                    f"{schema_name}.{table_name}: {e}"
                )
            try:
                cur = connection.cursor()
                cur.execute(
                    "SELECT 1 FROM sys.tables WHERE name = %s "
                    "AND schema_id = SCHEMA_ID(%s)",
                    (table_name, schema_name),
                )
                if cur.fetchone() is None:
                    return
            except Exception:
                pass
            time.sleep(0.1 * (i + 1))
        logging.error(
            f"cleanup: {schema_name}.{table_name} still present after "
            f"{attempts} drop attempts"
        )

    def drop_schema(self, schema_name: str, attempts: int = 8) -> None:
        """DROP SCHEMA with deadlock-victim retry.  Same rationale as
        ``_drop_with_retry`` for tables — SQL Server elects the DROP as the
        deadlock victim under concurrent test load.
        """
        for i in range(attempts):
            cur = self.connection.cursor()
            try:
                cur.execute(f"DROP SCHEMA IF EXISTS [{schema_name}]")
            except Exception as e:
                logging.warning(
                    f"cleanup: DROP SCHEMA attempt {i + 1} failed on "
                    f"{schema_name}: {e}"
                )
            try:
                cur = self.connection.cursor()
                cur.execute("SELECT 1 FROM sys.schemas WHERE name = %s", (schema_name,))
                if cur.fetchone() is None:
                    return
            except Exception:
                pass
            time.sleep(0.1 * (i + 1))
        logging.error(
            f"cleanup: schema {schema_name} still present after "
            f"{attempts} drop attempts"
        )

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
        self.execute_sql(query, tuple(params))

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
        self.execute_sql(create_sql)
        return table_name

    def get_table_contents(
        self,
        table_name: str,
        column_names: list[str],
        sort_by: Union[str, tuple, None] = None,
    ) -> list[dict[str, Union[str, int, bool, float]]]:
        select_query = f"SELECT {','.join(column_names)} FROM {table_name};"
        self.execute_sql(select_query)
        rows: list[Any] = self.cursor.fetchall()
        result: list[dict[str, Union[str, int, bool, float]]] = []
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
        self.execute_sql(query, (table_name,))
        rows = self.cursor.fetchall()

        schema_props: dict[str, ColumnProperties] = {}
        for column_name, type_name, is_nullable in rows:
            schema_props[str(column_name)] = ColumnProperties(
                str(type_name).lower(), str(is_nullable).upper() == "YES"
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


class RowCountChecker:
    """Polls ``SELECT count(*)`` over the wire protocol until at least
    ``n_expected_entries`` rows are present in ``table_name``.

    Unlike :class:`EntryCountChecker`, it counts server-side instead of
    fetching every row, so polling stays cheap even against a multi-hundred-
    thousand-row table. That makes it the right checker for throughput /
    deadline tests driven by ``wait_result_with_checker``: the pipeline runs in
    a background process and this returns ``True`` as soon as the target count
    is reached, so the test passes promptly on a fast writer and fails at the
    timeout (not after the whole — possibly very slow — run) on a broken one.
    """

    def __init__(
        self,
        n_expected_entries: int,
        db_context: WireProtocolSupporterContext,
        table_name: str,
    ):
        self.n_expected_entries = n_expected_entries
        self.db_context = db_context
        self.table_name = table_name

    def __call__(self) -> bool:
        try:
            self.db_context.cursor.execute(f'SELECT count(*) FROM "{self.table_name}"')
            return self.db_context.cursor.fetchone()[0] >= self.n_expected_entries
        except Exception:
            return False


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


def check_write_quotes_table_name_with_special_characters(
    *,
    write,
    db_context,
    quote_ident,
    init_mode="replace",
):
    """Shared regression for the SQL writers: a ``table_name`` that
    PostgreSQL / MySQL / SQL Server accept only when quoted (here a
    hyphen, which is legal in all three when double-quoted / backticked
    / bracketed) must be interpolated safely into the generated
    ``CREATE TABLE`` + ``INSERT`` statements. Before the shared
    ``quote_ident`` callback on ``SqlQueryTemplate::new`` /
    ``TableWriterInitMode::initialize`` this failed at pipeline start
    with an opaque engine-worker ``db error``.

    ``write`` is a callable ``(table, table_name, init_mode)`` that
    forwards to the connector's ``write`` (bound over connection
    settings by the caller). ``quote_ident`` is the per-DB identifier
    quoter used to construct the ``SELECT`` that reads the row back
    (the test driver does raw string interpolation into SQL).
    """
    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | hello
        """
    )
    # Use a randomized name with the hyphen embedded so two parallel
    # pytest pipelines (or repeat-mode within one) cannot race on the
    # same destination table.
    name = f"my-pw-{uuid.uuid4().hex[:8]}"
    write(table, table_name=name, init_mode=init_mode)
    try:
        rows = db_context.get_table_contents(
            quote_ident(name),
            ["k", "v"],
        )
        assert rows == [{"k": 1, "v": "hello"}]
    finally:
        try:
            db_context.execute_sql(f"DROP TABLE IF EXISTS {quote_ident(name)}")
        except Exception:
            pass


def check_write_quotes_reserved_word_column_name(
    *,
    write,
    db_context,
    quote_ident,
    reserved_word="user",
    table_name=None,
    init_mode="replace",
):
    """Shared regression for the SQL writers: a column whose name is a
    reserved word (``user`` is reserved in PostgreSQL, MySQL, and SQL
    Server) must survive CREATE TABLE and every generated INSERT. The
    bug was that ``field_list``, ``PRIMARY KEY (...)``, ``ON
    CONFLICT``/``MERGE ON``, and ``UPDATE SET``/``DELETE WHERE`` tokens
    were all emitted as raw column names.

    The ``quote_ident`` callback is the per-DB identifier quoter used
    to build the ``SELECT`` that reads the column back; the shared
    writer-side fix lives inside ``SqlQueryTemplate::new`` /
    ``TableWriterInitMode::initialize``.
    """

    class InputSchema(pw.Schema):
        user: str
        v: int

    # ``user`` is a reserved word but is accepted as a Pathway schema
    # field name; rebind the field name if the caller overrides.
    if reserved_word != "user":
        raise NotImplementedError(
            "helper currently fixes the schema field to 'user'; "
            "extend with dynamic rebinding if another reserved word is needed"
        )

    table = pw.debug.table_from_rows(
        InputSchema,
        [("alice", 1), ("bob", 2)],
    )
    if table_name is None:
        # Random per-invocation name so concurrent invocations of this
        # helper (parallel pytest pipelines, repeat mode) do not race
        # on the same destination table.
        table_name = f"reserved_word_col_{uuid.uuid4().hex[:8]}"
    write(table, table_name=table_name, init_mode=init_mode)

    quoted_col = quote_ident(reserved_word)
    try:
        rows = db_context.get_table_contents(
            table_name,
            [quoted_col, "v"],
            sort_by="v",
        )
        assert rows == [
            {quoted_col: "alice", "v": 1},
            {quoted_col: "bob", "v": 2},
        ]
    finally:
        try:
            db_context.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass


PINECONE_VECTOR_DIM = 3


def _pinecone_plaintext_host(host: str) -> str:
    """Rewrite a Pinecone host to a plaintext ``http://`` URL.

    Pinecone Local advertises its per-index hosts with an ``https://`` scheme even
    though it only serves plaintext HTTP, so the scheme has to be forced down to
    ``http://`` before connecting to the data plane.
    """
    for prefix in ("https://", "http://"):
        if host.startswith(prefix):
            host = host[len(prefix) :]
            break
    return "http://" + host


class PineconeContext:
    """Test helper around a `Pinecone Local <https://docs.pinecone.io/guides/operations/local-development>`_ instance.

    The control-plane host defaults to ``http://localhost:5080`` and can be
    pointed at the in-network service name via the ``PINECONE_HOST`` environment
    variable (set to the compose service name when running under docker-compose).
    """

    def __init__(self) -> None:
        from pinecone import Pinecone, ServerlessSpec

        self._ServerlessSpec = ServerlessSpec
        self.pinecone_host = os.environ.get("PINECONE_HOST", "localhost")
        self.control_host = f"http://{self.pinecone_host}:5080"
        self.api_key = "pclocal"
        self.client = Pinecone(api_key=self.api_key, host=self.control_host)
        self._created: list[str] = []
        self._wait_until_control_ready()

    def _wait_until_control_ready(self, timeout: float = 60.0) -> None:
        # The emulator image is distroless, so it can't expose a docker
        # healthcheck; poll the control plane until it answers instead.
        deadline = time.time() + timeout
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                self.client.list_indexes()
                return
            except Exception as e:  # connection refused while still starting up
                last_error = e
                time.sleep(0.5)
        raise RuntimeError(
            f"Pinecone Local control plane at {self.control_host} did not become "
            f"ready within {timeout}s"
        ) from last_error

    def generate_index_name(self) -> str:
        # Pinecone index names must be lowercase alphanumeric or hyphens.
        return f"pw-test-{uuid.uuid4().hex[:12]}"

    def create_index(
        self, index_name: str, *, dimension: int = PINECONE_VECTOR_DIM
    ) -> None:
        self.client.create_index(
            name=index_name,
            dimension=dimension,
            metric="cosine",
            spec=self._ServerlessSpec(cloud="aws", region="us-east-1"),
        )
        self._created.append(index_name)
        self._wait_until_ready(index_name)

    def _wait_until_ready(self, index_name: str, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            desc = self.client.describe_index(index_name)
            if desc.host and getattr(desc.status, "ready", True):
                return
            time.sleep(0.5)

    def _data_index(self, index_name: str):
        raw_host = self.client.describe_index(index_name).host
        assert raw_host, f"Pinecone index {index_name!r} has no data-plane host yet"
        host = _pinecone_plaintext_host(raw_host)
        return self.client.Index(host=host)

    def count(self, index_name: str, namespace: str = "") -> int:
        stats = self._data_index(index_name).describe_index_stats()
        return stats.total_vector_count

    def fetch(self, index_name: str, ids: list, namespace: str = "") -> dict[str, dict]:
        res = self._data_index(index_name).fetch(
            ids=[str(i) for i in ids], namespace=namespace
        )
        return {
            key: {
                "values": [round(float(x), 4) for x in vec.values],
                "metadata": dict(vec.metadata) if vec.metadata else {},
            }
            for key, vec in res.vectors.items()
        }

    def wait_for_count(
        self, index_name: str, expected: int, namespace: str = "", timeout: float = 60.0
    ) -> int:
        """Poll until the index holds ``expected`` vectors (Pinecone is eventually
        consistent, so writes are not immediately visible). Returns the final
        observed count, which the caller asserts on."""
        deadline = time.time() + timeout
        count = self.count(index_name, namespace)
        while count != expected and time.time() < deadline:
            time.sleep(0.5)
            count = self.count(index_name, namespace)
        return count

    def cleanup(self) -> None:
        for index_name in self._created:
            try:
                self.client.delete_index(index_name)
            except Exception:
                pass
        self._created.clear()


def type_to_source_str(t: Any) -> str:
    """Return a Python source expression for *t* suitable for exec'd schema code."""
    if t is type(None):
        return "None"
    if t is Any:
        return "Any"
    if t in (int, float, bool, str, bytes):
        return t.__name__
    if t is pw.Pointer:
        return "pw.Pointer"
    if t is pw.Json:
        return "pw.Json"
    if t is pw.Duration:
        return "pw.Duration"
    if t is pw.DateTimeNaive:
        return "pw.DateTimeNaive"
    if t is pw.DateTimeUtc:
        return "pw.DateTimeUtc"

    origin = get_origin(t)
    args = get_args(t)

    if origin is Union or (
        hasattr(builtin_types, "UnionType") and isinstance(t, builtin_types.UnionType)
    ):
        return " | ".join(type_to_source_str(a) for a in args)

    if origin is list:
        return f"list[{type_to_source_str(args[0])}]"

    if origin is tuple:
        return f"tuple[{', '.join(type_to_source_str(a) for a in args)}]"

    if origin is np.ndarray:
        dims_arg, scalar_arg = args
        scalar_str = type_to_source_str(scalar_arg)
        if dims_arg is None:
            return f"np.ndarray[None, {scalar_str}]"
        return f"np.ndarray[{type_to_source_str(dims_arg)}, {scalar_str}]"

    # pw.PyObjectWrapper[X]
    if origin is pw.PyObjectWrapper and args:
        return f"pw.PyObjectWrapper[{args[0].__name__}]"

    if hasattr(t, "__name__"):
        return t.__name__
    return repr(t)


def generate_pkey_item_schema_code(ItemType: Any, *, primary_key: bool = True) -> str:
    """Return Python source that defines InputSchemaWithPkey for the given type.

    Used by the parsing suites' streaming tests, whose reader pipeline runs in
    a separate interpreter (``subprocess.Popen``) and therefore has to receive
    the schema as source code rather than as a Python object."""
    item_type_str = type_to_source_str(ItemType)
    lines: list[str] = []

    def add_import(line: str) -> None:
        if line not in lines:
            lines.append(line)

    # Walk the whole (possibly nested, e.g. list[PyObjectWrapper[...]]) type
    # and emit the imports its source form needs: numpy for ndarray, and the
    # inner class of a PyObjectWrapper if it comes from utils.
    def collect_imports(t: Any) -> None:
        origin = get_origin(t)
        if origin is np.ndarray:
            add_import("import numpy as np")
            add_import("from typing import Any")
        if origin is pw.PyObjectWrapper:
            args = get_args(t)
            if args:
                inner = args[0]
                module = getattr(inner, "__module__", "builtins")
                if not module.startswith(("builtins", "pathway")):
                    add_import(f"from utils import {inner.__name__}")
        for arg in get_args(t):
            if arg is not None:
                collect_imports(arg)

    collect_imports(ItemType)
    if lines:
        lines.append("")
    pkey_definition = (
        "pkey: int = pw.column_definition(primary_key=True)"
        if primary_key
        else "pkey: int"
    )
    lines += [
        "class InputSchemaWithPkey(pw.Schema):",
        f"    {pkey_definition}",
        f"    item: {item_type_str}",
    ]
    return "\n".join(lines)

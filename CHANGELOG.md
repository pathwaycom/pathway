# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [Unreleased]

### Added
- `pw.io.chroma.write` writes a Pathway table to a [Chroma](https://www.trychroma.com/) collection, keeping the collection in sync with the table as rows are added, changed, and removed. The columns are mapped onto Chroma's record fields explicitly: the optional `primary_key` becomes the record id (when omitted, the row's internal Pathway key is used instead), `embedding` the vector, the optional `document` column the stored text, and `metadata_columns` the record metadata. The collection must already exist. The server is addressed with `host`/`port` (plus `ssl`, `headers`, `tenant`, and `database` for authenticated deployments such as Chroma Cloud).
- `pw.io.qdrant.write` writes a Pathway table to a [Qdrant](https://qdrant.tech/) collection. Each row addition is upserted as a point and each deletion removes the corresponding point, so an update replaces a point rather than duplicating it. The `vector` column supplies the point vector (`list[float]` or a 1-D `numpy.ndarray`) and every other column is stored in the point payload. If the target collection does not exist, it is created on the first write using Cosine distance with the dimension of the written vectors. The `batch_size` parameter bounds how many points are sent per request, and an optional `api_key` authenticates against Qdrant Cloud or a secured instance.
- `pw.io.duckdb.write` writes a Pathway table into a DuckDB database file through a native, in-process connector, in either `"stream_of_changes"` or `"snapshot"` mode. Embeddings stored as `numpy` arrays or `list[float]` columns land in native `DOUBLE[]` list columns, so the result is directly searchable with DuckDB's vector-distance functions for RAG retrieval. The `detach_between_batches` option makes the writer close the database after every minibatch commit and reopen it for the next one, releasing the file lock in between — so a separate process (e.g. a query server) can read committed data with short-lived read-only connections while the pipeline keeps running; the writer retries the reopen with a backoff when a reader momentarily holds the lock.
- `pw.io.weaviate.write` writes a Pathway table to a [Weaviate](https://weaviate.io/) collection, keeping it in sync with the table: additions and updates upsert objects (keyed by a UUID derived from the required `primary_key`), and deletions remove them. An optional `vector` column is stored as the object's embedding; the remaining columns become object properties. The target collection must already exist. The connector writes in parallel across workers (`pathway spawn -n`), with `batch_size` and `concurrency` to tune throughput, and supports `api_key`/`headers` authentication for self-hosted and Weaviate Cloud deployments.
- `pw.io.pinecone.write` writes a Pathway table to a [Pinecone](https://www.pinecone.io/) index for use as a vector store in RAG pipelines. It keeps the index in sync with the current state of the table: a row is upserted under its record id and a row removed from the table is deleted from the index. By default the id is the table's internal row key (unique, and written in parallel across workers); pass a `primary_key` column to use your own ids instead, in which case the values must uniquely identify rows (a collision raises an error) and writing runs on a single worker. The `vector` column holds the dense embedding (`list[float]` or a 1-D `numpy.ndarray`); the remaining columns — or just those listed in `metadata_columns` — are stored as record metadata. Targets both Pinecone cloud and the local Pinecone emulator via the `host` parameter; the index must already exist with a matching dimension.
- `pw.run` and `pw.run_all` accept a new `udf_cache_directory` parameter. When set, the memoization cache of non-deterministic UDFs (any `pw.udf` without `deterministic=True`, whose results are stored so that row deletions replay the originally produced values) is kept in SQLite files in the given directory instead of in memory, so the pipeline's memory usage no longer grows with the number of cached results. Note that on many systems `/tmp` is a RAM-backed `tmpfs` — point the cache directory at a real disk to actually save memory. The on-disk cache is a runtime working set, not a durability mechanism: persistence snapshots remain the source of truth on restart, the cache files are recreated on every run and removed on shutdown. The default (`None`, in-memory cache) is unchanged.
- `pw.io.s3.read`, `pw.io.s3.read_from_digital_ocean`, `pw.io.s3.read_from_wasabi`, `pw.io.minio.read`, `pw.io.pyfilesystem.read`, and `pw.xpacks.connectors.sharepoint.read` now support the `format="only_metadata"` option (already available in `pw.io.fs.read` and `pw.io.gdrive.read`). In this mode the connector tracks additions, modifications, and deletions of objects in the source but does not download their contents — the resulting table contains only the `_metadata` column. This is useful for monitoring changes in large buckets or directories without spending time and traffic on fetching the objects themselves. For S3 and MinIO this also skips the object downloads entirely at the engine level, not just the parsing of their contents.
- Promoted `TwelveLabsVideoParser` and `MarengoEmbedder` out of the Video RAG example template and into the native `pathway.xpacks.llm` core library. You can now build Video RAG applications directly in Pathway by installing `pip install pathway[twelvelabs]`. The parser processes videos concurrently on an async executor and accepts the `capacity`, `retry_strategy`, `async_mode`, `video_format` and `on_error` parameters (`on_error="skip"` lets the pipeline continue when a single video fails to parse); oversized videos are rejected before the upload. `TwelveLabsVideoParser` requires a license key with the `advanced-parser` entitlement.

### Changed
- **BREAKING**: `pw.io.airbyte.read` replaced the `refresh_interval_ms` parameter (milliseconds) with `refresh_interval`, which takes a number of seconds or a `datetime.timedelta` / `pw.Duration` and defaults to 60 seconds (equal to the previous default of 60000 ms). Passing `refresh_interval_ms` now raises an error that includes the converted value to use. Migration: replace `refresh_interval_ms=60000` with `refresh_interval=60` (or `refresh_interval=datetime.timedelta(seconds=60)`).
- Duration parameters of connectors and xpack components now uniformly accept a number of seconds (`int` or `float`) or a `datetime.timedelta` / `pw.Duration`. This covers `refresh_interval` in `pw.io.gdrive.read`, `pw.io.pyfilesystem.read`, `pw.xpacks.connectors.sharepoint.read` and `pw.io.airbyte.read`, `poll_interval` and `max_transaction_duration` in `pw.io.elasticsearch.read`, `quick_access_window`, `compression_frequency` and `retention_period` in `pw.io.deltalake.TableOptimizer`, `idle_duration` in `pw.io.SynchronizedColumn`, `asset_poll_interval` and `asset_timeout` in `TwelveLabsVideoParser`, and `timeout` in `DocumentStoreClient` and `RAGClient`. Plain numbers keep meaning seconds wherever they did before; the new public alias `pw.io.DurationLike` names the accepted type. Invalid durations (negative, non-finite, wrong type) are now rejected at call time with an error naming the parameter. A zero polling interval stays allowed and means polling as often as possible; timeout parameters (`asset_timeout`, client `timeout`) must be strictly positive. This also fixes `pw.io.pyfilesystem.read` rejecting an integer `refresh_interval` (e.g. `refresh_interval=30`).
- `pw.xpacks.connectors.sharepoint.read` now supports the `max_backlog_size` parameter, already available in the other input connectors. It limits the number of entries read from SharePoint and kept in processing at any moment, which helps to avoid memory spikes when the source emits a large initial burst of data.
- `pw.io.mongodb.write` now distributes writes across all workers when Pathway runs with several workers (`pathway spawn -n N`), so write throughput scales with the worker count up to the capacity of the target MongoDB/Atlas deployment. Each document is still written by a single worker, so the result is identical to a single-worker run. Output that requests a global `sort_by` order continues to run on a single worker, since that is required to preserve the order.
- `pw.io.fs.read` now reads in parallel when Pathway runs with several workers and persistence is not enabled. Each file is assigned to exactly one worker by a stable hash of its path, so the read scales with the number of workers instead of running on a single one. The assignment is deterministic across workers and processes, requiring no coordination between them. All files present at start-up are still delivered as a single minibatch at one shared timestamp across every worker, so stateful operators see the initial snapshot atomically rather than split across the parallel readers. When persistence is enabled, the read runs on a single worker to keep recovery exactly consistent.
- `pw.io.postgres.write` now streams each batch into PostgreSQL through the binary `COPY` protocol instead of issuing one `INSERT` per row, giving a large throughput improvement (up to ~100x) on bulk writes. Both output modes use it: stream-of-changes copies straight into the target, while snapshot mode stages each batch in a temporary table and merges it with a single set-based upsert/delete.
- `pw.io.mssql.write` now streams each batch into SQL Server through the native bulk-load protocol (`INSERT BULK`) instead of issuing one `INSERT`/`MERGE` per row, giving a large throughput improvement (roughly ~25x) on bulk writes. Stream-of-changes mode bulk-loads straight into the target table when it is safe to do so (the connector created the table and no column name needs quoting), otherwise it stages each batch in a temporary table and applies it with a single set-based `INSERT ... SELECT`; snapshot mode stages each batch and applies it with one set-based `MERGE`/`DELETE` upsert.
- `pw.io.mysql.write` no longer issues one `INSERT` per row; it now sends each batch in bulk, giving a large throughput improvement. At start-up the connector probes the server and picks the fastest write path it permits: when the server allows `LOAD DATA LOCAL INFILE` (the `local_infile` setting is on), batches stream through it — straight into the target in stream-of-changes mode, or via a temporary staging table merged with a single set-based upsert in snapshot mode; otherwise it falls back to chunked multi-row `INSERT` statements, which work against any reachable server. Both paths produce identical results and require no configuration change.

### Fixed
- `pathway.xpacks.llm` (including `pathway.xpacks.llm.parsers` and `pathway.xpacks.llm.document_store`) imports again in an environment with only `pathway[xpack-llm]` installed. The module used to fail at import time with `ModuleNotFoundError` for `pdf2image` and `unstructured`, which belong to the `xpack-llm-docs` extra; these imports are lazy again, so parsers that need the missing packages fail only at construction, with an error naming the extra to install.
- `pw.io.nats.write` no longer loses messages or fails with a "too many requests" (429) error when writing to a JetStream stream at scale, especially with several workers (`pathway spawn -n N`). The JetStream writer used to fire every publish in a minibatch without waiting for acknowledgements, so a large batch — multiplied across workers publishing to the same stream — overwhelmed the server and most of the batch was dropped. The writer now bounds the number of un-acknowledged publishes in flight (applying backpressure) and retries transient failures with exponential backoff, so the whole batch is delivered.
- A failed run no longer leaves the failing input connector running in the background. When an input connector exhausts its error retries, the error is reported and `pw.run` raises — but the connector kept retrying and logging the same error inside the process indefinitely, which polluted logs and could destabilize subprocesses started afterwards. The connector now stops as soon as it reports the fatal error.
- `pw.io.mqtt.read` and `pw.io.mqtt.write` are now more robust: payloads larger than 10 KB are handled by default, the reader re-subscribes and rides out transient broker disconnects instead of stalling, and the writer no longer blocks the pipeline indefinitely when the broker keeps the connection alive but never confirms deliveries. Publish topics are also validated up front (empty or wildcard topics are rejected with a clear error).
- `pw.io.milvus.write` no longer intermittently fails with a "server unavailable" / "connect failed" error when pointed at a local `.db` file. The embedded local Milvus server reports itself as started before it actually accepts connections, so under load the first connection could lose the race against the server coming up; the connector now retries the initial connection until the local server is ready.
- `BedrockChat` now correctly routes `top_k` and other model-specific arguments to the AWS Converse API via `additionalModelRequestFields`.
- Improved concurrent write handling in pw.io.sqlite.write for SQLite databases. Writes to the same database file now produce deterministic output in multi-worker and multi-table setups.
- `pw.io.elasticsearch.write` no longer fails when a minibatch is big enough that its Elasticsearch `_bulk` request would exceed a server-side limit. The connector reads both the cluster's `http.max_content_length` (the `413 Request Entity Too Large` limit) and `indexing_pressure.memory.limit` (the `429 Too Many Requests` limit, which on a small-heap node trips well below 100 MB) at start-up, and splits the buffered documents across as many bulk requests as needed to stay under whichever is hit first — so large batches are still written in as few requests as possible instead of being rejected. (Both limits fall back to a conservative default if they cannot be read.)
- `pw.io.elasticsearch.write` now retries transient bulk failures with backoff instead of failing the run on the first hiccup. A whole-request rejection or an individual document failing with `429`/`503` (back-pressure / temporary unavailability) is retried — resending only the documents the server reports as not yet applied, so a retry never duplicates data — while deterministic per-document failures (e.g. a type-mismatched value rejected with `400`) are now logged and skipped rather than silently dropped.
- `pw.io.kafka.read` in `mode="static"` no longer intermittently reads zero rows (or fewer rows than were present) from a topic. The static reader subscribed to the topic and relied on a consumer-group rebalance to assign partitions before its first fetch; under load that assignment could fail to complete within the reader's polling budget, so the read finished having consumed nothing. The static reader now assigns its partitions explicitly and reads each up to the end offset captured at start-up, removing the consumer-group dependency entirely.
- Glob pattern filtering in KNN LSH has been optimized; a polynomial algorithm is used instead of exponential.
- `pw.io.airbyte.read` with `execution_type="local"` (venv method) no longer hangs indefinitely when the download of the connector package from PyPI stalls. Each `pip install` attempt now runs with a timeout and is retried, and a clear error is raised once all attempts fail. The connector package is also installed exactly once instead of three times, making the connector start-up faster.
- Pipelines with persistence enabled no longer intermittently lose the most recent input when a `pw.iterate` is present. The internal snapshot of the iterated state was written on a branch of the computation graph that no output depends on, so a checkpoint could be committed before those writes finished; after a restart the affected rows were neither replayed nor found in the snapshot, surfacing as "key missing in output table" errors. Checkpoint commits now wait for these writes. Additionally, tables passed to `pw.iterate` as extra (non-iterated) arguments are now included in the persisted state, so the iteration logic sees them correctly after a restart.
- Under `pw.PersistenceMode.OPERATOR_PERSISTING`, joins that preserve the left side's keys (`id=left.id`), including `join_left`, no longer misreport a row update arriving after a restart as a duplicate key. Previously the update was logged as a duplicate-key error and the affected row was replaced with an error value.
- With persistence enabled, a pipeline that is restarted more than once in quick succession no longer risks losing input rows. A restarted run could commit a checkpoint whose logical time fell into the previous (killed) run's range, accidentally certifying that run's partially-written state: the next restart then resumed from input offsets whose data was missing from the persisted operator state, so those rows were never replayed. Checkpoint commits are now clamped to the current run's own time range.
- With persistence enabled, data read right after a restart (new or modified files) now enters the computation at one shared timestamp across all input sources. Previously each source resumed on its own clock, and cross-source operators with key contracts (`.ix`, join with `id=...`, `with_universe_of`) could observe intermediate states, producing spurious "key missing" or duplicate-key errors on restart. When `autocommit_duration_ms=None` is set explicitly, the previous per-source behavior is kept, since there is no timer to close the shared start-up batch.

## [0.31.1] - 2026-06-12

### Added
- `pw.io.elasticsearch.read` reads an Elasticsearch index into Pathway. Since Elasticsearch has no change-data-capture API, the connector ingests by polling and reconciling the overlap between consecutive queries, so no row is missed or delivered twice. It is configured with `timestamp_column` (a numeric column it watermarks and orders by), `id_column` (a unique, sortable identifier used to deduplicate the overlap and as the Pathway row key), and `max_transaction_duration` (how late a row may still become visible). `mode="streaming"` (default) keeps polling at `poll_interval`; `mode="static"` reads the index once. The index is read in bounded pages of `read_batch_size` documents (default 10 000), each becoming one minibatch, and an idle index is detected and skipped without re-reading the overlap window. With persistence enabled, the connector resumes from the saved watermark, delivering only rows added since the last checkpoint. At startup it warns if `timestamp_column` or `id_column` is mapped in a way it cannot poll on (e.g. an `id_column` mapped as `text`, which Elasticsearch cannot sort by).
- `pw.io.clickhouse.write` writes a Pathway table to a ClickHouse table over the native protocol. Two output formats are available via `output_table_type`: the default `"stream_of_changes"` appends every change with `time`/`diff` columns, while `"snapshot"` maintains the current state in a `ReplacingMergeTree` keyed by the required `primary_key` (query it with `SELECT ... FINAL`). The `init_mode` parameter (`"default"`, `"create_if_not_exists"`, `"replace"`) controls whether the connector creates the destination table, and the destination is validated at start-up so a missing table, column, or incompatible type is reported immediately. Most scalar, `Optional`, `list`, `tuple`, and 1-D `np.ndarray` column types are supported (see the connector documentation for the full type mapping).
- `pw.io.iceberg.read` now decodes every Iceberg primitive type. The new arms are: `date` materializes as `DateTimeNaive` at midnight on the calendar day (Pathway has no date-only type); `time` materializes as `Duration` representing microseconds since midnight (same convention as the Postgres `TIME` mapping); `uuid` materializes as the canonical 8-4-4-4-12 hex string when the column is declared as `str` in the Pathway schema (or as 16 raw `bytes` when declared as `bytes`); `fixed(N)` materializes as `bytes`; `decimal(p, s)` materializes as either `float` (lossy, with a one-shot startup warning naming each affected column) or `str` (lossless decimal text — opt in by declaring the column as `str`).
- `pw.io.iceberg.write` now reconciles Pathway types against the destination table's existing schema and writes the narrower / alternatively-encoded representation when the target column already declares one: Pathway `int` into an existing `int` (32-bit) column with overflow detection, Pathway `float` into an existing `float` (32-bit) column (cast to 32-bit float; precision beyond ~7 significant decimal digits is lost), Pathway `str` into an existing `decimal(p, s)` column (parsed as decimal text) or `uuid` column (parsed as canonical UUID hex), Pathway `bytes` into an existing `fixed(N)` column (length-checked), and Pathway `Duration` into an existing `time` column (microseconds since midnight). When Pathway *creates* the destination table from a Pathway schema, the connector continues to emit the wide representations (`long` from Pathway `int` / `Duration`, `double` from `float`, `string` from `str`, `binary` from `bytes`); choosing a narrow / specialized type at create-time isn't exposed yet. Iceberg `date` is not supported on write at all — neither at create-time (Pathway has no date-only type to derive from) nor as an existing-column override. Iceberg `map<K, V>` remains unsupported on both sides.
- `pw.io.iceberg.read` and `pw.io.iceberg.write` now support Iceberg `struct<…>` columns through Pathway's positional `tuple[…]` type. Tuples are written with synthesized field names `[0], [1], …` (same convention `pw.io.deltalake.write` already uses). Reads ignore struct field *names* and bind tuple positions to struct field positions in the destination order; the mapping composes transitively, so `list[tuple[…]]` works as well. When writing into an existing table whose target column declares a struct with arbitrary field names, the writer adopts the destination's field names automatically, so the user's `tuple[…]` declaration only needs to align with the destination struct's *field order* — Pathway has no named-record type that would let a tuple bind to struct fields by name, so reordering the destination struct's fields out-of-band would silently misalign a Pathway pipeline declaring the column as `tuple[…]`.
- `pw.io.mongodb.read` now accepts four additional BSON types that were previously dropped at parse time. ``ObjectId`` and ``Decimal128`` map to ``str`` (the canonical 24-character hex form and the canonical decimal string respectively); ``RegularExpression`` maps to ``str`` formatted as ``"/<pattern>/<options>"``; ``Timestamp`` maps to ``int`` carrying the seconds-since-epoch ``time`` component (the companion ``increment`` field is dropped). When such a value is written back to MongoDB it is stored as an ordinary string (or integer for ``Timestamp``) field rather than under its original BSON type, so a write-then-read round-trip preserves the value but not the original BSON type of the column.
- `pw.io.postgres.write` now accepts a `schema_name` parameter for writing to tables in non-default PostgreSQL schemas.
- `pw.io.postgres.write` now supports pre-existing `INET`, `CIDR`, `MACADDR`, and `MACADDR8` columns from a `str` Pathway column, matching the reader round-trip.
- `pw.io.postgres.read` and `pw.io.postgres.write` now run extensive preflight validation that surfaces misconfigurations (PostgreSQL types that are not yet supported, array element type mismatches, nullability mismatches, `REPLICA IDENTITY NOTHING` on non-append-only streaming tables, etc.) as clear pipeline-start errors instead of silent row drops or opaque worker panics.
- `pw.io.mysql.read` reads a MySQL table into Pathway. In `mode="streaming"` (the default) it performs Change Data Capture by reading the MySQL binary log: it takes an initial snapshot and then continuously delivers inserts, updates, and deletes (requires `log_bin` on, `binlog_format=ROW`, `binlog_row_image=FULL`, and the `REPLICATION SLAVE` / `REPLICATION CLIENT` privileges). In `mode="static"` it reads the table once and terminates. The schema must declare at least one primary-key column. Every type produced by `pw.io.mysql.write` round-trips back, and common native MySQL types (`DECIMAL`, `DATE`, integer and text families, `JSON`, …) are parsed as well. Unlike PostgreSQL logical replication, the connector leaves no server-side state behind — there is no replication slot to retain logs and fill the disk; binary-log retention is governed solely by the server's own settings. With persistence enabled, the streaming connector saves the binary-log coordinates and resumes from them on restart, raising a clear error if the needed binary logs have already been purged by the server's normal expiry.

### Changed
- `pw.io.iceberg.read` and `pw.io.iceberg.write` now retry transient catalog errors automatically (e.g. concurrent-commit conflicts on write, transient REST/Glue catalog failures on read).
- `pw.io.postgres.write` now retries transient PostgreSQL errors automatically — SQLSTATE class 08 (connection exceptions), class 57 (admin / crash shutdown, `cannot_connect_now`), `serialization_failure` (40001), `deadlock_detected` (40P01), and any closed connection are retried up to three times with exponential backoff before the writer surfaces the error. Permanent failures (syntax errors, missing tables, constraint violations) still propagate on the first attempt.
- `pw.io.postgres.read` (streaming mode) no longer requires `user`, `password`, or `host` in `postgres_settings`. Missing components are omitted from the connection string and resolved by PostgreSQL's standard client defaults (OS user, `~/.pgpass`, UNIX socket), matching how static mode has always behaved. This unblocks deployments authenticated via `trust`, `peer`, `cert`, or other passwordless `pg_hba.conf` modes.
- `pw.io.postgres` connections now tag themselves in PostgreSQL as `application_name=pathway[:<name>]` (where `<name>` comes from the connector's `name` parameter), so operators can identify Pathway sessions in `pg_stat_activity`, `pg_stat_replication`, and server logs. The value is sanitized to printable ASCII and truncated to 63 bytes to match PostgreSQL's `NAMEDATALEN`. A user-supplied `application_name` in `postgres_settings` is left untouched.
- `pw.io.postgres` connections now default to TCP keepalives tuned for roughly five-minute dead-peer detection (`keepalives_idle=300`, `keepalives_interval=30`, `keepalives_count=3`, plus `tcp_user_timeout=300000`), so a SIGKILL'd Pathway process releases its temporary replication slot in minutes rather than the OS-inherited ~2 hour timeline. Each value is only applied when the user has not already set it in `postgres_settings`.
- `pw.io.mssql.read` and `pw.io.mssql.write` now validate configuration and schemas at call/init time, producing clear errors for cases that previously surfaced as opaque SQL Server failures partway through the run: invalid `primary_key` (passed in `stream_of_changes` mode, with duplicates, referring to a different table, or with `Optional` dtype), schema columns colliding with the auto-appended `time`/`diff` columns or differing only in letter case, non-existent source tables or columns, missing or incompatible destination columns (non-existent, IDENTITY, computed, or required `NOT NULL` columns absent from the Pathway schema), `Optional[T]` fields mapped to `NOT NULL` destination columns, and empty or NUL-containing `table_name` / `schema_name`.
- `pw.io.mssql.write` snapshot mode now supports `bytes`-typed primary keys, and verifies that the destination has a unique index covering exactly the configured `primary_key` columns — without it, the upsert could silently match the wrong rows.
- `pw.io.mssql.read` streaming/CDC mode now handles previously silent edge cases: it errors when more than one CDC capture instance is registered on the source table, recovers the persistence offset from CDC when no events have been observed yet (so subsequent runs resume from CDC instead of re-snapshotting), and raises a clear error if CDC cleanup advances retention past the connector's last read position.
- `pw.io.mssql.read` accepts SQL Server `NUMERIC(N, 0)` columns into a Pathway `int` schema and integer-family columns (`TINYINT` / `SMALLINT` / `INT` / `BIGINT`) into a Pathway `float` schema, matching the `int → float` tolerance of `pw.io.sqlite.read`.
- `pw.io.mssql.read` and `pw.io.mssql.write` now correctly handle identifiers containing `]`.
- `pw.io.kafka.read` now emits a `DeprecationWarning` (instead of a `SyntaxWarning`) when `topic` is passed as a list, and warns when an explicitly configured `auto.offset.reset` is overridden because `start_from_timestamp_ms` is set. It also logs a warning (previously an easy-to-miss info message) when `start_from_timestamp_ms` lands at or past the end of a partition, since no already-written data will be read from it.
- `pw.io.mysql.write` now retries only transient MySQL errors — connection drops, deadlocks, lock-wait timeouts, "too many connections", and "server is shutting down" — with exponential backoff, and lets permanent failures (missing tables, syntax errors, constraint violations) propagate on the first attempt. Previously every error was retried up to three times, delaying permanent failures by several seconds before they surfaced.

### Fixed
- `pw.io.mssql.read` in `mode="streaming"` no longer mistakes an unrelated table for a CDC-enabled one. SQL Server leaves a capture instance behind when a CDC-tracked table is dropped without `sp_cdc_disable_table`, and its dangling `source_object_id` can later be reused by a brand-new, non-CDC table; the CDC probe matched on that object id alone, so the reader would report the fresh table as CDC-enabled and then tail a stale change table forever instead of failing fast with a "CDC is not enabled on table" error. The probe now also requires the source table to currently exist and have `is_tracked_by_cdc = 1`.
- `pw.io.iceberg.read` in `mode="static"` no longer hangs on an Iceberg table that has no current snapshot (e.g. a table Pathway just created but never wrote data to). The reader treated the absence of a snapshot as "wait for one to appear" — which never returned in static mode — and now correctly reports zero rows and exits.
- `pw.io.iceberg.write`'s `min_commit_frequency` now actually rate-limits all commits over the lifetime of the run, not just the first one. Previously the last-commit timestamp was set at writer construction and never updated, so once the initial interval elapsed every subsequent minibatch was committed individually — producing one Iceberg snapshot per minibatch rather than at most one per `min_commit_frequency` window.
- `pw.io.iceberg.write` into an Iceberg table that was created outside Pathway (for example, a table pre-created via pyiceberg with a hand-rolled schema) now correctly writes each row's column data into the matching destination column. Previously the writer relied on column-position metadata that only happened to line up when Pathway had both created and written the table — so writes into externally-created tables either failed to load on read-back or silently bound data to the wrong destination columns.
- `pw.io.postgres.read` streaming mode now correctly parses negative time components of `INTERVAL` values in PostgreSQL's default text format.
- `pw.io.kafka.read`, `pw.io.kafka.write` and `pw.io.kafka.simple_read` now validate their arguments when the connector is created and raise a clear `ValueError`/`TypeError`, instead of deferring to an opaque error from the engine or librdkafka at run time. This covers, among others: an empty or missing topic name; a missing or empty `bootstrap.servers`; a missing or empty `group.id` on the reader; non-positive `max_backlog_size`, `parallel_readers` or `autocommit_duration_ms`; a negative `start_from_timestamp_ms`; a schema that declares the reserved `_metadata` column; `json_field_paths` entries that reference an unknown field or use an invalid JSON Pointer; and contradictory combinations such as `read_only_new=True` with `mode="static"`.
- `pw.io.kafka.write` now rejects configurations that would otherwise silently drop data or emit malformed messages: duplicate header names, header names colliding with the reserved `pathway_time`/`pathway_diff` headers, table columns named `time`/`diff` under `format="json"`, an empty `subject`, and a `subject` / `schema_registry_settings` pair where only one side is provided or the format is not `json`.
- `pw.io.kafka.write` now honors its documented `sort_by` parameter; previously the column reference was lost when the connector rebuilt its output table, raising an error at run time.
- `pw.io.kafka.read` with `mode="static"` and a `start_from_timestamp_ms` past the end of the topic now returns immediately, instead of stalling until the static-read polling budget is exhausted.
- `pw.io.kafka.SchemaRegistrySettings` and `pw.io.kafka.SchemaRegistryHeader` now validate their fields on construction (the `urls` list shape and non-emptiness, string-typed credentials, mutually exclusive authentication methods, and the `timeout` type and positivity), instead of failing later with an opaque error.
- `pw.io.mongodb.read` persistence: on restart, the replayed change-stream events are now delivered atomically, preventing an edge case where a crash partway through the replay could skip events that had been read from MongoDB but not yet processed downstream.
- `pw.io.mongodb.read` in `mode="streaming"` no longer loses change events that are committed around connector startup. The reader now anchors its change stream to a cluster operation time captured before the initial collection snapshot and keeps a single cursor for both catch-up and live tailing, instead of capturing oplog tokens with short-lived streams and then re-opening a separate live stream. The previous handoff left a gap in which an event arriving during startup — most easily triggered when several MongoDB readers start concurrently against the same server — could be skipped and never delivered.
- Passing a non-positive `max_batch_size` to any output connector that accepts it now raises a clear error (`max_batch_size must be a positive integer`). Previously the value was handled inconsistently: `0` was silently accepted and disabled size-based batching entirely, while a negative value surfaced an opaque `OverflowError`.
- `pw.io.mysql.write` now rejects, at construction, a schema column named `time` or `diff` (case-insensitive) in `stream_of_changes` mode, where it would collide with the `time`/`diff` metadata columns the connector appends. Previously the conflict surfaced mid-run as an opaque MySQL "Duplicate column name" error. Rename the column or switch to `output_table_type="snapshot"`, which does not append these columns.
- `pw.io.dynamodb.write` now correctly handles which column types may serve as a `partition_key` / `sort_key`. A `bytes` column (serialized as the DynamoDB `Binary` type) and a `pw.Json` column (serialized as `String`) are valid scalar key types but were previously rejected with a "can't be used in the index" error; they are now accepted. Conversely, a `bool` column was previously allowed as a key and the auto-created table declared it as `Binary`, so every write then failed with an opaque `Invalid attribute value type` error from DynamoDB — `bool` is now rejected up front with a clear message, since DynamoDB has no Boolean key type.
- `pw.io.dynamodb.write` no longer fails when a row is updated within a single minibatch. The update emits a retraction of the old row and an insertion of the new one under the same primary key; previously both were sent in the same `BatchWriteItem` request, which DynamoDB rejects with `Provided list of item keys contains duplicates`, so the write failed after exhausting its retries. The connector now reconciles the two operations per key within the batch (the insertion wins), so updates are applied as expected under the documented snapshot semantics.
- `pw.io.questdb.write` now validates its designated-timestamp arguments when the connector is created and raises a clear `ValueError`, instead of deferring to an opaque error from the engine at run time. This covers `designated_timestamp_policy="use_column"` passed without a `designated_timestamp` column, and a `designated_timestamp` column whose type is neither `DateTimeNaive` nor `DateTimeUtc`.

## [0.31.0] - 2026-05-25

### Added
- `pw.io.sqlite.write` connector, which writes a Pathway table into a SQLite database file. Supports two modes: `stream_of_changes` (default) appends each event alongside `time`/`diff` metadata columns, while `snapshot` maintains the current state of the table via `INSERT ... ON CONFLICT DO UPDATE` on insertions and `DELETE` on retractions, keyed on the `primary_key` parameter. Values are encoded using the same storage-class mapping that `pw.io.sqlite.read` accepts, so `write` / `read` round-trips every supported Pathway type losslessly. `init_mode` controls whether the destination table is left as-is, auto-created, or replaced on start-up.
- `pw.io.deltalake.read` now accepts Delta `decimal(p, s)` columns. The Pathway type declared in the schema chooses the projection: `float` converts each value to a double-precision binary float (lossy in general — the binary representation carries only ~15–17 significant decimal digits) and emits a one-time warning at startup naming each affected column; `str` formats the value with the column's scale and passes the resulting decimal text through unchanged, lossless for the full Delta precision range (up to 38 digits).
- `pw.io.deltalake.write` accepts a Pathway `str` column when writing into an existing Delta `decimal(p, s)` column: each row's text is parsed as decimal and stored as the column's fixed-point value. Combined with the lossless `decimal → str` read path, a Delta `decimal` column can round-trip through a Pathway pipeline with no precision loss. A string that can't be parsed as a decimal of the column's shape fails the write with an error message naming the offending value, the column's precision and scale, and the specific constraint it violated. Tables that don't contain a `decimal` column (or that are being created fresh by Pathway) are unaffected.
- `pw.io.deltalake.read` now accepts Delta `date` columns (mapped onto `DateTimeNaive` / `DateTimeUtc` at midnight on the calendar day, since Pathway has no native `Date` type) and `timestamp_millis` columns (mapped onto the same Pathway types with millisecond precision preserved).
- The panel widget for table visualization now accepts `page_size` and `table_height` parameters.

### Changed
- **BREAKING**: `pw.io.iceberg.write` to a Glue catalog no longer accepts `DateTimeUtc` columns. Glue's metastore has no timezone-aware timestamp type, so previous versions silently dropped the timezone on read-back; writes now fail with an explicit error instead of corrupting the zone. To store UTC timestamps in Glue, convert to `DateTimeNaive` with UTC-normalized values, or write through the REST catalog, which preserves the timezone.
- **BREAKING**: `pw.io.mongodb.write` now rejects at construction time tables whose schemas contain a column named ``_id`` (in either output mode) or a column named ``diff`` or ``time`` (in ``stream_of_changes`` mode). Previously such configurations accepted the input and then either silently dropped the user value or failed during ``pw.run()`` with a MongoDB server error. Rename the conflicting column before writing, or — for ``diff``/``time`` — switch to ``output_table_type="snapshot"``.
- `pw.io.mongodb.write` in ``stream_of_changes`` mode now emits every minibatch event. Row updates correctly produce both the ``diff = -1`` retraction of the old row and the ``diff = 1`` insertion of the new row, as the docstring describes.
- `pw.io.sqlite.read` now parses every Pathway column type. In addition to `int`, `float`, `str`, `bytes`, `pw.Json`, and their `Optional` forms, the reader now accepts `bool`, `pw.DateTimeNaive`, `pw.DateTimeUtc`, `pw.Duration`, `pw.Pointer`, `pw.PyObjectWrapper`, homogeneous `tuple` / `list`, and `np.ndarray`. Composite types are stored as `TEXT` using the same JSON encoding that `pw.io.jsonlines.write` emits. Booleans additionally accept PostgreSQL-style textual literals (`true`/`false`, `yes`/`no`, `on`/`off`, `t`/`f`, `y`/`n`; case-insensitive, whitespace-trimmed), and `float` columns tolerate values stored with `INTEGER` storage class.
- `pw.io.mssql.read` and `pw.io.mssql.write` now retry transient SQL Server errors automatically.
- `pw.io.mongodb.read` now surfaces an actionable error when the change stream is terminated by a DDL event (collection dropped or renamed, database dropped, or the server invalidates the cursor). Previously the connector silently skipped these events, which meant a watched collection that was recreated after a drop would produce empty output on subsequent runs.

### Fixed
- `pw.io.http.rest_connector` no longer raises `TypeError: Cannot instantiate typing.Any` when a request column has the inferred default schema type (`Any`). The cast step now skips columns typed as `Any` instead of attempting to call the type as a constructor.
- `pw.io.deltalake.read` now accepts Delta tables whose integer columns use any of the standard Parquet integer widths (`INT_8`, `INT_16`, `INT_32`, unsigned variants), and whose floating-point columns use `FLOAT` (32-bit) or `FLOAT16`. Previously only `INT_64` and `DOUBLE` were accepted, so tables produced by Spark / DuckDB / pandas with explicit narrower casts read back as zero rows with per-row conversion errors.
- `pw.io.deltalake.write` partition columns of type `pw.Pointer`, `pw.Duration`, and `pw.Json` now round-trip correctly through `pw.io.deltalake.read`. Previously the values were correctly placed in the partition path on write, but `pw.io.deltalake.read` did not accept those types on read-back and produced a conversion error for every row.

## [0.30.1] - 2026-04-23

### Added
- `pw.io.rabbitmq.read` and `pw.io.rabbitmq.write` connectors for reading from and writing to RabbitMQ Streams. Supports JSON, plaintext, and raw formats; streaming and static modes; persistence with offset recovery; dynamic topics (writing to different streams per row); `start_from` parameter (`"beginning"`, `"end"`, or `"timestamp"`); TLS configuration; and message metadata including AMQP 1.0 properties and application properties. Header values are JSON-encoded for round-trip compatibility. Requires a Pathway Scale or Enterprise license.
- `pw.io.mssql.read` connector, which reads data from a Microsoft SQL Server table. The connector first delivers a full snapshot of the table and then, if the streaming mode is used, tracks incremental changes via SQL Server Change Data Capture (CDC).
- `pw.io.mssql.write` connector, which writes a Pathway table to a Microsoft SQL Server table. Row additions and updates are applied as MERGE (upsert) statements keyed on the configured primary key columns, and row deletions are applied as DELETE statements.
- `pw.io.milvus.write` connector, which writes a Pathway table to a Milvus collection. Row additions are sent as upserts and row deletions are sent as deletes keyed on the configured primary key column. Requires a Pathway Scale license.
- `pathway spawn` now supports the `--addresses` and `--process-id` flags for multi-machine deployments. Pass a comma-separated list of `host:port` addresses for all processes and the index of the local process; Pathway will connect the cluster over TCP without requiring all processes to run on the same machine.
- `pw.xpacks.llm.parsers.AudioParser`, audio transcription parser based on OpenAI Whisper API. Accepts raw audio bytes and returns transcribed text, following the same interface as other Pathway document parsers.
- `pw.io.leann.write` connector for writing Pathway tables to LEANN vector indices. LEANN uses graph-based selective recomputation to achieve 97% storage reduction compared to traditional vector databases.
- `pw.iterate` now supports operator persistence. On restart, the iterate operator loads its previous input from an operator snapshot and reconverges inside the loop, allowing incremental processing of new data without replaying the full input stream.

## [0.30.0] - 2026-03-24

### Added
- `pw.io.mongodb.read` connector, which reads data from a MongoDB collection. The connector first delivers a full snapshot of the collection and then, if the streaming mode is used, subscribes to the change stream to receive incremental updates in real time.
- `pw.io.postgres.read` connector, which reads data from a PostgreSQL table directly by parsing the Write-Ahead Log (WAL).
- `pw.io.postgres.write` and `pw.io.postgres.read` now support serialization/deserialization of `np.ndarray` (`int`/`float` elements), homogeneous `tuple` and `list` (via Postgres `ARRAY`; multidimensional rectangular arrays supported).
- `pw.io.airbyte.read` now accepts a `dependency_overrides` parameter, allowing users to pin specific versions of transitive dependencies (e.g. `airbyte-cdk`) installed into the connector's virtual environment. This unblocks connectors broken by upstream dependency changes without waiting for upstream fixes.

### Changed
- **BREAKING**: `pw.io.mongodb.write` and `pw.io.mongodb.read` now serialize and deserialize `np.ndarray` columns as nested BSON arrays that preserve the array's shape. Previously, all ndarrays were flattened to a single BSON array regardless of dimensionality, making it impossible to reconstruct the original shape on read-back. For 1-D arrays the representation is identical to before (`[1, 2, 3]`); only multi-dimensional arrays are affected.
- **BREAKING**: The dependencies for `pw.io.pyfilesystem.read` are no longer included in the default package installation. To install them, please use `pip install pathway[pyfilesystem]`.
- Asynchronous callback for `pw.io.python.write` is now available as `pw.io.OnChangeCallbackAsync`.
- `pw.run` and `pw.run_all` now have the `event_loop` parameter to support reusing async state across multiple graph runs.

### Fixed
- `pathway web-dashboard` now waits for the metrics database to be created instead of terminating instantly.

## [0.29.1] - 2026-02-16

### Added
- `pw.io.kafka.read` and `pw.io.kafka.write` connectors now support OAUTHBEARER authentication.
- `pw.io.mongodb.write` connector now supports an `output_table_type` parameter with two modes: `stream_of_changes` (default) and `snapshot`. In `snapshot` mode, the connector maintains the current state of the Pathway table in MongoDB using the `_id` field as the primary key, while `stream_of_changes` preserves the existing behavior by writing all events with `time` and `diff` flags to reflect transactional minibatches and the nature of each change.
- Workers can now automatically scale up or down based on pipeline load, using a configurable monitoring window. This feature requires persistence to be enabled and can be configured via `worker_scaling_enabled` and `workload_tracking_window_ms` in `pw.persistence.Config`. Please refer to the tutorial for more details.
- `pw.io.postgres.write` now properly supports TLS configuration via `sslmode` and `sslrootcert` connection string parameters.

### Changed
- `pw.xpacks.connectors.read` now retries initial connection requests.

## [0.29.0] - 2026-01-22

### Added
- Pathway Web Dashboard providing user-friendly interface for monitoring Pathway pipelines in real time with interactive graph plotting and latency/memory metrics.
- `pw.io.kafka.read` now includes message headers in the parsed metadata. The headers are available at the top level of the metadata in the `headers` array. Each element of the array is a pair consisting of a string header name and a base64-encoded header value. If the header is null, the corresponding value is also null.
- `pw.xpacks.llm.llms.BedrockChat` - Native AWS Bedrock chat integration using the Converse API. Supports Claude, Llama, Titan, Mistral, and other Bedrock models.
- `pw.xpacks.llm.embedders.BedrockEmbedder` - Native AWS Bedrock embedding integration supporting Amazon Titan and Cohere embedding models.

### Changed
- Most Python dependencies are now imported only if the related capabilities are used by a program.
- **BREAKING**: Output connectors no longer wrap string header values in double quotes when sending them to Kafka or NATS. The string values are forwarded as-is. The `None` value is handled differently: in Kafka, it is serialized as a header without a value, while in NATS it becomes the string `"None"`.

## [0.28.0] - 2026-01-08

### Added
- `pw.io.kafka.read` and `pw.io.redpanda.read` now allow each schema field to be specified as coming from either the message key or the message value.
- Connector groups now support the specification of an idle duration. When this is set, if a source does not provide any data for the specified period of time, it will be excluded from the group until it produces data again.
- It is now possible to assign priorities to sources within a connector group. When a priority is set, it ensures that at any moment, the source is not lagging behind any other source with a higher priority in terms of the tracked column.
- Connector groups can now be used in the multiprocess runs.

### Changed
- **BREAKING**: The `__str__` and `dumps` methods in `pw.Json` no longer enforce the result to be an ASCII string. This way, the behavior of `pw.debug.compute_and_print` is now consistent with other output connectors.
- The window functions now internally use deterministic UDFs, where possible.
- The detection logic for the append-only Python connectors has been improved, syntactic analysis has been put in place.

## [0.27.1] - 2025-12-08

### Added
- `pw.Table.filter_out_results_of_forgetting` method, allowing to revert the effects of forgetting at a later stage.

### Changed
- The MCP server `tool` method now allows to pass an optional `description`, default value ​​being kept as the handler's docstring.
- `pw.io.kafka.read` and `pw.io.redpanda.read` now create a `key` column storing the contents of the message keys.

## [0.27.0] - 2025-11-13

### Added
- JetStream extension is now supported in both NATS read and write connectors.
- The Iceberg connectors now support Glue as a catalog backend.
- New `Table.add_update_timestamp_utc` function for tracking update time of rows in the table

### Changed
- **BREAKING** The API for the Iceberg connectors has changed. The `catalog` parameter is now required in both `pw.io.iceberg.read` and `pw.io.iceberg.write`. This parameter can be either of type `pw.io.iceberg.RestCatalog` or `pw.io.iceberg.GlueCatalog`, and it must contain the connection parameters.
- **BREAKING** `paddlepaddle` is no longer a dependency of the Pathway package. The reason is that choosing a specific version for the hardware it will be run on is advantageous from the performance point of view. To install `paddlepaddle` follow instructions on https://www.paddlepaddle.org.cn/en/install/quick.
- `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` now supports document reranking. This enables two-stage retrieval where initial vector similarity search is followed by reranking to improve document relevance ordering.

### Fixed
- Endpoints created by `pw.io.http.rest_connector` now accept requests both with and without a trailing slash. For example, `/endpoint/` and `/endpoint` are now treated equivalently.
- Schemas that inherit from other schemas now automatically preserve all properties from their parent schemas.
- Fixed an issue where the persistence configuration failed when provided with a relative filesystem path.
- Fixed unique name autogeneration for the Python connectors.

## [0.26.4] - 2025-10-16

### Added
- New external integration with [Qdrant](https://qdrant.tech/).
- `pw.io.mysql.write` method for writing to MySQL. It supports two output table types: stream of changes and a realtime-updated data snapshot.

### Changed
- `pw.io.deltalake.read` now accepts the `start_from_timestamp_ms` parameter for non-append-only tables. In this case, the connector will replay the history of changes in the table version by version starting from the state of the table at the given timestamp. The differences between versions will be applied atomically.
- Asynchronous UDFs for connecting to API based llm and embedding models now have by default retry strategy set to `pw.udfs.ExponentialRetryStrategy()`
- `pw.io.postgres.write` method now supports two output table types: stream of changes and realtime-updated data snapshot. The output table type can be chosen with the `output_table_type` parameter.
- `pw.io.postgres.write_snapshot` method has been deprecated.

## [0.26.3] - 2025-10-03

### Added
- New parser `pathway.xpacks.llm.parsers.PaddleOCRParser` supporting parsing of PDF, PPTX and images.

## [0.26.2] - 2025-10-01

### Added
- `pw.io.gdrive.read` now supports the `"only_metadata"` format. When this format is used, the table will contain only metadata updates for the tracked directory, without reading object contents.
- Detailed metrics can now be exported to SQLite. Enable this feature using the environment variable `PATHWAY_DETAILED_METRICS_DIR` or via `pw.set_monitoring_config()`.
- `pw.io.kinesis.read` and `pw.io.kinesis.write` methods for reading from and writing to AWS Kinesis.

### Fixed
- A bug leading to potentially unbounded memory consumption that could occur in `Table.forget` and `Table.sort` operators during multi-worker runs has been fixed.
- Improved memory efficiency during cold starts by compacting intermediary structures and reducing retained memory after backfilling.

### Changed
- The frequency of background operator snapshot compression in data persistence is limited to the greater of the user-defined `snapshot_interval` or 30 minutes when S3 or Azure is used as the backend, in order to avoid frequent calls to potentially expensive operations.
- The Google Drive input connector performance has been improved, especially when handling directories with many nested subdirectories.
- The MCP server `tool` method now allows to pass the optional data `title`, `output_schema`, `annotations` and `meta` to inform the LLM client.
- Relaxed boto3 dependency to <2.0.0.

## [0.26.1] - 2025-08-28

### Added
- `pw.Table.forget` to remove old (in terms of event time) entries from the pipeline.
- `pw.Table.buffer`, a stateful buffering operator that delays entries until `time_column <= max(time_column) - threshold` condition is met.
- `pw.Table.ignore_late` to filter out old (in terms of event time) entries.
- Rows batching for async UDFs. It can be enabled with `max_batch_size` parameter.

### Changed
- `pw.io.subscribe` and `pw.io.python.write` now work with async callbacks.
- The `diff` column in tables automatically created by `pw.io.postgres.write` and `pw.io.postgres.write_snapshot` in `replace` and `create_if_not_exists` initialization modes now uses the `smallint` type.
- `optimize_transaction_log` option has been removed from `pw.io.deltalake.TableOptimizer`.

### Fixed
- `pw.io.postgres.write` and `pw.io.postgres.write_snapshot` now respect the type optionality defined in the Pathway table schema when creating a new PostgreSQL table. This applies to the `replace` and `create_if_not_exists` initialization modes.

## [0.26.0] - 2025-08-14

### Added
- `path_filter` parameter in `pw.io.s3.read` and `pw.io.minio.read` functions. It enables post-filtering of object paths using a wildcard pattern (`*`, `?`), allowing exclusion of paths that pass the main `path` filter but do not match `path_filter`.
- Input connectors now support backpressure control via `max_backlog_size`, allowing to limit the number of read events in processing per connector. This is useful when the data source emits a large initial burst followed by smaller, incremental updates.
- `pw.reducers.count_distinct` and `pw.reducers.count_distinct_approximate` to count the number of distinct elements in a table. The `pw.reducers.count_distinct_approximate` allows you to save memory by decreasing the accuracy. It is possible to control this tradeoff by using the `precision` parameter.
- `pw.Table.join` (and its variants) now has two additional parameters - `left_exactly_once` and `right_exactly_once`. If the elements from a side of a join should be joined exactly once, `*_exactly_once` parameter of the side can be set to `True`. Then after getting a match an entry will be removed from the join state and the memory consumption will be reduced.

### Changed
- Delta table compression logging has been improved: logs now include table names, and verbose messages have been streamlined while preserving details of important processing steps.
- Improved initialization speed of `pw.io.s3.read` and `pw.io.minio.read`.
- `pw.io.s3.read` and `pw.io.minio.read` now limit the number and the total size of objects to be predownloaded.
- **BREAKING** optimized the implementation of `pw.reducers.min`, `pw.reducers.max`, `pw.reducers.argmin`, `pw.reducers.argmax`, `pw.reducers.any` reducers for append-only tables. It is a breaking change for programs using operator persistence. The persisted state will have to be recomputed.
- **BREAKING** optimized the implementation of `pw.reducers.sum` reducer on `float` and `np.ndarray` columns. It is a breaking change for programs using operator persistence. The persisted state will have to be recomputed.
- **BREAKING** the implementation of data persistence has been optimized for the case of many small objects in filesystem and S3 connectors. It is a breaking change for programs using data persistence. The persisted state will have to be recomputed.
- **BREAKING** the data snapshot logic in persistence has been optimized for the case of big input snapshots. It is a breaking change for programs using data persistence. The persisted state will have to be recomputed.
- Improved precision of `pw.reducers.sum` on `float` columns by introducing Neumeier summation.

## [0.25.1] - 2025-07-24

### Added
- `pw.xpacks.llm.mcp_server.PathwayMcp` that allows serving `pw.xpacks.llm.document_store.DocumentStore` and `pw.xpacks.llm.question_answering` endpoints as MCP (Model Context Protocol) tools.
- `pw.io.dynamodb.write` method for writing to Dynamo DB.

## [0.25.0] - 2025-07-10

### Added
- `pw.io.questdb.write` method for writing to Quest DB.
- `pw.io.fs.read` now supports the `"only_metadata"` format. When this format is used, the table will contain only metadata updates for the tracked directory, without reading file contents.
- `pw.Table.to_stream` that transforms a table to a stream of changes from this table.
- `pw.Table.stream_to_table`, `pw.Table.from_streams` that transform a streams of changes to tables.
- `pw.Table.assert_append_only` that sets append_only property of a table and verifies at runtime if the condition is met.

### Changed
- **BREAKING** The Elasticsearch and BigQuery connectors have been moved to the Scale license tier. You can obtain the Scale tier license for free at https://pathway.com/get-license.
- **BREAKING** `pw.io.fs.read` no longer accepts `format="raw"`. Use `format="binary"` to read binary objects, `format="plaintext_by_file"` to read plaintext objects per file, or `format="plaintext"` to read plaintext objects split into lines.
- **BREAKING** The `pw.io.s3_csv.read` connector has been removed. Please use `pw.io.s3.read` with `format="csv"` instead.

### Fixed
- `pw.io.s3.read` and `pw.io.s3.write` now also check the `AWS_PROFILE` environment variable for AWS credentials if none are explicitly provided.

## [0.24.1] - 2025-07-03

### Added
- Confluent Schema Registry support in Kafka and Redpanda input and output connectors.

### Changed
- `pw.io.airbyte.read` will now retry the pip install command if it fails during the installation of a connector. It only applies when using the PyPI version of the connector, not the Docker one.
- Environment variables used in YAML configuration files are no longer being parsed as if they were YAML files by the `pw.load_yaml`. Now, the value of the environment variable is only parsed if it's an integer, a float or a boolean.

## [0.24.0] - 2025-06-26

### Added
- `pw.io.mqtt.read` and `pw.io.mqtt.write` methods for reading from and writing to MQTT.

### Changed
- `pw.xpacks.llm.embedders.SentenceTransformerEmbedder` and `pw.xpacks.llm.llms.HFPipelineChat` are now computed in batches. The maximum size of a single batch can be set in the constructor with the argument `max_batch_size`.
- **BREAKING** Arguments `api_key` and `base_url` for `pw.xpacks.llm.llms.OpenAIChat` can no longer be set in the `__call__` method, and instead, if needed, should be set in the constructor.
- **BREAKING** Argument `api_key` for `pw.xpacks.llm.llms.OpenAIEmbedder` can no longer be set in the `__call__` method, and instead, if needed, should be set in the constructor.
- `pw.io.postgres.write` now accepts arbitrary types for the values of the `postgres_settings` dict. If a value is not a string, Python's `str()` method will be used.

### Removed
- `pw.io.kafka.read_from_upstash` has been removed, as the managed Kafka service in Upstash has been deprecated.

## [0.23.0] - 2025-06-12

### Added
- `pw.io.deltalake.write` now accepts an optional `pw.io.deltalake.TableOptimizer` object that defines the settings for the runtime output table optimization.

### Changed
- **BREAKING**: To use `pw.sql` you now have to install `pathway[sql]`.

### Fixed
- `pw.io.deltalake.read` now correctly reads data from partitioned tables in all cases.
- Added retries for all cloud-based persistence backend operations to improve reliability.

## [0.22.0] - 2025-06-05

### Added
- Data persistence can now be configured to use Azure Blob Storage as a backend. An Azure backend instance can be created using `pw.persistence.Backend.azure` and included in the persistence config.
- Added batching to UDFs. It is now possible to make UDFs operate on batches of data instead of single rows. To do so `max_batch_size` argument has to be set.

### Changed
- **BREAKING**: when creating `pw.DateTimeUtc` it is now obligatory to pass the time zone information.
- **BREAKING**: when creating `pw.DateTimeNaive` passing time zone information is not allowed.
- **BREAKING**: expressions are now evaluated in batches. Generally, it speeds up the computations but might increase the memory usage if the intermediate state in the expressions is large.

### Fixed
- Synchronization groups now correctly handle cases where the source file-like object is updated during the reading process.

## [0.21.6] - 2025-05-29

### Added
- `sort_by` method to `pw.BaseCustomAccumulator` that allows to sort rows within a single batch. When `sort_by` is defined the rows are reduced in the order specified by the `sort_by` method. It can for example be used to process entries in the order of event time.

### Changed
- `pw.Table.debug` now prints a whole row in a single line instead of printing each cell separately.
- Calling functions without arguments in YAML configurations files is now deprecated in `pw.load_yaml`. To call the function a mapping should be passed, e.g. empty mapping as `{}`. In the future `!` syntax without any mapping will be used to pass function objects without calling them.
- The license check error message now provides a more detailed explanation of the failure.
- When code is run using `pathway spawn` with multiple processes, if one process terminates with an error, all other processes will also be terminated.
- `pw.xpacks.llm.vector_store.VectorStoreServer` is being deprecated, and it is now subclass of `pw.xpacks.llm.document_store.DocumentStore`. Public API is being kept the same, however users are encouraged to switch to using `DocumentStore` from now on. 
- `pw.xpacks.llm.vector_store.VectorStoreClient` is being deprecated in favor of `pw.xpacks.llm.document_store.DocumentStoreClient`.
- `pw.io.deltalake.write` can now maintain the target table's snapshot on the output.

## [0.21.5] - 2025-05-09

### Changed
- `pw.io.deltalake.read` now processes Delta table version updates atomically, applying all changes together in a single minibatch.
- The panel widget for table visualization now has a horizontal scroll bar for large tables.
- Added the possibility to return value from any column from `pw.reducers.argmax` and `pw.reducers.argmin`, not only `id`.

### Fixed
- `pw.reducers.argmax` and `pw.reducers.argmin` work correctly with the result of `pw.Table.windowby`.

## [0.21.4] - 2025-04-24

### Added
- `pw.io.kafka.read` and `pw.io.redpanda.read` now support static mode.

### Changed
- The `inactivity_detection` function is now a method for append only tables. It no longer relies on an event timestamp column but now uses table processing times to detect inactivity periods.

## [0.21.3] - 2025-04-16

### Fixed
- The performance of input connectors is optimized in certain cases.
- The panel widget for table visualization does now a better formatting for timestamps and missing values. The pagination was also updated to better fit the widget and the default sorters in snapshot mode have been fixed.

## [0.21.2] - 2025-04-10

### Added
- Added synchronization group mechanism to align multiple data sources based on selected columns. It can be accessed with `pw.io.register_input_synchronization_group`.
- `pw.io.register_input_synchronization_group` now supports the following types of columns: `pw.DateTimeUtc`, `pw.DateTimeNaive`, `pw.DateTimeDuration`, and `int`.

### Changed
- Enhanced error reporting for runtime errors across most operators, providing a trace that simplifies identifying the root cause.

### Fixed
- Bugfix for problem with list_documents() when no documents present in store.
- The append-only property of tables created by `pw.io.kafka.read` is now set correctly.

## [0.21.1] - 2025-03-28

### Changed
- Input connectors now throttle parsing error messages if their share is more than 10% of the parsing attempts.
- New flag `return_status` for `inputs_query` method in `pw.xpacks.llm.DocumentStore`. If set to True, DocumentStore returns the status of indexing for each file.

## [0.21.0] - 2025-03-19

### Added
- All Pathway types can now be serialized to CSV using `pw.io.csv.write` and deserialized back using `pw.io.csv.read`.
- `pw.io.csv.read` now parses null-values in data when it can be done unambiguously.

### Changed
- **BREAKING**: Updated endpoints in `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer`:
  - Deprecated: `/v1/pw_list_documents`, `/v1/pw_ai_answer`
  - New: `/v2/list_documents`, `/v2/answer`
- RAG methods under the `pw.xpacks.llm.question_answering.RAGClient` are re-named, and they now use the new endpoints. Old methods are deprecated and will be removed in the future.
  - `pw_ai_summary` -> `summarize`
  - `pw_ai_answer` -> `answer`
  - `pw_list_documents` -> `list_documents`
- When `pw.io.deltalake.write` creates a table, it also stores its metadata in the columns of the created Delta table. This metadata can be used by Pathway when reading the table with `pw.io.deltalake.read` if no `schema` is specified.
- The `schema` parameter is now optional for `pw.io.deltalake.read`. If the table was created by Pathway and the `schema` was not specified by user, it is read from the table metadata.
- `pw.io.deltalake.write` now aligns the output metadata with the existing table's metadata, preserving any custom metadata in the sink.
- **BREAKING**: The `Bytes` type is now serialized and deserialized with base64 encoding and decoding when the CSV format is used.
- **BREAKING**: The `Duration` type is now serialized and deserialized as a number of nanoseconds when the CSV format is used.
- **BREAKING**: The `tuple` and `np.ndarray` types are now serialized and deserialized as their JSON representations when the CSV format is used.

### Fixed
- `pw.io.csv.write` now correctly escapes quote characters.
- `table_parsing_strategy="llm"` in `DoclingParser` now works correctly

## [0.20.1] - 2025-03-07

### Added
- Added `RecursiveSplitter`
- `pw.io.deltalake.write` now checks that the schema of the target table Delta Table corresponds to the schema of the Pathway table that is sent for the output. If the schemas differ, a human-readable error message is produced.

## [0.20.0] - 2025-02-25

### Added
- Added structure-aware chunking for `DoclingParser`.
- Added `table_parsing_strategy` for `DoclingParser`.
- Column expressions `as_int()`, `as_float()`, `as_str()`, and `as_bool()` now accept additional arguments, `unwrap` and `default`, to simplify null handling.
- Support for python tuples in expressions.

### Changed
- **BREAKING**: Changed the argument in `DoclingParser` from `parse_images` (bool) into `image_parsing_strategy` (Literal["llm"] | None).
- **BREAKING**: `doc_post_processors` argument in the `pw.xpacks.llm.document_store.DocumentStore` now longer accepts `pw.UDF`.
- Better error messages when using `pathway spawn` with multiple workers. Now error messages are printed only from the worker experiencing the error directly.

### Fixed
- `doc_post_processors` argument in the `pw.xpacks.llm.document_store.DocumentStore` had no effect. This is now fixed.

## [0.19.0] - 2025-02-20

### Added
- `LLMReranker` now supports custom prompts as well as custom response parsers allowing for other ranking scales apart from default 1-5.
- `pw.io.kafka.write` and `pw.io.nats.write` now support `ColumnReference` as a topic name. When a `ColumnReference` is provided, each message's topic is determined by the corresponding column value.
- `pw.io.python.write` accepting `ConnectorObserver` as an alternative to `pw.io.subscribe`.
- `pw.io.iceberg.read` and `pw.io.iceberg.write` now support S3 as data backend and AWS Glue catalog implementations.
- All output connectors now support the `sort_by` field for ordering output within a single minibatch.
- A new UDF executor `pw.udfs.fully_async_executor`. It allows for creation of non-blocking asynchronous UDFs which results can be returned in the future processing time.
- A Future data type to represent results of fully asynchronous UDFs.
- `pw.Table.await_futures` method to wait for results of fully asynchronous UDFs.
- `pw.io.deltalake.write` now supports partition columns specification.

### Changed
- **BREAKING**: Changed the interface of `LLMReranker`, the `use_logit_bias`, `cache_strategy`, `retry_strategy` and `kwargs` arguments are no longer supported.
- **BREAKING**: LLMReranker no longer inherits from pw.UDF
- **BREAKING**: `pw.stdlib.utils.AsyncTransformer.output_table` now returns a table with columns with Future data type.
- `pw.io.deltalake.read` can now read append-only tables without requiring explicit specification of primary key fields.


## [0.18.0] - 2025-02-07

### Added
- `pw.io.postgres.write` and `pw.io.postgres.write_snapshot` now handle serialization of `PyObjectWrapper` and `Timedelta` properly.
- New chunking options in `pathway.xpacks.llm.parsers.UnstructuredParser`
- Now all Pathway types can be serialized into JSON and consistently deserialized back.
- `table.col.dt.to_duration` converting an integer into a `pw.Duration`.
- `pw.Json` now supports storing datetime and duration type values in ISO format.

### Changed
- **BREAKING**: Changed the interface of `UnstructuredParser`
- **BREAKING**: The `Pointer` type is now serialized and deserialized as a string field in Iceberg and Delta Lake.
- **BREAKING**: The `Bytes` type is now serialized and deserialized with base64 encoding and decoding when the JSON format is used. A string field is used to store the encoded contents.
- **BREAKING**: The `Array` type is now serialized and deserialized as an object with two fields: `shape` denoting the shape of the stored multi-dimensional array and `elements` denoting the elements of the flattened array.
- **BREAKING**: Marked package as **py.typed** to indicate support for type hints.

### Removed
- **BREAKING**: Removed undocumented `license_key` argument from `pw.run` and `pw.run_all` methods. Instead, `pw.set_license_key` should be used.


## [0.17.0] - 2025-01-30

### Added
- `pw.io.iceberg.read` method for reading Apache Iceberg tables into Pathway.
- methods `pw.io.postgres.write` and `pw.io.postgres.write_snapshot` now accept an additional argument `init_mode`, which allows initializing the table before writing.
- `pw.io.deltalake.read` now supports serialization and deserialization for all Pathway data types.
- New parser `pathway.xpacks.llm.parsers.DoclingParser` supporting parsing of pdfs with tables and images.
- Output connectors now include an optional `name` parameter. If provided, this name will appear in logs and monitoring dashboards.
- Automatic naming for input and output connectors has been enhanced.

### Changed
- **BREAKING**: `pw.io.deltalake.read` now requires explicit specification of primary key fields.
- **BREAKING**: `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` now returns a dictionary from `pw_ai_answer` endpoint.
- `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` allows optionally returning context documents from `pw_ai_answer` endpoint.
- **BREAKING**: When using delay in temporal behavior, current time is updated immediately, not in the next batch.
- **BREAKING**: The `Pointer` type is now serialized to Delta Tables as raw bytes.
- `pw.io.kafka.write` now allows to specify `key` and `headers` for JSON and CSV data formats.
- `persistent_id` parameter in connectors has been renamed to `name`. This new `name` parameter allows you to assign names to connectors, which will appear in logs and monitoring dashboards.
- Changed names of parsers to be more consistent: `ParseUnstrutured` -> `UnstructuredParser`, `ParseUtf8` -> `Utf8Parser`. `ParseUnstrutured` and `ParseUtf8` are now deprecated.

### Fixed
- `generate_class` method in `Schema` now correctly renders columns of `UnionType` and `None` types.
- a bug in delay in temporal behavior. It was possible to emit a single entry twice in a specific situation.
- `pw.io.postgres.write_snapshot` now correctly handles tables that only have primary key columns.

### Removed
- **BREAKING**: `pw.indexing.build_sorted_index`, `pw.indexing.retrieve_prev_next_values`, `pw.indexing.sort_from_index` and `pw.indexing.SortedIndex` are removed. Sorting is now done with `pw.Table.sort`.
- **BREAKING**: Removed deprecated methods `pw.Table.unsafe_promise_same_universe_as`, `pw.Table.unsafe_promise_universes_are_pairwise_disjoint`, `pw.Table.unsafe_promise_universe_is_subset_of`, `pw.Table.left_join`, `pw.Table.right_join`, `pw.Table.outer_join`, `pw.stdlib.utils.AsyncTransformer.result`.
- **BREAKING**: Removed deprecated column `_pw_shard` in the result of `windowby`.
- **BREAKING**: Removed deprecated functions `pw.debug.parse_to_table`, `pw.udf_async`, `pw.reducers.npsum`, `pw.reducers.int_sum`, `pw.stdlib.utils.col.flatten_column`.
- **BREAKING**: Removed deprecated module `pw.asynchronous`.
- **BREAKING**: Removed deprecated access to functions from `pw.io` in `pw`.
- **BREAKING**: Removed deprecated classes `pw.UDFSync`, `pw.UDFAsync`.
- **BREAKING**: Removed class `pw.xpack.llm.parsers.OpenParse`. It's functionality has been replaced with `pw.xpack.llm.parsers.DoclingParser`.
- **BREAKING**: Removed deprecated arguments from input connectors: `value_columns`, `primary_key`, `types`, `default_values`. Schema should be used instead.

## [0.16.4] - 2025-01-09

### Fixed
- Google Drive connector in static mode now correctly displays in jupyter visualizations.

## [0.16.3] - 2025-01-02

### Added
- `pw.io.iceberg.write` method for writing Pathway tables into Apache Iceberg.

### Changed
- values of non-deterministic UDFs are not stored in tables that are `append_only`.
- `pw.Table.ix` has better runtime error message that includes id of the missing row.

### Fixed
- temporal behaviors in temporal operators (`windowby`, `interval_join`) now consume no CPU when no data passes through them.

## [0.16.2] - 2024-12-19

### Added
- `pw.xpacks.llm.prompts.RAGPromptTemplate`, set of prompt utilities that enable verifying templates and creating UDFs from prompt strings or callables.
- `pw.xpacks.llm.question_answering.BaseContextProcessor` streamlines development and tuning of representing retrieved context documents to the LLM.
- `pw.io.kafka.read` now supports `with_metadata` flag, which makes it possible to attach the metadata of the Kafka messages to the table entries.
- `pw.io.deltalake.read` can now stream the tables with deletions, if no deletion vectors were used.

### Changed
- `pw.io.sharepoint.read` now explicitly terminates with an error if it fails to read the data the specified number of times per row (the default is `8`).
- `pw.xpacks.llm.prompts.prompt_qa`, and other prompts expect 'context' and 'query' fields instead of 'docs'. 
- Removed support for `short_prompt_template` and `long_prompt_template` in `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer`. These prompt variants are no longer accepted during construction or in requests.
- `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` allows setting user created prompts. Templates are verified to include 'context' and 'query' placeholders.
- `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` can take a `BaseContextProcessor` that represents context documents to the LLM. Defaults to `pw.xpacks.llm.question_answering.SimpleContextProcessor` which filters metadata fields and joins the documents with new lines.

### Fixed
- The input of `pw.io.fs.read` and `pw.io.s3.read` is now correctly persisted in case deletions or modifications of already processed objects take place.

## [0.16.1] - 2024-12-12

### Changed
- `pw.io.s3.read` now monitors object deletions and modifications in the S3 source, when ran in streaming mode. When an object is deleted in S3, it is also removed from the engine. Similarly, if an object is modified in S3, the engine updates its state to reflect those changes.
- `pw.io.s3.read` now supports `with_metadata` flag, which makes it possible to attach the metadata of the source object to the table entries.

### Fixed
- `pw.xpacks.llm.document_store.DocumentStore` no longer requires `_metadata` column in the input table.

## [0.16.0] - 2024-11-29

### Added
- `pw.xpacks.llm.document_store.SlidesDocumentStore`, which is a subclass of `pw.xpacks.llm.document_store.DocumentStore` customized for retrieving slides from presentations.
- `pw.temporal.inactivity_detection` and `pw.temporal.utc_now` functions allowing for alerting and other time dependent usecases

### Changed
- `pw.Table.concat`, `pw.Table.with_id`, `pw.Table.with_id_from` no longer perform checks if ids are unique. It improves memory usage.
- table operations that store values (like `pw.Table.join`, `pw.Table.update_cells`) no longer store columns that are not used downstream.
- `append_only` column property is now propagated better (there are more places where we can infer it).
- **BREAKING**: Parsers and parser utilities including `OpenParse`, `ParseUnstructured`, `ParseUtf8`, `parse_images` are now async. Parser interface in the `VectorStore` and `DocumentStore` remains unchanged.
- **BREAKING**: Unused arguments from the constructor `pw.xpacks.llm.question_answering.DeckRetriever` are no longer accepted. 

### Fixed
- `query_as_of_now` of `pw.stdlib.indexing.DataIndex` and `pw.stdlib.indexing.HybridIndex` now work in constant memory for infinite query stream (no query-related data is kept after query is answered).

## [0.15.4] - 2024-11-18

### Added
- `pw.io.kafka.read` now supports reading entries starting from a specified timestamp.
- `pw.io.nats.read` and `pw.io.nats.write` methods for reading from and writing Pathway tables to NATS.

### Changed
- `pw.Table.diff` now supports setting `instance` parameter that allows computing differences for multiple groups.
- `pw.io.postgres.write_snapshot` now keeps the Postgres table fully in sync with the current state of the table in Pathway. This means that if an entry is deleted in Pathway, the same entry will also be deleted from the Postgres table managed by the output connector.

### Fixed
- `pw.PyObjectWrapper` is now picklable.
- `query_as_of_now` of `pw.stdlib.indexing.DataIndex` and `pw.stdlib.indexing.HybridIndex` now work in constant memory for infinite query stream (no query-related data is kept after query is answered).

## [0.15.3] - 2024-11-07

### Added
- `pw.io.mongodb.write` connector for writing Pathway tables in MongoDB.
- `pw.io.s3.read` now supports downloading objects from an S3 bucket in parallel.

### Changed
- `pw.io.fs.read` performance has been improved for directories containing a large number of files.

## [0.15.2] - 2024-10-24

### Added
- `pw.io.deltalake.read` now supports custom S3 Delta Lakes with HTTP endpoints.
- `pw.io.deltalake.read` now supports specifying both a custom endpoint and a custom region for Delta Lakes via `pw.io.s3.AwsS3Settings`.

### Changed
- Indices in `pathway.stdlib.indexing.nearest_neighbors` can now work also on numpy arrays. Previously they only accepted `list[float]`. Working with numpy arrays improves memory efficiency.
- `pw.io.s3.read` has been optimized to minimize new object requests whenever possible.
- It is now possible to set the size limit of cache in `pw.udfs.DiskCache`.
- State persistence now uses a single backend for both metadata and stream storage. The `pw.persistence.Config.simple_config` method is therefore deprecated. Now you can use the `pw.persistence.Config` constructor with the same parameters that were previously used in `simple_config`.

### Fixed
- `pw.io.bigquery.write` connector now correctly handles `pw.Json` columns.

## [0.15.1] - 2024-10-04

### Fixed
- `pw.temporal.session` and `pw.temporal.asof_join` now correctly works with multiple entries with the same time.
- Fixed an issue in `pw.stdlib.indexing` where filters would cause runtime errors while using `HybridIndexFactory`.


## [0.15.0] - 2024-09-12

### Added
- **Experimental** A ``pw.xpacks.llm.document_store.DocumentStore`` to process and index documents.
- ``pw.xpacks.llm.servers.DocumentStoreServer`` used to expose REST server for retrieving documents from ``pw.xpacks.llm.document_store.DocumentStore``.
- `pw.xpacks.stdlib.indexing.HybridIndex` used for querying multiple indices and combining their results.
- `pw.io.airbyte.read` now also supports streams that only operate in `full_refresh` mode.

### Changed
- Running servers for answering queries is extracted from `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` into `pw.xpacks.llm.servers.QARestServer` and `pw.xpacks.llm.servers.QASummaryRestServer`.
- **BREAKING**: `query` and `query_as_of_now` of `pathway.stdlib.indexing.data_index.DataIndex` now produce an empty list instead of `None` if no match is found.

## [0.14.3] - 2024-08-22

### Fixed
- `pw.io.deltalake.read` and `pw.io.deltalake.write` now correctly work with lakes hosted in S3 over min.io, Wasabi and Digital Ocean.

### Added
- The Pathway CLI command `spawn` can now execute code directly from a specified GitHub repository.
- A new CLI command, `spawn-from-env`, has been added. This command runs the Pathway CLI `spawn` command using arguments provided in the `PATHWAY_SPAWN_ARGS` environment variable.

## [0.14.2] - 2024-08-06

### Fixed
- Switched `pw.xpacks.llm.embedders.GeminiEmbedder` to be sync to resolve compatibility issues with the Google Colab runs.
- Pinned `surya-ocr` module version for stability.

## [0.14.1] - 2024-08-05

### Added
- `pw.xpacks.llm.embedders.GeminiEmbedder` which is a wrapper for Google Gemini Embedding services.

## [0.14.0] - 2024-07-25

### Fixed
- `pw.debug.table_to_pandas` now exports `int | None` columns correctly.

### Changed
- `pw.io.airbyte.read` can now be used with Airbyte connectors implemented in Python without requiring Docker.
- **BREAKING**: UDFs now verify the type of returned values at runtime. If it is possible to cast a returned value to a proper type, the values is cast. If the value does not match the expected type and can't be cast, an error is raised.
- **BREAKING**: `pw.reducers.ndarray` reducer requires input column to either have type `float`, `int` or `Array`.
- `pw.xpacks.llm.parsers.OpenParse` can now extract and parse images & diagrams from PDFs. This can be enabled by setting the `parse_images`. `processing_pipeline` can be also set to customize the post processing of doc elements.

## [0.13.2] - 2024-07-08

### Added
- `pw.io.deltalake.read` now supports S3 data sources.
- `pw.xpacks.llm.parsers.ImageParser` which allows parsing images with the vision LMs.
- `pw.xpacks.llm.parsers.SlideParser` that enables parsing PDF and PPTX slides with the vision LMs.
- `pw.xpacks.llm.parsers.question_answering.RAGClient`, Python client for Pathway hosted RAG apps.
- `pw.xpacks.llm.parsers.question_answeringDeckRetriever`, a RAG app that enables searching through slide decks with visual-heavy elements.

### Fixed
- `pw.xpacks.llm.vector_store.VectorStoreServer` now uses new indexes.

### Changed
- `pw.xpacks.llm.parsers.OpenParse` now supports any vision Language model including local and proprietary models via LiteLLM.


## [0.13.1] - 2024-06-27

### Added
- `pw.io.kafka.read` now accepts an autogenerate_key flag. This flag determines the primary key generation policy to apply when reading raw data from the source. You can either use the key from the Kafka message or have Pathway autogenerate one.
- `pw.io.deltalake.read` input connector that fetches changes from DeltaLake into a Pathway table.
- `pw.xpacks.llm.parsers.OpenParse` which allows parsing tables and images in PDFs.

### Fixed
- All S3 input connectors (including S3, Min.io, Digital Ocean, and Wasabi) now automatically retry network operations if a failure occurs.
- The issue where the connection to the S3 source fails after partially ingesting an object has been resolved by downloading the object in full first.

## [0.13.0] - 2024-06-13

### Added
- `pw.io.deltalake.write` now supports S3 destinations.

### Changed
- `pw.debug.compute_and_print` now allows passing more than one table.
- **BREAKING**: `path` parameter in `pw.io.deltalake.write` renamed to `uri`.

### Fixed
- A bug in `pw.Table.deduplicate`. If `persistent_id` is not set, it is no longer generated in `pw.PersistenceMode.SELECTIVE_PERSISTING` mode.

## [0.12.0] - 2024-06-08

### Added
- `pw.PyObjectWrapper` that enables passing python objects of any type to the engine.
- `cache_strategy` option added for `pw.io.http.rest_connector`. It enables cache configuration, which is useful for duplicated requests.
- `allow_misses` argument to `Table.ix` and `Table.ix_ref` methods which allows for filling rows with missing keys with None values.
- `pw.io.deltalake.write` output connector that streams the changes of a given table into a DeltaLake storage.
- `pw.io.airbyte.read` now supports data extraction with Google Cloud Runs.

### Removed
- **BREAKING**: Removed `Table.having` method.
- **BREAKING**: Removed `pw.DATE_TIME_UTC`, `pw.DATE_TIME_NAIVE` and `pw.DURATION` as dtype markers. Instead, `pw.DateTimeUtc`, `pw.DateTimeNaive` and `pw.Duration` should be used, which are wrappers for corresponding pandas types.
- **BREAKING**: Removed class transformers from public API: `pw.ClassArg`, `pw.attribute`, `pw.input_attribute`, `pw.input_method`, `pw.method`, `pw.output_attribute` and `pw.transformer`.
- **BREAKING**: Removed several methods from `pw.indexing` module: `binsearch_oracle`, `filter_cmp_helper`, `filter_smallest_k` and `prefix_sum_oracle`.

## [0.11.2] - 2024-05-27

### Added
- `pathway.assert_table_has_schema` and `pathway.table_transformer` now accept `allow_subtype` argument, which, if True, allows column types in the Table be subtypes of types in the Schema.
- `next` method to `pw.io.python.ConnectorSubject` (python connector) that enables passing values of any type to the engine, not only values that are json-serializable. The `next` method should be the preferred way of passing values from the python connector.

### Changed
- The `format` argument of `pw.io.python.read` is deprecated. A data format is inferred from the method used (`next_json`, `next_str`, `next_bytes`) and the provided schema.

### Removed
- Removed `pw.numba_apply` and `numba` dependency.

### Fixed
- Fixed `pw.this` desugaring bug, where `__getitem__` in `.ix` context was not working properly.
- `pw.io.sqlite.read` now checks if the data matches the passed schema.

## [0.11.1] - 2024-05-16

### Added
- `query` and `query_as_of_now` of `pathway.stdlib.indexing.data_index.DataIndex` now accept in `metadata_column` parameter a column with data of type `str | None`.
- `pathway.xpacks.connectors.sharepoint` module, available with Pathway Scale License.


## [0.11.0] - 2024-05-10

### Added
- Embedders in the LLM xpack now have method `get_embedding_dimension` that returns number of dimension used by the chosen embedder.
- `pathway.stdlib.indexing.nearest_neighbors`, with implementations of `pathway.stdlib.indexing.data_index.InnerIndex` based on k-NN via LSH (implemented in Pathway), and k-NN provided by USearch library.
- `pathway.stdlib.indexing.vector_document_index`, with a few predefined instances of `pathway.stdlib.indexing.data_index.DataIndex`.
- `pathway.stdlib.indexing.bm25`, with implementations of `pathway.stdlib.indexing.data_index.InnerIndex` based on BM25 index provided by Tantivy.
- `pathway.stdlib.indexing.full_text_document_index`, with a predefined instance of `pathway.stdlib.indexing.data_index.DataIndex`.
- Introduced the `reranker` module under `llm.xpacks`. Includes few re-ranking strategies and utility functions for RAG applications.

### Changed
- **BREAKING**: `windowby` generates IDs of produced rows differently than in the previous version.
- **BREAKING**: `pw.io.csv.write` prints printable non-ascii characters as regular text, not `\u{xxxx}`.
- **BREAKING**: Connector methods `pw.io.elasticsearch.read`, `pw.io.debezium.read`, `pw.io.fs.read`, `pw.io.jsonlines.read`, `pw.io.kafka.read`, `pw.io.python.read`, `pw.io.redpanda.read`, `pw.io.s3.read` now check the type of the input data. Previously it was not checked if the provided format was `"json"`/`"jsonlines"`. If the data is inconsistent with the provided schema, the row is skipped and the error message is emitted.
- **BREAKING**: `query` and `query_as_of_now` methods of `pathway.stdlib.indexing.data_index.DataIndex` now return `pathway.JoinResult`, to allow resolving column name conflicts (between columns in the table with queries and table with index data).
- **BREAKING**: DataIndex methods `query` and `query_as_of_now` now return score in a column named `_pw_index_reply_score` (defined as `_SCORE` variable in `pathway.stdlib.indexing.colnames.py`).

### Removed
- **BREAKING**: `pathway.stdlib.indexing.data_index.VectorDocumentIndex` class, some predefined instances are now meant to be obtained via methods provided in `pathway.stdlib.indexing.vector_document_index`.
- **BREAKING**: `with_distances` parameter of `query` and `query_as_of_now` methods in `pathway.stdlib.indexing.data_index.DataIndex`. Instead of 'distance', we now operate with a more general term 'score' (higher = better). For distance based indices score is usually defined as negative distance. Score is now always included in the answer, as long as underlying index returns something that indicates quality of a match.

## [0.10.1] - 2024-04-30

### Added
- `query` method to VectorStoreServer to enable compatible API with `DataIndex`.
- `AdaptiveRAGQuestionAnswerer` to xpacks.question_answering. End-to-end pipeline and accompanying code for `Private RAG` showcase.

## [0.10.0] - 2024-04-24

### Added
- Pathway now warns when unintentionally creating Table with empty universe.
- `pw.io.kafka.write` in `raw` and `plaintext` formats now supports output for tables with multiple columns. For such tables, it requires the specification of the column that must be used as a value of the produced Kafka messages and gives a possibility to provide column which must be used as a key.
- `pw.io.kafka.write` can now output values from the table using Kafka message headers in 'raw' and 'plaintext' output format.

### Changed
- `instance` arguments to `groupby`, `join`, `with_id_from` now determine how entries are distributed between machines.
- `flatten` results remain on the same machine as their source entries.
- `join` sends each record between machines at most once.
- **BREAKING**: `flatten`, `join`, `groupby` (if used with `instance`), `with_id_from` (if used with `instance`) generate IDs of the produced rows differently than in the previous versions.
- `pathway spawn` with multiple workers prints only output from the first worker.

## [0.9.0] - 2024-04-18

### Added
- `pw.reducers.latest` and `pw.reducers.earliest` that return the value with respectively maximal and minimal processing time assigned.
- `pw.io.kafka.write` can now produce messages containing raw bytes in case the table consists of a single binary column and `raw` mode is specified. Similarly, this method will provide plaintext messages if `plaintext` mode is chosen and the table consists of a single string-typed column.
- `pw.io.pubsub.write` connector for publishing Pathway tables into Google PubSub.
- Argument `strict_prompt` to `answer_with_geometric_rag_strategy` and `answer_with_geometric_rag_strategy_from_index` that allows optimizing prompts for smaller open-source LLM models.
- Temporarily switch LiteLLMChat's generation method to sync version due to a bug while using `json` mode with Ollama.

### Changed
- **BREAKING**: `pw.io.kafka.read` will not parse the messages from UTF-8 in case `raw` mode was specified. To preserve this behavior you can use the `plaintext` mode.
- **BREAKING**: `Table.flatten` now flattens one column and spreads every other column of the table, instead of taking other columns from the argument list.

## [0.8.6] - 2024-04-10

### Added
- `pw.io.bigquery.write` connector for writing Pathway tables into Google BigQuery.
- parameter `filepath_globpattern` to `query` method in `VectorStoreClient` for specifying which files should be considered in the query.
- Improved compatibility of `pw.Json` with standard methods such as `len()`, `int()`, `float()`, `bool()`, `iter()`, `reversed()` when feasible.

### Changed
- `pw.io.postgres.write` can now parallelize writes to several threads if several workers are configured.
- Pathway now checks types of pointers rigorously. Indexing table with mismatched number/types of columns vs what was used to create index will now result in a TypeError.
- `pw.Json.as_float()` method now supports integer JSON values.

## [0.8.5] - 2024-03-27

### Added
- New function `answer_with_geometric_rag_strategy_from_index`, which allows to use `answer_with_geometric_rag_strategy` without the need to first retrieve documents from index.
- Added support for custom state serialization to `udf_reducer`.
- Introduced `instance` parameter in `AsyncTransformer`. All calls with a given `(instance, processing_time)` pair are returned at the same processing time. Ordering is preserved within a single instance.
- Added `successful`, `failed`, `finished` properties to `AsyncTransformer`. They return tables with successful calls, failed calls and all finished calls, respectively.

### Changed
- Property `result` of `AsyncTransformer` is deprecated. Property `successful` should be used instead.
- `pw.io.csv.read`, `pw.io.jsonlines.read`, `pw.io.fs.read`, `pw.io.plaintext.read` now handle `path` as a glob pattern and read all matched files and directories recursively.

## [0.8.4] - 2024-03-18

### Fixed
- Pathway will only require `LiteLLM` package, if you use one of the wrappers for `LiteLLM`.
- Retries are implemented in `pw.io.airbyte.read`.
- State processing protocol is updated in `pw.io.airbyte.read`.

## [0.8.3] - 2024-03-13

### Added
- New parameters of `pw.UDF` class and `pw.udf` decorator: `return_type`, `deterministic`, `propagate_none`, `executor`, `cache_strategy`.
- The LLM Xpack now provides integrations with LlamaIndex and LangChain for running the Pathway VectorStore server.

### Changed
- Subclassing `UDFSync` and `UDFAsync` is deprecated. `UDF` should be subclassed to create a new UDF.
- Passing keyword arguments to `pw.apply`, `pw.apply_with_type`, `pw.apply_async` is deprecated. In the future, they'll be used for configuration, not passing data to the function.

### Fixed
- Fixed a minor bug with `Table.groupby()` method which sometimes prevented of accessing certain columns in the following `reduce()`.
- Fixed warnings from using OpenAI Async embedding model in the VectorStore in Colab.

## [0.8.2] - 2024-02-28

### Added
- `%:z` timezone format code to `strptime`.
- Support for Airbyte connectors `pw.io.airbyte`.

## [0.8.1] - 2024-02-15

### Added
- Introduced the `send_alerts` function in the `pw.io.slack` namespace, enabling users to send messages from a specified column directly to a Slack channel.
- Enhanced the `pw.io.http.rest_connector` by introducing an additional argument called `request_validator`. This feature empowers users to validate payloads and raise an `HTTP 400` error if necessary.

### Fixed
- Addressed an issue in `pw.io.xpacks.llm.VectorStoreServer` where the computation of the last modification timestamp for an indexed document was incorrect.

### Changed
- Improved the behavior of `pw.io.kafka.write`. It now includes retries when sending data to the output topic encounters failures.

## [0.8.0] - 2024-02-01

### Added
- `pw.io.http.rest_connector` now supports multiple HTTP request types.
- `pw.io.http.PathwayWebserver` now allows Cross-Origin Resource Sharing (CORS) to be enabled on newly added endpoints
- Wrappers for LiteLLM and HuggingFace chat services and SentenceTransformers embedding service are now added to Pathway xpack for LLMs.

### Changed
- `pw.run` now includes an additional parameter `runtime_typechecking` that enables strict type checking at runtime.
- Embedders in pathway.xpacks.llm.embedders now correctly process empty strings as queries.
- **BREAKING**: `pw.run` and `pw.run_all` now only accept keyword arguments.

### Fixed
- `pw.Duration` can now be returned from User-Defined Functions (UDFs) or used as a constant value without resulting in errors.
- `pw.io.debezium.read` now correctly handles tables that do not have a primary key.

## [0.7.10] - 2024-01-26

### Added
- `pw.io.http.rest_connector` can now generate Open API 3.0.3 schema that will be returned by the route ``/_schema``.
- Wrappers for OpenAI Chat and Embedding services are now added to Pathway xpack for LLMs. 
- A vector indexing pipeline that allows querying for the most similar documents. It is available as class `VectorStore` as part of Pathway xpack for LLMs.

### Fixed
- `pw.debug.table_from_markdown` now uses schema parameter (when set) to properly assign _simple types_ (`int, bool, float, str, bytes`) and optional _simple types_ to columns. 

## [0.7.9] - 2024-01-18

### Changed
- `pw.io.http.rest_connector` now also accepts port as a string for backwards compatibility.
- `pw.stdlib.ml.index.KNNIndex` now sorts by distance by default.

## [0.7.8] - 2024-01-18

### Added
- Support for comparisons of tuples has been added.
- Standalone versions of methods such as `pw.groupby`, `pw.join`, `pw.join_inner`, `pw.join_left`, `pw.join_right`, and `pw.join_outer` are now available.
- The `abs` function from Python can now be used on Pathway expressions.
- The `asof_join` method now has configurable temporal behavior. The `behavior` parameter can be used to pass the configuration.
- The state of the `deduplicate` operator can now be persisted.

### Changed
- `interval_join` can now work with intervals of zero length.
- The `pw.io.http.rest_connector` can now open multiple endpoints on the same port using a new `pw.io.http.PathwayWebserver` class.
- The `pw.xpacks.connectors.sharepoint.read` and `pw.io.gdrive.read` methods now support the size limit for a single object. If set, it will exclude too large files and won't read them.

## [0.7.7] - 2023-12-27

### Added
- pathway.xpacks.llm.splitter.TokenCountSplitter.

## [0.7.6] - 2023-12-22

## New Features

### Conversion Methods in `pw.Json`
- Introducing new methods for strict conversion of `pw.Json` to desired types within a UDF body:
  - `as_int()`
  - `as_float()`
  - `as_str()`
  - `as_bool()`
  - `as_list()`
  - `as_dict()`

### DateTime Functionality
- Added `table.col.dt.utc_from_timestamp` method: Creates `DateTimeUtc` from timestamps represented as `int`s or `float`s.
- Enhanced the `table.col.dt.timestamp` method with a new `unit` argument to specify the unit of the returned timestamp.

### Experimental Features
- Introduced an experimental xpack with a Microsoft SharePoint input connector.

## Enhancements

### Improved JSON Handling
- Index operator (`[]`) can now be directly applied to `pw.Json` within UDFs to access elements of JSON objects, arrays, and strings.

### Expanded Timestamp Functionality
- Enhanced the `table.col.dt.from_timestamp` method to create `DateTimeNaive` from timestamps represented as `int`s or `float`s.
- Deprecated not specifying the `unit` argument of the `table.col.dt.timestamp` method.

### KNNIndex Enhancements
- `KNNIndex` now supports returning computed distances.
- Added support for cosine similarity in `KNNIndex`.

### Deprecated Features
- The `offset` argument of `pw.stdlib.temporal.sliding` and `pw.stdlib.temporal.tumbling` is deprecated. Use `origin` instead, as it represents a point in time, not a duration.

## Bug Fixes

### DateTime Fixes
- Sliding window now works correctly with UTC Datetimes.

### `asof_join` Improvements
- Temporal column in `asof_join` no longer has to be named `t`.
- `asof_join` includes rows with equal times for all values of the `direction` parameter.

### Fixed Issues

- Fixed an issue with `pw.io.gdrive.read`: Shared folders support is now working seamlessly.

## [0.7.5] - 2023-12-15

### Added
- Added Table.split() method for splitting table based on an expression into two tables.
- Columns with datatype duration can now be multiplied and divided by floats.
- Columns with datatype duration now support both true and floor division (`/` and `//`) by integers.

### Changed
- Pathway is better at typing if_else expressions when optional types are involved.
- `table.flatten()` operator now supports Json array.
- Buffers (used to delay outputs, configured via delay in `common_behavior`) now flush the data when the computation is finished. The effect of this change can be seen when run in bounded (batch / multi-revision) mode.
- `pw.io.subscribe()` takes additional argument `on_time_end` - the callback function to be called on each closed time of computation.
- `pw.io.subscribe()` is now a single-worker operator, guaranteeing that `on_end` is triggered at most once.
- `KNNIndex` supports now metadata filtering. Each query can specify it's own filter in the JMESPath format.

### Fixed
- Resolved an optimization bug causing `pw.iterate` to malfunction when handling columns effectively pointing to the same data.

## [0.7.4] - 2023-12-05

### Changed
- Pathway now keeps track of `array` columntype better - it is able to keep track of Array dtype and number of dimensions, wherever applicable.

### Fixed
- Fixed issues with standalone panel+Bokeh dashboards to ensure optimal functionality and performance.

## [0.7.3] - 2023-11-30

### Added
- A method `weekday` has been added to the `dt` namespace, that can be called on column expressions containing datetime data. This method returns an integer that represents the day of the week.
- **EXPERIMENTAL**: Methods `show` and `plot` on Tables, providing visualizations of data using HoloViz Panel.
- Added support for `instance` parameter to `groupby`, `join`, `windowby` and temporal join methods.
- `pw.PersistenceMode.UDF_CACHING` persistence mode enabling automatic caching of `AsyncTransformer` invocations.

### Changed
- Methods `round` and `floor` on columns with datetimes now accept duration argument to be a string.
- `pw.debug.compute_and_print` and `pw.debug.compute_and_print_update_stream` have a new argument `n_rows` that limits the number of rows printed.
- `pw.debug.table_to_pandas` has a new argument `include_id` (by default `True`). If set to `False`, creates a new index for the Pandas DataFrame, rather than using the keys of the Pathway Table.
- `windowby` function `shard` argument is now deprecated and `instance` should be used.
- Special column name `_pw_shard` is now deprecated, and `_pw_instance` should be used.
- `pw.ReplayMode` now can be accessed as `pw.PersistenceMode`, while the `SPEEDRUN` and `REALTIME` variants are now accessible as `SPEEDRUN_REPLAY` and `REALTIME_REPLAY`.
- **EXPERIMENTAL**: `pw.io.gdrive.read` has a new argument `with_metadata` (by default `False`). If set to `True`, adds a `_metadata` column containing file metadata to the resulting table.
- Methods `get_nearest_items` and `get_nearest_items_asof_now` of `KNNIndex` allow to specify `k` (number of returned elements) separately in each query.

## [0.7.2] - 2023-11-24

### Added
- Added ability of creating custom reducers using `pw.reducers.udf_reducer` decorator. Use `pw.BaseCustomAccumulator` as a base class
  for creating accumulators. Decorating accumulator returns reducer following custom logic.
- A function `pw.debug.compute_and_print_update_stream` that computes and prints the update stream of the table.
- SQLite input connector (`pw.io.sqlite`).

### Changed
- `pw.debug.parse_to_table` is now deprecated, `pw.debug.table_from_markdown` should be used instead.
- `pw.schema_from_csv` now has `quote` and `double_quote_escapes` arguments.

### Fixed
- Schema returned from `pw.schema_from_csv` will have quotes removed from column names, so it will now work properly with `pw.io.csv.read`.

## [0.7.1] - 2023-11-17

### Added

- Experimental Google Drive input connector.
- Stateful deduplication function (`pw.stateful.deduplicate`) allowing alerting on significant changes.
- The ability to split data into batches in `pw.debug.table_from_markdown` and `pw.debug.table_from_pandas`.

## [0.7.0] - 2023-11-16

### Added
- class `Behavior`, a superclass of all behavior classes.
- class `ExactlyOnceBehavior` indicating we want to create a `CommonBehavior` that results in each window producing exactly one output (shifted in time by an optional `shift` parameter).
- function `exactly_once_behavior` creating an instance of `ExactlyOnceBehavior`.

### Changed
- **BREAKING**: `WindowBehavior` is now called `CommonBehavior`, as it can be also used with interval joins.
- **BREAKING**: `window_behavior` is now called `common_behavior`, as it can be also used with interval joins.
- Deprecating parameter `keep_queries` in `pw.io.http.rest_connector`. Now `delete_completed_queries` with an opposite meaning should be used instead. The default is still `delete_completed_queries=True` (equivalent to `keep_queries=False`) but it will soon be required to be set explicitly.

## [0.6.0] - 2023-11-10

### Added
- A flag `with_metadata` for the filesystem-based connectors to attach the source file metadata to the table entries.
- Methods `pw.debug.table_from_list_of_batches` and `pw.debug.table_from_list_of_batches_by_workers` for creating tables with defined data being inserted over time.

### Changed
- **BREAKING**: `pw.debug.table_from_pandas` and `pw.debug.table_from_markdown` now will create tables in the streaming mode, instead of static, if given table definition contains `_time` column.
- **BREAKING**: Renamed the parameter `keep_queries` in `pw.io.http.rest_connector` to `delete_queries` with the opposite meaning. It changes the default behavior - it was `keep_queries=False`, now it is `delete_queries=False`.

## [0.5.3] - 2023-10-27

### Added
- A method `get_nearest_items_asof_now` in `KNNIndex` that allows to get nearest neighbors without updating old queries in the future.
- A method `asof_now_join` in `Table` to join rows from left side of the join with right side of the join at their processing time. Past rows from left side are not used when new data appears on the right side.

## [0.5.2] - 2023-10-19

### Added
- `interval_join` now supports forgetting old entries. The configuration can be passed using `behavior` parameter of `interval_join` method.
- Decorator `@table_transformer` for marking that functions take Tables as arguments.
- Namespace for all columns `Table.C.*`.
- Output connectors now provide logs about the number of entries written and time taken.
- Filesystem connectors now support reading whole files as rows.
- Command line option for `pathway spawn` to record data and `pathway replay` command to replay data.

## [0.5.1] - 2023-10-04

### Fixed
- `select` operates only on consistent states.

## [0.5.0] - 2023-10-04

### Added
- `Schema` method `typehints` that returns dict of mypy-compatible typehints.
- Support for JSON parsing from CSV sources.
- `restrict` method in `Table` to restrict table universe to the universe of the other table.
- Better support for postgresql types in the output connector.

### Changed
- **BREAKING**: renamed `Table` method `dtypes` to `typehints`. It now returns a `dict` of mypy-compatible typehints.
- **BREAKING**: `Schema.__getitem__` returns a data class `ColumnSchema` containing all related information on particular column.
- **BREAKING**: `tuple` reducer used after intervals_over window now sorts values by time.
- **BREAKING**: expressions used in `select`, `filter`, `flatten`, `with_columns`, `with_id`, `with_id_from` have to have the same universe as the table. Earlier it was possible to use an expression from a superset of a table universe. To use expressions from wider universes, one can use `restrict` on the expression source table.
- **BREAKING**: `pw.universes.promise_are_equal(t1, t2)` no longer allows to use references from `t1` and `t2` in a single expression. To change the universe of a table, use `with_universe_of`.
- **BREAKING**: `ix` and `ix_ref` are temporarily broken inside joins (both temporal and ordinary).
- `select`, `filter`, `concat` keep columns as a single stream. The work for other operators is ongoing.

### Fixed
- Optional types other than string correctly output to PostgreSQL.

## [0.4.1] - 2023-09-25

### Added
- Support for messages compressed with zstd in the Kafka connector.

## [0.4.0] - 2023-09-21

### Added
- Support for JSON data format, including `pw.Json` type.
- Methods `as_int()`, `as_float()`, `as_str()`, `as_bool()` to convert values from `Json`.
- New argument `skip_nones` for `tuple` and `sorted_tuple` reducers.
- New argument `is_outer` for `intervals_over` window.
- `pw.schema_from_dict` and `pw.schema_from_csv` for generating schema based, respectively, on provided definition as a dictionary and CSV file with sample data.
- `generate_class` method in `Schema` class for generating schema class code.

### Changed
- Method `get()` and `[]` to support accessing elements in Jsons.
- Function `pw.assert_table_has_schema` for writing asserts checking, whether given table has the same schema as the one that is given as an argument.
- **BREAKING**: `ix` and `ix_ref` operations are now standalone transformations of `pw.Table` into `pw.Table`. Most of the usages remain the same, but sometimes user needs to provide a context (when e.g. using them inside `join` or `groupby` operations). `ix` and `ix_ref` are temporarily broken inside temporal joins.

### Fixed
- Fixed a bug where new-style optional types (e.g. `int | None`) were translated to `Any` dtype.

## [0.3.4] - 2023-09-18

### Fixed
- Incompatible `beartype` version is now excluded from dependencies.

## [0.3.3] - 2023-09-14

### Added
- Module `pathway.dt` to construct and manipulate DTypes.
- New argument `keep_queries` in `pw.io.http.rest_connector`.

### Changed
- Internal representation of DTypes. Inputting types is compatible backwards.
- Temporal functions now accept arguments of mixed types (ints and floats). For example, `pw.temporal.interval` can use ints while columns it interacts with are floats.
- Single-element arrays are now treated as arrays, not as scalars.

### Fixed
- `to_string()` method on datetimes always prints 9 fractional digits.
- `%f` format code in `strptime()` parses fractional part of a second correctly regardless of the number of digits.

## [0.3.2] - 2023-09-07

### Added

- `Table.cast_to_types()` function that can perform `pathway.cast` on multiple columns.
- `intervals_over` window, which allows to get temporally close data to given times.
- `demo.replay_csv_with_time` function that can replay a CSV file following the timestamps of a given column.

### Fixed

- Static data is now copied to ensure immutability.
- Improved error tracing mechanism to work with any type of error.

## [0.3.1] - 2023-08-29

### Added

- `tuple` reducer, that returns a tuple with values.
- `ndarray` reducer, that returns an array with values.

### Changed
- `numpy` arrays of `int32`, `uint32` and `float32` are now converted to their 64-bit variants instead of tuples.
- KNNIndex interface to take columns as inputs.
- Reducers now check types of their arguments.

### Fixed

- Fixed delayed reporting of output connector errors.
- Python objects are now freed more often, reducing peak memory usage.

## [0.3.0] - 2023-08-07

### Added

- `@` (matrix multiplication) operator.

### Changed

- Python version 3.10 or later is now required.
- Type checking is now more strict.

## [0.2.1] - 2023-07-31

### Changed

- Immediately forget queries in REST connector.
- Make type annotations mandatory in `Schema`.

### Fixed

- Fixed IDs coming from CSV source.
- Fixed indices of dataframes from pandas transformer.

## [0.2.0] - 2023-07-20

### Added

<img src="https://d14l3brkh44201.cloudfront.net/PathwayManul.svg"  alt="manul" width="50px"></img>

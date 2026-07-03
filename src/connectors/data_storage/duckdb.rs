// Copyright © 2026 Pathway

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::thread::sleep;
use std::time::{Duration, Instant};

use duckdb::types::{TimeUnit, Value as DuckValue};
pub use duckdb::Error as DuckDbDriverError;
use duckdb::{appender_params_from_iter, params_from_iter, Connection as DuckConnection};
use itertools::Itertools;
use log::warn;
use serde_json::{json, Value as JsonValue};

use crate::connectors::data_format::{create_bincoded_value, FormatterContext};
use crate::connectors::data_storage::{SqlQueryTemplate, TableContext, TableWriterInitMode};
use crate::connectors::{WriteError, Writer};
use crate::engine::time::DateTime as DateTimeTrait;
use crate::engine::{Type, Value};
use crate::python_api::ValueField;

/// Errors specific to the `DuckDB` output connector.
///
/// A dedicated enum (rather than wrapping the raw driver error directly into
/// [`WriteError`]) keeps the connector's failure modes self-contained and lets
/// us surface an actionable message for the misconfiguration users most
/// commonly hit — pointing the writer at a table that doesn't exist while using
/// the default init mode.
#[derive(Debug, thiserror::Error)]
pub enum DuckDbError {
    /// Any error the `duckdb` driver returns (opening the database file, running
    /// the init DDL, executing an INSERT/DELETE the connector built, …).
    #[error(transparent)]
    Driver(#[from] DuckDbDriverError),

    /// The destination name resolves to a `VIEW`. `DuckDB` views are read-only,
    /// so the writer cannot `INSERT` into them.
    #[error("destination {table_name:?} is a DuckDB view, which cannot be written to")]
    DestinationIsView { table_name: String },

    /// The destination name resolves to something that is neither a table nor a
    /// view (e.g. a sequence).
    #[error("destination {table_name:?} is a DuckDB {kind}, not a table")]
    DestinationIsNotATable { table_name: String, kind: String },

    /// `init_mode="default"` was used but the destination table does not exist.
    #[error(
        "destination table {table_name:?} does not exist in the DuckDB database. \
         Create it with a compatible schema, or use \
         init_mode=\"create_if_not_exists\" / \"replace\" to let the writer create it."
    )]
    DestinationTableNotFound { table_name: String },

    /// The destination table is missing one or more columns the writer must
    /// `INSERT` into (including the `time`/`diff` columns in stream-of-changes
    /// mode).
    #[error(
        "destination table {table_name:?} is missing required column(s) {columns:?}. \
         Add them, or use init_mode=\"replace\" to recreate the table."
    )]
    DestinationMissingColumn {
        table_name: String,
        columns: Vec<String>,
    },

    /// The destination table has an extra `NOT NULL` column without a default
    /// that the writer does not supply, so every write would violate it.
    #[error(
        "destination table {table_name:?} has NOT NULL column(s) {columns:?} without a \
         default that the Pathway table does not provide a value for."
    )]
    DestinationMissingRequiredValue {
        table_name: String,
        columns: Vec<String>,
    },

    /// An `Optional` Pathway column maps onto a `NOT NULL` destination column, so
    /// a `None` value would violate the constraint at write time.
    #[error(
        "Pathway column(s) {columns:?} are Optional but map onto NOT NULL column(s) \
         in destination table {table_name:?}; relax the destination columns or the schema."
    )]
    DestinationColumnRequiresNonNull {
        table_name: String,
        columns: Vec<String>,
    },

    /// A list/array/tuple/JSON column has a (possibly nested) element type that the
    /// connector cannot encode into the JSON value such columns are bound as. Without
    /// this check the `CREATE TABLE` succeeds and every preflight passes, but the
    /// first write fails with an opaque "unsupported type" error mid-stream.
    #[error(
        "column {column:?} of type {type_} cannot be written to DuckDB: list, array, \
         tuple and JSON columns may only contain booleans, integers, floats, strings, \
         JSON, or nested lists/tuples of those — not values such as bytes, datetimes or \
         durations."
    )]
    UnsupportedListElementType { column: String, type_: String },

    /// Snapshot mode upserts with `INSERT ... ON CONFLICT (primary_key)`, which
    /// `DuckDB` only accepts when a PRIMARY KEY or UNIQUE constraint matches the
    /// conflict target. A pre-existing destination table (`init_mode="default"`)
    /// that has the right columns but no such constraint would otherwise fail on
    /// the first upsert with an opaque binder error.
    #[error(
        "destination table {table_name:?} has no PRIMARY KEY or UNIQUE constraint on the \
         snapshot primary-key column(s) {columns:?} that snapshot mode's \
         INSERT ... ON CONFLICT requires. Add such a constraint, or use \
         init_mode=\"replace\" / \"create_if_not_exists\" to let the writer create the \
         table with it."
    )]
    DestinationMissingUniqueConstraint {
        table_name: String,
        columns: Vec<String>,
    },

    /// A detaching writer (`detach_between_batches=True`) could not re-acquire
    /// the database file within the retry budget because another process kept
    /// holding a conflicting lock the whole time.
    #[error(
        "could not lock DuckDB database file {path:?} within {timeout_secs} seconds: \
         a conflicting lock is held by another process. DuckDB allows either one \
         read-write process or several read-only processes per database file, never \
         both — make sure concurrent readers use short-lived read-only connections \
         (and retry on lock contention) and that no other writer process holds the \
         file open. Last error: {source}"
    )]
    DatabaseLocked {
        path: String,
        timeout_secs: u64,
        source: DuckDbDriverError,
    },

    /// `detach_between_batches=True` with an in-memory database: the database
    /// ceases to exist when its last connection closes, so detaching after every
    /// batch would silently drop all data. The Python side rejects this at
    /// `write()` time; this variant is the engine-side guard.
    #[error(
        "detach_between_batches cannot be used with an in-memory DuckDB database: \
         an in-memory database is dropped when its last connection closes, so all \
         data would be lost after every batch. Use an on-disk database file instead."
    )]
    DetachOnInMemoryDatabase,

    /// An array value has more (or fewer) dimensions than the destination column
    /// was created with. A column typed only as `np.ndarray` carries no
    /// dimensionality, so the writer creates a 1-D `DOUBLE[]` list; a value with a
    /// different number of dimensions cannot be cast into it.
    #[error(
        "column {column:?} expects a {expected}-dimensional array but a value with \
         {actual} dimension(s) was written. Declare the array's dimensionality in \
         the schema (e.g. a 2-D array type) so the destination column matches."
    )]
    ArrayDimensionalityMismatch {
        column: String,
        expected: usize,
        actual: usize,
    },
}

/// Quote a `DuckDB` identifier with double quotes, doubling any internal double
/// quotes. Applied to every user-supplied table or column name interpolated
/// into generated SQL so reserved words and characters that require quoting
/// (hyphens, spaces, mixed case, …) survive round-tripping.
fn quote_duckdb_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Upper bound on how many rows go into one multi-row `INSERT` statement in
/// stream-of-changes mode. `DuckDB` is columnar: a single-row `INSERT` per change
/// is very slow, while batching many rows into one statement amortizes the
/// per-statement cost and is orders of magnitude faster. The bound is on the
/// parameter count (rows × row width) rather than rows so a very wide schema
/// still produces a reasonable statement.
const MAX_PARAMS_PER_STATEMENT: usize = 8192;

/// Process-global registry of open on-disk `DuckDB` databases, keyed by the file
/// path. The `duckdb` crate's `Connection::open` creates a fresh, independent
/// database instance every time, so two writers pointed at the same `.duckdb`
/// file (e.g. writing several tables into one database in one run) would each get
/// their own instance and the last one to close would clobber the others' tables.
///
/// To match `DuckDB`'s own single-process instance sharing, the first writer for
/// a path opens the database and stores a [`Weak`] handle here; every writer then
/// takes its own connection on that shared instance via [`DuckConnection::try_clone`]
/// (so each keeps independent transactions) while holding a strong reference to
/// the anchor. The instance is closed once the last writer for the path is
/// dropped — the [`Weak`] keeps it from being pinned for the whole process.
static OPEN_DATABASES: LazyLock<Mutex<HashMap<String, Weak<Mutex<DuckConnection>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Open a connection on the shared instance for `path`, returning the working
/// connection (an independent clone) and the anchor to keep the instance alive
/// for the caller's lifetime. `:memory:` is never shared — each in-memory
/// database is intentionally private to its writer.
fn open_shared_connection(
    path: &str,
) -> Result<(DuckConnection, Arc<Mutex<DuckConnection>>), WriteError> {
    let mut registry = OPEN_DATABASES.lock().unwrap();
    let anchor = if let Some(existing) = registry.get(path).and_then(Weak::upgrade) {
        existing
    } else {
        let connection = DuckConnection::open(path)?;
        let anchor = Arc::new(Mutex::new(connection));
        registry.insert(path.to_owned(), Arc::downgrade(&anchor));
        anchor
    };
    let working = anchor.lock().unwrap().try_clone()?;
    Ok((working, anchor))
}

/// Retry schedule for a detaching writer re-acquiring the database file: a
/// momentarily-connected read-only reader holds a conflicting lock, so start
/// with a short delay ([`LOCK_RETRY_INITIAL_DELAY`]), double it up to
/// [`LOCK_RETRY_MAX_DELAY`], and give up after [`LOCK_RETRY_TOTAL`] — long
/// enough to ride out any well-behaved reader, short enough that a stuck
/// foreign process surfaces as a clear error rather than a silent stall.
const LOCK_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(10);
const LOCK_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);
const LOCK_RETRY_TOTAL: Duration = Duration::from_secs(30);

/// The `IO Error` message `DuckDB` produces when another process holds a
/// conflicting lock on the database file. Used to tell retryable lock
/// contention apart from permanent open failures (bad path, corrupt file, …).
fn is_lock_contention(error: &DuckDbDriverError) -> bool {
    error.to_string().contains("Could not set lock on file")
}

/// Count of writers per path that hold their connection for the whole run
/// (`detach_between_batches=False`). A detaching writer targeting the same path
/// consults this to warn that the file lock will never actually be released
/// between its batches — the shared instance in [`OPEN_DATABASES`] stays alive
/// as long as any holding writer keeps its anchor.
static HOLDING_WRITERS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// How the writer turns buffered changes into SQL, fixed at construction.
enum WritePlan {
    /// Snapshot mode: each `+1` upserts and each `-1` deletes, applied one row
    /// at a time to preserve per-key ordering within a minibatch.
    Snapshot {
        /// `INSERT ... ON CONFLICT (pk) DO UPDATE` for a single row.
        insertion: String,
        /// `DELETE ... WHERE pk = ?` for a single row.
        deletion: String,
        /// Positions of the primary-key columns within the value-field list,
        /// sorted ascending — used to project a retraction's bound parameters
        /// down to just the key columns for the `DELETE`.
        primary_key_indices: Vec<usize>,
    },
    /// Stream-of-changes mode: every change is appended with its `time`/`diff`
    /// metadata, batched into multi-row `INSERT`s.
    StreamOfChanges {
        /// `INSERT INTO t (cols, time, diff) VALUES ` — the multi-row value
        /// tuples are appended per flush.
        insert_prefix: String,
        /// A single `(ph1, …, ?, ?)` value tuple, repeated once per row.
        row_tuple: String,
        /// Bound parameters per row (value columns + `time` + `diff`).
        row_width: usize,
    },
}

/// `DuckDB` output connector.
///
/// Mirrors the Pathway collection into a `DuckDB` table in one of two modes:
///
/// * **stream of changes** — every change is appended as a row, with two extra
///   `time` (`BIGINT`) and `diff` (`SMALLINT`) metadata columns;
/// * **snapshot** — `+1` events become `INSERT ... ON CONFLICT (pk) DO UPDATE`
///   and `-1` events become `DELETE ... WHERE pk = ?`, so the destination table
///   always holds the current logical contents of the Pathway table, with no
///   `time`/`diff` columns.
///
/// Embeddings (Pathway `numpy` arrays or `list[float]` columns) are stored as
/// native `DuckDB` list columns (`DOUBLE[]`), directly searchable with
/// `list_cosine_similarity` & friends. Because the `duckdb` crate cannot bind a
/// list value as a statement parameter, list/array/tuple and JSON values are
/// bound as a JSON string and cast back into the destination type inside the
/// generated SQL (`CAST(CAST(? AS JSON) AS DOUBLE[])`).
#[allow(clippy::struct_excessive_bools)]
pub struct DuckDbWriter {
    /// Path to the database file (or `:memory:`). The connection is opened
    /// lazily, on the first non-empty flush — see
    /// [`DuckDbWriter::ensure_connection`].
    path: String,
    table_name: String,
    quoted_table_name: String,
    /// Schema and key columns, retained so the `CREATE TABLE` DDL can run when
    /// the connection is opened.
    value_fields: Vec<ValueField>,
    key_field_names: Option<Vec<String>>,
    snapshot_mode: bool,
    init_mode: TableWriterInitMode,
    buffer: Vec<FormatterContext>,
    max_batch_size: Option<usize>,
    /// How buffered changes are turned into SQL.
    plan: WritePlan,
    /// Whether the fast `DuckDB` `Appender` bulk path can be used:
    /// stream-of-changes mode with only scalar columns (the `Appender` rejects
    /// list/array/JSON values, which the SQL path handles via a JSON cast).
    appendable: bool,
    /// The open connection, created lazily on the first flush. `DuckDB` takes an
    /// exclusive lock on the database file, so opening it eagerly on every
    /// engine worker (each writer is constructed per worker) would make all but
    /// one fail. Because the sink is single-threaded, only the worker that
    /// actually receives data ever flushes, and so only it opens the file.
    connection: Option<DuckConnection>,
    /// Whether this writer is responsible for running the init-mode DDL. Only
    /// the single worker that owns the (single-threaded) sink — worker 0 — sets
    /// this. That worker opens the file even when the output is empty so the
    /// init-mode DDL still runs and an empty Pathway table still creates/replaces
    /// the destination table.
    needs_initialization: bool,
    /// Keeps the shared `DuckDB` database instance for [`Self::path`] alive while
    /// this writer is open, so several writers targeting the same file share one
    /// instance (see [`OPEN_DATABASES`]). `None` for `:memory:` and before the
    /// connection is opened.
    db_anchor: Option<Arc<Mutex<DuckConnection>>>,
    /// When `true`, the writer closes its connection (and drops its `db_anchor`)
    /// after every flush and re-opens the database on the next one, releasing
    /// the OS file lock between minibatches so other processes can read the file
    /// with short-lived read-only connections.
    detach_between_batches: bool,
    /// Whether the init-mode DDL and the destination preflight have already run.
    /// Tracked independently of the connection: a detaching writer re-opens the
    /// database on every flush, and re-running `init_mode="replace"` there would
    /// drop and recreate the table each minibatch, silently discarding all
    /// previously written data.
    initialized: bool,
    /// Whether this (non-detaching) writer has registered itself in
    /// [`HOLDING_WRITERS`], so `Drop` knows to deregister it.
    registered_as_holder: bool,
    /// A detaching writer warns at most once that a holding writer on the same
    /// path pins the shared instance, so the file lock is never released.
    warned_mixed_hold: bool,
}

impl DuckDbWriter {
    #[allow(clippy::fn_params_excessive_bools)]
    pub fn new(
        path: String,
        table_ctx: TableContext,
        snapshot_mode: bool,
        init_mode: TableWriterInitMode,
        max_batch_size: Option<usize>,
        needs_initialization: bool,
        detach_between_batches: bool,
    ) -> Result<Self, WriteError> {
        // An in-memory database is dropped when its last connection closes, so a
        // detaching writer would silently lose all data after every batch. The
        // Python bridge rejects this combination at `write()` time; this is the
        // engine-side guard.
        if detach_between_batches && path == ":memory:" {
            return Err(DuckDbError::DetachOnInMemoryDatabase.into());
        }
        // `TableContext::schema_name` is for connectors with a schema concept
        // (PostgreSQL, SQL Server); DuckDB has none here, so the Python bridge
        // sets it to an empty string and it is ignored.
        let TableContext {
            table_name,
            value_fields,
            key_field_names,
            ..
        } = table_ctx;
        let quoted_table_name = quote_duckdb_identifier(&table_name);

        // Build the SQL plan up front — this needs no connection and validates
        // the schema (unsupported types, missing keys, …) at construction time,
        // on every worker, before any file is touched.
        let plan = build_plan(
            snapshot_mode,
            &quoted_table_name,
            &value_fields,
            key_field_names.as_deref(),
        )?;

        // The Appender is the fast bulk path, but it cannot bind list/array/JSON
        // values (those go through a JSON cast in the SQL path) and it appends
        // every row, so it only fits stream-of-changes mode with scalar columns.
        let appendable = !snapshot_mode
            && value_fields
                .iter()
                .all(|field| !needs_json_cast(&field.type_));

        Ok(Self {
            path,
            table_name,
            quoted_table_name,
            value_fields,
            key_field_names,
            snapshot_mode,
            init_mode,
            buffer: Vec::new(),
            max_batch_size,
            plan,
            appendable,
            connection: None,
            needs_initialization,
            db_anchor: None,
            detach_between_batches,
            initialized: false,
            registered_as_holder: false,
            warned_mixed_hold: false,
        })
    }

    /// Open the database file if no connection is currently held, and run the
    /// init-mode DDL and the destination preflight on the first open only. A
    /// non-detaching writer opens once and keeps the connection for its whole
    /// lifetime; a detaching writer re-enters here on every flush, skipping the
    /// initialization (re-running `init_mode="replace"` would wipe the data
    /// written by earlier minibatches).
    fn ensure_connection(&mut self) -> Result<(), WriteError> {
        if self.connection.is_some() {
            return Ok(());
        }
        let connection = if self.path == ":memory:" {
            DuckConnection::open_in_memory()?
        } else {
            // Share a single instance per file path so several writers targeting
            // the same database don't clobber each other's tables.
            let (working, anchor) = if self.detach_between_batches {
                // A concurrent read-only reader may momentarily hold the file
                // lock — that is the point of detaching — so ride it out with a
                // bounded backoff instead of failing the flush on first contact.
                self.open_shared_connection_with_retry()?
            } else {
                open_shared_connection(&self.path)?
            };
            self.db_anchor = Some(anchor);
            if !self.detach_between_batches && !self.registered_as_holder {
                *HOLDING_WRITERS
                    .lock()
                    .unwrap()
                    .entry(self.path.clone())
                    .or_insert(0) += 1;
                self.registered_as_holder = true;
            }
            working
        };
        if !self.initialized {
            // Apply the requested init mode (create / replace). The shared builder
            // appends `time`/`diff` to the CREATE only in stream-of-changes mode,
            // and a `PRIMARY KEY (...)` when key fields are given — required so
            // snapshot-mode `INSERT ... ON CONFLICT` has a constraint to target.
            self.init_mode.initialize(
                &self.quoted_table_name,
                &self.value_fields,
                self.key_field_names.as_deref(),
                !self.snapshot_mode,
                |query| {
                    connection.execute_batch(query)?;
                    Ok(())
                },
                duckdb_data_type,
                quote_duckdb_identifier,
            )?;
            self.validate_destination(&connection)?;
            self.initialized = true;
        }
        self.connection = Some(connection);
        Ok(())
    }

    /// [`open_shared_connection`] with exponential backoff on lock contention,
    /// used by a detaching writer to re-acquire the database file: a read-only
    /// reader from another process may be connected right now, holding a
    /// conflicting lock that it will release shortly. Non-lock errors (bad path,
    /// corrupt file, …) fail immediately.
    fn open_shared_connection_with_retry(
        &self,
    ) -> Result<(DuckConnection, Arc<Mutex<DuckConnection>>), WriteError> {
        let started = Instant::now();
        let mut delay = LOCK_RETRY_INITIAL_DELAY;
        loop {
            match open_shared_connection(&self.path) {
                Err(WriteError::DuckDB(DuckDbError::Driver(error)))
                    if is_lock_contention(&error) =>
                {
                    if started.elapsed() + delay > LOCK_RETRY_TOTAL {
                        return Err(DuckDbError::DatabaseLocked {
                            path: self.path.clone(),
                            timeout_secs: LOCK_RETRY_TOTAL.as_secs(),
                            source: error,
                        }
                        .into());
                    }
                    sleep(delay);
                    delay = (delay * 2).min(LOCK_RETRY_MAX_DELAY);
                }
                result => return result,
            }
        }
    }

    /// Drop the working connection and the shared-instance anchor, closing the
    /// database (and thereby checkpointing the WAL and releasing the OS file
    /// lock) once no other writer in this process holds the instance. Warns —
    /// once — when a non-detaching writer targets the same path: its anchor pins
    /// the instance, so detaching never actually releases the lock.
    fn release_connection(&mut self) {
        self.connection = None;
        self.db_anchor = None;
        if !self.warned_mixed_hold
            && HOLDING_WRITERS
                .lock()
                .unwrap()
                .get(&self.path)
                .is_some_and(|&count| count > 0)
        {
            warn!(
                "DuckDB writer for table {:?} uses detach_between_batches=True, but \
                 another writer holds database {:?} open for the whole run; the file \
                 lock will not be released between batches until that writer finishes.",
                self.table_name, self.path
            );
            self.warned_mixed_hold = true;
        }
    }

    /// Preflight the destination table once the connection is open: reject views,
    /// a missing table (`init_mode="default"`), missing columns, extra `NOT NULL`
    /// columns the writer cannot fill, and `Optional` columns landing on `NOT
    /// NULL` destination columns — so misconfigurations surface as a clear error
    /// at start-up rather than an opaque driver error mid-write.
    #[allow(clippy::too_many_lines)]
    fn validate_destination(&self, connection: &DuckConnection) -> Result<(), WriteError> {
        let table = self.table_name.as_str();

        let kinds = query_single_column(
            connection,
            "SELECT table_type FROM information_schema.tables \
             WHERE lower(table_name) = lower(?)",
            table,
        )?;
        if let Some(kind) = kinds.first() {
            if kind == "VIEW" {
                return Err(DuckDbError::DestinationIsView {
                    table_name: table.to_owned(),
                }
                .into());
            } else if kind != "BASE TABLE" {
                return Err(DuckDbError::DestinationIsNotATable {
                    table_name: table.to_owned(),
                    kind: kind.to_lowercase(),
                }
                .into());
            }
        }

        // Each existing column as (lower-cased name, is_nullable, has_default).
        let mut statement = connection.prepare(
            "SELECT column_name, is_nullable = 'YES', column_default IS NOT NULL \
             FROM information_schema.columns WHERE lower(table_name) = lower(?)",
        )?;
        let mut existing: Vec<(String, bool, bool)> = Vec::new();
        let rows = statement.query_map([table], |row| {
            Ok((
                row.get::<_, String>(0)?.to_lowercase(),
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?;
        for row in rows {
            existing.push(row?);
        }

        if existing.is_empty() {
            return Err(DuckDbError::DestinationTableNotFound {
                table_name: table.to_owned(),
            }
            .into());
        }

        // Columns the writer inserts into: the value fields, plus time/diff in
        // stream-of-changes mode.
        let mut required: Vec<String> = self
            .value_fields
            .iter()
            .map(|field| field.name.clone())
            .collect();
        if !self.snapshot_mode {
            required.push("time".to_owned());
            required.push("diff".to_owned());
        }
        let required_lower: std::collections::HashSet<String> =
            required.iter().map(|name| name.to_lowercase()).collect();
        let existing_lower: std::collections::HashSet<&str> =
            existing.iter().map(|(name, _, _)| name.as_str()).collect();

        let missing: Vec<String> = required
            .iter()
            .filter(|name| !existing_lower.contains(name.to_lowercase().as_str()))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(DuckDbError::DestinationMissingColumn {
                table_name: table.to_owned(),
                columns: missing,
            }
            .into());
        }

        // Extra destination columns that are NOT NULL without a default: the
        // writer never supplies a value for them, so every write would fail.
        let requires_value: Vec<String> = existing
            .iter()
            .filter(|(name, nullable, has_default)| {
                !required_lower.contains(name) && !*nullable && !*has_default
            })
            .map(|(name, _, _)| name.clone())
            .collect();
        if !requires_value.is_empty() {
            return Err(DuckDbError::DestinationMissingRequiredValue {
                table_name: table.to_owned(),
                columns: requires_value,
            }
            .into());
        }

        // Optional Pathway columns landing on NOT NULL destination columns: a
        // None value would violate the constraint at write time.
        let not_nullable: std::collections::HashSet<&str> = existing
            .iter()
            .filter(|(_, nullable, _)| !*nullable)
            .map(|(name, _, _)| name.as_str())
            .collect();
        let optional_on_not_null: Vec<String> = self
            .value_fields
            .iter()
            .filter(|field| matches!(field.type_, Type::Optional(_)))
            .filter(|field| not_nullable.contains(field.name.to_lowercase().as_str()))
            .map(|field| field.name.clone())
            .collect();
        if !optional_on_not_null.is_empty() {
            return Err(DuckDbError::DestinationColumnRequiresNonNull {
                table_name: table.to_owned(),
                columns: optional_on_not_null,
            }
            .into());
        }

        // Snapshot mode upserts via `INSERT ... ON CONFLICT (primary_key)`, which
        // DuckDB binds against a PRIMARY KEY / UNIQUE constraint covering exactly
        // those columns. A table created by the writer always has it (the init DDL
        // adds `PRIMARY KEY (...)`), but a pre-existing table the user points at
        // may not — so verify one exists rather than letting the first upsert fail
        // with an opaque binder error.
        if self.snapshot_mode {
            let key_field_names = self
                .key_field_names
                .as_deref()
                .filter(|keys| !keys.is_empty())
                .expect("snapshot mode requires non-empty key fields");
            let wanted: std::collections::HashSet<String> =
                key_field_names.iter().map(|k| k.to_lowercase()).collect();

            // `unnest(constraint_column_names)` yields one row per (constraint,
            // column); group by `constraint_index` to rebuild each constraint's
            // column set, then check one of them equals the snapshot primary key.
            let mut statement = connection.prepare(
                "SELECT constraint_index, lower(unnest(constraint_column_names)) \
                 FROM duckdb_constraints() \
                 WHERE lower(table_name) = lower(?) \
                   AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')",
            )?;
            let mut by_constraint: std::collections::HashMap<
                i64,
                std::collections::HashSet<String>,
            > = std::collections::HashMap::new();
            let rows = statement.query_map([table], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (index, column) = row?;
                by_constraint.entry(index).or_default().insert(column);
            }
            let satisfied = by_constraint.values().any(|columns| *columns == wanted);
            if !satisfied {
                return Err(DuckDbError::DestinationMissingUniqueConstraint {
                    table_name: table.to_owned(),
                    columns: key_field_names.to_vec(),
                }
                .into());
            }
        }

        Ok(())
    }

    /// Bulk-append `buffer` through `DuckDB`'s `Appender` — the fast path for
    /// stream-of-changes mode with scalar columns. The column list is passed
    /// explicitly, so the append targets the right columns regardless of the
    /// destination table's physical column order.
    fn flush_with_appender(&self, buffer: &[FormatterContext]) -> Result<(), WriteError> {
        let connection = self
            .connection
            .as_ref()
            .expect("ensure_connection just opened the connection");
        let mut columns: Vec<&str> = self
            .value_fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        columns.push("time");
        columns.push("diff");
        let mut appender = connection.appender_with_columns(&self.table_name, &columns)?;
        for ctx in buffer {
            let mut row: Vec<DuckValue> = ctx
                .values
                .iter()
                .map(encode_value)
                .collect::<Result<_, _>>()?;
            row.push(DuckValue::BigInt(time_to_i64(ctx.time.0)?));
            row.push(DuckValue::BigInt(diff_to_i64(ctx.diff)?));
            appender.append_row(appender_params_from_iter(row.iter()))?;
        }
        appender.flush()?;
        Ok(())
    }
}

impl Writer for DuckDbWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.buffer.push(data);
        if let Some(max) = self.max_batch_size {
            if self.buffer.len() >= max {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        let result = self.flush_inner();
        // A detaching writer releases the file lock after every flush — also on
        // a failed one, so an error propagating out of the flush never leaks a
        // held lock that would linger until the process exits.
        if self.detach_between_batches {
            self.release_connection();
        }
        result
    }

    fn name(&self) -> String {
        format!("DuckDB({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        // DuckDB is an embedded, single-file analytical database that takes an
        // exclusive lock on the database file: it may be opened read-write by
        // only one connection at a time. Funneling the sink onto one worker
        // (combined with the lazy open in `ensure_connection`, so only the
        // worker that receives data opens the file) keeps that invariant. This
        // costs no real write throughput — DuckDB serializes writes to a file
        // regardless — and makes the output complete and deterministic.
        // Snapshot mode additionally needs single-threading for correct
        // UPSERT/DELETE ordering.
        true
    }
}

impl DuckDbWriter {
    fn flush_inner(&mut self) -> Result<(), WriteError> {
        // Open the file and run the init-mode DDL even with nothing buffered, so
        // an empty output still creates/replaces the destination table. Only the
        // single output-owning worker (index 0) does this — other workers never
        // receive data and must not contend for DuckDB's exclusive file lock.
        // Restricted to the modes that own table creation: "default" makes no
        // schema changes, so an empty output leaves the database untouched as
        // before (it still validates lazily once data arrives). Once the
        // initialization has run, an empty flush needs no connection at all — in
        // particular a detaching writer must not re-open (and re-lock) the file
        // on every idle minibatch.
        if self.needs_initialization
            && !self.initialized
            && self.init_mode != TableWriterInitMode::Default
        {
            self.ensure_connection()?;
        }
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_connection()?;
        let buffer = std::mem::take(&mut self.buffer);
        if self.appendable {
            return self.flush_with_appender(&buffer);
        }
        let connection = self
            .connection
            .as_mut()
            .expect("ensure_connection just opened the connection");
        let tx = connection.transaction()?;
        match &self.plan {
            WritePlan::Snapshot {
                insertion,
                deletion,
                primary_key_indices,
            } => {
                let mut insert_stmt = tx.prepare(insertion)?;
                let mut delete_stmt = tx.prepare(deletion)?;
                for ctx in &buffer {
                    for (value, field) in ctx.values.iter().zip(&self.value_fields) {
                        check_array_dimensionality(value, field)?;
                    }
                    let encoded: Vec<DuckValue> = ctx
                        .values
                        .iter()
                        .map(encode_value)
                        .collect::<Result<_, _>>()?;
                    if ctx.diff > 0 {
                        insert_stmt.execute(params_from_iter(encoded.iter()))?;
                    } else {
                        let key_params = select_primary_key(encoded, primary_key_indices);
                        delete_stmt.execute(params_from_iter(key_params.iter()))?;
                    }
                }
            }
            WritePlan::StreamOfChanges {
                insert_prefix,
                row_tuple,
                row_width,
            } => {
                let rows_per_statement = (MAX_PARAMS_PER_STATEMENT / row_width).max(1);
                for chunk in buffer.chunks(rows_per_statement) {
                    let mut query = String::with_capacity(
                        insert_prefix.len() + chunk.len() * (row_tuple.len() + 1),
                    );
                    query.push_str(insert_prefix);
                    for (row_index, _) in chunk.iter().enumerate() {
                        if row_index > 0 {
                            query.push(',');
                        }
                        query.push_str(row_tuple);
                    }
                    let mut params: Vec<DuckValue> = Vec::with_capacity(chunk.len() * row_width);
                    for ctx in chunk {
                        for (value, field) in ctx.values.iter().zip(&self.value_fields) {
                            check_array_dimensionality(value, field)?;
                        }
                        for value in &ctx.values {
                            params.push(encode_value(value)?);
                        }
                        params.push(DuckValue::BigInt(time_to_i64(ctx.time.0)?));
                        params.push(DuckValue::BigInt(diff_to_i64(ctx.diff)?));
                    }
                    tx.execute(&query, params_from_iter(params.iter()))?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
}

/// Deregister a holding writer from [`HOLDING_WRITERS`], so a detaching writer
/// that outlives it stops warning about a lock that is no longer pinned.
impl Drop for DuckDbWriter {
    fn drop(&mut self) {
        if self.registered_as_holder {
            let mut holders = HOLDING_WRITERS.lock().unwrap();
            if let Some(count) = holders.get_mut(&self.path) {
                *count -= 1;
                if *count == 0 {
                    holders.remove(&self.path);
                }
            }
        }
    }
}

fn time_to_i64(time: u64) -> Result<i64, WriteError> {
    i64::try_from(time).map_err(|_| WriteError::IntOutOfRange(i64::MAX))
}

fn diff_to_i64(diff: isize) -> Result<i64, WriteError> {
    i64::try_from(diff).map_err(|_| WriteError::IntOutOfRange(i64::MAX))
}

/// Project a row's full set of bound parameters down to just the primary-key
/// parameters, in the ascending-index order the `DELETE`'s `WHERE` clause
/// expects. Mirrors [`SqlQueryTemplate::primary_key_fields`].
fn select_primary_key<T>(mut fields: Vec<T>, primary_key_indices: &[usize]) -> Vec<T> {
    for (index, &pkey_index) in primary_key_indices.iter().enumerate() {
        fields.swap(index, pkey_index);
    }
    fields.truncate(primary_key_indices.len());
    fields
}

/// Map a Pathway type onto the `DuckDB` storage type the writer creates.
///
/// `is_nested` is `false` for a top-level column (so a `NOT NULL` constraint is
/// appended unless the column is `Optional`) and `true` when recursing into a
/// list/array element type (no constraint there). Float arrays and `list[float]`
/// land in `DOUBLE[]` list columns so they can be consumed directly by `DuckDB`'s
/// vector-distance functions in a RAG pipeline.
fn duckdb_data_type(type_: &Type, is_nested: bool) -> Result<String, WriteError> {
    if let Type::Optional(wrapped) = type_ {
        return duckdb_data_type(wrapped, true);
    }
    let base = match type_ {
        Type::Bool => "BOOLEAN".to_string(),
        Type::Int => "BIGINT".to_string(),
        Type::Float => "DOUBLE".to_string(),
        Type::String | Type::Pointer => "VARCHAR".to_string(),
        Type::Bytes | Type::PyObjectWrapper => "BLOB".to_string(),
        // DateTimeUtc is stored as a UTC wall-clock TIMESTAMP (microsecond
        // resolution), matching how the value is bound; this is deterministic
        // and independent of any session time zone.
        Type::DateTimeNaive | Type::DateTimeUtc => "TIMESTAMP".to_string(),
        Type::Duration => "INTERVAL".to_string(),
        Type::Json => "JSON".to_string(),
        Type::List(inner) => format!("{}[]", duckdb_data_type(inner, true)?),
        Type::Array(n_dim, wrapped) => {
            let inner = if matches!(wrapped.as_ref(), Type::Any) {
                "DOUBLE".to_string()
            } else {
                duckdb_data_type(wrapped, true)?
            };
            let n_dim = n_dim.unwrap_or(1);
            format!("{inner}{}", "[]".repeat(n_dim))
        }
        Type::Tuple(args) => {
            let element_types = args
                .iter()
                .map(|arg| duckdb_data_type(arg, true))
                .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
            if element_types.len() == 1 {
                format!("{}[]", element_types.into_iter().next().unwrap())
            } else {
                // Heterogeneous tuples have no natural list column type; store
                // them as a JSON value.
                "JSON".to_string()
            }
        }
        Type::Optional(_) => unreachable!("handled above"),
        Type::Any | Type::Future(_) => return Err(WriteError::UnsupportedType(type_.clone())),
    };
    let suffix = if is_nested { "" } else { " NOT NULL" };
    Ok(format!("{base}{suffix}"))
}

/// Whether a column of this type is bound as a JSON/text parameter that has to
/// be cast back into the destination type inside the SQL (because the `duckdb`
/// crate can't bind it natively).
fn needs_json_cast(type_: &Type) -> bool {
    matches!(
        type_.unoptionalize(),
        Type::Json | Type::List(_) | Type::Array(_, _) | Type::Tuple(_)
    )
}

/// Whether a value of this type can be encoded into the JSON representation that
/// list/array/tuple/JSON columns are bound as. Mirrors exactly the type arms
/// [`value_to_json`] knows how to encode: anything it would reject at write time
/// must be rejected here at construction time instead. `numpy` arrays only ever
/// hold integers or floats, so an [`Type::Array`] element of any other concrete
/// type can never be produced.
fn is_json_encodable(type_: &Type) -> bool {
    match type_ {
        Type::Bool | Type::Int | Type::Float | Type::String | Type::Pointer | Type::Json => true,
        Type::Optional(inner) | Type::List(inner) => is_json_encodable(inner),
        Type::Array(_, wrapped) => matches!(wrapped.as_ref(), Type::Int | Type::Float | Type::Any),
        Type::Tuple(args) => args.iter().all(is_json_encodable),
        _ => false,
    }
}

/// The placeholder fragment emitted for a value of `type_` in a `VALUES (...)`
/// list or a `WHERE col = ...` comparison. Scalars bind directly as `?`; list,
/// array, tuple and JSON values are bound as a JSON string and cast back into
/// the destination type.
fn placeholder_for(type_: &Type) -> Result<String, WriteError> {
    if needs_json_cast(type_) {
        let cast_type = duckdb_data_type(type_, true)?;
        Ok(format!("CAST(CAST(? AS JSON) AS {cast_type})"))
    } else {
        Ok("?".to_string())
    }
}

/// Validate that an array value's dimensionality matches the destination column.
/// A column typed only as `np.ndarray` (`Array(None, _)`) is created as a 1-D
/// `DOUBLE[]` list; an `Array(Some(n), _)` column is `n`-dimensional. A value
/// whose shape has a different number of dimensions cannot be cast into the
/// column, so reject it with a clear error instead of an opaque `DuckDB` cast
/// failure mid-write. Non-array fields/values are unaffected.
fn check_array_dimensionality(value: &Value, field: &ValueField) -> Result<(), WriteError> {
    let Type::Array(n_dim, _) = field.type_.unoptionalize() else {
        return Ok(());
    };
    let actual = match value {
        Value::IntArray(a) => a.shape().len(),
        Value::FloatArray(a) => a.shape().len(),
        _ => return Ok(()),
    };
    let expected = n_dim.unwrap_or(1);
    if actual != expected {
        return Err(DuckDbError::ArrayDimensionalityMismatch {
            column: field.name.clone(),
            expected,
            actual,
        }
        .into());
    }
    Ok(())
}

/// Encode a single Pathway [`Value`] into a `DuckDB` parameter. List/array/tuple
/// and JSON values become a JSON string (the surrounding `CAST(... AS JSON)`
/// from [`placeholder_for`] turns them back into the destination type).
fn encode_value(value: &Value) -> Result<DuckValue, WriteError> {
    Ok(match value {
        Value::None => DuckValue::Null,
        Value::Bool(b) => DuckValue::Boolean(*b),
        Value::Int(i) => DuckValue::BigInt(*i),
        Value::Float(f) => DuckValue::Double(f.into_inner()),
        Value::String(s) => DuckValue::Text(s.to_string()),
        Value::Pointer(p) => DuckValue::Text(p.to_string()),
        Value::Bytes(b) => DuckValue::Blob(b.to_vec()),
        // Pathway stores both naive and UTC datetimes as nanoseconds since the
        // epoch; DuckDB's default TIMESTAMP is microsecond resolution.
        Value::DateTimeNaive(dt) => {
            DuckValue::Timestamp(TimeUnit::Microsecond, dt.timestamp() / 1000)
        }
        Value::DateTimeUtc(dt) => {
            DuckValue::Timestamp(TimeUnit::Microsecond, dt.timestamp() / 1000)
        }
        Value::Duration(d) => DuckValue::Interval {
            months: 0,
            days: 0,
            nanos: d.nanoseconds(),
        },
        Value::Json(j) => DuckValue::Text(j.to_string()),
        Value::Tuple(_) | Value::IntArray(_) | Value::FloatArray(_) => DuckValue::Text(
            serde_json::to_string(&value_to_json(value)?).expect("JSON is serializable"),
        ),
        Value::PyObjectWrapper(_) => DuckValue::Blob(create_bincoded_value(value)?.into_bytes()),
        Value::Error | Value::Pending => return Err(WriteError::UnsupportedType(Type::Any)),
    })
}

/// Encode an `f64` into a JSON value the `DuckDB` `... AS DOUBLE[]` cast accepts.
/// `serde_json` cannot represent `NaN`/`±Infinity` as JSON numbers and silently
/// turns them into `null`, which would corrupt a stored embedding. `DuckDB`'s
/// `DOUBLE` represents these natively and casts the string forms `"NaN"`,
/// `"Infinity"`, `"-Infinity"` back into them, so non-finite values are emitted
/// as those strings instead.
fn float_to_json(f: f64) -> JsonValue {
    if f.is_finite() {
        json!(f)
    } else if f.is_nan() {
        json!("NaN")
    } else if f > 0.0 {
        json!("Infinity")
    } else {
        json!("-Infinity")
    }
}

/// Convert a Pathway list-like [`Value`] into a plain (possibly nested) JSON
/// array that `DuckDB` casts into a list column. `numpy` arrays are reshaped back
/// into nested arrays according to their stored shape.
fn value_to_json(value: &Value) -> Result<JsonValue, WriteError> {
    Ok(match value {
        Value::None => JsonValue::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::Float(f) => float_to_json(f.into_inner()),
        Value::String(s) => json!(s.as_str()),
        Value::Pointer(p) => json!(p.to_string()),
        Value::Tuple(items) => {
            let mut array = Vec::with_capacity(items.len());
            for item in items.iter() {
                array.push(value_to_json(item)?);
            }
            JsonValue::Array(array)
        }
        Value::IntArray(a) => {
            let flat: Vec<JsonValue> = a.iter().map(|item| json!(item)).collect();
            nest_by_shape(a.shape(), &flat, &mut 0)
        }
        Value::FloatArray(a) => {
            let flat: Vec<JsonValue> = a.iter().map(|item| float_to_json(*item)).collect();
            nest_by_shape(a.shape(), &flat, &mut 0)
        }
        Value::Json(j) => (**j).clone(),
        _ => return Err(WriteError::UnsupportedType(Type::Any)),
    })
}

/// Reshape a flat sequence of JSON values into nested arrays following `shape`,
/// consuming elements from `offset`. A scalar (empty `shape`) returns the next
/// element directly, so a 1-D array yields `[e0, e1, ...]`.
fn nest_by_shape(shape: &[usize], flat: &[JsonValue], offset: &mut usize) -> JsonValue {
    if shape.is_empty() {
        let value = flat[*offset].clone();
        *offset += 1;
        return value;
    }
    let mut array = Vec::with_capacity(shape[0]);
    for _ in 0..shape[0] {
        array.push(nest_by_shape(&shape[1..], flat, offset));
    }
    JsonValue::Array(array)
}

/// Run a one-parameter query and collect its single text column.
fn query_single_column(
    connection: &DuckConnection,
    sql: &str,
    param: &str,
) -> Result<Vec<String>, WriteError> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([param], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Build the SQL plan (`INSERT`/`UPSERT`, and for snapshot mode `DELETE`) for
/// the writer. Placeholders are type-aware, so list and JSON columns are cast
/// from a bound JSON string back into their destination type.
fn build_plan(
    snapshot_mode: bool,
    quoted_table_name: &str,
    value_fields: &[ValueField],
    key_field_names: Option<&[String]>,
) -> Result<WritePlan, WriteError> {
    // Reject up front any list/array/tuple/JSON column whose element type
    // `value_to_json` cannot encode. Otherwise the CREATE TABLE and every
    // preflight succeed, and the first write fails mid-stream with an opaque
    // "unsupported type" error that names neither the column nor the type.
    for field in value_fields {
        if needs_json_cast(&field.type_) && !is_json_encodable(&field.type_) {
            return Err(DuckDbError::UnsupportedListElementType {
                column: field.name.clone(),
                type_: format!("{:?}", field.type_),
            }
            .into());
        }
    }

    let column_list = value_fields
        .iter()
        .map(|field| quote_duckdb_identifier(&field.name))
        .join(", ");
    let value_placeholders = value_fields
        .iter()
        .map(|field| placeholder_for(&field.type_))
        .collect::<Result<Vec<_>, _>>()?;

    if snapshot_mode {
        let key_field_names = key_field_names
            .filter(|keys| !keys.is_empty())
            .ok_or(WriteError::EmptyKeyFieldsForSnapshot)?;
        let primary_key_indices =
            SqlQueryTemplate::primary_key_indices(value_fields, key_field_names)?;

        let key_set: std::collections::HashSet<&str> =
            key_field_names.iter().map(String::as_str).collect();
        let update_pairs = value_fields
            .iter()
            .filter(|field| !key_set.contains(field.name.as_str()))
            .map(|field| {
                let quoted = quote_duckdb_identifier(&field.name);
                format!("{quoted}=excluded.{quoted}")
            })
            .collect::<Vec<_>>();
        let conflict_target = key_field_names
            .iter()
            .map(|name| quote_duckdb_identifier(name))
            .join(", ");
        let on_conflict = if update_pairs.is_empty() {
            format!("ON CONFLICT ({conflict_target}) DO NOTHING")
        } else {
            format!(
                "ON CONFLICT ({conflict_target}) DO UPDATE SET {}",
                update_pairs.join(", ")
            )
        };
        let insertion_query = format!(
            "INSERT INTO {quoted_table_name} ({column_list}) VALUES ({}) {on_conflict}",
            value_placeholders.join(", ")
        );

        // DELETE compares each key column in ascending-index order, matching the
        // order `select_primary_key` projects the bound parameters into.
        let where_tokens = primary_key_indices
            .iter()
            .map(|&index| {
                let field = &value_fields[index];
                placeholder_for(&field.type_).map(|placeholder| {
                    format!("{}={placeholder}", quote_duckdb_identifier(&field.name))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let deletion_query = format!(
            "DELETE FROM {quoted_table_name} WHERE {}",
            where_tokens.join(" AND ")
        );

        Ok(WritePlan::Snapshot {
            insertion: insertion_query,
            deletion: deletion_query,
            primary_key_indices,
        })
    } else {
        // A single `(ph1, …, ?, ?)` value tuple; the flush repeats it once per
        // buffered row to form a multi-row INSERT.
        let row_tuple = format!("({}, ?, ?)", value_placeholders.join(", "));
        let insert_prefix =
            format!("INSERT INTO {quoted_table_name} ({column_list}, time, diff) VALUES ");
        Ok(WritePlan::StreamOfChanges {
            insert_prefix,
            row_tuple,
            row_width: value_fields.len() + 2,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::{
        duckdb_data_type, needs_json_cast, nest_by_shape, placeholder_for, select_primary_key,
        value_to_json,
    };
    use crate::engine::{Type, Value};

    #[test]
    fn data_type_scalars_are_not_null_at_top_level() {
        assert_eq!(
            duckdb_data_type(&Type::Int, false).unwrap(),
            "BIGINT NOT NULL"
        );
        assert_eq!(
            duckdb_data_type(&Type::Float, false).unwrap(),
            "DOUBLE NOT NULL"
        );
        assert_eq!(
            duckdb_data_type(&Type::String, false).unwrap(),
            "VARCHAR NOT NULL"
        );
        assert_eq!(
            duckdb_data_type(&Type::Bool, false).unwrap(),
            "BOOLEAN NOT NULL"
        );
        assert_eq!(
            duckdb_data_type(&Type::DateTimeUtc, false).unwrap(),
            "TIMESTAMP NOT NULL"
        );
        assert_eq!(
            duckdb_data_type(&Type::Duration, false).unwrap(),
            "INTERVAL NOT NULL"
        );
        assert_eq!(
            duckdb_data_type(&Type::Json, false).unwrap(),
            "JSON NOT NULL"
        );
    }

    #[test]
    fn optional_drops_the_not_null_constraint() {
        let optional_int = Type::Optional(Arc::new(Type::Int));
        assert_eq!(duckdb_data_type(&optional_int, false).unwrap(), "BIGINT");
    }

    #[test]
    fn data_type_lists_and_arrays_become_duckdb_lists() {
        let list_float = Type::List(Arc::new(Type::Float));
        assert_eq!(
            duckdb_data_type(&list_float, false).unwrap(),
            "DOUBLE[] NOT NULL"
        );
        let array_2d = Type::Array(Some(2), Arc::new(Type::Float));
        assert_eq!(
            duckdb_data_type(&array_2d, false).unwrap(),
            "DOUBLE[][] NOT NULL"
        );
        // An untyped array defaults to a DOUBLE list (embedding-friendly).
        let array_any = Type::Array(None, Arc::new(Type::Any));
        assert_eq!(
            duckdb_data_type(&array_any, false).unwrap(),
            "DOUBLE[] NOT NULL"
        );
    }

    #[test]
    fn homogeneous_tuple_is_a_list_heterogeneous_is_json() {
        let homo = Type::Tuple(Arc::from(vec![Type::Int, Type::Int]));
        assert_eq!(duckdb_data_type(&homo, false).unwrap(), "BIGINT[] NOT NULL");
        let hetero = Type::Tuple(Arc::from(vec![Type::Int, Type::String]));
        assert_eq!(duckdb_data_type(&hetero, false).unwrap(), "JSON NOT NULL");
    }

    #[test]
    fn placeholders_are_type_aware() {
        assert_eq!(placeholder_for(&Type::Int).unwrap(), "?");
        assert_eq!(placeholder_for(&Type::String).unwrap(), "?");
        assert!(!needs_json_cast(&Type::Int));

        let list_float = Type::List(Arc::new(Type::Float));
        assert!(needs_json_cast(&list_float));
        assert_eq!(
            placeholder_for(&list_float).unwrap(),
            "CAST(CAST(? AS JSON) AS DOUBLE[])"
        );

        // The cast survives an Optional wrapper.
        let optional_list = Type::Optional(Arc::new(Type::List(Arc::new(Type::Float))));
        assert_eq!(
            placeholder_for(&optional_list).unwrap(),
            "CAST(CAST(? AS JSON) AS DOUBLE[])"
        );
    }

    #[test]
    fn nest_by_shape_reshapes_flat_elements() {
        let flat = vec![json!(1), json!(2), json!(3)];
        assert_eq!(nest_by_shape(&[3], &flat, &mut 0), json!([1, 2, 3]));
        let flat = vec![json!(1), json!(2), json!(3), json!(4)];
        assert_eq!(
            nest_by_shape(&[2, 2], &flat, &mut 0),
            json!([[1, 2], [3, 4]])
        );
    }

    #[test]
    fn select_primary_key_projects_key_columns_in_order() {
        let fields = vec![10, 20, 30, 40];
        // Keys at positions 1 and 3 -> values 20 and 40.
        assert_eq!(select_primary_key(fields, &[1, 3]), vec![20, 40]);
    }

    #[test]
    fn value_to_json_handles_scalars_and_tuples() {
        assert_eq!(value_to_json(&Value::Int(5)).unwrap(), json!(5));
        let tuple = Value::Tuple(Arc::from(vec![Value::Int(1), Value::Int(2)]));
        assert_eq!(value_to_json(&tuple).unwrap(), json!([1, 2]));
    }

    #[test]
    fn non_finite_floats_become_duckdb_castable_strings() {
        use super::float_to_json;
        // Finite floats stay JSON numbers.
        assert_eq!(float_to_json(1.5), json!(1.5));
        // serde_json cannot represent these as numbers; DuckDB casts the string
        // forms back into native nan/inf, so they must not collapse to null.
        assert_eq!(float_to_json(f64::NAN), json!("NaN"));
        assert_eq!(float_to_json(f64::INFINITY), json!("Infinity"));
        assert_eq!(float_to_json(f64::NEG_INFINITY), json!("-Infinity"));
    }
}

// Copyright © 2026 Pathway

use log::info;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::mem::take;

use chrono::Timelike;
use hex;
use tiberius::{Client, ColumnData, Config, Query};
use tokio::net::TcpStream;
use tokio::runtime::Runtime as TokioRuntime;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::{
    CommitPossibility, ConnectorMode, ConversionError, SqlQueryTemplate, TableContext,
    TableWriterInitMode, ValuesMap,
};
use crate::connectors::metadata::MssqlMetadata;
use crate::connectors::offset::{Offset, OffsetKey, OffsetValue, EMPTY_OFFSET};
use crate::connectors::{DataEventType, ReadError, ReadResult, Reader, ReaderContext, StorageType};
use crate::connectors::{WriteError, Writer};
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::time::DateTime as DateTimeTrait;
use crate::engine::{Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries_if, RetryConfig};

const MAX_MSSQL_RETRIES: usize = 3;

/// SQL Server error 1205: transaction chosen as deadlock victim.  Microsoft's
/// documented contract is "rerun the transaction"; the read/write paths here
/// hit it under heavy parallelism because every CREATE/DROP fires
/// `sys.sp_cdc_ddl_event_internal` (database-level CDC trigger) and every
/// `cdc.fn_cdc_get_all_changes_*` query competes with the capture agent for
/// the same metadata locks.
const MSSQL_ERR_DEADLOCK_VICTIM: u32 = 1205;

/// Classify an MSSQL error as transient: only deadlock-victim (1205) and the
/// kernel-level ephemeral-port exhaustion that surfaces inside tiberius's
/// `connect(2)` as `EADDRNOTAVAIL` ("Cannot assign requested address").
/// Everything else — auth failures, missing tables, syntax errors, persisted
/// LSN out of retention — is permanent and propagates on the first attempt
/// so callers see real bugs without backoff latency.
fn is_transient_mssql(err: &MssqlError) -> bool {
    match err {
        MssqlError::ConnectionFailed { reason } => {
            reason.contains("Cannot assign requested address")
        }
        MssqlError::Driver(tiberius::error::Error::Server(token)) => {
            token.code() == MSSQL_ERR_DEADLOCK_VICTIM
        }
        _ => false,
    }
}

fn is_transient_read(err: &ReadError) -> bool {
    matches!(err, ReadError::Mssql(m) if is_transient_mssql(m))
}

fn is_transient_write(err: &WriteError) -> bool {
    matches!(err, WriteError::Mssql(m) if is_transient_mssql(m))
}

type MssqlClient = Client<Compat<TcpStream>>;

/// Errors specific to MSSQL connector operations.
///
/// A dedicated enum surfaces actionable messages for the most common failure
/// modes that users encounter, while delegating everything else to the tiberius
/// driver error.
#[derive(Debug, thiserror::Error)]
pub enum MssqlError {
    /// General tiberius driver or SQL Server error not covered by a more specific variant.
    #[error(transparent)]
    Driver(#[from] tiberius::error::Error),

    /// TCP connection to the SQL Server host failed.
    ///
    /// Check that the `Server` address and port in the connection string are correct
    /// and that the server is running and reachable from this host.
    #[error(
        "failed to connect to SQL Server: {reason}; \
         verify the Server address and port in the connection string"
    )]
    ConnectionFailed { reason: String },

    /// SQL Server rejected the login credentials (SQL Server error 18456).
    ///
    /// Verify the `User Id` and `Password` fields in the connection string.
    #[error(
        "SQL Server authentication failed: {reason}; \
         verify the User Id and Password in the connection string"
    )]
    AuthenticationFailed { reason: String },

    /// The requested database does not exist or the login has no access to it
    /// (SQL Server error 4060).
    ///
    /// Verify the `Database` field in the connection string and that the user has
    /// been granted access to that database.
    #[error(
        "cannot open SQL Server database: {reason}; \
         verify the Database name in the connection string and that the user has access"
    )]
    DatabaseNotFound { reason: String },

    /// The target table does not exist in the database (SQL Server error 208).
    ///
    /// Use `init_mode="create_if_not_exists"` to create the table automatically,
    /// or check that `table_name` and `schema_name` are spelled correctly.
    #[error(
        "table not found in the SQL Server database: {reason}; \
         use init_mode=\"create_if_not_exists\" to create it automatically"
    )]
    TableNotFound { reason: String },

    /// CDC is not enabled at the database level.
    ///
    /// Enable it with: `EXEC sys.sp_cdc_enable_db`
    #[error(
        "CDC is not enabled on the current database; \
         enable it with: EXEC sys.sp_cdc_enable_db"
    )]
    CdcNotEnabledOnDatabase,

    /// CDC is not enabled on the source table.
    ///
    /// Enable it with:
    /// `EXEC sys.sp_cdc_enable_table @source_schema=N'<schema>', @source_name=N'<table>', @role_name=NULL`
    #[error(
        "CDC is not enabled on table '{schema}.{table}'; enable it with: \
         EXEC sys.sp_cdc_enable_table \
         @source_schema=N'{schema}', @source_name=N'{table}', @role_name=NULL"
    )]
    CdcNotEnabledOnTable { schema: String, table: String },

    /// Reader's Pathway schema declares columns that don't exist on the
    /// source table (static mode) or aren't captured by the CDC capture
    /// instance (streaming mode).  Without this preflight the SELECT
    /// against the source or `cdc.fn_cdc_get_all_changes_*` would fail
    /// per-row with the generic SQL Server "invalid column name" error
    /// many seconds into the run.
    #[error(
        "schema column(s) {missing:?} declared by pw.io.mssql.read are missing \
         from {origin} for table '{schema}.{table}'; rename the schema columns \
         to match the source, or {hint}"
    )]
    SchemaColumnsMissing {
        schema: String,
        table: String,
        missing: Vec<String>,
        origin: &'static str,
        hint: &'static str,
    },

    /// The Pathway schema declares an `Optional[T]` column whose
    /// destination counterpart is `NOT NULL`.  Pathway can emit `None`
    /// for that column, which the writer would bind as SQL `NULL` —
    /// SQL Server then rejects every such row with a NULL-constraint
    /// violation.  Either drop the `Optional` from the Pathway schema
    /// (only do this if the source data is guaranteed to never be
    /// missing) or alter the destination to make the column nullable.
    #[error(
        "Pathway schema declares column(s) {columns:?} as `Optional` but the \
         destination table '{schema}.{table}' has them as NOT NULL; INSERT \
         statements that supply `None` would fail with a NULL-constraint violation."
    )]
    OptionalIntoNotNullDestination {
        schema: String,
        table: String,
        columns: Vec<String>,
    },

    /// The destination table has columns that are `NOT NULL` without a
    /// default, are not declared `IDENTITY` and are not computed — so SQL
    /// Server requires every INSERT to supply a value for them — but the
    /// Pathway schema doesn't include them.  Every flush would fail with
    /// "Cannot insert the value NULL into column" (error 515).  Add the
    /// columns to the Pathway schema, or alter the destination to make
    /// them nullable / give them a default.
    #[error(
        "destination table '{schema}.{table}' has required column(s) {columns:?} \
         (NOT NULL, no default, not IDENTITY/computed) that are missing from the \
         Pathway schema; every INSERT would fail with a NULL-constraint violation. \
         Add them to the Pathway schema, or ALTER the destination to make them \
         nullable or give them a DEFAULT."
    )]
    DestinationRequiredColumnsMissing {
        schema: String,
        table: String,
        columns: Vec<String>,
    },

    /// One of the writer's value-field columns is a `COMPUTED` column on
    /// the destination table.  SQL Server forbids INSERT statements that supply
    /// a value for a computed column (error 271), so every flush would
    /// fail.  Drop the column from the Pathway schema (computed columns
    /// are auto-derived) or use `init_mode="replace"` to recreate the
    /// destination without computed columns.
    #[error(
        "destination column(s) {columns:?} on table '{schema}.{table}' are \
         declared as COMPUTED in SQL Server; pw.io.mssql.write cannot supply \
         values for computed columns. Drop these columns from the Pathway \
         schema, or use init_mode=\"replace\" against a fresh table."
    )]
    ComputedColumnInSchema {
        schema: String,
        table: String,
        columns: Vec<String>,
    },

    /// One of the writer's value-field columns is an `IDENTITY` column on
    /// the destination table.  SQL Server forbids INSERT statements that supply
    /// an explicit value for an IDENTITY column unless `SET IDENTITY_INSERT
    /// ON` is in effect (which Pathway does not issue), so every flush
    /// would fail with error 8101.  Either remove the column from the
    /// Pathway schema (and let SQL Server auto-generate it) or use
    /// `init_mode="replace"` to recreate the destination without IDENTITY.
    #[error(
        "destination column(s) {columns:?} on table '{schema}.{table}' are \
         declared as IDENTITY in SQL Server; pw.io.mssql.write cannot supply \
         explicit values for IDENTITY columns. Drop these columns from the \
         Pathway schema, or use init_mode=\"replace\" against a fresh table."
    )]
    IdentityColumnInSchema {
        schema: String,
        table: String,
        columns: Vec<String>,
    },

    /// Writer's Pathway schema declares columns that don't exist on the
    /// destination table.  Without this preflight the first INSERT/MERGE
    /// would fail with the generic "Invalid column name" SQL Server error
    /// many seconds into the run.  For `stream_of_changes` mode the
    /// auto-appended `time` / `diff` metadata columns are checked too —
    /// `init_mode="default"` against a destination created without them
    /// would otherwise fail every flush.
    #[error(
        "destination column(s) {missing:?} required by pw.io.mssql.write are missing \
         from table '{schema}.{table}'; rename the schema columns to match the \
         destination, ALTER TABLE the destination to add them, or use \
         init_mode=\"create_if_not_exists\" / init_mode=\"replace\" against a fresh table"
    )]
    DestinationColumnsMissing {
        schema: String,
        table: String,
        missing: Vec<String>,
    },

    /// Snapshot-mode writer's `primary_key` columns don't match any unique
    /// index on the destination table.  SQL Server's MERGE matches rows on
    /// `target.k = source.k` but does not require a unique constraint —
    /// without one, MERGE would silently match (and update/insert) the
    /// wrong rows.  Add a `PRIMARY KEY` or `UNIQUE` constraint on the
    /// configured primary-key columns, or use `init_mode="replace"` /
    /// `init_mode="create_if_not_exists"` against a fresh table.
    #[error(
        "snapshot-mode primary_key {expected:?} on table '{schema}.{table}' \
         has no matching unique index in the destination (found unique \
         indexes: {actual:?}); MERGE would silently upsert the wrong rows"
    )]
    SnapshotPkMismatch {
        schema: String,
        table: String,
        expected: Vec<String>,
        actual: Vec<Vec<String>>,
    },

    /// More than one CDC capture instance is registered against the source
    /// table.  SQL Server allows up to two capture instances per table — used
    /// when migrating a capture-instance schema — and there is no way for the
    /// connector to choose between them deterministically.  Disable one of
    /// the capture instances with `sp_cdc_disable_table @capture_instance=…`
    /// or contact us if you need a knob to select one explicitly.
    #[error(
        "table '{schema}.{table}' has multiple CDC capture instances ({instances:?}); \
         disable all but one with EXEC sys.sp_cdc_disable_table \
         @source_schema=N'{schema}', @source_name=N'{table}', @capture_instance=N'<name>'"
    )]
    AmbiguousCaptureInstance {
        schema: String,
        table: String,
        instances: Vec<String>,
    },

    /// The persisted CDC LSN is older than the retention window of the capture
    /// instance.  SQL Server's CDC cleanup job drops rows from `cdc.*_CT` once
    /// they are older than the configured retention (default 4320 minutes),
    /// independently of any consumer.  When the engine tries to resume from a
    /// saved LSN that has already been purged, SQL Server raises error 313.
    /// To recover, delete the persistence directory and restart — the connector
    /// will then re-snapshot the source table from scratch.
    #[error(
        "the persisted CDC position is outside the SQL Server retention window; \
         delete the persistence directory and restart to trigger a full table rescan"
    )]
    CdcLsnOutOfRetention,
}

/// Wrap a SQL Server identifier in brackets, doubling any embedded `]`
/// per SQL Server's standard escape rule.  Without doubling, a name
/// containing `]` would terminate the bracket-quoted literal early and the
/// surrounding statement would be parsed as malformed SQL.
fn quote_identifier(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Wrap a string for use inside an `N'...'` Unicode string literal,
/// doubling any embedded `'` per SQL Server's standard escape rule.
fn n_string_literal(s: &str) -> String {
    format!("N'{}'", s.replace('\'', "''"))
}

/// Build a schema-qualified table name using MSSQL bracket quoting.
fn qualified_table_name(schema_name: &str, table_name: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name),
    )
}

async fn connect_mssql(config: &Config) -> Result<MssqlClient, tiberius::error::Error> {
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let client = Client::connect(config.clone(), tcp.compat_write()).await?;
    Ok(client)
}

async fn detect_server_version(
    client: &mut MssqlClient,
) -> Result<(String, String), tiberius::error::Error> {
    let row = client
        .simple_query(
            "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)), \
             CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128))",
        )
        .await?
        .into_row()
        .await?;
    if let Some(row) = row {
        let version: Option<&str> = row.get(0);
        let edition: Option<&str> = row.get(1);
        Ok((
            version.unwrap_or("unknown").to_string(),
            edition.unwrap_or("unknown").to_string(),
        ))
    } else {
        Ok(("unknown".to_string(), "unknown".to_string()))
    }
}

/// Unified MSSQL reader for both static and streaming (CDC) modes.
///
/// In `ConnectorMode::Static` the connector reads the table once and emits
/// `ReadResult::Finished`.  In `ConnectorMode::Streaming` it reads a snapshot
/// of the table on first call to `read()`, then continuously polls CDC change
/// tables for new inserts, updates, and deletes.
pub struct MssqlReader {
    runtime: TokioRuntime,
    config: Config,
    schema_name: String,
    table_name: String,
    schema: Vec<(String, Type)>,
    key_column_names: Vec<String>,
    mode: ConnectorMode,
    snapshot: Vec<ReadResult>,
    is_initialized: bool,
    client: Option<MssqlClient>,
    capture_instance: String,
    /// The capture instance's original `start_lsn` from `cdc.change_tables`,
    /// fixed for the lifetime of the instance.  CDC cleanup advances
    /// `fn_cdc_get_min_lsn(...)` but never `start_lsn`, so the gap between
    /// the two tells us exactly how many LSNs cleanup has purged — and lets
    /// `fetch_cdc_rows_in_range` tell apart "the instance was just created
    /// and never had rows below `min_lsn`" (clamp is safe) from "cleanup
    /// raced past us and rows we needed have been purged" (must error).
    capture_start_lsn: Vec<u8>,
    last_lsn: Option<Vec<u8>>,
    snapshot_version: u64,
    /// True when CDC is enabled on the target table and the connector is
    /// therefore able to track LSNs.  In streaming mode this is always `true`
    /// once `initialize` succeeds (CDC is mandatory).  In static mode it
    /// reflects whether CDC happens to be enabled: when it is, snapshot rows
    /// are stamped with an LSN so persistence can resume from there; when it
    /// is not, rows carry `EMPTY_OFFSET`.  `seek` forces this to `true` (or
    /// errors) when persistence is configured — the engine only calls `seek`
    /// in that case.
    tracks_lsn: bool,
    /// Persisted CDC LSN from a previous run, populated by `seek()`.  When set,
    /// `initialize()` skips the full table snapshot and instead replays CDC
    /// changes in (`saved_last_lsn`, current max LSN] as one block.
    saved_last_lsn: Option<Vec<u8>>,
    /// `true` once `seek()` has run, which the engine only does when the
    /// pipeline has a `persistence_config` attached.  Used by
    /// `check_schema_columns_exist` to decide whether the connector will
    /// ever read from the CDC change tables (so the schema must match
    /// `cdc.captured_columns`) or only from the source table (so matching
    /// `sys.columns` is enough).
    persistence_enabled: bool,
}

impl MssqlReader {
    pub fn new(
        config: Config,
        schema_name: String,
        table_name: String,
        schema: Vec<(String, Type)>,
        key_field_names: Option<Vec<String>>,
        mode: ConnectorMode,
    ) -> Result<Self, ReadError> {
        let key_column_names = key_field_names
            .unwrap_or_else(|| schema.iter().map(|(name, _)| name.clone()).collect());
        Ok(Self {
            runtime: create_async_tokio_runtime()?,
            config,
            schema_name,
            table_name,
            schema,
            key_column_names,
            mode,
            snapshot: Vec::new(),
            is_initialized: false,
            client: None,
            capture_instance: String::new(),
            capture_start_lsn: Vec::new(),
            last_lsn: None,
            snapshot_version: 0,
            tracks_lsn: false,
            saved_last_lsn: None,
            persistence_enabled: false,
        })
    }

    /// Check if a tiberius `ColumnData` represents a SQL NULL value.
    fn is_column_null(col_data: &ColumnData<'_>) -> bool {
        matches!(
            col_data,
            ColumnData::Bit(None)
                | ColumnData::U8(None)
                | ColumnData::I16(None)
                | ColumnData::I32(None)
                | ColumnData::I64(None)
                | ColumnData::F32(None)
                | ColumnData::F64(None)
                | ColumnData::String(None)
                | ColumnData::Binary(None)
                | ColumnData::Numeric(None)
                | ColumnData::DateTime(None)
                | ColumnData::DateTime2(None)
                | ColumnData::SmallDateTime(None)
                | ColumnData::DateTimeOffset(None)
                | ColumnData::Guid(None)
                | ColumnData::Xml(None)
                | ColumnData::Date(None)
                | ColumnData::Time(None)
        )
    }

    /// Convert a single row value at `col_idx` to a Pathway `Value`.
    ///
    /// Uses `row.get()` for datetime types (leveraging tiberius's built-in chrono
    /// `FromSql` implementations) and raw `ColumnData` matching for all other types.
    #[allow(clippy::too_many_lines)]
    fn convert_row_value(
        row: &tiberius::Row,
        col_idx: usize,
        field_name: &str,
        dtype: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        // Handle datetime types via row.get() which uses tiberius's FromSql
        let inner_dtype = match dtype {
            Type::Optional(inner) => inner.as_ref(),
            other => other,
        };
        let is_optional = matches!(dtype, Type::Optional(_));

        match inner_dtype {
            Type::DateTimeNaive | Type::Any
                if matches!(
                    row.cells().nth(col_idx).map(|(_, cd)| cd),
                    Some(
                        ColumnData::DateTime(_)
                            | ColumnData::DateTime2(_)
                            | ColumnData::SmallDateTime(_)
                            | ColumnData::Date(_)
                    )
                ) =>
            {
                // For DATE columns, get NaiveDate and convert to NaiveDateTime at midnight
                let ndt: Option<chrono::NaiveDateTime> = if matches!(
                    row.cells().nth(col_idx).map(|(_, cd)| cd),
                    Some(ColumnData::Date(_))
                ) {
                    row.get::<chrono::NaiveDate, _>(col_idx)
                        .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight is always valid"))
                } else {
                    row.get(col_idx)
                };
                match ndt {
                    Some(ndt) => crate::engine::DateTimeNaive::try_from(ndt)
                        .map(Value::DateTimeNaive)
                        .map_err(|_| {
                            Box::new(ConversionError::new(
                                format!("{ndt:?}"),
                                field_name.to_owned(),
                                dtype.clone(),
                                None,
                            ))
                        }),
                    None if is_optional => Ok(Value::None),
                    None => Err(Box::new(ConversionError::new(
                        "NULL".to_string(),
                        field_name.to_owned(),
                        dtype.clone(),
                        None,
                    ))),
                }
            }
            Type::DateTimeUtc | Type::Any
                if matches!(
                    row.cells().nth(col_idx).map(|(_, cd)| cd),
                    Some(ColumnData::DateTimeOffset(_))
                ) =>
            {
                let dt: Option<chrono::DateTime<chrono::Utc>> = row.get(col_idx);
                match dt {
                    Some(dt) => crate::engine::DateTimeUtc::try_from(dt)
                        .map(Value::DateTimeUtc)
                        .map_err(|_| {
                            Box::new(ConversionError::new(
                                format!("{dt:?}"),
                                field_name.to_owned(),
                                dtype.clone(),
                                None,
                            ))
                        }),
                    None if is_optional => Ok(Value::None),
                    None => Err(Box::new(ConversionError::new(
                        "NULL".to_string(),
                        field_name.to_owned(),
                        dtype.clone(),
                        None,
                    ))),
                }
            }
            Type::Duration | Type::Any
                if matches!(
                    row.cells().nth(col_idx).map(|(_, cd)| cd),
                    Some(ColumnData::Time(_))
                ) =>
            {
                let nt: Option<chrono::NaiveTime> = row.get(col_idx);
                match nt {
                    Some(nt) => {
                        let total_usecs = i64::from(nt.hour()) * 3_600_000_000
                            + i64::from(nt.minute()) * 60_000_000
                            + i64::from(nt.second()) * 1_000_000
                            + i64::from(nt.nanosecond() / 1_000);
                        crate::engine::Duration::new_with_unit(total_usecs, "us")
                            .map(Value::Duration)
                            .map_err(|_| {
                                Box::new(ConversionError::new(
                                    format!("{nt:?}"),
                                    field_name.to_owned(),
                                    dtype.clone(),
                                    None,
                                ))
                            })
                    }
                    None if is_optional => Ok(Value::None),
                    None => Err(Box::new(ConversionError::new(
                        "NULL".to_string(),
                        field_name.to_owned(),
                        dtype.clone(),
                        None,
                    ))),
                }
            }
            _ => {
                // For non-datetime types, use ColumnData matching
                let col_data = row
                    .cells()
                    .nth(col_idx)
                    .map(|(_, cd)| cd)
                    .expect("column index out of bounds");
                Self::convert_column_data(col_data, field_name, dtype)
            }
        }
    }

    fn convert_column_data(
        col_data: &ColumnData<'_>,
        field_name: &str,
        dtype: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let value = match (dtype, col_data) {
            (Type::Optional(_) | Type::Any, col) if Self::is_column_null(col) => Some(Value::None),
            (Type::Optional(inner), other) => {
                Self::convert_column_data(other, field_name, inner).ok()
            }
            (Type::Bool | Type::Any, ColumnData::Bit(Some(v))) => Some(Value::Bool(*v)),
            (Type::Int | Type::Any, ColumnData::U8(Some(v))) => Some(Value::Int(i64::from(*v))),
            (Type::Int | Type::Any, ColumnData::I16(Some(v))) => Some(Value::Int(i64::from(*v))),
            (Type::Int | Type::Any, ColumnData::I32(Some(v))) => Some(Value::Int(i64::from(*v))),
            (Type::Int | Type::Any, ColumnData::I64(Some(v))) => Some(Value::Int(*v)),
            (Type::Float | Type::Any, ColumnData::F32(Some(v))) => {
                Some(Value::Float(f64::from(*v).into()))
            }
            (Type::Float | Type::Any, ColumnData::F64(Some(v))) => Some(Value::Float((*v).into())),
            // Allow `Type::Float` schemas to read SQL Server INT-family
            // columns: TINYINT/SMALLINT/INT round-trip exactly through
            // f64; BIGINT loses precision above 2**53 but matches IEEE 754
            // semantics.  `Type::Any` is intentionally not included here —
            // the existing `Type::Int | Type::Any` patterns above already
            // handle integer columns when no specific type is requested.
            (Type::Float, ColumnData::U8(Some(v))) => Some(Value::Float(f64::from(*v).into())),
            (Type::Float, ColumnData::I16(Some(v))) => Some(Value::Float(f64::from(*v).into())),
            (Type::Float, ColumnData::I32(Some(v))) => Some(Value::Float(f64::from(*v).into())),
            #[allow(clippy::cast_precision_loss)]
            (Type::Float, ColumnData::I64(Some(v))) => Some(Value::Float((*v as f64).into())),
            (Type::Float | Type::Any, ColumnData::Numeric(Some(n))) => {
                let s = n.to_string();
                s.parse::<f64>().ok().map(|f| Value::Float(f.into()))
            }
            // SQL Server's NUMERIC/DECIMAL columns can hold integer-shaped
            // values (scale = 0).  Without this branch, a Pathway schema
            // that declares such a column as `Type::Int` would error
            // per-row even though the actual values are integers.  Reject
            // values with non-zero fractional parts so the user sees a
            // clear error rather than a silent truncation.  `Type::Any` is
            // intentionally not included — it falls through to the
            // `Float | Any` branch above so the existing `NUMERIC → Float`
            // semantics are preserved for unspecified-type columns.
            // `int_part()` is used directly (not `to_string()`): tiberius's
            // `Display`/`Debug` always emits `"int.dec"`, so a scale-0
            // value formats as `"12345.0"` and would not round-trip
            // through `parse::<i64>`.
            (Type::Int, ColumnData::Numeric(Some(n))) => {
                if n.scale() == 0 {
                    i64::try_from(n.int_part()).ok().map(Value::Int)
                } else {
                    None
                }
            }
            (Type::String | Type::Any, ColumnData::String(Some(s))) => {
                Some(Value::String(s.to_string().into()))
            }
            (Type::Json, ColumnData::String(Some(s))) => {
                serde_json::from_str::<serde_json::Value>(s)
                    .ok()
                    .map(Value::from)
            }
            (Type::Bytes | Type::Any, ColumnData::Binary(Some(b))) => {
                Some(Value::Bytes(b.to_vec().into()))
            }
            (Type::String | Type::Any, ColumnData::Guid(Some(guid))) => {
                Some(Value::String(guid.to_string().into()))
            }
            (Type::String | Type::Any, ColumnData::Xml(Some(xml))) => {
                Some(Value::String(xml.to_string().into()))
            }
            (Type::Pointer, ColumnData::String(Some(s))) => {
                crate::engine::value::parse_pathway_pointer(s).ok()
            }
            (Type::Duration, ColumnData::I64(Some(v))) => {
                crate::engine::Duration::new_with_unit(*v, "us")
                    .map(Value::Duration)
                    .ok()
            }
            (Type::List(_) | Type::Tuple(_) | Type::Array(..), ColumnData::String(Some(s))) => {
                serde_json::from_str::<serde_json::Value>(s)
                    .ok()
                    .and_then(|json_val| {
                        crate::connectors::data_format::parse_value_from_json(&json_val, dtype)
                    })
            }
            (Type::PyObjectWrapper, ColumnData::Binary(Some(b))) => {
                bincode::deserialize::<Value>(b.as_ref()).ok()
            }
            _ => None,
        };
        if let Some(value) = value {
            Ok(value)
        } else {
            let value_repr = limit_length(format!("{col_data:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
            Err(Box::new(ConversionError::new(
                value_repr,
                field_name.to_owned(),
                dtype.clone(),
                None,
            )))
        }
    }

    /// Probe CDC availability for the target table.
    ///
    /// In streaming mode CDC is mandatory — any missing prerequisite is an
    /// error.  In static mode CDC is optional: when present it enables LSN
    /// tracking and therefore persistence, when absent the connector simply
    /// reads the table and emits rows with `EMPTY_OFFSET`.
    ///
    /// On success `self.capture_instance` is populated iff the returned flag
    /// is `true`.
    fn probe_cdc_availability(&mut self, cdc_required: bool) -> Result<bool, ReadError> {
        let schema_name = self.schema_name.clone();
        let table_name = self.table_name.clone();
        let config = self.config.clone();
        let runtime = &self.runtime;

        // Retry the whole probe on 1205/EADDRNOTAVAIL — both read-only,
        // idempotent, with a fresh connection per attempt.
        let outcome = execute_with_retries_if(
            || {
                runtime.block_on(async {
                    let mut client = connect_mssql(&config)
                        .await
                        .map_err(|e| ReadError::Mssql(classify_mssql_error(e)))?;

                    let row = client
                        .simple_query(
                            "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()",
                        )
                        .await
                        .map_err(mssql_read_err)?
                        .into_row()
                        .await
                        .map_err(mssql_read_err)?;
                    if let Some(row) = &row {
                        let enabled: Option<bool> = row.get(0);
                        if enabled != Some(true) {
                            if cdc_required {
                                return Err(ReadError::Mssql(MssqlError::CdcNotEnabledOnDatabase));
                            }
                            return Ok::<Option<(String, Vec<u8>)>, ReadError>(None);
                        }
                    }

                    let qualified_name = qualified_table_name(&schema_name, &table_name);
                    let qualified_lit = n_string_literal(&qualified_name);
                    let query = format!(
                        "SELECT capture_instance, start_lsn FROM cdc.change_tables \
                         WHERE source_object_id = OBJECT_ID({qualified_lit}) \
                         ORDER BY capture_instance"
                    );
                    let rows = client
                        .simple_query(&query)
                        .await
                        .map_err(mssql_read_err)?
                        .into_first_result()
                        .await
                        .map_err(mssql_read_err)?;

                    let entries: Vec<(String, Vec<u8>)> = rows
                        .iter()
                        .filter_map(|r| {
                            let inst: Option<&str> = r.get(0);
                            let start: Option<&[u8]> = r.get(1);
                            inst.zip(start).map(|(i, s)| (i.to_owned(), s.to_vec()))
                        })
                        .collect();

                    match entries.len() {
                        0 if cdc_required => {
                            Err(ReadError::Mssql(MssqlError::CdcNotEnabledOnTable {
                                schema: schema_name.clone(),
                                table: table_name.clone(),
                            }))
                        }
                        0 => Ok(None),
                        1 => Ok(Some(entries.into_iter().next().unwrap())),
                        _ => Err(ReadError::Mssql(MssqlError::AmbiguousCaptureInstance {
                            schema: schema_name.clone(),
                            table: table_name.clone(),
                            instances: entries.into_iter().map(|(i, _)| i).collect(),
                        })),
                    }
                })
            },
            is_transient_read,
            RetryConfig::default(),
            MAX_MSSQL_RETRIES,
        )?;

        if let Some((capture_instance, start_lsn)) = outcome {
            self.capture_instance = capture_instance;
            self.capture_start_lsn = start_lsn;
            self.tracks_lsn = true;
            info!(
                "MSSQL reader initialized for table '{}.{}', capture_instance='{}' (LSN tracking enabled)",
                self.schema_name, self.table_name, self.capture_instance
            );
            Ok(true)
        } else {
            self.tracks_lsn = false;
            info!(
                "MSSQL reader initialized for table '{}.{}' (static mode, no CDC — LSN tracking disabled)",
                self.schema_name, self.table_name
            );
            Ok(false)
        }
    }

    /// Verify each Pathway schema column exists on the side we'll read
    /// from: `sys.columns` of the source for static mode, and
    /// `cdc.captured_columns` for streaming mode (CDC captures a fixed
    /// snapshot of the column list at `sp_cdc_enable_table` time, so a
    /// column added to the source after enable is invisible to CDC even if
    /// it exists in `sys.columns`).  Surfaces the same `Invalid column
    /// name` error SQL Server would give later, but at init time and with
    /// the offending column listed.
    fn check_schema_columns_exist(&mut self) -> Result<(), ReadError> {
        let schema_name = self.schema_name.clone();
        let table_name = self.table_name.clone();
        let qualified_name = qualified_table_name(&schema_name, &table_name);
        let qualified_lit = n_string_literal(&qualified_name);
        // We will read from the CDC change tables iff the connector ever
        // calls `load_resume_delta` (persistence) or `poll_cdc_changes`
        // (streaming).  Static-mode without persistence reads only from
        // the source table, so checking against `cdc.captured_columns`
        // there would be a false positive when CDC was enabled with a
        // partial `@captured_column_list`.
        let cdc = self.tracks_lsn && (self.mode.is_polling_enabled() || self.persistence_enabled);
        let capture_instance = self.capture_instance.clone();
        let schema = self.schema.clone();
        let pathway_cols: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();

        let actual_cols = self.block_on_with_retry(async |client| {
            let query = if cdc {
                let inst_lit = n_string_literal(&capture_instance);
                format!(
                    "SELECT cc.column_name FROM cdc.captured_columns cc \
                     JOIN cdc.change_tables ct ON cc.object_id = ct.object_id \
                     WHERE ct.capture_instance = {inst_lit}"
                )
            } else {
                // Accept both user tables ('U') and views ('V') — static-mode
                // reads from views are valid (`SELECT FROM view` works).  An
                // OBJECT_ID type filter of 'U' would falsely reject them.
                format!(
                    "SELECT c.name FROM sys.columns c \
                     JOIN sys.objects o ON c.object_id = o.object_id \
                     WHERE o.object_id = OBJECT_ID({qualified_lit}) \
                     AND o.type IN ('U', 'V')"
                )
            };
            let rows = client
                .simple_query(&query)
                .await
                .map_err(mssql_read_err)?
                .into_first_result()
                .await
                .map_err(mssql_read_err)?;
            let cols: HashSet<String> = rows
                .iter()
                .filter_map(|r| r.get::<&str, _>(0).map(str::to_lowercase))
                .collect();
            // Empty result = either the object doesn't exist or it
            // genuinely has zero columns (impossible for tables/views).
            // Distinguish the two so the user gets a `TableNotFound`
            // message — not a "all your schema columns are missing" pile —
            // when the table really isn't there.  CDC mode skips this
            // because `probe_cdc_availability` already validated that the
            // table has a capture instance, which implies the table exists.
            if !cdc && cols.is_empty() {
                let probe = format!(
                    "SELECT 1 FROM sys.objects \
                     WHERE object_id = OBJECT_ID({qualified_lit}) \
                     AND type IN ('U', 'V')"
                );
                let exists_row = client
                    .simple_query(&probe)
                    .await
                    .map_err(mssql_read_err)?
                    .into_row()
                    .await
                    .map_err(mssql_read_err)?;
                if exists_row.is_none() {
                    return Err(ReadError::Mssql(MssqlError::TableNotFound {
                        reason: format!(
                            "table {qualified_name} does not exist (or is \
                             not a user table / view)"
                        ),
                    }));
                }
            }
            Ok::<_, ReadError>(cols)
        })?;

        let missing: Vec<String> = pathway_cols
            .into_iter()
            .filter(|c| !actual_cols.contains(&c.to_lowercase()))
            .collect();
        if !missing.is_empty() {
            let (origin, hint) = if cdc {
                (
                    "the CDC capture instance's column list",
                    "drop and re-enable CDC on the table so the new columns get captured",
                )
            } else {
                (
                    "the source table",
                    "ALTER TABLE the source to add the columns",
                )
            };
            return Err(ReadError::Mssql(MssqlError::SchemaColumnsMissing {
                schema: schema_name,
                table: table_name,
                missing,
                origin,
                hint,
            }));
        }
        Ok(())
    }

    /// Initialize the reader: probe CDC setup (unless `seek` already did),
    /// then either resume from a persisted LSN or load the table snapshot.
    /// Sets `is_initialized = true` on success.
    fn initialize(&mut self) -> Result<(), ReadError> {
        // `seek` probes with `cdc_required=true` when persistence is on, so
        // by the time we're here `tracks_lsn` is already set iff persistence
        // is configured.  Probe now only when that didn't happen (no
        // persistence), and use the mode-based requirement: streaming needs
        // CDC, static doesn't.
        if !self.tracks_lsn {
            let cdc_required = self.mode.is_polling_enabled();
            self.probe_cdc_availability(cdc_required)?;
        }
        self.check_schema_columns_exist()?;

        // Resume path: a previous run persisted an LSN.  Skip the full table
        // dump and replay only the CDC window strictly after the saved LSN.
        //
        // Clone (rather than `take`) so that retries still see the saved
        // LSN: if `load_resume_delta` fails — for example because the LSN
        // is outside the CDC retention window — the engine will retry up
        // to `max_allowed_consecutive_errors` times and must get the same
        // error each time.  Clearing the LSN on the first attempt would
        // silently fall back to `load_snapshot` and mask the problem.
        if self.tracks_lsn {
            if let Some(saved_lsn) = self.saved_last_lsn.clone() {
                self.load_resume_delta(&saved_lsn)?;
                self.saved_last_lsn = None;
                self.is_initialized = true;
                return Ok(());
            }
        }
        self.load_snapshot()?;
        self.is_initialized = true;
        Ok(())
    }

    /// Query the capture instance's validity interval: the minimum LSN
    /// retained (`fn_cdc_get_min_lsn`) and the current maximum LSN
    /// (`fn_cdc_get_max_lsn`).  Either value can be `None` — the minimum is
    /// `None` for a capture instance that has not produced any rows yet, the
    /// maximum is `None` before the capture agent has processed its first
    /// transaction.
    async fn fetch_capture_instance_lsn_bounds(
        client: &mut MssqlClient,
        capture_instance: &str,
    ) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), ReadError> {
        let capture_lit = n_string_literal(capture_instance);
        let min_lsn_row = client
            .simple_query(&format!("SELECT sys.fn_cdc_get_min_lsn({capture_lit})"))
            .await
            .map_err(mssql_read_err)?
            .into_row()
            .await
            .map_err(mssql_read_err)?;
        let min_lsn: Option<Vec<u8>> =
            min_lsn_row.and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec));

        let max_lsn_row = client
            .simple_query("SELECT sys.fn_cdc_get_max_lsn()")
            .await
            .map_err(mssql_read_err)?
            .into_row()
            .await
            .map_err(mssql_read_err)?;
        let max_lsn: Option<Vec<u8>> =
            max_lsn_row.and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec));

        Ok((min_lsn, max_lsn))
    }

    /// Run `body` against a tiberius client, retrying on transient errors
    /// (1205 deadlock victim, EADDRNOTAVAIL on connect).  Reuses the cached
    /// connection from `self.client` for the first attempt; on retry drops it
    /// and reconnects (a tiberius session can be left in a poisoned state
    /// after a server-side rollback).  Restores the working connection to
    /// `self.client` on success and clears it on permanent failure.
    fn block_on_with_retry<T, F>(&mut self, mut body: F) -> Result<T, ReadError>
    where
        F: AsyncFnMut(&mut MssqlClient) -> Result<T, ReadError>,
    {
        let mut client_opt = self.client.take();
        let config = self.config.clone();
        let runtime = &self.runtime;
        let mut attempt: usize = 0;
        let result = execute_with_retries_if(
            || {
                if attempt > 0 {
                    client_opt = None;
                }
                attempt += 1;
                runtime.block_on(async {
                    if client_opt.is_none() {
                        client_opt = Some(
                            connect_mssql(&config)
                                .await
                                .map_err(|e| ReadError::Mssql(classify_mssql_error(e)))?,
                        );
                    }
                    let client = client_opt.as_mut().unwrap();
                    body(client).await
                })
            },
            is_transient_read,
            RetryConfig::default(),
            MAX_MSSQL_RETRIES,
        );
        if result.is_err() {
            self.client = None;
        } else {
            self.client = client_opt;
        }
        result
    }

    /// Resume-path loader: fetch the CDC changes in (`saved_lsn`, current max
    /// LSN] and emit them as a single `NewSource`/`FinishedSource` block
    /// stamped with the current max LSN, so the whole catch-up window commits
    /// as one transaction.
    ///
    /// Before issuing the CDC query, verify that `saved_lsn` has not been
    /// purged by the SQL Server cleanup job: if it predates
    /// `fn_cdc_get_min_lsn(capture_instance)`, return
    /// [`MssqlError::CdcLsnOutOfRetention`] instead of silently losing data.
    fn load_resume_delta(&mut self, saved_lsn: &[u8]) -> Result<(), ReadError> {
        let capture_instance = self.capture_instance.clone();
        let capture_start_lsn = self.capture_start_lsn.clone();
        let schema = self.schema.clone();
        let key_column_names = self.key_column_names.clone();

        let (changes, new_max_lsn) = self.block_on_with_retry(async |client| {
            let (min_lsn, max_lsn) =
                Self::fetch_capture_instance_lsn_bounds(client, &capture_instance).await?;

            // Retention check: reject saved LSNs that predate the capture
            // instance's minimum LSN — those rows have been cleaned up and we
            // cannot reconstruct a correct delta from them.
            if let Some(ref min) = min_lsn {
                if saved_lsn < min.as_slice() {
                    return Err(ReadError::Mssql(MssqlError::CdcLsnOutOfRetention));
                }
            }

            let Some(to_lsn) = max_lsn else {
                // Capture agent has not produced any LSN yet; nothing to do.
                return Ok::<_, ReadError>((Vec::new(), Some(saved_lsn.to_vec())));
            };

            if saved_lsn >= to_lsn.as_slice() {
                // Already caught up; resume live polling from the saved LSN.
                return Ok((Vec::new(), Some(saved_lsn.to_vec())));
            }

            let rows = Self::fetch_cdc_rows_in_range(
                client,
                saved_lsn,
                &to_lsn,
                &schema,
                &capture_instance,
                &capture_start_lsn,
            )
            .await
            .map_err(classify_resume_cdc_error)?;
            Ok((rows, Some(to_lsn)))
        })?;

        if !changes.is_empty() {
            self.snapshot_version += 1;
            let block_offset = match new_max_lsn.as_deref() {
                Some(lsn) => Self::lsn_offset(lsn),
                None => EMPTY_OFFSET,
            };
            let mut results = Vec::with_capacity(changes.len() + 2);
            results.push(ReadResult::NewSource(
                MssqlMetadata::new(self.snapshot_version).into(),
            ));
            results.extend(Self::build_gap_results(
                &changes,
                &schema,
                &key_column_names,
                &block_offset,
            ));
            results.push(ReadResult::FinishedSource {
                commit_possibility: CommitPossibility::Possible,
            });
            results.reverse();
            self.snapshot = results;
        }

        self.last_lsn = new_max_lsn;
        Ok(())
    }

    /// Extract column values from a row into a `HashMap`.
    ///
    /// `col_offset` is added to each column index — pass `0` for snapshot rows,
    /// `4` for CDC rows (which have four metadata columns before the user data).
    fn extract_row_values(
        row: &tiberius::Row,
        schema: &[(String, Type)],
        col_offset: usize,
    ) -> HashMap<String, Result<Value, Box<ConversionError>>> {
        let mut row_values = HashMap::with_capacity(schema.len());
        for (col_idx, (col_name, col_dtype)) in schema.iter().enumerate() {
            let value = Self::convert_row_value(row, col_idx + col_offset, col_name, col_dtype);
            row_values.insert(col_name.clone(), value);
        }
        row_values
    }

    /// Step 1: Record the current CDC max LSN (retry until the capture agent
    /// has started).
    ///
    /// `fn_cdc_get_max_lsn` returns NULL until the capture agent has processed
    /// at least one transaction; we retry up to 20 times (10 s) before giving up.
    async fn fetch_lsn_before(
        client: &mut Client<Compat<TcpStream>>,
    ) -> Result<Option<Vec<u8>>, ReadError> {
        let mut lsn: Option<Vec<u8>> = None;
        for _ in 0..20usize {
            let row = client
                .simple_query("SELECT sys.fn_cdc_get_max_lsn()")
                .await
                .map_err(mssql_read_err)?
                .into_row()
                .await
                .map_err(mssql_read_err)?;
            lsn = row.and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec));
            if lsn.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Ok(lsn)
    }

    /// Step 2: Read the full table snapshot via a plain SELECT.
    async fn fetch_snapshot_rows(
        client: &mut Client<Compat<TcpStream>>,
        query_str: &str,
    ) -> Result<Vec<tiberius::Row>, ReadError> {
        client
            .simple_query(query_str)
            .await
            .map_err(|e| ReadError::Mssql(classify_mssql_error(e)))?
            .into_first_result()
            .await
            .map_err(|e| ReadError::Mssql(classify_mssql_error(e)))
    }

    /// Step 3: Record the CDC max LSN again, immediately after the snapshot SELECT.
    async fn fetch_lsn_after(
        client: &mut Client<Compat<TcpStream>>,
    ) -> Result<Option<Vec<u8>>, ReadError> {
        let row = client
            .simple_query("SELECT sys.fn_cdc_get_max_lsn()")
            .await
            .map_err(mssql_read_err)?
            .into_row()
            .await
            .map_err(mssql_read_err)?;
        Ok(row.and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec)))
    }

    /// Execute a CDC change-table query for the half-open window
    /// (`from_lsn_exclusive`, `to_lsn_inclusive`].
    ///
    /// Increments `from_lsn_exclusive` by one.  When the capture instance's
    /// `fn_cdc_get_min_lsn` is greater than that point, two cases are
    /// possible and they need different responses:
    ///
    /// 1. The capture instance's `min_lsn` still equals its `start_lsn`
    ///    (i.e., cleanup has not run on this instance) — the gap between
    ///    `from_lsn_exclusive` and `min_lsn` is just the period before
    ///    capture started, and no CT rows ever existed there.  Clamping
    ///    is correct.
    /// 2. Cleanup has advanced `min_lsn` past `start_lsn` and past
    ///    `from_lsn_exclusive` — CT rows that used to exist in
    ///    (`from_lsn_exclusive`, `min_lsn`] have been purged.  Surface
    ///    [`MssqlError::CdcLsnOutOfRetention`] so the user can clear the
    ///    persistence directory and re-snapshot, rather than silently
    ///    losing the rows.
    ///
    /// `capture_start_lsn` is the value of `cdc.change_tables.start_lsn`
    /// captured by [`probe_cdc_availability`].  CDC never advances it, so
    /// comparing it to the live `min_lsn` cleanly distinguishes the two
    /// cases above without round-tripping `cdc.change_tables` per query.
    async fn fetch_cdc_rows_in_range(
        client: &mut MssqlClient,
        from_lsn_exclusive: &[u8],
        to_lsn_inclusive: &[u8],
        schema: &[(String, Type)],
        capture_instance: &str,
        capture_start_lsn: &[u8],
    ) -> Result<Vec<tiberius::Row>, ReadError> {
        let from_hex = hex::encode(from_lsn_exclusive);
        let inc_row = client
            .simple_query(&format!("SELECT sys.fn_cdc_increment_lsn(0x{from_hex})"))
            .await
            .map_err(mssql_read_err)?
            .into_row()
            .await
            .map_err(mssql_read_err)?;
        let incremented_lsn: Option<Vec<u8>> =
            inc_row.and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec));

        let Some(inc_lsn) = incremented_lsn else {
            return Ok(Vec::new());
        };

        let capture_lit = n_string_literal(capture_instance);
        let min_lsn_row = client
            .simple_query(&format!("SELECT sys.fn_cdc_get_min_lsn({capture_lit})"))
            .await
            .map_err(mssql_read_err)?
            .into_row()
            .await
            .map_err(mssql_read_err)?;
        let min_lsn: Option<Vec<u8>> =
            min_lsn_row.and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec));

        // Cleanup-vs-fresh-instance discriminator.  See doc comment above.
        if let Some(ref min) = min_lsn {
            if min.as_slice() > inc_lsn.as_slice() && min.as_slice() > capture_start_lsn {
                return Err(ReadError::Mssql(MssqlError::CdcLsnOutOfRetention));
            }
        }

        let effective_from = match min_lsn {
            Some(ref min) if min.as_slice() > inc_lsn.as_slice() => min.clone(),
            _ => inc_lsn,
        };

        if effective_from.as_slice() > to_lsn_inclusive {
            return Ok(Vec::new());
        }

        let from_hex = hex::encode(&effective_from);
        let to_hex = hex::encode(to_lsn_inclusive);
        let user_cols = schema
            .iter()
            .map(|(n, _)| quote_identifier(n))
            .collect::<Vec<_>>()
            .join(",");
        let cdc_query = format!(
            "SELECT __$start_lsn,__$seqval,__$operation,__$update_mask,\
             {user_cols} FROM cdc.fn_cdc_get_all_changes_{capture_instance}\
             (0x{from_hex}, 0x{to_hex}, N'all update old')"
        );

        client
            .simple_query(&cdc_query)
            .await
            .map_err(mssql_read_err)?
            .into_first_result()
            .await
            .map_err(mssql_read_err)
    }

    /// Step 4: Fetch CDC changes that raced with the snapshot read.
    ///
    /// Queries `cdc.fn_cdc_get_all_changes_{capture_instance}` for the
    /// half-open window (`lsn_before`, `lsn_after`].  Returns an empty `Vec`
    /// when the two LSNs are equal or the effective range is empty.
    async fn fetch_gap_cdc_changes(
        client: &mut MssqlClient,
        lsn_before: &[u8],
        lsn_after: &[u8],
        schema: &[(String, Type)],
        capture_instance: &str,
        capture_start_lsn: &[u8],
    ) -> Result<Vec<tiberius::Row>, ReadError> {
        if lsn_before == lsn_after {
            return Ok(Vec::new());
        }
        Self::fetch_cdc_rows_in_range(
            client,
            lsn_before,
            lsn_after,
            schema,
            capture_instance,
            capture_start_lsn,
        )
        .await
    }

    /// Step 5a: Convert raw snapshot rows into `ReadResult::Data(Insert)` entries.
    ///
    /// All snapshot rows are emitted as inserts regardless of their prior
    /// state.  When `emit_keys` is `true` a composite key is derived from
    /// `key_column_names` so that Pathway can correlate these rows with
    /// subsequent CDC deletes/inserts — required whenever LSN tracking is
    /// active, i.e. streaming mode and static mode on a CDC-enabled table.
    fn build_snapshot_results(
        rows: &[tiberius::Row],
        schema: &[(String, Type)],
        key_column_names: &[String],
        emit_keys: bool,
        offset: &Offset,
    ) -> Vec<ReadResult> {
        rows.iter()
            .map(|row| {
                let row_values = Self::extract_row_values(row, schema, 0);
                let key = if emit_keys {
                    Some(
                        key_column_names
                            .iter()
                            .map(|name| {
                                row_values
                                    .get(name)
                                    .and_then(|r| r.as_ref().ok().cloned())
                                    .unwrap_or(Value::None)
                            })
                            .collect(),
                    )
                } else {
                    None
                };
                let values: ValuesMap = row_values.into();
                ReadResult::Data(
                    ReaderContext::from_diff(DataEventType::Insert, key, values),
                    offset.clone(),
                )
            })
            .collect()
    }

    /// Step 5b: Convert gap CDC rows into `ReadResult::Data` entries.
    ///
    /// CDC operation codes:
    /// - `1` = delete before-image, `3` = delete after-update-old → **Delete**
    /// - `2` = insert,              `4` = insert after-update-new → **Insert**
    ///
    /// Rows with unrecognized operation codes are silently skipped.
    /// User columns start at offset 4 (after the four CDC metadata columns).
    fn build_gap_results(
        gap_changes: &[tiberius::Row],
        schema: &[(String, Type)],
        key_column_names: &[String],
        offset: &Offset,
    ) -> Vec<ReadResult> {
        gap_changes
            .iter()
            .filter_map(|row| {
                let operation: Option<i32> = row.get(2);
                let event_type = match operation.unwrap_or(0) {
                    1 | 3 => DataEventType::Delete,
                    2 | 4 => DataEventType::Insert,
                    _ => return None,
                };
                // User columns start at index 4 (after four CDC metadata columns).
                let row_values = Self::extract_row_values(row, schema, 4);
                let key: Vec<Value> = key_column_names
                    .iter()
                    .map(|name| {
                        row_values
                            .get(name)
                            .and_then(|r| r.as_ref().ok().cloned())
                            .unwrap_or(Value::None)
                    })
                    .collect();
                let values: ValuesMap = row_values.into();
                Some(ReadResult::Data(
                    ReaderContext::from_diff(event_type, Some(key), values),
                    offset.clone(),
                ))
            })
            .collect()
    }

    /// Build the persistence offset for a CDC read window whose upper bound
    /// (inclusive) is `lsn`.  A block of rows stamped with this offset is
    /// reconstructible on restart by replaying everything strictly after `lsn`.
    fn lsn_offset(lsn: &[u8]) -> Offset {
        (OffsetKey::Mssql, OffsetValue::MssqlCdcLsn(lsn.to_vec()))
    }

    /// Load the current table contents into `self.snapshot`.
    ///
    /// In streaming mode the snapshot and the CDC changes that raced with it are
    /// emitted as a **single** `NewSource … FinishedSource` block so that
    /// Pathway reconciles them atomically:
    ///
    /// 1. Record `lsn_before` = current CDC max LSN (retry until the capture
    ///    agent has started).
    /// 2. Read the full table snapshot.
    /// 3. Record `lsn_after` = CDC max LSN again.
    /// 4. Query CDC for all changes in (`lsn_before`, `lsn_after`] — these are
    ///    the rows that changed while the SELECT was running.
    /// 5. Emit snapshot rows followed by gap CDC rows inside one block.
    /// 6. Set `last_lsn = lsn_after` so that `poll_cdc_changes` picks up only
    ///    changes that arrived strictly after the snapshot window.
    ///
    /// Results are stored in reverse order so that `Vec::pop` yields them in
    /// the correct sequence: `NewSource` first, data rows, then `FinishedSource`.
    fn load_snapshot(&mut self) -> Result<(), ReadError> {
        let columns_str = self
            .schema
            .iter()
            .map(|(n, _)| quote_identifier(n))
            .collect::<Vec<_>>()
            .join(",");
        let full_table_name = qualified_table_name(&self.schema_name, &self.table_name);
        let query_str = format!("SELECT {columns_str} FROM {full_table_name}");
        let tracks_lsn = self.tracks_lsn;
        let streaming = self.mode.is_polling_enabled();
        let key_column_names = self.key_column_names.clone();
        let schema = self.schema.clone();
        let capture_instance = self.capture_instance.clone();
        let capture_start_lsn = self.capture_start_lsn.clone();

        let (rows, max_lsn, gap_changes) = self.block_on_with_retry(async |client| {
            // Step 1: record LSN before snapshot read.
            // Retry until the CDC capture agent has processed at least one
            // transaction (fn_cdc_get_max_lsn returns NULL until then).
            let lsn_before: Option<Vec<u8>> = if tracks_lsn {
                Self::fetch_lsn_before(client).await?
            } else {
                None
            };

            // Step 2: read the full table snapshot.
            let rows = Self::fetch_snapshot_rows(client, &query_str).await?;

            // Step 3: record LSN after snapshot read.
            let lsn_after: Option<Vec<u8>> = if tracks_lsn {
                Self::fetch_lsn_after(client).await?
            } else {
                None
            };

            // Step 4: fetch CDC changes that raced with the snapshot read.
            let gap_changes: Vec<tiberius::Row> =
                match (tracks_lsn, lsn_before.as_deref(), lsn_after.as_deref()) {
                    (true, Some(before), Some(after)) => {
                        Self::fetch_gap_cdc_changes(
                            client,
                            before,
                            after,
                            &schema,
                            &capture_instance,
                            &capture_start_lsn,
                        )
                        .await?
                    }
                    _ => Vec::new(),
                };

            // Step 5: pick the offset that gets stamped on the snapshot
            // block.  Prefer `lsn_after` (post-snapshot) and fall back to
            // `lsn_before` if `fn_cdc_get_max_lsn()` somehow regressed.  If
            // both are still NULL — capture agent has not produced a single
            // LSN yet on a freshly CDC-enabled, otherwise idle database —
            // fall back to the capture instance's `fn_cdc_get_min_lsn`,
            // which CDC sets at `sp_cdc_enable_table` time.  Without this
            // fallback the block stamps `EMPTY_OFFSET`, persistence saves
            // no MSSQL offset, and Run 2 silently re-snapshots — exactly
            // the behavior the persistence docstring promises against.
            let mut max_lsn = lsn_after.or(lsn_before);
            if tracks_lsn && max_lsn.is_none() {
                let (min_lsn, _) =
                    Self::fetch_capture_instance_lsn_bounds(client, &capture_instance).await?;
                max_lsn = min_lsn;
            }

            Ok::<_, ReadError>((rows, max_lsn, gap_changes))
        })?;

        self.snapshot_version += 1;
        // Stamp the whole snapshot block — original rows + gap CDC rows — with
        // the post-snapshot LSN.  On restart from this offset, poll_cdc_changes
        // will resume strictly after this LSN and will therefore not replay
        // either the snapshot or the gap.
        let block_offset = match max_lsn.as_deref() {
            Some(lsn) => Self::lsn_offset(lsn),
            None => EMPTY_OFFSET,
        };
        let mut results = Vec::with_capacity(rows.len() + gap_changes.len() + 2);
        results.push(ReadResult::NewSource(
            MssqlMetadata::new(self.snapshot_version).into(),
        ));

        // Step 5a: snapshot rows — all emitted as inserts.  Keys are needed
        // whenever we track LSNs (streaming, or static + CDC) so that CDC
        // deletes/inserts delivered on restart match the snapshot rows.
        results.extend(Self::build_snapshot_results(
            &rows,
            &schema,
            &key_column_names,
            tracks_lsn,
            &block_offset,
        ));

        // Step 5b: gap CDC rows — changes that raced with the snapshot read, emitted in
        // the same block so Pathway reconciles them in one atomic transaction.
        results.extend(Self::build_gap_results(
            &gap_changes,
            &schema,
            &key_column_names,
            &block_offset,
        ));

        results.push(ReadResult::FinishedSource {
            commit_possibility: CommitPossibility::Possible,
        });
        results.reverse();
        self.snapshot = results;

        // Step 6: advance last_lsn so poll_cdc_changes starts strictly after
        // the snapshot window.
        if streaming {
            self.last_lsn = max_lsn;
        }

        Ok(())
    }

    /// Poll for CDC changes since the last processed LSN and push results into
    /// `self.snapshot` as a complete `NewSource … FinishedSource` block.
    fn poll_cdc_changes(&mut self) -> Result<(), ReadError> {
        // Bootstrap `last_lsn` if `load_snapshot` couldn't produce one (the
        // min-LSN fallback should make this unreachable in practice, but
        // stay defensive: a `None` here would otherwise cause every poll to
        // be a silent no-op and live CDC events would never be delivered).
        if self.last_lsn.is_none() {
            let capture_instance = self.capture_instance.clone();
            let bootstrap_lsn = self.block_on_with_retry(async |client| {
                let (min_lsn, max_lsn) =
                    Self::fetch_capture_instance_lsn_bounds(client, &capture_instance).await?;
                Ok::<_, ReadError>(max_lsn.or(min_lsn))
            })?;
            if bootstrap_lsn.is_none() {
                return Ok(());
            }
            self.last_lsn = bootstrap_lsn;
        }
        let from_lsn = self.last_lsn.clone().expect("just bootstrapped above");

        let capture_instance = self.capture_instance.clone();
        let capture_start_lsn = self.capture_start_lsn.clone();
        let schema = self.schema.clone();
        let key_column_names = self.key_column_names.clone();

        let (changes, new_max_lsn) = self.block_on_with_retry(async |client| {
            let lsn_row = client
                .simple_query("SELECT sys.fn_cdc_get_max_lsn()")
                .await
                .map_err(mssql_read_err)?
                .into_row()
                .await
                .map_err(mssql_read_err)?;

            let new_max_lsn: Option<Vec<u8>> =
                lsn_row.and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec));

            if new_max_lsn.as_ref() == Some(&from_lsn) {
                return Ok::<_, ReadError>((Vec::new(), Some(from_lsn.clone())));
            }

            let Some(to_lsn) = &new_max_lsn else {
                return Ok((Vec::new(), new_max_lsn));
            };

            let rows = Self::fetch_cdc_rows_in_range(
                client,
                &from_lsn,
                to_lsn,
                &schema,
                &capture_instance,
                &capture_start_lsn,
            )
            .await?;

            Ok((rows, new_max_lsn))
        })?;

        if !changes.is_empty() {
            self.snapshot_version += 1;
            let block_offset = match new_max_lsn.as_deref() {
                Some(lsn) => Self::lsn_offset(lsn),
                None => EMPTY_OFFSET,
            };
            let mut results = Vec::with_capacity(changes.len() + 2);
            results.push(ReadResult::NewSource(
                MssqlMetadata::new(self.snapshot_version).into(),
            ));
            results.extend(Self::build_gap_results(
                &changes,
                &schema,
                &key_column_names,
                &block_offset,
            ));
            results.push(ReadResult::FinishedSource {
                commit_possibility: CommitPossibility::Possible,
            });
            results.reverse();
            self.snapshot = results;
        }

        if let Some(new_lsn) = new_max_lsn {
            self.last_lsn = Some(new_lsn);
        }

        Ok(())
    }

    fn wait_period() -> std::time::Duration {
        std::time::Duration::from_millis(500)
    }
}

impl Reader for MssqlReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        // The engine only calls `seek` when persistence is configured.  In
        // that case CDC is mandatory — the LSN we persist comes from CDC —
        // so probe for it right here and let the generic
        // `CdcNotEnabledOn{Database,Table}` errors fire if it's missing.
        // This also populates `self.capture_instance`, which
        // `load_resume_delta` needs below.  `initialize()` will notice that
        // `tracks_lsn` is already set and skip a redundant probe.
        self.persistence_enabled = true;
        self.probe_cdc_availability(true)?;
        if let Some(OffsetValue::MssqlCdcLsn(bytes)) = frontier.get_offset(&OffsetKey::Mssql) {
            self.saved_last_lsn = Some(bytes.clone());
        }
        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(result) = self.snapshot.pop() {
                return Ok(result);
            }
            if !self.is_initialized {
                self.initialize().inspect_err(|_e| {
                    self.client = None;
                })?;
            } else if self.mode.is_polling_enabled() {
                self.poll_cdc_changes().inspect_err(|_e| {
                    self.client = None;
                })?;
                if self.snapshot.is_empty() {
                    std::thread::sleep(Self::wait_period());
                }
            } else {
                return Ok(ReadResult::Finished);
            }
        }
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        MAX_MSSQL_RETRIES
    }

    fn short_description(&self) -> Cow<'static, str> {
        if self.mode.is_polling_enabled() {
            format!("MSSQL-CDC({})", self.table_name).into()
        } else {
            format!("MSSQL({})", self.table_name).into()
        }
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mssql
    }
}

pub struct MssqlWriter {
    runtime: TokioRuntime,
    config: Config,
    client: Option<MssqlClient>,
    max_batch_size: Option<usize>,
    buffer: Vec<FormatterContext>,
    snapshot_mode: bool,
    table_name: String,
    query_template: SqlQueryTemplate,
    // For snapshot mode, we need a custom MERGE statement
    merge_query: Option<String>,
    // Types for every value field in schema order — used for typed NULL binding.
    value_field_types: Vec<Type>,
    // Types for PK fields in PK order — used for typed NULL binding in deletes.
    pk_field_types: Vec<Type>,
}

impl MssqlWriter {
    /// Verify the destination table exists.  Used for `TableWriterInitMode::Default`,
    /// which doesn't issue DDL: without this check the first INSERT would surface
    /// `TableNotFound` many seconds after `pw.run()` starts.
    async fn check_table_exists(
        client: &mut MssqlClient,
        full_table_name: &str,
        full_table_lit: &str,
    ) -> Result<(), WriteError> {
        let probe = format!("SELECT OBJECT_ID({full_table_lit}, N'U')");
        let row = client
            .simple_query(&probe)
            .await
            .map_err(mssql_write_err)?
            .into_row()
            .await
            .map_err(mssql_write_err)?;
        let exists = row.as_ref().and_then(|r| r.get::<i32, _>(0)).is_some();
        if !exists {
            return Err(WriteError::Mssql(MssqlError::TableNotFound {
                reason: format!("table {full_table_name} does not exist"),
            }));
        }
        Ok(())
    }

    /// Verify each Pathway value field has a matching column in the
    /// destination table, that none of those columns is an IDENTITY column
    /// (which SQL Server refuses to INSERT into without
    /// `SET IDENTITY_INSERT ON`), and — for `stream_of_changes` mode —
    /// that the auto-appended `[time]` / `[diff]` metadata columns exist.
    /// Skipped after `Replace` since `build_create_table_query` produces a
    /// table with exactly the expected columns by construction.
    #[allow(clippy::too_many_lines)]
    async fn check_destination_columns(
        client: &mut MssqlClient,
        schema_name: &str,
        table_name: &str,
        full_table_lit: &str,
        value_fields: &[ValueField],
        require_time_diff: bool,
    ) -> Result<(), WriteError> {
        // Pull is_identity, is_computed, is_nullable, and "has default
        // constraint" all in one round-trip — every check_destination_*
        // assertion below is decided from this single result set.
        // `default_object_id != 0` means SQL Server has a DEFAULT
        // constraint (column-level or table-level) for this column, so
        // omitting it from INSERT is safe.
        let query = format!(
            "SELECT name, is_identity, is_computed, is_nullable, \
                    CASE WHEN default_object_id <> 0 THEN 1 ELSE 0 END \
             FROM sys.columns \
             WHERE object_id = OBJECT_ID({full_table_lit}, N'U')"
        );
        let rows = client
            .simple_query(&query)
            .await
            .map_err(mssql_write_err)?
            .into_first_result()
            .await
            .map_err(mssql_write_err)?;
        let mut actual: HashSet<String> = HashSet::new();
        let mut identity: HashSet<String> = HashSet::new();
        let mut computed: HashSet<String> = HashSet::new();
        // Set of destination columns that are NOT NULL (incl. PK / identity
        // / computed) — checked against Pathway's `Optional` value_fields
        // so we don't bind `NULL` into a column that rejects it.
        let mut not_null: HashSet<String> = HashSet::new();
        // Original-cased name list, in catalog order, for the
        // required-but-missing report.
        let mut required_dest: Vec<String> = Vec::new();
        for r in &rows {
            if let Some(name) = r.get::<&str, _>(0) {
                let lower = name.to_lowercase();
                actual.insert(lower.clone());
                let is_identity = r.get::<bool, _>(1).unwrap_or(false);
                let is_computed = r.get::<bool, _>(2).unwrap_or(false);
                let is_nullable = r.get::<bool, _>(3).unwrap_or(true);
                let has_default = r.get::<i32, _>(4).unwrap_or(0) != 0;
                if is_identity {
                    identity.insert(lower.clone());
                }
                if is_computed {
                    computed.insert(lower.clone());
                }
                if !is_nullable {
                    not_null.insert(lower.clone());
                }
                if !is_nullable && !is_identity && !is_computed && !has_default {
                    required_dest.push(name.to_owned());
                }
            }
        }

        let mut missing: Vec<String> = Vec::new();
        let mut identity_conflicts: Vec<String> = Vec::new();
        let mut computed_conflicts: Vec<String> = Vec::new();
        let mut optional_into_not_null: Vec<String> = Vec::new();
        for f in value_fields {
            let lower = f.name.to_lowercase();
            if !actual.contains(&lower) {
                missing.push(f.name.clone());
                continue;
            }
            if identity.contains(&lower) {
                identity_conflicts.push(f.name.clone());
            }
            if computed.contains(&lower) {
                computed_conflicts.push(f.name.clone());
            }
            // `Optional[T]` Pathway fields can emit `None`, which the
            // writer binds as SQL NULL.  A NOT NULL destination column
            // (regardless of the underlying type) rejects every such row.
            if matches!(f.type_, Type::Optional(_)) && not_null.contains(&lower) {
                optional_into_not_null.push(f.name.clone());
            }
        }
        if require_time_diff {
            for col in ["time", "diff"] {
                if !actual.contains(col) {
                    missing.push(col.to_owned());
                }
            }
        }
        if !missing.is_empty() {
            return Err(WriteError::Mssql(MssqlError::DestinationColumnsMissing {
                schema: schema_name.to_owned(),
                table: table_name.to_owned(),
                missing,
            }));
        }
        if !identity_conflicts.is_empty() {
            return Err(WriteError::Mssql(MssqlError::IdentityColumnInSchema {
                schema: schema_name.to_owned(),
                table: table_name.to_owned(),
                columns: identity_conflicts,
            }));
        }
        if !computed_conflicts.is_empty() {
            return Err(WriteError::Mssql(MssqlError::ComputedColumnInSchema {
                schema: schema_name.to_owned(),
                table: table_name.to_owned(),
                columns: computed_conflicts,
            }));
        }
        if !optional_into_not_null.is_empty() {
            return Err(WriteError::Mssql(
                MssqlError::OptionalIntoNotNullDestination {
                    schema: schema_name.to_owned(),
                    table: table_name.to_owned(),
                    columns: optional_into_not_null,
                },
            ));
        }
        // Required destination columns (NOT NULL / no default / not
        // IDENTITY / not computed) that aren't covered by the Pathway
        // schema would fail every INSERT with a NULL-constraint
        // violation.  In `stream_of_changes` mode the auto-appended
        // `[time]` and `[diff]` columns are also supplied by every
        // INSERT, so include them in the "Pathway provides" set.
        let mut pathway_set: HashSet<String> =
            value_fields.iter().map(|f| f.name.to_lowercase()).collect();
        if require_time_diff {
            pathway_set.insert("time".to_owned());
            pathway_set.insert("diff".to_owned());
        }
        let required_missing: Vec<String> = required_dest
            .into_iter()
            .filter(|c| !pathway_set.contains(&c.to_lowercase()))
            .collect();
        if !required_missing.is_empty() {
            return Err(WriteError::Mssql(
                MssqlError::DestinationRequiredColumnsMissing {
                    schema: schema_name.to_owned(),
                    table: table_name.to_owned(),
                    columns: required_missing,
                },
            ));
        }
        Ok(())
    }

    /// Verify the destination table has a unique constraint (PRIMARY KEY or
    /// UNIQUE index) on exactly the configured `primary_key` columns.  The
    /// snapshot-mode `MERGE` matches rows on `target.k = source.k`; without
    /// a unique constraint backing those columns SQL Server happily updates
    /// every row that matches and our snapshot semantics break silently.
    /// Skipped when the writer just (re)created the table itself, since
    /// `build_create_table_query` always emits the matching PRIMARY KEY.
    async fn check_snapshot_pk_matches(
        client: &mut MssqlClient,
        schema_name: &str,
        table_name: &str,
        full_table_lit: &str,
        key_field_names: &[String],
    ) -> Result<(), WriteError> {
        // INCLUDE columns (`is_included_column = 1`) are stored alongside
        // key columns in sys.index_columns but don't participate in
        // uniqueness; filtered indexes (`has_filter = 1`) only enforce
        // uniqueness over a subset of rows so they can't back our MERGE;
        // disabled indexes don't enforce anything.  Filter all three out
        // up front so the set-comparison below sees exactly the columns
        // SQL Server actually uses to enforce uniqueness.
        let query = format!(
            "SELECT i.name, c.name \
             FROM sys.indexes i \
             JOIN sys.index_columns ic \
                 ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
             JOIN sys.columns c \
                 ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
             WHERE i.object_id = OBJECT_ID({full_table_lit}, N'U') \
             AND i.is_unique = 1 \
             AND i.is_disabled = 0 \
             AND i.has_filter = 0 \
             AND ic.is_included_column = 0 \
             ORDER BY i.index_id, ic.key_ordinal"
        );
        let rows = client
            .simple_query(&query)
            .await
            .map_err(mssql_write_err)?
            .into_first_result()
            .await
            .map_err(mssql_write_err)?;

        let mut indexes: HashMap<String, Vec<String>> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for r in &rows {
            let idx_name: Option<&str> = r.get(0);
            let col_name: Option<&str> = r.get(1);
            if let (Some(idx), Some(col)) = (idx_name, col_name) {
                let entry = indexes.entry(idx.to_owned()).or_default();
                if entry.is_empty() {
                    order.push(idx.to_owned());
                }
                entry.push(col.to_owned());
            }
        }

        // SQL Server identifiers are case-insensitive under the default
        // collation, so compare case-folded.
        let our_pk: HashSet<String> = key_field_names.iter().map(|s| s.to_lowercase()).collect();
        let matches = indexes.values().any(|cols| {
            cols.len() == our_pk.len()
                && cols
                    .iter()
                    .map(|s| s.to_lowercase())
                    .collect::<HashSet<_>>()
                    == our_pk
        });
        if matches {
            return Ok(());
        }

        let actual: Vec<Vec<String>> = order
            .into_iter()
            .filter_map(|idx| indexes.remove(&idx))
            .collect();
        Err(WriteError::Mssql(MssqlError::SnapshotPkMismatch {
            schema: schema_name.to_owned(),
            table: table_name.to_owned(),
            expected: key_field_names.to_vec(),
            actual,
        }))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn new(
        config: Config,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        table_ctx: &TableContext,
        mode: TableWriterInitMode,
    ) -> Result<MssqlWriter, WriteError> {
        let TableContext {
            schema_name,
            table_name,
            value_fields,
            key_field_names,
        } = table_ctx;
        let value_fields = value_fields.as_slice();
        let key_field_names = key_field_names.as_deref();
        let runtime = create_async_tokio_runtime()?;
        let full_table_name = qualified_table_name(schema_name, table_name);

        // Collect initialization queries with bracket-quoted identifiers.
        // We bypass the shared create_table_if_not_exists because it generates
        // unquoted column names that break SQL Server reserved-word identifiers.
        let mut init_queries: Vec<String> = Vec::new();
        let full_table_lit = n_string_literal(&full_table_name);
        match mode {
            TableWriterInitMode::Default => {}
            TableWriterInitMode::Replace => {
                init_queries.push(format!(
                    "IF OBJECT_ID({full_table_lit}, N'U') IS NOT NULL \
                     DROP TABLE {full_table_name}"
                ));
                init_queries.push(Self::build_create_table_query(
                    &full_table_name,
                    value_fields,
                    key_field_names,
                    !snapshot_mode,
                )?);
            }
            TableWriterInitMode::CreateIfNotExists => {
                init_queries.push(Self::build_create_table_query(
                    &full_table_name,
                    value_fields,
                    key_field_names,
                    !snapshot_mode,
                )?);
            }
        }

        // Establish connection and execute initialization queries with
        // 1205/EADDRNOTAVAIL retry.  Both init queries are idempotent —
        // ``DROP TABLE IF EXISTS`` is a no-op when the table is already
        // gone, and ``IF OBJECT_ID(N'…', N'U') IS NULL CREATE TABLE …`` is
        // a no-op when it already exists — so re-running the whole sequence
        // is safe.  Each retry opens a fresh tiberius connection: the
        // ``sys.sp_cdc_ddl_event_internal`` trigger that fires on every
        // CREATE/DROP (in databases with database-level CDC enabled) is the
        // dominant 1205 source here, and a clean session avoids carrying
        // any post-rollback state forward.  `Default` issues no DDL — instead
        // we preflight via `check_table_exists` so a missing table surfaces
        // immediately, not from the first INSERT.  For snapshot mode + an
        // existing destination we additionally verify a matching unique
        // index covers the configured `primary_key` columns: without that
        // the MERGE statement happily upserts the wrong rows.
        let preflight_default = matches!(mode, TableWriterInitMode::Default);
        let preflight_snapshot_pk = snapshot_mode && !matches!(mode, TableWriterInitMode::Replace);
        // `Replace` rebuilds the table from our schema, so column existence
        // matches by construction.  For `Default` and `CreateIfNotExists`
        // (when the table already existed) the destination's columns might
        // not match — validate before any data is sent.
        let preflight_columns = !matches!(mode, TableWriterInitMode::Replace);
        let client = execute_with_retries_if(
            || {
                runtime.block_on(async {
                    let mut client = connect_mssql(&config)
                        .await
                        .map_err(|e| WriteError::Mssql(classify_mssql_error(e)))?;

                    if let Ok((version, edition)) = detect_server_version(&mut client).await {
                        info!("Connected to MSSQL Server: version={version}, edition={edition}");
                    }

                    if preflight_default {
                        Self::check_table_exists(&mut client, &full_table_name, &full_table_lit)
                            .await?;
                    }

                    for query in &init_queries {
                        client
                            .execute(query.as_str(), &[])
                            .await
                            .map_err(mssql_write_err)?;
                    }

                    if preflight_columns {
                        Self::check_destination_columns(
                            &mut client,
                            schema_name,
                            table_name,
                            &full_table_lit,
                            value_fields,
                            !snapshot_mode,
                        )
                        .await?;
                    }

                    if preflight_snapshot_pk {
                        // For `CreateIfNotExists`, `init_queries` may have
                        // just created a table whose PRIMARY KEY we
                        // generated ourselves — that one is guaranteed to
                        // match by construction, but we still need to
                        // validate when the table already existed.  The
                        // sys.indexes query handles both cases uniformly.
                        // Defer empty/`None` key_field_names to
                        // `SqlQueryTemplate::new`'s existing
                        // `EmptyKeyFieldsForSnapshot` check, which has the
                        // canonical error message.
                        if let Some(keys) = key_field_names.filter(|k| !k.is_empty()) {
                            Self::check_snapshot_pk_matches(
                                &mut client,
                                schema_name,
                                table_name,
                                &full_table_lit,
                                keys,
                            )
                            .await?;
                        }
                    }
                    Ok::<_, WriteError>(client)
                })
            },
            is_transient_write,
            RetryConfig::default(),
            MAX_MSSQL_RETRIES,
        )?;

        let merge_query = if snapshot_mode {
            key_field_names.map(|keys| build_merge_query(&full_table_name, value_fields, keys))
        } else {
            None
        };

        let query_template = SqlQueryTemplate::new(
            snapshot_mode,
            &full_table_name,
            value_fields,
            key_field_names,
            false,
            |i| format!("@P{}", i + 1),
            Self::on_insert_conflict_condition,
            quote_identifier,
        )?;

        let value_field_types: Vec<Type> = value_fields.iter().map(|f| f.type_.clone()).collect();
        let pk_field_types: Vec<Type> = key_field_names
            .unwrap_or(&[])
            .iter()
            .filter_map(|k| {
                value_fields
                    .iter()
                    .find(|f| &f.name == k)
                    .map(|f| f.type_.clone())
            })
            .collect();

        Ok(MssqlWriter {
            runtime,
            config,
            client: Some(client),
            max_batch_size,
            buffer: Vec::new(),
            snapshot_mode,
            table_name: full_table_name,
            query_template,
            merge_query,
            value_field_types,
            pk_field_types,
        })
    }

    fn build_create_table_query(
        full_table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        include_special_fields: bool,
    ) -> Result<String, WriteError> {
        let key_set: std::collections::HashSet<&str> = key_field_names
            .map(|keys| keys.iter().map(String::as_str).collect())
            .unwrap_or_default();

        let mut columns: Vec<String> = value_fields
            .iter()
            .map(|field| {
                let is_key = key_set.contains(field.name.as_str());
                Self::mssql_data_type(&field.type_, false, is_key)
                    .map(|dtype_str| format!("{} {dtype_str}", quote_identifier(&field.name)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        if include_special_fields {
            columns.push("[time] BIGINT NOT NULL".to_string());
            columns.push("[diff] SMALLINT NOT NULL".to_string());
        }

        let primary_key_clause =
            key_field_names
                .filter(|keys| !keys.is_empty())
                .map_or(String::new(), |keys| {
                    let quoted = keys
                        .iter()
                        .map(|k| quote_identifier(k))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(", PRIMARY KEY ({quoted})")
                });

        let full_table_lit = n_string_literal(full_table_name);
        Ok(format!(
            "IF OBJECT_ID({full_table_lit}, N'U') IS NULL \
             CREATE TABLE {full_table_name} ({}{primary_key_clause})",
            columns.join(", ")
        ))
    }

    fn on_insert_conflict_condition(
        _table_name: &str,
        _key_field_names: &[String],
        _value_fields: &[ValueField],
        _legacy_mode: bool,
    ) -> String {
        // For MSSQL, we handle upserts via MERGE separately.
        // This is only used as a fallback; the actual snapshot insert
        // uses the merge_query field.
        String::new()
    }

    fn mssql_data_type(type_: &Type, is_nested: bool, is_key: bool) -> Result<String, WriteError> {
        let not_null_suffix = if is_nested { "" } else { " NOT NULL" };
        Ok(match type_ {
            Type::Bool => format!("BIT{not_null_suffix}"),
            Type::Int | Type::Duration => format!("BIGINT{not_null_suffix}"),
            Type::Float => format!("FLOAT{not_null_suffix}"),
            Type::Pointer | Type::String => {
                if is_key {
                    format!("NVARCHAR(450){not_null_suffix}")
                } else {
                    format!("NVARCHAR(MAX){not_null_suffix}")
                }
            }
            Type::Bytes | Type::PyObjectWrapper => {
                if is_key {
                    // SQL Server cannot index a VARBINARY(MAX) column, so a
                    // PRIMARY KEY constraint on it would fail at CREATE TABLE
                    // with an opaque error.  900 bytes is the largest value
                    // a key column may take in a clustered index.
                    format!("VARBINARY(900){not_null_suffix}")
                } else {
                    format!("VARBINARY(MAX){not_null_suffix}")
                }
            }
            Type::Json | Type::Tuple(_) | Type::List(_) | Type::Array(_, _) => {
                format!("NVARCHAR(MAX){not_null_suffix}")
            }
            Type::DateTimeNaive => format!("DATETIME2(6){not_null_suffix}"),
            Type::DateTimeUtc => format!("DATETIMEOFFSET(6){not_null_suffix}"),
            Type::Optional(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }
                let wrapped = Self::mssql_data_type(wrapped, true, is_key)?;
                return Ok(wrapped);
            }
            Type::Any | Type::Future(_) => return Err(WriteError::UnsupportedType(type_.clone())),
        })
    }

    fn bind_null_for_type(query: &mut Query<'_>, dtype: &Type) {
        match dtype {
            Type::Bool => query.bind(Option::<bool>::None),
            Type::Int | Type::Duration => query.bind(Option::<i64>::None),
            Type::Float => query.bind(Option::<f64>::None),
            Type::Bytes | Type::PyObjectWrapper => query.bind(Option::<Vec<u8>>::None),
            Type::DateTimeNaive => query.bind(Option::<chrono::NaiveDateTime>::None),
            Type::DateTimeUtc => {
                query.bind(Option::<chrono::DateTime<chrono::FixedOffset>>::None);
            }
            Type::Optional(inner) => Self::bind_null_for_type(query, inner),
            // String, Pointer, Json, Tuple, List, Array, and any unknown type
            _ => query.bind(Option::<String>::None),
        }
    }

    fn bind_value(query: &mut Query<'_>, value: &Value, dtype: &Type) -> Result<(), WriteError> {
        match value {
            Value::None => {
                Self::bind_null_for_type(query, dtype);
            }
            Value::Bool(b) => query.bind(*b),
            Value::Int(i) => query.bind(*i),
            Value::Float(f) => {
                let f: f64 = (*f).into();
                query.bind(f);
            }
            Value::Pointer(p) => query.bind(p.to_string()),
            Value::String(s) => query.bind(s.to_string()),
            Value::Bytes(b) => query.bind(b.to_vec()),
            Value::DateTimeNaive(dt) => {
                let ndt: chrono::NaiveDateTime = dt.as_chrono_datetime();
                query.bind(ndt);
            }
            Value::DateTimeUtc(dt) => {
                let ndt: chrono::NaiveDateTime = dt.as_chrono_datetime();
                let utc_dt: chrono::DateTime<chrono::FixedOffset> =
                    chrono::DateTime::from_naive_utc_and_offset(
                        ndt,
                        chrono::FixedOffset::east_opt(0).unwrap(),
                    );
                query.bind(utc_dt);
            }
            Value::Duration(d) => query.bind(d.microseconds()),
            Value::Json(j) => query.bind(j.to_string()),
            Value::PyObjectWrapper(_) => {
                let bytes = bincode::serialize(value).map_err(|e| *e)?;
                query.bind(bytes);
            }
            Value::Tuple(_) | Value::IntArray(_) | Value::FloatArray(_) => {
                let json_val = crate::connectors::data_format::serialize_value_to_json(value)
                    .map_err(WriteError::from)?;
                query.bind(json_val.to_string());
            }
            Value::Error | Value::Pending => {
                return Err(WriteError::from(
                    crate::connectors::data_format::FormatterError::ValueNonSerializable(
                        value.kind(),
                        "MSSQL",
                    ),
                ))
            }
        }
        Ok(())
    }

    fn flush_impl(&mut self) -> Result<(), WriteError> {
        struct PreparedRow {
            query_str: String,
            values: Vec<Value>,
            value_types: Vec<Type>,
            time: u64,
            diff: isize,
            is_delete: bool,
        }

        let buffer = take(&mut self.buffer);
        let snapshot_mode = self.snapshot_mode;
        let merge_query = self.merge_query.clone();
        let config = self.config.clone();
        let value_field_types = self.value_field_types.clone();
        let pk_field_types = self.pk_field_types.clone();

        // Pre-build query strings and extract PK values outside async block

        let mut prepared: Vec<PreparedRow> = Vec::with_capacity(buffer.len());
        for data in &buffer {
            if snapshot_mode && data.diff > 0 {
                let query_str = if let Some(ref merge_q) = merge_query {
                    build_merge_query_parameterized(merge_q, data.values.len())
                } else {
                    self.query_template.build_query(data.diff)
                };
                prepared.push(PreparedRow {
                    query_str,
                    values: data.values.clone(),
                    value_types: value_field_types.clone(),
                    time: data.time.0,
                    diff: data.diff,
                    is_delete: false,
                });
            } else if snapshot_mode && data.diff < 0 {
                let pk_values = self.query_template.primary_key_fields(data.values.clone());
                let query_str = self.query_template.build_query(-1);
                prepared.push(PreparedRow {
                    query_str,
                    values: pk_values,
                    value_types: pk_field_types.clone(),
                    time: data.time.0,
                    diff: data.diff,
                    is_delete: true,
                });
            } else {
                let query_str = self.query_template.build_query(data.diff);
                prepared.push(PreparedRow {
                    query_str,
                    values: data.values.clone(),
                    value_types: value_field_types.clone(),
                    time: data.time.0,
                    diff: data.diff,
                    is_delete: false,
                });
            }
        }

        // Take client out of self so we can pass it into the async block
        let mut client_opt = self.client.take();

        let result = self.runtime.block_on(async {
            // Reconnect if needed
            if client_opt.is_none() {
                let c = connect_mssql(&config)
                    .await
                    .map_err(|e| WriteError::Mssql(classify_mssql_error(e)))?;
                client_opt = Some(c);
            }
            let client = client_opt.as_mut().unwrap();

            // Execute all statements in a transaction
            client
                .simple_query("BEGIN TRANSACTION")
                .await
                .map_err(mssql_write_err)?
                .into_results()
                .await
                .map_err(mssql_write_err)?;

            for row in &prepared {
                let mut query = Query::new(row.query_str.clone());
                for (val, dtype) in row.values.iter().zip(row.value_types.iter()) {
                    Self::bind_value(&mut query, val, dtype)?;
                }
                // For stream-of-changes (non-snapshot, non-delete), append time and diff
                if !snapshot_mode && !row.is_delete {
                    query.bind(row.time.cast_signed());
                    #[allow(clippy::cast_possible_truncation)]
                    query.bind(row.diff as i32);
                }
                query.execute(client).await.map_err(mssql_write_err)?;
            }

            client
                .simple_query("COMMIT TRANSACTION")
                .await
                .map_err(mssql_write_err)?
                .into_results()
                .await
                .map_err(mssql_write_err)?;

            Ok::<_, WriteError>(())
        });

        if result.is_err() {
            // Drop the connection on error so we reconnect on next flush.
            // Restore the buffer so that retries have data to send.
            self.client = None;
            self.buffer = buffer;
        } else {
            // Put the client back
            self.client = client_opt;
        }

        result
    }
}

impl Writer for MssqlWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.buffer.push(data);
        if let Some(max_batch_size) = self.max_batch_size {
            if self.buffer.len() == max_batch_size {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        execute_with_retries_if(
            || self.flush_impl(),
            is_transient_write,
            RetryConfig::default(),
            MAX_MSSQL_RETRIES,
        )?;

        Ok(())
    }

    fn name(&self) -> String {
        format!("MSSQL({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}

/// SQL Server error 18456: login failed for user.
const MSSQL_ERR_LOGIN_FAILED: u32 = 18456;
/// SQL Server error 4060: cannot open database requested by the login.
const MSSQL_ERR_DATABASE_NOT_FOUND: u32 = 4060;
/// SQL Server error 208: invalid object name (table/view does not exist).
const MSSQL_ERR_INVALID_OBJECT_NAME: u32 = 208;
/// SQL Server error 313: LSN specified in the parameters to `fn_cdc_get_all_changes`
/// is not within the range of valid change-tracking LSNs for the capture
/// instance.  We only interpret this as a retention problem on the resume
/// path via [`classify_resume_cdc_error`]; everywhere else we leave it
/// unmapped so a programming bug that queries CDC with a bad LSN surfaces
/// as a generic driver error rather than a misleading retention message.
const MSSQL_ERR_LSN_OUT_OF_RANGE: u32 = 313;

/// Classify a raw tiberius error into the most specific [`MssqlError`] variant.
///
/// Well-known SQL Server error codes are mapped to named variants with
/// actionable descriptions; everything else falls back to [`MssqlError::Driver`].
#[allow(clippy::needless_pass_by_value)]
fn classify_mssql_error(e: tiberius::error::Error) -> MssqlError {
    match &e {
        tiberius::error::Error::Io { .. } | tiberius::error::Error::Tls(_) => {
            MssqlError::ConnectionFailed {
                reason: e.to_string(),
            }
        }
        tiberius::error::Error::Server(token_err) => match token_err.code() {
            MSSQL_ERR_LOGIN_FAILED => MssqlError::AuthenticationFailed {
                reason: e.to_string(),
            },
            MSSQL_ERR_DATABASE_NOT_FOUND => MssqlError::DatabaseNotFound {
                reason: e.to_string(),
            },
            MSSQL_ERR_INVALID_OBJECT_NAME => MssqlError::TableNotFound {
                reason: e.to_string(),
            },
            _ => MssqlError::Driver(e),
        },
        _ => MssqlError::Driver(e),
    }
}

/// Remap a `ReadError` coming from a resume-path CDC query: SQL Server error
/// 313 ("LSN specified is not within the range …") is translated to
/// [`MssqlError::CdcLsnOutOfRetention`] because on the resume path it
/// unambiguously means the saved LSN was dropped by the CDC cleanup job
/// between our pre-check and the actual CDC query.  All other errors pass
/// through untouched.
fn classify_resume_cdc_error(err: ReadError) -> ReadError {
    if let ReadError::Mssql(MssqlError::Driver(tiberius::error::Error::Server(ref token))) = err {
        if token.code() == MSSQL_ERR_LSN_OUT_OF_RANGE {
            return ReadError::Mssql(MssqlError::CdcLsnOutOfRetention);
        }
    }
    err
}

#[allow(clippy::needless_pass_by_value)]
fn mssql_read_err(e: tiberius::error::Error) -> ReadError {
    ReadError::Mssql(classify_mssql_error(e))
}

#[allow(clippy::needless_pass_by_value)]
fn mssql_write_err(e: tiberius::error::Error) -> WriteError {
    WriteError::Mssql(classify_mssql_error(e))
}

/// Build a MERGE statement template for snapshot upserts with @P placeholders.
fn build_merge_query(
    table_name: &str,
    value_fields: &[ValueField],
    key_field_names: &[String],
) -> String {
    let field_names: Vec<&str> = value_fields.iter().map(|f| f.name.as_str()).collect();
    let source_cols = field_names
        .iter()
        .map(|n| quote_identifier(n))
        .collect::<Vec<_>>()
        .join(", ");

    let on_clause = key_field_names
        .iter()
        .map(|k| {
            let q = quote_identifier(k);
            format!("target.{q} = source.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    let update_set = value_fields
        .iter()
        .filter(|f| !key_field_names.contains(&f.name))
        .map(|f| {
            let q = quote_identifier(&f.name);
            format!("target.{q} = source.{q}")
        })
        .collect::<Vec<_>>()
        .join(", ");

    let insert_cols = field_names
        .iter()
        .map(|n| quote_identifier(n))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_source = field_names
        .iter()
        .map(|n| format!("source.{}", quote_identifier(n)))
        .collect::<Vec<_>>()
        .join(", ");

    // When all fields are primary keys there are no non-key columns to update.
    // In that case omit the WHEN MATCHED branch: a matching row is already identical.
    let when_matched = if update_set.is_empty() {
        String::new()
    } else {
        format!("WHEN MATCHED THEN UPDATE SET {update_set} ")
    };

    // Template: {PARAMS} will be replaced with actual @P placeholders at execution time
    format!(
        "MERGE INTO {table_name} AS target \
         USING (SELECT {{PARAMS}}) AS source ({source_cols}) \
         ON {on_clause} \
         {when_matched}\
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_source});"
    )
}

/// Replace {{PARAMS}} in a MERGE template with @P1, @P2, ... placeholders.
fn build_merge_query_parameterized(merge_template: &str, param_count: usize) -> String {
    let params: Vec<String> = (1..=param_count).map(|i| format!("@P{i}")).collect();
    let params_str = params.join(", ");
    merge_template.replace("{PARAMS}", &params_str)
}

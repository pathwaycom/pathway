// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration as StdDuration;

use log::info;
use mysql::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql::binlog::row::BinlogRow;
use mysql::binlog::RowsEventFlags;
use mysql::prelude::Queryable;
pub use mysql::Error as MysqlError;
use mysql::{
    BinlogDumpFlags, BinlogRequest, Conn as MysqlRawConn, Opts as MysqlOpts, Params as MysqlParams,
    Pool as MysqlConnectionPool, PooledConn as MysqlConnection, Row as MysqlQueryRow,
    TxOpts as MysqlTxOpts, Value as MysqlValue,
};

use crate::connectors::data_format::{FormatterContext, FormatterError};
use crate::connectors::data_storage::{
    CommitPossibility, ConnectorMode, ConversionError, SqlQueryTemplate, TableWriterInitMode,
    ValuesMap,
};
use crate::connectors::metadata::MysqlMetadata;
use crate::connectors::offset::{Offset, OffsetKey, OffsetValue, EMPTY_OFFSET};
use crate::connectors::{
    DataEventType, ReadError, ReadResult, Reader, ReaderContext, StorageType, WriteError, Writer,
};
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::time::DateTime as DateTimeTrait;
use crate::engine::value::parse_pathway_pointer;
use crate::engine::{DateTimeNaive, DateTimeUtc, Duration as EngineDuration, Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries_if, RetryConfig};

const MAX_MYSQL_RETRIES: usize = 3;

pub struct MysqlWriter {
    pool: MysqlConnectionPool,
    current_connection: MysqlConnection,
    max_batch_size: Option<usize>,
    buffer: Vec<FormatterContext>,
    snapshot_mode: bool,
    table_name: String,
    query_template: SqlQueryTemplate,
}

/// Escapes a `MySQL` identifier with backticks and doubles any internal
/// backticks. Used everywhere the `MySQL` writer interpolates a
/// user-supplied table or column name into generated SQL so reserved
/// words and characters requiring quoting survive round-tripping.
fn mysql_quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Classify a `MySQL` error as transient — i.e. the operation has a
/// realistic chance of succeeding on a fresh connection or a rerun. Used
/// to gate the writer's retry loop so that permanent failures (missing
/// table, unknown column, syntax error, duplicate key, access denied, …)
/// propagate on the first attempt instead of burning several seconds of
/// backoff on a guaranteed-fail rerun. Mirrors the Postgres writer's
/// `is_transient_pg_error`.
fn is_transient_mysql_error(error: &MysqlError) -> bool {
    if let MysqlError::MySqlError(server_error) = error {
        // Server-side errors carry a SQLSTATE plus a vendor code. Retry
        // only the handful that succeed on rerun: SQLSTATE class `08`
        // (connection exception), deadlocks, lock-wait timeouts, "too
        // many connections" and "server is shutting down". Everything
        // else the server reports is deterministic and must fail fast.
        let sql_state_class = server_error.state.get(..2).unwrap_or_default();
        return sql_state_class == "08"
            || matches!(
                server_error.code,
                1205 /* ER_LOCK_WAIT_TIMEOUT */
                    | 1213 /* ER_LOCK_DEADLOCK */
                    | 1040 /* ER_CON_COUNT_ERROR */
                    | 1053 /* ER_SERVER_SHUTDOWN */
            );
    }
    // Transport / driver / TLS failures mean the stream is gone or was
    // never established; a fresh pooled connection may succeed.
    error.is_connectivity_error()
}

fn is_transient_mysql_write(error: &WriteError) -> bool {
    matches!(error, WriteError::Mysql(e) if is_transient_mysql_error(e))
}

impl MysqlWriter {
    pub fn new(
        pool: MysqlConnectionPool,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        mode: TableWriterInitMode,
    ) -> Result<MysqlWriter, WriteError> {
        // Interpolating `table_name` raw would reject reserved words and
        // characters requiring backticks; pre-quote once so the shared
        // template builders see the safe form, mirroring how the
        // Postgres writer handles it.
        let quoted_table_name = mysql_quote_identifier(table_name);
        let mut connection = pool.get_conn()?;
        let mut transaction = connection.start_transaction(MysqlTxOpts::default())?;
        mode.initialize(
            &quoted_table_name,
            value_fields,
            key_field_names,
            !snapshot_mode,
            |query| {
                transaction.query_drop(query)?;
                Ok(())
            },
            Self::mysql_data_type,
            mysql_quote_identifier,
        )?;
        transaction.commit()?;

        let writer = MysqlWriter {
            current_connection: pool.get_conn()?,
            pool,
            max_batch_size,
            snapshot_mode,
            table_name: table_name.to_string(),
            query_template: SqlQueryTemplate::new(
                snapshot_mode,
                &quoted_table_name,
                value_fields,
                key_field_names,
                false,
                |_| "?".to_string(),
                Self::on_insert_conflict_condition,
                mysql_quote_identifier,
            )?,
            buffer: Vec::new(),
        };

        Ok(writer)
    }

    // Generates a statement that is used in "on conflict" part of the query
    fn on_insert_conflict_condition(
        _table_name: &str,
        _key_field_names: &[String],
        value_fields: &[ValueField],
        _legacy_mode: bool,
    ) -> String {
        let update_pairs = value_fields
            .iter()
            .map(|field| format!("{name}=new.{name}", name = field.name))
            .collect::<Vec<_>>();

        format!("AS new ON DUPLICATE KEY UPDATE {}", update_pairs.join(", "))
    }

    fn mysql_data_type(type_: &Type, is_nested: bool) -> Result<String, WriteError> {
        let not_null_suffix = if is_nested { "" } else { " NOT NULL" };
        Ok(match type_ {
            Type::Bool => format!("BOOLEAN{not_null_suffix}"),
            Type::Int => format!("BIGINT{not_null_suffix}"),
            Type::Float => format!("DOUBLE{not_null_suffix}"),
            Type::Pointer | Type::String => format!("TEXT{not_null_suffix}"),
            Type::Bytes | Type::PyObjectWrapper => format!("BLOB{not_null_suffix}"),
            Type::Json => format!("JSON{not_null_suffix}"),
            Type::Duration => format!("TIME(6){not_null_suffix}"),
            Type::DateTimeNaive | Type::DateTimeUtc => format!("DATETIME(6){not_null_suffix}"),
            Type::Optional(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }
                let wrapped = Self::mysql_data_type(wrapped, true)?;
                return Ok(wrapped);
            }
            Type::Any | Type::Tuple(_) | Type::List(_) | Type::Array(_, _) | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()))
            }
        })
    }

    fn to_mysql_date(dt: DateTimeNaive) -> MysqlValue {
        MysqlValue::Date(
            dt.year().try_into().expect("years must fit u16"),
            dt.month().try_into().expect("months must fit u8"),
            dt.day().try_into().expect("days must fit u8"),
            dt.hour().try_into().expect("hours must fit u8"),
            dt.minute().try_into().expect("minutes must fit u8"),
            dt.second().try_into().expect("seconds must fit u8"),
            dt.microsecond()
                .try_into()
                .expect("microseconds must fit u32"),
        )
    }

    fn to_mysql_value(value: &Value) -> Result<MysqlValue, WriteError> {
        match value {
            Value::None => Ok(MysqlValue::NULL),
            Value::Bool(b) => Ok(MysqlValue::from(b)),
            Value::Int(i) => Ok(MysqlValue::Int(*i)),
            Value::Float(f) => Ok(MysqlValue::Double((*f).into())),
            Value::Pointer(p) => Ok(MysqlValue::Bytes(p.to_string().into())),
            Value::String(s) => Ok(MysqlValue::Bytes(s.to_string().into())),
            Value::Bytes(b) => Ok(MysqlValue::Bytes(b.to_vec())),
            Value::DateTimeNaive(dt) => Ok(Self::to_mysql_date(*dt)),
            Value::DateTimeUtc(dt) => {
                Ok(Self::to_mysql_date(dt.to_naive_in_timezone("UTC").unwrap()))
            }
            Value::Duration(d) => {
                let is_negative = d < &EngineDuration::new(0);
                let mut total_microseconds = d.microseconds();
                if is_negative {
                    total_microseconds *= -1;
                }
                let microseconds: u32 = (total_microseconds % 1_000_000)
                    .try_into()
                    .expect("microsecond part must fit u32");

                let total_seconds = total_microseconds / 1_000_000;
                let seconds: u8 = (total_seconds % 60)
                    .try_into()
                    .expect("second part must fit u8");

                let total_minutes = total_seconds / 60;
                let minutes: u8 = (total_minutes % 60)
                    .try_into()
                    .expect("minute part must fit u8");

                let total_hours = total_minutes / 60;
                let hours: u8 = (total_hours % 24)
                    .try_into()
                    .expect("hour part must fit u8");

                let total_days = total_hours / 24;
                let days: u32 = total_days.try_into().expect("day part must fit u32");
                Ok(MysqlValue::Time(
                    is_negative,
                    days,
                    hours,
                    minutes,
                    seconds,
                    microseconds,
                ))
            }
            Value::Json(j) => Ok(MysqlValue::Bytes(j.to_string().into())),
            Value::PyObjectWrapper(_) => Ok(MysqlValue::Bytes(
                bincode::serialize(value).map_err(|e| *e)?,
            )),
            Value::IntArray(_)
            | Value::FloatArray(_)
            | Value::Tuple(_)
            | Value::Error
            | Value::Pending => Err(FormatterError::ValueNonSerializable(value.kind(), "MySQL"))?,
        }
    }

    fn flush_with_current_connection(&mut self) -> Result<(), WriteError> {
        let mut transaction = self
            .current_connection
            .start_transaction(MysqlTxOpts::default())?;
        for data in &self.buffer {
            let mut params: Vec<_> = data
                .values
                .iter()
                .map(Self::to_mysql_value)
                .collect::<Result<_, _>>()?;
            let params = if self.snapshot_mode {
                if data.diff > 0 {
                    params
                } else {
                    self.query_template.primary_key_fields(params)
                }
            } else {
                params.push(MysqlValue::Int(data.time.0.try_into().unwrap()));
                params.push(MysqlValue::Int(data.diff.try_into().unwrap()));
                params
            };
            transaction.exec_drop(
                self.query_template.build_query(data.diff),
                MysqlParams::Positional(params),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

impl Writer for MysqlWriter {
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
            || {
                let cc_save_result = self.flush_with_current_connection();
                // Only replace the connection when the failure is
                // transient and a retry will follow; a permanent error
                // (missing table, bad SQL, …) is about to propagate, so
                // reconnecting would just open an unused connection.
                if cc_save_result
                    .as_ref()
                    .err()
                    .is_some_and(is_transient_mysql_write)
                {
                    self.current_connection = self.pool.get_conn()?;
                }
                cc_save_result
            },
            is_transient_mysql_write,
            RetryConfig::default(),
            MAX_MYSQL_RETRIES,
        )?;
        self.buffer.clear();

        Ok(())
    }

    fn name(&self) -> String {
        format!("MySQL({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}

/// How often the streaming reader re-opens a non-blocking binlog dump to pick
/// up changes that arrived since the last poll. Mirrors the MSSQL CDC reader's
/// 500 ms cadence.
const BINLOG_POLL_INTERVAL: StdDuration = StdDuration::from_millis(500);

/// Upper bound on the number of row changes drained into a single
/// `NewSource … FinishedSource` block per poll. A large backlog (for example a
/// resume after long downtime) is then delivered in bounded chunks instead of
/// one unbounded in-memory block. The cap is applied at binlog *event*
/// boundaries: `UPDATE_ROWS` events carry both the before- and after-image of a
/// row in a single event, so a chunk never splits an update's delete/insert
/// pair, and resuming from a chunk boundary is always safe under the UPSERT
/// session semantics.
const MAX_ROWS_PER_BLOCK: usize = 10_000;

/// The number of consecutive transient read errors (connection blips while
/// polling the binlog, etc.) the engine tolerates before aborting. Each retry
/// re-opens the dump from the last committed position, so retries are
/// idempotent.
const MAX_MYSQL_READ_RETRIES: usize = 8;

/// Errors specific to the `MySQL` input connector.
///
/// A dedicated enum (rather than wrapping the raw driver error) lets the engine
/// surface actionable messages for the misconfigurations users most commonly
/// hit when setting up binlog-based change data capture.
#[derive(Debug, thiserror::Error)]
pub enum MysqlReaderError {
    /// General `MySQL` driver or server error not covered by a more specific variant.
    #[error(transparent)]
    Driver(#[from] MysqlError),

    /// The connection string does not name a database, so the table cannot be
    /// qualified. `MySQL` connection strings carry the database after the host,
    /// e.g. `mysql://user:pass@host:3306/mydb`.
    #[error(
        "the MySQL connection string does not specify a database; \
         pw.io.mysql.read needs one to qualify the table, e.g. \
         mysql://user:password@host:3306/your_database"
    )]
    DatabaseNotSpecified,

    /// Binary logging is disabled on the server (`log_bin = OFF`).
    #[error(
        "binary logging is disabled on the MySQL server (`log_bin` is OFF), so \
         streaming change capture is impossible. Start mysqld with --log-bin (and \
         restart the server), or use mode=\"static\" for a one-off snapshot read."
    )]
    BinlogDisabled,

    /// `binlog_format` is not `ROW`.
    #[error(
        "MySQL `binlog_format` is {0:?}, but pw.io.mysql.read requires ROW-based \
         binary logging — STATEMENT and MIXED do not record per-row images and \
         cannot be turned into change events. Set `binlog_format=ROW` \
         (SET GLOBAL binlog_format=ROW, or binlog_format=ROW in my.cnf)."
    )]
    BinlogFormatNotRow(String),

    /// `binlog_row_image` is not `FULL`.
    #[error(
        "MySQL `binlog_row_image` is {0:?}, but pw.io.mysql.read requires FULL row \
         images so that every column of a changed row can be reconstructed from \
         the binary log. Set `binlog_row_image=FULL` (the server default)."
    )]
    BinlogRowImageNotFull(String),

    /// The current binary-log coordinates could not be read.
    #[error(
        "could not read the current binary-log position from MySQL ({0}); the \
         connecting user needs the REPLICATION CLIENT privilege \
         (GRANT REPLICATION CLIENT ON *.* TO <user>)."
    )]
    MasterStatusUnavailable(String),

    /// The target table does not exist in the configured database.
    #[error("table {table:?} was not found in MySQL database {database:?}")]
    TableNotFound { database: String, table: String },

    /// The schema declares columns absent from the source table.
    #[error(
        "schema column(s) {missing:?} declared by pw.io.mysql.read are missing \
         from table {table:?}; rename the schema columns to match the source \
         table, or ALTER TABLE to add them"
    )]
    SchemaColumnsMissing { table: String, missing: Vec<String> },

    /// The persisted binary-log file is no longer present on the server.
    #[error(
        "the persisted binary-log file {filename:?} is no longer present on the \
         MySQL server — it was purged by the server's normal binary-log expiry \
         (binlog_expire_logs_seconds / expire_logs_days), which runs \
         independently of any reader. The change history needed to resume from \
         the saved position is gone. To recover, delete the persistence directory \
         and restart, which triggers a full table rescan; pick a binary-log \
         retention long enough to cover your longest expected downtime."
    )]
    BinlogPurged { filename: String },
}

fn mysql_read_err(error: MysqlError) -> ReadError {
    ReadError::Mysql(MysqlReaderError::Driver(error))
}

fn conversion_error(value: &MysqlValue, field_name: &str, dtype: &Type) -> ConversionError {
    let value_repr = limit_length(format!("{value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
    ConversionError::new(value_repr, field_name.to_owned(), dtype.clone(), None)
}

fn value_bytes(value: &MysqlValue) -> Option<&[u8]> {
    match value {
        MysqlValue::Bytes(bytes) => Some(bytes),
        _ => None,
    }
}

/// Reconstruct a [`DateTimeNaive`] from a `MySQL` `DATETIME(6)` value — the form
/// [`MysqlWriter`] writes `DateTimeNaive` and `DateTimeUtc` into.
fn mysql_value_to_naive(value: &MysqlValue) -> Option<Value> {
    if let MysqlValue::Date(year, month, day, hour, minute, second, micros) = value {
        let date =
            chrono::NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day))?;
        let datetime = date.and_hms_micro_opt(
            u32::from(*hour),
            u32::from(*minute),
            u32::from(*second),
            *micros,
        )?;
        DateTimeNaive::try_from(datetime)
            .ok()
            .map(Value::DateTimeNaive)
    } else {
        None
    }
}

/// Reconstruct a [`DateTimeUtc`] from a `MySQL` `DATETIME(6)` value. The writer
/// converts a `DateTimeUtc` to its naive UTC wall-clock before storing, so the
/// stored value is interpreted back as UTC here.
fn mysql_value_to_utc(value: &MysqlValue) -> Option<Value> {
    if let MysqlValue::Date(year, month, day, hour, minute, second, micros) = value {
        let date =
            chrono::NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day))?;
        let datetime = date.and_hms_micro_opt(
            u32::from(*hour),
            u32::from(*minute),
            u32::from(*second),
            *micros,
        )?;
        DateTimeUtc::try_from(datetime.and_utc())
            .ok()
            .map(Value::DateTimeUtc)
    } else {
        None
    }
}

/// Reconstruct an [`EngineDuration`] from a `MySQL` `TIME(6)` value — the form
/// [`MysqlWriter`] writes `Duration` into. `MySQL`'s `TIME` range is limited to
/// ±838:59:59, so durations outside that range do not round-trip.
fn mysql_value_to_duration(value: &MysqlValue) -> Option<Value> {
    if let MysqlValue::Time(is_negative, days, hours, minutes, seconds, micros) = value {
        let total_micros = i64::from(*days) * 86_400_000_000
            + i64::from(*hours) * 3_600_000_000
            + i64::from(*minutes) * 60_000_000
            + i64::from(*seconds) * 1_000_000
            + i64::from(*micros);
        let total_micros = if *is_negative {
            -total_micros
        } else {
            total_micros
        };
        EngineDuration::new_with_unit(total_micros, "us")
            .ok()
            .map(Value::Duration)
    } else {
        None
    }
}

/// Convert a single `MySQL` value into a Pathway [`Value`] of the requested
/// [`Type`]. This is the exact inverse of [`MysqlWriter::to_mysql_value`] /
/// [`MysqlWriter::mysql_data_type`], so the value scheme round-trips through
/// `pw.io.mysql.write` → `pw.io.mysql.read`.
///
/// Both the snapshot reader (binary protocol via `exec`) and the binlog reader
/// hand typed [`MysqlValue`]s here, so the same conversion serves both paths.
#[allow(clippy::too_many_lines)]
fn convert_mysql_value(
    value: &MysqlValue,
    field_name: &str,
    dtype: &Type,
) -> Result<Value, Box<ConversionError>> {
    let inner_dtype = match dtype {
        Type::Optional(inner) => inner.as_ref(),
        other => other,
    };
    let is_optional = matches!(dtype, Type::Optional(_));

    if matches!(value, MysqlValue::NULL) {
        if is_optional || matches!(inner_dtype, Type::Any) {
            return Ok(Value::None);
        }
        return Err(Box::new(conversion_error(value, field_name, dtype)));
    }

    let converted: Option<Value> = match inner_dtype {
        // A MySQL `BOOLEAN`/`TINYINT(1)` arrives as an integer; the `Bytes`
        // branch covers the text representation a `DECIMAL`/`NUMERIC` column
        // would yield.
        Type::Bool => match value {
            MysqlValue::Int(int_value) => Some(Value::Bool(*int_value != 0)),
            MysqlValue::UInt(uint_value) => Some(Value::Bool(*uint_value != 0)),
            MysqlValue::Bytes(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|string| string.trim().parse::<i64>().ok())
                .map(|int_value| Value::Bool(int_value != 0)),
            _ => None,
        },
        // The `Bytes` branch lets a scale-0 `DECIMAL`/`NUMERIC` column (returned
        // as a text literal) be declared as `int`.
        Type::Int => match value {
            MysqlValue::Int(int_value) => Some(Value::Int(*int_value)),
            MysqlValue::UInt(uint_value) => i64::try_from(*uint_value).ok().map(Value::Int),
            MysqlValue::Bytes(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|string| string.trim().parse::<i64>().ok())
                .map(Value::Int),
            _ => None,
        },
        // Accept every MySQL numeric form for a `Float` column. Integers and
        // 32-bit floats are widened to f64; values above 2**53 lose precision,
        // matching IEEE-754 semantics — the same allowance the MSSQL reader makes.
        // The `Bytes` branch covers `DECIMAL`/`NUMERIC` columns, whose textual
        // representation is parsed as a float.
        Type::Float => match value {
            MysqlValue::Double(double_value) => Some(Value::Float((*double_value).into())),
            MysqlValue::Float(float_value) => Some(Value::Float(f64::from(*float_value).into())),
            #[allow(clippy::cast_precision_loss)]
            MysqlValue::Int(int_value) => Some(Value::Float((*int_value as f64).into())),
            #[allow(clippy::cast_precision_loss)]
            MysqlValue::UInt(uint_value) => Some(Value::Float((*uint_value as f64).into())),
            MysqlValue::Bytes(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|string| string.trim().parse::<f64>().ok())
                .map(|float_value| Value::Float(float_value.into())),
            _ => None,
        },
        Type::String => value_bytes(value)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map(|string| Value::String(string.into())),
        Type::Pointer => value_bytes(value)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|string| parse_pathway_pointer(string).ok()),
        Type::Bytes => value_bytes(value).map(|bytes| Value::Bytes(bytes.to_vec().into())),
        Type::Json => value_bytes(value)
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            .map(Value::from),
        Type::DateTimeNaive => mysql_value_to_naive(value),
        Type::DateTimeUtc => mysql_value_to_utc(value),
        Type::Duration => mysql_value_to_duration(value),
        // PyObjectWrapper values are written as a bincode-serialized `Value`
        // (see `MysqlWriter::to_mysql_value`); deserialize the BLOB back.
        Type::PyObjectWrapper => {
            value_bytes(value).and_then(|bytes| bincode::deserialize::<Value>(bytes).ok())
        }
        _ => None,
    };

    converted.ok_or_else(|| Box::new(conversion_error(value, field_name, dtype)))
}

/// Unified `MySQL` reader for both static and streaming (binlog) modes.
///
/// In [`ConnectorMode::Static`] the connector issues a plain `SELECT`, emits
/// every row as an insert, and returns [`ReadResult::Finished`]. In
/// [`ConnectorMode::Streaming`] it captures the current binary-log coordinates,
/// reads a snapshot, then tails the binary log: each poll opens a *non-blocking*
/// `COM_BINLOG_DUMP` from the last committed coordinates, drains every change up
/// to the current end of the log, and delivers them as one atomic block stamped
/// with the resulting coordinates.
pub struct MysqlReader {
    opts: MysqlOpts,
    database: String,
    table_name: String,
    schema: Vec<(String, Type)>,
    key_column_names: Vec<String>,
    mode: ConnectorMode,
    server_id: u32,
    snapshot: Vec<ReadResult>,
    is_initialized: bool,
    snapshot_version: u64,
    /// Physical column order of the source table (lower-cased), in
    /// `ORDINAL_POSITION` order. Binlog row images are positional in this order,
    /// so this is how a binlog row is mapped back to named schema fields.
    physical_columns: Vec<String>,
    /// For each schema field (in `schema` order), its index into a full binlog
    /// row image. Computed once at initialization from `physical_columns`.
    column_indices: Vec<usize>,
    /// Binary-log coordinates consumed so far. The next poll resumes here.
    binlog_filename: String,
    binlog_position: u64,
    /// Persisted coordinates to resume from, populated by `seek()` when
    /// persistence is enabled.
    saved_position: Option<(String, u64)>,
}

impl MysqlReader {
    pub fn new(
        opts: MysqlOpts,
        table_name: String,
        schema: Vec<(String, Type)>,
        key_field_names: Option<Vec<String>>,
        mode: ConnectorMode,
        server_id: u32,
    ) -> Result<Self, ReadError> {
        let database = opts
            .get_db_name()
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .ok_or(ReadError::Mysql(MysqlReaderError::DatabaseNotSpecified))?;
        let key_column_names = key_field_names
            .unwrap_or_else(|| schema.iter().map(|(name, _)| name.clone()).collect());
        Ok(Self {
            opts,
            database,
            table_name,
            schema,
            key_column_names,
            mode,
            server_id,
            snapshot: Vec::new(),
            is_initialized: false,
            snapshot_version: 0,
            physical_columns: Vec::new(),
            column_indices: Vec::new(),
            binlog_filename: String::new(),
            binlog_position: 0,
            saved_position: None,
        })
    }

    fn connect(&self) -> Result<MysqlRawConn, ReadError> {
        MysqlRawConn::new(self.opts.clone()).map_err(mysql_read_err)
    }

    /// Load the table's physical column order (`information_schema.COLUMNS`),
    /// verifying that the table exists and that every schema column is present.
    /// Populates `physical_columns` and `column_indices`.
    fn load_physical_columns(&mut self, conn: &mut MysqlRawConn) -> Result<(), ReadError> {
        let query = "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                     ORDER BY ORDINAL_POSITION";
        let columns: Vec<String> = conn
            .exec(query, (self.database.clone(), self.table_name.clone()))
            .map_err(mysql_read_err)?;
        if columns.is_empty() {
            return Err(ReadError::Mysql(MysqlReaderError::TableNotFound {
                database: self.database.clone(),
                table: self.table_name.clone(),
            }));
        }
        let physical_columns: Vec<String> =
            columns.iter().map(|name| name.to_lowercase()).collect();

        let missing: Vec<String> = self
            .schema
            .iter()
            .map(|(name, _)| name.clone())
            .filter(|name| !physical_columns.contains(&name.to_lowercase()))
            .collect();
        if !missing.is_empty() {
            return Err(ReadError::Mysql(MysqlReaderError::SchemaColumnsMissing {
                table: self.table_name.clone(),
                missing,
            }));
        }

        self.column_indices = self
            .schema
            .iter()
            .map(|(name, _)| {
                physical_columns
                    .iter()
                    .position(|column| column == &name.to_lowercase())
                    .expect("schema columns verified to be present above")
            })
            .collect();
        self.physical_columns = physical_columns;
        Ok(())
    }

    /// Verify that binary logging is enabled, ROW-based, and uses FULL row
    /// images — the prerequisites for streaming change capture.
    fn check_binlog_settings(conn: &mut MysqlRawConn) -> Result<(), ReadError> {
        let log_bin: Option<i64> = conn
            .query_first("SELECT @@GLOBAL.log_bin")
            .map_err(mysql_read_err)?;
        if log_bin != Some(1) {
            return Err(ReadError::Mysql(MysqlReaderError::BinlogDisabled));
        }

        let binlog_format: Option<String> = conn
            .query_first("SELECT @@GLOBAL.binlog_format")
            .map_err(mysql_read_err)?;
        let binlog_format = binlog_format.unwrap_or_default();
        if !binlog_format.eq_ignore_ascii_case("ROW") {
            return Err(ReadError::Mysql(MysqlReaderError::BinlogFormatNotRow(
                binlog_format,
            )));
        }

        // `binlog_row_image` exists on MySQL 5.6+/MariaDB and defaults to FULL.
        // Treat an error reading it (very old servers) as "FULL" rather than
        // failing the preflight on a variable that doesn't exist.
        if let Ok(Some(row_image)) =
            conn.query_first::<String, _>("SELECT @@GLOBAL.binlog_row_image")
        {
            if !row_image.eq_ignore_ascii_case("FULL") {
                return Err(ReadError::Mysql(MysqlReaderError::BinlogRowImageNotFull(
                    row_image,
                )));
            }
        }
        Ok(())
    }

    /// Read the current binary-log coordinates. Uses `SHOW MASTER STATUS`
    /// (`MySQL` ≤ 8.3) and falls back to `SHOW BINARY LOG STATUS` (`MySQL` ≥ 8.4,
    /// where the former was removed).
    fn capture_master_position(conn: &mut MysqlRawConn) -> Result<(String, u64), ReadError> {
        let row: Option<MysqlQueryRow> = match conn.query_first("SHOW MASTER STATUS") {
            Ok(row) => row,
            Err(_) => conn
                .query_first("SHOW BINARY LOG STATUS")
                .map_err(|error| {
                    ReadError::Mysql(MysqlReaderError::MasterStatusUnavailable(error.to_string()))
                })?,
        };
        let mut row = row.ok_or_else(|| {
            ReadError::Mysql(MysqlReaderError::MasterStatusUnavailable(
                "the server returned an empty status row; is binary logging enabled?".to_owned(),
            ))
        })?;
        let filename: String = row.take(0).flatten().ok_or_else(|| {
            ReadError::Mysql(MysqlReaderError::MasterStatusUnavailable(
                "the status row has no File column".to_owned(),
            ))
        })?;
        let position: u64 = row.take(1).flatten().ok_or_else(|| {
            ReadError::Mysql(MysqlReaderError::MasterStatusUnavailable(
                "the status row has no Position column".to_owned(),
            ))
        })?;
        Ok((filename, position))
    }

    /// Confirm the saved binary-log file is still present on the server before
    /// resuming from it; otherwise the change history we need has been purged.
    // TODO(reset-master): this is a name-only check. `RESET MASTER` wipes the
    // logs and restarts numbering at `.000001`, regenerating a file with the
    // same name our saved offset points into; the name match then passes and we
    // resume into an unrelated file at a stale byte offset. `SHOW BINARY LOGS`
    // also returns a `File_size` column — additionally verify the saved position
    // does not exceed it and fail loudly when it does, to catch that case.
    fn verify_binlog_file_present(
        conn: &mut MysqlRawConn,
        filename: &str,
    ) -> Result<(), ReadError> {
        let rows: Vec<MysqlQueryRow> = conn.query("SHOW BINARY LOGS").map_err(mysql_read_err)?;
        let present = rows.iter().any(|row| {
            row.get::<String, _>(0)
                .is_some_and(|log_name| log_name == filename)
        });
        if !present {
            return Err(ReadError::Mysql(MysqlReaderError::BinlogPurged {
                filename: filename.to_owned(),
            }));
        }
        Ok(())
    }

    fn extract_key(
        values: &HashMap<String, Result<Value, Box<ConversionError>>>,
        key_column_names: &[String],
    ) -> Vec<Value> {
        key_column_names
            .iter()
            .map(|name| {
                values
                    .get(name)
                    .and_then(|result| result.as_ref().ok().cloned())
                    .unwrap_or(Value::None)
            })
            .collect()
    }

    /// Convert a snapshot `SELECT` row (binary protocol, so values are typed) to
    /// a per-column value map keyed by schema field name. The `SELECT` lists
    /// columns in `schema` order, so column index `i` is `schema[i]`.
    fn select_row_to_values(
        &self,
        row: &MysqlQueryRow,
    ) -> HashMap<String, Result<Value, Box<ConversionError>>> {
        let mut values = HashMap::with_capacity(self.schema.len());
        for (index, (field_name, dtype)) in self.schema.iter().enumerate() {
            let converted = match row.as_ref(index) {
                Some(value) => convert_mysql_value(value, field_name, dtype),
                None => Err(Box::new(ConversionError::new(
                    "<missing column in SELECT result>".to_owned(),
                    field_name.clone(),
                    dtype.clone(),
                    None,
                ))),
            };
            values.insert(field_name.clone(), converted);
        }
        values
    }

    /// Convert a full binlog row image to a per-column value map keyed by schema
    /// field name. Binlog row images are positional in physical table order, so
    /// each schema field is read from its precomputed `column_indices` slot.
    fn binlog_row_to_values(
        &self,
        row: &BinlogRow,
    ) -> HashMap<String, Result<Value, Box<ConversionError>>> {
        let mut values = HashMap::with_capacity(self.schema.len());
        for ((field_name, dtype), &physical_index) in self.schema.iter().zip(&self.column_indices) {
            let converted = match row.as_ref(physical_index) {
                Some(binlog_value) => match MysqlValue::try_from(binlog_value.clone()) {
                    Ok(value) => convert_mysql_value(&value, field_name, dtype),
                    Err(_) => Err(Box::new(ConversionError::new(
                        "<unconvertible binlog value>".to_owned(),
                        field_name.clone(),
                        dtype.clone(),
                        None,
                    ))),
                },
                None => Err(Box::new(ConversionError::new(
                    "<column missing from binlog row image; require binlog_row_image=FULL>"
                        .to_owned(),
                    field_name.clone(),
                    dtype.clone(),
                    None,
                ))),
            };
            values.insert(field_name.clone(), converted);
        }
        values
    }

    /// Read the full table snapshot via a prepared (binary-protocol) `SELECT`,
    /// returning one insert change per row.
    fn load_snapshot(
        &self,
        conn: &mut MysqlRawConn,
    ) -> Result<Vec<(DataEventType, Vec<Value>, ValuesMap)>, ReadError> {
        let columns = self
            .schema
            .iter()
            .map(|(name, _)| mysql_quote_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT {columns} FROM {}.{}",
            mysql_quote_identifier(&self.database),
            mysql_quote_identifier(&self.table_name),
        );
        let rows: Vec<MysqlQueryRow> = conn.exec(query, ()).map_err(mysql_read_err)?;
        let mut changes = Vec::with_capacity(rows.len());
        for row in &rows {
            let values = self.select_row_to_values(row);
            let key = Self::extract_key(&values, &self.key_column_names);
            changes.push((DataEventType::Insert, key, values.into()));
        }
        Ok(changes)
    }

    fn is_our_table(&self, table_map_event: &TableMapEvent) -> bool {
        table_map_event
            .database_name()
            .eq_ignore_ascii_case(&self.database)
            && table_map_event
                .table_name()
                .eq_ignore_ascii_case(&self.table_name)
    }

    /// Translate a binlog rows event into ordered change records. A
    /// `WRITE_ROWS` event yields one insert per row, `DELETE_ROWS` one delete,
    /// and `UPDATE_ROWS` a delete of the before-image followed by an insert of
    /// the after-image (so a primary-key change is handled correctly).
    fn collect_row_changes(
        &self,
        rows_event: &RowsEventData,
        table_map_event: &TableMapEvent,
        out: &mut Vec<(DataEventType, Vec<Value>, ValuesMap)>,
    ) -> Result<(), ReadError> {
        for row_result in rows_event.rows(table_map_event) {
            // A row image we cannot decode means our view of the table is no
            // longer trustworthy: fail loudly rather than silently dropping the
            // row, which would lose a change with no signal to the user.
            let (before_image, after_image) =
                row_result.map_err(|error| mysql_read_err(error.into()))?;
            if let Some(before) = &before_image {
                let values = self.binlog_row_to_values(before);
                let key = Self::extract_key(&values, &self.key_column_names);
                out.push((DataEventType::Delete, key, values.into()));
            }
            if let Some(after) = &after_image {
                let values = self.binlog_row_to_values(after);
                let key = Self::extract_key(&values, &self.key_column_names);
                out.push((DataEventType::Insert, key, values.into()));
            }
        }
        Ok(())
    }

    fn binlog_offset(filename: &str, position: u64) -> Offset {
        (
            OffsetKey::Mysql,
            OffsetValue::MysqlBinlogPos {
                filename: filename.to_owned(),
                position,
            },
        )
    }

    /// Wrap a batch of changes in a `NewSource … FinishedSource` block, stamped
    /// with `offset`, ordered so that `Vec::pop` serves them in sequence.
    fn build_block(
        &mut self,
        changes: Vec<(DataEventType, Vec<Value>, ValuesMap)>,
        offset: &Offset,
    ) -> Vec<ReadResult> {
        self.snapshot_version += 1;
        let mut results = Vec::with_capacity(changes.len() + 2);
        results.push(ReadResult::NewSource(
            MysqlMetadata::new(self.snapshot_version).into(),
        ));
        for (event_type, key, values) in changes {
            results.push(ReadResult::Data(
                ReaderContext::from_diff(event_type, Some(key), values),
                offset.clone(),
            ));
        }
        results.push(ReadResult::FinishedSource {
            commit_possibility: CommitPossibility::Possible,
        });
        results.reverse();
        results
    }

    /// Initialize the reader: validate columns and (for streaming) binlog
    /// settings, then either resume from a persisted position or load a snapshot.
    fn initialize(&mut self) -> Result<(), ReadError> {
        let mut conn = self.connect()?;
        self.load_physical_columns(&mut conn)?;

        let streaming = self.mode.is_polling_enabled();
        if streaming {
            Self::check_binlog_settings(&mut conn)?;
        }

        // Resume path: a previous run persisted binary-log coordinates. Verify
        // the file still exists, then go straight to streaming from there — the
        // snapshot is already reflected in the restored engine state.
        //
        // Clone (rather than `take`) so a failed resume re-hits the same error
        // on every engine retry. If the saved binary log has been purged,
        // `verify_binlog_file_present` errors; were the position cleared on the
        // first attempt, the retry would silently fall through to a cold-start
        // re-snapshot — masking the data gap instead of failing loudly. Clear
        // it only once the resume has actually succeeded. (Mirrors the MSSQL
        // reader's handling of its persisted LSN.)
        if let Some((filename, position)) = self.saved_position.clone() {
            Self::verify_binlog_file_present(&mut conn, &filename)?;
            self.binlog_filename = filename;
            self.binlog_position = position;
            self.saved_position = None;
            info!(
                "MySQL reader resuming from persisted binlog position {}:{}",
                self.binlog_filename, self.binlog_position
            );
            self.is_initialized = true;
            return Ok(());
        }

        // Cold start. For streaming, record the binary-log position *before* the
        // snapshot SELECT so the first poll replays everything that happened
        // during/after the snapshot; under the UPSERT session those changes
        // reconcile idempotently with the snapshot rows.
        let (filename, position) = if streaming {
            Self::capture_master_position(&mut conn)?
        } else {
            (String::new(), 0)
        };

        let snapshot_changes = self.load_snapshot(&mut conn)?;
        let offset = if streaming {
            Self::binlog_offset(&filename, position)
        } else {
            EMPTY_OFFSET
        };
        self.snapshot = self.build_block(snapshot_changes, &offset);

        self.binlog_filename = filename;
        self.binlog_position = position;
        if streaming {
            info!(
                "MySQL reader initialized snapshot for table '{}.{}', tailing binlog from {}:{}",
                self.database, self.table_name, self.binlog_filename, self.binlog_position
            );
        }
        self.is_initialized = true;
        Ok(())
    }

    /// Open a non-blocking binlog dump from the current coordinates, drain all
    /// changes (up to `MAX_ROWS_PER_BLOCK`, rounded up to the end of the current
    /// statement so a resume never lands mid-statement), and, if any affected our
    /// table, push them into `self.snapshot` as one atomic block stamped with the
    /// last statement-boundary coordinates.
    // TODO(per-transaction-blocks): emit one `NewSource … FinishedSource` block
    // per source transaction (bounded by the `XID`/COMMIT event), as the Postgres
    // reader does for each `BEGIN … COMMIT`, instead of one block spanning up to
    // `MAX_ROWS_PER_BLOCK` ending at a statement boundary. That gives full
    // transaction-level atomicity (every diff of a transaction in one minibatch).
    fn poll_binlog(&mut self) -> Result<(), ReadError> {
        let conn = self.connect()?;
        let request = BinlogRequest::new(self.server_id)
            .with_filename(self.binlog_filename.as_bytes().to_vec())
            .with_pos(self.binlog_position)
            .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);
        let stream = conn.get_binlog_stream(request).map_err(mysql_read_err)?;

        // Track coordinates locally and only commit them back to `self` after a
        // clean drain, so a mid-drain error simply replays from the last
        // committed position on the next attempt.
        let mut filename = self.binlog_filename.clone();
        let mut position = self.binlog_position;
        // The last position at which no statement is open. A statement's rows can
        // be split across several rows events sharing one `TableMapEvent` (sent
        // once, valid until the rows event with `STMT_END`); a dump that starts
        // mid-statement is not re-sent that map, so committing a mid-statement
        // position would silently drop the rest of the statement on resume. We
        // therefore only ever commit (and only break) at a statement boundary.
        let mut safe_filename = self.binlog_filename.clone();
        let mut safe_position = self.binlog_position;
        let mut in_statement = false;
        let mut table_maps: HashMap<u64, TableMapEvent<'static>> = HashMap::new();
        let mut changes: Vec<(DataEventType, Vec<Value>, ValuesMap)> = Vec::new();

        for event_result in stream {
            let event = event_result.map_err(mysql_read_err)?;
            let log_pos = u64::from(event.header().log_pos());
            let Some(event_data) = event
                .read_data()
                .map_err(|error| mysql_read_err(error.into()))?
            else {
                continue;
            };
            match event_data {
                EventData::RotateEvent(rotate_event) => {
                    // Real rotation advances to the next file; the artificial
                    // rotate re-sent at the start of every dump names the file we
                    // requested at our current position, so the monotonic guard
                    // makes it a no-op.
                    let next_filename = rotate_event.name().to_string();
                    let next_position = rotate_event.position();
                    if binlog_coords_cmp(&next_filename, next_position, &filename, position).is_gt()
                    {
                        filename = next_filename;
                        position = next_position;
                    }
                }
                EventData::TableMapEvent(table_map_event) => {
                    table_maps.insert(table_map_event.table_id(), table_map_event.into_owned());
                    // A table map opens a statement's row section.
                    in_statement = true;
                    if log_pos > position {
                        position = log_pos;
                    }
                }
                EventData::RowsEvent(rows_event) => {
                    if let Some(table_map_event) = table_maps.get(&rows_event.table_id()) {
                        if self.is_our_table(table_map_event) {
                            // TODO(schema-evolution): the binlog carries column
                            // count + types but not names, and `column_indices`
                            // is computed once at startup. A mid-stream `ALTER`
                            // that adds/reorders columns shifts physical
                            // positions and silently maps values to the wrong
                            // fields. Compare
                            // `table_map_event.columns_count()` against
                            // `self.physical_columns.len()` here and fail loudly
                            // on a mismatch (asking the user to restart).
                            self.collect_row_changes(&rows_event, table_map_event, &mut changes)?;
                        }
                    }
                    // The statement's row section closes on the rows event that
                    // carries `STMT_END`; until then more split rows events for it
                    // may follow, relying on the same (non-re-sent) table map.
                    if rows_event.flags().contains(RowsEventFlags::STMT_END) {
                        in_statement = false;
                    }
                    if log_pos > position {
                        position = log_pos;
                    }
                }
                _ => {
                    // Any other event (FORMAT_DESCRIPTION re-sent at dump start,
                    // GTID, QUERY/BEGIN, XID, …). Advance only forward: the
                    // artificial FORMAT_DESCRIPTION carries a small early log_pos
                    // that must not move us backward when resuming mid-file.
                    if log_pos > position {
                        position = log_pos;
                    }
                }
            }
            // Only positions where no statement is open are safe to commit or
            // break at: the next dump will start there and MySQL will re-send the
            // table map before any rows event. Breaking mid-statement could drop
            // the rest of a split statement on resume.
            if !in_statement {
                safe_filename.clone_from(&filename);
                safe_position = position;
                if changes.len() >= MAX_ROWS_PER_BLOCK {
                    break;
                }
            }
        }

        if !changes.is_empty() {
            let offset = Self::binlog_offset(&safe_filename, safe_position);
            self.snapshot = self.build_block(changes, &offset);
        }
        self.binlog_filename = safe_filename;
        self.binlog_position = safe_position;
        Ok(())
    }
}

/// Order two binary-log coordinates `(file, position)` the way `MySQL` advances
/// through them.
///
/// `MySQL` names binary-log files `basename.NNNNNN`, where the numeric suffix
/// increments on every rotation and is zero-padded to at least six digits. Once
/// the sequence passes `999999` the suffix widens to seven digits, and a plain
/// lexicographic compare then inverts the order — `mysql-bin.1000000` would sort
/// before `mysql-bin.999999`. Parsing the suffix as an integer keeps the order
/// monotonic across that rollover: compare the basename prefix as text first
/// (constant within one server, so normally a tie), then the suffix numerically,
/// then the in-file `position`. Falls back to a whole-string compare when a name
/// has no `.`-separated numeric suffix, so an unexpected naming scheme degrades
/// safely instead of misordering.
#[must_use]
pub(crate) fn binlog_coords_cmp(
    lhs_file: &str,
    lhs_position: u64,
    rhs_file: &str,
    rhs_position: u64,
) -> Ordering {
    match (split_binlog_name(lhs_file), split_binlog_name(rhs_file)) {
        (Some((lhs_prefix, lhs_seq)), Some((rhs_prefix, rhs_seq))) => lhs_prefix
            .cmp(rhs_prefix)
            .then(lhs_seq.cmp(&rhs_seq))
            .then(lhs_position.cmp(&rhs_position)),
        _ => lhs_file.cmp(rhs_file).then(lhs_position.cmp(&rhs_position)),
    }
}

/// Split `basename.NNNNNN` into `(basename, sequence)`, or `None` when the name
/// has no `.`-separated numeric suffix.
fn split_binlog_name(name: &str) -> Option<(&str, u64)> {
    let (prefix, suffix) = name.rsplit_once('.')?;
    Some((prefix, suffix.parse::<u64>().ok()?))
}

impl Reader for MysqlReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(result) = self.snapshot.pop() {
                return Ok(result);
            }
            let outcome = if !self.is_initialized {
                self.initialize()
            } else if self.mode.is_polling_enabled() {
                self.poll_binlog()
            } else {
                return Ok(ReadResult::Finished);
            };
            // The engine re-invokes `read()` immediately when it returns an
            // error, with no backoff of its own. A failure that recurs on every
            // call (a missing table, bad credentials, a wrong binlog position)
            // would therefore spin this loop, flooding the log until the engine
            // gives up. Pause before propagating so the retry cadence stays
            // bounded — the same interval used between idle binlog polls.
            if let Err(error) = outcome {
                std::thread::sleep(BINLOG_POLL_INTERVAL);
                return Err(error);
            }
            if self.snapshot.is_empty() {
                std::thread::sleep(BINLOG_POLL_INTERVAL);
            }
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        if let Some(OffsetValue::MysqlBinlogPos { filename, position }) =
            frontier.get_offset(&OffsetKey::Mysql)
        {
            self.saved_position = Some((filename.clone(), *position));
        }
        Ok(())
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        MAX_MYSQL_READ_RETRIES
    }

    fn short_description(&self) -> Cow<'static, str> {
        if self.mode.is_polling_enabled() {
            format!("MySQL-binlog({})", self.table_name).into()
        } else {
            format!("MySQL({})", self.table_name).into()
        }
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mysql
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{binlog_coords_cmp, MysqlReader};
    use crate::connectors::offset::{OffsetKey, OffsetValue};
    use crate::connectors::Reader;
    use crate::persistence::frontier::OffsetAntichain;

    fn binlog_frontier(filename: &str, position: u64) -> OffsetAntichain {
        let mut frontier = OffsetAntichain::new();
        frontier.advance_offset(
            OffsetKey::Mysql,
            OffsetValue::MysqlBinlogPos {
                filename: filename.to_owned(),
                position,
            },
        );
        frontier
    }

    #[test]
    fn binlog_coords_compare_suffix_numerically_not_lexicographically() {
        // Same digit width: zero-padding already makes lexicographic == numeric.
        assert_eq!(
            binlog_coords_cmp("mysql-bin.000002", 4, "mysql-bin.000001", 4),
            Ordering::Greater
        );
        // Same file: the in-file position breaks the tie.
        assert_eq!(
            binlog_coords_cmp("mysql-bin.000001", 120, "mysql-bin.000001", 4),
            Ordering::Greater
        );
        // Width rollover: numerically 100000 > 99999, but lexicographically
        // "100000" < "99999". The numeric comparison must win.
        assert_eq!(
            binlog_coords_cmp("mysql-bin.100000", 4, "mysql-bin.99999", 4),
            Ordering::Greater
        );
        // The canonical six-to-seven digit MySQL rollover.
        assert_eq!(
            binlog_coords_cmp("mysql-bin.1000000", 4, "mysql-bin.999999", 4),
            Ordering::Greater
        );
        // A non-numeric suffix degrades to a whole-string comparison.
        assert_eq!(
            binlog_coords_cmp("binlog.index", 4, "binlog.index", 4),
            Ordering::Equal
        );
    }

    #[test]
    fn merge_frontiers_advances_to_numerically_later_binlog_file() {
        // Merging a frontier at mysql-bin.99999 with one at mysql-bin.100000
        // must converge to mysql-bin.100000: it is the newer file even though it
        // sorts earlier lexicographically.
        let older = binlog_frontier("mysql-bin.99999", 4);
        let newer = binlog_frontier("mysql-bin.100000", 4);

        let expected = OffsetValue::MysqlBinlogPos {
            filename: "mysql-bin.100000".to_owned(),
            position: 4,
        };

        // The newer file must win regardless of merge argument order.
        let merged = MysqlReader::merge_two_frontiers(&older, &newer);
        assert_eq!(merged.get_offset(&OffsetKey::Mysql), Some(&expected));

        let merged_swapped = MysqlReader::merge_two_frontiers(&newer, &older);
        assert_eq!(
            merged_swapped.get_offset(&OffsetKey::Mysql),
            Some(&expected)
        );
    }
}

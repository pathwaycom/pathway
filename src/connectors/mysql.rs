// Copyright © 2026 Pathway

use mysql::prelude::Queryable;
pub use mysql::Error as MysqlError;
use mysql::{
    Params as MysqlParams, Pool as MysqlConnectionPool, PooledConn as MysqlConnection,
    TxOpts as MysqlTxOpts, Value as MysqlValue,
};

use crate::connectors::data_format::{FormatterContext, FormatterError};
use crate::connectors::data_storage::{SqlQueryTemplate, TableWriterInitMode};
use crate::connectors::{WriteError, Writer};
use crate::engine::time::DateTime as DateTimeTrait;
use crate::engine::{DateTimeNaive, Duration as EngineDuration, Type, Value};
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

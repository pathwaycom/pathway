// Copyright © 2026 Pathway

use log::{error, info};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::take;
use std::thread::sleep;
use std::time::Duration;

use chrono::Timelike;
use tiberius::{Client, ColumnData, Config, Query};
use hex;
use tokio::net::TcpStream;
use tokio::runtime::Runtime as TokioRuntime;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::{
    CommitPossibility, ConversionError, SqlQueryTemplate, TableWriterInitMode, ValuesMap,
};
use crate::connectors::metadata::MssqlMetadata;
use crate::connectors::offset::EMPTY_OFFSET;
use crate::connectors::{DataEventType, ReadError, ReadResult, Reader, ReaderContext, StorageType};
use crate::connectors::{WriteError, Writer};
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::time::DateTime as DateTimeTrait;
use crate::engine::{Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries, RetryConfig};

const MAX_MSSQL_RETRIES: usize = 3;

type MssqlClient = Client<Compat<TcpStream>>;

/// Build a schema-qualified table name using MSSQL bracket quoting.
fn qualified_table_name(schema_name: &str, table_name: &str) -> String {
    format!("[{}].[{}]", schema_name, table_name)
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

// ========================= Reader =========================

pub struct MssqlReader {
    runtime: TokioRuntime,
    config: Config,
    schema_name: String,
    table_name: String,
    schema: Vec<(String, Type)>,

    stored_state: HashMap<i64, ValuesMap>,
    queued_updates: VecDeque<ReadResult>,
    snapshot_version: u64,
}

impl MssqlReader {
    pub fn new(
        config: Config,
        schema_name: String,
        table_name: String,
        schema: Vec<(String, Type)>,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            runtime: create_async_tokio_runtime()?,
            config,
            schema_name,
            table_name,
            schema,
            stored_state: HashMap::new(),
            queued_updates: VecDeque::new(),
            snapshot_version: 0,
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
            (Type::Optional(_) | Type::Any, col) if Self::is_column_null(col) => {
                Some(Value::None)
            }
            (Type::Optional(inner), other) => {
                Self::convert_column_data(other, field_name, inner).ok()
            }
            (Type::Bool | Type::Any, ColumnData::Bit(Some(v))) => Some(Value::Bool(*v)),
            (Type::Int | Type::Any, ColumnData::I16(Some(v))) => Some(Value::Int(i64::from(*v))),
            (Type::Int | Type::Any, ColumnData::I32(Some(v))) => Some(Value::Int(i64::from(*v))),
            (Type::Int | Type::Any, ColumnData::I64(Some(v))) => Some(Value::Int(*v)),
            (Type::Float | Type::Any, ColumnData::F32(Some(v))) => {
                Some(Value::Float(f64::from(*v).into()))
            }
            (Type::Float | Type::Any, ColumnData::F64(Some(v))) => Some(Value::Float((*v).into())),
            (Type::Float | Type::Any, ColumnData::Numeric(Some(n))) => {
                let s = n.to_string();
                s.parse::<f64>().ok().map(|f| Value::Float(f.into()))
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

    fn load_table(&mut self) -> Result<(), ReadError> {
        let column_names: Vec<&str> = self
            .schema
            .iter()
            .map(|(name, _dtype)| name.as_str())
            .collect();

        // MSSQL doesn't have _rowid_, so we use ROW_NUMBER() as a synthetic row identity.
        // For tables with a primary key, this will produce stable ordering.
        let columns_str = column_names.join(",");
        let full_table_name = qualified_table_name(&self.schema_name, &self.table_name);
        let query_str = format!(
            "SELECT {columns_str}, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS _rn FROM {full_table_name}",
        );

        let rows = self.runtime.block_on(async {
            let mut client = connect_mssql(&self.config).await.map_err(|e| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;
            let stream = client.simple_query(&query_str).await.map_err(|e| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
            let rows = stream.into_first_result().await.map_err(|e| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
            Ok::<_, ReadError>(rows)
        })?;

        let mut present_rowids = HashSet::new();
        for row in &rows {
            // The last column is _rn (ROW_NUMBER)
            let rowid: i64 = match row.get::<i64, _>(self.schema.len()) {
                Some(v) => v,
                None => continue,
            };

            let mut values = HashMap::with_capacity(self.schema.len());
            for (column_idx, (column_name, column_dtype)) in self.schema.iter().enumerate() {
                let value = Self::convert_row_value(row, column_idx, column_name, column_dtype);
                values.insert(column_name.clone(), value);
            }
            let values: ValuesMap = values.into();
            self.stored_state
                .entry(rowid)
                .and_modify(|current_values| {
                    if current_values != &values {
                        let key = vec![Value::Int(rowid)];
                        self.queued_updates.push_back(ReadResult::Data(
                            ReaderContext::from_diff(
                                DataEventType::Delete,
                                Some(key.clone()),
                                take(current_values),
                            ),
                            EMPTY_OFFSET,
                        ));
                        self.queued_updates.push_back(ReadResult::Data(
                            ReaderContext::from_diff(
                                DataEventType::Insert,
                                Some(key),
                                values.clone(),
                            ),
                            EMPTY_OFFSET,
                        ));
                        current_values.clone_from(&values);
                    }
                })
                .or_insert_with(|| {
                    let key = vec![Value::Int(rowid)];
                    self.queued_updates.push_back(ReadResult::Data(
                        ReaderContext::from_diff(DataEventType::Insert, Some(key), values.clone()),
                        EMPTY_OFFSET,
                    ));
                    values
                });
            present_rowids.insert(rowid);
        }

        self.stored_state.retain(|rowid, values| {
            if present_rowids.contains(rowid) {
                true
            } else {
                let key = vec![Value::Int(*rowid)];
                self.queued_updates.push_back(ReadResult::Data(
                    ReaderContext::from_diff(DataEventType::Delete, Some(key), take(values)),
                    EMPTY_OFFSET,
                ));
                false
            }
        });

        if !self.queued_updates.is_empty() {
            self.queued_updates.push_back(ReadResult::FinishedSource {
                commit_possibility: CommitPossibility::Possible,
            });
        }

        Ok(())
    }

    fn wait_period() -> Duration {
        Duration::from_millis(500)
    }

    fn retry_after_error_period() -> Duration {
        Duration::from_millis(500)
    }
}

impl Reader for MssqlReader {
    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        todo!("seek is not supported for MSSQL source: persistent history of changes unavailable")
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(queued_update) = self.queued_updates.pop_front() {
                return Ok(queued_update);
            }

            match self.load_table() {
                Ok(()) => {
                    if !self.queued_updates.is_empty() {
                        self.snapshot_version += 1;
                        return Ok(ReadResult::NewSource(
                            MssqlMetadata::new(self.snapshot_version).into(),
                        ));
                    }
                }
                Err(e) => {
                    error!("MSSQL load_table error: {e}");
                    sleep(Self::retry_after_error_period());
                    continue;
                }
            }

            sleep(Self::wait_period());
        }
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("MSSQL({})", self.table_name).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mssql
    }
}

// ========================= CDC Reader =========================

/// CDC (Change Data Capture) reader for MSSQL.
///
/// Uses MSSQL's built-in CDC feature which tracks changes via the transaction log
/// and exposes them through `cdc.fn_cdc_get_all_changes_*` functions.
///
/// Requirements:
/// - SQL Server Developer or Enterprise edition
/// - CDC must be enabled on the database (`sys.sp_cdc_enable_db`)
/// - CDC must be enabled on the target table (`sys.sp_cdc_enable_table`)
pub struct MssqlCdcReader {
    runtime: TokioRuntime,
    config: Config,
    client: Option<MssqlClient>,
    schema_name: String,
    table_name: String,
    capture_instance: String,
    schema: Vec<(String, Type)>,
    last_lsn: Option<Vec<u8>>,
    queued_updates: VecDeque<ReadResult>,
    snapshot_done: bool,
    snapshot_version: u64,
}

impl MssqlCdcReader {
    pub fn new(
        config: Config,
        schema_name: String,
        table_name: String,
        schema: Vec<(String, Type)>,
    ) -> Result<Self, ReadError> {
        let runtime = create_async_tokio_runtime()?;

        // Verify CDC is enabled and discover the capture instance name
        let capture_instance = runtime.block_on(async {
            let mut client = connect_mssql(&config).await.map_err(|e| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;

            // Check if CDC is enabled on the database
            let row = client
                .simple_query("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()")
                .await
                .map_err(mssql_read_err)?
                .into_row()
                .await
                .map_err(mssql_read_err)?;

            if let Some(row) = &row {
                let enabled: Option<bool> = row.get(0);
                if enabled != Some(true) {
                    return Err(ReadError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "CDC is not enabled on the current database. \
                         Enable it with: EXEC sys.sp_cdc_enable_db;",
                    )));
                }
            }

            // Check if the table has CDC enabled and get capture instance
            let qualified_name = qualified_table_name(&schema_name, &table_name);
            let query = format!(
                "SELECT capture_instance FROM cdc.change_tables \
                 WHERE source_object_id = OBJECT_ID(N'{qualified_name}')"
            );
            let row = client
                .simple_query(&query)
                .await
                .map_err(mssql_read_err)?
                .into_row()
                .await
                .map_err(mssql_read_err)?;

            match row {
                Some(row) => {
                    let instance: Option<&str> = row.get(0);
                    Ok(instance.unwrap_or("").to_string())
                }
                None => Err(ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "CDC is not enabled on table '{schema_name}.{table_name}'. Enable it with: \
                         EXEC sys.sp_cdc_enable_table @source_schema=N'{schema_name}', \
                         @source_name=N'{table_name}', @role_name=NULL;",
                    ),
                ))),
            }
        })?;

        info!(
            "MSSQL CDC reader initialized for table '{}.{}', capture_instance='{}'",
            schema_name, table_name, capture_instance
        );

        Ok(Self {
            runtime,
            config,
            client: None,
            schema_name,
            table_name,
            capture_instance,
            schema,
            last_lsn: None,
            queued_updates: VecDeque::new(),
            snapshot_done: false,
            snapshot_version: 0,
        })
    }

    /// Load the initial snapshot of the table.
    fn load_snapshot(&mut self) -> Result<(), ReadError> {
        let column_names: Vec<&str> = self.schema.iter().map(|(n, _)| n.as_str()).collect();
        let columns_str = column_names.join(",");
        let full_table_name = qualified_table_name(&self.schema_name, &self.table_name);
        let query_str = format!("SELECT {columns_str} FROM {full_table_name}");

        let mut client_opt = self.client.take();
        let config = self.config.clone();

        let result = self.runtime.block_on(async {
            if client_opt.is_none() {
                client_opt = Some(connect_mssql(&config).await.map_err(|e| {
                    ReadError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        e.to_string(),
                    ))
                })?);
            }
            let client = client_opt.as_mut().unwrap();

            // Batch LSN + snapshot SELECT in a single query to minimize the race window
            // between capturing the LSN position and reading the table data.
            let batch_query = format!(
                "SELECT sys.fn_cdc_get_max_lsn(); {query_str}"
            );
            let mut result_sets = client
                .simple_query(&batch_query)
                .await
                .map_err(mssql_read_err)?
                .into_results()
                .await
                .map_err(mssql_read_err)?;

            // First result set: max LSN
            let max_lsn = result_sets
                .first()
                .and_then(|rs| rs.first())
                .and_then(|r| r.get::<&[u8], _>(0).map(|b| b.to_vec()));

            // Second result set: table snapshot rows
            let rows = if result_sets.len() > 1 {
                take(&mut result_sets[1])
            } else {
                Vec::new()
            };

            Ok::<_, ReadError>((rows, max_lsn))
        });

        if result.is_err() {
            self.client = None; // force reconnect on next call
        } else {
            self.client = client_opt; // put connection back
        }
        let (rows, max_lsn) = result?;

        for row in rows.iter() {
            let mut values = HashMap::with_capacity(self.schema.len());
            for (col_idx, (col_name, col_dtype)) in self.schema.iter().enumerate() {
                let value = MssqlReader::convert_row_value(row, col_idx, col_name, col_dtype);
                values.insert(col_name.clone(), value);
            }
            let values: ValuesMap = values.into();
            self.queued_updates.push_back(ReadResult::Data(
                ReaderContext::from_diff(DataEventType::Insert, None, values),
                EMPTY_OFFSET,
            ));
        }

        self.last_lsn = max_lsn;
        self.snapshot_done = true;

        if !self.queued_updates.is_empty() {
            self.queued_updates.push_back(ReadResult::FinishedSource {
                commit_possibility: CommitPossibility::Possible,
            });
        }

        Ok(())
    }

    /// Poll for CDC changes since the last processed LSN.
    fn poll_cdc_changes(&mut self) -> Result<(), ReadError> {
        let from_lsn = match &self.last_lsn {
            Some(lsn) => lsn.clone(),
            None => return Ok(()),
        };

        let capture_instance = self.capture_instance.clone();
        let schema = self.schema.clone();

        let mut client_opt = self.client.take();
        let config = self.config.clone();

        let result = self.runtime.block_on(async {
            if client_opt.is_none() {
                client_opt = Some(connect_mssql(&config).await.map_err(|e| {
                    ReadError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        e.to_string(),
                    ))
                })?);
            }
            let client = client_opt.as_mut().unwrap();

            // Get current max LSN
            let lsn_row = client
                .simple_query("SELECT sys.fn_cdc_get_max_lsn()")
                .await
                .map_err(mssql_read_err)?
                .into_row()
                .await
                .map_err(mssql_read_err)?;

            let new_max_lsn: Option<Vec<u8>> =
                lsn_row.and_then(|r| r.get::<&[u8], _>(0).map(|b| b.to_vec()));

            // If max LSN hasn't changed, no new changes
            if new_max_lsn.as_ref() == Some(&from_lsn) {
                return Ok::<_, ReadError>((Vec::new(), Some(from_lsn)));
            }

            // Increment from_lsn by 1 to avoid re-reading the last processed LSN
            // Use sys.fn_cdc_increment_lsn to get the next LSN
            let from_lsn_hex = hex::encode(&from_lsn);
            let increment_query = format!("SELECT sys.fn_cdc_increment_lsn(0x{from_lsn_hex})");
            let inc_row = client
                .simple_query(&increment_query)
                .await
                .map_err(mssql_read_err)?
                .into_row()
                .await
                .map_err(mssql_read_err)?;

            let incremented_lsn: Option<Vec<u8>> =
                inc_row.and_then(|r| r.get::<&[u8], _>(0).map(|b| b.to_vec()));

            let inc_lsn = match incremented_lsn {
                Some(lsn) => lsn,
                None => return Ok((Vec::new(), new_max_lsn)),
            };

            let to_lsn = match &new_max_lsn {
                Some(lsn) => lsn,
                None => return Ok((Vec::new(), new_max_lsn)),
            };

            let inc_lsn_hex = hex::encode(&inc_lsn);
            let to_lsn_hex = hex::encode(to_lsn);

            // Query CDC changes using 'all update old' to get both before and after images.
            // capture_instance is safe to interpolate — it comes from cdc.change_tables (trusted DB result).
            let cdc_query = format!(
                "SELECT * FROM cdc.fn_cdc_get_all_changes_{capture_instance}\
                 (0x{inc_lsn_hex}, 0x{to_lsn_hex}, N'all update old')"
            );

            let stream = client
                .simple_query(&cdc_query)
                .await
                .map_err(mssql_read_err)?;
            let rows = stream.into_first_result().await.map_err(mssql_read_err)?;

            Ok((rows, new_max_lsn))
        });

        if result.is_err() {
            self.client = None; // force reconnect on next call
        } else {
            self.client = client_opt; // put connection back
        }
        let (changes, new_max_lsn) = result?;

        for row in &changes {
            // CDC rows have these system columns at the start:
            // __$start_lsn, __$seqval, __$operation, __$update_mask, then user columns
            let operation: Option<i32> = row.get(2);
            let operation = operation.unwrap_or(0);

            // Map CDC operation codes to Pathway events
            // 1 = DELETE, 2 = INSERT, 3 = UPDATE (before), 4 = UPDATE (after)
            let event_type = match operation {
                1 => DataEventType::Delete,
                2 => DataEventType::Insert,
                3 => DataEventType::Delete, // UPDATE before-image
                4 => DataEventType::Insert, // UPDATE after-image
                _ => continue,
            };

            // User columns start at index 4 (after the 4 system columns)
            let mut values = HashMap::with_capacity(schema.len());
            for (col_idx, (col_name, col_dtype)) in schema.iter().enumerate() {
                let value =
                    MssqlReader::convert_row_value(row, col_idx + 4, col_name, col_dtype);
                values.insert(col_name.clone(), value);
            }
            let values: ValuesMap = values.into();
            self.queued_updates.push_back(ReadResult::Data(
                ReaderContext::from_diff(event_type, None, values),
                EMPTY_OFFSET,
            ));
        }

        if let Some(new_lsn) = new_max_lsn {
            self.last_lsn = Some(new_lsn);
        }

        if !self.queued_updates.is_empty() {
            self.queued_updates.push_back(ReadResult::FinishedSource {
                commit_possibility: CommitPossibility::Possible,
            });
        }

        Ok(())
    }

    fn wait_period() -> Duration {
        Duration::from_millis(500)
    }

    fn retry_after_error_period() -> Duration {
        Duration::from_millis(500)
    }
}

impl Reader for MssqlCdcReader {
    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        Err(ReadError::PersistenceNotSupported(StorageType::Mssql))
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        let mut consecutive_errors: usize = 0;

        loop {
            if let Some(queued_update) = self.queued_updates.pop_front() {
                return Ok(queued_update);
            }

            if !self.snapshot_done {
                match self.load_snapshot() {
                    Ok(()) => {
                        consecutive_errors = 0;
                        if !self.queued_updates.is_empty() {
                            self.snapshot_version += 1;
                            return Ok(ReadResult::NewSource(
                                MssqlMetadata::new(self.snapshot_version).into(),
                            ));
                        }
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        error!(
                            "MSSQL CDC snapshot error ({consecutive_errors}/{MAX_MSSQL_RETRIES}): {e}"
                        );
                        if consecutive_errors >= MAX_MSSQL_RETRIES {
                            return Err(e);
                        }
                        self.client = None; // force reconnect
                        sleep(Self::retry_after_error_period());
                        continue;
                    }
                }
            } else {
                match self.poll_cdc_changes() {
                    Ok(()) => {
                        consecutive_errors = 0;
                        if !self.queued_updates.is_empty() {
                            self.snapshot_version += 1;
                            return Ok(ReadResult::NewSource(
                                MssqlMetadata::new(self.snapshot_version).into(),
                            ));
                        }
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        error!(
                            "MSSQL CDC poll error ({consecutive_errors}/{MAX_MSSQL_RETRIES}): {e}"
                        );
                        if consecutive_errors >= MAX_MSSQL_RETRIES {
                            return Err(e);
                        }
                        self.client = None; // force reconnect
                        sleep(Self::retry_after_error_period());
                        continue;
                    }
                }
            }

            sleep(Self::wait_period());
        }
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("MSSQL-CDC({})", self.table_name).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mssql
    }
}

// ========================= Writer =========================

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
}

impl MssqlWriter {
    pub fn new(
        config: Config,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        mode: TableWriterInitMode,
    ) -> Result<MssqlWriter, WriteError> {
        let runtime = create_async_tokio_runtime()?;
        let full_table_name = qualified_table_name(schema_name, table_name);

        // Collect initialization queries
        let mut init_queries: Vec<String> = Vec::new();
        let key_set: std::collections::HashSet<&str> = key_field_names
            .map(|keys| keys.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        let mut field_idx = 0usize;
        mode.initialize(
            &full_table_name,
            value_fields,
            key_field_names,
            !snapshot_mode,
            |query| {
                let adapted = adapt_create_table_query(query);
                init_queries.push(adapted);
                Ok(())
            },
            |type_: &Type, is_nested: bool| {
                let is_key = if !is_nested {
                    let name = &value_fields[field_idx].name;
                    field_idx += 1;
                    key_set.contains(name.as_str())
                } else {
                    false
                };
                Self::mssql_data_type(type_, is_nested, is_key)
            },
        )?;

        // Establish connection and execute initialization queries
        let client = runtime.block_on(async {
            let mut client = connect_mssql(&config).await.map_err(|e| {
                WriteError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;

            if let Ok((version, edition)) = detect_server_version(&mut client).await {
                info!("Connected to MSSQL Server: version={version}, edition={edition}");
            }

            for query in &init_queries {
                client.execute(query.as_str(), &[]).await.map_err(|e| {
                    WriteError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;
            }
            Ok::<_, WriteError>(client)
        })?;

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
        )?;

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
        })
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
            Type::Int => format!("BIGINT{not_null_suffix}"),
            Type::Float => format!("FLOAT{not_null_suffix}"),
            Type::Pointer | Type::String => {
                if is_key {
                    format!("NVARCHAR(450){not_null_suffix}")
                } else {
                    format!("NVARCHAR(MAX){not_null_suffix}")
                }
            }
            Type::Bytes | Type::PyObjectWrapper => format!("VARBINARY(MAX){not_null_suffix}"),
            Type::Json => format!("NVARCHAR(MAX){not_null_suffix}"),
            Type::Duration => format!("BIGINT{not_null_suffix}"),
            Type::DateTimeNaive => format!("DATETIME2(6){not_null_suffix}"),
            Type::DateTimeUtc => format!("DATETIMEOFFSET(6){not_null_suffix}"),
            Type::Optional(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }
                let wrapped = Self::mssql_data_type(wrapped, true, is_key)?;
                return Ok(wrapped);
            }
            Type::Any | Type::Tuple(_) | Type::List(_) | Type::Array(_, _) | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()))
            }
        })
    }

    fn bind_value<'a>(query: &mut Query<'a>, value: &Value) -> Result<(), WriteError> {
        match value {
            Value::None => query.bind(Option::<String>::None),
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
            Value::IntArray(_)
            | Value::FloatArray(_)
            | Value::Tuple(_)
            | Value::Error
            | Value::Pending => {
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
        let buffer = take(&mut self.buffer);
        let snapshot_mode = self.snapshot_mode;
        let merge_query = self.merge_query.clone();
        let config = self.config.clone();

        // Pre-build query strings and extract PK values outside async block
        struct PreparedRow {
            query_str: String,
            values: Vec<Value>,
            time: u64,
            diff: isize,
            is_delete: bool,
        }

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
                    time: data.time.0,
                    diff: data.diff,
                    is_delete: true,
                });
            } else {
                let query_str = self.query_template.build_query(data.diff);
                prepared.push(PreparedRow {
                    query_str,
                    values: data.values.clone(),
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
                let c = connect_mssql(&config).await.map_err(|e| {
                    WriteError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        e.to_string(),
                    ))
                })?;
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
                for val in &row.values {
                    Self::bind_value(&mut query, val)?;
                }
                // For stream-of-changes (non-snapshot, non-delete), append time and diff
                if !snapshot_mode && !row.is_delete {
                    query.bind(row.time as i64);
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
            // Drop the connection on error so we reconnect on next flush
            self.client = None;
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

        execute_with_retries(
            || self.flush_impl(),
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

// ========================= Helpers =========================

fn mssql_read_err(e: tiberius::error::Error) -> ReadError {
    ReadError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    ))
}

fn mssql_write_err(e: tiberius::error::Error) -> WriteError {
    WriteError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    ))
}

/// Adapt "CREATE TABLE IF NOT EXISTS" to MSSQL syntax.
/// MSSQL doesn't support IF NOT EXISTS in CREATE TABLE.
fn adapt_create_table_query(query: &str) -> String {
    if query.starts_with("CREATE TABLE IF NOT EXISTS") {
        let table_and_rest = &query["CREATE TABLE IF NOT EXISTS ".len()..];
        // Extract table name (first whitespace-delimited token)
        let table_name = table_and_rest
            .split_whitespace()
            .next()
            .unwrap_or(table_and_rest);
        format!(
            "IF OBJECT_ID(N'{table_name}', N'U') IS NULL {query}",
            table_name = table_name.replace('\'', "''"),
            query = query.replace("IF NOT EXISTS ", ""),
        )
    } else {
        query.to_string()
    }
}

/// Build a MERGE statement template for snapshot upserts with @P placeholders.
fn build_merge_query(
    table_name: &str,
    value_fields: &[ValueField],
    key_field_names: &[String],
) -> String {
    let field_names: Vec<&str> = value_fields.iter().map(|f| f.name.as_str()).collect();
    let source_cols = field_names.join(", ");

    let on_clause = key_field_names
        .iter()
        .map(|k| format!("target.{k} = source.{k}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    let update_set = value_fields
        .iter()
        .filter(|f| !key_field_names.contains(&f.name))
        .map(|f| format!("target.{name} = source.{name}", name = f.name))
        .collect::<Vec<_>>()
        .join(", ");

    let insert_cols = field_names.join(", ");
    let insert_source = field_names
        .iter()
        .map(|n| format!("source.{n}"))
        .collect::<Vec<_>>()
        .join(", ");

    // Template: {{PARAM_COUNT}} will be replaced with actual @P placeholders at execution time
    format!(
        "MERGE INTO {table_name} AS target \
         USING (SELECT {{PARAMS}}) AS source ({source_cols}) \
         ON {on_clause} \
         WHEN MATCHED THEN UPDATE SET {update_set} \
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_source});"
    )
}

/// Replace {{PARAMS}} in a MERGE template with @P1, @P2, ... placeholders.
fn build_merge_query_parameterized(merge_template: &str, param_count: usize) -> String {
    let params: Vec<String> = (1..=param_count).map(|i| format!("@P{i}")).collect();
    let params_str = params.join(", ");
    merge_template.replace("{PARAMS}", &params_str)
}

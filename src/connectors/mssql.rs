// Copyright © 2026 Pathway

use log::{error, info};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::take;
use std::thread::sleep;
use std::time::Duration;

use tiberius::{Client, ColumnData, Config};
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
use crate::engine::{Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries, RetryConfig};

const MAX_MSSQL_RETRIES: usize = 3;

type MssqlClient = Client<Compat<TcpStream>>;

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
    table_name: String,
    schema: Vec<(String, Type)>,

    stored_state: HashMap<i64, ValuesMap>,
    queued_updates: VecDeque<ReadResult>,
    snapshot_version: u64,
}

impl MssqlReader {
    pub fn new(
        config: Config,
        table_name: String,
        schema: Vec<(String, Type)>,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            runtime: create_async_tokio_runtime()?,
            config,
            table_name,
            schema,
            stored_state: HashMap::new(),
            queued_updates: VecDeque::new(),
            snapshot_version: 0,
        })
    }

    fn convert_to_value(
        col_data: &ColumnData<'_>,
        field_name: &str,
        dtype: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let value = match (dtype, col_data) {
            (Type::Optional(_) | Type::Any, ColumnData::Null) => Some(Value::None),
            (Type::Optional(inner), other) => Self::convert_to_value(other, field_name, inner).ok(),
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
            (Type::DateTimeNaive | Type::Any, ColumnData::DateTime(Some(dt))) => {
                let s = format!("{dt}");
                chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
                    .ok()
                    .map(|ndt| Value::DateTimeNaive(ndt.into()))
            }
            (Type::DateTimeNaive | Type::Any, ColumnData::DateTime2(Some(dt))) => {
                let s = format!("{dt}");
                chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
                    .ok()
                    .map(|ndt| Value::DateTimeNaive(ndt.into()))
            }
            (Type::DateTimeNaive | Type::Any, ColumnData::SmallDateTime(Some(dt))) => {
                let s = format!("{dt}");
                chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|ndt| Value::DateTimeNaive(ndt.into()))
            }
            (Type::DateTimeUtc | Type::Any, ColumnData::DateTimeOffset(Some(dto))) => {
                let s = format!("{dto}");
                chrono::DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f %:z")
                    .ok()
                    .map(|dt| {
                        let utc = dt.naive_utc();
                        Value::DateTimeUtc(utc.into())
                    })
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
        let query_str = format!(
            "SELECT {columns_str}, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS _rn FROM {}",
            self.table_name
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
                let col_data = &row[column_idx];
                let value = Self::convert_to_value(col_data, column_name, column_dtype);
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
            let query = format!(
                "SELECT capture_instance FROM cdc.change_tables \
                 WHERE source_object_id = OBJECT_ID(N'{}')",
                table_name.replace('\'', "''")
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
                        "CDC is not enabled on table '{}'. Enable it with: \
                         EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', \
                         @source_name=N'{}', @role_name=NULL;",
                        table_name, table_name
                    ),
                ))),
            }
        })?;

        info!(
            "MSSQL CDC reader initialized for table '{}', capture_instance='{}'",
            table_name, capture_instance
        );

        Ok(Self {
            runtime,
            config,
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
        let query_str = format!("SELECT {columns_str} FROM {}", self.table_name);

        let (rows, max_lsn) = self.runtime.block_on(async {
            let mut client = connect_mssql(&self.config).await.map_err(|e| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;

            // Get the current max LSN so we know where to start CDC polling from
            let lsn_row = client
                .simple_query("SELECT sys.fn_cdc_get_max_lsn()")
                .await
                .map_err(mssql_read_err)?
                .into_row()
                .await
                .map_err(mssql_read_err)?;

            let max_lsn: Option<Vec<u8>> =
                lsn_row.and_then(|r| r.get::<&[u8], _>(0).map(|b| b.to_vec()));

            let stream = client
                .simple_query(&query_str)
                .await
                .map_err(mssql_read_err)?;
            let rows = stream.into_first_result().await.map_err(mssql_read_err)?;
            Ok::<_, ReadError>((rows, max_lsn))
        })?;

        for (row_idx, row) in rows.iter().enumerate() {
            let mut values = HashMap::with_capacity(self.schema.len());
            for (col_idx, (col_name, col_dtype)) in self.schema.iter().enumerate() {
                let col_data = &row[col_idx];
                let value = MssqlReader::convert_to_value(col_data, col_name, col_dtype);
                values.insert(col_name.clone(), value);
            }
            let values: ValuesMap = values.into();
            let key = vec![Value::Int(row_idx as i64)];
            self.queued_updates.push_back(ReadResult::Data(
                ReaderContext::from_diff(DataEventType::Insert, Some(key), values),
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

        let (changes, new_max_lsn) = self.runtime.block_on(async {
            let mut client = connect_mssql(&self.config).await.map_err(|e| {
                ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;

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
            let from_lsn_hex = bytes_to_hex(&from_lsn);
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

            let inc_lsn_hex = bytes_to_hex(&inc_lsn);
            let to_lsn_hex = bytes_to_hex(to_lsn);

            // Query CDC changes using 'all update old' to get both before and after images
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
        })?;

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
                let col_data = &row[col_idx + 4];
                let value = MssqlReader::convert_to_value(col_data, col_name, col_dtype);
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
        todo!("seek is not yet supported for MSSQL CDC source")
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(queued_update) = self.queued_updates.pop_front() {
                return Ok(queued_update);
            }

            if !self.snapshot_done {
                match self.load_snapshot() {
                    Ok(()) => {
                        if !self.queued_updates.is_empty() {
                            self.snapshot_version += 1;
                            return Ok(ReadResult::NewSource(
                                MssqlMetadata::new(self.snapshot_version).into(),
                            ));
                        }
                    }
                    Err(e) => {
                        error!("MSSQL CDC snapshot error: {e}");
                        sleep(Self::retry_after_error_period());
                        continue;
                    }
                }
            } else {
                match self.poll_cdc_changes() {
                    Ok(()) => {
                        if !self.queued_updates.is_empty() {
                            self.snapshot_version += 1;
                            return Ok(ReadResult::NewSource(
                                MssqlMetadata::new(self.snapshot_version).into(),
                            ));
                        }
                    }
                    Err(e) => {
                        error!("MSSQL CDC poll error: {e}");
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
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        mode: TableWriterInitMode,
    ) -> Result<MssqlWriter, WriteError> {
        let runtime = create_async_tokio_runtime()?;

        // Collect initialization queries
        let mut init_queries: Vec<String> = Vec::new();
        mode.initialize(
            table_name,
            value_fields,
            key_field_names,
            !snapshot_mode,
            |query| {
                let adapted = adapt_create_table_query(query);
                init_queries.push(adapted);
                Ok(())
            },
            Self::mssql_data_type,
        )?;

        // Execute initialization queries
        if !init_queries.is_empty() {
            runtime.block_on(async {
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
                Ok::<_, WriteError>(())
            })?;
        }

        let merge_query = if snapshot_mode {
            key_field_names.map(|keys| build_merge_query(table_name, value_fields, keys))
        } else {
            None
        };

        let query_template = SqlQueryTemplate::new(
            snapshot_mode,
            table_name,
            value_fields,
            key_field_names,
            false,
            |i| format!("@P{}", i + 1),
            Self::on_insert_conflict_condition,
        )?;

        Ok(MssqlWriter {
            runtime,
            config,
            max_batch_size,
            buffer: Vec::new(),
            snapshot_mode,
            table_name: table_name.to_string(),
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

    fn mssql_data_type(type_: &Type, is_nested: bool) -> Result<String, WriteError> {
        let not_null_suffix = if is_nested { "" } else { " NOT NULL" };
        Ok(match type_ {
            Type::Bool => format!("BIT{not_null_suffix}"),
            Type::Int => format!("BIGINT{not_null_suffix}"),
            Type::Float => format!("FLOAT{not_null_suffix}"),
            Type::Pointer | Type::String => format!("NVARCHAR(MAX){not_null_suffix}"),
            Type::Bytes | Type::PyObjectWrapper => format!("VARBINARY(MAX){not_null_suffix}"),
            Type::Json => format!("NVARCHAR(MAX){not_null_suffix}"),
            Type::Duration => format!("BIGINT{not_null_suffix}"),
            Type::DateTimeNaive => format!("DATETIME2(6){not_null_suffix}"),
            Type::DateTimeUtc => format!("DATETIMEOFFSET(6){not_null_suffix}"),
            Type::Optional(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }
                let wrapped = Self::mssql_data_type(wrapped, true)?;
                return Ok(wrapped);
            }
            Type::Any | Type::Tuple(_) | Type::List(_) | Type::Array(_, _) | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()))
            }
        })
    }

    fn value_to_sql_literal(value: &Value) -> Result<String, WriteError> {
        match value {
            Value::None => Ok("NULL".to_string()),
            Value::Bool(b) => Ok(if *b { "1".to_string() } else { "0".to_string() }),
            Value::Int(i) => Ok(i.to_string()),
            Value::Float(f) => {
                let f: f64 = (*f).into();
                Ok(format!("{f}"))
            }
            Value::Pointer(p) => Ok(format!("N'{}'", p.to_string().replace('\'', "''"))),
            Value::String(s) => Ok(format!("N'{}'", s.replace('\'', "''"))),
            Value::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02X}")).collect();
                Ok(format!("0x{hex}"))
            }
            Value::DateTimeNaive(dt) => Ok(format!("'{dt}'")),
            Value::DateTimeUtc(dt) => Ok(format!("'{dt}'")),
            Value::Duration(d) => Ok(d.microseconds().to_string()),
            Value::Json(j) => Ok(format!("N'{}'", j.to_string().replace('\'', "''"))),
            Value::PyObjectWrapper(_) => {
                let bytes = bincode::serialize(value).map_err(|e| *e)?;
                let hex: String = bytes.iter().map(|byte| format!("{byte:02X}")).collect();
                Ok(format!("0x{hex}"))
            }
            Value::IntArray(_)
            | Value::FloatArray(_)
            | Value::Tuple(_)
            | Value::Error
            | Value::Pending => Err(WriteError::from(
                crate::connectors::data_format::FormatterError::ValueNonSerializable(
                    value.kind(),
                    "MSSQL",
                ),
            )),
        }
    }

    fn flush_impl(&mut self) -> Result<(), WriteError> {
        let buffer = take(&mut self.buffer);
        self.runtime.block_on(async {
            let mut client = connect_mssql(&self.config).await.map_err(|e| {
                WriteError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;

            // Execute all statements in a transaction
            client
                .simple_query("BEGIN TRANSACTION")
                .await
                .map_err(mssql_write_err)?
                .into_results()
                .await
                .map_err(mssql_write_err)?;

            for data in &buffer {
                let query_str = if self.snapshot_mode && data.diff > 0 {
                    if let Some(ref merge_q) = self.merge_query {
                        // Build MERGE with actual values
                        build_merge_with_values(merge_q, &data.values)?
                    } else {
                        build_insert_with_values(
                            &self.query_template.build_query(data.diff),
                            &data.values,
                            self.snapshot_mode,
                            data.time.0,
                            data.diff,
                        )?
                    }
                } else if self.snapshot_mode && data.diff < 0 {
                    build_delete_with_values(&self.query_template, &data.values)?
                } else {
                    build_insert_with_values(
                        &self.query_template.build_query(data.diff),
                        &data.values,
                        self.snapshot_mode,
                        data.time.0,
                        data.diff,
                    )?
                };

                client
                    .execute(&*query_str, &[])
                    .await
                    .map_err(mssql_write_err)?;
            }

            client
                .simple_query("COMMIT TRANSACTION")
                .await
                .map_err(mssql_write_err)?
                .into_results()
                .await
                .map_err(mssql_write_err)?;

            Ok::<_, WriteError>(())
        })?;

        // Only clear buffer on success (it was already taken via take())
        Ok(())
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

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
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

/// Build a MERGE statement template for snapshot upserts.
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

    // Template: VALUES placeholders will be replaced at execution time
    format!(
        "MERGE INTO {table_name} AS target \
         USING (SELECT {{VALUES}}) AS source ({source_cols}) \
         ON {on_clause} \
         WHEN MATCHED THEN UPDATE SET {update_set} \
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_source});"
    )
}

/// Replace {{VALUES}} in a MERGE template with actual SQL literal values.
fn build_merge_with_values(merge_template: &str, values: &[Value]) -> Result<String, WriteError> {
    let sql_values: Vec<String> = values
        .iter()
        .map(MssqlWriter::value_to_sql_literal)
        .collect::<Result<_, _>>()?;
    let values_str = sql_values.join(", ");
    Ok(merge_template.replace("{VALUES}", &values_str))
}

/// Build an INSERT statement with literal values (for stream-of-changes mode).
fn build_insert_with_values(
    query_template: &str,
    values: &[Value],
    snapshot_mode: bool,
    time: u64,
    diff: isize,
) -> Result<String, WriteError> {
    let mut sql_values: Vec<String> = values
        .iter()
        .map(MssqlWriter::value_to_sql_literal)
        .collect::<Result<_, _>>()?;

    if !snapshot_mode {
        sql_values.push(time.to_string());
        sql_values.push(diff.to_string());
    }

    // Replace @P1, @P2, etc. with actual values
    let mut result = query_template.to_string();
    for (i, val) in sql_values.iter().enumerate() {
        result = result.replace(&format!("@P{}", i + 1), val);
    }
    Ok(result)
}

/// Build a DELETE statement with literal values for snapshot mode deletions.
fn build_delete_with_values(
    query_template: &SqlQueryTemplate,
    values: &[Value],
) -> Result<String, WriteError> {
    let pk_values = query_template.primary_key_fields(values.to_vec());
    let sql_values: Vec<String> = pk_values
        .iter()
        .map(MssqlWriter::value_to_sql_literal)
        .collect::<Result<_, _>>()?;

    let delete_query = query_template.build_query(-1);
    let mut result = delete_query;
    for (i, val) in sql_values.iter().enumerate() {
        result = result.replace(&format!("@P{}", i + 1), val);
    }
    Ok(result)
}

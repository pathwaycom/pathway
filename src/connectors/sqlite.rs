// Copyright © 2026 Pathway

use log::error;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::take;
use std::str::from_utf8;
use std::thread::sleep;
use std::time::Duration;

use crate::connectors::data_storage::{CommitPossibility, ConversionError, ValuesMap};
use crate::connectors::metadata::SQLiteMetadata;
use crate::connectors::offset::EMPTY_OFFSET;
use crate::connectors::{DataEventType, ReadError, ReadResult, Reader, ReaderContext, StorageType};
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::{Type, Value};
use crate::persistence::frontier::OffsetAntichain;

use rusqlite::types::ValueRef as SqliteValue;
use rusqlite::Connection as SqliteConnection;

const SQLITE_DATA_VERSION_PRAGMA: &str = "data_version";

pub struct SqliteReader {
    connection: SqliteConnection,
    table_name: String,
    schema: Vec<(String, Type)>,

    last_saved_data_version: Option<i64>,
    stored_state: HashMap<i64, ValuesMap>,
    queued_updates: VecDeque<ReadResult>,
}

impl SqliteReader {
    pub fn new(
        connection: SqliteConnection,
        table_name: String,
        schema: Vec<(String, Type)>,
    ) -> Self {
        Self {
            connection,
            table_name,
            schema,

            last_saved_data_version: None,
            queued_updates: VecDeque::new(),
            stored_state: HashMap::new(),
        }
    }

    /// Data version is required to check if there was an update in the database.
    /// There are also hooks, but they only work for changes happened in the same
    /// connection.
    /// More details why hooks don't help here: <https://sqlite.org/forum/forumpost/3174b39eeb79b6a4>
    pub fn data_version(&self) -> i64 {
        loop {
            let version: ::rusqlite::Result<i64> = self.connection.pragma_query_value(
                Some(::rusqlite::DatabaseName::Main),
                SQLITE_DATA_VERSION_PRAGMA,
                |row| row.get(0),
            );
            match version {
                Ok(version) => return version,
                Err(e) => error!("pragma.data_version request has failed: {e}"),
            }
            sleep(Self::retry_after_error_period());
        }
    }

    /// Convert raw `SQLite` field into one of internal value types
    /// There are only five supported types: null, integer, real, text, blob
    /// See also: <https://www.sqlite.org/datatype3.html>
    fn convert_to_value(
        orig_value: SqliteValue<'_>,
        field_name: &str,
        dtype: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let value = match (dtype, orig_value) {
            (Type::Optional(_) | Type::Any, SqliteValue::Null) => Some(Value::None),
            (Type::Optional(arg), value) => Self::convert_to_value(value, field_name, arg).ok(),
            (Type::Int | Type::Any, SqliteValue::Integer(val)) => Some(Value::Int(val)),
            (Type::Float | Type::Any, SqliteValue::Real(val)) => Some(Value::Float(val.into())),
            (Type::String | Type::Any, SqliteValue::Text(val)) => from_utf8(val)
                .ok()
                .map(|parsed_string| Value::String(parsed_string.into())),
            (Type::Json, SqliteValue::Text(val)) => from_utf8(val)
                .ok()
                .and_then(|parsed_string| {
                    serde_json::from_str::<serde_json::Value>(parsed_string).ok()
                })
                .map(Value::from),
            (Type::Bytes | Type::Any, SqliteValue::Blob(val)) => Some(Value::Bytes(val.into())),
            _ => None,
        };
        if let Some(value) = value {
            Ok(value)
        } else {
            let value_repr = limit_length(format!("{orig_value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
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
        let query = format!(
            "SELECT {},_rowid_ FROM {}",
            column_names.join(","),
            self.table_name
        );

        let mut statement = self.connection.prepare(&query)?;
        let mut rows = statement.query([])?;

        let mut present_rowids = HashSet::new();
        while let Some(row) = rows.next()? {
            let rowid: i64 = row.get(self.schema.len())?;
            let mut values = HashMap::with_capacity(self.schema.len());
            for (column_idx, (column_name, column_dtype)) in self.schema.iter().enumerate() {
                let value =
                    Self::convert_to_value(row.get_ref(column_idx)?, column_name, column_dtype);
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

impl Reader for SqliteReader {
    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        todo!("seek is not supported for Sqlite source: persistent history of changes unavailable")
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(queued_update) = self.queued_updates.pop_front() {
                return Ok(queued_update);
            }

            let current_data_version = self.data_version();
            if self.last_saved_data_version != Some(current_data_version) {
                self.load_table()?;
                self.last_saved_data_version = Some(current_data_version);
                return Ok(ReadResult::NewSource(
                    SQLiteMetadata::new(current_data_version).into(),
                ));
            }
            // Sleep to avoid non-stop pragma requests of a table
            // that did not change
            sleep(Self::wait_period());
        }
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("SQLite({})", self.table_name).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Sqlite
    }
}

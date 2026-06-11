// Copyright © 2026 Pathway

use base64::Engine;

use questdb::ingress::{
    Buffer as QuestDBBuffer, Sender as QuestDBSender, Timestamp as QuestDBTimestamp,
    TimestampMicros as QuestDBTimestampMicros, TimestampNanos as QuestDBTimestampNanos,
};

use crate::connectors::data_format::{
    create_bincoded_value, serialize_value_to_json, FormatterContext, FormatterError,
};
use crate::connectors::{SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::time::DateTime;
use crate::engine::Value;

use super::{WriteError, Writer};

#[allow(clippy::enum_variant_names)]
pub enum QuestDBAtColumnPolicy {
    UseNow,
    UsePathwayTime,
    UseColumn(usize),
}

// Flush the ILP buffer to QuestDB once it grows past this many bytes,
// instead of accumulating a whole minibatch. questdb-rs rejects a single
// flush above its max_buf_size (default 100 MiB), so an unbounded buffer
// crashes on large minibatches; chunking also bounds the writer's memory
// and overlaps serialization with the network. Overridable via
// PATHWAY_QUESTDB_FLUSH_BYTES (mainly for benchmarking).
const QUESTDB_DEFAULT_FLUSH_BYTES: usize = 48 * 1024 * 1024;

pub struct QuestDBWriter {
    sender: QuestDBSender,
    table_name: String,
    field_names: Vec<String>,
    designated_timestamp_policy: QuestDBAtColumnPolicy,
    buffer: QuestDBBuffer,
    has_updates: bool,
    max_buffer_bytes: usize,
}

impl QuestDBWriter {
    // Returns `Result` for symmetry with the other SQL/ingest writers and
    // so reintroducing a fallible setup step here stays source-compatible.
    #[allow(clippy::unnecessary_wraps)]
    pub fn new(
        sender: QuestDBSender,
        table_name: String,
        field_names: Vec<String>,
        designated_timestamp_policy: QuestDBAtColumnPolicy,
    ) -> Result<Self, WriteError> {
        // The ILP buffer is row-oriented: every row begins with a
        // `table()` call and ends with `at()`/`at_now()`. We therefore
        // start each row inside `write` rather than priming the buffer
        // here.
        let buffer = QuestDBBuffer::new();
        let max_buffer_bytes = std::env::var("PATHWAY_QUESTDB_FLUSH_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(QUESTDB_DEFAULT_FLUSH_BYTES);
        Ok(Self {
            sender,
            table_name,
            field_names,
            designated_timestamp_policy,
            buffer,
            has_updates: false,
            max_buffer_bytes,
        })
    }

    fn put_value_into_buffer(
        buffer: &mut QuestDBBuffer,
        value: Value,
        column_name: &str,
    ) -> Result<(), WriteError> {
        match value {
            Value::None => buffer, // just don't specify the value
            Value::Bool(b) => buffer.column_bool(column_name, b)?,
            Value::Int(i) => buffer.column_i64(column_name, i)?,
            Value::Float(f) => buffer.column_f64(column_name, *f)?,
            Value::String(s) => buffer.column_str(column_name, s)?,
            Value::DateTimeNaive(dt) => buffer.column_ts(
                column_name,
                QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(dt.timestamp())),
            )?,
            Value::DateTimeUtc(dt) => buffer.column_ts(
                column_name,
                QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(dt.timestamp())),
            )?,
            Value::Duration(d) => buffer.column_i64(column_name, d.nanoseconds())?,
            Value::Json(j) => buffer.column_str(column_name, j.to_string())?,
            Value::PyObjectWrapper(_) => {
                buffer.column_str(column_name, create_bincoded_value(&value)?)?
            }
            Value::Pointer(p) => buffer.column_str(column_name, p.to_string())?,
            Value::Bytes(b) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                buffer.column_str(column_name, encoded)?
            }
            Value::IntArray(_) | Value::FloatArray(_) | Value::Tuple(_) => {
                let json_value = serialize_value_to_json(&value)?;
                buffer.column_str(column_name, json_value.to_string())?
            }
            Value::Pending | Value::Error => Err(FormatterError::ValueNonSerializable(
                value.kind(),
                "QuestDB",
            ))?,
        };
        Ok(())
    }
}

impl Writer for QuestDBWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        // Begin a fresh ILP row. This MUST be called once per row,
        // before any `column_*` call: `table()` opens a row and the
        // trailing `at()`/`at_now()` closes it. Previously the buffer
        // was primed only in `new`/`flush`, so the second and later
        // rows of any multi-row minibatch hit
        // "Bad call to `column`, should have called `flush` or `table`".
        self.buffer.table(self.table_name.as_str())?;

        let (at_timestamp, skip_column_id) =
            if let QuestDBAtColumnPolicy::UseColumn(column_id) = self.designated_timestamp_policy {
                let at_value = &data.values[column_id];
                match at_value {
                    Value::DateTimeNaive(dt) => (
                        Some(QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(
                            dt.timestamp(),
                        ))),
                        column_id,
                    ),
                    Value::DateTimeUtc(dt) => (
                        Some(QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(
                            dt.timestamp(),
                        ))),
                        column_id,
                    ),
                    _ => return Err(WriteError::QuestDBAtColumnNotTime(at_value.clone())),
                }
            } else {
                (None, data.values.len())
            };

        for (column_id, (value, column_name)) in data
            .values
            .into_iter()
            .zip(self.field_names.iter())
            .enumerate()
        {
            if column_id == skip_column_id {
                continue;
            }
            Self::put_value_into_buffer(&mut self.buffer, value, column_name.as_str())?;
        }
        self.buffer.column_i64(
            SPECIAL_FIELD_DIFF,
            data.diff
                .try_into()
                .expect("pathway diff can only be 1 or -1"),
        )?;
        let pathway_time_casted = QuestDBTimestampMicros::new(
            data.time
                .0
                .checked_mul(1000) // Pathway minibatch time is milliseconds, hence multiplication by 1000 is needed
                .expect("pathway time must fit 64bit signed integer")
                .try_into()
                .expect("pathway time must be nonnegative"),
        );
        match self.designated_timestamp_policy {
            QuestDBAtColumnPolicy::UseNow => {
                self.buffer
                    .column_ts(SPECIAL_FIELD_TIME, pathway_time_casted)?;
                self.buffer.at_now()?;
            }
            QuestDBAtColumnPolicy::UsePathwayTime => self.buffer.at(pathway_time_casted)?,
            QuestDBAtColumnPolicy::UseColumn(_) => {
                self.buffer
                    .column_ts(SPECIAL_FIELD_TIME, pathway_time_casted)?;
                self.buffer
                    .at(at_timestamp.expect("at_timestamp must have defined upstream"))?;
            }
        }
        self.has_updates = true;

        // Bound the in-flight buffer: flush mid-minibatch once it grows
        // past the threshold so we never hand `questdb-rs` a buffer above
        // its `max_buf_size` (which would panic the worker) and so memory
        // stays bounded for arbitrarily large minibatches.
        if self.buffer.len() >= self.max_buffer_bytes {
            self.sender.flush(&mut self.buffer)?;
            self.has_updates = false;
        }

        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.has_updates {
            // `Sender::flush` drains (clears) the buffer; the next
            // `write` starts the following row with its own `table()`.
            self.sender.flush(&mut self.buffer)?;
            self.has_updates = false;
        }
        Ok(())
    }

    fn name(&self) -> String {
        format!("QuestDB({})", self.table_name)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }
}

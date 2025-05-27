use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;
use deltalake::arrow::datatypes::Schema as ArrowSchema;

use super::{
    construct_column_order, construct_column_types_map, delta::open_and_read_delta_table,
    MaintenanceMode, SPECIAL_FIELD_ID,
};
use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_lake::arrow::array_for_type as arrow_array_for_type;
use crate::connectors::WriteError;
use crate::engine::{Key, Value};
use crate::python_api::ValueField;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PayloadType {
    FullSnapshot,
    Diff,
}

pub trait ColumnBuffer: Send {
    fn add_event(&mut self, data: FormatterContext) -> Result<(), WriteError>;
    fn build_update_record_batch(&mut self) -> Result<(ArrowRecordBatch, PayloadType), WriteError>;
    fn on_changes_written(&mut self);
    fn has_updates(&self) -> bool;
}

pub struct AppendOnlyColumnBuffer {
    schema: Arc<ArrowSchema>,
    buffered_columns: Vec<Vec<Value>>,
}

impl AppendOnlyColumnBuffer {
    pub fn new(schema: Arc<ArrowSchema>) -> Self {
        let empty_buffered_columns = vec![Vec::new(); schema.fields().len()];
        Self {
            schema,
            buffered_columns: empty_buffered_columns,
        }
    }
}

impl ColumnBuffer for AppendOnlyColumnBuffer {
    fn add_event(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for (index, value) in data.values.into_iter().enumerate() {
            self.buffered_columns[index].push(value);
        }
        let time_column_idx = self.buffered_columns.len() - 2;
        let diff_column_idx = self.buffered_columns.len() - 1;
        self.buffered_columns[time_column_idx].push(Value::Int(data.time.0.try_into().unwrap()));
        self.buffered_columns[diff_column_idx].push(Value::Int(data.diff.try_into().unwrap()));
        Ok(())
    }

    fn build_update_record_batch(&mut self) -> Result<(ArrowRecordBatch, PayloadType), WriteError> {
        build_append_record_batch(&self.buffered_columns, &self.schema)
    }

    fn on_changes_written(&mut self) {
        for column in &mut self.buffered_columns {
            column.clear();
        }
    }

    fn has_updates(&self) -> bool {
        !self.buffered_columns[0].is_empty()
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IncorrectSnapshotError {
    #[error("no '_id' field found in the schema")]
    NoIdField,

    #[error("the number of fields mismatches between the schema and the actual snapshot")]
    FieldNumberMismatch,

    #[error("'_id' field is not a pointer: {0}")]
    IdFieldIncorrectType(Value),
}

pub struct SnapshotColumnBuffer {
    schema: Arc<ArrowSchema>,
    state: HashMap<Key, Vec<Value>>,
    has_updates: bool,

    append_columns: Vec<Vec<Value>>,
    has_only_appends: bool,
}

impl SnapshotColumnBuffer {
    pub fn new(
        current_snapshot: Vec<Vec<Value>>,
        schema: Arc<ArrowSchema>,
    ) -> Result<Self, WriteError> {
        let key_field_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == SPECIAL_FIELD_ID)
            .ok_or(WriteError::IncorrectInitialSnapshot(
                IncorrectSnapshotError::NoIdField,
            ))?;

        let mut state = HashMap::with_capacity(current_snapshot.len());
        for mut entry in current_snapshot {
            if entry.len() != schema.fields().len() {
                return Err(WriteError::IncorrectInitialSnapshot(
                    IncorrectSnapshotError::FieldNumberMismatch,
                ));
            }
            let key = entry.remove(key_field_idx);
            let Value::Pointer(key) = key else {
                return Err(WriteError::IncorrectInitialSnapshot(
                    IncorrectSnapshotError::IdFieldIncorrectType(key),
                ));
            };
            state.insert(key, entry);
        }

        let empty_append_columns = vec![Vec::new(); schema.fields().len()];

        Ok(Self {
            schema,
            state,
            has_updates: false,
            append_columns: empty_append_columns,
            has_only_appends: true,
        })
    }

    pub fn new_for_delta_table(
        path: &str,
        storage_options: HashMap<String, String>,
        value_fields: &[ValueField],
        schema: Arc<ArrowSchema>,
    ) -> Result<Self, WriteError> {
        let column_types = construct_column_types_map(value_fields, MaintenanceMode::Snapshot);
        let column_order = construct_column_order(value_fields, MaintenanceMode::Snapshot);
        let existing_table =
            open_and_read_delta_table(path, storage_options, &column_types, &column_order)?;
        Self::new(existing_table, schema)
    }

    fn handle_row_addition(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        if self.has_only_appends {
            let id_column_index = self.append_columns.len() - 1;
            for (index, value) in data.values.iter().enumerate() {
                self.append_columns[index].push(value.clone());
            }
            self.append_columns[id_column_index].push(Value::Pointer(data.key));
        }

        match self.state.entry(data.key) {
            Entry::Occupied(_) => Err(WriteError::TableAlreadyContainsKey(data.key)),
            Entry::Vacant(entry) => {
                entry.insert(data.values);
                Ok(())
            }
        }
    }

    fn handle_row_deletion(&mut self, data: &FormatterContext) -> Result<(), WriteError> {
        self.state
            .remove(&data.key)
            .ok_or(WriteError::TableDoesntContainKey(data.key))?;
        Ok(())
    }

    fn build_snapshot_record_batch(&self) -> Result<(ArrowRecordBatch, PayloadType), WriteError> {
        let num_fields = self.schema.fields().len();
        let id_column_index = num_fields - 1;
        let mut buffered_columns = vec![Vec::with_capacity(self.state.len()); num_fields];

        for (key, values) in &self.state {
            values
                .iter()
                .enumerate()
                .for_each(|(index, value)| buffered_columns[index].push(value.clone()));
            buffered_columns[id_column_index].push(Value::Pointer(*key));
        }

        let arrow_columns = buffered_columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                arrow_array_for_type(self.schema.field(index).data_type(), &column)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok((
            ArrowRecordBatch::try_new(self.schema.clone(), arrow_columns)?,
            PayloadType::FullSnapshot,
        ))
    }
}

impl ColumnBuffer for SnapshotColumnBuffer {
    fn add_event(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        if data.diff == 1 {
            self.handle_row_addition(data)?;
        } else if data.diff == -1 {
            self.has_only_appends = false;
            self.handle_row_deletion(&data)?;
        } else {
            panic!("Unexpected value of diff: {}", data.diff);
        }
        self.has_updates = true;
        Ok(())
    }

    fn build_update_record_batch(&mut self) -> Result<(ArrowRecordBatch, PayloadType), WriteError> {
        if self.has_only_appends {
            build_append_record_batch(&self.append_columns, &self.schema)
        } else {
            self.build_snapshot_record_batch()
        }
    }

    fn on_changes_written(&mut self) {
        self.has_updates = false;
        self.has_only_appends = true;
        for column in &mut self.append_columns {
            column.clear();
        }
    }

    fn has_updates(&self) -> bool {
        self.has_updates
    }
}

fn build_append_record_batch(
    buffered_columns: &[Vec<Value>],
    schema: &Arc<ArrowSchema>,
) -> Result<(ArrowRecordBatch, PayloadType), WriteError> {
    let mut data_columns = Vec::new();
    for (index, column) in buffered_columns.iter().enumerate() {
        let arrow_array = arrow_array_for_type(schema.field(index).data_type(), column)?;
        data_columns.push(arrow_array);
    }
    Ok((
        ArrowRecordBatch::try_new(schema.clone(), data_columns)?,
        PayloadType::Diff,
    ))
}

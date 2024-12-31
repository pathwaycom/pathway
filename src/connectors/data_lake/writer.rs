use std::sync::Arc;
use std::time::{Duration, Instant};

use deltalake::arrow::array::Array as ArrowArray;
use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;
use deltalake::arrow::array::{
    BinaryArray as ArrowBinaryArray, BooleanArray as ArrowBooleanArray,
    Float64Array as ArrowFloat64Array, Int64Array as ArrowInt64Array,
    StringArray as ArrowStringArray, TimestampMicrosecondArray as ArrowTimestampArray,
};
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
};

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_lake::LakeBatchWriter;
use crate::connectors::{WriteError, Writer};
use crate::engine::time::DateTime as EngineDateTime;
use crate::engine::{Type, Value};
use crate::python_api::ValueField;

const SPECIAL_OUTPUT_FIELDS: [(&str, Type); 2] = [("time", Type::Int), ("diff", Type::Int)];

#[allow(clippy::module_name_repetitions)]
pub struct LakeWriter {
    batch_writer: Box<dyn LakeBatchWriter>,
    schema: Arc<ArrowSchema>,
    buffered_columns: Vec<Vec<Value>>,
    min_commit_frequency: Option<Duration>,
    last_commit_at: Instant,
}

impl LakeWriter {
    pub fn new(
        batch_writer: Box<dyn LakeBatchWriter>,
        value_fields: &Vec<ValueField>,
        min_commit_frequency: Option<Duration>,
    ) -> Result<Self, WriteError> {
        let schema = Arc::new(Self::construct_schema(value_fields)?);
        let mut empty_buffered_columns = Vec::new();
        empty_buffered_columns.resize_with(schema.flattened_fields().len(), Vec::new);
        Ok(Self {
            batch_writer,
            schema,
            buffered_columns: empty_buffered_columns,
            min_commit_frequency,

            // before the first commit, the time should be
            // measured from the moment of the start
            last_commit_at: Instant::now(),
        })
    }

    fn array_of_target_type<ElementType>(
        values: &Vec<Value>,
        mut to_simple_type: impl FnMut(&Value) -> Result<ElementType, WriteError>,
    ) -> Result<Vec<Option<ElementType>>, WriteError> {
        let mut values_vec: Vec<Option<ElementType>> = Vec::new();
        for value in values {
            if matches!(value, Value::None) {
                values_vec.push(None);
                continue;
            }
            values_vec.push(Some(to_simple_type(value)?));
        }
        Ok(values_vec)
    }

    fn arrow_array_for_type(
        type_: &ArrowDataType,
        values: &Vec<Value>,
    ) -> Result<Arc<dyn ArrowArray>, WriteError> {
        match type_ {
            ArrowDataType::Boolean => {
                let v = Self::array_of_target_type::<bool>(values, |v| match v {
                    Value::Bool(b) => Ok(*b),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowBooleanArray::from(v)))
            }
            ArrowDataType::Int64 => {
                let v = Self::array_of_target_type::<i64>(values, |v| match v {
                    Value::Int(i) => Ok(*i),
                    Value::Duration(d) => Ok(d.microseconds()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowInt64Array::from(v)))
            }
            ArrowDataType::Float64 => {
                let v = Self::array_of_target_type::<f64>(values, |v| match v {
                    Value::Float(f) => Ok((*f).into()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowFloat64Array::from(v)))
            }
            ArrowDataType::Utf8 => {
                let v = Self::array_of_target_type::<String>(values, |v| match v {
                    Value::String(s) => Ok(s.to_string()),
                    Value::Pointer(p) => Ok(p.to_string()),
                    Value::Json(j) => Ok(j.to_string()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowStringArray::from(v)))
            }
            ArrowDataType::Binary => {
                let mut vec_owned = Self::array_of_target_type::<Vec<u8>>(values, |v| match v {
                    Value::Bytes(b) => Ok(b.to_vec()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                let mut vec_refs = Vec::new();
                for item in &mut vec_owned {
                    vec_refs.push(item.as_mut().map(|v| v.as_slice()));
                }
                Ok(Arc::new(ArrowBinaryArray::from(vec_refs)))
            }
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None) => {
                let v = Self::array_of_target_type::<i64>(values, |v| match v {
                    #[allow(clippy::cast_possible_truncation)]
                    Value::DateTimeNaive(dt) => Ok(dt.timestamp_microseconds()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowTimestampArray::from(v)))
            }
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some(tz)) => {
                let v = Self::array_of_target_type::<i64>(values, |v| match v {
                    #[allow(clippy::cast_possible_truncation)]
                    Value::DateTimeUtc(dt) => Ok(dt.timestamp_microseconds()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowTimestampArray::from(v).with_timezone(&**tz)))
            }
            _ => panic!("provided type {type_} is unknown to the engine"),
        }
    }

    fn prepare_arrow_batch(&self) -> Result<ArrowRecordBatch, WriteError> {
        let mut data_columns = Vec::new();
        for (index, column) in self.buffered_columns.iter().enumerate() {
            data_columns.push(Self::arrow_array_for_type(
                self.schema.field(index).data_type(),
                column,
            )?);
        }
        Ok(ArrowRecordBatch::try_new(
            self.schema.clone(),
            data_columns,
        )?)
    }

    fn arrow_data_type(type_: &Type) -> Result<ArrowDataType, WriteError> {
        Ok(match type_ {
            Type::Bool => ArrowDataType::Boolean,
            Type::Int | Type::Duration => ArrowDataType::Int64,
            Type::Float => ArrowDataType::Float64,
            Type::Pointer | Type::String | Type::Json => ArrowDataType::Utf8,
            Type::Bytes => ArrowDataType::Binary,
            // DeltaLake timestamps are stored in microseconds:
            // https://docs.rs/deltalake/latest/deltalake/kernel/enum.PrimitiveType.html#variant.Timestamp
            Type::DateTimeNaive => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
            Type::DateTimeUtc => {
                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()))
            }
            Type::Optional(wrapped) => return Self::arrow_data_type(wrapped),
            Type::Any
            | Type::Array(_, _)
            | Type::Tuple(_)
            | Type::List(_)
            | Type::PyObjectWrapper => return Err(WriteError::UnsupportedType(type_.clone())),
        })
    }

    pub fn construct_schema(value_fields: &Vec<ValueField>) -> Result<ArrowSchema, WriteError> {
        let mut schema_fields: Vec<ArrowField> = Vec::new();
        for field in value_fields {
            schema_fields.push(ArrowField::new(
                field.name.clone(),
                Self::arrow_data_type(&field.type_)?,
                field.type_.can_be_none(),
            ));
        }
        for (field, type_) in SPECIAL_OUTPUT_FIELDS {
            schema_fields.push(ArrowField::new(
                field,
                Self::arrow_data_type(&type_)?,
                false,
            ));
        }
        Ok(ArrowSchema::new(schema_fields))
    }
}

impl Writer for LakeWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for (index, value) in data.values.into_iter().enumerate() {
            self.buffered_columns[index].push(value);
        }
        let time_column_idx = self.buffered_columns.len() - 2;
        let diff_column_idx = self.buffered_columns.len() - 1;
        self.buffered_columns[time_column_idx].push(Value::Int(data.time.0.try_into().unwrap()));
        self.buffered_columns[diff_column_idx].push(Value::Int(data.diff.try_into().unwrap()));
        Ok(())
    }

    fn flush(&mut self, forced: bool) -> Result<(), WriteError> {
        let commit_needed = !self.buffered_columns[0].is_empty()
            && (self
                .min_commit_frequency
                .map_or(true, |f| self.last_commit_at.elapsed() >= f)
                || forced);
        if commit_needed {
            self.batch_writer.write_batch(self.prepare_arrow_batch()?)?;
            for column in &mut self.buffered_columns {
                column.clear();
            }
        }
        Ok(())
    }
}

use log::warn;
use std::collections::HashMap;

use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;
use deltalake::kernel::DataType as DeltaTableKernelType;
use deltalake::kernel::PrimitiveType as DeltaTablePrimitiveType;
use deltalake::kernel::StructField as DeltaTableStructField;
use deltalake::operations::create::CreateBuilder as DeltaTableCreateBuilder;
use deltalake::protocol::SaveMode as DeltaTableSaveMode;
use deltalake::writer::{DeltaWriter, RecordBatchWriter as DTRecordBatchWriter};
use deltalake::{open_table_with_storage_options as open_delta_table, DeltaTable, TableProperty};

use super::{LakeBatchWriter, SPECIAL_OUTPUT_FIELDS};
use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::WriteError;
use crate::engine::Type;
use crate::python_api::ValueField;

#[allow(clippy::module_name_repetitions)]
pub struct DeltaBatchWriter {
    table: DeltaTable,
    writer: DTRecordBatchWriter,
}

impl DeltaBatchWriter {
    pub fn new(
        path: &str,
        value_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
    ) -> Result<Self, WriteError> {
        let table = Self::open_table(path, value_fields, storage_options)?;
        let writer = DTRecordBatchWriter::for_table(&table)?;
        Ok(Self { table, writer })
    }

    pub fn open_table(
        path: &str,
        schema_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
    ) -> Result<DeltaTable, WriteError> {
        let mut struct_fields = Vec::new();
        for field in schema_fields {
            struct_fields.push(DeltaTableStructField::new(
                field.name.clone(),
                Self::delta_table_primitive_type(&field.type_)?,
                field.type_.can_be_none(),
            ));
        }
        for (field, type_) in SPECIAL_OUTPUT_FIELDS {
            struct_fields.push(DeltaTableStructField::new(
                field,
                Self::delta_table_primitive_type(&type_)?,
                false,
            ));
        }

        let runtime = create_async_tokio_runtime()?;
        let table: DeltaTable = runtime
            .block_on(async {
                let builder = DeltaTableCreateBuilder::new()
                    .with_location(path)
                    .with_save_mode(DeltaTableSaveMode::Append)
                    .with_columns(struct_fields)
                    .with_configuration_property(TableProperty::AppendOnly, Some("true"))
                    .with_storage_options(storage_options.clone());

                builder.await
            })
            .or_else(
                |e| {
                    warn!("Unable to create DeltaTable for output: {e}. Trying to open the existing one by this path.");
                    runtime.block_on(async {
                        open_delta_table(path, storage_options).await
                    })
                }
            )?;

        Ok(table)
    }

    fn delta_table_primitive_type(type_: &Type) -> Result<DeltaTableKernelType, WriteError> {
        Ok(DeltaTableKernelType::Primitive(match type_ {
            Type::Bool => DeltaTablePrimitiveType::Boolean,
            Type::Float => DeltaTablePrimitiveType::Double,
            Type::String | Type::Json => DeltaTablePrimitiveType::String,
            Type::Bytes => DeltaTablePrimitiveType::Binary,
            Type::DateTimeNaive => DeltaTablePrimitiveType::TimestampNtz,
            Type::DateTimeUtc => DeltaTablePrimitiveType::Timestamp,
            Type::Int | Type::Duration => DeltaTablePrimitiveType::Long,
            Type::Optional(wrapped) => return Self::delta_table_primitive_type(wrapped),
            Type::Any
            | Type::Array(_, _)
            | Type::Tuple(_)
            | Type::List(_)
            | Type::PyObjectWrapper
            | Type::Pointer => return Err(WriteError::UnsupportedType(type_.clone())),
        }))
    }
}

impl LakeBatchWriter for DeltaBatchWriter {
    fn write_batch(&mut self, batch: ArrowRecordBatch) -> Result<(), WriteError> {
        create_async_tokio_runtime()?.block_on(async {
            self.writer.write(batch).await?;
            self.writer.flush_and_commit(&mut self.table).await?;
            Ok::<(), WriteError>(())
        })
    }
}

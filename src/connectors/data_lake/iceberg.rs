use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;
use deltalake::parquet::file::properties::WriterProperties;
use iceberg::spec::{
    NestedField, PrimitiveType as IcebergPrimitiveType, Schema as IcebergSchema,
    Type as IcebergType, UnboundPartitionSpec,
};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use tokio::runtime::Runtime as TokioRuntime;

use super::{LakeBatchWriter, SPECIAL_OUTPUT_FIELDS};
use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::WriteError;
use crate::engine::Type;
use crate::python_api::ValueField;
use crate::timestamp::current_unix_timestamp_ms;

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct IcebergDBParams {
    uri: String,
    warehouse: Option<String>,
    namespace: Vec<String>,
}

impl IcebergDBParams {
    pub fn new(uri: String, warehouse: Option<String>, namespace: Vec<String>) -> Self {
        Self {
            uri,
            warehouse,
            namespace,
        }
    }

    pub fn create_catalog(&self) -> RestCatalog {
        let config_builder = RestCatalogConfig::builder().uri(self.uri.clone());
        let config = if let Some(warehouse) = &self.warehouse {
            config_builder.warehouse(warehouse.clone()).build()
        } else {
            config_builder.build()
        };
        RestCatalog::new(config)
    }

    pub fn ensure_namespace(
        &self,
        runtime: &TokioRuntime,
        catalog: &RestCatalog,
    ) -> Result<Namespace, WriteError> {
        let ident = NamespaceIdent::from_strs(self.namespace.clone())?;
        Ok(runtime.block_on(async {
            if let Ok(ns) = catalog.get_namespace(&ident).await {
                return Ok(ns);
            }
            catalog
                .create_namespace(
                    &ident,
                    HashMap::from([("author".to_string(), "pathway".to_string())]),
                )
                .await
        })?)
    }
}

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct IcebergTableParams {
    name: String,
    schema: IcebergSchema,
}

impl IcebergTableParams {
    pub fn new(name: String, fields: &[ValueField]) -> Result<Self, WriteError> {
        let schema = Self::build_schema(fields)?;
        Ok(Self { name, schema })
    }

    pub fn ensure_table(
        &self,
        runtime: &TokioRuntime,
        catalog: &RestCatalog,
        namespace: &Namespace,
        warehouse: Option<&String>,
    ) -> Result<IcebergTable, WriteError> {
        let table_ident = TableIdent::new(namespace.name().clone(), self.name.clone());
        let table = runtime.block_on(async {
            if let Ok(t) = catalog.load_table(&table_ident).await {
                Ok(t)
            } else {
                let creation_builder = TableCreation::builder()
                    .name(self.name.clone())
                    .partition_spec(UnboundPartitionSpec::builder().with_spec_id(1).build())
                    .properties(HashMap::from([(
                        "author".to_string(),
                        "pathway".to_string(),
                    )]))
                    .schema(self.schema.clone());

                let creation = if let Some(warehouse) = warehouse {
                    creation_builder.location(warehouse.clone()).build()
                } else {
                    creation_builder.build()
                };

                catalog.create_table(namespace.name(), creation).await
            }
        })?;

        Ok(table)
    }

    fn build_schema(fields: &[ValueField]) -> Result<IcebergSchema, WriteError> {
        let mut nested_fields = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            nested_fields.push(Arc::new(NestedField::new(
                (index + 1).try_into().unwrap(),
                field.name.clone(),
                Self::iceberg_type(&field.type_)?,
                false, // No optional fields
            )));
        }
        let mut current_field_index = fields.len();
        for (name, type_) in SPECIAL_OUTPUT_FIELDS {
            current_field_index += 1;
            nested_fields.push(Arc::new(NestedField::new(
                current_field_index.try_into().unwrap(),
                name,
                Self::iceberg_type(&type_)?,
                false,
            )));
        }
        let iceberg_schema = IcebergSchema::builder()
            .with_fields(nested_fields)
            .build()?;
        Ok(iceberg_schema)
    }

    fn iceberg_type(type_: &Type) -> Result<IcebergType, WriteError> {
        Ok(IcebergType::Primitive(match type_ {
            Type::Bool => IcebergPrimitiveType::Boolean,
            Type::Float => IcebergPrimitiveType::Double,
            Type::String | Type::Json => IcebergPrimitiveType::String,
            Type::Bytes => IcebergPrimitiveType::Binary,
            Type::DateTimeNaive => IcebergPrimitiveType::Timestamp,
            Type::DateTimeUtc => IcebergPrimitiveType::Timestamptz,
            Type::Int | Type::Duration => IcebergPrimitiveType::Long,
            Type::Optional(wrapped) => return Self::iceberg_type(wrapped),
            Type::Any
            | Type::Array(_, _)
            | Type::Tuple(_)
            | Type::List(_)  // TODO: it is possible to support lists with the usage of IcebergType::List
            | Type::PyObjectWrapper
            | Type::Pointer => return Err(WriteError::UnsupportedType(type_.clone())),
        }))
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct IcebergBatchWriter {
    runtime: TokioRuntime,
    catalog: RestCatalog,
    table: IcebergTable,
    table_ident: TableIdent,
}

impl IcebergBatchWriter {
    pub fn new(
        db_params: &IcebergDBParams,
        table_params: &IcebergTableParams,
    ) -> Result<Self, WriteError> {
        let runtime = create_async_tokio_runtime()?;
        let catalog = db_params.create_catalog();
        let namespace = db_params.ensure_namespace(&runtime, &catalog)?;
        let table = table_params.ensure_table(
            &runtime,
            &catalog,
            &namespace,
            db_params.warehouse.as_ref(),
        )?;
        Ok(Self {
            runtime,
            catalog,
            table,
            table_ident: TableIdent::new(namespace.name().clone(), table_params.name.clone()),
        })
    }

    fn create_writer_builder(
        table: &IcebergTable,
    ) -> Result<
        DataFileWriterBuilder<
            ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
        >,
        WriteError,
    > {
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("block-{}", current_unix_timestamp_ms()),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
            table.file_io().clone(),
            location_generator.clone(),
            file_name_generator.clone(),
        );
        Ok(DataFileWriterBuilder::new(parquet_writer_builder, None))
    }
}

impl LakeBatchWriter for IcebergBatchWriter {
    fn write_batch(&mut self, batch: ArrowRecordBatch) -> Result<(), WriteError> {
        let writer_builder = Self::create_writer_builder(&self.table)?;
        self.runtime.block_on(async {
            // Prepare a new data block
            let mut data_file_writer = writer_builder.clone().build().await?;
            data_file_writer.write(batch).await?;
            let data_file = data_file_writer.close().await?;

            // Append the prepared data block to the table and commit the change
            let tx = Transaction::new(&self.table);
            let mut append_action = tx.fast_append(None, vec![])?;
            append_action.add_data_files(data_file.clone())?;
            let tx = append_action.apply().await?;
            let _ = tx.commit(&self.catalog).await?;

            self.table = self.catalog.load_table(&self.table_ident).await?;

            Ok::<(), WriteError>(())
        })
    }
}

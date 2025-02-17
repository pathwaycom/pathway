use log::warn;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use deltalake::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use deltalake::parquet::file::properties::WriterProperties;
use futures::{stream, StreamExt, TryStreamExt};
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::{
    ListType as IcebergListType, NestedField, NestedField as IcebergNestedField,
    PrimitiveType as IcebergPrimitiveType, Schema as IcebergSchema, Type as IcebergType,
};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Error as IcebergError;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use tokio::runtime::Runtime as TokioRuntime;

use super::{
    columns_into_pathway_values, LakeBatchWriter, LakeWriterSettings, SPECIAL_OUTPUT_FIELDS,
};
use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_storage::ConnectorMode;
use crate::connectors::metadata::IcebergMetadata;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError,
};
use crate::engine::Type;
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;
use crate::timestamp::current_unix_timestamp_ms;

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct IcebergDBParams {
    uri: String,
    warehouse: Option<String>,
    namespace: Vec<String>,
    props: HashMap<String, String>,
}

impl IcebergDBParams {
    pub fn new(
        uri: String,
        warehouse: Option<String>,
        namespace: Vec<String>,
        props: HashMap<String, String>,
    ) -> Self {
        Self {
            uri,
            warehouse,
            namespace,
            props,
        }
    }

    pub fn create_catalog(&self) -> RestCatalog {
        let config_builder = RestCatalogConfig::builder().uri(self.uri.clone());
        let config = if let Some(warehouse) = &self.warehouse {
            config_builder
                .warehouse(warehouse.clone())
                .props(self.props.clone())
                .build()
        } else {
            config_builder.props(self.props.clone()).build()
        };
        RestCatalog::new(config)
    }

    pub fn ensure_namespace(
        &self,
        runtime: &TokioRuntime,
        catalog: &RestCatalog,
    ) -> Result<Namespace, IcebergError> {
        let ident = NamespaceIdent::from_strs(self.namespace.clone())?;
        runtime.block_on(async {
            if let Ok(ns) = catalog.get_namespace(&ident).await {
                return Ok(ns);
            }
            catalog
                .create_namespace(
                    &ident,
                    HashMap::from([("author".to_string(), "pathway".to_string())]),
                )
                .await
        })
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
        let iceberg_type = match type_ {
            Type::Bool => IcebergType::Primitive(IcebergPrimitiveType::Boolean),
            Type::Float => IcebergType::Primitive(IcebergPrimitiveType::Double),
            Type::String | Type::Json | Type::Pointer => {
                IcebergType::Primitive(IcebergPrimitiveType::String)
            }
            Type::Bytes | Type::PyObjectWrapper => {
                IcebergType::Primitive(IcebergPrimitiveType::Binary)
            }
            Type::DateTimeNaive => IcebergType::Primitive(IcebergPrimitiveType::Timestamp),
            Type::DateTimeUtc => IcebergType::Primitive(IcebergPrimitiveType::Timestamptz),
            Type::Int | Type::Duration => IcebergType::Primitive(IcebergPrimitiveType::Long),
            Type::Optional(wrapped) => Self::iceberg_type(wrapped)?,
            Type::List(element_type) => {
                let element_type_is_optional = element_type.is_optional();
                let nested_element_type = Self::iceberg_type(element_type.unoptionalize())?;
                let nested_type = IcebergNestedField::new(
                    0,
                    "element",
                    nested_element_type,
                    !element_type_is_optional,
                );
                let array_type = IcebergListType::new(nested_type.into());
                IcebergType::List(array_type)
            }
            Type::Any | Type::Array(_, _) | Type::Tuple(_) | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()))
            }
        };
        Ok(iceberg_type)
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

    fn settings(&self) -> LakeWriterSettings {
        LakeWriterSettings {
            use_64bit_size_type: true,
            utc_timezone_name: "+00:00".into(),
        }
    }

    fn name(&self) -> String {
        format!(
            "Iceberg({}, {})",
            self.table_ident.namespace.to_url_string(),
            self.table_ident.name
        )
    }
}

/// Wrapper for `FileScanTask` that allows to compare them.
#[derive(Debug, Eq, Hash, PartialEq)]
struct FileScanTaskDescriptor {
    data_file_path: String,
    start: u64,
    length: u64,
}

impl FileScanTaskDescriptor {
    fn for_task(task: &FileScanTask) -> Self {
        Self {
            data_file_path: task.data_file_path.clone(),
            start: task.start,
            length: task.length,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub type IcebergSnapshotId = i64;

#[allow(clippy::module_name_repetitions)]
pub struct IcebergReader {
    catalog: RestCatalog,
    table_ident: TableIdent,
    column_types: HashMap<String, Type>,
    streaming_mode: ConnectorMode,

    runtime: TokioRuntime,
    current_table_plan: HashMap<FileScanTaskDescriptor, FileScanTask>,
    current_snapshot_id: Option<IcebergSnapshotId>,
    diff_queue: VecDeque<ReadResult>,
    is_initialized: bool,
}

const ICEBERG_SLEEP_BETWEEN_SNAPSHOT_CHECKS: Duration = Duration::from_millis(100);

impl IcebergReader {
    pub fn new(
        db_params: &IcebergDBParams,
        table_params: &IcebergTableParams,
        column_types: HashMap<String, Type>,
        streaming_mode: ConnectorMode,
    ) -> Result<Self, ReadError> {
        let runtime = create_async_tokio_runtime()?;
        let catalog = db_params.create_catalog();
        let namespace = db_params.ensure_namespace(&runtime, &catalog)?;
        let table_ident = TableIdent::new(namespace.name().clone(), table_params.name.clone());

        // Check that the table exists.
        runtime.block_on(async { catalog.load_table(&table_ident).await })?;

        Ok(Self {
            catalog,
            table_ident,
            column_types,
            streaming_mode,

            runtime,
            current_table_plan: HashMap::new(),
            current_snapshot_id: None,
            diff_queue: VecDeque::new(),
            is_initialized: false,
        })
    }

    fn wait_for_snapshot_update(&mut self) -> Result<(), ReadError> {
        self.runtime.block_on(async {
            while self.diff_queue.is_empty() {
                let table = self.catalog.load_table(&self.table_ident).await?;
                let available_snapshot_id = table.metadata().current_snapshot_id();
                let snapshot_id_changed = available_snapshot_id != self.current_snapshot_id;
                if available_snapshot_id.is_none() || !snapshot_id_changed {
                    sleep(ICEBERG_SLEEP_BETWEEN_SNAPSHOT_CHECKS);
                    continue;
                }

                // The snapshot has been updated at this point.
                let updated_table_plan: Vec<FileScanTask> = table
                    .scan()
                    .build()?
                    // TODO: there can be many files, yet the diff may consist only of a few of them.
                    // But the versions of an iceberg table form a tree.
                    // So the following solution should be possible:
                    // - Find the least common ancestor of the current and the updated snapshot.
                    // - Traverse the path from the old version to the LCA and undo the changes on this path.
                    // - Traverse the path from the LCA to the new version and apply changes on this path.
                    // More reading on the protocol must be done to understand how to implement this.
                    .plan_files()
                    .await?
                    .try_collect()
                    .await?;

                let updated_table_plan: HashMap<FileScanTaskDescriptor, FileScanTask> =
                    updated_table_plan
                        .into_iter()
                        .map(|task| (FileScanTaskDescriptor::for_task(&task), task))
                        .collect();

                // Find the difference between the current and the updated table plan.
                let insertion_tasks =
                    Self::table_plans_difference(&updated_table_plan, &self.current_table_plan);
                let diffs = Self::create_version_diffs(
                    &table,
                    &self.column_types,
                    insertion_tasks,
                    DataEventType::Insert,
                    available_snapshot_id.unwrap(),
                )
                .await?;
                self.diff_queue.extend(diffs);

                let deletion_tasks =
                    Self::table_plans_difference(&self.current_table_plan, &updated_table_plan);
                let diffs = Self::create_version_diffs(
                    &table,
                    &self.column_types,
                    deletion_tasks,
                    DataEventType::Delete,
                    available_snapshot_id.unwrap(),
                )
                .await?;
                self.diff_queue.extend(diffs);

                if !self.diff_queue.is_empty() {
                    let new_source_metadata = IcebergMetadata::new(available_snapshot_id.unwrap());
                    self.diff_queue
                        .push_front(ReadResult::NewSource(new_source_metadata.into()));
                    self.diff_queue.push_back(ReadResult::FinishedSource {
                        commit_allowed: true,
                    });
                }

                self.current_snapshot_id = available_snapshot_id;
                self.current_table_plan = updated_table_plan;
            }

            Ok(())
        })
    }

    /// Return a vector of tasks that are in the plan `model` but not in the plan `other`.
    fn table_plans_difference(
        model: &HashMap<FileScanTaskDescriptor, FileScanTask>,
        other: &HashMap<FileScanTaskDescriptor, FileScanTask>,
    ) -> Vec<FileScanTask> {
        let model_keys: HashSet<_> = model.keys().collect();
        let other_keys: HashSet<_> = other.keys().collect();
        let keys_difference: Vec<_> = model_keys.difference(&other_keys).collect();
        keys_difference
            .into_iter()
            .map(|key| model[key].clone())
            .collect()
    }

    async fn create_version_diffs(
        table: &IcebergTable,
        column_types: &HashMap<String, Type>,
        difference_tasks: Vec<FileScanTask>,
        event_type: DataEventType,
        snapshot_id: IcebergSnapshotId,
    ) -> Result<Vec<ReadResult>, IcebergError> {
        let iceberg_task_stream: FileScanTaskStream =
            stream::iter(difference_tasks.into_iter().map(Ok)).boxed();

        let reader_builder = table.reader_builder();
        let entries: Vec<_> = reader_builder
            .build()
            .read(iceberg_task_stream)?
            .try_collect()
            .await?;
        let mut result = Vec::new();
        for entry in entries {
            let converted_values = columns_into_pathway_values(&entry, column_types);
            for values_map in converted_values {
                let deferred_read_result = ReadResult::Data(
                    ReaderContext::from_diff(event_type, None, values_map),
                    (
                        OffsetKey::Empty,
                        OffsetValue::IcebergSnapshot { snapshot_id },
                    ),
                );
                result.push(deferred_read_result);
            }
        }

        Ok(result)
    }
}

impl Reader for IcebergReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(result) = self.diff_queue.pop_front() {
                return Ok(result);
            }
            if self.streaming_mode.is_polling_enabled() || !self.is_initialized {
                self.is_initialized = true;
                self.wait_for_snapshot_update()?;
            } else {
                return Ok(ReadResult::Finished);
            }
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::IcebergSnapshot { snapshot_id }) = offset_value else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in Iceberg frontier: {offset_value:?}");
            }
            return Ok(());
        };

        self.runtime.block_on(async {
            let table = self.catalog.load_table(&self.table_ident).await?;
            let current_table_plan: Vec<FileScanTask> = table
                .scan()
                .snapshot_id(*snapshot_id)
                .build()?
                .plan_files()
                .await?
                .try_collect()
                .await?;

            #[allow(clippy::mutable_key_type)]
            let current_table_plan: HashMap<FileScanTaskDescriptor, FileScanTask> =
                current_table_plan
                    .into_iter()
                    .map(|task| (FileScanTaskDescriptor::for_task(&task), task))
                    .collect();
            self.current_table_plan = current_table_plan;

            Ok::<(), IcebergError>(())
        })?;

        self.current_snapshot_id = Some(*snapshot_id);
        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!(
            "Iceberg({}, {})",
            self.table_ident.namespace.to_url_string(),
            self.table_ident.name
        )
        .into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Iceberg
    }
}

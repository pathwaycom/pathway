use log::{error, info, warn};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::fmt::Write as WriteTrait;
use std::fs::File;
use std::hash::RandomState;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;
use deltalake::arrow::datatypes::TimeUnit as ArrowTimeUnit;
use deltalake::datafusion::execution::context::SessionContext as DeltaSessionContext;
use deltalake::datafusion::logical_expr::col;
use deltalake::datafusion::parquet::file::reader::SerializedFileReader as DeltaLakeParquetReader;
use deltalake::datafusion::prelude::Expr;
use deltalake::datafusion::scalar::ScalarValue;
use deltalake::kernel::Action as DeltaLakeAction;
use deltalake::kernel::ArrayType as DeltaTableArrayType;
use deltalake::kernel::CommitInfo as DeltaTableCommitInfo;
use deltalake::kernel::DataType as DeltaTableKernelType;
use deltalake::kernel::PrimitiveType as DeltaTablePrimitiveType;
use deltalake::kernel::StructField as DeltaTableStructField;
use deltalake::kernel::StructType as DeltaTableStructType;
use deltalake::operations::create::CreateBuilder as DeltaTableCreateBuilder;
use deltalake::operations::optimize::OptimizeBuilder;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::operations::vacuum::VacuumMetrics;
use deltalake::parquet::record::reader::RowIter as ParquetRowIterator;
use deltalake::parquet::record::Row as ParquetRow;
use deltalake::protocol::SaveMode as DeltaTableSaveMode;
use deltalake::table::PeekCommit as DeltaLakePeekCommit;
use deltalake::writer::{DeltaWriter, RecordBatchWriter as DTRecordBatchWriter};
use deltalake::{open_table_with_storage_options as open_delta_table, DeltaTable, TableProperty};
use deltalake::{DeltaOps, DeltaTableError, PartitionFilter, PartitionValue};
use indexmap::IndexMap;
use itertools::Itertools;
use s3::bucket::Bucket as S3Bucket;
use tempfile::tempfile;
use tokio::runtime::Runtime as TokioRuntime;

use super::{
    columns_into_pathway_values, parquet_row_into_values_map, LakeBatchWriter, LakeWriterSettings,
    MaintenanceMode, MetadataPerColumn, PATHWAY_COLUMN_META_FIELD, SPECIAL_OUTPUT_FIELDS,
};
use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::{
    parse_bool_advanced, NDARRAY_ELEMENTS_FIELD_NAME, NDARRAY_SHAPE_FIELD_NAME,
};
use crate::connectors::data_lake::buffering::PayloadType;
use crate::connectors::data_lake::ArrowDataType;
use crate::connectors::data_storage::{
    CommitPossibility, ConnectorMode, ConversionError, ValuesMap,
};
use crate::connectors::metadata::ParquetMetadata;
use crate::connectors::scanner::S3Scanner;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError, SPECIAL_FIELD_TIME,
};
use crate::engine::time::{DateTime, DateTimeNaive};
use crate::engine::{Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::{BackfillingThreshold, ValueField};
use crate::timestamp::current_unix_timestamp_ms;

#[derive(Debug)]
pub struct FieldMismatchDetails {
    schema_field: DeltaTableStructField,
    user_field: DeltaTableStructField,
}

impl fmt::Display for FieldMismatchDetails {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut error_parts = Vec::new();
        if self.schema_field.data_type != self.user_field.data_type {
            let error_part = format!(
                "data type differs (existing table={}, schema={})",
                self.schema_field.data_type, self.user_field.data_type
            );
            error_parts.push(error_part);
        }
        if self.schema_field.nullable != self.user_field.nullable {
            let error_part = format!(
                "nullability differs (existing table={}, schema={})",
                self.schema_field.nullable, self.user_field.nullable
            );
            error_parts.push(error_part);
        }
        write!(
            f,
            "field \"{}\": {}",
            self.schema_field.name,
            error_parts.join(", ")
        )
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct DeltaOptimizerRule {
    field_name: String,
    time_format: String,
    quick_access_window: std::time::Duration,
    compression_frequency: std::time::Duration,
    retention_period: chrono::TimeDelta,

    last_cutoff_value: Option<String>,
    last_compression_instant: Option<Instant>,
}

impl DeltaOptimizerRule {
    pub fn new(
        field_name: String,
        time_format: String,
        quick_access_window: std::time::Duration,
        compression_frequency: std::time::Duration,
        retention_period: chrono::TimeDelta,
    ) -> Self {
        Self {
            field_name,
            time_format,
            quick_access_window,
            compression_frequency,
            retention_period,

            last_cutoff_value: None,
            last_compression_instant: None,
        }
    }

    pub fn cutoff_value_to_apply(&self) -> Option<String> {
        // Note: this place has to be modified if there is a need to work
        // with time column different from the current time.
        let cutoff_time = chrono::Utc::now() - self.quick_access_window;
        let cutoff_value = cutoff_time.format(&self.time_format).to_string();

        if Some(&cutoff_value) == self.last_cutoff_value.as_ref() {
            return None;
        }
        let last_compression_is_too_recent = self
            .last_compression_instant
            .is_some_and(|t| t.elapsed() < self.compression_frequency);
        if last_compression_is_too_recent {
            return None;
        }

        Some(cutoff_value)
    }

    pub fn optimizer_filters_for_cutoff_value(&self, cutoff_value: &str) -> Vec<PartitionFilter> {
        let partition_filter = PartitionFilter {
            key: self.field_name.clone(),
            value: PartitionValue::LessThanOrEqual(cutoff_value.to_string()),
        };
        vec![partition_filter]
    }

    pub fn on_cutoff_value_optimized(&mut self, cutoff_value: String) {
        self.last_cutoff_value = Some(cutoff_value);
        self.last_compression_instant = Some(Instant::now());
    }
}

#[derive(Debug)]
pub struct SchemaMismatchDetails {
    outside_existing_schema: Vec<String>,
    missing_in_user_schema: Vec<String>,
    mismatching_types: Vec<FieldMismatchDetails>,
}

impl fmt::Display for SchemaMismatchDetails {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut error_parts = Vec::new();
        if !self.outside_existing_schema.is_empty() {
            let error_part = format!(
                "Fields in the provided schema that aren't present in the existing table: {:?}",
                self.outside_existing_schema
            );
            error_parts.push(error_part);
        }
        if !self.missing_in_user_schema.is_empty() {
            let error_part = format!(
                "Fields in the existing table that aren't present in the provided schema: {:?}",
                self.missing_in_user_schema
            );
            error_parts.push(error_part);
        }
        if !self.mismatching_types.is_empty() {
            let formatted_mismatched_types = self
                .mismatching_types
                .iter()
                .map(|item| format!("{item}"))
                .join(", ");
            let error_part =
                format!("Fields with mismatching types: [{formatted_mismatched_types}]");
            error_parts.push(error_part);
        }
        write!(f, "{}", error_parts.join("; "))
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct DeltaBatchWriter {
    table: DeltaTable,
    writer: DTRecordBatchWriter,
    metadata_per_column: MetadataPerColumn,
    optimizer_rule: Option<DeltaOptimizerRule>,
}

impl DeltaBatchWriter {
    pub fn new(
        path: &str,
        value_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
        partition_columns: Vec<String>,
        table_type: MaintenanceMode,
        optimizer_rule: Option<DeltaOptimizerRule>,
    ) -> Result<Self, WriteError> {
        let (table, metadata_per_column) = Self::open_table(
            path,
            value_fields,
            storage_options,
            partition_columns,
            table_type,
        )?;
        let writer = DTRecordBatchWriter::for_table(&table)?;
        Ok(Self {
            table,
            writer,
            metadata_per_column,
            optimizer_rule,
        })
    }

    pub fn open_table(
        path: &str,
        schema_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
        partition_columns: Vec<String>,
        table_type: MaintenanceMode,
    ) -> Result<(DeltaTable, MetadataPerColumn), WriteError> {
        let mut struct_fields = Vec::new();
        for field in schema_fields {
            let mut metadata = Vec::new();
            if let Some(field_metadata) = &field.metadata {
                metadata.push((PATHWAY_COLUMN_META_FIELD, field_metadata.to_string()));
            }
            struct_fields.push(
                DeltaTableStructField::new(
                    field.name.clone(),
                    Self::delta_table_type(&field.type_)?,
                    field.type_.can_be_none(),
                )
                .with_metadata(metadata),
            );
        }
        for (field, type_) in table_type.additional_output_fields() {
            struct_fields.push(DeltaTableStructField::new(
                field,
                Self::delta_table_type(&type_)?,
                false,
            ));
        }

        let runtime = create_async_tokio_runtime()?;
        let table: DeltaTable = runtime
            .block_on(async {
                let mut builder = DeltaTableCreateBuilder::new()
                    .with_location(path)
                    .with_save_mode(DeltaTableSaveMode::Append)
                    .with_columns(struct_fields.clone())
                    .with_storage_options(storage_options.clone())
                    .with_partition_columns(partition_columns);

                if table_type.is_append_only() {
                    builder = builder.with_configuration_property(TableProperty::AppendOnly, Some("true"));
                }

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

        let existing_schema = &table.schema().unwrap().fields;
        let metadata_per_column: HashMap<_, _> = existing_schema
            .into_iter()
            .map(|(name, column)| {
                let arrow_metadata = column
                    .metadata()
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect();
                (name.to_string(), arrow_metadata)
            })
            .collect();

        Self::ensure_schema_compliance(existing_schema, &struct_fields)?;
        Ok((table, metadata_per_column))
    }

    fn ensure_schema_compliance(
        existing_schema: &IndexMap<String, DeltaTableStructField>,
        user_schema: &[DeltaTableStructField],
    ) -> Result<(), WriteError> {
        let mut outside_existing_schema: Vec<String> = Vec::new();
        let mut missing_in_user_schema: Vec<String> = Vec::new();
        let mut mismatching_types = Vec::new();
        let mut has_error = false;

        let mut defined_user_columns = HashSet::new();
        for user_column in user_schema {
            let name = &user_column.name;
            defined_user_columns.insert(name.to_string());
            let Some(schema_column) = existing_schema.get(name) else {
                outside_existing_schema.push(name.to_string());
                has_error = true;
                continue;
            };
            let nullability_differs = user_column.nullable != schema_column.nullable;
            let data_type_differs = user_column.data_type != schema_column.data_type;
            if nullability_differs || data_type_differs {
                mismatching_types.push(FieldMismatchDetails {
                    schema_field: schema_column.clone(),
                    user_field: user_column.clone(),
                });
                has_error = true;
            }
        }
        for schema_column in existing_schema.keys() {
            if !defined_user_columns.contains(schema_column) {
                missing_in_user_schema.push(schema_column.to_string());
                has_error = true;
            }
        }

        if has_error {
            let schema_mismatch_details = SchemaMismatchDetails {
                outside_existing_schema,
                missing_in_user_schema,
                mismatching_types,
            };
            Err(WriteError::DeltaTableSchemaMismatch(
                schema_mismatch_details,
            ))
        } else {
            Ok(())
        }
    }

    fn delta_table_type(type_: &Type) -> Result<DeltaTableKernelType, WriteError> {
        let delta_type = match type_ {
            Type::Bool => DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Boolean),
            Type::Float => DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Double),
            Type::String | Type::Json | Type::Pointer => {
                DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::String)
            }
            Type::PyObjectWrapper | Type::Bytes => {
                DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Binary)
            }
            Type::DateTimeNaive => {
                DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::TimestampNtz)
            }
            Type::DateTimeUtc => {
                DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Timestamp)
            }
            Type::Int | Type::Duration => {
                DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Long)
            }
            Type::List(element_type) => {
                let element_type_is_optional = element_type.is_optional();
                let nested_element_type = Self::delta_table_type(element_type.unoptionalize())?;
                let array_type =
                    DeltaTableArrayType::new(nested_element_type, element_type_is_optional);
                DeltaTableKernelType::Array(array_type.into())
            }
            Type::Array(_, nested_type) => {
                let wrapped_type = nested_type.as_ref();
                let elements_kernel_type = match wrapped_type {
                    Type::Int => DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Long),
                    Type::Float => DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Double),
                    _ => panic!("Type::Array can't contain elements of the type {wrapped_type:?}"),
                };
                let shape_data_type = DeltaTableKernelType::Array(
                    DeltaTableArrayType::new(
                        DeltaTableKernelType::Primitive(DeltaTablePrimitiveType::Long),
                        true,
                    )
                    .into(),
                );
                let elements_data_type = DeltaTableKernelType::Array(
                    DeltaTableArrayType::new(elements_kernel_type, true).into(),
                );
                let struct_descriptor = DeltaTableStructType::new(vec![
                    DeltaTableStructField::new(NDARRAY_SHAPE_FIELD_NAME, shape_data_type, false),
                    DeltaTableStructField::new(
                        NDARRAY_ELEMENTS_FIELD_NAME,
                        elements_data_type,
                        false,
                    ),
                ]);
                DeltaTableKernelType::Struct(struct_descriptor.into())
            }
            Type::Tuple(nested_types) => {
                let mut struct_fields = Vec::new();
                for (index, nested_type) in nested_types.iter().enumerate() {
                    let nested_type_is_optional = nested_type.is_optional();
                    let nested_delta_type = Self::delta_table_type(nested_type)?;
                    struct_fields.push(DeltaTableStructField::new(
                        format!("[{index}]"),
                        nested_delta_type,
                        nested_type_is_optional,
                    ));
                }
                let struct_descriptor = DeltaTableStructType::new(struct_fields);
                DeltaTableKernelType::Struct(struct_descriptor.into())
            }
            Type::Optional(wrapped) => return Self::delta_table_type(wrapped),
            Type::Any | Type::Future(_) => return Err(WriteError::UnsupportedType(type_.clone())),
        };
        Ok(delta_type)
    }

    fn format_vacuum_metrics(metrics: VacuumMetrics) -> String {
        let mut result = String::new();
        let mut partition_paths = Vec::with_capacity(metrics.files_deleted.len());
        for deleted_file in metrics.files_deleted {
            let file_path = Path::new(&deleted_file);
            if let Some(partition_path) = file_path.parent() {
                partition_paths.push(partition_path.display().to_string());
            }
        }
        partition_paths.sort();
        partition_paths.dedup();
        write!(
            &mut result,
            "Dry run: {} Optimized partitions: {:?}",
            metrics.dry_run, partition_paths
        )
        .unwrap();
        result
    }

    async fn maybe_optimize_table(&mut self) -> Result<(), WriteError> {
        // Saving the name for logs before the mutable borrow
        let connector_name = self.name();
        if let Some(optimizer_rule) = self.optimizer_rule.as_mut() {
            let cutoff_to_apply = optimizer_rule.cutoff_value_to_apply();
            if let Some(cutoff_to_apply) = cutoff_to_apply {
                let filters_to_apply =
                    optimizer_rule.optimizer_filters_for_cutoff_value(&cutoff_to_apply);
                let (optimized_table, metrics) =
                    OptimizeBuilder::new(self.table.log_store(), self.table.snapshot()?.clone())
                        .with_filters(&filters_to_apply)
                        .await?;
                info!("Table {connector_name}: has been optimized. Metrics: {metrics:?}");

                let (_vacuumed_table, metrics) = VacuumBuilder::new(
                    optimized_table.log_store(),
                    optimized_table.snapshot()?.clone(),
                )
                .with_retention_period(optimizer_rule.retention_period)
                .with_enforce_retention_duration(false)
                .with_dry_run(false)
                .await?;

                info!(
                    "Table {connector_name}: outdated Parquet blocks have been removed. {}",
                    Self::format_vacuum_metrics(metrics)
                );
                optimizer_rule.on_cutoff_value_optimized(cutoff_to_apply);
                self.table.update().await?;
            }
        }
        Ok(())
    }
}

impl LakeBatchWriter for DeltaBatchWriter {
    fn write_batch(
        &mut self,
        batch: ArrowRecordBatch,
        payload_type: PayloadType,
    ) -> Result<(), WriteError> {
        create_async_tokio_runtime()?.block_on(async {
            self.table.update().await?;
            match payload_type {
                PayloadType::FullSnapshot => {
                    DeltaOps(self.table.clone())
                        .write(vec![batch])
                        .with_save_mode(DeltaTableSaveMode::Overwrite)
                        .await?;
                }
                PayloadType::Diff => {
                    self.writer = DTRecordBatchWriter::for_table(&self.table)?;
                    self.writer.write(batch).await?;
                    self.writer.flush_and_commit(&mut self.table).await?;
                }
            }
            self.table.update().await?;

            if let Err(e) = self.maybe_optimize_table().await {
                warn!("Failed to optimize table {}: {e}", self.name());
            }
            Ok::<(), WriteError>(())
        })
    }

    fn settings(&self) -> LakeWriterSettings {
        LakeWriterSettings {
            use_64bit_size_type: false,
            utc_timezone_name: "UTC".into(),
            timestamp_unit: ArrowTimeUnit::Microsecond,
        }
    }

    fn metadata_per_column(&self) -> &MetadataPerColumn {
        &self.metadata_per_column
    }

    fn name(&self) -> String {
        format!("DeltaTable({})", self.table.table_uri())
    }
}

pub enum ObjectDownloader {
    Local,
    S3(Box<S3Bucket>),
}

impl ObjectDownloader {
    fn download_object(&self, path: &str) -> Result<File, ReadError> {
        let obj = match self {
            Self::Local => File::open(path)?,
            Self::S3(bucket) => {
                let contents = S3Scanner::download_object_from_path_and_bucket(path, bucket)?;
                let mut tempfile = tempfile()?;
                tempfile.write_all(contents.bytes())?;
                tempfile.flush()?;
                tempfile.seek(SeekFrom::Start(0))?;
                tempfile
            }
        };
        Ok(obj)
    }
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct DeltaReaderAction {
    action_type: DataEventType,
    path: String,
    is_last_in_version: bool,
    partition_values: ValuesMap,
}

impl DeltaReaderAction {
    pub fn new(action_type: DataEventType, path: String, partition_values: ValuesMap) -> Self {
        Self {
            action_type,
            path,
            partition_values,
            is_last_in_version: false,
        }
    }

    pub fn set_last_in_version(&mut self) {
        self.is_last_in_version = true;
    }
}

#[derive(Debug)]
enum ParquetReaderOutcome {
    SourceEvent(ReadResult),
    Row(ParquetRow),
}

#[derive(Debug)]
enum BackfillingEntry {
    SourceEvent(ReadResult),
    Entry(ValuesMap),
}

pub fn open_and_read_delta_table<S: ::std::hash::BuildHasher>(
    uri: &str,
    storage_options: HashMap<String, String, RandomState>,
    column_types: &HashMap<String, Type, S>,
    column_order: &[String],
) -> Result<Vec<Vec<Value>>, DeltaTableError> {
    let runtime = create_async_tokio_runtime()?;
    let table = runtime.block_on(async { open_delta_table(uri, storage_options).await })?;
    read_delta_table(&runtime, table, column_types, column_order)
}

const MAX_ENTRY_PARSING_ERRORS: usize = 10;

pub fn read_delta_table<S: std::hash::BuildHasher>(
    runtime: &TokioRuntime,
    table: DeltaTable,
    column_types: &HashMap<String, Type, S>,
    column_order: &[String],
) -> Result<Vec<Vec<Value>>, DeltaTableError> {
    let ctx = DeltaSessionContext::new();
    let df = ctx.read_table(Arc::new(table))?;

    let map_entries = runtime.block_on(async {
        let results = df.collect().await?;
        let mut entries = Vec::new();
        let mut n_errors = 0;

        for record_batch in results {
            for value_map in columns_into_pathway_values(&record_batch, column_types) {
                match value_map.to_pure_hashmap() {
                    Ok(map) => entries.push(map),
                    Err(e) if n_errors < MAX_ENTRY_PARSING_ERRORS => {
                        warn!("Entry doesn't match expected schema: {e}");
                        n_errors += 1;
                    }
                    Err(_) => {
                        n_errors += 1;
                    }
                }
            }
        }

        if n_errors > MAX_ENTRY_PARSING_ERRORS {
            warn!(
                "Some entries don't match the expected schema: {} error messages omitted",
                n_errors - MAX_ENTRY_PARSING_ERRORS
            );
        }

        Ok::<_, DeltaTableError>(entries)
    })?;

    let result = map_entries
        .into_iter()
        .map(|entry| {
            column_order
                .iter()
                .map(|col| entry.get(col).cloned().unwrap_or(Value::None))
                .collect()
        })
        .collect();

    Ok(result)
}

#[allow(clippy::module_name_repetitions)]
pub struct DeltaTableReader {
    table: DeltaTable,
    streaming_mode: ConnectorMode,
    column_types: HashMap<String, Type>,
    base_path: String,
    object_downloader: ObjectDownloader,

    reader: Option<ParquetRowIterator<'static>>,
    current_version: i64,
    rows_read_within_version: i64,
    parquet_files_queue: VecDeque<DeltaReaderAction>,
    current_action: Option<DeltaReaderAction>,
    backfilling_entries_queue: VecDeque<BackfillingEntry>,
}

const APPEND_ONLY_PROPERTY_NAME: &str = "delta.appendOnly";
const DELTA_LAKE_INITIAL_POLL_DURATION: Duration = Duration::from_millis(5);
const DELTA_LAKE_MAX_POLL_DURATION: Duration = Duration::from_millis(100);
const DELTA_LAKE_POLL_BACKOFF: u32 = 2;

impl DeltaTableReader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: &str,
        object_downloader: ObjectDownloader,
        storage_options: HashMap<String, String>,
        mut column_types: HashMap<String, Type>,
        streaming_mode: ConnectorMode,
        start_from_timestamp_ms: Option<i64>,
        has_primary_key: bool,
        backfilling_thresholds: Vec<BackfillingThreshold>,
    ) -> Result<Self, ReadError> {
        let runtime = create_async_tokio_runtime()?;
        let mut table =
            runtime.block_on(async { open_delta_table(path, storage_options).await })?;
        let table_props = &table.metadata()?.configuration;
        let append_only_property = table_props.get(APPEND_ONLY_PROPERTY_NAME);
        let is_append_only = {
            if let Some(Some(append_only_property)) = append_only_property {
                parse_bool_advanced(append_only_property).unwrap_or(false)
            } else {
                false
            }
        };
        if !has_primary_key && !is_append_only {
            return Err(ReadError::PrimaryKeyRequired);
        }
        let mut current_version = table.version();

        let mut parquet_files_queue = VecDeque::new();
        let mut backfilling_entries_queue = VecDeque::new();
        let mut snapshot_loading_needed = backfilling_thresholds.is_empty();

        if let Some(start_from_timestamp_ms) = start_from_timestamp_ms {
            assert!(backfilling_thresholds.is_empty()); // Checked upstream in python_api.rs
            Self::handle_start_from_timestamp_ms(
                &runtime,
                &mut table,
                start_from_timestamp_ms,
                is_append_only,
                &mut current_version,
                &mut snapshot_loading_needed,
            )?;
        } else {
            snapshot_loading_needed = true;
        }
        if snapshot_loading_needed {
            parquet_files_queue =
                Self::get_reader_actions_for_table(&runtime, &table, path, &column_types)?;
        }

        if !backfilling_thresholds.is_empty() {
            parquet_files_queue.clear();
            backfilling_entries_queue = Self::create_backfilling_files_queue(
                &runtime,
                table.clone(),
                backfilling_thresholds,
                &mut column_types,
            )?;
        }

        Ok(Self {
            table,
            column_types,
            streaming_mode,
            base_path: path.to_string(),

            current_version,
            object_downloader,
            reader: None,
            backfilling_entries_queue,
            parquet_files_queue,
            rows_read_within_version: 0,
            current_action: None,
        })
    }

    fn handle_start_from_timestamp_ms(
        runtime: &TokioRuntime,
        table: &mut DeltaTable,
        start_from_timestamp_ms: i64,
        is_append_only: bool,
        current_version: &mut i64,
        snapshot_loading_needed: &mut bool,
    ) -> Result<(), ReadError> {
        let current_timestamp = current_unix_timestamp_ms();
        if start_from_timestamp_ms > current_timestamp.try_into().unwrap() {
            warn!("The timestamp {start_from_timestamp_ms} is greater than the current timestamp {current_timestamp}. All new entries will be read.");
        }
        let (earliest_version, latest_version) = runtime.block_on(async {
            Ok::<(i64, i64), ReadError>((
                table.get_earliest_version().await?,
                table.get_latest_version().await?,
            ))
        })?;
        let snapshot = table.snapshot()?;

        let mut last_version_below_threshold = None;
        let mut version_at_threshold = None;
        for version in earliest_version..=latest_version {
            let Some(timestamp) = snapshot.version_timestamp(version) else {
                continue;
            };
            if timestamp < start_from_timestamp_ms {
                last_version_below_threshold = Some(version);
            } else {
                if timestamp == start_from_timestamp_ms {
                    version_at_threshold = Some(version);
                }
                break;
            }
        }

        if !is_append_only && version_at_threshold.is_some() {
            *current_version = version_at_threshold.unwrap();
        } else if let Some(last_version_below_threshold) = last_version_below_threshold {
            *current_version = last_version_below_threshold;
        } else {
            *current_version = earliest_version;
            warn!(
                    "All available versions are newer than the specified timestamp {start_from_timestamp_ms}. The read will start from the beginning, version {current_version}."
                );
            // NB: All versions are newer than the requested one, meaning that we need to read the
            // full state at the `earliest_version` and then continue incrementally.
        }

        if is_append_only && last_version_below_threshold.is_some() {
            // We've found the threshold version, we read only diffs from this version onwards.
            *snapshot_loading_needed = false;
        }

        runtime.block_on(async { table.load_version(*current_version).await })?;
        Ok(())
    }

    fn record_batch_has_pathway_fields(batch: &ArrowRecordBatch) -> bool {
        for (field, _) in SPECIAL_OUTPUT_FIELDS {
            if let Some(time_column) = batch.column_by_name(field) {
                if *time_column.data_type() != ArrowDataType::Int64 {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    #[allow(clippy::too_many_lines)]
    fn create_backfilling_files_queue(
        runtime: &TokioRuntime,
        table: DeltaTable,
        backfilling_thresholds: Vec<BackfillingThreshold>,
        column_types: &mut HashMap<String, Type>,
    ) -> Result<VecDeque<BackfillingEntry>, ReadError> {
        let mut binary_partition_columns = Vec::new();
        for partition_column in table.metadata()?.partition_columns.clone() {
            let Some(type_) = column_types.get(&partition_column) else {
                continue;
            };
            if *type_.unoptionalize() == Type::Bytes {
                binary_partition_columns.push(partition_column);
            }
        }

        let backfilling_started_at = Instant::now();
        let ctx = DeltaSessionContext::new();
        ctx.register_table("table", Arc::new(table))?;
        let mut df = runtime.block_on(async { ctx.table("table").await })?;
        for threshold in backfilling_thresholds {
            let literal = Expr::Literal(Self::scalar_value_for_queries(&threshold.threshold));
            let column = col(threshold.field);
            df = match threshold.comparison_op.as_str() {
                ">=" => df.filter(column.gt_eq(literal))?,
                "<=" => df.filter(column.lt_eq(literal))?,
                "<" => df.filter(column.lt(literal))?,
                ">" => df.filter(column.gt(literal))?,
                "==" => df.filter(column.eq(literal))?,
                "!=" => df.filter(column.not_eq(literal))?,
                _ => panic!(
                    "Unsupported comparison operation: {}",
                    threshold.comparison_op
                ),
            };
        }

        let has_pathway_meta_column = column_types.get(SPECIAL_FIELD_TIME).is_some();
        let mut pathway_meta_column_added = false;

        let mut backfilling_entries = Vec::new();
        runtime.block_on(async {
            let results = df.collect().await?;
            let mut is_first_entry = true;
            for entry in results {
                if is_first_entry
                    && !has_pathway_meta_column
                    && Self::record_batch_has_pathway_fields(&entry)
                {
                    // We're dealing with the output of Pathway process. It means that it also needs to be
                    // processed atomically with respect to times.
                    // To accomplish that, we temporarily add Pathway's meta field "time", use it in the
                    // batch splits, and disregard later on.
                    column_types.insert(SPECIAL_FIELD_TIME.to_string(), Type::Int);
                    pathway_meta_column_added = true;
                }
                let value_maps = columns_into_pathway_values(&entry, column_types);
                for mut value_map in value_maps {
                    // Perhaps a bug in DataFusion: when a query returns a binary partition column,
                    // it returns the value of this column as a sequence of escaped characters.
                    for column in &binary_partition_columns {
                        let value = value_map.get_mut(column).unwrap();
                        if let Ok(Value::Bytes(bytes)) = value {
                            *value = String::from_utf8(bytes.to_vec())
                                .ok()
                                .and_then(|x| Self::decode_escaped_binary(&x))
                                .map(|x| Value::Bytes(x.into()))
                                .ok_or_else(|| {
                                    Box::new(ConversionError::new(
                                        format!("{value:?}"),
                                        column.to_string(),
                                        Type::Bytes,
                                        None,
                                    ))
                                });
                        }
                    }
                    backfilling_entries.push(value_map);
                }
                is_first_entry = false;
            }
            log::info!(
                "DeltaLake backfilling entries count: {} (elapsed time: {:?})",
                backfilling_entries.len(),
                backfilling_started_at.elapsed()
            );
            Ok::<(), ReadError>(())
        })?;
        warn!("Backfilling thresholds won't be applied for any data the follows after the initially read batch.");

        let is_pathway_output = has_pathway_meta_column || pathway_meta_column_added;
        if is_pathway_output && !backfilling_entries.is_empty() {
            let mut backfilling_entries_queue = VecDeque::new();
            let mut prev_time = None;
            backfilling_entries
                .sort_by_key(|entry| entry.get(SPECIAL_FIELD_TIME).unwrap().clone().ok());
            backfilling_entries_queue.push_back(BackfillingEntry::SourceEvent(
                ReadResult::NewSource(ParquetMetadata::new(None).into()),
            ));
            for mut entry in backfilling_entries {
                let current_time: Option<Value> =
                    entry.get(SPECIAL_FIELD_TIME).unwrap().clone().ok().clone();
                let is_new_block = prev_time.is_some() && current_time != prev_time;
                if is_new_block {
                    backfilling_entries_queue.push_back(BackfillingEntry::SourceEvent(
                        ReadResult::FinishedSource {
                            // Applicable only for append-only tables, hence no need to avoid squashing diff = +1 with diff = -1
                            commit_possibility: CommitPossibility::Possible,
                        },
                    ));
                    backfilling_entries_queue.push_back(BackfillingEntry::SourceEvent(
                        ReadResult::NewSource(ParquetMetadata::new(None).into()),
                    ));
                }
                if pathway_meta_column_added {
                    // Pathway meta columns weren't requested by the user: they were added by us
                    // artificially, to perform the data merging. Now they need to be removed from the
                    // entry to correspond to what was requested.
                    entry.remove(SPECIAL_FIELD_TIME);
                }
                backfilling_entries_queue.push_back(BackfillingEntry::Entry(entry));
                prev_time = current_time;
            }
            backfilling_entries_queue.push_back(BackfillingEntry::SourceEvent(
                ReadResult::FinishedSource {
                    // Same as above, we don't force commits, since the situation with losing/collapsing +1 and -1 events
                    // is not possible here
                    commit_possibility: CommitPossibility::Possible,
                },
            ));
            if pathway_meta_column_added {
                column_types.remove(SPECIAL_FIELD_TIME);
            }
            Ok(backfilling_entries_queue)
        } else {
            Ok(backfilling_entries
                .into_iter()
                .map(BackfillingEntry::Entry)
                .collect())
        }
    }

    fn scalar_value_for_queries(value: &Value) -> ScalarValue {
        match value {
            Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
            Value::Int(i) => ScalarValue::Int64(Some(*i)),
            Value::Float(f) => ScalarValue::Float64(Some((*f).into())),
            Value::String(s) => ScalarValue::Utf8(Some(s.to_string())),
            Value::Bytes(b) => ScalarValue::Binary(Some(b.to_vec())),
            Value::DateTimeNaive(dt) => {
                ScalarValue::TimestampMicrosecond(Some(dt.timestamp_microseconds()), None)
            }
            Value::DateTimeUtc(dt) => ScalarValue::TimestampMicrosecond(
                Some(dt.timestamp_microseconds()),
                Some("UTC".into()),
            ),
            Value::Duration(dt) => ScalarValue::DurationMicrosecond(Some(dt.microseconds())),
            _ => todo!("querying is not supported for {value:?}"),
        }
    }

    fn get_reader_actions_for_table(
        runtime: &TokioRuntime,
        table: &DeltaTable,
        base_path: &str,
        column_types: &HashMap<String, Type>,
    ) -> Result<VecDeque<DeltaReaderAction>, ReadError> {
        let history = runtime.block_on(async {
            Ok::<Vec<DeltaTableCommitInfo>, ReadError>(table.history(None).await?)
        })?;
        Self::get_reader_actions(table, base_path, history, column_types)
    }

    fn get_reader_actions(
        table: &DeltaTable,
        base_path: &str,
        mut history: Vec<DeltaTableCommitInfo>,
        column_types: &HashMap<String, Type>,
    ) -> Result<VecDeque<DeltaReaderAction>, ReadError> {
        // Historical events without timestamps are useless for grouping parquet files
        // into atomically processed versions, therefore there is a need to remove them
        let original_history_len = history.len();
        history.retain(|item| item.timestamp.is_some());
        if history.len() != original_history_len {
            warn!("Some of the historical entries don't have the timestamp, therefore some version updates may be merged into one. Original number of historical entries: {original_history_len}. Entries available after filtering: {}", history.len());
        }
        history.sort_by_key(|item| item.timestamp);

        let mut actions_with_timestamp: Vec<_> = table
            .snapshot()?
            .file_actions()?
            .into_iter()
            .map(|action| {
                let partition_values =
                    Self::parse_partition_values(&action.partition_values, column_types);
                (
                    DeltaReaderAction::new(
                        DataEventType::Insert,
                        Self::ensure_absolute_path_with_base(&action.path, base_path),
                        partition_values,
                    ),
                    action.modification_time,
                )
            })
            .collect();
        actions_with_timestamp.sort_by_key(|item| item.1);

        let mut actions: VecDeque<DeltaReaderAction> =
            VecDeque::with_capacity(actions_with_timestamp.len());
        let mut n_total_versions = 0;
        let mut current_commit_idx = 0;
        for (action, timestamp) in actions_with_timestamp {
            let mut is_new_block = false;
            while current_commit_idx < history.len()
                && timestamp
                    > history[current_commit_idx]
                        .timestamp
                        .expect("events without timestamp have been filtered before")
            {
                // Every historical entry corresponds to a commit. If this action corresponds
                // to a further historical entry, it means the start of the new atomically
                // processed version block.
                is_new_block = true;
                current_commit_idx += 1;
            }

            if !actions.is_empty() && is_new_block {
                actions
                    .back_mut()
                    .expect("actions are not empty")
                    .set_last_in_version();
                n_total_versions += 1;
            }
            actions.push_back(action);
        }

        // The last actions always terminates atomically processed batch
        if let Some(last_action) = actions.back_mut() {
            last_action.set_last_in_version();
            n_total_versions += 1;
        }

        info!("The first read of Delta table at {base_path} uses {n_total_versions} versions and {} parquet files", actions.len());
        Ok(actions)
    }

    fn ensure_absolute_path(&self, path: &str) -> String {
        Self::ensure_absolute_path_with_base(path, &self.base_path)
    }

    fn ensure_absolute_path_with_base(path: &str, base_path: &str) -> String {
        if path.starts_with(base_path) {
            return path.to_string();
        }
        if base_path.ends_with('/') {
            format!("{base_path}{path}")
        } else {
            format!("{base_path}/{path}")
        }
    }

    fn decode_escaped_binary(input: &str) -> Option<Vec<u8>> {
        // Decode binary partition column values created by Delta writer
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
        let mut bytes = Vec::new();
        let mut chars = input.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\\' && chars.peek() == Some(&'u') {
                chars.next(); // consume 'u'
                let hex: String = chars.by_ref().take(4).collect();
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    bytes.push(byte);
                } else {
                    error!("Invalid escape: \\u{hex}");
                    return None;
                }
            } else {
                error!("Unexpected character in binary escape: {c}");
                return None;
            }
        }

        Some(bytes)
    }

    fn parse_partition_values(
        partition_values: &HashMap<String, Option<String>>,
        column_types: &HashMap<String, Type>,
    ) -> ValuesMap {
        // Deserialize partition value according to the protocol
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
        let mut parsed_values: HashMap<String, Result<Value, Box<ConversionError>>> =
            HashMap::new();
        for (key, value) in partition_values {
            let Some(expected_type) = column_types.get(key) else {
                // There is a partition value, but it is not included in the user-requested fields.
                continue;
            };
            let Some(serialized_value) = value else {
                if expected_type.is_optional() {
                    parsed_values.insert(key.to_string(), Ok(Value::None));
                } else {
                    parsed_values.insert(
                        key.to_string(),
                        Err(Box::new(ConversionError::new(
                            "None".to_string(),
                            key.to_string(),
                            expected_type.clone(),
                            None,
                        ))),
                    );
                }
                continue;
            };
            let parsed_value = match expected_type.unoptionalize() {
                Type::String => Some(Value::String(serialized_value.to_string().into())),
                Type::Int => serialized_value.parse::<i64>().map(Value::Int).ok(),
                Type::Float => serialized_value
                    .parse::<f64>()
                    .map(|x| Value::Float(x.into()))
                    .ok(),
                Type::Bool => match serialized_value.to_lowercase().as_str() {
                    "false" => Some(Value::Bool(false)),
                    "true" => Some(Value::Bool(true)),
                    _ => None,
                },
                Type::DateTimeUtc => {
                    DateTimeNaive::strptime(serialized_value, "%Y-%m-%d %H:%M:%S%.f")
                        .map(|x| x.to_utc_from_timezone("+00:00").unwrap())
                        .map(Value::from)
                        .ok()
                }
                Type::DateTimeNaive => {
                    DateTimeNaive::strptime(serialized_value, "%Y-%m-%d %H:%M:%S%.f")
                        .map(Value::from)
                        .ok()
                }
                Type::Bytes => {
                    Self::decode_escaped_binary(serialized_value).map(|x| Value::Bytes(x.into()))
                }
                _ => None,
            };
            if let Some(parsed_value) = parsed_value {
                parsed_values.insert(key.to_string(), Ok(parsed_value));
            } else {
                parsed_values.insert(
                    key.to_string(),
                    Err(Box::new(ConversionError::new(
                        serialized_value.to_string(),
                        key.to_string(),
                        expected_type.clone(),
                        None,
                    ))),
                );
            }
        }
        parsed_values.into()
    }

    fn upgrade_table_version(&mut self, is_polling_enabled: bool) -> Result<(), ReadError> {
        let runtime = create_async_tokio_runtime()?;
        runtime.block_on(async {
            self.parquet_files_queue.clear();
            let mut sleep_duration = DELTA_LAKE_INITIAL_POLL_DURATION;
            while self.parquet_files_queue.is_empty() {
                let diff = self
                    .table
                    .log_store()
                    .peek_next_commit(self.current_version)
                    .await?;
                let DeltaLakePeekCommit::New(next_version, txn_actions) = diff else {
                    if !is_polling_enabled {
                        break;
                    }
                    // Fully up to date, no changes yet
                    sleep(sleep_duration);
                    sleep_duration *= DELTA_LAKE_POLL_BACKOFF;
                    if sleep_duration > DELTA_LAKE_MAX_POLL_DURATION {
                        sleep_duration = DELTA_LAKE_MAX_POLL_DURATION;
                    }
                    continue;
                };

                let mut added_blocks = VecDeque::new();
                let mut data_changed = false;
                for action in txn_actions {
                    // Protocol description for Delta Lake actions:
                    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
                    let action = match action {
                        DeltaLakeAction::Remove(action) => {
                            if action.deletion_vector.is_some() {
                                return Err(ReadError::DeltaDeletionVectorsNotSupported);
                            }
                            data_changed |= action.data_change;
                            let action_path = self.ensure_absolute_path(&action.path);
                            let partition_values = Self::parse_partition_values(
                                &action.partition_values.unwrap_or_default(),
                                &self.column_types,
                            );
                            DeltaReaderAction::new(
                                DataEventType::Delete,
                                action_path,
                                partition_values,
                            )
                        }
                        DeltaLakeAction::Add(action) => {
                            data_changed |= action.data_change;
                            let action_path = self.ensure_absolute_path(&action.path);
                            let partition_values = Self::parse_partition_values(
                                &action.partition_values,
                                &self.column_types,
                            );
                            DeltaReaderAction::new(
                                DataEventType::Insert,
                                action_path,
                                partition_values,
                            )
                        }
                        _ => continue,
                    };
                    added_blocks.push_back(action);
                }

                self.current_version = next_version;
                self.rows_read_within_version = 0;
                if data_changed {
                    added_blocks
                        .back_mut()
                        .expect("if there is a data change, there should be at least one block")
                        .set_last_in_version();
                    self.parquet_files_queue = added_blocks;
                }
            }
            Ok(())
        })
    }

    fn read_next_row_native(
        &mut self,
        is_polling_enabled: bool,
    ) -> Result<ParquetReaderOutcome, ReadError> {
        if let Some(ref mut reader) = &mut self.reader {
            match reader.next() {
                Some(Ok(row)) => return Ok(ParquetReaderOutcome::Row(row)),
                Some(Err(parquet_err)) => return Err(ReadError::Parquet(parquet_err)),
                None => {
                    // The Pathway time advancement (e.g. commit) is only possible if it was the
                    // last Parquet block within a version.
                    let is_last_in_version = self
                        .current_action
                        .as_ref()
                        .expect("current action must be set if there's a reader")
                        .is_last_in_version;

                    let source_event = ReadResult::FinishedSource {
                        commit_possibility: if is_last_in_version {
                            // The versions are read on-line, force to avoid squashing same-key events
                            // with the previous or the next versions.
                            // Note that it can be less strict if the batch only has additions.
                            CommitPossibility::Forced
                        } else {
                            CommitPossibility::Forbidden
                        },
                    };
                    self.reader = None;
                    self.current_action = None;

                    return Ok(ParquetReaderOutcome::SourceEvent(source_event));
                }
            };
        }
        if self.parquet_files_queue.is_empty() {
            self.upgrade_table_version(is_polling_enabled)?;
            if self.parquet_files_queue.is_empty() {
                return Err(ReadError::NoObjectsToRead);
            }
        }
        let next_action = self.parquet_files_queue.pop_front().unwrap();
        let local_object = self.object_downloader.download_object(&next_action.path)?;
        let new_block_metadata = ParquetMetadata::new(Some(next_action.path.clone()));

        self.current_action = Some(next_action);
        self.reader = Some(DeltaLakeParquetReader::try_from(local_object)?.into_iter());

        let source_event = ReadResult::NewSource(new_block_metadata.into());
        Ok(ParquetReaderOutcome::SourceEvent(source_event))
    }
}

impl Reader for DeltaTableReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        let mut row_map = if let Some(maybe_row_map) = self.backfilling_entries_queue.pop_front() {
            match maybe_row_map {
                BackfillingEntry::SourceEvent(event) => return Ok(event),
                BackfillingEntry::Entry(entry) => entry,
            }
        } else {
            let parquet_row =
                match self.read_next_row_native(self.streaming_mode.is_polling_enabled()) {
                    Ok(ParquetReaderOutcome::Row(row)) => row,
                    Ok(ParquetReaderOutcome::SourceEvent(event)) => return Ok(event),
                    Err(ReadError::NoObjectsToRead) => return Ok(ReadResult::Finished),
                    Err(other) => return Err(other),
                };
            parquet_row_into_values_map(&parquet_row, &self.column_types)
        };
        if let Some(current_action) = self.current_action.as_ref() {
            row_map.merge(&current_action.partition_values);
        }

        self.rows_read_within_version += 1;
        Ok(ReadResult::Data(
            ReaderContext::from_diff(
                self.current_action
                    .as_ref()
                    // If there's no current action, backfilling thresholds were used. They only imply insertions
                    .map_or(DataEventType::Insert, |action| action.action_type),
                None,
                row_map,
            ),
            (
                OffsetKey::Empty,
                OffsetValue::DeltaTablePosition {
                    version: self.current_version,
                    rows_read_within_version: self.rows_read_within_version,
                },
            ),
        ))
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        // The offset denotes the last fully processed Delta Table version.
        // Then, the `seek` loads this checkpoint and ensures that no diffs
        // from the current version will be applied.
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::DeltaTablePosition { version, .. }) = offset_value else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in DeltaLake frontier: {offset_value:?}");
            }
            return Ok(());
        };

        self.reader = None;
        let runtime = create_async_tokio_runtime()?;

        // The last saved offset corresponds to the last version that has been read in full
        self.current_version = *version;
        runtime.block_on(async { self.table.load_version(self.current_version).await })?;
        self.parquet_files_queue.clear();

        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("DeltaTable({})", self.base_path).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::DeltaLake
    }
}

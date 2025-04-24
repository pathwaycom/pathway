use log::{info, warn};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;
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
use deltalake::parquet::record::reader::RowIter as ParquetRowIterator;
use deltalake::parquet::record::Row as ParquetRow;
use deltalake::protocol::SaveMode as DeltaTableSaveMode;
use deltalake::table::PeekCommit as DeltaLakePeekCommit;
use deltalake::writer::{DeltaWriter, RecordBatchWriter as DTRecordBatchWriter};
use deltalake::{open_table_with_storage_options as open_delta_table, DeltaTable, TableProperty};
use indexmap::IndexMap;
use itertools::Itertools;
use s3::bucket::Bucket as S3Bucket;
use tempfile::tempfile;
use tokio::runtime::Runtime as TokioRuntime;

use super::{
    columns_into_pathway_values, parquet_row_into_values_map, LakeBatchWriter, LakeWriterSettings,
    MetadataPerColumn, PATHWAY_COLUMN_META_FIELD, SPECIAL_FIELD_TIME, SPECIAL_OUTPUT_FIELDS,
};
use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::parse_bool_advanced;
use crate::connectors::data_lake::ArrowDataType;
use crate::connectors::data_storage::{ConnectorMode, ValuesMap};
use crate::connectors::metadata::ParquetMetadata;
use crate::connectors::scanner::S3Scanner;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError,
};
use crate::engine::time::DateTime;
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

#[derive(Debug)]
pub struct SchemaMismatchDetails {
    columns_outside_existing_schema: Vec<String>,
    columns_missing_in_user_schema: Vec<String>,
    columns_mismatching_types: Vec<FieldMismatchDetails>,
}

impl fmt::Display for SchemaMismatchDetails {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut error_parts = Vec::new();
        if !self.columns_outside_existing_schema.is_empty() {
            let error_part = format!(
                "Fields in the provided schema that aren't present in the existing table: {:?}",
                self.columns_outside_existing_schema
            );
            error_parts.push(error_part);
        }
        if !self.columns_missing_in_user_schema.is_empty() {
            let error_part = format!(
                "Fields in the existing table that aren't present in the provided schema: {:?}",
                self.columns_missing_in_user_schema
            );
            error_parts.push(error_part);
        }
        if !self.columns_mismatching_types.is_empty() {
            let formatted_mismatched_types = self
                .columns_mismatching_types
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
}

impl DeltaBatchWriter {
    pub fn new(
        path: &str,
        value_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
        partition_columns: Vec<String>,
    ) -> Result<Self, WriteError> {
        let (table, metadata_per_column) =
            Self::open_table(path, value_fields, storage_options, partition_columns)?;
        let writer = DTRecordBatchWriter::for_table(&table)?;
        Ok(Self {
            table,
            writer,
            metadata_per_column,
        })
    }

    pub fn open_table(
        path: &str,
        schema_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
        partition_columns: Vec<String>,
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
        for (field, type_) in SPECIAL_OUTPUT_FIELDS {
            struct_fields.push(DeltaTableStructField::new(
                field,
                Self::delta_table_type(&type_)?,
                false,
            ));
        }

        let runtime = create_async_tokio_runtime()?;
        let table: DeltaTable = runtime
            .block_on(async {
                let builder = DeltaTableCreateBuilder::new()
                    .with_location(path)
                    .with_save_mode(DeltaTableSaveMode::Append)
                    .with_columns(struct_fields.clone())
                    .with_configuration_property(TableProperty::AppendOnly, Some("true"))
                    .with_storage_options(storage_options.clone())
                    .with_partition_columns(partition_columns);

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
        let mut columns_outside_existing_schema: Vec<String> = Vec::new();
        let mut columns_missing_in_user_schema: Vec<String> = Vec::new();
        let mut columns_mismatching_types = Vec::new();
        let mut has_error = false;

        let mut defined_user_columns = HashSet::new();
        for user_column in user_schema {
            let name = &user_column.name;
            defined_user_columns.insert(name.to_string());
            let Some(schema_column) = existing_schema.get(name) else {
                columns_outside_existing_schema.push(name.to_string());
                has_error = true;
                continue;
            };
            let nullability_differs = user_column.nullable != schema_column.nullable;
            let data_type_differs = user_column.data_type != schema_column.data_type;
            if nullability_differs || data_type_differs {
                columns_mismatching_types.push(FieldMismatchDetails {
                    schema_field: schema_column.clone(),
                    user_field: user_column.clone(),
                });
                has_error = true;
            }
        }
        for schema_column in existing_schema.keys() {
            if !defined_user_columns.contains(schema_column) {
                columns_missing_in_user_schema.push(schema_column.to_string());
                has_error = true;
            }
        }

        if has_error {
            let schema_mismatch_details = SchemaMismatchDetails {
                columns_outside_existing_schema,
                columns_missing_in_user_schema,
                columns_mismatching_types,
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
                    DeltaTableStructField::new("shape", shape_data_type, false),
                    DeltaTableStructField::new("elements", elements_data_type, false),
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
}

impl LakeBatchWriter for DeltaBatchWriter {
    fn write_batch(&mut self, batch: ArrowRecordBatch) -> Result<(), WriteError> {
        create_async_tokio_runtime()?.block_on(async {
            self.writer.write(batch).await?;
            self.writer.flush_and_commit(&mut self.table).await?;
            Ok::<(), WriteError>(())
        })
    }

    fn settings(&self) -> LakeWriterSettings {
        LakeWriterSettings {
            use_64bit_size_type: false,
            utc_timezone_name: "UTC".into(),
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
}

impl DeltaReaderAction {
    pub fn new(action_type: DataEventType, path: String) -> Self {
        Self {
            action_type,
            path,
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

        let mut parquet_files_queue = {
            let history = runtime.block_on(async {
                Ok::<Vec<DeltaTableCommitInfo>, ReadError>(table.history(None).await?)
            })?;
            Self::get_reader_actions(&table, path, history)?
        };
        let mut backfilling_entries_queue = VecDeque::new();

        if let Some(start_from_timestamp_ms) = start_from_timestamp_ms {
            assert!(backfilling_thresholds.is_empty()); // Checked upstream in python_api.rs
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
            for version in earliest_version..=latest_version {
                let Some(timestamp) = snapshot.version_timestamp(version) else {
                    continue;
                };
                if timestamp < start_from_timestamp_ms {
                    last_version_below_threshold = Some(version);
                } else {
                    break;
                }
            }
            if let Some(last_version_below_threshold) = last_version_below_threshold {
                runtime
                    .block_on(async { table.load_version(last_version_below_threshold).await })?;
                current_version = last_version_below_threshold;
                parquet_files_queue.clear();
            } else {
                warn!("All available versions are newer than the specified timestamp {start_from_timestamp_ms}. The read will start from the beginning.");
            }
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

    fn create_backfilling_files_queue(
        runtime: &TokioRuntime,
        table: DeltaTable,
        backfilling_thresholds: Vec<BackfillingThreshold>,
        column_types: &mut HashMap<String, Type>,
    ) -> Result<VecDeque<BackfillingEntry>, ReadError> {
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
                for value_map in value_maps {
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
                            commit_allowed: true,
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
                    commit_allowed: true,
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

    fn get_reader_actions(
        table: &DeltaTable,
        base_path: &str,
        mut history: Vec<DeltaTableCommitInfo>,
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
                (
                    DeltaReaderAction::new(
                        DataEventType::Insert,
                        Self::ensure_absolute_path_with_base(&action.path, base_path),
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
                            DeltaReaderAction::new(DataEventType::Delete, action_path)
                        }
                        DeltaLakeAction::Add(action) => {
                            data_changed |= action.data_change;
                            let action_path = self.ensure_absolute_path(&action.path);
                            DeltaReaderAction::new(DataEventType::Insert, action_path)
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
                    let source_event = ReadResult::FinishedSource {
                        commit_allowed: self
                            .current_action
                            .as_ref()
                            .expect("current action must be set if there's a reader")
                            .is_last_in_version,
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
        let row_map = if let Some(maybe_row_map) = self.backfilling_entries_queue.pop_front() {
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

use log::warn;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use deltalake::arrow::datatypes::DataType as ArrowDataType;
use deltalake::arrow::datatypes::TimeUnit as ArrowTimeUnit;
use deltalake::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use deltalake::datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use deltalake::parquet::file::properties::WriterProperties;
use futures::{stream, StreamExt, TryStreamExt};
use iceberg::arrow::type_to_arrow_type as iceberg_type_to_arrow_type;
use iceberg::arrow::FieldMatchMode;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::{
    ListType as IcebergListType, NestedField, PrimitiveType as IcebergPrimitiveType,
    Schema as IcebergSchema, StructType as IcebergStructType, Type as IcebergType,
};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog as IcebergCatalog;
use iceberg::{Namespace, NamespaceIdent, TableCreation, TableIdent};
use tokio::runtime::Runtime as TokioRuntime;

use super::{
    columns_into_pathway_values, LakeBatchWriter, LakeWriterSettings, SPECIAL_OUTPUT_FIELDS,
};
use crate::connectors::data_format::{
    NDARRAY_ELEMENTS_FIELD_NAME, NDARRAY_SHAPE_FIELD_NAME, NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
};
use crate::connectors::data_storage::data_lake::buffering::PayloadType;
use crate::connectors::data_storage::data_lake::MetadataPerColumn;
use crate::connectors::data_storage::{CommitPossibility, ConnectorMode};
use crate::connectors::metadata::IcebergMetadata;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError,
};
use crate::engine::Type;
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries, execute_with_retries_async, RetryConfig};
use crate::timestamp::current_unix_timestamp_ms;

/// Errors specific to the Iceberg connector.
///
/// Wraps the underlying iceberg-rust `iceberg::Error` plus connector-specific
/// schema-mismatch and Arrow-conversion failures that previously lived as
/// individual variants on the global `ReadError` / `WriteError` enums. Mirrors
/// the pattern `MssqlError`, `PostgresError`, etc. follow — a single
/// `Iceberg(#[from] IcebergError)` arm carries every Iceberg-shaped failure.
#[derive(Debug, thiserror::Error)]
pub enum IcebergError {
    /// An error surfaced by iceberg-rust itself (catalog, IO, parquet writer, etc.).
    #[error(transparent)]
    Library(#[from] iceberg::Error),

    // ---- Reader schema preflight ----
    #[error(
        "Iceberg schema mismatch: the Pathway schema declares column '{column}' which is not present in the iceberg table; available columns are: [{available}]"
    )]
    ReadColumnNotInTable { column: String, available: String },

    #[error(
        "Iceberg schema mismatch: column '{column}' is a Pathway tuple of arity {user_arity}, but the iceberg struct has {struct_arity} fields; Pathway tuples bind to iceberg struct fields positionally, so the arities must match"
    )]
    ReadStructArityMismatch {
        column: String,
        user_arity: usize,
        struct_arity: usize,
    },

    #[error(
        "Iceberg schema mismatch: column '{column}' is declared as Pathway type {pathway_type} but the iceberg column has type {iceberg_type}; this pair has no defined read decoding"
    )]
    ReadColumnTypeMismatch {
        column: String,
        pathway_type: String,
        iceberg_type: String,
    },

    // ---- Writer schema preflight ----
    #[error(
        "Iceberg schema mismatch: the destination table requires a non-null column '{column}' that the Pathway table does not produce; add it to your Pathway schema or make the column nullable in iceberg"
    )]
    WriteRequiredColumnMissing { column: String },

    #[error(
        "Iceberg schema mismatch: the Pathway table declares column '{column}' which is not present in the destination iceberg table; available columns are: [{available}]"
    )]
    WriteColumnNotInTable { column: String, available: String },

    #[error(
        "Iceberg schema mismatch: column '{column}' is a Pathway tuple of arity {user_arity}, but the destination iceberg struct has {struct_arity} fields; Pathway tuples bind to iceberg struct fields positionally, so the arities must match"
    )]
    WriteStructArityMismatch {
        column: String,
        user_arity: usize,
        struct_arity: usize,
    },

    #[error(
        "Iceberg schema mismatch: column '{column}' is declared as Pathway type {pathway_type} but the destination iceberg column has type {iceberg_type}; this pair has no defined write encoding"
    )]
    WriteColumnTypeMismatch {
        column: String,
        pathway_type: String,
        iceberg_type: String,
    },

    #[error(
        "Iceberg timestamp precision mismatch on column '{column}': you passed timestamp_unit=\"{chosen}\" but the destination column expects \"{destination}\". Pass timestamp_unit=\"{destination}\" when writing, or recreate the iceberg column at the precision you want."
    )]
    WriteTimestampUnitMismatch {
        column: String,
        chosen: String,
        destination: String,
    },

    // ---- Arrow-conversion failures emitted on the Iceberg write path ----
    #[error("integer value {value} does not fit into {target}")]
    IntegerNarrowingOverflow { value: i64, target: ArrowDataType },

    #[error("fixed_size_binary width must be non-negative, got {width}")]
    FixedSizeBinaryNegativeWidth { width: i32 },

    #[error("fixed_size_binary value has length {value_len}, expected exactly {expected} bytes")]
    FixedSizeBinaryLengthMismatch { value_len: usize, expected: usize },

    #[error("string value {value:?} is not a valid UUID")]
    UuidParseError { value: String },

    #[error("date value {value} doesn't fit into an Iceberg date column (i32 days since epoch)")]
    DateOutOfRange { value: String },
}

fn ensure_namespace(
    runtime: &TokioRuntime,
    catalog: &dyn IcebergCatalog,
    namespace: &[String],
) -> Result<Namespace, iceberg::Error> {
    let ident = NamespaceIdent::from_strs(namespace)?;
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

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct IcebergTableParams {
    name: String,
    schema: IcebergSchema,
    metadata_per_column: MetadataPerColumn,
    timestamp_unit: ArrowTimeUnit,
    /// Pathway type the user declared for each user-supplied column. Used at
    /// `IcebergBatchWriter::new` time to reconcile against an existing table's
    /// schema and emit Arrow-type overrides where the existing column is
    /// narrower / encoded differently (`int`, `float`, `decimal`, `uuid`,
    /// `fixed(N)`).
    user_pathway_types: HashMap<String, Type>,
}

impl IcebergTableParams {
    pub fn new(
        name: String,
        fields: &[ValueField],
        timestamp_unit: ArrowTimeUnit,
    ) -> Result<Self, WriteError> {
        let schema = Self::build_schema(fields, timestamp_unit)?;
        let mut metadata_per_column = MetadataPerColumn::new();
        for field in schema.as_struct().fields() {
            let mut metadata = HashMap::with_capacity(1);
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string());
            metadata_per_column.insert(field.name.clone(), metadata);
        }
        let user_pathway_types = fields
            .iter()
            .map(|f| (f.name.clone(), f.type_.clone()))
            .collect();
        Ok(Self {
            name,
            schema,
            metadata_per_column,
            timestamp_unit,
            user_pathway_types,
        })
    }

    pub fn metadata_per_column(&self) -> MetadataPerColumn {
        self.metadata_per_column.clone()
    }

    pub fn ensure_table(
        &self,
        runtime: &TokioRuntime,
        catalog: &dyn IcebergCatalog,
        namespace: &Namespace,
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

                let creation = creation_builder.build();
                catalog.create_table(namespace.name(), creation).await
            }
        })?;

        Ok(table)
    }

    fn build_schema(
        fields: &[ValueField],
        timestamp_unit: ArrowTimeUnit,
    ) -> Result<IcebergSchema, WriteError> {
        let mut field_id_counter: i32 = 0;
        let mut next_field_id = || {
            field_id_counter += 1;
            field_id_counter
        };
        let mut nested_fields = Vec::with_capacity(fields.len());
        for field in fields {
            let id = next_field_id();
            let nested_type = Self::iceberg_type(&field.type_, timestamp_unit, &mut next_field_id)?;
            nested_fields.push(Arc::new(NestedField::new(
                id,
                field.name.clone(),
                nested_type,
                false, // No optional fields
            )));
        }
        for (name, type_) in SPECIAL_OUTPUT_FIELDS {
            let id = next_field_id();
            let nested_type = Self::iceberg_type(&type_, timestamp_unit, &mut next_field_id)?;
            nested_fields.push(Arc::new(NestedField::new(id, name, nested_type, false)));
        }
        let iceberg_schema = IcebergSchema::builder()
            .with_fields(nested_fields)
            .build()?;
        Ok(iceberg_schema)
    }

    #[allow(clippy::too_many_lines)] // one match arm per Pathway type — splits naturally as a long table.
    fn iceberg_type(
        type_: &Type,
        timestamp_unit: ArrowTimeUnit,
        next_field_id: &mut dyn FnMut() -> i32,
    ) -> Result<IcebergType, WriteError> {
        let iceberg_type = match type_ {
            Type::Bool => IcebergType::Primitive(IcebergPrimitiveType::Boolean),
            Type::Float => IcebergType::Primitive(IcebergPrimitiveType::Double),
            Type::String | Type::Json | Type::Pointer => {
                IcebergType::Primitive(IcebergPrimitiveType::String)
            }
            Type::Bytes | Type::PyObjectWrapper => {
                IcebergType::Primitive(IcebergPrimitiveType::Binary)
            }
            Type::DateTimeNaive => match timestamp_unit {
                ArrowTimeUnit::Microsecond => {
                    IcebergType::Primitive(IcebergPrimitiveType::Timestamp)
                }
                ArrowTimeUnit::Nanosecond => {
                    IcebergType::Primitive(IcebergPrimitiveType::TimestampNs)
                }
                _ => unreachable!(),
            },
            Type::DateTimeUtc => match timestamp_unit {
                ArrowTimeUnit::Microsecond => {
                    IcebergType::Primitive(IcebergPrimitiveType::Timestamptz)
                }
                ArrowTimeUnit::Nanosecond => {
                    IcebergType::Primitive(IcebergPrimitiveType::TimestamptzNs)
                }
                _ => unreachable!(),
            },
            Type::Int | Type::Duration => IcebergType::Primitive(IcebergPrimitiveType::Long),
            Type::Optional(wrapped) => Self::iceberg_type(wrapped, timestamp_unit, next_field_id)?,
            Type::List(element_type) => {
                let element_type_is_optional = element_type.is_optional();
                let nested_element_type = Self::iceberg_type(
                    element_type.unoptionalize(),
                    timestamp_unit,
                    next_field_id,
                )?;
                let element_id = next_field_id();
                let nested_type = NestedField::new(
                    element_id,
                    NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
                    nested_element_type,
                    !element_type_is_optional,
                );
                let array_type = IcebergListType::new(nested_type.into());
                IcebergType::List(array_type)
            }
            // Pathway `Type::Array(_, T)` (`np.ndarray`) is encoded as a struct
            // of `{shape: list<long>, elements: list<T>}` — identical to the
            // shape Delta's `delta_table_type` already uses, so a table created
            // by either connector reads back the same way through the other.
            // The dimensionality is implicit in the length of `shape` per row;
            // no static info is required at table-creation time.
            Type::Array(_, nested_type) => {
                let wrapped_type = nested_type.as_ref();
                let elements_iceberg_type = match wrapped_type {
                    Type::Int => IcebergType::Primitive(IcebergPrimitiveType::Long),
                    Type::Float => IcebergType::Primitive(IcebergPrimitiveType::Double),
                    _ => return Err(WriteError::UnsupportedType(type_.clone())),
                };
                // `required = false` (i.e. nullable) on the list elements
                // matches what `arrow_data_type` emits for `Type::Array`
                // (nullable inner-list elements). Mismatch would surface as
                // an Arrow "Incompatible type" schema/array nullability
                // disagreement at parquet-write time.
                let shape_element_id = next_field_id();
                let shape_list = IcebergListType::new(
                    NestedField::new(
                        shape_element_id,
                        NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
                        IcebergType::Primitive(IcebergPrimitiveType::Long),
                        false,
                    )
                    .into(),
                );
                let elements_element_id = next_field_id();
                let elements_list = IcebergListType::new(
                    NestedField::new(
                        elements_element_id,
                        NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
                        elements_iceberg_type,
                        false,
                    )
                    .into(),
                );
                let shape_field_id = next_field_id();
                let elements_field_id = next_field_id();
                IcebergType::Struct(IcebergStructType::new(vec![
                    Arc::new(NestedField::new(
                        shape_field_id,
                        NDARRAY_SHAPE_FIELD_NAME,
                        IcebergType::List(shape_list),
                        true,
                    )),
                    Arc::new(NestedField::new(
                        elements_field_id,
                        NDARRAY_ELEMENTS_FIELD_NAME,
                        IcebergType::List(elements_list),
                        true,
                    )),
                ]))
            }
            // Pathway `tuple[T1, T2, …]` is positional; Iceberg `struct` requires
            // names. Synthesize the positional names `"[0]", "[1]", …` (same
            // convention DeltaBatchWriter uses) and let the recursion assign each
            // nested field a fresh schema-wide-unique id.
            Type::Tuple(nested_types) => {
                let mut struct_fields = Vec::with_capacity(nested_types.len());
                for (index, nested_type) in nested_types.iter().enumerate() {
                    let nested_type_is_optional = nested_type.is_optional();
                    let nested_iceberg_type = Self::iceberg_type(
                        nested_type.unoptionalize(),
                        timestamp_unit,
                        next_field_id,
                    )?;
                    let id = next_field_id();
                    struct_fields.push(Arc::new(NestedField::new(
                        id,
                        format!("[{index}]"),
                        nested_iceberg_type,
                        !nested_type_is_optional,
                    )));
                }
                IcebergType::Struct(IcebergStructType::new(struct_fields))
            }
            Type::Any | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()));
            }
        };
        Ok(iceberg_type)
    }
}

const MAX_CATALOG_RETRIES: usize = 5;

#[allow(clippy::module_name_repetitions)]
pub struct IcebergBatchWriter {
    runtime: TokioRuntime,
    catalog: Box<dyn IcebergCatalog>,
    table: IcebergTable,
    table_ident: TableIdent,
    metadata_per_column: MetadataPerColumn,
    timestamp_unit: ArrowTimeUnit,
    /// Per-column Arrow-type overrides discovered when reconciling the user's
    /// Pathway types against an existing table's Iceberg schema. The wider
    /// arrow schema constructed from the user's Pathway types is rewritten at
    /// `construct_arrow_schema` time so that:
    ///
    /// - Pathway `int` writes into existing Iceberg `int` (32-bit) as Arrow
    ///   `Int32`, with overflow detection in `array_for_type`.
    /// - Pathway `float` writes into existing `float` as `Float32`.
    /// - Pathway `str` writes into existing `decimal(p,s)` as `Decimal128`,
    ///   parsing the string into the unscaled `i128`.
    /// - Pathway `str` writes into existing `uuid` as `FixedSizeBinary(16)`,
    ///   parsing canonical UUID hex.
    /// - Pathway `bytes` writes into existing `fixed(N)` as
    ///   `FixedSizeBinary(N)`, length-checked.
    ///
    /// The map is empty in the common case (no narrower / alternative encoding
    /// columns in the existing schema, or the table is created fresh).
    arrow_type_overrides: HashMap<String, ArrowDataType>,
}

impl IcebergBatchWriter {
    #[allow(clippy::too_many_lines)] // sequence of independent preflight checks; splitting hurts readability
    pub fn new(
        runtime: TokioRuntime,
        catalog: Box<dyn IcebergCatalog>,
        namespace: &[String],
        table_params: &IcebergTableParams,
    ) -> Result<Self, WriteError> {
        let namespace = execute_with_retries(
            || ensure_namespace(&runtime, catalog.as_ref(), namespace),
            RetryConfig::default(),
            MAX_CATALOG_RETRIES,
        )?;
        let table = execute_with_retries(
            || table_params.ensure_table(&runtime, catalog.as_ref(), &namespace),
            RetryConfig::default(),
            MAX_CATALOG_RETRIES,
        )?;

        // Preflight: every required (non-null) column the destination table
        // declares must be either produced by the user's Pathway table or
        // one of the special columns Pathway itself appends (`time`,
        // `diff`). Without this check, a missing required column surfaces
        // at the first flush as the deep-stack iceberg-rust error
        // `DataInvalid => Field id N not found in struct array`, with no
        // hint as to which column the user has to add.
        let pathway_appended: std::collections::HashSet<&str> = SPECIAL_OUTPUT_FIELDS
            .iter()
            .map(|(name, _)| *name)
            .collect();
        let user_supplied: std::collections::HashSet<&str> = table_params
            .user_pathway_types
            .keys()
            .map(String::as_str)
            .collect();
        let iceberg_column_names: std::collections::HashSet<&str> = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        for field in table.metadata().current_schema().as_struct().fields() {
            if !field.required {
                continue;
            }
            let name = field.name.as_str();
            if pathway_appended.contains(name) || user_supplied.contains(name) {
                continue;
            }
            return Err(IcebergError::WriteRequiredColumnMissing {
                column: field.name.clone(),
            }
            .into());
        }

        // Preflight: every column in the user's Pathway table must have a
        // home in the destination iceberg table. Without this check, an
        // extra Pathway column knocks the writer's column ordering off
        // and surfaces as an obscure arrow type-mismatch on a *different*
        // column ("Field 'time' has type Int64, array has type Utf8" when
        // the actual problem is an unexpected `extra` Pathway column).
        for user_column in table_params.user_pathway_types.keys() {
            if !iceberg_column_names.contains(user_column.as_str()) {
                let mut available: Vec<&str> = iceberg_column_names.iter().copied().collect();
                available.sort_unstable();
                return Err(IcebergError::WriteColumnNotInTable {
                    column: user_column.clone(),
                    available: available.join(", "),
                }
                .into());
            }
        }

        // Preflight: when a user's Pathway tuple column binds to an existing
        // iceberg struct column, arities must match. Without this check, the
        // mismatch surfaces deep inside arrow as a panic from
        // `StructArray::try_new` ("Incorrect array length for StructArray
        // field 'X', expected N got 0") and the message names *an inner
        // struct field* — not the user-facing column name nor the arity
        // mismatch itself.
        for field in table.metadata().current_schema().as_struct().fields() {
            let Some(user_type) = table_params.user_pathway_types.get(&field.name) else {
                continue;
            };
            let (Type::Tuple(user_fields), IcebergType::Struct(struct_type)) =
                (user_type.unoptionalize(), field.field_type.as_ref())
            else {
                continue;
            };
            let struct_arity = struct_type.fields().len();
            let user_arity = user_fields.len();
            if user_arity != struct_arity {
                return Err(IcebergError::WriteStructArityMismatch {
                    column: field.name.clone(),
                    user_arity,
                    struct_arity,
                }
                .into());
            }
        }

        // Preflight: every user column's Pathway type must be encodable into
        // its destination iceberg column's type. Without this check, a
        // type clash (e.g. Pathway `int` → iceberg `string`) surfaces as a
        // deep arrow / parquet error at the first flush:
        // `Arrow: Incompatible type. Field 'X' has type Utf8, array has
        // type Int64` — accurate but uses arrow type names the user never
        // chose, instead of the Pathway / iceberg names they wrote.
        for field in table.metadata().current_schema().as_struct().fields() {
            let Some(user_type) = table_params.user_pathway_types.get(&field.name) else {
                continue;
            };
            if !pathway_to_iceberg_compatible(user_type.unoptionalize(), &field.field_type) {
                return Err(IcebergError::WriteColumnTypeMismatch {
                    column: field.name.clone(),
                    pathway_type: format!("{user_type}"),
                    iceberg_type: format!("{}", field.field_type),
                }
                .into());
            }
        }

        // Preflight: when the user picks `timestamp_unit="us"` vs `"ns"`,
        // Pathway emits arrow Timestamp at the chosen precision. The
        // destination iceberg column may be `timestamp` (µs) or
        // `timestamp_ns` — those are *different* iceberg primitives, not
        // alternate precisions, so writing across the precision boundary
        // fails as `Arrow: Incompatible type. Field 'X' has type
        // Timestamp(µs), array has type Timestamp(ns)`. Use the user's
        // configured timestamp_unit to detect the clash before any parquet
        // is written.
        for field in table.metadata().current_schema().as_struct().fields() {
            let Some(user_type) = table_params.user_pathway_types.get(&field.name) else {
                continue;
            };
            let user_type_unopt = user_type.unoptionalize();
            let IcebergType::Primitive(primitive) = field.field_type.as_ref() else {
                continue;
            };
            let mismatch = match (user_type_unopt, primitive, table_params.timestamp_unit) {
                (
                    Type::DateTimeNaive,
                    IcebergPrimitiveType::Timestamp,
                    ArrowTimeUnit::Nanosecond,
                )
                | (
                    Type::DateTimeUtc,
                    IcebergPrimitiveType::Timestamptz,
                    ArrowTimeUnit::Nanosecond,
                ) => Some(("ns", "us")),
                (
                    Type::DateTimeNaive,
                    IcebergPrimitiveType::TimestampNs,
                    ArrowTimeUnit::Microsecond,
                )
                | (
                    Type::DateTimeUtc,
                    IcebergPrimitiveType::TimestamptzNs,
                    ArrowTimeUnit::Microsecond,
                ) => Some(("us", "ns")),
                _ => None,
            };
            if let Some((chosen, destination)) = mismatch {
                return Err(IcebergError::WriteTimestampUnitMismatch {
                    column: field.name.clone(),
                    chosen: chosen.to_string(),
                    destination: destination.to_string(),
                }
                .into());
            }
        }

        let arrow_type_overrides = compute_arrow_type_overrides(
            table.metadata().current_schema(),
            &table_params.user_pathway_types,
        );

        // Iceberg parquet writers stamp every column with its field id in
        // `PARQUET_FIELD_ID_META_KEY`; on read, iceberg matches columns by id
        // (not by name). When pathway writes into a table created elsewhere,
        // the user's IcebergTableParams-built schema's ids don't match the
        // existing table's ids. Rebuild metadata_per_column from the actual
        // loaded schema so the parquet output stays aligned.
        let metadata_per_column =
            metadata_per_column_from_iceberg_schema(table.metadata().current_schema());

        Ok(Self {
            runtime,
            catalog,
            table,
            table_ident: TableIdent::new(namespace.name().clone(), table_params.name.clone()),
            metadata_per_column,
            timestamp_unit: table_params.timestamp_unit,
            arrow_type_overrides,
        })
    }

    fn create_writer_builder(
        table: &IcebergTable,
    ) -> Result<
        DataFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
        WriteError,
    > {
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("block-{}", current_unix_timestamp_ms()),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        // Use name-based matching between the Arrow batch and the iceberg
        // schema. Pathway's writer already aligns names at every level —
        // top-level columns use the user's names, struct children use the
        // synthesized `[N]` (matching what Pathway emits via
        // `iceberg_type(Type::Tuple)`), list elements use `"element"` —
        // and the existing-column override rewrites the Arrow nested type
        // to whatever names the destination table declares. The default
        // `FieldMatchMode::Id` would require us to attach
        // `PARQUET_FIELD_ID_META_KEY` metadata to every nested arrow field,
        // which `arrow_data_type` doesn't do for structs / list elements.
        let parquet_writer_builder = ParquetWriterBuilder::new_with_match_mode(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
            FieldMatchMode::Name,
        );
        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        Ok(DataFileWriterBuilder::new(rolling_file_writer_builder))
    }
}

impl LakeBatchWriter for IcebergBatchWriter {
    fn write_batch(
        &mut self,
        batch: ArrowRecordBatch,
        payload_type: PayloadType,
    ) -> Result<(), WriteError> {
        assert_eq!(payload_type, PayloadType::Diff);
        let writer_builder = Self::create_writer_builder(&self.table)?;
        self.runtime.block_on(async {
            // Prepare a new data block once. The file write is expensive
            // (parquet encoding + storage I/O) and the data file produced
            // here is just bytes on disk — uncommitted, so safe to keep
            // around across multiple commit attempts.
            let mut data_file_writer = writer_builder.build(None).await?;
            data_file_writer.write(batch).await?;
            let data_file = data_file_writer.close().await?;

            // Retry only the catalog commit portion. Iceberg uses optimistic
            // concurrency: a concurrent writer (or an external compaction)
            // that wins the race fails our commit with a conflict, and the
            // same path also catches transient catalog blips (network, REST
            // restart). Both are resolved the same way — rebase on the
            // latest table state and re-run the append — so reload
            // `self.table` at the *start* of every attempt. Reloading only
            // after a successful commit (as a previous version did) left a
            // retry rebuilding the identical transaction from the stale
            // pre-conflict snapshot, so a genuine conflict could never
            // converge; it also re-appended the data file whenever the
            // post-commit reload itself failed. The leading reload further
            // means back-to-back batches rebase on our own previous commit
            // instead of conflicting with it.
            //
            // The data file was written to storage once, before this loop,
            // and is immutable, so re-appending it on a fresh transaction is
            // safe. Caveat: if a commit lands but its *reply* is lost on the
            // wire, the next attempt reloads, doesn't see its effect in time,
            // and appends the file again — one duplicate snapshot pointing
            // at the same data file. Pathway's required primary key dedupes
            // the duplicate rows downstream, so user-visible state stays
            // correct; the table just carries one extra snapshot.
            execute_with_retries_async(
                async || {
                    self.table = self.catalog.load_table(&self.table_ident).await?;
                    let tx = Transaction::new(&self.table);
                    let append_action = tx.fast_append().add_data_files(data_file.clone());
                    let tx = append_action.apply(tx)?;
                    tx.commit(self.catalog.as_ref()).await?;
                    Ok::<(), WriteError>(())
                },
                RetryConfig::default(),
                MAX_CATALOG_RETRIES,
            )
            .await
        })
    }

    fn settings(&self) -> LakeWriterSettings {
        LakeWriterSettings {
            use_64bit_size_type: true,
            utc_timezone_name: "+00:00".into(),
            timestamp_unit: self.timestamp_unit,
        }
    }

    fn name(&self) -> String {
        format!(
            "Iceberg({}, {})",
            self.table_ident.namespace.to_url_string(),
            self.table_ident.name
        )
    }

    fn metadata_per_column(&self) -> &MetadataPerColumn {
        &self.metadata_per_column
    }

    fn arrow_type_overrides(&self) -> HashMap<String, ArrowDataType> {
        self.arrow_type_overrides.clone()
    }
}

/// Type-compatibility check used by the reader's preflight: does the
/// iceberg column's type have a defined decoding into the user's declared
/// Pathway type? The decoding set is broader than the writer's encoding
/// set — e.g. an iceberg `decimal(p, s)` column may be read as Pathway
/// `float` (lossy with warning) *or* `str` (lossless), and any narrow
/// integer reads into Pathway `int`. Mirrors the arms in
/// `column_into_pathway_values` and the parquet-record path used by
/// snapshot reads.
fn iceberg_to_pathway_compatible(iceberg_type: &IcebergType, user_type: &Type) -> bool {
    use iceberg::spec::PrimitiveType as P;
    // `Any` accepts anything — same convention as the arrow / parquet readers.
    if matches!(user_type, Type::Any) {
        return true;
    }
    match (iceberg_type, user_type) {
        (IcebergType::List(list), Type::List(inner)) => iceberg_to_pathway_compatible(
            list.element_field.field_type.as_ref(),
            inner.unoptionalize(),
        ),
        // A Pathway tuple binds to an iceberg struct positionally. Recurse
        // into each position so an inner-type mismatch is caught at startup
        // instead of degrading into per-row read errors (pw.run() exits
        // without raising). Arity is validated by a dedicated preflight that
        // produces a clearer message, so only the common prefix is checked
        // here — a prefix-compatible arity mismatch still reaches it.
        (IcebergType::Struct(struct_type), Type::Tuple(user_fields)) => user_fields
            .iter()
            .zip(struct_type.fields())
            .all(|(user_field, struct_field)| {
                iceberg_to_pathway_compatible(&struct_field.field_type, user_field.unoptionalize())
            }),
        // `np.ndarray` is read back from a `struct<shape: list<long>,
        // elements: list<T>>` whose children are resolved *by name*
        // (`convert_arrow_struct_array_to_ndarray`). Accept only structs that
        // carry both named list fields with a compatible element type — any
        // other struct would decode to a per-row error at read time.
        (IcebergType::Struct(struct_type), Type::Array(_, element_type)) => {
            let list_element = |field_name: &str| -> Option<&IcebergType> {
                struct_type
                    .fields()
                    .iter()
                    .find(|field| field.name == field_name)
                    .and_then(|field| match field.field_type.as_ref() {
                        IcebergType::List(list) => Some(list.element_field.field_type.as_ref()),
                        _ => None,
                    })
            };
            let shape_ok = matches!(
                list_element(NDARRAY_SHAPE_FIELD_NAME),
                Some(IcebergType::Primitive(P::Int | P::Long))
            );
            let elements_ok = list_element(NDARRAY_ELEMENTS_FIELD_NAME).is_some_and(|elem| {
                iceberg_to_pathway_compatible(elem, element_type.unoptionalize())
            });
            shape_ok && elements_ok
        }
        #[allow(clippy::match_like_matches_macro, clippy::unnested_or_patterns)]
        // flat or-pattern table reads more naturally than the nested form
        (IcebergType::Primitive(primitive), _) => match (primitive, user_type) {
            (P::Boolean, Type::Bool)
            | (P::Int | P::Long, Type::Int | Type::Duration)
            | (P::Float | P::Double, Type::Float)
            | (P::Decimal { .. }, Type::Float | Type::String)
            | (P::Date, Type::DateTimeNaive | Type::DateTimeUtc)
            | (P::Time, Type::Duration)
            | (P::Timestamp | P::TimestampNs, Type::DateTimeNaive)
            | (P::Timestamptz | P::TimestamptzNs, Type::DateTimeUtc)
            | (P::String, Type::String | Type::Json | Type::Pointer)
            | (P::Uuid, Type::String | Type::Bytes)
            | (P::Fixed(_) | P::Binary, Type::Bytes | Type::PyObjectWrapper) => true,
            _ => false,
        },
        _ => false,
    }
}

/// Type-compatibility check used by the writer's preflight: does Pathway's
/// `user_type` have a defined encoding into `iceberg_type`? Covers both the
/// natural mapping (`int` → `long`, `float` → `double`, etc.) and the
/// existing-column overrides (`int` → `int(32)`, `str` → `decimal`, …) so a
/// "compatible" pair here matches what `compute_arrow_type_overrides` and
/// the wider `arrow_data_type` / `array_for_type` machinery actually
/// accept at write time.
fn pathway_to_iceberg_compatible(user_type: &Type, iceberg_type: &IcebergType) -> bool {
    use iceberg::spec::PrimitiveType as P;
    match (user_type, iceberg_type) {
        // Composites recurse, otherwise schema mismatch.
        (Type::List(inner), IcebergType::List(list)) => pathway_to_iceberg_compatible(
            inner.unoptionalize(),
            list.element_field.field_type.as_ref(),
        ),
        (Type::Tuple(user_fields), IcebergType::Struct(struct_type)) => {
            // Arity gets its own preflight (clearer error), but for compat
            // we also need inner-type matching: a tuple where one position
            // disagrees with the destination struct's field type would
            // otherwise blow up deep inside `array_for_type` with the
            // arrow-side names, not the column-side identifier.
            user_fields.len() == struct_type.fields().len()
                && user_fields
                    .iter()
                    .zip(struct_type.fields())
                    .all(|(u, s)| pathway_to_iceberg_compatible(u.unoptionalize(), &s.field_type))
        }
        (Type::Array(_, _), IcebergType::Struct(_)) => true, // shape-checked at write time
        #[allow(clippy::match_like_matches_macro, clippy::unnested_or_patterns)]
        // flat or-pattern table reads more naturally than the nested form
        (_, IcebergType::Primitive(primitive)) => match (user_type, primitive) {
            (Type::Bool, P::Boolean)
            | (Type::Int | Type::Duration, P::Long)
            | (Type::Float, P::Double)
            | (Type::String | Type::Json | Type::Pointer, P::String)
            | (Type::Bytes | Type::PyObjectWrapper, P::Binary)
            | (Type::DateTimeNaive, P::Timestamp | P::TimestampNs)
            | (Type::DateTimeUtc, P::Timestamptz | P::TimestamptzNs)
            // Existing-column overrides:
            | (Type::Int, P::Int)
            | (Type::Float, P::Float)
            | (Type::String, P::Decimal { .. } | P::Uuid)
            | (Type::Bytes, P::Fixed(_))
            | (Type::Duration, P::Time)
            | (Type::DateTimeNaive, P::Date) => true,
            _ => false,
        },
        _ => false,
    }
}

/// Build `MetadataPerColumn` from a (just-loaded) Iceberg schema, attaching
/// each field's actual id under `PARQUET_FIELD_ID_META_KEY`. This is what
/// `construct_arrow_schema` propagates onto the Arrow output schema and what
/// the parquet writer encodes per column.
fn metadata_per_column_from_iceberg_schema(schema: &IcebergSchema) -> MetadataPerColumn {
    let mut out = MetadataPerColumn::new();
    for field in schema.as_struct().fields() {
        let mut metadata = HashMap::with_capacity(1);
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string());
        out.insert(field.name.clone(), metadata);
    }
    out
}

/// Inspect the existing table's Iceberg schema and the user's declared Pathway
/// types, and produce per-column Arrow-type overrides for the cases where the
/// natural Pathway → Iceberg mapping (Pathway `int` → `long`, `float` →
/// `double`, `str` → `string`, `bytes` → `binary`) would clash with a narrower
/// or alternatively-encoded existing column. Mirrors the override mechanism
/// `DeltaBatchWriter` uses for `decimal`, extended to cover Iceberg's `int`,
/// `float`, `uuid`, and `fixed(N)`.
///
/// Columns not in `user_pathway_types` (notably the `time` and `diff`
/// special columns added by `IcebergTableParams::build_schema`) are left as
/// emitted by `construct_arrow_schema` — they're always Pathway `int` →
/// Arrow `Int64`, matching the iceberg-rest tables Pathway creates.
fn compute_arrow_type_overrides(
    iceberg_schema: &IcebergSchema,
    user_pathway_types: &HashMap<String, Type>,
) -> HashMap<String, ArrowDataType> {
    let mut overrides = HashMap::new();
    for field in iceberg_schema.as_struct().fields() {
        let Some(user_type) = user_pathway_types.get(&field.name) else {
            continue;
        };
        let user_type_unopt = user_type.unoptionalize();
        // Pathway `tuple[…]` ↔ existing Iceberg `struct<…>`. Reuse iceberg's
        // own iceberg-type → arrow-type conversion so that the arrow batch
        // produced by `array_for_type::Struct` carries the table's actual
        // field names (and arrow-level field metadata such as parquet field
        // ids), not Pathway's synthetic `[N]` names. Pathway tuple position N
        // is taken to mean "the N-th field of the existing struct".
        if let (Type::Tuple(_), IcebergType::Struct(_)) =
            (user_type_unopt, field.field_type.as_ref())
        {
            if let Ok(arrow_type) = iceberg_type_to_arrow_type(&field.field_type) {
                if matches!(arrow_type, ArrowDataType::Struct(_)) {
                    overrides.insert(field.name.clone(), arrow_type);
                }
            }
            continue;
        }
        let IcebergType::Primitive(primitive) = &field.field_type.as_ref() else {
            continue;
        };
        let arrow_override = match (user_type_unopt, primitive) {
            (Type::Int, IcebergPrimitiveType::Int) => Some(ArrowDataType::Int32),
            (Type::Float, IcebergPrimitiveType::Float) => Some(ArrowDataType::Float32),
            (Type::String, IcebergPrimitiveType::Decimal { precision, scale }) => {
                let precision_u8 = u8::try_from(*precision).ok();
                let scale_i8 = i8::try_from(*scale).ok();
                if let (Some(p), Some(s)) = (precision_u8, scale_i8) {
                    Some(ArrowDataType::Decimal128(p, s))
                } else {
                    // Iceberg caps precision at 38 and scale at precision, so this
                    // should never fail; if it does, fall back to the default mapping.
                    None
                }
            }
            (Type::String, IcebergPrimitiveType::Uuid) => Some(ArrowDataType::FixedSizeBinary(16)),
            (Type::Bytes, IcebergPrimitiveType::Fixed(length)) => i32::try_from(*length)
                .ok()
                .map(ArrowDataType::FixedSizeBinary),
            // Pathway `Duration` ↔ Iceberg `time` (µs since midnight). Pathway
            // `Duration` is also the natural mapping for Iceberg `long` columns
            // that carry duration data, so we don't override that — only the
            // `time` case needs a different Arrow type than the default.
            (Type::Duration, IcebergPrimitiveType::Time) => {
                Some(ArrowDataType::Time64(ArrowTimeUnit::Microsecond))
            }
            // Pathway has no date-only type, so writes into an existing Iceberg
            // `date` column take a `DateTimeNaive` and silently drop the
            // time-of-day component (see `Date32` arm in `array_for_type`).
            (Type::DateTimeNaive, IcebergPrimitiveType::Date) => Some(ArrowDataType::Date32),
            _ => None,
        };
        if let Some(arrow_type) = arrow_override {
            overrides.insert(field.name.clone(), arrow_type);
        }
    }
    overrides
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
    catalog: Box<dyn IcebergCatalog>,
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
        runtime: TokioRuntime,
        catalog: Box<dyn IcebergCatalog>,
        namespace: &[String],
        table_params: &IcebergTableParams,
        column_types: HashMap<String, Type>,
        streaming_mode: ConnectorMode,
    ) -> Result<Self, ReadError> {
        let namespace = ensure_namespace(&runtime, catalog.as_ref(), namespace)?;
        let table_ident = TableIdent::new(namespace.name().clone(), table_params.name.clone());

        // Check that the table exists.
        let table = runtime.block_on(async { catalog.load_table(&table_ident).await })?;

        // Preflight: every column in the user's Pathway schema must be present
        // in the iceberg table. Without this, missing columns surface as a
        // per-row `NoDefault` parse error during `pw.run()` — the user only
        // sees their pipeline silently produce zero rows or a flood of
        // identical row-level errors. Catching it here names the missing
        // column and lists what's actually available.
        let iceberg_field_names: std::collections::HashSet<&str> = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        for user_column in column_types.keys() {
            if !iceberg_field_names.contains(user_column.as_str()) {
                let mut available: Vec<&str> = iceberg_field_names.iter().copied().collect();
                available.sort_unstable();
                return Err(IcebergError::ReadColumnNotInTable {
                    column: user_column.clone(),
                    available: available.join(", "),
                }
                .into());
            }
        }

        // Preflight: each user column's Pathway type must have a defined
        // decoding from its iceberg column's type. Without this check, the
        // per-row reader emits "Parse error: cannot create a field 'X'
        // with type T from value Y" to stderr and the pipeline silently
        // produces zero output — pw.run() exits without raising.
        for field in table.metadata().current_schema().as_struct().fields() {
            let Some(user_type) = column_types.get(&field.name) else {
                continue;
            };
            if !iceberg_to_pathway_compatible(&field.field_type, user_type.unoptionalize()) {
                return Err(IcebergError::ReadColumnTypeMismatch {
                    column: field.name.clone(),
                    pathway_type: format!("{user_type}"),
                    iceberg_type: format!("{}", field.field_type),
                }
                .into());
            }
        }

        // Preflight: when a user declares a Pathway `tuple` for an iceberg
        // `struct` column, arities must match. The per-row reader does
        // produce an explanatory error ("struct arity N != tuple arity M")
        // but only logs it to stderr per row and pw.run() exits without
        // raising — the pipeline silently produces no output.
        for field in table.metadata().current_schema().as_struct().fields() {
            let Some(user_type) = column_types.get(&field.name) else {
                continue;
            };
            let (Type::Tuple(user_fields), IcebergType::Struct(struct_type)) =
                (user_type.unoptionalize(), field.field_type.as_ref())
            else {
                continue;
            };
            let struct_arity = struct_type.fields().len();
            let user_arity = user_fields.len();
            if user_arity != struct_arity {
                return Err(IcebergError::ReadStructArityMismatch {
                    column: field.name.clone(),
                    user_arity,
                    struct_arity,
                }
                .into());
            }
        }

        // Mirror DeltaTableReader: warn at startup when reading an Iceberg
        // `decimal(p, s)` column as Pathway `float`, since that path goes
        // through f64 and is lossy beyond ~15-17 significant decimal digits.
        // Declaring the column as `str` in the Pathway schema reads it
        // losslessly as decimal text.
        for field in table.metadata().current_schema().as_struct().fields() {
            let Some(user_type) = column_types.get(&field.name) else {
                continue;
            };
            if !matches!(user_type.unoptionalize(), Type::Float) {
                continue;
            }
            if let IcebergType::Primitive(IcebergPrimitiveType::Decimal { precision, scale }) =
                field.field_type.as_ref()
            {
                warn!(
                    "Iceberg column '{}' is decimal({}, {}); reading it as Pathway 'float' \
                     will lose precision. Declare the column as 'str' in the Pathway schema \
                     to read it losslessly as decimal text.",
                    field.name, precision, scale,
                );
            }
        }

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

    fn wait_for_snapshot_update(&mut self, should_block: bool) -> Result<(), ReadError> {
        self.runtime.block_on(async {
            while self.diff_queue.is_empty() {
                // Catalog calls go over the network for every poll. A
                // transient blip — REST server restart, S3 / Glue
                // throttling, momentary DNS hiccup — would otherwise tear
                // down a long-running streaming pipeline. Retry the
                // load_table call before treating the error as fatal.
                let table: IcebergTable = execute_with_retries_async(
                    async || self.catalog.load_table(&self.table_ident).await,
                    RetryConfig::default(),
                    MAX_CATALOG_RETRIES,
                )
                .await?;
                let available_snapshot_id = table.metadata().current_snapshot_id();
                let snapshot_id_changed = available_snapshot_id != self.current_snapshot_id;
                if available_snapshot_id.is_none() || !snapshot_id_changed {
                    if !should_block {
                        // Static mode: nothing new to ingest, return so the
                        // caller can signal `Finished`.
                        return Ok(());
                    }
                    // `tokio::time::sleep` yields the runtime worker; the
                    // previous `std::thread::sleep` here pinned the
                    // (single-threaded, currently private) tokio runtime for
                    // the full poll interval, which would silently stall any
                    // co-tenant task if the runtime were ever shared.
                    tokio::time::sleep(ICEBERG_SLEEP_BETWEEN_SNAPSHOT_CHECKS).await;
                    continue;
                }

                // The snapshot has been updated at this point. Plan files
                // also goes through the catalog / manifest reads; same
                // transient-failure surface, same retry shape.
                //
                // TODO: there can be many files, yet the diff may consist
                // only of a few of them. But the versions of an iceberg
                // table form a tree. So the following solution should be
                // possible:
                // - Find the least common ancestor of the current and the
                //   updated snapshot.
                // - Traverse the path from the old version to the LCA and
                //   undo the changes on this path.
                // - Traverse the path from the LCA to the new version and
                //   apply changes on this path.
                // More reading on the protocol must be done to understand
                // how to implement this.
                let updated_table_plan: Vec<FileScanTask> = execute_with_retries_async(
                    async || {
                        table
                            .scan()
                            .build()?
                            .plan_files()
                            .await?
                            .try_collect()
                            .await
                    },
                    RetryConfig::default(),
                    MAX_CATALOG_RETRIES,
                )
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
                        commit_possibility: CommitPossibility::Possible,
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
    ) -> Result<Vec<ReadResult>, iceberg::Error> {
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
            let should_block = self.streaming_mode.is_polling_enabled();
            if should_block || !self.is_initialized {
                self.is_initialized = true;
                self.wait_for_snapshot_update(should_block)?;
                if !should_block && self.diff_queue.is_empty() {
                    return Ok(ReadResult::Finished);
                }
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
            // Persistence-restart path: a single transient catalog blip
            // here would prevent the pipeline from coming back up at all.
            // Retry both catalog calls so a five-second REST hiccup
            // doesn't turn into a manual restart.
            let table: IcebergTable = execute_with_retries_async(
                async || self.catalog.load_table(&self.table_ident).await,
                RetryConfig::default(),
                MAX_CATALOG_RETRIES,
            )
            .await?;
            let current_table_plan: Vec<FileScanTask> = execute_with_retries_async(
                async || {
                    table
                        .scan()
                        .snapshot_id(*snapshot_id)
                        .build()?
                        .plan_files()
                        .await?
                        .try_collect()
                        .await
                },
                RetryConfig::default(),
                MAX_CATALOG_RETRIES,
            )
            .await?;

            #[allow(clippy::mutable_key_type)]
            let current_table_plan: HashMap<FileScanTaskDescriptor, FileScanTask> =
                current_table_plan
                    .into_iter()
                    .map(|task| (FileScanTaskDescriptor::for_task(&task), task))
                    .collect();
            self.current_table_plan = current_table_plan;

            Ok::<(), iceberg::Error>(())
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

// Copyright © 2026 Pathway

//! The Avro data format: the generation of an Avro schema from the engine
//! types and the formatter encoding rows into Avro binary datums.
//!
//! The payload of a formatted message is a bare Avro datum of a record — no
//! container header and no schema identifier inside the body. The schema
//! itself travels out of band: a sink declares it to whatever schema
//! registry its target keeps, and a source obtains it back from there
//! through an [`AvroSchemaProvider`]. Which registry that is, and how it is
//! reached, is the storage's business — nothing here knows.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::schema::Schema as AvroSchema;
use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;
use base64::engine::general_purpose;
use base64::Engine;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::BigDecimal;
use serde_json::json;
use serde_json::Value as JsonValue;

use crate::connectors::data_format::{Formatter, FormatterContext, FormatterError};
use crate::connectors::exploration::ExploredField;
use crate::engine::error::{DynError, DynResult};
use crate::engine::time::{DateTime, DateTimeNaive, DateTimeUtc, Duration as EngineDuration};
use crate::engine::{Key, Timestamp, Type, Value};
use crate::python_api::ValueField;

/// The name under which the generated record schema is registered. The
/// namespace keeps the records produced by Pathway distinguishable from the
/// application-defined ones.
const RECORD_NAME: &str = "Row";
const RECORD_NAMESPACE: &str = "pathway";

// The field-level custom attribute marking an engine type whose Avro
// encoding alone cannot express it. Written into the generated schemas and
// honored by the schema deduction, so a write→deduce roundtrip preserves the
// column type.
const PATHWAY_TYPE_ATTRIBUTE: &str = "pathwayType";
const DURATION_TYPE_MARKER: &str = "Duration";

// The largest decimal scale rendered into a string. The schemas come from
// the broker without validation, and rendering a decimal materializes
// `scale` digits — an absurd scale must neither panic nor allocate
// unboundedly.
const MAX_RENDERED_DECIMAL_SCALE: usize = 16_384;

#[derive(Debug, thiserror::Error)]
pub enum AvroError {
    #[error("failed to construct an Avro schema: {0}")]
    SchemaConstruction(apache_avro::Error),

    #[error(
        "the type {type_:?} of the field \"{field}\" cannot be represented \
         in an Avro schema"
    )]
    UnsupportedType { field: String, type_: Type },

    #[error(
        "the tuple type of the field \"{field}\" mixes element types, which \
         an Avro array cannot represent"
    )]
    HeterogeneousTuple { field: String },

    #[error("failed to encode a row into Avro: {0}")]
    Encoding(apache_avro::Error),

    #[error("the value {value} of the field \"{field}\" does not match the declared type")]
    ValueTypeMismatch { field: String, value: Value },

    #[error(
        "the registered schema is not an Avro record: the connector reads \
         record-typed topics only"
    )]
    NonRecordSchema,

    #[error(
        "the schema refers to the named type \"{name}\" by reference, which \
         the schema deduction does not support. Pass an explicit schema \
         instead"
    )]
    UnresolvedRef { name: String },

    #[error("the primary-key field \"{field}\" is not a column of the table")]
    UnknownKeyField { field: String },
}

/// The Avro schema of one field type, per the documented type-conversion
/// table. `None` values are only representable through the `Optional`
/// wrapper, which becomes a `["null", T]` union with a `null` default —
/// keeping the schema evolution of optional columns backward-compatible.
fn engine_type_to_avro(type_: &Type, field: &str) -> Result<JsonValue, AvroError> {
    let schema = match type_ {
        Type::Bool => json!("boolean"),
        // The duration is a plain number of microseconds: the Avro `duration`
        // logical type counts months and days, whose length depends on the
        // calendar, and cannot represent an exact time interval.
        Type::Int | Type::Duration => json!("long"),
        Type::Float => json!("double"),
        // Pointers are serialized by their string representation, and Json
        // travels as its JSON text.
        Type::String | Type::Pointer | Type::Json => json!("string"),
        Type::Bytes => json!("bytes"),
        Type::DateTimeUtc => json!({"type": "long", "logicalType": "timestamp-micros"}),
        Type::DateTimeNaive => json!({"type": "long", "logicalType": "local-timestamp-micros"}),
        Type::List(element_type) => {
            json!({"type": "array", "items": engine_type_to_avro(element_type, field)?})
        }
        Type::Tuple(element_types) => {
            let Some((first, rest)) = element_types.split_first() else {
                return Err(AvroError::UnsupportedType {
                    field: field.to_string(),
                    type_: type_.clone(),
                });
            };
            if rest.iter().any(|other| other != first) {
                return Err(AvroError::HeterogeneousTuple {
                    field: field.to_string(),
                });
            }
            json!({"type": "array", "items": engine_type_to_avro(first, field)?})
        }
        Type::Optional(inner) => {
            return Ok(json!(["null", engine_type_to_avro(inner, field)?]));
        }
        Type::Any | Type::Array(_, _) | Type::PyObjectWrapper | Type::Future(_) => {
            return Err(AvroError::UnsupportedType {
                field: field.to_string(),
                type_: type_.clone(),
            });
        }
    };
    Ok(schema)
}

/// The JSON of the record schema generated for the given fields: what the
/// formatter encodes the rows with, and what a sink declares to the schema
/// registry of its target (see `Formatter::wire_schema`).
pub fn generate_avro_schema_json(value_fields: &[ValueField]) -> Result<JsonValue, AvroError> {
    let mut fields = Vec::with_capacity(value_fields.len());
    for value_field in value_fields {
        let type_schema = engine_type_to_avro(&value_field.type_, &value_field.name)?;
        let mut field = json!({
            "name": value_field.name,
            "type": type_schema,
        });
        if matches!(value_field.type_, Type::Optional(_)) {
            // The explicit null default keeps adding an optional column a
            // backward-compatible schema change for the broker's policy.
            field["default"] = JsonValue::Null;
        }
        if matches!(value_field.type_.unoptionalize(), Type::Duration) {
            // Durations travel as plain longs (the Avro `duration` logical
            // type is calendar-based and cannot carry an exact interval), so
            // the field spells out the unit for the external consumers and
            // carries a marker the schema deduction restores the engine type
            // from.
            field[PATHWAY_TYPE_ATTRIBUTE] = json!(DURATION_TYPE_MARKER);
            field["doc"] = json!("a duration in microseconds");
        }
        fields.push(field);
    }
    Ok(json!({
        "type": "record",
        "name": RECORD_NAME,
        "namespace": RECORD_NAMESPACE,
        "fields": fields,
    }))
}

/// The parsed form of the generated schema.
pub fn generate_avro_schema(value_fields: &[ValueField]) -> Result<AvroSchema, AvroError> {
    let schema_json = generate_avro_schema_json(value_fields)?;
    AvroSchema::parse(&schema_json).map_err(AvroError::SchemaConstruction)
}

/// Encodes one engine value into its Avro counterpart, following the
/// documented conversion table. The `type_` is the declared column type: it
/// decides the representation (e.g. whether an integer is a plain long or a
/// microsecond timestamp).
fn engine_value_to_avro(value: &Value, type_: &Type, field: &str) -> Result<AvroValue, AvroError> {
    let mismatch = || AvroError::ValueTypeMismatch {
        field: field.to_string(),
        value: value.clone(),
    };
    if let Type::Optional(inner) = type_ {
        // The generated union is always ["null", T]: the branch indices are 0
        // for null and 1 for the value.
        return Ok(match value {
            Value::None => AvroValue::Union(0, Box::new(AvroValue::Null)),
            _ => AvroValue::Union(1, Box::new(engine_value_to_avro(value, inner, field)?)),
        });
    }
    let avro_value = match (type_, value) {
        (Type::Bool, Value::Bool(b)) => AvroValue::Boolean(*b),
        (Type::Int, Value::Int(i)) => AvroValue::Long(*i),
        (Type::Float, Value::Float(f)) => AvroValue::Double(f.into_inner()),
        (Type::String, Value::String(s)) => AvroValue::String(s.to_string()),
        (Type::Bytes, Value::Bytes(b)) => AvroValue::Bytes(b.to_vec()),
        (Type::Pointer, Value::Pointer(key)) => AvroValue::String(key.to_string()),
        (Type::Json, Value::Json(json)) => AvroValue::String(json.to_string()),
        (Type::DateTimeUtc, Value::DateTimeUtc(dt)) => {
            AvroValue::TimestampMicros(dt.timestamp() / 1000)
        }
        (Type::DateTimeNaive, Value::DateTimeNaive(dt)) => {
            AvroValue::LocalTimestampMicros(dt.timestamp() / 1000)
        }
        (Type::Duration, Value::Duration(d)) => AvroValue::Long(d.nanoseconds() / 1000),
        (Type::List(element_type), Value::Tuple(elements)) => AvroValue::Array(
            elements
                .iter()
                .map(|element| engine_value_to_avro(element, element_type, field))
                .collect::<Result<_, _>>()?,
        ),
        (Type::Tuple(element_types), Value::Tuple(elements)) => {
            let element_type = element_types.first().ok_or_else(mismatch)?;
            AvroValue::Array(
                elements
                    .iter()
                    .map(|element| engine_value_to_avro(element, element_type, field))
                    .collect::<Result<_, _>>()?,
            )
        }
        _ => return Err(mismatch()),
    };
    Ok(avro_value)
}

/// Formats rows as bare Avro record datums. The engine time and the diff are
/// deliberately not part of the record: the schema is the user-facing
/// contract of the topic, and the message-queue writers already attach both
/// as the `pathway_time` / `pathway_diff` message properties.
pub struct AvroFormatter {
    schema: Arc<AvroSchema>,
    // The JSON the schema was built from: what the sinks declare to their
    // schema registry. Kept verbatim rather than re-derived from `schema`,
    // whose canonical form drops the defaults, the documentation and the
    // `pathwayType` markers the readers rely on.
    schema_json: String,
    // The payload fields, each paired with its position among the row values
    // the formatter receives: the row may also carry service columns (the
    // dynamic topic, the keys, the headers), which stay out of the record —
    // the Avro schema is a public, versioned contract of the topic.
    value_fields: Vec<(usize, ValueField)>,
}

impl AvroFormatter {
    pub fn new(value_fields: Vec<(usize, ValueField)>) -> Result<AvroFormatter, AvroError> {
        let payload_fields: Vec<ValueField> = value_fields
            .iter()
            .map(|(_, field)| field.clone())
            .collect();
        let schema_json = generate_avro_schema_json(&payload_fields)?;
        let schema = AvroSchema::parse(&schema_json).map_err(AvroError::SchemaConstruction)?;
        Ok(AvroFormatter {
            schema: Arc::new(schema),
            schema_json: schema_json.to_string(),
            value_fields,
        })
    }
}

impl Formatter for AvroFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        let mut record = Vec::with_capacity(self.value_fields.len());
        for (position, value_field) in &self.value_fields {
            let value = values
                .get(*position)
                .ok_or(FormatterError::ColumnsValuesCountMismatch)?;
            let avro_value = engine_value_to_avro(value, &value_field.type_, &value_field.name)
                .map_err(FormatterError::Avro)?;
            record.push((value_field.name.clone(), avro_value));
        }
        let raw_bytes = to_avro_datum(&self.schema, AvroValue::Record(record))
            .map_err(|e| FormatterError::Avro(AvroError::Encoding(e)))?;
        Ok(FormatterContext::new_single_payload(
            raw_bytes,
            *key,
            values.to_vec(),
            time,
            diff,
        ))
    }

    fn wire_schema(&self) -> Option<String> {
        Some(self.schema_json.clone())
    }

    fn short_description(&self) -> Cow<'static, str> {
        "avro".into()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AvroParseError {
    #[error(
        "the message carries no schema version: it was produced without a \
         schema, and the topic's registry cannot describe its layout"
    )]
    NoSchemaVersion,

    #[error("failed to obtain the writer schema {schema_id:?} from the registry: {message}")]
    SchemaLookup { schema_id: Vec<u8>, message: String },

    #[error("failed to decode the Avro payload: {0}")]
    Decoding(apache_avro::Error),

    #[error("the Avro value of the field \"{field}\" cannot be converted to the type {type_:?}")]
    IncompatibleValue { field: String, type_: Type },

    #[error("the decoded Avro value is not a record")]
    NotARecord,

    #[error(
        "the field \"{field}\" is missing from the decoded record and the \
         column declares no default"
    )]
    FieldMissing { field: String },

    #[error("the primary-key field \"{field}\" could not be parsed")]
    KeyFieldUnparsable { field: String },
}

/// The source of writer schemas for the parser: an implementation looks the
/// schema up by the identifier the transport attached to the message — a
/// registry version, a fingerprint, whatever the transport stamps. The
/// implementations live next to their storage, and so does everything about
/// obtaining a schema *reliably*: caching, retrying a source that is
/// momentarily unavailable, and deciding when a failure is final. The
/// parser sees either a schema or a failure it reports for the row.
pub trait AvroSchemaProvider: Send {
    fn get_schema(&mut self, schema_id: &[u8]) -> Result<Arc<AvroSchema>, String>;
}

/// Parses bare Avro record datums. The writer schema of every message is
/// obtained through the [`AvroSchemaProvider`] by the schema identifier the
/// connector reports via the source metadata; the datum is decoded with that
/// writer schema alone, and the decoded fields are then projected onto the
/// table's columns by name, with the per-type conversions of
/// [`avro_value_to_engine`]. A column missing from the decoded record falls
/// back to its default — so the messages written under the older versions of
/// an evolving schema stay readable. The apache-avro schema resolution is
/// deliberately not used: it is strict about the logical types
/// (timestamp-millis does not resolve into timestamp-micros, enums and uuids
/// do not resolve into strings), which would make most of the documented
/// conversion table unreachable.
pub struct AvroParser {
    reader_schema: Arc<AvroSchema>,
    value_fields: Vec<ValueField>,
    key_field_positions: Option<Vec<usize>>,
    schema_provider: Option<Box<dyn AvroSchemaProvider>>,
    current_schema_id: Option<Vec<u8>>,
    metadata_column_value: Value,
    // Whether the table declares a `_metadata` column. The connector reports
    // the source metadata for every message (the avro reader always needs
    // the schema version from it), but serializing the whole metadata tree
    // into a value is only worth doing when some column will carry it.
    has_metadata_column: bool,
    // Whether the table schema was written by the user (as opposed to being
    // deduced from the source's own schema). An explicit schema doubles as the decoding
    // schema for the messages produced without a schema version — the user
    // vouches for the layout — while under a deduced schema such messages
    // are undecodable and reported per row.
    explicit_schema: bool,
    // The per-column decoding plans, keyed by the schema version the
    // message carried (the empty key stands for the reader schema used when
    // no version is present). A plan captures everything the hot path would
    // otherwise recompute per message: the position of every table column
    // in the decoded record, the Json-text treatment of its strings, and
    // whether the schema contains decimals to normalize.
    projection_plans: HashMap<Vec<u8>, Arc<ProjectionPlan>>,
}

/// See the `projection_plans` field of [`AvroParser`].
struct ProjectionPlan {
    columns: Vec<ColumnPlan>,
    /// The named types of the writer schema, kept only when the schema
    /// contains decimals: the normalization walk resolves the `Schema::Ref`
    /// reuse through them. `None` means no decimals — the walk is skipped.
    decimal_names: Option<HashMap<apache_avro::schema::Name, AvroSchema>>,
}

enum ColumnPlan {
    /// The `_metadata` column, filled by the connector.
    Metadata,
    /// The value of the decoded record's field at this position.
    Field { position: usize, json_as_text: bool },
    /// The writer schema does not carry the column — e.g. one added by a
    /// newer schema version. The column falls back to its declared default;
    /// an optional column without one becomes None, the same null default
    /// its generated Avro form declares (see `engine_type_to_avro`).
    Absent,
}

/// Builds the decoding plan of one writer schema against the table columns.
fn build_projection_plan(
    value_fields: &[ValueField],
    writer_schema: &AvroSchema,
) -> ProjectionPlan {
    let names = schema_names(writer_schema);
    let decimal_names =
        schema_contains_decimal(writer_schema, &names, &mut Vec::new()).then(|| {
            names
                .iter()
                .map(|(name, schema)| (name.clone(), (*schema).clone()))
                .collect()
        });
    let writer_record = match writer_schema {
        AvroSchema::Record(record) => Some(record),
        _ => None,
    };
    let columns = value_fields
        .iter()
        .map(|value_field| {
            if value_field.name == crate::connectors::data_format::METADATA_FIELD_NAME {
                return ColumnPlan::Metadata;
            }
            let position = writer_record
                .and_then(|record| record.lookup.get(&value_field.name))
                .copied();
            match position {
                Some(position) => ColumnPlan::Field {
                    position,
                    json_as_text: writer_declares_json_text(
                        writer_record.map(|record| &record.fields[position].schema),
                    ),
                },
                None => ColumnPlan::Absent,
            }
        })
        .collect();
    ProjectionPlan {
        columns,
        decimal_names,
    }
}

impl AvroParser {
    pub fn new(
        value_fields: Vec<ValueField>,
        key_field_names: Option<&[String]>,
        schema_provider: Option<Box<dyn AvroSchemaProvider>>,
        explicit_schema: bool,
    ) -> Result<AvroParser, AvroError> {
        // The `_metadata` column is filled by the connector, not decoded from
        // the payload, so it stays out of the reader schema.
        let payload_fields: Vec<ValueField> = value_fields
            .iter()
            .filter(|field| field.name != super::METADATA_FIELD_NAME)
            .cloned()
            .collect();
        let has_metadata_column = payload_fields.len() != value_fields.len();
        let reader_schema = Arc::new(generate_avro_schema(&payload_fields)?);
        let key_field_positions = key_field_names
            .map(|names| {
                names
                    .iter()
                    .map(|name| {
                        value_fields
                            .iter()
                            .position(|field| &field.name == name)
                            .ok_or_else(|| AvroError::UnknownKeyField {
                                field: name.clone(),
                            })
                    })
                    .collect::<Result<Vec<usize>, AvroError>>()
            })
            .transpose()?;
        Ok(AvroParser {
            reader_schema,
            value_fields,
            key_field_positions,
            schema_provider,
            current_schema_id: None,
            metadata_column_value: Value::None,
            has_metadata_column,
            explicit_schema,
            projection_plans: HashMap::new(),
        })
    }

    fn writer_schema(&mut self) -> Result<Arc<AvroSchema>, AvroParseError> {
        let Some(provider) = self.schema_provider.as_mut() else {
            // Without a registry the payloads are decoded directly with the
            // reader schema.
            return Ok(self.reader_schema.clone());
        };
        let Some(schema_id) = self.current_schema_id.as_ref() else {
            if self.explicit_schema {
                // The message was produced without a schema; the explicit
                // table schema doubles as the decoding schema — the user
                // vouches for the topic layout.
                return Ok(self.reader_schema.clone());
            }
            return Err(AvroParseError::NoSchemaVersion);
        };
        provider
            .get_schema(schema_id)
            .map_err(|message| AvroParseError::SchemaLookup {
                schema_id: schema_id.clone(),
                message,
            })
    }

    /// The cache key of the writer schema of the current message: its
    /// version, or the empty key for the version-less (reader-schema)
    /// decoding.
    fn cache_key(&self) -> &[u8] {
        self.current_schema_id.as_deref().unwrap_or(&[])
    }

    fn projection_plan(&mut self, writer_schema: &AvroSchema) -> Arc<ProjectionPlan> {
        if let Some(plan) = self.projection_plans.get(self.cache_key()) {
            return plan.clone();
        }
        let plan = Arc::new(build_projection_plan(&self.value_fields, writer_schema));
        self.projection_plans
            .insert(self.cache_key().to_vec(), plan.clone());
        plan
    }

    fn parse_payload(&mut self, payload: &[u8]) -> Result<DecodedRecord, AvroParseError> {
        let writer_schema = self.writer_schema()?;
        let plan = self.projection_plan(&writer_schema);
        let mut reader = payload;
        let decoded = apache_avro::from_avro_datum(&writer_schema, &mut reader, None)
            .map_err(AvroParseError::Decoding)?;
        let decoded = match &plan.decimal_names {
            Some(names) => normalize_decimals(decoded, &writer_schema, names),
            None => decoded,
        };
        let AvroValue::Record(fields) = decoded else {
            return Err(AvroParseError::NotARecord);
        };
        Ok((fields, plan))
    }
}

/// The decoded fields of one record, together with the decoding plan of the
/// writer schema they were decoded with.
type DecodedRecord = (Vec<(String, AvroValue)>, Arc<ProjectionPlan>);

/// Whether the writer declares the field as JSON text: a flat `"string"`, or
/// its nullable `["null", "string"]` form — the shapes the formatter writes
/// the `Json` columns as (see `engine_type_to_avro`), possibly inside
/// arrays. A string from a wider union is the producer's deliberate string
/// branch and must stay a JSON string value, not be parsed as JSON text —
/// otherwise `"42"` from `["string", "long"]` would silently turn into the
/// number 42.
fn writer_declares_json_text(schema: Option<&AvroSchema>) -> bool {
    match schema {
        // Without the writer schema context, assume the Pathway shape.
        Some(AvroSchema::String) | None => true,
        Some(AvroSchema::Union(union)) => {
            let mut non_null = union
                .variants()
                .iter()
                .filter(|variant| !matches!(variant, AvroSchema::Null));
            matches!(
                (non_null.next(), non_null.next()),
                (Some(AvroSchema::String), None)
            )
        }
        Some(AvroSchema::Array(array)) => writer_declares_json_text(Some(&array.items)),
        Some(_) => false,
    }
}

/// The named types of a schema, for resolving the `Schema::Ref` reuse of a
/// named type during the decimal walks. Empty when the resolution fails —
/// the walk then simply does not descend into the references.
fn schema_names(schema: &AvroSchema) -> apache_avro::schema::NamesRef<'_> {
    apache_avro::schema::ResolvedSchema::try_from(schema)
        .map(|resolved| resolved.get_names().clone())
        .unwrap_or_default()
}

/// The lookup form the decimal walks use: either the borrowed names of a
/// freshly resolved schema (at plan build) or the owned copy the plan keeps.
trait NamedSchemas {
    fn get_schema(&self, name: &apache_avro::schema::Name) -> Option<&AvroSchema>;
}

impl NamedSchemas for apache_avro::schema::NamesRef<'_> {
    fn get_schema(&self, name: &apache_avro::schema::Name) -> Option<&AvroSchema> {
        self.get(name).copied()
    }
}

impl NamedSchemas for HashMap<apache_avro::schema::Name, AvroSchema> {
    fn get_schema(&self, name: &apache_avro::schema::Name) -> Option<&AvroSchema> {
        self.get(name)
    }
}

/// Whether the schema contains a decimal anywhere: guards the (rare)
/// normalization walk off the hot path. Follows the named-type references
/// through `names`, guarding against the recursive schemas with `visited`.
fn schema_contains_decimal(
    schema: &AvroSchema,
    names: &impl NamedSchemas,
    visited: &mut Vec<apache_avro::schema::Name>,
) -> bool {
    match schema {
        AvroSchema::Decimal(_) => true,
        AvroSchema::Record(record) => record
            .fields
            .iter()
            .any(|field| schema_contains_decimal(&field.schema, names, visited)),
        AvroSchema::Array(array) => schema_contains_decimal(&array.items, names, visited),
        AvroSchema::Map(map) => schema_contains_decimal(&map.types, names, visited),
        AvroSchema::Union(union) => union
            .variants()
            .iter()
            .any(|variant| schema_contains_decimal(variant, names, visited)),
        AvroSchema::Ref { name } => {
            if visited.contains(name) {
                return false;
            }
            visited.push(name.clone());
            names
                .get_schema(name)
                .is_some_and(|resolved| schema_contains_decimal(resolved, names, visited))
        }
        _ => false,
    }
}

/// Rewrites every decoded decimal in the value into its exact string form,
/// guided by the schema: the value itself carries only the unscaled integer,
/// while the scale lives in the schema node. Walks records, arrays, maps,
/// unions and the named-type references, so the decimals nested in the
/// composite fields are covered too.
fn normalize_decimals(
    value: AvroValue,
    schema: &AvroSchema,
    names: &impl NamedSchemas,
) -> AvroValue {
    match (value, schema) {
        (AvroValue::Decimal(decimal), AvroSchema::Decimal(decimal_schema)) => {
            // The scale is broker-supplied and unvalidated: an absurd one
            // must not panic (or allocate an absurd string) — the value is
            // then left as it came, and the projection reports it as an
            // incompatible per-row error like any other undecodable value.
            match i64::try_from(decimal_schema.scale) {
                Ok(scale) if decimal_schema.scale <= MAX_RENDERED_DECIMAL_SCALE => {
                    let unscaled = BigInt::from(decimal);
                    AvroValue::String(BigDecimal::new(unscaled, scale).to_string())
                }
                _ => AvroValue::Decimal(decimal),
            }
        }
        (value, AvroSchema::Ref { name }) => match names.get_schema(name) {
            // The value itself is finite, so the recursion terminates even
            // for the recursive named types.
            Some(resolved) => normalize_decimals(value, resolved, names),
            None => value,
        },
        (AvroValue::Union(position, inner), AvroSchema::Union(union)) => {
            match union.variants().get(position as usize) {
                Some(variant) => AvroValue::Union(
                    position,
                    Box::new(normalize_decimals(*inner, variant, names)),
                ),
                None => AvroValue::Union(position, inner),
            }
        }
        (AvroValue::Record(fields), AvroSchema::Record(record)) => AvroValue::Record(
            fields
                .into_iter()
                .map(|(name, value)| {
                    let field_schema = record
                        .fields
                        .iter()
                        .find(|field| field.name == name)
                        .map(|field| &field.schema);
                    match field_schema {
                        Some(schema) => (name, normalize_decimals(value, schema, names)),
                        None => (name, value),
                    }
                })
                .collect(),
        ),
        (AvroValue::Array(elements), AvroSchema::Array(array)) => AvroValue::Array(
            elements
                .into_iter()
                .map(|element| normalize_decimals(element, &array.items, names))
                .collect(),
        ),
        (AvroValue::Map(map), AvroSchema::Map(map_schema)) => AvroValue::Map(
            map.into_iter()
                .map(|(key, value)| (key, normalize_decimals(value, &map_schema.types, names)))
                .collect(),
        ),
        (value, _) => value,
    }
}

/// Converts a decoded Avro value — as the writer schema encoded it — into
/// the engine value of the declared column type. This is where the lenient
/// side of the conversion table lives: integer and float width promotions,
/// every timestamp precision, enums and uuids read as strings, fixed read as
/// bytes.
fn avro_value_to_engine(
    value: AvroValue,
    type_: &Type,
    field: &str,
    json_as_text: bool,
) -> Result<Value, AvroParseError> {
    let incompatible = || AvroParseError::IncompatibleValue {
        field: field.to_string(),
        type_: type_.clone(),
    };
    if let Type::Optional(inner) = type_ {
        return match value {
            AvroValue::Null => Ok(Value::None),
            AvroValue::Union(_, boxed) => match *boxed {
                AvroValue::Null => Ok(Value::None),
                inner_value => avro_value_to_engine(inner_value, inner, field, json_as_text),
            },
            other => avro_value_to_engine(other, inner, field, json_as_text),
        };
    }
    // A resolved non-optional value may still arrive wrapped in a union when
    // the writer declared one.
    if let AvroValue::Union(_, boxed) = value {
        return avro_value_to_engine(*boxed, type_, field, json_as_text);
    }
    let engine_value = match (type_, value) {
        (Type::Bool, AvroValue::Boolean(b)) => Value::Bool(b),
        (Type::Int, AvroValue::Long(i)) => Value::Int(i),
        (Type::Int, AvroValue::Int(i)) => Value::Int(i.into()),
        (Type::Float, AvroValue::Double(f)) => Value::Float(f.into()),
        (Type::Float, AvroValue::Float(f)) => Value::Float(f64::from(f).into()),
        // The integer-to-float promotions the Avro schema resolution would
        // perform: with the resolution bypassed (see `AvroParser`), they are
        // this conversion's job.
        #[allow(clippy::cast_precision_loss)]
        (Type::Float, AvroValue::Long(l)) => Value::Float((l as f64).into()),
        (Type::Float, AvroValue::Int(i)) => Value::Float(f64::from(i).into()),
        (Type::String, AvroValue::String(s)) => Value::String(s.into()),
        (Type::String, AvroValue::Enum(_, symbol)) => Value::String(symbol.into()),
        (Type::String, AvroValue::Uuid(uuid)) => Value::String(uuid.to_string().into()),
        (Type::String, AvroValue::BigDecimal(decimal)) => Value::String(decimal.to_string().into()),
        // The inverse of the write-side Pointer serialization (see
        // `engine_type_to_avro`), completing the roundtrip.
        (Type::Pointer, AvroValue::String(s)) => {
            crate::engine::value::parse_pathway_pointer(&s).map_err(|_| incompatible())?
        }
        (Type::Bytes, AvroValue::Bytes(b) | AvroValue::Fixed(_, b)) => Value::Bytes(b.into()),
        // A string read into a Json column is parsed as JSON text only when
        // the writer declares the field as one (`json_as_text` — the shape
        // the Pathway formatter writes the Json columns in, see
        // `writer_declares_json_text`). A string branch of a wider union is
        // the producer's deliberate choice and stays a JSON string value —
        // otherwise "42" from ["string", "long"] would silently become the
        // number 42. Everything else — nested records, maps, multi-branch
        // unions, which the deduction types as Json — converts structurally.
        (Type::Json, AvroValue::String(s)) => {
            if json_as_text {
                let json: serde_json::Value =
                    serde_json::from_str(&s).map_err(|_| incompatible())?;
                Value::from(json)
            } else {
                Value::from(serde_json::Value::String(s))
            }
        }
        (Type::Json, value) => Value::from(avro_value_to_json(value).ok_or_else(incompatible)?),
        (Type::DateTimeUtc, AvroValue::TimestampMicros(us)) => Value::DateTimeUtc(
            DateTimeUtc::new(us.checked_mul(1000).ok_or_else(incompatible)?),
        ),
        (Type::DateTimeUtc, AvroValue::TimestampMillis(ms)) => Value::DateTimeUtc(
            DateTimeUtc::new(ms.checked_mul(1_000_000).ok_or_else(incompatible)?),
        ),
        (Type::DateTimeUtc, AvroValue::TimestampNanos(ns)) => {
            Value::DateTimeUtc(DateTimeUtc::new(ns))
        }
        (Type::DateTimeNaive, AvroValue::LocalTimestampMicros(us)) => Value::DateTimeNaive(
            DateTimeNaive::new(us.checked_mul(1000).ok_or_else(incompatible)?),
        ),
        (Type::DateTimeNaive, AvroValue::LocalTimestampMillis(ms)) => Value::DateTimeNaive(
            DateTimeNaive::new(ms.checked_mul(1_000_000).ok_or_else(incompatible)?),
        ),
        (Type::DateTimeNaive, AvroValue::LocalTimestampNanos(ns)) => {
            Value::DateTimeNaive(DateTimeNaive::new(ns))
        }
        (Type::DateTimeNaive, AvroValue::Date(days)) => Value::DateTimeNaive(DateTimeNaive::new(
            i64::from(days)
                .checked_mul(86_400 * 1_000_000_000)
                .ok_or_else(incompatible)?,
        )),
        (Type::Duration, AvroValue::Long(us) | AvroValue::TimeMicros(us)) => Value::Duration(
            EngineDuration::new(us.checked_mul(1000).ok_or_else(incompatible)?),
        ),
        (Type::Duration, AvroValue::TimeMillis(ms)) => Value::Duration(EngineDuration::new(
            i64::from(ms)
                .checked_mul(1_000_000)
                .ok_or_else(incompatible)?,
        )),
        (Type::List(element_type), AvroValue::Array(elements)) => {
            avro_elements_to_engine(elements, element_type, field, json_as_text)?
        }
        (Type::Tuple(element_types), AvroValue::Array(elements)) => {
            let element_type = element_types.first().ok_or_else(incompatible)?;
            avro_elements_to_engine(elements, element_type, field, json_as_text)?
        }
        _ => return Err(incompatible()),
    };
    Ok(engine_value)
}

/// The elements of a decoded Avro array converted into an engine tuple:
/// the shared body of the `List` and `Tuple` column conversions.
fn avro_elements_to_engine(
    elements: Vec<AvroValue>,
    element_type: &Type,
    field: &str,
    json_as_text: bool,
) -> Result<Value, AvroParseError> {
    Ok(Value::Tuple(
        elements
            .into_iter()
            .map(|element| avro_value_to_engine(element, element_type, field, json_as_text))
            .collect::<Result<Arc<[_]>, _>>()?,
    ))
}

/// The structural JSON form of a decoded Avro value, for the columns typed
/// as Json — nested records, maps and multi-branch unions are deduced into
/// them. The logical time types keep their raw integer representation, and
/// bytes are base64-encoded (JSON cannot carry them natively). `None` marks
/// the values without a faithful JSON form: non-finite floats, unscaled
/// decimals and the calendar duration.
fn avro_value_to_json(value: AvroValue) -> Option<JsonValue> {
    let json = match value {
        AvroValue::Null => JsonValue::Null,
        AvroValue::Boolean(b) => json!(b),
        AvroValue::Int(i) | AvroValue::TimeMillis(i) | AvroValue::Date(i) => json!(i),
        AvroValue::Long(l)
        | AvroValue::TimeMicros(l)
        | AvroValue::TimestampMillis(l)
        | AvroValue::TimestampMicros(l)
        | AvroValue::TimestampNanos(l)
        | AvroValue::LocalTimestampMillis(l)
        | AvroValue::LocalTimestampMicros(l)
        | AvroValue::LocalTimestampNanos(l) => json!(l),
        AvroValue::Float(f) => JsonValue::Number(serde_json::Number::from_f64(f.into())?),
        AvroValue::Double(d) => JsonValue::Number(serde_json::Number::from_f64(d)?),
        AvroValue::String(s) => JsonValue::String(s),
        AvroValue::Enum(_, symbol) => JsonValue::String(symbol),
        AvroValue::Uuid(uuid) => JsonValue::String(uuid.to_string()),
        AvroValue::BigDecimal(decimal) => JsonValue::String(decimal.to_string()),
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => {
            JsonValue::String(general_purpose::STANDARD.encode(b))
        }
        AvroValue::Array(elements) => JsonValue::Array(
            elements
                .into_iter()
                .map(avro_value_to_json)
                .collect::<Option<Vec<_>>>()?,
        ),
        AvroValue::Map(map) => JsonValue::Object(
            map.into_iter()
                .map(|(key, value)| Some((key, avro_value_to_json(value)?)))
                .collect::<Option<_>>()?,
        ),
        AvroValue::Record(fields) => JsonValue::Object(
            fields
                .into_iter()
                .map(|(key, value)| Some((key, avro_value_to_json(value)?)))
                .collect::<Option<_>>()?,
        ),
        AvroValue::Union(_, inner) => avro_value_to_json(*inner)?,
        // The calendar duration is a triple of calendar units; its faithful
        // JSON form is the structure itself.
        AvroValue::Duration(duration) => json!({
            "months": u32::from(duration.months()),
            "days": u32::from(duration.days()),
            "milliseconds": u32::from(duration.millis()),
        }),
        AvroValue::Decimal(_) => return None,
    };
    Some(json)
}

impl super::Parser for AvroParser {
    fn parse(&mut self, data: &super::ReaderContext) -> super::ParseResult {
        use super::ReaderContext::{Empty, KeyValue, RawBytes};
        let (event, payload) = match data {
            KeyValue((_key, Some(payload))) => {
                (crate::connectors::DataEventType::Insert, payload.as_slice())
            }
            RawBytes(event, payload) => (*event, payload.as_slice()),
            // A tombstone carries no payload to decode; the message-queue
            // reads run in the Native session, where no deletion semantics
            // apply to it.
            KeyValue((_, None)) | Empty => return Ok(vec![]),
            _ => return Err(super::ParseError::UnsupportedReaderContext.into()),
        };
        let (decoded_fields, plan) = self
            .parse_payload(payload)
            .map_err(|e| DynError::from(super::ParseError::Avro(e)))?;
        // Taking the values out of the slots avoids cloning the decoded
        // Avro values (which may be deep structures) on the hot path; the
        // decoded record's fields sit in the writer schema order the plan
        // was built against.
        let mut slots: Vec<Option<AvroValue>> = decoded_fields
            .into_iter()
            .map(|(_, value)| Some(value))
            .collect();
        let mut values: Vec<DynResult<Value>> = Vec::with_capacity(self.value_fields.len());
        for (value_field, column_plan) in self.value_fields.iter().zip(&plan.columns) {
            let taken = match column_plan {
                ColumnPlan::Metadata => {
                    values.push(Ok(self.metadata_column_value.clone()));
                    continue;
                }
                ColumnPlan::Field {
                    position,
                    json_as_text,
                } => slots
                    .get_mut(*position)
                    .and_then(Option::take)
                    .map(|avro_value| (avro_value, *json_as_text)),
                ColumnPlan::Absent => None,
            };
            let value = match taken {
                Some((avro_value, json_as_text)) => avro_value_to_engine(
                    avro_value,
                    &value_field.type_,
                    &value_field.name,
                    json_as_text,
                ),
                // See `ColumnPlan::Absent`.
                None => match &value_field.default {
                    Some(default) => Ok(default.clone()),
                    None if matches!(value_field.type_, Type::Optional(_)) => Ok(Value::None),
                    None => Err(AvroParseError::FieldMissing {
                        field: value_field.name.clone(),
                    }),
                },
            };
            values.push(value.map_err(|e| DynError::from(super::ParseError::Avro(e))));
        }
        let key = self.key_field_positions.as_ref().map(|positions| {
            let mut key_values = Vec::with_capacity(positions.len());
            for position in positions {
                match &values[*position] {
                    Ok(value) => key_values.push(value.clone()),
                    Err(_) => {
                        return Err(DynError::from(super::ParseError::Avro(
                            AvroParseError::KeyFieldUnparsable {
                                field: self.value_fields[*position].name.clone(),
                            },
                        )))
                    }
                }
            }
            Ok(key_values)
        });
        Ok(vec![super::ParsedEventWithErrors::new(
            self.session_type(),
            event,
            key,
            values,
        )])
    }

    fn on_new_source_started(&mut self, metadata: &crate::connectors::metadata::SourceMetadata) {
        self.current_schema_id = metadata.schema_id();
        if self.has_metadata_column {
            self.metadata_column_value = metadata.serialize().into();
        }
    }

    fn column_count(&self) -> usize {
        self.value_fields.len()
    }

    fn needs_source_metadata(&self) -> bool {
        // The writer schema of every message is resolved by the version the
        // source metadata carries. Without a schema provider the reader
        // schema decodes everything, and the metadata is not needed.
        self.schema_provider.is_some()
    }

    fn short_description(&self) -> Cow<'static, str> {
        "avro".into()
    }
}

/// How the payloads described by an Avro-grammar registry schema are
/// actually encoded on the wire. The same schema deduces into different
/// column types depending on it: the JSON payloads are read by the JSON
/// parser, which takes durations as raw nanosecond numbers and datetimes as
/// formatted strings — so deducing the Avro logical time types into the
/// engine time types there would misread the payload numbers by orders of
/// magnitude, or reject them outright. The deduction table must match the
/// parser that will actually read the rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadEncoding {
    AvroDatum,
    Json,
}

/// Maps an Avro schema node onto the engine type it is read as, per the
/// documented conversion table. Everything the engine cannot represent
/// structurally — nested records, maps, multi-branch unions, the calendar
/// `duration` — lands in `Json`; `decimal` becomes its exact string form.
/// With the [`PayloadEncoding::Json`] encoding the logical time types map to
/// plain integers: the raw numbers the JSON payloads carry.
fn avro_schema_to_engine_type(
    schema: &AvroSchema,
    encoding: PayloadEncoding,
) -> Result<Type, AvroError> {
    let type_ = match schema {
        AvroSchema::Boolean => Type::Bool,
        AvroSchema::Int | AvroSchema::Long => Type::Int,
        AvroSchema::Float | AvroSchema::Double => Type::Float,
        // Decimals map to their exact string form: turning them into
        // floats would silently lose precision.
        AvroSchema::String
        | AvroSchema::Uuid
        | AvroSchema::Enum(_)
        | AvroSchema::Decimal(_)
        | AvroSchema::BigDecimal => Type::String,
        AvroSchema::Bytes | AvroSchema::Fixed(_) => Type::Bytes,
        AvroSchema::TimestampMillis
        | AvroSchema::TimestampMicros
        | AvroSchema::TimestampNanos
        | AvroSchema::LocalTimestampMillis
        | AvroSchema::LocalTimestampMicros
        | AvroSchema::LocalTimestampNanos
        | AvroSchema::Date
        | AvroSchema::TimeMillis
        | AvroSchema::TimeMicros
            if encoding == PayloadEncoding::Json =>
        {
            Type::Int
        }
        AvroSchema::TimestampMillis | AvroSchema::TimestampMicros | AvroSchema::TimestampNanos => {
            Type::DateTimeUtc
        }
        AvroSchema::LocalTimestampMillis
        | AvroSchema::LocalTimestampMicros
        | AvroSchema::LocalTimestampNanos
        | AvroSchema::Date => Type::DateTimeNaive,
        AvroSchema::TimeMillis | AvroSchema::TimeMicros => Type::Duration,
        AvroSchema::Array(array) => {
            Type::List(avro_schema_to_engine_type(&array.items, encoding)?.into())
        }
        AvroSchema::Union(union) => {
            let variants = union.variants();
            let non_null: Vec<&AvroSchema> = variants
                .iter()
                .filter(|variant| !matches!(variant, AvroSchema::Null))
                .collect();
            let has_null = non_null.len() != variants.len();
            match non_null.as_slice() {
                [] => {
                    return Err(AvroError::UnsupportedType {
                        field: String::new(),
                        type_: Type::Any,
                    })
                }
                [single] => {
                    let inner = avro_schema_to_engine_type(single, encoding)?;
                    if has_null {
                        Type::Optional(inner.into())
                    } else {
                        inner
                    }
                }
                // A union of several concrete branches has no engine
                // counterpart; its values are only representable as JSON.
                _ if has_null => Type::Optional(Arc::new(Type::Json)),
                _ => Type::Json,
            }
        }
        // Nested records, maps and the calendar duration carry structure the
        // engine models as JSON.
        AvroSchema::Record(_) | AvroSchema::Map(_) | AvroSchema::Duration => Type::Json,
        // A reference to a named type declared elsewhere in the schema:
        // resolving it would need the whole-schema context the per-field
        // mapping does not carry.
        AvroSchema::Ref { name } => {
            return Err(AvroError::UnresolvedRef {
                name: name.to_string(),
            })
        }
        AvroSchema::Null => {
            return Err(AvroError::UnsupportedType {
                field: String::new(),
                type_: Type::Any,
            })
        }
    };
    Ok(type_)
}

/// The non-null branch of a nullable-union schema (the shape the defaults
/// of the optional fields are declared against); other schemas pass
/// through.
fn nullable_inner(schema: &AvroSchema) -> &AvroSchema {
    if let AvroSchema::Union(union) = schema {
        if let Some(inner) = union
            .variants()
            .iter()
            .find(|variant| !matches!(variant, AvroSchema::Null))
        {
            return inner;
        }
    }
    schema
}

/// The elements of an array default converted into an engine tuple: the
/// shared body of the `List` and `Tuple` default conversions.
fn avro_default_elements(
    elements: &[JsonValue],
    element_type: &Type,
    schema: &AvroSchema,
) -> Option<Value> {
    let element_schema = match nullable_inner(schema) {
        AvroSchema::Array(array) => &array.items,
        _ => return None,
    };
    elements
        .iter()
        .map(|element| avro_default_to_engine_value(element, element_type, element_schema))
        .collect::<Option<Arc<[Value]>>>()
        .map(Value::Tuple)
}

/// The engine value of an Avro field default, for the defaults the engine can
/// carry; the unrepresentable ones are simply not propagated. A dropped
/// default degrades the evolution story (the pre-evolution messages fail
/// with a missing field despite the schema declaring them readable), so the
/// conversion covers the composite and the logical-time defaults too. The
/// `schema` node supplies what the engine type alone cannot: the unit of a
/// numeric time default (both millis and micros deduce into one engine
/// type).
fn avro_default_to_engine_value(
    default: &JsonValue,
    type_: &Type,
    schema: &AvroSchema,
) -> Option<Value> {
    match (type_.unoptionalize(), default) {
        (_, JsonValue::Null) => Some(Value::None),
        (Type::Bool, JsonValue::Bool(b)) => Some(Value::Bool(*b)),
        (Type::Int, JsonValue::Number(n)) => n.as_i64().map(Value::Int),
        (Type::Float, JsonValue::Number(n)) => n.as_f64().map(|f| Value::Float(f.into())),
        // A decimal default is a byte string (ISO-8859-1 code points) of
        // the unscaled two's-complement integer: rendered through the
        // schema's scale, it matches the exact string form the decoded
        // values take.
        (Type::String, JsonValue::String(s))
            if matches!(nullable_inner(schema), AvroSchema::Decimal(_)) =>
        {
            let AvroSchema::Decimal(decimal_schema) = nullable_inner(schema) else {
                return None;
            };
            if decimal_schema.scale > MAX_RENDERED_DECIMAL_SCALE {
                return None;
            }
            let scale = i64::try_from(decimal_schema.scale).ok()?;
            let bytes = s
                .chars()
                .map(|ch| u8::try_from(u32::from(ch)).ok())
                .collect::<Option<Vec<u8>>>()?;
            let unscaled = BigInt::from_signed_bytes_be(&bytes);
            Some(Value::String(
                BigDecimal::new(unscaled, scale).to_string().into(),
            ))
        }
        (Type::String, JsonValue::String(s)) => Some(Value::String(s.as_str().into())),
        // Avro encodes a bytes/fixed default as a string whose code points
        // are the byte values (ISO-8859-1).
        (Type::Bytes, JsonValue::String(s)) => s
            .chars()
            .map(|ch| u8::try_from(u32::from(ch)).ok())
            .collect::<Option<Vec<u8>>>()
            .map(|bytes| Value::Bytes(bytes.into())),
        (Type::DateTimeUtc, JsonValue::Number(n)) => {
            let raw = n.as_i64()?;
            let nanoseconds = match nullable_inner(schema) {
                AvroSchema::TimestampMillis => raw.checked_mul(1_000_000)?,
                AvroSchema::TimestampMicros => raw.checked_mul(1_000)?,
                AvroSchema::TimestampNanos => raw,
                _ => return None,
            };
            Some(Value::DateTimeUtc(DateTimeUtc::new(nanoseconds)))
        }
        (Type::DateTimeNaive, JsonValue::Number(n)) => {
            let raw = n.as_i64()?;
            let nanoseconds = match nullable_inner(schema) {
                AvroSchema::LocalTimestampMillis => raw.checked_mul(1_000_000)?,
                AvroSchema::LocalTimestampMicros => raw.checked_mul(1_000)?,
                AvroSchema::LocalTimestampNanos => raw,
                AvroSchema::Date => raw.checked_mul(86_400 * 1_000_000_000)?,
                _ => return None,
            };
            Some(Value::DateTimeNaive(DateTimeNaive::new(nanoseconds)))
        }
        (Type::Duration, JsonValue::Number(n)) => {
            let raw = n.as_i64()?;
            let nanoseconds = match nullable_inner(schema) {
                AvroSchema::TimeMillis => raw.checked_mul(1_000_000)?,
                // A plain long is the Pathway-written microsecond duration
                // (see `DURATION_TYPE_MARKER`) — the same unit `time-micros`
                // uses.
                AvroSchema::TimeMicros | AvroSchema::Long => raw.checked_mul(1_000)?,
                _ => return None,
            };
            Some(Value::Duration(EngineDuration::new(nanoseconds)))
        }
        (Type::Json, default) => Some(Value::from(default.clone())),
        (Type::List(element_type), JsonValue::Array(elements)) => {
            avro_default_elements(elements, element_type, schema)
        }
        (Type::Tuple(element_types), JsonValue::Array(elements)) => {
            avro_default_elements(elements, element_types.first()?, schema)
        }
        _ => None,
    }
}

/// Deduces the columns of a table from the JSON of an Avro record schema —
/// the registry-driven half of `schema=None`. A non-record schema (a topic
/// typed with a primitive) is rejected: the parser reads record datums only,
/// so deducing a column for it would build a table no message can enter.
pub fn avro_schema_to_explored_fields(
    schema_json: &str,
    encoding: PayloadEncoding,
) -> Result<Vec<ExploredField>, AvroError> {
    let schema = AvroSchema::parse_str(schema_json).map_err(AvroError::SchemaConstruction)?;
    let AvroSchema::Record(record) = &schema else {
        return Err(AvroError::NonRecordSchema);
    };
    let mut fields = Vec::with_capacity(record.fields.len());
    for record_field in &record.fields {
        let mut type_ =
            avro_schema_to_engine_type(&record_field.schema, encoding).map_err(|e| match e {
                AvroError::UnsupportedType { type_, .. } => AvroError::UnsupportedType {
                    field: record_field.name.clone(),
                    type_,
                },
                other => other,
            })?;
        // The marker the generated schemas attach to the types whose Avro
        // encoding alone cannot express them (see `PATHWAY_TYPE_ATTRIBUTE`):
        // a long marked as a Duration is deduced back into one. Restricted
        // to the Avro-encoded payloads: the JSON parser reads Duration
        // columns in different units, and Pathway registers such markers
        // for the Avro format only.
        let duration_marked = encoding == PayloadEncoding::AvroDatum
            && record_field
                .custom_attributes
                .get(PATHWAY_TYPE_ATTRIBUTE)
                .and_then(|value| value.as_str())
                == Some(DURATION_TYPE_MARKER);
        if duration_marked {
            type_ = match type_ {
                Type::Int => Type::Duration,
                Type::Optional(inner) if *inner == Type::Int => {
                    Type::Optional(Type::Duration.into())
                }
                other => other,
            };
        }
        let default = record_field.default.as_ref().and_then(|default| {
            avro_default_to_engine_value(default, &type_, &record_field.schema)
        });
        fields.push(ExploredField {
            name: record_field.name.clone(),
            type_,
            default,
            doc: record_field.doc.clone(),
        });
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::data_format::{FieldSource, Parser};
    use crate::connectors::ReaderContext;

    /// A provider that never yields a schema: the availability concerns
    /// (retrying, caching, deciding what is final) belong to the storage
    /// that implements it, so the parser only sees the verdict.
    struct FailingProvider;

    impl AvroSchemaProvider for FailingProvider {
        fn get_schema(&mut self, _schema_id: &[u8]) -> Result<Arc<AvroSchema>, String> {
            Err("the registry refused the lookup".to_string())
        }
    }

    fn int_field(name: &str) -> ValueField {
        ValueField::new(name.to_string(), Type::Int, FieldSource::Payload)
    }

    fn encoded_row(schema: &AvroSchema, value: i64) -> Vec<u8> {
        to_avro_datum(
            schema,
            AvroValue::Record(vec![("x".to_string(), AvroValue::Long(value))]),
        )
        .expect("the test datum encodes")
    }

    #[test]
    fn json_payload_deduction_maps_logical_time_types_to_integers() {
        let schema_json = r#"{"type":"record","name":"R","fields":[
            {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
            {"name":"t","type":{"type":"int","logicalType":"time-millis"}}]}"#;
        let avro = avro_schema_to_explored_fields(schema_json, PayloadEncoding::AvroDatum).unwrap();
        assert_eq!(avro[0].type_, Type::DateTimeUtc);
        assert_eq!(avro[1].type_, Type::Duration);
        // The JSON payloads carry the raw numbers and are read by the JSON
        // parser, whose time representations differ — so the deduction maps
        // the logical time types to plain integers there.
        let json = avro_schema_to_explored_fields(schema_json, PayloadEncoding::Json).unwrap();
        assert_eq!(json[0].type_, Type::Int);
        assert_eq!(json[1].type_, Type::Int);
    }

    #[test]
    fn logical_time_defaults_are_deduced_with_their_units() {
        use crate::engine::time::{DateTimeNaive, DateTimeUtc};
        let schema_json = r#"{"type":"record","name":"R","fields":[
            {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"},"default":1},
            {"name":"day","type":{"type":"int","logicalType":"date"},"default":1}]}"#;
        let fields =
            avro_schema_to_explored_fields(schema_json, PayloadEncoding::AvroDatum).unwrap();
        // The unit comes from the schema node: a microsecond timestamp
        // default of 1 is 1000 ns, a date default of 1 is one day.
        assert_eq!(
            fields[0].default,
            Some(Value::DateTimeUtc(DateTimeUtc::new(1_000)))
        );
        assert_eq!(
            fields[1].default,
            Some(Value::DateTimeNaive(DateTimeNaive::new(
                86_400 * 1_000_000_000
            )))
        );
    }

    #[test]
    fn union_strings_stay_strings_in_json_columns() {
        let flat = AvroSchema::parse_str(r#""string""#).unwrap();
        assert!(writer_declares_json_text(Some(&flat)));
        let nullable = AvroSchema::parse_str(r#"["null","string"]"#).unwrap();
        assert!(writer_declares_json_text(Some(&nullable)));
        let multi = AvroSchema::parse_str(r#"["string","long"]"#).unwrap();
        assert!(!writer_declares_json_text(Some(&multi)));

        // A string branch of a wider union keeps its string identity, while
        // the flat-string (Pathway roundtrip) form parses the JSON text.
        let value = AvroValue::String("42".to_string());
        let as_branch = avro_value_to_engine(value.clone(), &Type::Json, "f", false).unwrap();
        assert_eq!(
            as_branch,
            Value::from(serde_json::Value::String("42".to_string()))
        );
        let as_text = avro_value_to_engine(value, &Type::Json, "f", true).unwrap();
        assert_eq!(as_text, Value::from(serde_json::json!(42)));
    }

    #[test]
    fn pointer_columns_survive_the_roundtrip() {
        let pointer = Value::Pointer(crate::engine::Key::for_value(&Value::Int(7)));
        let encoded = engine_value_to_avro(&pointer, &Type::Pointer, "p").unwrap();
        assert!(matches!(encoded, AvroValue::String(_)));
        let decoded = avro_value_to_engine(encoded, &Type::Pointer, "p", true).unwrap();
        assert_eq!(decoded, pointer);
    }

    #[test]
    fn calendar_durations_convert_into_json_structures() {
        use apache_avro::{Days, Duration as CalendarDuration, Millis, Months};
        let value = AvroValue::Duration(CalendarDuration::new(
            Months::new(1),
            Days::new(2),
            Millis::new(3),
        ));
        let converted = avro_value_to_engine(value, &Type::Json, "d", false).unwrap();
        assert_eq!(
            converted,
            Value::from(serde_json::json!({"months": 1, "days": 2, "milliseconds": 3}))
        );
    }

    #[test]
    fn absurd_decimal_scales_do_not_panic_the_normalization() {
        let schema = AvroSchema::parse_str(
            r#"{"type":"bytes","logicalType":"decimal","precision":30000,"scale":20000}"#,
        )
        .unwrap();
        let value = AvroValue::Decimal(apache_avro::Decimal::from(vec![9u8]));
        let names: HashMap<apache_avro::schema::Name, AvroSchema> = HashMap::new();
        // Over the rendering cap: the value stays a decimal and is reported
        // as an incompatible per-row error downstream, instead of a panic
        // or an absurd allocation here.
        let normalized = normalize_decimals(value, &schema, &names);
        assert!(matches!(normalized, AvroValue::Decimal(_)));
    }

    #[test]
    fn decimal_defaults_render_through_the_schema_scale() {
        // Tested through the conversion directly: apache-avro validates the
        // record-level decimal defaults so strictly that they rarely parse,
        // but the registry schemas of other ecosystems may still carry them.
        let schema = AvroSchema::parse_str(
            r#"{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}"#,
        )
        .unwrap();
        let default = JsonValue::String("d".to_string());
        // The default byte string "d" is the unscaled integer 0x64 = 100:
        // with the scale of 2 it reads "1.00", matching the decoded values'
        // form.
        assert_eq!(
            avro_default_to_engine_value(&default, &Type::String, &schema),
            Some(Value::String("1.00".into()))
        );
    }

    #[test]
    fn an_unobtainable_writer_schema_fails_the_row() {
        let value_fields = vec![int_field("x")];
        let schema = generate_avro_schema(&value_fields).unwrap();
        let mut parser =
            AvroParser::new(value_fields, None, Some(Box::new(FailingProvider)), false).unwrap();
        parser.current_schema_id = Some(vec![0; 8]);
        let payload = encoded_row(&schema, 7);
        let result = parser.parse(&ReaderContext::RawBytes(
            crate::connectors::DataEventType::Insert,
            payload,
        ));
        assert!(result.is_err());
    }
}

use itertools::Itertools;
use log::{error, info, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use native_tls::Error as NativeTlsError;
use native_tls::{Certificate, TlsConnector};
use ndarray::ArrayD;
use ordered_float::OrderedFloat;
use pg_walstream::{
    CancellationToken, ChangeEvent, EventType, LogicalReplicationStream, ReplicationSlotOptions,
    ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use postgres::types::ToSql;
use postgres::Client as PsqlClient;
use postgres::NoTls;
use postgres_native_tls::MakeTlsConnector;
use tokio::runtime::Runtime as TokioRuntime;
use uuid::Uuid;

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::{
    CommitPossibility, ConversionError, SqlQueryTemplate, TableWriterInitMode, ValuesMap,
};
use crate::connectors::metadata::PostgresMetadata;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError, Writer,
};
use crate::engine::value::parse_pathway_pointer;
use crate::engine::{DateTimeNaive, DateTimeUtc, Duration as EngineDuration, Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;

#[derive(Debug, thiserror::Error)]
pub enum SslError {
    #[error(transparent)]
    Tls(#[from] NativeTlsError),

    #[error("ssl certificate is not provided")]
    CertificateNotProvided,

    #[error("ssl mode is not expected in the raw connection string, it is deduced downstream")]
    UnexpectedSslMode,
}

const SSLMODE_PARAM: &str = "sslmode=";
const SSLMODE_DISABLE: &str = "sslmode=disable";
const SSLMODE_REQUIRE: &str = "sslmode=require";

#[derive(Debug, Clone, Copy)]
pub enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    pub fn connect(
        self,
        connection_string: &str,
        ssl_cert_path: Option<String>,
    ) -> Result<PsqlClient, WriteError> {
        if connection_string.contains(SSLMODE_PARAM) {
            return Err(WriteError::Ssl(SslError::UnexpectedSslMode));
        }

        match self {
            Self::Disable => Ok(PsqlClient::connect(
                &format!("{connection_string} {SSLMODE_DISABLE}"),
                NoTls,
            )?),
            Self::Allow => {
                if let Ok(c) =
                    PsqlClient::connect(&format!("{connection_string} {SSLMODE_DISABLE}"), NoTls)
                {
                    return Ok(c);
                }

                let tls = self.build_connector(None)?;
                Ok(PsqlClient::connect(
                    &format!("{connection_string} {SSLMODE_REQUIRE}"),
                    tls,
                )?)
            }
            Self::Prefer => {
                if let Ok(tls) = self.build_connector(None) {
                    if let Ok(c) =
                        PsqlClient::connect(&format!("{connection_string} {SSLMODE_REQUIRE}"), tls)
                    {
                        return Ok(c);
                    }
                }
                Ok(PsqlClient::connect(
                    &format!("{connection_string} {SSLMODE_DISABLE}"),
                    NoTls,
                )?)
            }
            Self::Require | Self::VerifyCa | Self::VerifyFull => {
                let tls = self.build_connector(ssl_cert_path)?;
                Ok(PsqlClient::connect(
                    &format!("{connection_string} {SSLMODE_REQUIRE}"),
                    tls,
                )?)
            }
        }
    }

    fn build_connector(
        self,
        ssl_cert_path: Option<String>,
    ) -> Result<MakeTlsConnector, WriteError> {
        let mut builder = TlsConnector::builder();
        match self {
            Self::Disable => unreachable!("Disable uses NoTls"),
            Self::Allow | Self::Prefer => {
                builder.danger_accept_invalid_certs(true);
                builder.danger_accept_invalid_hostnames(true);
            }
            Self::Require => {
                if let Some(path) = ssl_cert_path {
                    let cert = std::fs::read(path)?;
                    let cert = Certificate::from_pem(&cert).map_err(SslError::Tls)?;
                    builder.add_root_certificate(cert);
                } else {
                    builder.danger_accept_invalid_certs(true);
                    builder.danger_accept_invalid_hostnames(true);
                }
            }
            Self::VerifyCa | Self::VerifyFull => {
                let path =
                    ssl_cert_path.ok_or(WriteError::Ssl(SslError::CertificateNotProvided))?;
                let cert = std::fs::read(path)?;
                let cert = Certificate::from_pem(&cert).map_err(SslError::Tls)?;
                builder.add_root_certificate(cert);
                if matches!(self, Self::VerifyCa) {
                    builder.danger_accept_invalid_hostnames(true);
                }
            }
        }
        let connector = builder.build().map_err(SslError::Tls)?;
        Ok(MakeTlsConnector::new(connector))
    }
}

pub fn create_psql_client(
    connection_string: &str,
    ssl_mode: SslMode,
    ssl_cert_path: Option<String>,
) -> Result<PsqlClient, WriteError> {
    ssl_mode.connect(connection_string, ssl_cert_path)
}

pub struct PsqlWriter {
    client: PsqlClient,
    max_batch_size: Option<usize>,
    buffer: Vec<FormatterContext>,
    snapshot_mode: bool,
    table_name: String,
    query: SqlQueryTemplate,
    legacy_mode: bool,
}

mod to_sql {
    use std::error::Error;

    use bytes::{BufMut, BytesMut};
    use chrono::{DateTime, NaiveDateTime, Utc};
    use half::f16;
    use ndarray::ArrayD;
    use numpy::Ix1;
    use ordered_float::OrderedFloat;
    use pgvector::{HalfVector, Vector};
    use postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};

    use crate::engine::time::DateTime as _;
    use crate::engine::Value;

    #[derive(Debug, thiserror::Error)]
    enum ToSqlError {
        #[error("cannot convert value of type {pathway_type:?} to Postgres type {postgres_type}")]
        WrongPathwayType {
            pathway_type: String,
            postgres_type: Type,
        },

        #[error("array serialization error: {0}")]
        ArraySerialization(#[from] ArraySerializationError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ArraySerializationError {
        #[error("the number of shape dimensions does not fit in i32")]
        DimensionsOverflow,

        #[error("unknown OID for element type {0}")]
        UnknownOid(Type),

        #[error("serialized element length does not fit in i32")]
        ElementLengthOverflow,

        #[error("jagged (non-rectangular) nested array")]
        JaggedArray,

        #[error("array shape element does not fit in i32")]
        ShapeElementOverflow,
    }

    /// Recursively determines the shape (dimensions) of a nested `Value::Tuple`.
    /// Returns None if the array is not rectangular (jagged).
    fn tuple_shape(value: &Value) -> Option<Vec<usize>> {
        match value {
            Value::Tuple(elements) => {
                if elements.is_empty() {
                    return Some(vec![0]);
                }
                let len = elements.len();
                // Check if elements are themselves tuples (nested array)
                if let Value::Tuple(_) = &elements[0] {
                    let inner_shape = tuple_shape(&elements[0])?;
                    // Ensure all elements have the same inner shape
                    for elem in elements.iter().skip(1) {
                        let s = tuple_shape(elem)?;
                        if s != inner_shape {
                            return None; // Jagged - not rectangular
                        }
                    }
                    let mut shape = vec![len];
                    shape.extend(inner_shape);
                    Some(shape)
                } else {
                    Some(vec![len])
                }
            }
            _ => Some(vec![]), // Scalar leaf
        }
    }

    /// Flattens a (potentially nested) `Value::Tuple` into a flat list of leaf Values.
    fn flatten_tuple(value: &Value, out: &mut Vec<Value>) {
        match value {
            Value::Tuple(elements) => {
                for elem in elements.iter() {
                    flatten_tuple(elem, out);
                }
            }
            other => out.push(other.clone()),
        }
    }

    /// Returns the Postgres element OID for common scalar types.
    fn element_oid(ty: &Type) -> Option<u32> {
        Some(match ty {
            t if *t == Type::BOOL => 16,
            t if *t == Type::INT2 => 21,
            t if *t == Type::INT4 => 23,
            t if *t == Type::INT8 => 20,
            t if *t == Type::FLOAT4 => 700,
            t if *t == Type::FLOAT8 => 701,
            t if *t == Type::TEXT || *t == Type::VARCHAR => 25,
            t if *t == Type::BYTEA => 17,
            t if *t == Type::JSONB => 3802,
            t if *t == Type::JSON => 114,
            t if *t == Type::TIMESTAMP => 1114,
            t if *t == Type::TIMESTAMPTZ => 1184,
            t if *t == Type::INTERVAL => 1186,
            t if *t == Type::UUID => 2950,
            _ => return None,
        })
    }

    /// Writes a Postgres binary-format array into `out`.
    ///
    /// Layout:
    /// - i32: ndim
    /// - i32: `has_nulls` flag (0 or 1)
    /// - u32: element OID
    /// - for each dim: i32 size, i32 `lower_bound` (always 1)
    /// - for each element: i32 length (-1 for NULL), then bytes
    fn write_pg_array(
        elem_type: &Type,
        shape: &[i32],
        elements: &[Value],
        out: &mut BytesMut,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        let ndim =
            i32::try_from(shape.len()).map_err(|_| ArraySerializationError::DimensionsOverflow)?;
        let oid = element_oid(elem_type)
            .ok_or_else(|| ArraySerializationError::UnknownOid(elem_type.clone()))?;

        // Check for NULLs. The protocol requires the flag to be a signed 32-bit integer.
        let has_nulls = i32::from(elements.iter().any(|v| matches!(v, Value::None)));

        out.put_i32(ndim);
        out.put_i32(has_nulls);
        out.put_u32(oid);

        for &dim_size in shape {
            out.put_i32(dim_size);
            out.put_i32(1); // lower bound is always 1 in Postgres
        }

        for elem in elements {
            if matches!(elem, Value::None) {
                out.put_i32(-1); // NULL
            } else {
                // Serialize element into a temporary buffer
                let mut elem_buf = BytesMut::new();
                match elem.to_sql(elem_type, &mut elem_buf)? {
                    IsNull::Yes => {
                        out.put_i32(-1);
                    }
                    IsNull::No => {
                        let len = i32::try_from(elem_buf.len())
                            .map_err(|_| ArraySerializationError::ElementLengthOverflow)?;
                        out.put_i32(len);
                        out.extend_from_slice(&elem_buf);
                    }
                }
            }
        }

        Ok(())
    }

    fn ndarray_shape(shape: &[usize]) -> Result<Vec<i32>, ArraySerializationError> {
        shape
            .iter()
            .map(|&d| i32::try_from(d).map_err(|_| ArraySerializationError::ShapeElementOverflow))
            .collect()
    }

    impl Value {
        /// Attempt to serialize this value into `out` as a `PostgreSQL` array.
        ///
        /// Returns `Ok(true)` if serialization succeeded, `Ok(false)` if the value
        /// cannot be represented as a PG array and the caller should fall through.
        fn try_to_sql_as_array(
            &self,
            elem_type: &Type,
            out: &mut BytesMut,
        ) -> Result<bool, Box<dyn Error + Sync + Send>> {
            let maybe: Option<(Vec<usize>, Vec<Value>)> = match self {
                Self::Tuple(_) => {
                    let shape = tuple_shape(self).ok_or(ArraySerializationError::JaggedArray)?;
                    let mut flat = Vec::new();
                    flatten_tuple(self, &mut flat);
                    Some((shape, flat))
                }
                Self::IntArray(arr) => {
                    let flat = arr.iter().map(|&i| Value::Int(i)).collect();
                    Some((arr.shape().to_vec(), flat))
                }
                Self::FloatArray(arr) => {
                    let flat = arr.iter().map(|&f| Value::Float(OrderedFloat(f))).collect();
                    Some((arr.shape().to_vec(), flat))
                }
                _ => None,
            };

            if let Some((shape, flat)) = maybe {
                write_pg_array(elem_type, &ndarray_shape(&shape)?, &flat, out)?;
                Ok(true)
            } else {
                Ok(false)
            }
        }

        /// Try to forward a `Value::Int` to one of the compatible PG numeric types.
        ///
        /// Returns `Some(result)` if a compatible type was found and serialized,
        /// `None` if no PG type matched.
        fn try_int_to_sql(
            &self,
            i: i64,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Option<Result<IsNull, Box<dyn Error + Sync + Send>>> {
            macro_rules! try_forward {
                ($type:ty, $expr:expr) => {
                    if <$type as ToSql>::accepts(ty) {
                        let value: $type = match $expr {
                            Ok(v) => v,
                            Err(e) => return Some(Err(e.into())),
                        };
                        assert!(matches!(self.encode_format(ty), Format::Binary));
                        assert!(matches!(value.encode_format(ty), Format::Binary));
                        return Some(value.to_sql(ty, out));
                    }
                };
            }
            try_forward!(i64, Ok::<i64, std::convert::Infallible>(i));
            try_forward!(i32, i32::try_from(i));
            try_forward!(i16, i16::try_from(i));
            try_forward!(i8, i8::try_from(i));
            #[allow(clippy::cast_precision_loss)]
            {
                try_forward!(f64, Ok::<f64, std::convert::Infallible>(i as f64));
                try_forward!(f32, Ok::<f32, std::convert::Infallible>(i as f32));
            }
            None
        }

        /// Try to forward a `Value::Float` to `f64` or `f32`.
        ///
        /// Returns `Some(result)` if a compatible type was found and serialized,
        /// `None` if no PG type matched.
        fn try_float_to_sql(
            &self,
            f: f64,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Option<Result<IsNull, Box<dyn Error + Sync + Send>>> {
            macro_rules! try_forward {
                ($type:ty, $expr:expr) => {
                    if <$type as ToSql>::accepts(ty) {
                        let value: $type = match ($expr as $type).try_into() {
                            Ok(v) => v,
                            Err(e) => return Some(Err(e.into())),
                        };
                        assert!(matches!(self.encode_format(ty), Format::Binary));
                        assert!(matches!(value.encode_format(ty), Format::Binary));
                        return Some(value.to_sql(ty, out));
                    }
                };
            }
            try_forward!(f64, f);
            #[allow(clippy::cast_possible_truncation)]
            let f32_val = f as f32;
            try_forward!(f32, f32_val);
            None
        }

        /// Try to forward a 1-D `Value::FloatArray` to `pgvector::Vector` or `pgvector::HalfVector`.
        ///
        /// Returns `Some(result)` if a compatible type was found and serialized,
        /// `None` if no PG type matched (including multi-dimensional arrays — TODO).
        #[allow(clippy::cast_possible_truncation)]
        fn try_float_array_to_sql(
            &self,
            a: &ArrayD<f64>,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Option<Result<IsNull, Box<dyn Error + Sync + Send>>> {
            // TODO: handle regular (non-vector) multi-dimensional arrays
            if a.ndim() != 1 {
                return None;
            }
            let v = a
                .clone()
                .into_dimensionality::<Ix1>()
                .expect("ndim == 1")
                .to_vec();

            if <Vector as ToSql>::accepts(ty) {
                let v_32: Vec<f32> = v.iter().map(|e| *e as f32).collect();
                let value = Vector::from(v_32);
                assert!(matches!(self.encode_format(ty), Format::Binary));
                assert!(matches!(value.encode_format(ty), Format::Binary));
                return Some(value.to_sql(ty, out));
            }
            if <HalfVector as ToSql>::accepts(ty) {
                let v_16: Vec<f16> = v.iter().map(|e| f16::from_f64(*e)).collect();
                let value = HalfVector::from(v_16);
                assert!(matches!(self.encode_format(ty), Format::Binary));
                assert!(matches!(value.encode_format(ty), Format::Binary));
                return Some(value.to_sql(ty, out));
            }
            None
        }
    }

    impl ToSql for Value {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            macro_rules! try_forward {
                ($type:ty, $expr:expr) => {
                    if <$type as ToSql>::accepts(ty) {
                        let value: $type = $expr.try_into()?;
                        assert!(matches!(self.encode_format(ty), Format::Binary));
                        assert!(matches!(value.encode_format(ty), Format::Binary));
                        return value.to_sql(ty, out);
                    }
                };
            }

            // Handle Postgres array types for Value::Tuple (List) and ndarray types
            if let postgres::types::Kind::Array(elem_type) = ty.kind() {
                if self.try_to_sql_as_array(elem_type, out)? {
                    return Ok(IsNull::No);
                }
            }

            // Try to forward each variant to a compatible native Postgres type.
            // If no compatible type is found, fall through to the error at the bottom.
            #[allow(clippy::match_same_arms)]
            let pathway_type = match self {
                Self::None => return Ok(IsNull::Yes),
                Self::Bool(b) => {
                    try_forward!(bool, *b);
                    "bool"
                }
                Self::Int(i) => {
                    if let Some(result) = self.try_int_to_sql(*i, ty, out) {
                        return result;
                    }
                    "int"
                }
                Self::Float(OrderedFloat(f)) => {
                    if let Some(result) = self.try_float_to_sql(*f, ty, out) {
                        return result;
                    }
                    "float"
                }
                Self::Pointer(p) => {
                    try_forward!(String, p.to_string());
                    "pointer"
                }
                Self::String(s) => {
                    try_forward!(&str, s.as_str());
                    "string"
                }
                Self::Bytes(b) => {
                    try_forward!(&[u8], &b[..]);
                    "bytes"
                }
                Self::Tuple(t) => {
                    try_forward!(&[Value], &t[..]);
                    "tuple"
                }
                Self::IntArray(_) => "int array", // TODO
                Self::FloatArray(a) => {
                    if let Some(result) = self.try_float_array_to_sql(a, ty, out) {
                        return result;
                    }
                    "float array"
                }
                Self::DateTimeNaive(dt) => {
                    try_forward!(NaiveDateTime, dt.as_chrono_datetime());
                    "naive date/time"
                }
                Self::DateTimeUtc(dt) => {
                    try_forward!(DateTime<Utc>, dt.as_chrono_datetime().and_utc());
                    "UTC date/time"
                }
                Self::Duration(dr) => {
                    try_forward!(i64, dr.microseconds());
                    "duration"
                }
                Self::Json(j) => {
                    try_forward!(&serde_json::Value, &**j);
                    "JSON"
                }
                Self::Error => "error",
                Self::PyObjectWrapper(_) => {
                    try_forward!(Vec<u8>, bincode::serialize(self).map_err(|e| *e)?);
                    "python object"
                }
                Self::Pending => "pending",
            };

            Err(Box::new(ToSqlError::WrongPathwayType {
                pathway_type: pathway_type.to_owned(),
                postgres_type: ty.clone(),
            }))
        }

        fn accepts(_ty: &Type) -> bool {
            true // we double-check anyway
        }

        to_sql_checked!();
    }
}

impl Writer for PsqlWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.buffer.push(data);
        if let Some(max_batch_size) = self.max_batch_size {
            if self.buffer.len() == max_batch_size {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut transaction = self.client.transaction()?;

        for data in self.buffer.drain(..) {
            // We reuse `Value`'s serialization to pass additional `time` and `diff` columns.
            let diff = Value::Int(data.diff.try_into().unwrap());
            let time = Value::Int(data.time.0.try_into().unwrap());
            let mut params: Vec<_> = data
                .values
                .iter()
                .map(|v| v as &(dyn ToSql + Sync))
                .collect();
            let params = if self.snapshot_mode {
                if data.diff > 0 && self.legacy_mode {
                    params.push(&time);
                    params.push(&diff);
                    params
                } else if data.diff > 0 && !self.legacy_mode {
                    params
                } else {
                    self.query.primary_key_fields(params)
                }
            } else {
                params.push(&time);
                params.push(&diff);
                params
            };

            let query = self.query.build_query(data.diff);
            transaction
                .execute(&query, params.as_slice())
                .map_err(|error| WriteError::PsqlQueryFailed {
                    query: query.clone(),
                    error,
                })?;
        }

        transaction.commit()?;

        Ok(())
    }

    fn name(&self) -> String {
        format!("Postgres({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}

impl PsqlWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut client: PsqlClient,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        init_mode: TableWriterInitMode,
        legacy_mode: bool,
    ) -> Result<PsqlWriter, WriteError> {
        let mut transaction = client.transaction()?;
        init_mode.initialize(
            table_name,
            value_fields,
            key_field_names,
            !snapshot_mode || legacy_mode,
            |query| {
                transaction.execute(query, &[])?;
                Ok(())
            },
            Self::postgres_data_type,
        )?;
        transaction.commit()?;

        let query = SqlQueryTemplate::new(
            snapshot_mode,
            table_name,
            value_fields,
            key_field_names,
            legacy_mode,
            |index| format!("${}", index + 1),
            Self::on_insert_conflict_condition,
        )?;

        let writer = PsqlWriter {
            client,
            max_batch_size,
            query,
            buffer: Vec::new(),
            snapshot_mode,
            table_name: table_name.to_string(),
            legacy_mode,
        };

        Ok(writer)
    }

    // Generates a statement that is used in "on conflict" part of the query
    fn on_insert_conflict_condition(
        table_name: &str,
        key_field_names: &[String],
        value_fields: &[ValueField],
        legacy_mode: bool,
    ) -> String {
        let mut value_field_positions = Vec::new();
        let mut key_field_positions = vec![0; key_field_names.len()];
        for (value_index, value_field) in value_fields.iter().enumerate() {
            value_field_positions.push(value_index);
            for (key_index, key_field) in key_field_names.iter().enumerate() {
                if *key_field == value_field.name {
                    value_field_positions.pop();
                    key_field_positions[key_index] = value_index;
                    break;
                }
            }
        }

        let mut legacy_update_pairs = Vec::with_capacity(2);
        if legacy_mode {
            legacy_update_pairs.push(format!("time=${}", value_fields.len() + 1));
            legacy_update_pairs.push(format!("diff=${}", value_fields.len() + 2));
        }

        let update_pairs = value_field_positions
            .iter()
            .map(|position| format!("{}=${}", value_fields[*position].name, *position + 1))
            .chain(legacy_update_pairs)
            .join(",");

        if update_pairs.is_empty() {
            format!(
                "ON CONFLICT ({}) DO NOTHING",
                key_field_names.iter().join(",")
            )
        } else {
            let update_condition = key_field_positions
                .iter()
                .map(|position| {
                    format!(
                        "{}.{}=${}",
                        table_name,
                        value_fields[*position].name,
                        *position + 1
                    )
                })
                .join(" AND ");

            format!(
                "ON CONFLICT ({}) DO UPDATE SET {update_pairs} WHERE {update_condition}",
                key_field_names.iter().join(",")
            )
        }
    }

    fn postgres_data_type(type_: &Type, is_nested: bool) -> Result<String, WriteError> {
        let not_null_suffix = if is_nested { "" } else { " NOT NULL" };
        Ok(match type_ {
            Type::Bool => format!("BOOLEAN{not_null_suffix}"),
            Type::Int | Type::Duration => format!("BIGINT{not_null_suffix}"),
            Type::Float => format!("DOUBLE PRECISION{not_null_suffix}"),
            Type::Pointer | Type::String => format!("TEXT{not_null_suffix}"),
            Type::Bytes | Type::PyObjectWrapper => format!("BYTEA{not_null_suffix}"),
            Type::Json => format!("JSONB{not_null_suffix}"),
            Type::DateTimeNaive => format!("TIMESTAMP{not_null_suffix}"),
            Type::DateTimeUtc => format!("TIMESTAMPTZ{not_null_suffix}"),
            Type::Optional(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }
                Self::postgres_data_type(wrapped, true)?
            }
            Type::List(wrapped) => {
                let mut nested_type = wrapped;
                while let Type::List(inner_nested_type) = &**nested_type {
                    nested_type = inner_nested_type;
                }
                let nested_type = nested_type.unoptionalize().clone();
                if matches!(nested_type, Type::Tuple(_)) || matches!(nested_type, Type::Array(_, _))
                {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }
                format!(
                    "{}[]{not_null_suffix}",
                    Self::postgres_data_type(&nested_type, true)?
                )
            }
            Type::Tuple(fields) => {
                let mut iter = fields.iter();
                if !fields.is_empty() && iter.all(|field| field == &fields[0]) {
                    let first = Self::postgres_data_type(&fields[0], true)?;
                    return Ok(format!("{first}[]{not_null_suffix}"));
                }
                return Err(WriteError::UnsupportedType(type_.clone()));
            }
            Type::Array(_, element_type) => {
                // The only supported nested types are int and float
                if !matches!(**element_type, Type::Int | Type::Float) {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }

                let base = Self::postgres_data_type(element_type, true)?;
                format!("{base}[]{not_null_suffix}")
            }
            Type::Any | Type::Future(_) => return Err(WriteError::UnsupportedType(type_.clone())),
        })
    }
}

mod from_sql {
    use std::error::Error;
    use std::sync::Arc;

    use bytes::Buf;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use half::f16;
    use ndarray::ArrayD;
    use ordered_float::OrderedFloat;
    use pgvector::{HalfVector, Vector};
    use postgres::types::{FromSql, Kind, Type};

    use crate::engine::time::{DateTimeNaive, DateTimeUtc};
    use crate::engine::{Duration, Value};

    #[derive(Debug, thiserror::Error)]
    enum FromSqlError {
        #[error("cannot convert Postgres type {postgres_type} to a Pathway value")]
        UnsupportedPostgresType { postgres_type: Type },

        #[error("array ndim value {0} is out of range")]
        ArrayNdimOutOfRange(i32),

        #[error("array dimension size {0} is out of range")]
        ArrayDimSizeOutOfRange(i32),

        #[error("array element length {0} is out of range")]
        ArrayElemLengthOutOfRange(i32),
    }

    fn float_array(floats: Vec<f64>) -> Result<Value, Box<dyn Error + Sync + Send>> {
        let len = floats.len();
        let arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[len]), floats)?;
        Ok(arr.into())
    }

    /// Parses a Postgres binary array while preserving its multidimensional structure.
    /// Returns `Value::Tuple`.
    fn parse_binary_array(
        elem_type: &Type,
        raw: &[u8],
    ) -> Result<Value, Box<dyn Error + Sync + Send>> {
        let mut buf = raw;

        let ndim = buf.get_i32();
        let _has_nulls = buf.get_i32();
        let _elem_oid = buf.get_u32();

        if ndim == 0 {
            // Empty array
            return Ok(Value::Tuple(Arc::new([])));
        }

        // Read shape of all dimensions
        let ndim_usize =
            usize::try_from(ndim).map_err(|_| FromSqlError::ArrayNdimOutOfRange(ndim))?;
        let mut shape: Vec<usize> = Vec::with_capacity(ndim_usize);
        for _ in 0..ndim {
            let dim_size = buf.get_i32();
            let _lower_bound = buf.get_i32();
            shape.push(
                usize::try_from(dim_size)
                    .map_err(|_| FromSqlError::ArrayDimSizeOutOfRange(dim_size))?,
            );
        }

        // Read all elements of the flat representation into a buffer
        let total_elements: usize = shape.iter().product();
        let mut flat_elements: Vec<Value> = Vec::with_capacity(total_elements);
        for _ in 0..total_elements {
            let elem_len = buf.get_i32();
            if elem_len == -1 {
                flat_elements.push(Value::None);
            } else {
                let elem_len = usize::try_from(elem_len)
                    .map_err(|_| FromSqlError::ArrayElemLengthOutOfRange(elem_len))?;
                let elem_raw = &buf[..elem_len];
                buf.advance(elem_len);
                flat_elements.push(Value::from_sql(elem_type, elem_raw)?);
            }
        }

        // Build nested `Value::Tuple` elements
        Ok(rebuild_nested(&flat_elements, &shape, &mut 0))
    }

    /// Recursively build `Value::Tuple` from flatten elements.
    /// `cursor` is the index of the next element to be read.
    fn rebuild_nested(flat: &[Value], shape: &[usize], cursor: &mut usize) -> Value {
        if shape.len() == 1 {
            // Last dimension, just take shape[0] last elements
            let start = *cursor;
            *cursor += shape[0];
            Value::Tuple(flat[start..*cursor].to_vec().into())
        } else {
            // Build shape[0] nested tuples, each with the tail shape[1..]
            let mut children: Vec<Value> = Vec::with_capacity(shape[0]);
            for _ in 0..shape[0] {
                children.push(rebuild_nested(flat, &shape[1..], cursor));
            }
            Value::Tuple(children.into())
        }
    }

    impl<'a> FromSql<'a> for Value {
        #[allow(clippy::too_many_lines)]
        fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
            // bool
            if <bool as FromSql>::accepts(ty) {
                return Ok(Value::Bool(bool::from_sql(ty, raw)?));
            }

            // Postgres binary interval: i64 usecs + i32 days + i32 months
            if *ty == Type::INTERVAL {
                let mut buf = raw;
                let usecs = buf.get_i64();
                let days = buf.get_i32();
                let months = buf.get_i32();
                let total_usecs = usecs
                    + i64::from(days) * 86_400_000_000
                    + i64::from(months) * 30 * 86_400_000_000;
                return Ok(Value::Duration(Duration::new_with_unit(total_usecs, "us")?));
            }

            // integers from biggest to smallest
            if <i64 as FromSql>::accepts(ty) {
                return Ok(Value::Int(i64::from_sql(ty, raw)?));
            }
            if <i32 as FromSql>::accepts(ty) {
                return Ok(Value::Int(i64::from(i32::from_sql(ty, raw)?)));
            }
            if <i16 as FromSql>::accepts(ty) {
                return Ok(Value::Int(i64::from(i16::from_sql(ty, raw)?)));
            }
            if <i8 as FromSql>::accepts(ty) {
                return Ok(Value::Int(i64::from(i8::from_sql(ty, raw)?)));
            }

            // floats
            if <f64 as FromSql>::accepts(ty) {
                return Ok(Value::Float(OrderedFloat(f64::from_sql(ty, raw)?)));
            }
            if <f32 as FromSql>::accepts(ty) {
                return Ok(Value::Float(OrderedFloat(f64::from(f32::from_sql(
                    ty, raw,
                )?))));
            }

            // OID → int
            if *ty == Type::OID {
                let mut buf = raw;
                let oid = buf.get_u32();
                return Ok(Value::Int(i64::from(oid)));
            }

            if *ty == Type::NUMERIC {
                // Binary: ndigits (i16), weight (i16), sign (u16), dscale (i16), digits (i16 each, base 10000)
                let mut buf = raw;
                let ndigits = buf.get_i16();
                let weight = buf.get_i16();
                let sign = buf.get_u16();
                let _dscale = buf.get_i16();

                let mut value: f64 = 0.0;
                for i in 0..i32::from(ndigits) {
                    let digit = f64::from(buf.get_i16());
                    value += digit * 10_000f64.powi(i32::from(weight) - i);
                }
                if sign == 0x4000 {
                    value = -value;
                }
                return Ok(Value::Float(OrderedFloat(value)));
            }

            // addresses → string
            if *ty == Type::INET || *ty == Type::CIDR {
                // Binary format: family (1 byte), prefix_len (1 byte), is_cidr (1 byte), addr_len (1 byte), addr bytes
                let family = raw[0];
                let prefix_len = raw[1];
                let addr_len = raw[3] as usize;
                let addr_bytes = &raw[4..4 + addr_len];
                let ip: std::net::IpAddr = if family == 2 {
                    // AF_INET
                    let octets: [u8; 4] = addr_bytes.try_into()?;
                    std::net::Ipv4Addr::from(octets).into()
                } else {
                    // AF_INET6
                    let octets: [u8; 16] = addr_bytes.try_into()?;
                    std::net::Ipv6Addr::from(octets).into()
                };
                let max_prefix = if family == 2 { 32 } else { 128 };
                let s = if *ty == Type::CIDR || prefix_len < max_prefix {
                    format!("{ip}/{prefix_len}")
                } else {
                    ip.to_string()
                };
                return Ok(Value::String(s.into()));
            }

            if *ty == Type::MACADDR {
                // 6 raw bytes → "xx:xx:xx:xx:xx:xx"
                let s = format!(
                    "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                    raw[0], raw[1], raw[2], raw[3], raw[4], raw[5]
                );
                return Ok(Value::String(s.into()));
            }

            // uuid
            if *ty == Type::UUID {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&raw[..16]);
                let uuid = uuid::Uuid::from_bytes(bytes);
                return Ok(Value::String(uuid.to_string().into()));
            }

            // text
            if <String as FromSql>::accepts(ty) {
                return Ok(Value::String(String::from_sql(ty, raw)?.into()));
            }

            // bytes
            if <Vec<u8> as FromSql>::accepts(ty) {
                return Ok(Value::Bytes(Vec::<u8>::from_sql(ty, raw)?.into()));
            }

            // JSON
            if <serde_json::Value as FromSql>::accepts(ty) {
                return Ok(serde_json::Value::from_sql(ty, raw)?.into());
            }

            // TIME (without timezone) — nanoseconds since midnight
            if *ty == Type::TIME {
                let mut buf = raw;
                let usecs = buf.get_i64();
                return Ok(Value::Duration(Duration::new_with_unit(usecs, "us")?));
            }

            // TIMETZ — time with timezone offset, convert to UTC
            if *ty == Type::TIMETZ {
                let mut buf = raw;
                let usecs = buf.get_i64();
                let offset_secs = buf.get_i32(); // offset from UTC in seconds, sign is inverted in PG
                let usecs_utc = usecs + i64::from(offset_secs) * 1_000_000;
                return Ok(Value::Duration(Duration::new_with_unit(usecs_utc, "us")?));
            }

            // date
            if *ty == Type::DATE {
                let mut buf = raw;
                let days = buf.get_i32();
                let pg_epoch =
                    chrono::NaiveDate::from_ymd_opt(2000, 1, 1).ok_or("invalid epoch")?;
                let naive_date = pg_epoch
                    .checked_add_signed(chrono::Duration::days(i64::from(days)))
                    .ok_or("date out of range")?;
                let naive_datetime = naive_date.and_hms_opt(0, 0, 0).ok_or("invalid time")?;
                return Ok(Value::DateTimeNaive(DateTimeNaive::try_from(
                    naive_datetime,
                )?));
            }

            // chrono timestamps
            if <DateTime<Utc> as FromSql>::accepts(ty) {
                return Ok(Value::DateTimeUtc(DateTimeUtc::try_from(
                    DateTime::<Utc>::from_sql(ty, raw)?,
                )?));
            }
            if <NaiveDateTime as FromSql>::accepts(ty) {
                return Ok(Value::DateTimeNaive(DateTimeNaive::try_from(
                    NaiveDateTime::from_sql(ty, raw)?,
                )?));
            }

            // pgvector
            if <Vector as FromSql>::accepts(ty) {
                let floats = Vec::<f32>::from(Vector::from_sql(ty, raw)?)
                    .into_iter()
                    .map(f64::from)
                    .collect();
                return float_array(floats);
            }
            if <HalfVector as FromSql>::accepts(ty) {
                let floats = Vec::<f16>::from(HalfVector::from_sql(ty, raw)?)
                    .into_iter()
                    .map(f64::from)
                    .collect();
                return float_array(floats);
            }

            // Generic array case: parse as `Value::Tuple`
            // If the desired type is ndarray, convert later
            if let Kind::Array(elem_type) = ty.kind() {
                return parse_binary_array(elem_type, raw);
            }

            Err(Box::new(FromSqlError::UnsupportedPostgresType {
                postgres_type: ty.clone(),
            }))
        }

        fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
            Ok(Value::None)
        }

        fn accepts(_ty: &Type) -> bool {
            true
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error(transparent)]
    WalReader(#[from] pg_walstream::error::ReplicationError),

    #[error("Publication '{name}' does not exist")]
    PublicationNotFound { name: String },

    #[error(transparent)]
    Query(#[from] postgres::Error),

    #[error("Modification of a non-append-only table: {0:?}")]
    AppendOnlyNotRespected(ChangeEvent),

    #[error(
        "Table {schema}.{table} has no primary key; non-append-only tables without a primary key are not supported"
    )]
    NoPrimaryKey { schema: String, table: String },

    #[error("Fields not found in table {schema}.{table}: {missing_fields:?}")]
    MissingValueFields {
        schema: String,
        table: String,
        missing_fields: Vec<String>,
    },

    #[error("
        Primary key mismatch for a non-append-only table {schema}.{table}: expected columns {expected:?}, got {actual:?}
    ")]
    PrimaryKeyMismatch {
        schema: String,
        table: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },
}

/// Holds all table-specific context that is passed around instead of individual parameters.
#[derive(Clone)]
pub struct TableContext {
    pub schema_name: String,
    pub table_name: String,
    pub value_fields: Vec<ValueField>,
    pub key_field_names: Option<Vec<String>>,
    pub is_append_only: bool,
}

impl TableContext {
    pub fn new(
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        is_append_only: bool,
    ) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            value_fields: value_fields.to_vec(),
            key_field_names: key_field_names.map(<[String]>::to_vec),
            is_append_only,
        }
    }

    pub fn key_field_names_deref(&self) -> Option<&[String]> {
        self.key_field_names.as_deref()
    }
}

fn emit_offset(total_entries_read: &mut usize) -> (OffsetKey, OffsetValue) {
    *total_entries_read += 1;
    (
        OffsetKey::Empty,
        OffsetValue::PostgresReadEntriesCount(*total_entries_read),
    )
}

pub struct WalReader {
    runtime: TokioRuntime,
    stream: LogicalReplicationStream,
    cancel_token: CancellationToken,
    table_ctx: TableContext,
    effective_snapshot_name: String,
}

#[derive(Debug)]
pub enum PgEventAfterParsing {
    SimpleChange(ReadResult),
    Truncate,
}

const MAX_SLOT_NAME_LEN: usize = 63;
const SLOT_NAME_PREFIX: &str = "pw_repl_";
const SLOT_NAME_TAG_LEN: usize = 8;

impl WalReader {
    fn new(settings: ReplicationSettings, table_ctx: TableContext) -> Result<Self, ReadError> {
        let runtime = create_async_tokio_runtime()?;

        let generated_slot_name =
            Self::generate_replication_slot_name(&table_ctx.schema_name, &table_ctx.table_name);
        let config = ReplicationStreamConfig::new(
            settings
                .replication_slot_name
                .unwrap_or_else(|| generated_slot_name.clone()),
            settings.publication_name.clone(),
            2,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        )
        .with_slot_options(ReplicationSlotOptions {
            temporary: true,
            two_phase: false,
            reserve_wal: false,
            snapshot: Some("export".to_string()),
            failover: false,
        });
        let cancel_token = CancellationToken::new();

        let stream = runtime.block_on(async {
            let mut stream =
                LogicalReplicationStream::new(&settings.connection_string, config).await?;
            stream.ensure_replication_slot().await?;
            Ok::<LogicalReplicationStream, ReplicationError>(stream)
        })?;

        let effective_snapshot_name = settings
            .snapshot_name
            .or_else(|| stream.exported_snapshot_name().map(str::to_owned))
            .unwrap_or(generated_slot_name);

        Ok(Self {
            runtime,
            stream,
            cancel_token,
            table_ctx,
            effective_snapshot_name,
        })
    }

    fn start_replication(&mut self) -> Result<(), ReadError> {
        Ok(self.runtime.block_on(async {
            self.stream.start(None).await?;
            Ok::<(), ReplicationError>(())
        })?)
    }

    fn generate_replication_slot_name(schema_name: &str, table_name: &str) -> String {
        let tag = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(SLOT_NAME_TAG_LEN)
            .collect::<String>();

        let mut base = format!("{schema_name}_{table_name}");

        let reserved = SLOT_NAME_PREFIX.len() + 1 + SLOT_NAME_TAG_LEN;
        let max_base_len = MAX_SLOT_NAME_LEN - reserved;

        if base.len() > max_base_len {
            base.truncate(max_base_len);
        }

        format!("{SLOT_NAME_PREFIX}{base}_{tag}")
    }

    fn parse_pg_array_string(
        s: &str,
        element_type: &Type,
        field_name: &str,
    ) -> Result<Value, String> {
        let s = s.trim();
        if !s.starts_with('{') || !s.ends_with('}') {
            return Err(format!("Array not in {{...}} format: {s}"));
        }
        let inner = &s[1..s.len() - 1];

        if inner.is_empty() {
            return Ok(Value::Tuple(Arc::new([])));
        }

        let is_nested = inner.trim_start().starts_with('{');

        if is_nested {
            let inner_element_type = match element_type {
                Type::List(t) => t.as_ref(),
                other => other,
            };
            let parts = Self::split_top_level(inner);
            let items: Result<Vec<Value>, String> = parts
                .iter()
                .map(|p| Self::parse_pg_array_string(p.trim(), inner_element_type, field_name))
                .collect();
            Ok(Value::Tuple(items?.into()))
        } else {
            let parts = Self::split_top_level(inner);
            let items: Result<Vec<Value>, String> = parts
                .iter()
                .map(|p| {
                    let trimmed = p.trim();
                    if trimmed.eq_ignore_ascii_case("null") {
                        return Ok(Value::None);
                    }
                    let unquoted = if trimmed.starts_with('"') && trimmed.ends_with('"') {
                        Self::unescape_pg_string(&trimmed[1..trimmed.len() - 1])
                    } else {
                        trimmed.to_string()
                    };

                    // Bytes need special handling: parse \xdeadbeef directly
                    // without going through JsonValue::String which mangles backslashes
                    if matches!(element_type, Type::Bytes) {
                        let hex = unquoted
                            .strip_prefix("\\x")
                            .ok_or_else(|| format!("Bytes not in \\x format: {unquoted}"))?;
                        let bytes = hex::decode(hex)
                            .map_err(|e| format!("Cannot decode hex bytes '{hex}': {e}"))?;
                        return Ok(Value::Bytes(bytes.into()));
                    }

                    let json_val = JsonValue::String(unquoted);
                    Self::parse_value_from_postgres(&json_val, element_type, field_name)
                        .map_err(|e| e.to_string())
                })
                .collect();
            Ok(Value::Tuple(items?.into()))
        }
    }

    /// Unescapes a Postgres-quoted string content (after outer quotes are stripped).
    /// Handles both double-quote escaping ("") and backslash escaping (\).
    fn unescape_pg_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '\\' => {
                    if let Some(next) = chars.next() {
                        result.push(next);
                    } else {
                        result.push('\\');
                    }
                }
                '"' => {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                    }
                    result.push('"');
                }
                _ => result.push(ch),
            }
        }
        result
    }

    /// Splits a string by top-level commas (not inside `{}` and not inside `""`).
    fn split_top_level(s: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut depth = 0i32;
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = s.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' if !in_quotes => {
                    in_quotes = true;
                    current.push(ch);
                }
                '"' if in_quotes => {
                    // Check for escaped quote: "" inside quoted string
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current.push('"');
                        current.push('"');
                    } else {
                        in_quotes = false;
                        current.push(ch);
                    }
                }
                '\\' if in_quotes => {
                    // Backslash escape — keep both the backslash and the next char as-is
                    current.push(ch);
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                }
                '{' if !in_quotes => {
                    depth += 1;
                    current.push(ch);
                }
                '}' if !in_quotes => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if !in_quotes && depth == 0 => {
                    parts.push(current.trim().to_string());
                    current = String::new();
                }
                _ => {
                    current.push(ch);
                }
            }
        }
        if !current.trim().is_empty() {
            parts.push(current.trim().to_string());
        }
        parts
    }

    fn parse_value_from_postgres(
        json_val: &JsonValue,
        type_: &Type,
        field_name: &str,
    ) -> Result<Value, Box<ConversionError>> {
        if json_val.is_null() {
            return Ok(Value::None);
        }

        let err = |msg: String| {
            Box::new(ConversionError {
                value_repr: format!("{json_val}"),
                field_name: field_name.to_string(),
                type_: type_.clone(),
                original_error_message: msg,
            })
        };

        let s = match json_val {
            JsonValue::String(s) => s.as_str(),
            JsonValue::Bool(b) => return Ok(Value::Bool(*b)),
            JsonValue::Number(n) => return Self::parse_number(n, type_, &err),
            _other => return Err(err("Unexpected value type".to_string())),
        };

        Self::parse_value_from_str(s, type_, field_name, &err)
    }

    fn parse_number<E>(
        n: &serde_json::Number,
        type_: &Type,
        err: &E,
    ) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        if let Some(i) = n.as_i64() {
            if matches!(type_, Type::Duration) {
                return Ok(Value::Duration(
                    EngineDuration::new_with_unit(i, "us").map_err(|e| {
                        err(format!("Cannot create duration from an integer field: {e}"))
                    })?,
                ));
            }
            return Ok(Value::Int(i));
        }
        if let Some(f) = n.as_f64() {
            return Ok(Value::Float(OrderedFloat(f)));
        }
        Err(err("Failed to parse number".to_string()))
    }

    fn parse_value_from_str<E>(
        s: &str,
        type_: &Type,
        field_name: &str,
        err: &E,
    ) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let value = match type_ {
            Type::Bool => Self::parse_bool_from_str(s, err)?,
            Type::Int => {
                let i: i64 = s
                    .parse()
                    .map_err(|e| err(format!("Cannot parse int '{s}': {e}")))?;
                Value::Int(i)
            }
            Type::Float => {
                let f: f64 = s
                    .parse()
                    .map_err(|e| err(format!("Cannot parse float '{s}': {e}")))?;
                Value::Float(OrderedFloat(f))
            }
            Type::String | Type::Any => Value::String(s.into()),
            Type::Bytes => Self::parse_bytes_from_str(s, err)?,
            Type::Pointer => parse_pathway_pointer(s)
                .map_err(|e| err(format!("Cannot parse Pointer '{s}': {e}")))?,
            Type::DateTimeNaive => Self::parse_datetime_naive(s, err)?,
            Type::DateTimeUtc => Self::parse_datetime_utc(s, err)?,
            Type::Duration => Self::parse_duration_from_str(s, err)?,
            Type::Json => {
                let json: JsonValue = serde_json::from_str(s)
                    .map_err(|e| err(format!("Cannot parse JSON '{s}': {e}")))?;
                json.into()
            }
            Type::List(element_type) => {
                return Self::parse_pg_array_string(s, element_type, field_name)
                    .map_err(|e| err(format!("Cannot parse list '{s}': {e}")));
            }
            Type::Array(_, element_type) => Self::parse_pg_array(s, element_type, field_name, err)?,
            Type::Tuple(element_types) => Self::parse_pg_tuple(s, element_types, field_name, err)?,
            Type::Optional(inner_type) => {
                let json_val = JsonValue::String(s.to_string());
                Self::parse_value_from_postgres(&json_val, inner_type, field_name)?
            }
            Type::PyObjectWrapper => Self::parse_py_object_wrapper(s, err)?,
            other @ Type::Future(_) => {
                return Err(err(format!("Unsupported type for parsing: {other}")))
            }
        };

        Ok(value)
    }

    fn parse_bool_from_str<E>(s: &str, err: &E) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        match s {
            "t" | "true" | "TRUE" | "1" => Ok(Value::Bool(true)),
            "f" | "false" | "FALSE" | "0" => Ok(Value::Bool(false)),
            _ => Err(err(format!("Cannot parse bool: {s}"))),
        }
    }

    fn parse_bytes_from_str<E>(s: &str, err: &E) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let hex = s
            .strip_prefix("\\x")
            .ok_or_else(|| err(format!("Bytes not in \\x format: {s}")))?;
        let bytes =
            hex::decode(hex).map_err(|e| err(format!("Cannot decode hex bytes '{hex}': {e}")))?;
        Ok(Value::Bytes(bytes.into()))
    }

    fn parse_datetime_naive<E>(s: &str, err: &E) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        // Full timestamp
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
            return DateTimeNaive::try_from(dt)
                .map(Value::DateTimeNaive)
                .map_err(|e| err(format!("DateTime out of range '{s}': {e}")));
        }
        // Date-only (e.g. from DATE column in WAL streaming)
        if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            let dt = d
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| err("invalid time".to_string()))?;
            return DateTimeNaive::try_from(dt)
                .map(Value::DateTimeNaive)
                .map_err(|e| err(format!("DateTime out of range '{s}': {e}")));
        }
        Err(err(format!(
            "Cannot parse DateTimeNaive '{s}': premature end of input"
        )))
    }

    fn parse_datetime_utc<E>(s: &str, err: &E) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let dt = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
            .map_err(|e| err(format!("Cannot parse DateTimeUtc '{s}': {e}")))?;
        DateTimeUtc::try_from(dt.to_utc())
            .map(Value::DateTimeUtc)
            .map_err(|e| err(format!("DateTime out of range '{s}': {e}")))
    }

    fn parse_duration_from_str<E>(s: &str, err: &E) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let d = s
            .parse::<i64>()
            .map_err(|e| format!("{e}"))
            .and_then(|us| EngineDuration::new_with_unit(us, "us").map_err(|e| format!("{e}")))
            .or_else(|_| Self::parse_postgres_interval(s))
            .map_err(|e| err(format!("Cannot parse Duration '{s}': {e}")))?;
        Ok(Value::Duration(d))
    }

    fn parse_py_object_wrapper<E>(s: &str, err: &E) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let hex_str = s
            .strip_prefix("\\x")
            .ok_or_else(|| err(format!("PyObjectWrapper (Bytea) not in \\x format: {s}")))?;
        let raw_bytes = hex::decode(hex_str)
            .map_err(|e| err(format!("Cannot decode hex for PyObjectWrapper: {e}")))?;
        let value: Value = bincode::deserialize(&raw_bytes)
            .map_err(|e| err(format!("Bincode deserialization failed: {e}")))?;
        match value {
            Value::PyObjectWrapper(wrapper) => Ok(Value::PyObjectWrapper(wrapper)),
            other => Err(err(format!(
                "Expected PyObjectWrapper after bincode, got: {other:?}"
            ))),
        }
    }

    fn parse_pg_array<E>(
        s: &str,
        element_type: &Type,
        field_name: &str,
        err: &E,
    ) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let inner = s
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| err(format!("Array not in {{...}} format: {s}")))?;

        if inner.is_empty() {
            return match element_type {
                Type::Int => Ok(ArrayD::from_elem(ndarray::IxDyn(&[0]), 0i64).into()),
                Type::Float => Ok(ArrayD::from_elem(ndarray::IxDyn(&[0]), 0f64).into()),
                _ => Ok(Value::Tuple(Arc::new([]))),
            };
        }

        match element_type {
            Type::Int => {
                let (elems, shape) = Self::parse_nested_array(s, &|p| {
                    p.trim().parse::<i64>().map_err(|e| format!("{e}"))
                })
                .map_err(|e| err(format!("Cannot parse int array '{s}': {e}")))?;
                let arr = ArrayD::from_shape_vec(ndarray::IxDyn(&shape), elems)
                    .map_err(|e| err(format!("Cannot build int array: {e}")))?;
                Ok(arr.into())
            }
            Type::Float => {
                let (elems, shape) = Self::parse_nested_array(s, &|p| {
                    p.trim().parse::<f64>().map_err(|e| format!("{e}"))
                })
                .map_err(|e| err(format!("Cannot parse float array '{s}': {e}")))?;
                let arr = ArrayD::from_shape_vec(ndarray::IxDyn(&shape), elems)
                    .map_err(|e| err(format!("Cannot build float array: {e}")))?;
                Ok(arr.into())
            }
            other_type => {
                let parts = Self::split_top_level(inner);
                let items = parts
                    .iter()
                    .map(|p| {
                        let trimmed = p.trim();
                        if trimmed.eq_ignore_ascii_case("null") {
                            return Ok(Value::None);
                        }
                        let unquoted = if trimmed.starts_with('"') && trimmed.ends_with('"') {
                            &trimmed[1..trimmed.len() - 1]
                        } else {
                            trimmed
                        };
                        let element_json = JsonValue::String(unquoted.to_string());
                        Self::parse_value_from_postgres(&element_json, other_type, field_name)
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        err(format!(
                            "Cannot parse array of '{other_type}' from '{s}': {e}"
                        ))
                    })?;
                Ok(Value::Tuple(items.into()))
            }
        }
    }

    fn parse_nested_array<T, F>(s: &str, parse_elem: &F) -> Result<(Vec<T>, Vec<usize>), String>
    where
        F: Fn(&str) -> Result<T, String>,
    {
        if s.starts_with('{') {
            let inner = s
                .strip_prefix('{')
                .and_then(|s| s.strip_suffix('}'))
                .ok_or_else(|| format!("Expected {{...}}, got: {s}"))?;

            if inner.is_empty() {
                return Ok((vec![], vec![0]));
            }

            let parts = Self::split_top_level(inner);
            let mut all_elems = vec![];
            let mut inner_shape: Option<Vec<usize>> = None;

            for part in &parts {
                let (elems, shape) = Self::parse_nested_array(part.trim(), parse_elem)?;
                if let Some(ref expected) = inner_shape {
                    if *expected != shape {
                        return Err("Inconsistent sub-array shapes".to_string());
                    }
                } else {
                    inner_shape = Some(shape);
                }
                all_elems.extend(elems);
            }

            let mut full_shape = vec![parts.len()];
            if let Some(ref sub) = inner_shape {
                full_shape.extend_from_slice(sub);
            }

            Ok((all_elems, full_shape))
        } else {
            let v = parse_elem(s.trim())?;
            Ok((vec![v], vec![]))
        }
    }

    fn parse_pg_tuple<E>(
        s: &str,
        element_types: &[Type],
        field_name: &str,
        err: &E,
    ) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let s_trimmed = s.trim();
        if !(s_trimmed.len() >= 2 && s_trimmed.starts_with('{') && s_trimmed.ends_with('}')) {
            return Err(err(format!(
                "Invalid Postgres tuple format (expected {{...}}): {s}"
            )));
        }

        let inner = &s_trimmed[1..s_trimmed.len() - 1];
        if inner.is_empty() && element_types.is_empty() {
            return Ok(Value::Tuple(Arc::new([])));
        }

        let parts = Self::split_top_level(inner);
        if parts.len() != element_types.len() {
            return Err(err(format!(
                "Tuple length mismatch: expected {}, got {}",
                element_types.len(),
                parts.len()
            )));
        }

        let items = parts
            .iter()
            .zip(element_types.iter())
            .map(|(p, t)| {
                let item_trimmed = p.trim();
                if item_trimmed.eq_ignore_ascii_case("null") {
                    return Ok(Value::None);
                }
                let unquoted = if item_trimmed.starts_with('"') && item_trimmed.ends_with('"') {
                    Self::unescape_pg_string(&item_trimmed[1..item_trimmed.len() - 1])
                } else {
                    item_trimmed.to_string()
                };
                let element_json = JsonValue::String(unquoted);
                Self::parse_value_from_postgres(&element_json, t, field_name)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Tuple(items.into()))
    }

    fn parse_postgres_interval(s: &str) -> Result<EngineDuration, String> {
        let mut total_nanos: i64 = 0;
        let mut remaining = s;

        while !remaining.is_empty() {
            remaining = remaining.trim();
            if remaining.is_empty() {
                break;
            }

            if remaining.contains(':') {
                let time_part = remaining.split_whitespace().next().unwrap_or(remaining);

                // Strip timezone offset if present: "12:30:00+02:00" or "12:30:00-05:30"
                let (time_part_no_tz, offset_nanos) = if let Some(sign_pos) =
                    time_part.find(['+', '-']).filter(|&i| i > 0)
                // skip leading minus for negative times
                {
                    let (t, tz) = time_part.split_at(sign_pos);
                    let sign: i64 = if tz.starts_with('+') { -1 } else { 1 };
                    let tz_parts: Vec<&str> = tz[1..].split(':').collect();
                    let tz_hours: i64 = tz_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
                    let tz_mins: i64 = tz_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let offset = sign * (tz_hours * 3_600 + tz_mins * 60) * 1_000_000_000;
                    (t, offset)
                } else {
                    (time_part, 0)
                };

                let parts: Vec<&str> = time_part_no_tz.split(':').collect();
                if parts.len() == 3 {
                    let hours: i64 = parts[0]
                        .parse()
                        .map_err(|e| format!("Cannot parse hours in '{s}': {e}"))?;
                    let minutes: i64 = parts[1]
                        .parse()
                        .map_err(|e| format!("Cannot parse minutes in '{s}': {e}"))?;
                    let (secs, nanos) = if let Some((sec_str, frac_str)) = parts[2].split_once('.')
                    {
                        let secs: i64 = sec_str
                            .parse()
                            .map_err(|e| format!("Cannot parse seconds in '{s}': {e}"))?;
                        let frac_padded = format!("{frac_str:0<9}");
                        let nanos: i64 = frac_padded[..9]
                            .parse()
                            .map_err(|e| format!("Cannot parse nanoseconds in '{s}': {e}"))?;
                        (secs, nanos)
                    } else {
                        let secs: i64 = parts[2]
                            .parse()
                            .map_err(|e| format!("Cannot parse seconds in '{s}': {e}"))?;
                        (secs, 0)
                    };
                    total_nanos += (hours * 3_600 + minutes * 60 + secs) * 1_000_000_000
                        + nanos
                        + offset_nanos;
                    remaining = &remaining[time_part.len()..];
                    continue;
                }
            }

            let mut parts = remaining.splitn(2, char::is_whitespace);
            let number_str = parts.next().unwrap_or("");
            remaining = parts.next().unwrap_or("").trim();

            let number: i64 = number_str.parse().map_err(|e| {
                format!("Cannot parse number '{number_str}' in interval '{s}': {e}")
            })?;

            let mut unit_parts = remaining.splitn(2, char::is_whitespace);
            let unit = unit_parts.next().unwrap_or("");
            remaining = unit_parts.next().unwrap_or("");

            let nanos_per_unit: i64 = match unit {
                "year" | "years" => 365 * 24 * 3_600 * 1_000_000_000,
                "mon" | "mons" | "month" | "months" => 30 * 24 * 3_600 * 1_000_000_000,
                "week" | "weeks" => 7 * 24 * 3_600 * 1_000_000_000,
                "day" | "days" => 24 * 3_600 * 1_000_000_000,
                "hour" | "hours" => 3_600 * 1_000_000_000,
                "minute" | "minutes" | "min" | "mins" => 60 * 1_000_000_000,
                "second" | "seconds" | "sec" | "secs" => 1_000_000_000,
                "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => 1_000_000,
                "microsecond" | "microseconds" | "us" | "usec" | "usecs" => 1_000,
                other => return Err(format!("Unknown interval unit '{other}' in '{s}'")),
            };
            total_nanos += number * nanos_per_unit;
        }

        Ok(EngineDuration::new(total_nanos))
    }

    fn parse_values_from_event(
        event_type: DataEventType,
        raw: &HashMap<String, JsonValue>,
        table_ctx: &TableContext,
    ) -> ReaderContext {
        let values_map: ValuesMap = table_ctx
            .value_fields
            .iter()
            .map(|field| {
                let json_val = raw.get(&field.name).unwrap_or(&JsonValue::Null);
                let result = Self::parse_value_from_postgres(json_val, &field.type_, &field.name);
                (field.name.clone(), result)
            })
            .collect::<HashMap<_, _>>()
            .into();

        let key = table_ctx.key_field_names.as_deref().map(|key_names| {
            key_names
                .iter()
                .map(|name| {
                    values_map
                        .get(name)
                        .and_then(|r| r.as_ref().ok())
                        .cloned()
                        .unwrap_or(Value::None)
                })
                .collect::<Vec<Value>>()
        });

        ReaderContext::Diff((event_type, key, values_map))
    }

    fn parse_incoming_event(
        table_ctx: &TableContext,
        event: ChangeEvent,
        total_entries_read: &mut usize,
    ) -> Result<Vec<PgEventAfterParsing>, ReplicationError> {
        let event_type = if matches!(event.event_type, EventType::Delete { .. }) {
            DataEventType::Delete
        } else {
            DataEventType::Insert
        };

        if table_ctx.is_append_only
            && matches!(
                event.event_type,
                EventType::Truncate(_) | EventType::Update { .. } | EventType::Delete { .. }
            )
        {
            return Err(ReplicationError::AppendOnlyNotRespected(event));
        }

        Ok(match event.event_type {
            EventType::Begin { transaction_id, .. } => vec![PgEventAfterParsing::SimpleChange(
                ReadResult::NewSource(PostgresMetadata::new(Some(transaction_id)).into()),
            )],
            EventType::Commit { .. } => vec![PgEventAfterParsing::SimpleChange(
                ReadResult::FinishedSource {
                    commit_possibility: CommitPossibility::Possible,
                },
            )],
            EventType::Truncate(tables_from_event) => {
                let expected_full_name =
                    format!("{}.{}", table_ctx.schema_name, table_ctx.table_name);
                let has_target_table = tables_from_event.iter().any(|t| **t == *expected_full_name);
                if !has_target_table {
                    return Ok(Vec::new());
                }
                vec![PgEventAfterParsing::Truncate]
            }
            EventType::Update {
                schema: schema_from_event,
                table: table_from_event,
                old_data,
                new_data,
                ..
            } => {
                if *table_from_event != *table_ctx.table_name
                    || *schema_from_event != *table_ctx.schema_name
                {
                    return Ok(Vec::new());
                }
                let mut result = Vec::with_capacity(2);
                if let Some(old_data) = old_data {
                    result.push(PgEventAfterParsing::SimpleChange(ReadResult::Data(
                        Self::parse_values_from_event(
                            DataEventType::Delete,
                            &old_data.into_hash_map(),
                            table_ctx,
                        ),
                        emit_offset(total_entries_read),
                    )));
                }
                result.push(PgEventAfterParsing::SimpleChange(ReadResult::Data(
                    Self::parse_values_from_event(
                        DataEventType::Insert,
                        &new_data.into_hash_map(),
                        table_ctx,
                    ),
                    emit_offset(total_entries_read),
                )));
                result
            }
            EventType::Insert {
                table: table_from_event,
                data,
                schema: schema_from_event,
                ..
            }
            | EventType::Delete {
                table: table_from_event,
                old_data: data,
                schema: schema_from_event,
                ..
            } => {
                if *table_from_event != *table_ctx.table_name
                    || *schema_from_event != *table_ctx.schema_name
                {
                    return Ok(Vec::new());
                }
                let ctx =
                    Self::parse_values_from_event(event_type, &data.into_hash_map(), table_ctx);
                vec![PgEventAfterParsing::SimpleChange(ReadResult::Data(
                    ctx,
                    emit_offset(total_entries_read),
                ))]
            }
            _ => Vec::new(),
        })
    }

    fn get_next_records(
        &mut self,
        total_entries_read: &mut usize,
    ) -> Result<Vec<PgEventAfterParsing>, ReadError> {
        let table_ctx = &self.table_ctx;
        self.runtime.block_on(async {
            loop {
                match self.stream.next_event_with_retry(&self.cancel_token).await {
                    Ok(event) => {
                        self.stream
                            .shared_lsn_feedback
                            .update_applied_lsn(event.lsn.value());
                        let parsed_events =
                            Self::parse_incoming_event(table_ctx, event, total_entries_read)?;
                        if !parsed_events.is_empty() {
                            return Ok(parsed_events);
                        }
                    }
                    Err(pg_walstream::ReplicationError::Cancelled(_)) => {
                        info!("Cancelled, shutting down gracefully");
                        return Ok(vec![PgEventAfterParsing::SimpleChange(
                            ReadResult::Finished,
                        )]);
                    }
                    Err(e) => return Err(ReadError::PostgresReplication(e.into())),
                }
            }
        })
    }
}

pub struct ReplicationSettings {
    pub connection_string: String,
    pub publication_name: String,
    pub replication_slot_name: Option<String>,
    pub snapshot_name: Option<String>,
}

pub struct PsqlReader {
    client: PsqlClient,
    table_ctx: TableContext,
    prepared_records: Vec<ReadResult>,
    wal_reader: Option<WalReader>,
    collection_keys: HashSet<Option<Vec<Value>>>,
    is_initialized: bool,
    total_entries_read: usize,
}

impl PsqlReader {
    pub fn new(
        mut client: PsqlClient,
        replication_settings: Option<ReplicationSettings>,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        is_append_only: bool,
    ) -> Result<Self, ReadError> {
        let table_ctx = TableContext::new(
            schema_name,
            table_name,
            value_fields,
            key_field_names,
            is_append_only,
        );

        Self::validate_table_schema(&mut client, &table_ctx)?;

        let wal_reader = if let Some(replication_settings) = replication_settings {
            let rows = client
                .query(
                    "SELECT 1 FROM pg_publication WHERE pubname = $1",
                    &[&replication_settings.publication_name],
                )
                .map_err(ReplicationError::Query)?;
            if rows.is_empty() {
                return Err(ReplicationError::PublicationNotFound {
                    name: replication_settings.publication_name.clone(),
                }
                .into());
            }
            Some(WalReader::new(replication_settings, table_ctx.clone())?)
        } else {
            None
        };

        Ok(Self {
            client,
            table_ctx,
            prepared_records: Vec::new(),
            wal_reader,
            collection_keys: HashSet::new(),
            is_initialized: false,
            total_entries_read: 0,
        })
    }

    fn validate_table_schema(
        client: &mut PsqlClient,
        table_ctx: &TableContext,
    ) -> Result<(), ReplicationError> {
        let columns_query = "
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
        ";
        let column_rows = client.query(
            columns_query,
            &[&table_ctx.schema_name, &table_ctx.table_name],
        )?;
        let actual_columns: HashSet<String> =
            column_rows.iter().map(|r| r.get::<_, String>(0)).collect();

        let missing: Vec<String> = table_ctx
            .value_fields
            .iter()
            .map(|f| f.name.clone())
            .filter(|name| !actual_columns.contains(name))
            .collect();
        if !missing.is_empty() {
            return Err(ReplicationError::MissingValueFields {
                schema: table_ctx.schema_name.clone(),
                table: table_ctx.table_name.clone(),
                missing_fields: missing,
            });
        }

        if !table_ctx.is_append_only {
            let pk_query = "
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema    = kcu.table_schema
                AND tc.table_name      = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
                AND tc.table_name   = $2
                ORDER BY kcu.ordinal_position
            ";
            let pk_rows =
                client.query(pk_query, &[&table_ctx.schema_name, &table_ctx.table_name])?;
            let actual_pk: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

            if actual_pk.is_empty() {
                return Err(ReplicationError::NoPrimaryKey {
                    schema: table_ctx.schema_name.clone(),
                    table: table_ctx.table_name.clone(),
                });
            }

            if let Some(ref given_keys) = table_ctx.key_field_names {
                let mut given_sorted = given_keys.clone();
                let mut actual_sorted = actual_pk.clone();
                given_sorted.sort();
                actual_sorted.sort();

                if given_sorted != actual_sorted {
                    return Err(ReplicationError::PrimaryKeyMismatch {
                        schema: table_ctx.schema_name.clone(),
                        table: table_ctx.table_name.clone(),
                        expected: given_keys.clone(),
                        actual: actual_pk,
                    });
                }
            }
        }
        Ok(())
    }

    fn initialize_prepared_records(&mut self) -> Result<(), ReadError> {
        self.prepared_records = Self::read_prepared_records(
            &mut self.client,
            &self.table_ctx,
            self.wal_reader
                .as_ref()
                .map(|wr| wr.effective_snapshot_name.clone()),
            &mut self.total_entries_read,
        )?;

        if let Some(ref mut wal_reader) = self.wal_reader {
            wal_reader.start_replication()?;
        }

        self.is_initialized = true;

        Ok(())
    }

    fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn read_prepared_records(
        client: &mut PsqlClient,
        table_ctx: &TableContext,
        snapshot_name: Option<String>,
        total_entries_read: &mut usize,
    ) -> Result<Vec<ReadResult>, ReadError> {
        let has_txn = snapshot_name.is_some();
        if let Some(snapshot_name) = snapshot_name {
            client.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;", &[])?;
            if let Err(e) = client.execute(
                &format!(
                    "SET TRANSACTION SNAPSHOT {};",
                    Self::quote_literal(&snapshot_name)
                ),
                &[],
            ) {
                error!("The specified snapshot is no longer available");
                return Err(ReadError::Postgres(e));
            }
        }
        let prepared_records =
            Self::select_prepared_records(client, table_ctx, total_entries_read)?;
        if has_txn {
            client.execute("COMMIT;", &[])?;
        }
        Ok(prepared_records)
    }

    fn precise_parsed_value(
        value_field: &ValueField,
        parsed_value: Result<Value, Box<ConversionError>>,
    ) -> Result<Value, Box<ConversionError>> {
        let parsed_value = parsed_value?;

        let make_error = |value: &Value, message: &str| {
            Box::new(ConversionError {
                value_repr: format!("{value:?}"),
                field_name: value_field.name.clone(),
                type_: value_field.type_.clone(),
                original_error_message: message.to_string(),
            })
        };

        if parsed_value == Value::None {
            return value_field
                .type_
                .is_optional()
                .then_some(parsed_value)
                .ok_or_else(|| make_error(&Value::None, "non-optional value is undefined"));
        }

        Self::precise_value_by_type(parsed_value.clone(), &value_field.type_)
            .map_err(|e| make_error(&parsed_value, &e))
    }

    /// Converts a parsed value to the precise type required by the schema, recursing into
    /// `List` and `Tuple` containers. Handles `Duration` (stored as microseconds integer),
    /// `Pointer` (stored as string), and `PyObjectWrapper` (stored as serialized bytes).
    fn precise_value_by_type(value: Value, type_: &Type) -> Result<Value, String> {
        if value == Value::None {
            return if type_.is_optional() {
                Ok(Value::None)
            } else {
                Err("non-optional value is undefined".to_string())
            };
        }

        match type_.unoptionalize() {
            Type::PyObjectWrapper => Self::precise_py_object_wrapper(&value),
            Type::Duration => Self::precise_duration(value),
            Type::Pointer => Self::precise_pointer(&value),
            Type::List(element_type) => Self::precise_list(value, element_type),
            Type::Tuple(element_types) => Self::precise_tuple(value, element_types),
            Type::Array(ndim, element_type) => {
                Self::precise_array(value, ndim.as_ref(), element_type)
            }
            _ => Ok(value),
        }
    }

    fn precise_py_object_wrapper(value: &Value) -> Result<Value, String> {
        let Value::Bytes(bytes) = value else {
            return Err(format!("unexpected type for PyObjectWrapper: {value:?}"));
        };
        bincode::deserialize::<Value>(bytes).map_err(|e| e.to_string())
    }

    fn precise_duration(value: Value) -> Result<Value, String> {
        if matches!(value, Value::Duration(_)) {
            return Ok(value);
        }
        let Value::Int(i) = value else {
            return Err(format!("unexpected type for Duration: {value:?}"));
        };
        EngineDuration::new_with_unit(i, "us")
            .map(Value::Duration)
            .map_err(|e| e.to_string())
    }

    fn precise_pointer(value: &Value) -> Result<Value, String> {
        let Value::String(s) = value else {
            return Err(format!("unexpected type for Pointer: {value:?}"));
        };
        parse_pathway_pointer(s).map_err(|e| e.to_string())
    }

    fn precise_list(value: Value, element_type: &Type) -> Result<Value, String> {
        let Value::Tuple(elements) = value else {
            return Err(format!("expected Tuple for List type, got {value:?}"));
        };
        let converted: Result<Vec<Value>, _> = elements
            .iter()
            .map(|v| Self::precise_value_by_type(v.clone(), element_type))
            .collect();
        converted.map(Value::from)
    }

    fn precise_tuple(value: Value, element_types: &[Type]) -> Result<Value, String> {
        let Value::Tuple(elements) = value else {
            return Err(format!("expected Tuple for Tuple type, got {value:?}"));
        };
        if elements.len() != element_types.len() {
            return Err(format!(
                "tuple length mismatch: expected {}, got {}",
                element_types.len(),
                elements.len()
            ));
        }
        let converted: Result<Vec<Value>, _> = elements
            .iter()
            .zip(element_types.iter())
            .map(|(v, t)| Self::precise_value_by_type(v.clone(), t))
            .collect();
        converted.map(Value::from)
    }

    fn precise_array(
        value: Value,
        ndim: Option<&usize>,
        element_type: &Type,
    ) -> Result<Value, String> {
        // from_sql does not know the target type and returns `Value::Tuple`.
        // Convert it to `Value::IntArray` or `Value::FloatArray` with the correct shape.
        let Value::Tuple(_) = &value else {
            // Verify that the value is already the expected array type (e.g. pgvector returned
            // `Value::FloatArray`). Any other value — a scalar float, date, etc. — is an error.
            let is_expected = match element_type {
                Type::Int => matches!(value, Value::IntArray(_)),
                Type::Float => matches!(value, Value::FloatArray(_)),
                _ => false,
            };
            if is_expected {
                return Ok(value);
            }
            return Err(format!(
                "expected an array value for element type {element_type}, got {value:?}"
            ));
        };

        let (shape, flat) = Self::flatten_tuple_with_shape(&value)?;

        // ndim == 0 means "any dimensionality" in the schema.
        if ndim != Some(&0) && Some(shape.len()) != ndim.copied() {
            return Err(format!(
                "expected array with {:?} dimensions, got {}",
                ndim,
                shape.len()
            ));
        }

        match element_type {
            Type::Int => {
                let ints: Result<Vec<i64>, _> = flat
                    .iter()
                    .map(|v| match v {
                        Value::Int(i) => Ok(*i),
                        Value::None => Err("unexpected NULL in non-optional int array"),
                        _ => Err("unexpected non-int element in int array"),
                    })
                    .collect();
                let ints = ints.map_err(std::string::ToString::to_string)?;
                ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), ints)
                    .map(Value::from)
                    .map_err(|e| e.to_string())
            }
            Type::Float => {
                let floats: Result<Vec<f64>, _> = flat
                    .iter()
                    .map(|v| match v {
                        Value::Float(OrderedFloat(f)) => Ok(*f),
                        #[allow(clippy::cast_precision_loss)]
                        Value::Int(i) => Ok(*i as f64),
                        Value::None => Err("unexpected NULL in non-optional float array"),
                        _ => Err("unexpected non-float element in float array"),
                    })
                    .collect();
                let floats = floats.map_err(std::string::ToString::to_string)?;
                ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), floats)
                    .map(Value::from)
                    .map_err(|e| e.to_string())
            }
            other => Err(format!("unsupported element type for Array: {other}")),
        }
    }

    /// Recursively traverses nested `Value::Tuple`, returning the shape and flat leaf elements.
    /// Validates that the array is rectangular (all rows have equal length).
    fn flatten_tuple_with_shape(value: &Value) -> Result<(Vec<usize>, Vec<Value>), String> {
        let mut flat = Vec::new();
        let shape = Self::collect_shape(value, &mut flat, 0)?;
        Ok((shape, flat))
    }

    /// Recursively collects the elements of nested `Value::Tuple` into `flat`.
    /// Returns the shape as a Vec<usize> where each element is the size of that dimension.
    fn collect_shape(
        value: &Value,
        flat: &mut Vec<Value>,
        depth: usize,
    ) -> Result<Vec<usize>, String> {
        match value {
            Value::Tuple(elements) => {
                let len = elements.len();
                if len == 0 {
                    return Ok(vec![0]);
                }

                let mut first_inner_shape: Option<Vec<usize>> = None;
                for elem in elements.iter() {
                    match elem {
                        Value::Tuple(_) => {
                            let inner_shape = Self::collect_shape(elem, flat, depth + 1)?;
                            if let Some(ref expected) = first_inner_shape {
                                if *expected != inner_shape {
                                    return Err(format!(
                                    "jagged array at depth {depth}: expected inner shape {expected:?}, got {inner_shape:?}"
                                ));
                                }
                            } else {
                                first_inner_shape = Some(inner_shape);
                            }
                        }
                        leaf => {
                            if first_inner_shape.is_some() {
                                return Err(format!(
                                "mixed nesting at depth {depth}: expected nested Tuple, got scalar"
                            ));
                            }
                            flat.push(leaf.clone());
                        }
                    }
                }

                let mut shape = vec![len];
                if let Some(inner) = first_inner_shape {
                    shape.extend(inner);
                }
                Ok(shape)
            }
            // Scalar leaf reached at the top level — should not normally occur.
            leaf => {
                flat.push(leaf.clone());
                Ok(vec![])
            }
        }
    }

    /// Escapes a `PostgreSQL` identifier by wrapping it in double quotes
    /// and doubling any internal double-quote characters.
    fn quote_identifier(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn select_prepared_records(
        client: &mut PsqlClient,
        table_ctx: &TableContext,
        total_entries_read: &mut usize,
    ) -> Result<Vec<ReadResult>, ReadError> {
        let select_all_query = format!(
            "SELECT {} FROM {}.{}",
            table_ctx
                .value_fields
                .iter()
                .map(|vf| Self::quote_identifier(&vf.name))
                .join(","),
            Self::quote_identifier(&table_ctx.schema_name),
            Self::quote_identifier(&table_ctx.table_name),
        );
        let snapshot = client.query(&select_all_query, &[])?;

        let mut prepared_records = Vec::with_capacity(snapshot.len() + 2);
        prepared_records.push(ReadResult::NewSource(PostgresMetadata::new(None).into()));
        for row in snapshot {
            let mut parsed_row_value = HashMap::with_capacity(row.len());
            for (index, value_field) in table_ctx.value_fields.iter().enumerate().take(row.len()) {
                let parsed_value: Result<Value, Box<ConversionError>> =
                    row.try_get(index).map_err(|err| {
                        Box::new(ConversionError {
                            value_repr: format!("{row:?}"),
                            field_name: value_field.name.clone(),
                            type_: value_field.type_.clone(),
                            original_error_message: format!("{err}"),
                        })
                    });
                let parsed_value = Self::precise_parsed_value(value_field, parsed_value);
                parsed_row_value.insert(value_field.name.clone(), parsed_value);
            }

            let (key_parts, is_correct) =
                if let Some(key_field_names) = table_ctx.key_field_names.as_deref() {
                    let mut key_parts = Vec::with_capacity(key_field_names.len());
                    let mut is_correct = true;
                    for name in key_field_names {
                        if let Some(Ok(value)) = parsed_row_value.get(name) {
                            key_parts.push(value.clone());
                        } else {
                            is_correct = false;
                            break;
                        }
                    }
                    (Some(key_parts), is_correct)
                } else {
                    (None, true)
                };
            if !is_correct {
                warn!("Failed to parse the primary key part from the row: {parsed_row_value:?}");
                continue;
            }

            let reader_context =
                ReaderContext::from_diff(DataEventType::Insert, key_parts, parsed_row_value.into());
            prepared_records.push(ReadResult::Data(
                reader_context,
                emit_offset(total_entries_read),
            ));
        }
        prepared_records.push(ReadResult::FinishedSource {
            commit_possibility: CommitPossibility::Possible,
        });
        prepared_records.reverse();

        Ok(prepared_records)
    }
}

impl Reader for PsqlReader {
    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        Err(ReadError::PersistenceNotSupported(StorageType::Postgres))
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if !self.is_initialized {
            self.initialize_prepared_records()?;
        }

        if let Some(prepared_record) = self.prepared_records.pop() {
            if let ReadResult::Data(ReaderContext::Diff((event_type, ref key, _)), (_, _)) =
                prepared_record
            {
                match event_type {
                    DataEventType::Insert => self.collection_keys.insert(key.clone()),
                    DataEventType::Delete => self.collection_keys.remove(key),
                };
            }
            return Ok(prepared_record);
        }

        if let Some(ref mut wal_reader) = self.wal_reader {
            while self.prepared_records.is_empty() {
                let mut parsed_pg_events = Vec::new();
                while parsed_pg_events.is_empty() {
                    parsed_pg_events = wal_reader.get_next_records(&mut self.total_entries_read)?;
                }
                for event in parsed_pg_events {
                    // TODO: have a clearer invariant here (truncate can only be the single item)
                    if let PgEventAfterParsing::SimpleChange(prepared_record) = event {
                        self.prepared_records.push(prepared_record);
                    } else {
                        for key in &self.collection_keys {
                            self.prepared_records.push(ReadResult::Data(
                                ReaderContext::Diff((
                                    DataEventType::Delete,
                                    key.clone(),
                                    HashMap::with_capacity(0).into(),
                                )),
                                emit_offset(&mut self.total_entries_read),
                            ));
                        }
                    }
                }
            }
            Ok(self.prepared_records.pop().unwrap())
        } else {
            Ok(ReadResult::Finished)
        }
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Postgres
    }
}

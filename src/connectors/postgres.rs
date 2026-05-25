use itertools::Itertools;
use log::{error, info, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

use native_tls::Error as NativeTlsError;
use native_tls::{Certificate, TlsConnector};
use ndarray::ArrayD;
use ordered_float::OrderedFloat;
use pg_walstream::{
    CancellationToken, ChangeEvent, ColumnValue, EventType, LogicalReplicationStream,
    ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig, RowData, StreamingMode,
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
    format_error_chain, CommitPossibility, ConversionError, SqlQueryTemplate, TableContext,
    TableWriterInitMode, ValuesMap,
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
use crate::retry::execute_with_retries_if;

#[derive(Debug, thiserror::Error)]
pub enum SslError {
    #[error(transparent)]
    Tls(#[from] NativeTlsError),

    #[error("ssl certificate is not provided")]
    CertificateNotProvided,

    #[error("ssl mode is not expected in the raw connection string, it is deduced downstream")]
    UnexpectedSslMode,
}

/// Top-level `PostgreSQL` error type. Every postgres-specific failure
/// — raw driver errors, TLS handshake failures, WAL replication
/// errors, query/preflight validation errors — lives here so that
/// `ReadError` and `WriteError` only need to carry a single
/// `Postgres(#[from] PostgresError)` variant.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PostgresError {
    #[error(transparent)]
    Postgres(#[from] postgres::Error),

    #[error(transparent)]
    Ssl(#[from] SslError),

    #[error(transparent)]
    Replication(#[from] ReplicationError),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("query {query:?} failed: {}", format_error_chain(.error))]
    PsqlQueryFailed {
        query: String,
        error: postgres::Error,
    },

    #[error(
        "destination table \"{schema}\".\"{table}\" has generated column(s) {columns:?} \
         (GENERATED ALWAYS AS ... / AS IDENTITY) that pw.Schema also names; \
         drop them from the schema — the reader will still return their computed \
         values on SELECT, or use init_mode=\"replace\"/\"create_if_not_exists\" to \
         have Pathway recreate the table without generated columns"
    )]
    GeneratedColumnInSchema {
        schema: String,
        table: String,
        columns: Vec<String>,
    },

    #[error(
        "primary_key columns {primary_key:?} do not correspond to any UNIQUE or \
         PRIMARY KEY constraint on \"{schema}\".\"{table}\". Add one in PostgreSQL \
         (ALTER TABLE ... ADD PRIMARY KEY (...) or ALTER TABLE ... ADD UNIQUE (...)) \
         or set init_mode=\"replace\" / \"create_if_not_exists\" to have Pathway \
         recreate the table with a matching PRIMARY KEY"
    )]
    NoMatchingUniqueConstraint {
        schema: String,
        table: String,
        primary_key: Vec<String>,
    },

    #[error(
        "destination table \"{schema}\".\"{table}\" has NOT NULL column(s) \
         {columns:?} without a DEFAULT value that pw.Schema does not name; \
         add them to the Pathway schema, give them a DEFAULT in PostgreSQL, \
         or use init_mode=\"replace\" / \"create_if_not_exists\" to have \
         Pathway recreate the table"
    )]
    ExtraNotNullColumns {
        schema: String,
        table: String,
        columns: Vec<String>,
    },

    #[error(
        "pw.Schema references column(s) {columns:?} that do not exist in \
         destination table \"{schema}\".\"{table}\"; check the spelling, \
         or use init_mode=\"replace\" / \"create_if_not_exists\" to have \
         Pathway recreate the table with these columns"
    )]
    WriterMissingDestinationColumns {
        schema: String,
        table: String,
        columns: Vec<String>,
    },

    #[error(
        "destination \"{schema}\".\"{table}\" is a {kind}, not a regular \
         table; choose a different table_name, drop the {kind} in \
         PostgreSQL first, or refine your pipeline to read from it instead"
    )]
    DestinationIsNotATable {
        schema: String,
        table: String,
        kind: &'static str,
    },

    #[error(
        "schema \"{schema}\" does not exist in the PostgreSQL database; \
         create it (CREATE SCHEMA \"{schema}\") or fix the schema_name parameter"
    )]
    SchemaDoesNotExist { schema: String },

    #[error(
        "destination column \"{schema}\".\"{table}\".\"{column}\" has \
         PostgreSQL type '{actual_udt}' which is not compatible with the \
         declared Pathway type '{declared}'"
    )]
    WriterColumnTypeMismatch {
        schema: String,
        table: String,
        column: String,
        declared: String,
        actual_udt: String,
    },

    #[error(
        "destination table \"{schema}\".\"{table}\" has metadata column \
         \"{column}\" declared as '{actual_udt}', which is too narrow — \
         Pathway emits a 64-bit '{expected}' value there (timestamps in \
         milliseconds since epoch). Use BIGINT for 'time' / SMALLINT or \
         BIGINT for 'diff'"
    )]
    MetadataColumnTooNarrow {
        schema: String,
        table: String,
        column: String,
        actual_udt: String,
        expected: &'static str,
    },

    #[error(
        "pw.Schema declares nullable column(s) {columns:?}, but destination \
         \"{schema}\".\"{table}\" marks them NOT NULL. A None row would be \
         rejected by PostgreSQL at flush time and panic the writer worker. \
         Either drop the Optional wrapper in the schema (after filtering out \
         nulls upstream, e.g. via .filter(t.x.is_not_none())), make the \
         destination column nullable (ALTER TABLE ... ALTER COLUMN ... DROP \
         NOT NULL), or use init_mode=\"replace\" / \"create_if_not_exists\" \
         so Pathway recreates the table with matching nullability"
    )]
    OptionalSchemaVsNotNullDestination {
        schema: String,
        table: String,
        columns: Vec<String>,
    },
}

const SSLMODE_PARAM: &str = "sslmode=";
const SSLMODE_DISABLE: &str = "sslmode=disable";
const SSLMODE_REQUIRE: &str = "sslmode=require";

/// Escapes a `PostgreSQL` identifier by wrapping it in double quotes
/// and doubling any internal double-quote characters. Shared by the
/// reader and writer so that table and schema names containing
/// characters that require quoting (hyphens, mixed case, reserved
/// words, ...) are accepted uniformly on both sides.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

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
    ) -> Result<PsqlClient, PostgresError> {
        if connection_string.contains(SSLMODE_PARAM) {
            return Err(PostgresError::Ssl(SslError::UnexpectedSslMode));
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
    ) -> Result<MakeTlsConnector, PostgresError> {
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
                    ssl_cert_path.ok_or(PostgresError::Ssl(SslError::CertificateNotProvided))?;
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
) -> Result<PsqlClient, PostgresError> {
    ssl_mode.connect(connection_string, ssl_cert_path)
}

/// Bundle of inputs sufficient to (re)open a `PostgreSQL` connection.
/// Held by `PsqlWriter` so that a transient failure on the flush path
/// can drop the current `PsqlClient` and rebuild a fresh one from the
/// same parameters without plumbing four positional arguments through
/// every retry call site.
#[derive(Clone)]
pub struct PsqlConnectionConfig {
    pub connection_string: String,
    pub ssl_mode: SslMode,
    pub ssl_cert_path: Option<String>,
}

impl PsqlConnectionConfig {
    pub fn connect(&self) -> Result<PsqlClient, PostgresError> {
        create_psql_client(
            &self.connection_string,
            self.ssl_mode,
            self.ssl_cert_path.clone(),
        )
    }
}

/// Number of times the writer retries a transient failure before giving
/// up. Matches the MSSQL connector's `MAX_MSSQL_RETRIES` so operators see
/// comparable behavior across the two SQL backends.
const MAX_PSQL_RETRIES: usize = 3;

/// Classify a `postgres::Error` as transient — i.e. the operation has a
/// realistic chance of succeeding on a fresh connection. Used to gate
/// the writer's retry loop so that real bugs (syntax errors,
/// constraint violations, missing tables, type mismatches) propagate
/// on the first attempt without burning backoff latency.
///
/// Two signals make an error transient:
/// 1. `postgres::Error::is_closed()` — the underlying TCP/TLS stream
///    is gone (PG restart, network blip, server-side timeout). The
///    next attempt will reconnect from scratch.
/// 2. SQLSTATE class `08` (connection exceptions) or class `57`
///    (operator intervention: `admin_shutdown`, `crash_shutdown`,
///    `cannot_connect_now`). Both are universally documented as
///    "retry the operation." `40001` (`serialization_failure`) and
///    `40P01` (`deadlock_detected`) are listed as "rerun the
///    transaction" by `PostgreSQL` and benefit from a backoff.
fn is_transient_pg_error(err: &postgres::Error) -> bool {
    if err.is_closed() {
        return true;
    }
    if let Some(db_err) = err.as_db_error() {
        let code = db_err.code().code();
        let class = code.get(..2).unwrap_or("");
        return class == "08" || class == "57" || code == "40001" || code == "40P01";
    }
    false
}

fn is_transient_pg_write(err: &WriteError) -> bool {
    let WriteError::Postgres(pg) = err else {
        return false;
    };
    match pg {
        PostgresError::Postgres(e) => is_transient_pg_error(e),
        PostgresError::PsqlQueryFailed { error, .. } => is_transient_pg_error(error),
        // A TLS handshake error during reconnect is transient — the
        // certificate / mode is unchanged across retries, so a
        // failure here is a transport-level issue (the freshly
        // reconnected stream failed to negotiate). Try again.
        PostgresError::Ssl(_) => true,
        _ => false,
    }
}

pub struct PsqlWriter {
    /// `None` between a failed flush and the next reconnect. Held as
    /// `Option` so that the retry loop can drop a poisoned connection
    /// and reconnect on the next attempt without forcing an explicit
    /// `is_closed()` probe.
    client: Option<PsqlClient>,
    /// Stored for reconnect after a transient failure drops `client`.
    /// We can't keep the original `PsqlClient` factory closure on the
    /// struct (the closure captures `tls.root_cert_path` which is
    /// non-`Copy`), so we just keep the inputs and re-call
    /// `create_psql_client` on the retry path.
    connection_string: String,
    ssl_mode: SslMode,
    ssl_cert_path: Option<String>,
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
    use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
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
        #[allow(clippy::too_many_lines)]
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            // PostgreSQL ``DOMAIN`` types share the wire format of the
            // wrapped base type. rust-postgres' built-in ``ToSql``
            // ``accepts`` impls match the *exact* ``Type`` (not the
            // kind), so they reject every domain — without this
            // unwrap, writing into a DOMAIN column would fall through
            // every branch and surface as an opaque
            // ``WrongPathwayType`` error at flush time. Recurse with
            // the base type so the rest of the function sees the
            // unwrapped scalar.
            if let postgres::types::Kind::Domain(base) = ty.kind() {
                return self.to_sql(base, out);
            }
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
                    // PostgreSQL UUID binary layout is the raw 16
                    // bytes of the uuid. Parse the user's string here
                    // (accepting both canonical `xxxxxxxx-xxxx-…` and
                    // hyphen-less hex forms, same as uuid::parse_str)
                    // when the destination column is UUID. A malformed
                    // value surfaces as a uuid::Error via `?` and gets
                    // wrapped into `PostgresError::PsqlQueryFailed` by
                    // the flush path — no opaque driver panic. TEXT
                    // remains the default for `str` when Pathway
                    // creates the table itself.
                    if matches!(*ty, Type::UUID) {
                        let uuid = uuid::Uuid::parse_str(s)?;
                        out.extend_from_slice(uuid.as_bytes());
                        return Ok(IsNull::No);
                    }
                    // ENUM types accept the raw UTF-8 label as their
                    // binary representation. rust-postgres' `&str`
                    // ToSql rejects `Kind::Enum`, so without this
                    // branch writing a Pathway `str` column into an
                    // ENUM column panicked the worker.
                    if let postgres::types::Kind::Enum(_) = ty.kind() {
                        out.extend_from_slice(s.as_bytes());
                        return Ok(IsNull::No);
                    }
                    // INET / CIDR binary wire format:
                    //   family (u8: 2=AF_INET, 3=AF_INET6)
                    //   prefix_len (u8)
                    //   is_cidr (u8: 0 for INET, 1 for CIDR)
                    //   addr_len (u8: 4 for IPv4, 16 for IPv6)
                    //   raw address bytes
                    // The reader side already emits the host/prefix
                    // form the same way libpq's `inet_out` does —
                    // matching it here closes the round-trip.
                    if matches!(*ty, Type::INET | Type::CIDR) {
                        let (host_str, prefix_override) = match s.split_once('/') {
                            Some((h, p)) => (
                                h,
                                Some(
                                    p.parse::<u8>()
                                        .map_err(|e| format!("Cannot parse prefix '{p}': {e}"))?,
                                ),
                            ),
                            None => (s.as_str(), None),
                        };
                        let ip: std::net::IpAddr = host_str
                            .parse()
                            .map_err(|e| format!("Cannot parse IP '{host_str}': {e}"))?;
                        let (family, addr_bytes): (u8, Vec<u8>) = match ip {
                            std::net::IpAddr::V4(v4) => (2, v4.octets().to_vec()),
                            std::net::IpAddr::V6(v6) => (3, v6.octets().to_vec()),
                        };
                        let max_prefix: u8 = if family == 2 { 32 } else { 128 };
                        let prefix_len = prefix_override.unwrap_or(max_prefix);
                        if prefix_len > max_prefix {
                            return Err(format!(
                                "prefix length {prefix_len} exceeds {max_prefix} for family {family}"
                            )
                            .into());
                        }
                        let addr_len = u8::try_from(addr_bytes.len()).expect("4 or 16");
                        let is_cidr = u8::from(matches!(*ty, Type::CIDR));
                        out.put_u8(family);
                        out.put_u8(prefix_len);
                        out.put_u8(is_cidr);
                        out.put_u8(addr_len);
                        out.extend_from_slice(&addr_bytes);
                        return Ok(IsNull::No);
                    }
                    // MACADDR: 6 raw bytes, MACADDR8: 8 raw bytes.
                    // The reader already formats as lowercase
                    // colon-separated hex, so we accept that form
                    // here (plus the hyphen separator that libpq
                    // also tolerates).
                    if matches!(*ty, Type::MACADDR | Type::MACADDR8) {
                        let expected_len = if matches!(*ty, Type::MACADDR) { 6 } else { 8 };
                        let hex_only: String = s
                            .chars()
                            .filter(|c| !matches!(c, ':' | '-' | '.'))
                            .collect();
                        if hex_only.len() != expected_len * 2 {
                            return Err(format!(
                                "MAC address '{s}' does not have {} hex digits",
                                expected_len * 2
                            )
                            .into());
                        }
                        let bytes = hex::decode(&hex_only)
                            .map_err(|e| format!("Cannot decode MAC '{s}': {e}"))?;
                        out.extend_from_slice(&bytes);
                        return Ok(IsNull::No);
                    }
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
                    // PostgreSQL DATE binary layout is an i32 count of
                    // days since 2000-01-01. Emit that directly when
                    // the destination column is DATE; any non-midnight
                    // time components are silently truncated, matching
                    // PostgreSQL's own implicit TIMESTAMP→DATE cast.
                    // TIMESTAMP remains the default CREATE TABLE type
                    // for DateTimeNaive (see `postgres_data_type`).
                    if matches!(*ty, Type::DATE) {
                        let naive_date = dt.as_chrono_datetime().date();
                        let pg_epoch = NaiveDate::from_ymd_opt(2000, 1, 1)
                            .expect("2000-01-01 is a valid date");
                        let days = (naive_date - pg_epoch).num_days();
                        let days_i32 = i32::try_from(days)?;
                        out.put_i32(days_i32);
                        return Ok(IsNull::No);
                    }
                    try_forward!(NaiveDateTime, dt.as_chrono_datetime());
                    "naive date/time"
                }
                Self::DateTimeUtc(dt) => {
                    try_forward!(DateTime<Utc>, dt.as_chrono_datetime().and_utc());
                    "UTC date/time"
                }
                Self::Duration(dr) => {
                    // Emit PostgreSQL's INTERVAL binary layout when
                    // the destination column is INTERVAL:
                    //   i64 microseconds, i32 days, i32 months
                    // We pack the entire duration into `microseconds`
                    // and leave days/months at zero so the round-trip
                    // through FromSql (which multiplies months by
                    // 30*86400*10^6) is exact. BIGINT remains the
                    // default CREATE TABLE type for Duration (see
                    // `postgres_data_type`); this branch keeps a
                    // pre-existing INTERVAL column usable.
                    if matches!(*ty, Type::INTERVAL) {
                        out.put_i64(dr.microseconds());
                        out.put_i32(0);
                        out.put_i32(0);
                        return Ok(IsNull::No);
                    }
                    // PostgreSQL TIME binary layout is i64 microseconds
                    // since midnight. The input connector already maps
                    // TIME columns to pw.Duration, so the natural
                    // round-trip would fail without this branch —
                    // rust-postgres' i64 ToSql does not accept Type::TIME.
                    if matches!(*ty, Type::TIME) {
                        out.put_i64(dr.microseconds());
                        return Ok(IsNull::No);
                    }
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
            // Defensive ``>=`` rather than ``==``: an upstream caller
            // that passes ``max_batch_size = 0`` (or any value smaller
            // than the buffer ever observes here) used to leave the
            // buffer growing without bound because the equality check
            // was never satisfied. Python rejects the zero case
            // up-front, so this branch only fires for a buffer that
            // happens to overshoot the configured size by a single
            // ``write`` — the loop guarantees a flush in that case.
            if self.buffer.len() >= max_batch_size {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        execute_with_retries_if(
            || self.flush_impl(),
            is_transient_pg_write,
            crate::retry::RetryConfig::default(),
            MAX_PSQL_RETRIES,
        )
    }

    fn name(&self) -> String {
        format!("Postgres({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}

impl PsqlWriter {
    /// Rebuild a fresh `PsqlClient` from the stored connection
    /// parameters. Called from `flush_impl` between retry attempts
    /// when the previous client was dropped because it observed a
    /// transient error.
    fn reconnect(&self) -> Result<PsqlClient, PostgresError> {
        create_psql_client(
            &self.connection_string,
            self.ssl_mode,
            self.ssl_cert_path.clone(),
        )
    }

    /// Inner flush body. Returns the buffer to `self.buffer` on
    /// failure so that the surrounding retry loop can re-attempt with
    /// the same rows, and drops `self.client` on failure so the next
    /// attempt reconnects from scratch rather than reusing a poisoned
    /// session. `Vec::drain(..)` cannot be used here — its iterator
    /// clears the remaining buffer on drop, which would silently drop
    /// rows on a retriable mid-flush failure.
    fn flush_impl(&mut self) -> Result<(), WriteError> {
        let buffer = take(&mut self.buffer);

        // Reconnect lazily — if a previous flush succeeded `client` is
        // still populated and we reuse it; if a previous flush failed
        // we dropped the client and connect now.
        if self.client.is_none() {
            match self.reconnect() {
                Ok(client) => self.client = Some(client),
                Err(e) => {
                    self.buffer = buffer;
                    return Err(e.into());
                }
            }
        }
        // ``unwrap`` is safe: the block above either populated
        // ``self.client`` or returned. ``expect`` documents the
        // invariant for future readers.
        let client = self
            .client
            .as_mut()
            .expect("client populated by reconnect above");

        let result = (|| -> Result<(), WriteError> {
            let mut transaction = client.transaction()?;
            for data in &buffer {
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
                    .map_err(|error| PostgresError::PsqlQueryFailed {
                        query: query.clone(),
                        error,
                    })?;
            }
            transaction.commit()?;
            Ok(())
        })();

        if let Err(e) = result {
            // Drop the poisoned client so the next retry reconnects
            // from scratch; restore the buffer so retries (and the
            // user, if all retries are exhausted) see the unflushed
            // rows.
            self.client = None;
            self.buffer = buffer;
            return Err(e);
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub fn new(
        connection_config: PsqlConnectionConfig,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        table_ctx: &TableContext,
        init_mode: TableWriterInitMode,
        legacy_mode: bool,
    ) -> Result<PsqlWriter, WriteError> {
        let mut client = connection_config.connect()?;
        let TableContext {
            schema_name,
            table_name,
            value_fields,
            key_field_names,
        } = table_ctx;
        let value_fields = value_fields.as_slice();
        let key_field_names = key_field_names.as_deref();
        // Compose the fully qualified target as quote(schema).quote(table)
        // so generated SQL targets the right schema and so identifiers
        // PostgreSQL accepts only when quoted (hyphens, mixed case,
        // reserved words) survive interpolation. The `quote_ident`
        // callback handles column and primary-key names uniformly
        // inside the shared template builders.
        let quoted_table_name = format!(
            "{}.{}",
            quote_identifier(schema_name),
            quote_identifier(table_name),
        );
        // When the user opts out of table creation (`init_mode=default`),
        // the destination table may contain `GENERATED ALWAYS AS (...)
        // STORED` columns or `GENERATED ALWAYS AS IDENTITY` columns
        // that are not writable through a plain INSERT. Without this
        // preflight, PostgreSQL rejects the generated INSERT at flush
        // time and the worker panics with an opaque `db error`. Surface
        // it as a clean Pathway-level error at pipeline start so the
        // user knows which column to drop from the schema.
        // A non-existent schema_name is a user-facing configuration
        // error — without this check, every init_mode panics the
        // worker with an opaque "db error" from libpq.
        Self::validate_schema_exists(&mut client, schema_name)?;

        // If the destination name already exists as a view, sequence,
        // index, or other non-table relation, every downstream step
        // (CREATE TABLE on init_mode=Replace/CreateIfNotExists, or
        // INSERT on init_mode=Default) fails with an opaque driver
        // error. Detect it up-front regardless of init_mode.
        Self::validate_destination_is_table_or_absent(&mut client, schema_name, table_name)?;

        // Preflights run whenever the table may already exist and
        // not be overwritten: `Default` (we never touch it) and
        // `CreateIfNotExists` (no-op CREATE when the table exists,
        // falling through to INSERT against the user's schema).
        // `Replace` drops + recreates the table, so the pre-existing
        // shape is irrelevant there.
        let validate_existing = matches!(
            init_mode,
            TableWriterInitMode::Default | TableWriterInitMode::CreateIfNotExists
        );
        if validate_existing {
            // The next three checks all read from
            // `information_schema.columns` for the same table. Do them
            // in declared order so the first failing one surfaces the
            // most specific message: (1) schema column doesn't exist,
            // (2) schema column matches a GENERATED ALWAYS column,
            // (3) destination carries an extra NOT NULL.
            let appends_time_diff = !snapshot_mode || legacy_mode;
            Self::validate_schema_columns_exist_in_destination(
                &mut client,
                schema_name,
                table_name,
                value_fields,
                appends_time_diff,
            )?;
            // Reject destinations whose column type is incompatible
            // with the Pathway declared type. Without this check the
            // INSERT panics the worker with a bare "error serializing
            // parameter N" from the driver at flush time.
            Self::validate_destination_column_types(
                &mut client,
                schema_name,
                table_name,
                value_fields,
            )?;
            // Pathway emits i64 for the `time` metadata column
            // (milliseconds since epoch, routinely > i32::MAX) and
            // i16 for `diff`. A destination that pre-declares them
            // with narrower types would silently accept small test
            // values but overflow in production.
            if appends_time_diff {
                Self::validate_time_diff_column_widths(&mut client, schema_name, table_name)?;
            }
            Self::validate_no_always_generated_columns(
                &mut client,
                schema_name,
                table_name,
                value_fields,
            )?;
            // Reject destination columns declared NOT NULL without a
            // DEFAULT that the writer's INSERT would silently omit.
            // Stream-of-changes mode injects the `time`/`diff`
            // metadata columns itself; snapshot / legacy modes don't.
            Self::validate_no_extra_not_null_columns(
                &mut client,
                schema_name,
                table_name,
                value_fields,
                appends_time_diff,
            )?;
            // Reject Optional[T] schema columns writing into NOT NULL
            // destination columns — a single None row would otherwise
            // panic the worker with a constraint-violation error that
            // can land arbitrarily far into the job's lifetime.
            Self::validate_nullability_matches_destination(
                &mut client,
                schema_name,
                table_name,
                value_fields,
            )?;
            // For snapshot mode, the generated INSERT ... ON CONFLICT
            // (...) DO UPDATE statement requires a full UNIQUE or
            // PRIMARY KEY constraint on the primary_key columns.
            // `init_mode=replace` / `create_if_not_exists` paths create
            // the table with PRIMARY KEY themselves, so this check is
            // only needed when the user pre-created the table.
            if snapshot_mode {
                if let Some(keys) = key_field_names {
                    Self::validate_matching_unique_constraint(
                        &mut client,
                        schema_name,
                        table_name,
                        keys,
                    )?;
                }
            }
        }
        let mut transaction = client.transaction()?;
        init_mode.initialize(
            &quoted_table_name,
            value_fields,
            key_field_names,
            !snapshot_mode || legacy_mode,
            |query| {
                transaction.execute(query, &[])?;
                Ok(())
            },
            Self::postgres_data_type,
            quote_identifier,
        )?;
        transaction.commit()?;

        let query = SqlQueryTemplate::new(
            snapshot_mode,
            &quoted_table_name,
            value_fields,
            key_field_names,
            legacy_mode,
            |index| format!("${}", index + 1),
            Self::on_insert_conflict_condition,
            quote_identifier,
        )?;

        let PsqlConnectionConfig {
            connection_string,
            ssl_mode,
            ssl_cert_path,
        } = connection_config;
        let writer = PsqlWriter {
            client: Some(client),
            connection_string,
            ssl_mode,
            ssl_cert_path,
            max_batch_size,
            query,
            buffer: Vec::new(),
            snapshot_mode,
            table_name: table_name.clone(),
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

    /// Rejects any `value_fields` entry whose destination column is
    /// `GENERATED ALWAYS AS (expr) STORED` or `GENERATED ALWAYS AS IDENTITY` —
    /// both forms are non-writable through a plain INSERT and would
    /// otherwise surface as a raw `db error` panic from the engine
    /// worker mid-flush. The `information_schema.columns` view exposes
    /// these shapes via the `is_generated` and `identity_generation`
    /// columns respectively.
    fn validate_no_always_generated_columns(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
               AND (is_generated = 'ALWAYS' OR identity_generation = 'ALWAYS')",
            &[&schema_name, &table_name],
        )?;
        let non_writable: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        let offending: Vec<String> = value_fields
            .iter()
            .map(|f| f.name.clone())
            .filter(|name| non_writable.contains(name))
            .collect();
        if !offending.is_empty() {
            return Err(PostgresError::GeneratedColumnInSchema {
                schema: schema_name.to_string(),
                table: table_name.to_string(),
                columns: offending,
            });
        }
        Ok(())
    }

    /// Verifies that the requested `schema_name` exists in the
    /// database. Without this check the CREATE TABLE / INSERT steps
    /// panic with an opaque driver error. `pg_namespace` is readable
    /// by every role.
    fn validate_schema_exists(
        client: &mut PsqlClient,
        schema_name: &str,
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT 1 FROM pg_namespace WHERE nspname = $1",
            &[&schema_name],
        )?;
        if rows.is_empty() {
            return Err(PostgresError::SchemaDoesNotExist {
                schema: schema_name.to_string(),
            });
        }
        Ok(())
    }

    /// Rejects a destination name that already exists as something
    /// other than an ordinary or partitioned table — views,
    /// materialized views, sequences, indices, composite types, and
    /// so on. All of these make both the `CREATE TABLE`-based init
    /// modes and the `INSERT` of `init_mode=Default` fail with an
    /// opaque driver panic; surfacing the shape up-front lets the
    /// user pick a different `table_name` or drop the offending
    /// object.
    fn validate_destination_is_table_or_absent(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT c.relkind
             FROM pg_class c
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema_name, &table_name],
        )?;
        let Some(row) = rows.first() else {
            return Ok(());
        };
        let relkind: i8 = row.get::<_, i8>(0);
        let kind: Option<&'static str> = match relkind.cast_unsigned() as char {
            'r' | 'p' => None, // ordinary / partitioned table
            'v' => Some("view"),
            'm' => Some("materialized view"),
            'i' | 'I' => Some("index"),
            'S' => Some("sequence"),
            'c' => Some("composite type"),
            'f' => Some("foreign table"),
            't' => Some("TOAST table"),
            _ => Some("non-table relation"),
        };
        if let Some(kind) = kind {
            return Err(PostgresError::DestinationIsNotATable {
                schema: schema_name.to_string(),
                table: table_name.to_string(),
                kind,
            });
        }
        Ok(())
    }

    /// Rejects a user schema that references columns missing from the
    /// destination table, and — in stream-of-changes / legacy modes —
    /// also requires the `time` / `diff` metadata columns the writer
    /// is about to INSERT into. Without this check the writer's
    /// generated `INSERT` embeds unknown column names and `PostgreSQL`
    /// rejects it at flush time with an opaque worker panic.
    fn validate_schema_columns_exist_in_destination(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        appends_time_diff: bool,
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2",
            &[&schema_name, &table_name],
        )?;
        // If the table doesn't exist at all, skip — the downstream
        // `INSERT` will surface the missing-table error as before,
        // and other preflights (e.g. the snapshot-mode PK check)
        // already short-circuit for non-existent tables.
        if rows.is_empty() {
            return Ok(());
        }
        let actual: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        let mut required: Vec<String> = value_fields.iter().map(|f| f.name.clone()).collect();
        if appends_time_diff {
            required.push("time".to_string());
            required.push("diff".to_string());
        }
        let missing: Vec<String> = required
            .into_iter()
            .filter(|name| !actual.contains(name))
            .collect();
        if !missing.is_empty() {
            return Err(PostgresError::WriterMissingDestinationColumns {
                schema: schema_name.to_string(),
                table: table_name.to_string(),
                columns: missing,
            });
        }
        Ok(())
    }

    /// Rejects a destination whose `time` metadata column is too
    /// narrow to hold a Pathway timestamp (ms since epoch, i64) or
    /// whose `diff` metadata column is too narrow to hold an i16.
    ///
    /// Without this check, small test values (debug time = 0) pass
    /// while production-time values overflow at flush with an opaque
    /// driver panic.
    fn validate_time_diff_column_widths(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT column_name, udt_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
               AND column_name IN ('time', 'diff')",
            &[&schema_name, &table_name],
        )?;
        for row in rows {
            let col: String = row.get(0);
            let udt: String = row.get(1);
            let (expected, acceptable): (&'static str, &[&str]) = match col.as_str() {
                // Pathway timestamps are 64-bit milliseconds since
                // epoch — anything narrower than int8 overflows.
                "time" => ("BIGINT", &["int8"][..]),
                // Pathway diff is +1 / -1 — int2 / int4 / int8 all fit.
                "diff" => ("SMALLINT or BIGINT", &["int2", "int4", "int8"][..]),
                _ => continue,
            };
            if !acceptable.iter().any(|s| *s == udt) {
                return Err(PostgresError::MetadataColumnTooNarrow {
                    schema: schema_name.to_string(),
                    table: table_name.to_string(),
                    column: col,
                    actual_udt: udt,
                    expected,
                });
            }
        }
        Ok(())
    }

    /// Rejects a schema column declared ``Optional[T]`` that maps to
    /// a destination column marked ``NOT NULL``. Without this check,
    /// the very first None row surfaces as a worker panic with an
    /// opaque ``violates not-null constraint`` message at flush time —
    /// potentially long after pipeline start when small test fixtures
    /// happen not to contain nulls.
    fn validate_nullability_matches_destination(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
               AND is_nullable = 'NO'",
            &[&schema_name, &table_name],
        )?;
        if rows.is_empty() {
            return Ok(());
        }
        let not_null_cols: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        let offending: Vec<String> = value_fields
            .iter()
            .filter(|f| f.type_.is_optional())
            .map(|f| f.name.clone())
            .filter(|name| not_null_cols.contains(name))
            .collect();
        if !offending.is_empty() {
            return Err(PostgresError::OptionalSchemaVsNotNullDestination {
                schema: schema_name.to_string(),
                table: table_name.to_string(),
                columns: offending,
            });
        }
        Ok(())
    }

    /// Rejects a destination whose column type cannot accept values
    /// of the declared Pathway type. Mirrors the reader's
    /// `validate_table_schema` type check; uses the shared
    /// `is_pathway_type_compatible_with_pg_udt` predicate.
    fn validate_destination_column_types(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT column_name, udt_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2",
            &[&schema_name, &table_name],
        )?;
        if rows.is_empty() {
            return Ok(());
        }
        let actual_udt_by_name: HashMap<String, String> = rows
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect();
        for field in value_fields {
            let Some(actual_udt) = actual_udt_by_name.get(&field.name) else {
                // Handled by validate_schema_columns_exist_in_destination.
                continue;
            };
            if !is_pathway_type_compatible_with_pg_udt(
                &field.type_,
                actual_udt,
                CompatDirection::Write,
            ) {
                return Err(PostgresError::WriterColumnTypeMismatch {
                    schema: schema_name.to_string(),
                    table: table_name.to_string(),
                    column: field.name.clone(),
                    declared: format!("{}", field.type_),
                    actual_udt: actual_udt.clone(),
                });
            }
        }
        Ok(())
    }

    /// Rejects a destination table that carries a NOT NULL column
    /// without a DEFAULT that the writer's INSERT would silently omit.
    /// `PostgreSQL` would reject every row at flush time with a NOT
    /// NULL constraint violation that surfaces only as a worker
    /// panic with an opaque `db error`. We also skip identity
    /// columns (both ALWAYS and BY DEFAULT) and generated columns —
    /// `PostgreSQL` fills those on its own.
    ///
    /// The stream-of-changes / legacy paths append `time` / `diff`
    /// metadata columns to every INSERT themselves; the caller flags
    /// that via `appends_time_diff` so those two names are not
    /// counted as offending.
    fn validate_no_extra_not_null_columns(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        appends_time_diff: bool,
    ) -> Result<(), PostgresError> {
        let rows = client.query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
               AND is_nullable = 'NO'
               AND column_default IS NULL
               AND is_generated = 'NEVER'
               AND identity_generation IS NULL",
            &[&schema_name, &table_name],
        )?;
        let schema_cols: HashSet<String> = value_fields.iter().map(|f| f.name.clone()).collect();
        let offending: Vec<String> = rows
            .iter()
            .map(|r| r.get::<_, String>(0))
            .filter(|name| {
                !(schema_cols.contains(name)
                    || appends_time_diff && matches!(name.as_str(), "time" | "diff"))
            })
            .collect();
        if !offending.is_empty() {
            return Err(PostgresError::ExtraNotNullColumns {
                schema: schema_name.to_string(),
                table: table_name.to_string(),
                columns: offending,
            });
        }
        Ok(())
    }

    /// Rejects a `primary_key` that doesn't match any UNIQUE or
    /// `PRIMARY KEY` constraint on the destination table. Snapshot-mode
    /// `INSERT ... ON CONFLICT (...)` statements require such a
    /// constraint — without it, `PostgreSQL` rejects the UPSERT at
    /// flush time with an opaque driver message. This runs only in
    /// `init_mode=Default`; the Replace / `CreateIfNotExists` paths
    /// emit the `PRIMARY KEY` themselves.
    ///
    /// We compare column *sets* (order-independent) because
    /// `PostgreSQL`'s `ON CONFLICT` clause matches a constraint
    /// regardless of column order. If the table doesn't exist at
    /// all, skip the check — the downstream `INSERT` will surface the
    /// missing-table error as before.
    fn validate_matching_unique_constraint(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        primary_key: &[String],
    ) -> Result<(), PostgresError> {
        let table_exists_rows = client.query(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = $2",
            &[&schema_name, &table_name],
        )?;
        if table_exists_rows.is_empty() {
            return Ok(());
        }
        let rows = client.query(
            "SELECT tc.constraint_name, kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema   = kcu.table_schema
              AND tc.table_name     = kcu.table_name
             WHERE tc.table_schema = $1 AND tc.table_name = $2
               AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')",
            &[&schema_name, &table_name],
        )?;
        let expected: HashSet<String> = primary_key.iter().cloned().collect();
        let mut by_constraint: HashMap<String, HashSet<String>> = HashMap::new();
        for row in rows {
            let cname: String = row.get(0);
            let col: String = row.get(1);
            by_constraint.entry(cname).or_default().insert(col);
        }
        let matches = by_constraint.values().any(|cols| *cols == expected);
        if !matches {
            return Err(PostgresError::NoMatchingUniqueConstraint {
                schema: schema_name.to_string(),
                table: table_name.to_string(),
                primary_key: primary_key.to_vec(),
            });
        }
        Ok(())
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
            // PostgreSQL ``DOMAIN`` types share the wire format of the
            // wrapped base type. rust-postgres usually reports the
            // base type to ``from_sql`` directly, but this defensive
            // unwrap covers any future protocol change or library
            // upgrade where the domain wrapper would otherwise reach
            // every ``accepts`` check (each of which compares the
            // exact ``Type``) and silently drop the row.
            if let Kind::Domain(base) = ty.kind() {
                return Self::from_sql(base, raw);
            }
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

                // Special values are encoded with ndigits=0 and a
                // distinctive `sign` byte; map them to their IEEE-754
                // counterparts rather than falling through the digit
                // loop (which would silently return 0.0).
                let special = match sign {
                    0xC000 => Some(f64::NAN),
                    0xD000 => Some(f64::INFINITY),
                    0xF000 => Some(f64::NEG_INFINITY),
                    _ => None,
                };
                if let Some(v) = special {
                    return Ok(Value::Float(OrderedFloat(v)));
                }

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

            if *ty == Type::MACADDR8 {
                // 8 raw bytes → "xx:xx:xx:xx:xx:xx:xx:xx" (EUI-64)
                let s = format!(
                    "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                    raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7]
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

            // Enums: PostgreSQL wire format is the raw UTF-8 label bytes.
            // rust-postgres' `String::accepts` rejects enum types
            // because their kind is `Kind::Enum`, not `Kind::Simple`.
            // Without this branch, reading a row whose schema column
            // is declared as `str` over an ENUM column silently
            // dropped the row — the conversion error bubbled up as a
            // per-row `ConversionError` which most sinks discard.
            if let Kind::Enum(_) = ty.kind() {
                return Ok(Value::String(
                    std::str::from_utf8(raw)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)?
                        .to_string()
                        .into(),
                ));
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

    #[error(
        "Publication '{publication}' does not cover {schema}.{table}; \
         add it with: ALTER PUBLICATION {publication} ADD TABLE {schema}.{table}"
    )]
    PublicationDoesNotCoverTable {
        publication: String,
        schema: String,
        table: String,
    },

    #[error(
        "Publication '{publication}' does not forward {missing:?} events \
         required by a non-append-only reader; fix it with: \
         ALTER PUBLICATION {publication} SET (publish = 'insert, update, delete, truncate')"
    )]
    PublicationMissingEventTypes {
        publication: String,
        missing: Vec<&'static str>,
    },

    #[error(
        "Table {schema}.{table} has REPLICA IDENTITY NOTHING; a \
         non-append-only streaming reader requires UPDATE / DELETE \
         events to carry the primary-key columns, so PostgreSQL would \
         forward incomplete rows (or reject the modifications \
         outright). Restore it with: \
         ALTER TABLE {schema}.{table} REPLICA IDENTITY DEFAULT \
         (or USING INDEX / FULL if you need the full row). \
         If the table is truly append-only, pass is_append_only=True."
    )]
    ReplicaIdentityNothing { schema: String, table: String },

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

    #[error(
        "Schema '{schema}' does not exist in the PostgreSQL database; \
         create it (CREATE SCHEMA \"{schema}\") or fix the schema_name parameter"
    )]
    ReaderSchemaDoesNotExist { schema: String },

    #[error(
        "Table {schema}.{table} does not exist in the PostgreSQL database; \
         create it or fix the table_name parameter"
    )]
    ReaderTableDoesNotExist { schema: String, table: String },

    #[error(
        "Column {schema}.{table}.{column} has PostgreSQL type '{actual_udt}' \
         which is not compatible with the declared Pathway type '{declared}'"
    )]
    ColumnTypeMismatch {
        schema: String,
        table: String,
        column: String,
        declared: String,
        actual_udt: String,
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
    is_append_only: bool,
    effective_snapshot_name: String,
}

#[derive(Debug)]
pub enum PgEventAfterParsing {
    SimpleChange(Box<ReadResult>),
    Truncate,
}

const MAX_SLOT_NAME_LEN: usize = 63;
const SLOT_NAME_PREFIX: &str = "pw_repl_";
const SLOT_NAME_TAG_LEN: usize = 8;

impl WalReader {
    fn new(
        settings: ReplicationSettings,
        table_ctx: TableContext,
        is_append_only: bool,
    ) -> Result<Self, ReadError> {
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
            Duration::from_mins(1),
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
            is_append_only,
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

        // PostgreSQL rejects replication slot names that contain
        // anything other than lower-case ASCII letters, digits, and
        // underscores (per the docs and the libpq error message:
        // ``replication slot names may only contain lower case
        // letters, numbers, and the underscore character``). Without
        // this sanitization a perfectly valid quoted table name like
        // ``"MyTable"`` or ``"my-table"`` — both accepted on every
        // other code path because identifiers are quoted — would
        // crash slot creation at pipeline start with that opaque
        // server-side message. We lowercase ASCII letters and replace
        // every other byte with ``_``; the trailing 8-char UUID tag
        // still guarantees uniqueness across collisions caused by
        // sanitization.
        let sanitize = |s: &str| {
            s.chars()
                .map(|c| match c {
                    'a'..='z' | '0'..='9' | '_' => c,
                    'A'..='Z' => c.to_ascii_lowercase(),
                    _ => '_',
                })
                .collect::<String>()
        };
        let mut base = format!("{}_{}", sanitize(schema_name), sanitize(table_name));

        let reserved = SLOT_NAME_PREFIX.len() + 1 + SLOT_NAME_TAG_LEN;
        let max_base_len = MAX_SLOT_NAME_LEN - reserved;

        // ``base`` is now ASCII-only (every char is a single byte),
        // so ``truncate`` is safe — it cannot split a UTF-8
        // codepoint. Without the sanitization above, a multi-byte
        // table_name (``テーブル``) would panic ``String::truncate``
        // on the char-boundary check whenever the byte length
        // overflowed ``max_base_len``.
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
        Self::py_object_wrapper_from_bincode(&raw_bytes, err)
    }

    fn py_object_wrapper_from_bincode<E>(
        raw_bytes: &[u8],
        err: &E,
    ) -> Result<Value, Box<ConversionError>>
    where
        E: Fn(String) -> Box<ConversionError>,
    {
        let value: Value = bincode::deserialize(raw_bytes)
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
                    // PostgreSQL's default ``intervalstyle = 'postgres'``
                    // output attaches a leading ``-`` to the *hours*
                    // field alone when the whole time-of-day component
                    // is negative — e.g. ``-1.5 hours`` is printed as
                    // ``-01:30:00``, not ``-01:-30:-00``. A naive parse
                    // (hours = -1, minutes = 30, seconds = 0) would
                    // wrongly yield -30 minutes; propagate the sign
                    // across all three fields instead.
                    let is_negative = parts[0].trim_start().starts_with('-');
                    let hours_abs_str = if is_negative {
                        parts[0].trim_start().trim_start_matches('-')
                    } else {
                        parts[0]
                    };
                    let hours: i64 = hours_abs_str
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
                    let sign: i64 = if is_negative { -1 } else { 1 };
                    total_nanos += sign
                        * ((hours * 3_600 + minutes * 60 + secs) * 1_000_000_000 + nanos)
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

    fn parse_value_from_column_value(
        column_value: &ColumnValue,
        type_: &Type,
        field_name: &str,
    ) -> Result<Value, Box<ConversionError>> {
        match column_value {
            ColumnValue::Null => Ok(Value::None),
            ColumnValue::Text(_) => {
                let s = column_value.as_str().unwrap_or_default();
                let err = |msg: String| {
                    Box::new(ConversionError {
                        value_repr: s.to_string(),
                        field_name: field_name.to_string(),
                        type_: type_.clone(),
                        original_error_message: msg,
                    })
                };
                Self::parse_value_from_str(s, type_, field_name, &err)
            }
            ColumnValue::Binary(bytes) => {
                let err = |msg: String| {
                    Box::new(ConversionError {
                        value_repr: format!("<{} binary bytes>", bytes.len()),
                        field_name: field_name.to_string(),
                        type_: type_.clone(),
                        original_error_message: msg,
                    })
                };
                match type_ {
                    Type::Bytes => Ok(Value::Bytes(bytes.as_ref().into())),
                    Type::PyObjectWrapper => {
                        Self::py_object_wrapper_from_bincode(bytes.as_ref(), &err)
                    }
                    other => Err(err(format!(
                        "Cannot convert binary column data into type: {other}"
                    ))),
                }
            }
        }
    }

    fn parse_values_from_event(
        event_type: DataEventType,
        raw: &RowData,
        table_ctx: &TableContext,
    ) -> ReaderContext {
        let values_map: ValuesMap = table_ctx
            .value_fields
            .iter()
            .map(|field| {
                let result = match raw.get(&field.name) {
                    Some(cv) => Self::parse_value_from_column_value(cv, &field.type_, &field.name),
                    None => Ok(Value::None),
                };
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
        is_append_only: bool,
        event: ChangeEvent,
        total_entries_read: &mut usize,
    ) -> Result<Vec<PgEventAfterParsing>, ReplicationError> {
        let event_type = if matches!(event.event_type, EventType::Delete { .. }) {
            DataEventType::Delete
        } else {
            DataEventType::Insert
        };

        if is_append_only
            && matches!(
                event.event_type,
                EventType::Truncate(_) | EventType::Update { .. } | EventType::Delete { .. }
            )
        {
            return Err(ReplicationError::AppendOnlyNotRespected(event));
        }

        Ok(match event.event_type {
            EventType::Begin { transaction_id, .. } => {
                vec![PgEventAfterParsing::SimpleChange(Box::new(
                    ReadResult::NewSource(PostgresMetadata::new(Some(transaction_id)).into()),
                ))]
            }
            EventType::Commit { .. } => vec![PgEventAfterParsing::SimpleChange(Box::new(
                ReadResult::FinishedSource {
                    commit_possibility: CommitPossibility::Possible,
                },
            ))],
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
                    result.push(PgEventAfterParsing::SimpleChange(Box::new(
                        ReadResult::Data(
                            Self::parse_values_from_event(
                                DataEventType::Delete,
                                &old_data,
                                table_ctx,
                            ),
                            emit_offset(total_entries_read),
                        ),
                    )));
                }
                result.push(PgEventAfterParsing::SimpleChange(Box::new(
                    ReadResult::Data(
                        Self::parse_values_from_event(DataEventType::Insert, &new_data, table_ctx),
                        emit_offset(total_entries_read),
                    ),
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
                let ctx = Self::parse_values_from_event(event_type, &data, table_ctx);
                vec![PgEventAfterParsing::SimpleChange(Box::new(
                    ReadResult::Data(ctx, emit_offset(total_entries_read)),
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
        let is_append_only = self.is_append_only;
        self.runtime.block_on(async {
            loop {
                match self.stream.next_event_with_retry(&self.cancel_token).await {
                    Ok(event) => {
                        self.stream
                            .shared_lsn_feedback
                            .update_applied_lsn(event.lsn.value());
                        let parsed_events = Self::parse_incoming_event(
                            table_ctx,
                            is_append_only,
                            event,
                            total_entries_read,
                        )?;
                        if !parsed_events.is_empty() {
                            return Ok(parsed_events);
                        }
                    }
                    Err(pg_walstream::ReplicationError::Cancelled(_)) => {
                        info!("Cancelled, shutting down gracefully");
                        return Ok(vec![PgEventAfterParsing::SimpleChange(Box::new(
                            ReadResult::Finished,
                        ))]);
                    }
                    Err(e) => {
                        return Err(PostgresError::Replication(ReplicationError::from(e)).into())
                    }
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

/// `PostgreSQL` built-in scalar types that neither the reader's
/// ``FromSql`` nor the writer's ``ToSql`` has a dedicated branch for.
/// These would otherwise pass the permissive "custom or array" fallback
/// of ``is_pathway_type_compatible_with_pg_udt`` (because the
/// ``udt_name`` is not in the known-supported list), then fail
/// silently at runtime: the reader drops the row with a logged
/// warning and the writer panics the worker with
/// ``error serializing parameter N``. Listing them here turns the
/// failure into a clean preflight ``ColumnTypeMismatch``.
///
/// Extend this list conservatively — a genuinely user-defined type
/// (enum, domain, composite) also surfaces with an unfamiliar
/// ``udt_name``, so we must *not* reject every unknown name.
const KNOWN_UNSUPPORTED_PG_UDTS: &[&str] = &[
    // currency & bit strings
    "money",
    "bit",
    "varbit",
    // full-text search
    "tsvector",
    "tsquery",
    // XML
    "xml",
    // geometric
    "point",
    "line",
    "lseg",
    "box",
    "path",
    "polygon",
    "circle",
    // range / multirange
    "int4range",
    "int8range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
    "int4multirange",
    "int8multirange",
    "nummultirange",
    "tsmultirange",
    "tstzmultirange",
    "datemultirange",
    // miscellaneous built-ins without a Pathway mapping
    "jsonpath",
    "pg_lsn",
    "txid_snapshot",
    "xid",
    "xid8",
    "cid",
    "tid",
    "aclitem",
    "refcursor",
    // object identifier aliases
    "regproc",
    "regprocedure",
    "regoper",
    "regoperator",
    "regclass",
    "regcollation",
    "regtype",
    "regrole",
    "regnamespace",
    "regconfig",
    "regdictionary",
];

/// Direction of the data flow. Some Pathway↔Postgres type pairings
/// are legal only on one side — e.g. the reader has a dedicated
/// NUMERIC→f64 branch but the writer has no f64→NUMERIC serializer,
/// so the *same* ``(pw.Float, numeric)`` pair must be accepted when
/// reading and rejected when writing.
#[derive(Clone, Copy, Debug)]
pub enum CompatDirection {
    Read,
    Write,
}

/// Tests whether the `PostgreSQL` column with `udt_name = actual_udt`
/// can legitimately be parsed into the Pathway `declared` type.
/// Used by both the reader preflight (to reject a Pathway schema
/// that lies about column types) and the writer preflight (to
/// reject an `INSERT` whose serialization would fail at flush
/// because the destination column is of an incompatible type).
///
/// Mirror of the read-side mapping described in `360.postgres.rst`.
/// This is a scalar-only check — container types (ARRAY, custom
/// DOMAIN / ENUM / extensions like `pgvector`) fall through to
/// `permissive = true` because the runtime `ToSql` / `FromSql`
/// impls validate them and enumerating every element shape here
/// would be brittle.
fn is_pathway_type_compatible_with_pg_udt(
    declared: &Type,
    actual_udt: &str,
    direction: CompatDirection,
) -> bool {
    // Explicitly reject known-unsupported built-ins. Without this
    // early return, the permissive fallback below would let them
    // pass preflight because their udt_name is not in the
    // known-supported list, and the runtime parser would then drop
    // rows silently (reader) or panic the worker (writer).
    if KNOWN_UNSUPPORTED_PG_UDTS.contains(&actual_udt) {
        return false;
    }
    // When the destination is a PostgreSQL array (``_int4`` →
    // ``int4[]``), validate that the *leaf* element types line up.
    // Without this, a ``list[str]`` against a ``BIGINT[]`` passes
    // preflight and fails at flush with a bare "cannot convert
    // value of type 'string' to Postgres type int8" per element.
    // PG keeps a single ``_T`` udt regardless of nesting depth, so
    // peel off every List/Array/homogeneous-Tuple layer on the
    // declared side and compare the leaf to ``T``.
    if let Some(elem_udt) = actual_udt.strip_prefix('_') {
        let mut leaf = declared.unoptionalize().clone();
        let mut is_container = false;
        loop {
            let next = match &leaf {
                Type::List(inner) | Type::Array(_, inner) => {
                    is_container = true;
                    inner.as_ref().clone()
                }
                Type::Tuple(fields)
                    if !fields.is_empty() && fields.iter().all(|f| f == &fields[0]) =>
                {
                    is_container = true;
                    fields[0].clone()
                }
                _ => break,
            };
            leaf = next.unoptionalize().clone();
        }
        if is_container {
            // Rely on the scalar matcher (below, after the
            // custom/array gate) to decide whether ``leaf`` is
            // serializable into ``elem_udt``.
            return is_pathway_type_compatible_with_pg_udt(&leaf, elem_udt, direction);
        }
        // Declared is a scalar against an array column: fall through
        // to the scalar match, which rejects any scalar pathway type
        // against an underscore-prefixed UDT via the default arm.
    }
    // Arrays (both built-in like `_int4` and pgvector's `vector`/`halfvec`)
    // and any non-standard udt_name (custom domains / enums) are
    // permitted unconditionally — the runtime parser validates them.
    let looks_like_custom_or_array = actual_udt.starts_with('_')
        || matches!(actual_udt, "vector" | "halfvec")
        || !matches!(
            actual_udt,
            "bool"
                | "int2"
                | "int4"
                | "int8"
                | "oid"
                | "float4"
                | "float8"
                | "numeric"
                | "text"
                | "varchar"
                | "bpchar"
                | "name"
                | "uuid"
                | "inet"
                | "cidr"
                | "macaddr"
                | "macaddr8"
                | "bytea"
                | "json"
                | "jsonb"
                | "timestamp"
                | "timestamptz"
                | "date"
                | "time"
                | "timetz"
                | "interval"
        );
    if looks_like_custom_or_array {
        return true;
    }
    let is_read = matches!(direction, CompatDirection::Read);
    match declared.unoptionalize() {
        Type::Bool => actual_udt == "bool",
        // `oid` reads cleanly into an i64 (we widen 32-bit OID → 64-bit
        // signed), but no ``try_forward`` arm on the write side
        // accepts ``Type::OID`` for ``i64``/``i32``/``i16``/``i8``
        // (rust-postgres routes OID exclusively through ``u32``).
        // Allow it on read only.
        Type::Int => {
            let base = matches!(actual_udt, "int2" | "int4" | "int8");
            base || (is_read && actual_udt == "oid")
        }
        // Reader decodes NUMERIC into f64 via our own digit-array
        // parser; rust-postgres' ``<f64 as ToSql>::accepts`` rejects
        // NUMERIC, and we have no f64→NUMERIC serializer, so writing
        // silently corrupted messages. Reject the write preflight.
        Type::Float => {
            let base = matches!(actual_udt, "float4" | "float8");
            base || (is_read && actual_udt == "numeric")
        }
        Type::String => matches!(
            actual_udt,
            "text"
                | "varchar"
                | "bpchar"
                | "name"
                | "uuid"
                | "inet"
                | "cidr"
                | "macaddr"
                | "macaddr8"
        ),
        Type::Pointer => actual_udt == "text",
        Type::Bytes | Type::PyObjectWrapper => actual_udt == "bytea",
        Type::Json => matches!(actual_udt, "json" | "jsonb"),
        Type::DateTimeNaive => matches!(actual_udt, "timestamp" | "date"),
        Type::DateTimeUtc => actual_udt == "timestamptz",
        // ``time`` / ``timetz`` both reach the reader via a dedicated
        // binary branch. The writer only encodes INTERVAL and TIME —
        // TIMETZ would need a session-offset parameter we don't carry
        // on ``Value::Duration``, so reject it on writes.
        Type::Duration => {
            let base = matches!(actual_udt, "int2" | "int4" | "int8" | "interval" | "time");
            base || (is_read && matches!(actual_udt, "oid" | "timetz"))
        }
        Type::List(_) | Type::Tuple(_) | Type::Array(_, _) => false,
        // `unoptionalize()` strips the outer Optional; Pathway
        // doesn't allow nested Optional, so the Optional arm is
        // never reached in practice but must be covered
        // exhaustively.
        Type::Any | Type::Future(_) | Type::Optional(_) => true,
    }
}

impl PsqlReader {
    #[allow(clippy::too_many_lines)]
    pub fn new(
        mut client: PsqlClient,
        replication_settings: Option<ReplicationSettings>,
        table_ctx: TableContext,
        is_append_only: bool,
    ) -> Result<Self, ReadError> {
        Self::validate_table_schema(&mut client, &table_ctx, is_append_only)?;

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
            // Ensure the publication actually forwards events for the
            // target table. Without this check a publication that
            // exists but was created for a different table lets
            // `PsqlReader::new` succeed, the snapshot SELECT runs
            // normally, and the streaming loop then sits idle forever
            // because pgoutput has nothing to forward for this table.
            // `pg_publication_tables` is a catalog view that expands
            // both explicit `FOR TABLE` and `FOR ALL TABLES`
            // publications, so a single membership query covers both.
            let coverage_rows = client
                .query(
                    "SELECT 1 FROM pg_publication_tables \
                     WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
                    &[
                        &replication_settings.publication_name,
                        &table_ctx.schema_name,
                        &table_ctx.table_name,
                    ],
                )
                .map_err(ReplicationError::Query)?;
            if coverage_rows.is_empty() {
                return Err(ReplicationError::PublicationDoesNotCoverTable {
                    publication: replication_settings.publication_name.clone(),
                    schema: table_ctx.schema_name.clone(),
                    table: table_ctx.table_name.clone(),
                }
                .into());
            }
            // Publication's `publish` option filters event types
            // *inside* PostgreSQL, so a subset like `publish='insert'`
            // silently drops UPDATE/DELETE/TRUNCATE before they reach
            // the reader. For a non-append-only reader this is data
            // drift without any signal; for an append-only reader we
            // only need INSERT events forwarded (unexpected UPDATE/DELETEs
            // are caught later by the `AppendOnlyNotRespected` check
            // if the publication does forward them). Require
            // `pubinsert` unconditionally; the other three only when
            // `is_append_only = false`.
            let publish_rows = client
                .query(
                    "SELECT pubinsert, pubupdate, pubdelete, pubtruncate \
                     FROM pg_publication WHERE pubname = $1",
                    &[&replication_settings.publication_name],
                )
                .map_err(ReplicationError::Query)?;
            // pg_publication existence was verified above — a missing
            // row here would indicate a concurrent DROP, which we
            // surface the same way.
            let publish_row =
                publish_rows
                    .first()
                    .ok_or_else(|| ReplicationError::PublicationNotFound {
                        name: replication_settings.publication_name.clone(),
                    })?;
            let pubinsert: bool = publish_row.get(0);
            let pubupdate: bool = publish_row.get(1);
            let pubdelete: bool = publish_row.get(2);
            let pubtruncate: bool = publish_row.get(3);
            let mut missing: Vec<&'static str> = Vec::new();
            if !pubinsert {
                missing.push("insert");
            }
            if !is_append_only {
                if !pubupdate {
                    missing.push("update");
                }
                if !pubdelete {
                    missing.push("delete");
                }
                if !pubtruncate {
                    missing.push("truncate");
                }
            }
            if !missing.is_empty() {
                return Err(ReplicationError::PublicationMissingEventTypes {
                    publication: replication_settings.publication_name.clone(),
                    missing,
                }
                .into());
            }
            // ``REPLICA IDENTITY NOTHING`` strips the primary-key
            // columns from WAL UPDATE/DELETE events, so a
            // non-append-only Pathway reader can never match the
            // retraction to the right snapshot row. PostgreSQL
            // itself also rejects UPDATE/DELETE on such a table
            // when it's part of a publication that forwards those
            // event types — but the error only surfaces at INSERT
            // time, not at pipeline start. Catch it up front.
            if !is_append_only {
                let rid_rows = client
                    .query(
                        "SELECT c.relreplident \
                         FROM pg_class c \
                         JOIN pg_namespace n ON c.relnamespace = n.oid \
                         WHERE n.nspname = $1 AND c.relname = $2",
                        &[&table_ctx.schema_name, &table_ctx.table_name],
                    )
                    .map_err(ReplicationError::Query)?;
                if let Some(row) = rid_rows.first() {
                    let relreplident: i8 = row.get(0);
                    if relreplident.cast_unsigned() as char == 'n' {
                        return Err(ReplicationError::ReplicaIdentityNothing {
                            schema: table_ctx.schema_name.clone(),
                            table: table_ctx.table_name.clone(),
                        }
                        .into());
                    }
                }
            }
            Some(WalReader::new(
                replication_settings,
                table_ctx.clone(),
                is_append_only,
            )?)
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
        is_append_only: bool,
    ) -> Result<(), ReplicationError> {
        // Schema existence first — a missing schema_name otherwise
        // surfaces as a misleading "Fields not found" (the columns
        // query returns an empty set regardless of whether the table
        // exists or the schema does).
        let schema_rows = client.query(
            "SELECT 1 FROM pg_namespace WHERE nspname = $1",
            &[&table_ctx.schema_name],
        )?;
        if schema_rows.is_empty() {
            return Err(ReplicationError::ReaderSchemaDoesNotExist {
                schema: table_ctx.schema_name.clone(),
            });
        }
        let columns_query = "
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
        ";
        let column_rows = client.query(
            columns_query,
            &[&table_ctx.schema_name, &table_ctx.table_name],
        )?;
        if column_rows.is_empty() {
            // Schema exists (checked above), so no columns means the
            // table itself doesn't exist. Surface that directly
            // rather than the misleading "Fields not found".
            return Err(ReplicationError::ReaderTableDoesNotExist {
                schema: table_ctx.schema_name.clone(),
                table: table_ctx.table_name.clone(),
            });
        }
        let actual_udt_by_name: HashMap<String, String> = column_rows
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect();

        let missing: Vec<String> = table_ctx
            .value_fields
            .iter()
            .map(|f| f.name.clone())
            .filter(|name| !actual_udt_by_name.contains_key(name))
            .collect();
        if !missing.is_empty() {
            return Err(ReplicationError::MissingValueFields {
                schema: table_ctx.schema_name.clone(),
                table: table_ctx.table_name.clone(),
                missing_fields: missing,
            });
        }

        // Upfront type compatibility check. The reader's runtime
        // `FromSql` only tests whether it *can* produce some Value for
        // the column, not whether that Value's type matches what the
        // user declared — so a pw.Schema that lies about the column
        // type used to silently deliver wrong-typed rows downstream
        // (surfacing only when a later operator type-checked and
        // panicked). Catch it here with a clear message.
        for field in &table_ctx.value_fields {
            let actual_udt = &actual_udt_by_name[&field.name];
            if !is_pathway_type_compatible_with_pg_udt(
                &field.type_,
                actual_udt,
                CompatDirection::Read,
            ) {
                return Err(ReplicationError::ColumnTypeMismatch {
                    schema: table_ctx.schema_name.clone(),
                    table: table_ctx.table_name.clone(),
                    column: field.name.clone(),
                    declared: format!("{}", field.type_),
                    actual_udt: actual_udt.clone(),
                });
            }
        }

        if !is_append_only {
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
                return Err(e.into());
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
                .map(|vf| quote_identifier(&vf.name))
                .join(","),
            quote_identifier(&table_ctx.schema_name),
            quote_identifier(&table_ctx.table_name),
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
                        self.prepared_records.push(*prepared_record);
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
